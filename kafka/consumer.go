package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/internal/coordinator/ingestion"
)

// pollTimeoutMs is the timeout for the initial Poll call when no messages are buffered.
const pollTimeoutMs = 100

// maxPollBatch is the maximum number of messages to drain per poll cycle.
const maxPollBatch = 1000

var (
	transformerMu       sync.RWMutex
	transformerRegistry = map[string]func() MessageTransformer{}
)

// RegisterTransformer registers a named message transformer factory.
func RegisterTransformer(name string, factory func() MessageTransformer) {
	transformerMu.Lock()
	defer transformerMu.Unlock()
	transformerRegistry[name] = factory
}

// NewTransformer creates a transformer by name. Falls back to AutoDetectTransformer.
func NewTransformer(name string) MessageTransformer {
	transformerMu.RLock()
	factory, ok := transformerRegistry[name]
	transformerMu.RUnlock()
	if ok {
		return factory()
	}
	return &AutoDetectTransformer{}
}

func init() {
	RegisterTransformer("", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("auto", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("proto", func() MessageTransformer { return &ProtoTransformer{} })
}

// ProtoTransformer deserializes protobuf MetadataRecord payloads.
type ProtoTransformer struct{}

func (t *ProtoTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
	var record pb.MetadataRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return &record, nil
}

// AutoDetectTransformer auto-detects JSON vs protobuf payloads.
// JSON payloads start with '{'; everything else is treated as protobuf.
type AutoDetectTransformer struct{}

func (t *AutoDetectTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
	if len(payload) > 0 && payload[0] == '{' {
		return unmarshalJSONToProto(payload)
	}
	var record pb.MetadataRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return &record, nil
}

// MessageTransformer transforms raw Kafka message bytes into a MetadataRecord.
type MessageTransformer interface {
	Transform(payload []byte) (*pb.MetadataRecord, error)
}

// pendingFlush tracks a submitted record awaiting DB flush confirmation.
type pendingFlush struct {
	flushed   chan error
	topic     *string
	partition int32
	offset    kafka.Offset
}

// Consumer reads metadata records from a Kafka topic and submits them
// through the IngestionService for proper dim/agg column resolution.
type Consumer struct {
	bootstrapServers string
	groupID          string
	topic            string
	transformer      MessageTransformer
	service          *ingestion.Service
	tableName        string
	log              *zap.Logger

	// pendingFlushes tracks records submitted to the BatchingWriter but not
	// yet confirmed as flushed. Drained each poll cycle — no goroutines needed.
	pendingFlushes []pendingFlush

	lastOffsets   map[int32]kafka.Offset
	pendingCommit []kafka.TopicPartition
}

// NewConsumer creates a Kafka metadata consumer.
func NewConsumer(
	bootstrapServers, groupID, topic, tableName string,
	transformer MessageTransformer,
	service *ingestion.Service,
	log *zap.Logger,
) *Consumer {
	return &Consumer{
		bootstrapServers: bootstrapServers,
		groupID:          groupID,
		topic:            topic,
		tableName:        tableName,
		transformer:      transformer,
		service:          service,
		log:              log.With(zap.String("topic", topic), zap.String("table", tableName)),
		lastOffsets:      make(map[int32]kafka.Offset),
	}
}

// Run starts consuming from the Kafka topic until ctx is canceled.
// It batch-polls messages: waits up to pollTimeoutMs for the first message,
// then drains all buffered messages (up to maxPollBatch) with non-blocking polls.
func (c *Consumer) Run(ctx context.Context) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  c.bootstrapServers,
		"group.id":           c.groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		// Pre-fetch up to 1MB per partition for better batching.
		"fetch.min.bytes":           1,
		"max.partition.fetch.bytes": 1048576,
	})
	if err != nil {
		c.log.Error("failed to create kafka consumer",
			zap.String("bootstrapServers", c.bootstrapServers),
			zap.Error(err))
		return
	}
	defer consumer.Close()

	if err := consumer.Subscribe(c.topic, nil); err != nil {
		c.log.Error("failed to subscribe to kafka topic",
			zap.String("topic", c.topic),
			zap.Error(err))
		return
	}

	c.log.Info("kafka consumer started",
		zap.String("bootstrapServers", c.bootstrapServers),
		zap.String("groupId", c.groupID),
	)

	for {
		select {
		case <-ctx.Done():
			c.drainFlushesBlocking()
			c.commitPending(consumer)
			c.log.Info("kafka consumer stopped")
			return
		default:
		}

		// Check which submitted records have been flushed and queue their
		// offsets for commit — no goroutines needed.
		c.drainFlushes()
		c.commitPending(consumer)

		// Wait for at least one message.
		ev := consumer.Poll(pollTimeoutMs)
		if ev == nil {
			continue
		}

		// Process first message, then drain all buffered messages.
		processed := 0
		if c.handleEvent(ctx, ev, &processed) {
			c.commitPending(consumer)
			return // fatal error
		}

		for processed < maxPollBatch {
			ev = consumer.Poll(0) // non-blocking: returns nil if no buffered messages
			if ev == nil {
				break
			}
			if c.handleEvent(ctx, ev, &processed) {
				c.commitPending(consumer)
				return // fatal error
			}
		}

		if processed > 0 {
			c.log.Debug("poll batch processed", zap.Int("messages", processed))
		}
	}
}

// handleEvent processes a single Kafka event. Returns true if a fatal error
// occurred and the consumer should stop.
func (c *Consumer) handleEvent(ctx context.Context, ev kafka.Event, processed *int) bool {
	switch e := ev.(type) {
	case *kafka.Message:
		c.handleMessage(ctx, e)
		*processed++
	case kafka.Error:
		if e.IsFatal() {
			c.log.Error("fatal kafka error, stopping consumer", zap.Error(e))
			c.drainFlushes()
			return true
		}
		c.log.Warn("kafka consumer error (transient)", zap.Error(e))
	}
	return false
}

// handleMessage transforms and ingests a single Kafka message through the IngestionService.
// The flush channel is tracked in pendingFlushes and drained each poll cycle.
func (c *Consumer) handleMessage(ctx context.Context, msg *kafka.Message) {
	record, err := c.transformer.Transform(msg.Value)
	if err != nil {
		c.log.Warn("transform failed",
			zap.Int32("partition", msg.TopicPartition.Partition),
			zap.Any("offset", msg.TopicPartition.Offset),
			zap.Error(err),
		)
		return
	}

	flushed := make(chan error, 1)

	if err := c.service.IngestWithCallback(ctx, c.tableName, record, flushed); err != nil {
		c.log.Warn("ingest failed",
			zap.Int32("partition", msg.TopicPartition.Partition),
			zap.Any("offset", msg.TopicPartition.Offset),
			zap.Error(err),
		)
		return
	}

	c.pendingFlushes = append(c.pendingFlushes, pendingFlush{
		flushed:   flushed,
		topic:     msg.TopicPartition.Topic,
		partition: msg.TopicPartition.Partition,
		offset:    msg.TopicPartition.Offset,
	})
}

// drainFlushesBlocking waits for all pending flushes to complete (with a timeout)
// before shutdown. This ensures Kafka offsets are committed for records that are
// still being flushed by the BatchingWriter.
func (c *Consumer) drainFlushesBlocking() {
	if len(c.pendingFlushes) == 0 {
		return
	}
	deadline := time.After(5 * time.Second)
	for _, pf := range c.pendingFlushes {
		select {
		case err := <-pf.flushed:
			if err != nil {
				c.log.Warn("flush failed on shutdown, skipping offset commit",
					zap.Int32("partition", pf.partition),
					zap.Any("offset", pf.offset),
					zap.Error(err),
				)
				continue
			}
			c.pendingCommit = append(c.pendingCommit, kafka.TopicPartition{
				Topic:     pf.topic,
				Partition: pf.partition,
				Offset:    pf.offset + 1,
			})
		case <-deadline:
			c.log.Error("shutdown drain timeout, uncommitted flushes remain",
				zap.Int("remaining", len(c.pendingFlushes)))
			c.pendingFlushes = nil
			return
		}
	}
	c.pendingFlushes = nil
}

// drainFlushes checks all pending flush channels without blocking. Completed
// flushes have their offsets queued for commit; failed flushes are dropped.
// Entries that haven't received a result yet are retained for the next cycle.
func (c *Consumer) drainFlushes() {
	remaining := c.pendingFlushes[:0] // reuse backing array
	for _, pf := range c.pendingFlushes {
		select {
		case err := <-pf.flushed:
			if err != nil {
				c.log.Warn("flush failed, skipping offset commit",
					zap.Int32("partition", pf.partition),
					zap.Any("offset", pf.offset),
					zap.Error(err),
				)
				continue
			}
			if existing, ok := c.lastOffsets[pf.partition]; !ok || pf.offset > existing {
				c.lastOffsets[pf.partition] = pf.offset
			}
			c.pendingCommit = append(c.pendingCommit, kafka.TopicPartition{
				Topic:     pf.topic,
				Partition: pf.partition,
				Offset:    pf.offset + 1,
			})
		default:
			// Not flushed yet — keep for next cycle.
			remaining = append(remaining, pf)
		}
	}
	c.pendingFlushes = remaining
}

// commitPending commits any pending offsets to Kafka.
func (c *Consumer) commitPending(consumer *kafka.Consumer) {
	if len(c.pendingCommit) == 0 {
		return
	}

	// Deduplicate: keep highest offset per partition.
	best := make(map[int32]kafka.TopicPartition)
	for _, tp := range c.pendingCommit {
		if existing, ok := best[tp.Partition]; !ok || tp.Offset > existing.Offset {
			best[tp.Partition] = tp
		}
	}
	c.pendingCommit = c.pendingCommit[:0]

	deduped := make([]kafka.TopicPartition, 0, len(best))
	for _, tp := range best {
		deduped = append(deduped, tp)
	}

	if _, err := consumer.CommitOffsets(deduped); err != nil {
		c.log.Warn("offset commit failed", zap.Error(err))
	}
}

// LastPolledOffsets returns the last polled offset per partition.
// Only safe to call from the consumer's goroutine or after Run returns.
func (c *Consumer) LastPolledOffsets() map[int32]kafka.Offset {
	result := make(map[int32]kafka.Offset, len(c.lastOffsets))
	for k, v := range c.lastOffsets {
		result[k] = v
	}
	return result
}

// PartitionWatermark holds low/high watermarks for a single partition.
type PartitionWatermark struct {
	Partition int32
	Low       int64
	High      int64
}

// QueryWatermarks fetches the low and high watermarks for all assigned partitions.
// Must be called after the consumer has started (from the consumer's goroutine or
// via the kafka.Consumer handle). Returns nil if the consumer is not available.
func QueryWatermarks(consumer *kafka.Consumer, topic string, timeoutMs int) ([]PartitionWatermark, error) {
	meta, err := consumer.GetMetadata(&topic, false, timeoutMs)
	if err != nil {
		return nil, fmt.Errorf("get metadata: %w", err)
	}

	topicMeta, ok := meta.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found in metadata", topic)
	}

	watermarks := make([]PartitionWatermark, 0, len(topicMeta.Partitions))
	for _, p := range topicMeta.Partitions {
		low, high, err := consumer.QueryWatermarkOffsets(topic, p.ID, timeoutMs)
		if err != nil {
			return nil, fmt.Errorf("query watermarks partition %d: %w", p.ID, err)
		}
		watermarks = append(watermarks, PartitionWatermark{
			Partition: p.ID,
			Low:       low,
			High:      high,
		})
	}
	return watermarks, nil
}

// ComputeLag calculates per-partition lag as high watermark minus last committed offset.
func ComputeLag(watermarks []PartitionWatermark, lastOffsets map[int32]kafka.Offset) map[int32]int64 {
	lag := make(map[int32]int64, len(watermarks))
	for _, wm := range watermarks {
		committed, ok := lastOffsets[wm.Partition]
		if !ok {
			lag[wm.Partition] = wm.High - wm.Low
		} else {
			lag[wm.Partition] = wm.High - int64(committed) - 1
			if lag[wm.Partition] < 0 {
				lag[wm.Partition] = 0
			}
		}
	}
	return lag
}

// unmarshalJSONToProto parses a JSON payload into a MetadataRecord.
// Supports a flat JSON format with fields matching the proto message.
func unmarshalJSONToProto(payload []byte) (*pb.MetadataRecord, error) {
	var raw struct {
		State        string `json:"state"`
		MinTimestamp int64  `json:"min_timestamp"`
		MaxTimestamp int64  `json:"max_timestamp"`
		RawSizeBytes int64  `json:"raw_size_bytes"`
		RecordCount  int32  `json:"record_count"`
		IR           *struct {
			StorageBackend string `json:"storage_backend"`
			Bucket         string `json:"bucket"`
			Path           string `json:"path"`
			SizeBytes      int64  `json:"size_bytes"`
		} `json:"ir"`
		Dims []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			Width int32  `json:"width"`
		} `json:"dims"`
		Aggs []struct {
			Field     string `json:"field"`
			Qualifier string `json:"qualifier"`
			Type      string `json:"type"`
			IntVal    int64  `json:"int_val"`
		} `json:"aggs"`
		SelfDescribingKV []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"self_describing_kv"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	record := &pb.MetadataRecord{
		File: &pb.FileFields{
			State:        raw.State,
			MinTimestamp: raw.MinTimestamp,
			MaxTimestamp: raw.MaxTimestamp,
			RawSizeBytes: raw.RawSizeBytes,
			RecordCount:  raw.RecordCount,
		},
	}

	if raw.IR != nil {
		record.File.Ir = &pb.IrFileInfo{
			ClpIrStorageBackend: raw.IR.StorageBackend,
			ClpIrBucket:         raw.IR.Bucket,
			ClpIrPath:           raw.IR.Path,
			ClpIrSizeBytes:      raw.IR.SizeBytes,
		}
	}

	for _, d := range raw.Dims {
		width := d.Width
		if width == 0 {
			width = 64
		}
		record.Dim = append(record.Dim, &pb.DimEntry{
			Key: d.Key,
			Value: &pb.DimensionValue{
				Value: &pb.DimensionValue_Str{
					Str: &pb.StringDimension{Value: d.Value, MaxLength: width},
				},
			},
		})
	}

	for _, a := range raw.Aggs {
		aggType := pb.AggType_GTE
		if a.Type != "" {
			if v, ok := pb.AggType_value[a.Type]; ok {
				aggType = pb.AggType(v)
			}
		}
		record.Agg = append(record.Agg, &pb.AggEntry{
			Field:     a.Field,
			Qualifier: a.Qualifier,
			AggType:   aggType,
			Value:     &pb.AggEntry_IntVal{IntVal: a.IntVal},
		})
	}

	// Parse self-describing key-value entries. The key prefix determines the
	// entry type:
	//   sketch/{type}/{field}              → SketchEntry (value is base64 raw filter bytes)
	//   dim/{typeSpec}/{field}             → DimEntry
	//   agg_int/{type}/{field}[/{qual}]   → AggEntry (INT)
	//   agg_float/{type}/{field}[/{qual}] → AggEntry (FLOAT)
	for _, kv := range raw.SelfDescribingKV {
		if err := parseSelfDescribingEntry(kv.Key, kv.Value, record); err != nil {
			// Skip malformed entries rather than dropping the entire record.
			// The entry is passed through as-is so it can be inspected later.
			record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
				Key:   kv.Key,
				Value: kv.Value,
			})
		}
	}

	return record, nil
}

// parseSelfDescribingEntry parses a slash-delimited self-describing key and
// appends the appropriate typed entry to the MetadataRecord.
func parseSelfDescribingEntry(key, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		// Pass through as-is to SelfDescribingKv for unknown prefixes.
		record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
			Key:   key,
			Value: value,
		})
		return nil
	}

	switch {
	case parts[0] == "sketch":
		// sketch/{type}/{field} — value is base64-encoded raw filter bytes.
		// The type is in the key, so we reconstruct the msgpack snapshot.
		return parseSelfDescribingSketch(parts[1], value, record)

	case parts[0] == "dim":
		// dim/{typeSpec}/{field} — parse type spec and field name
		return parseSelfDescribingDim(parts[1], value, record)

	case parts[0] == "agg_int":
		// agg_int/{type}/{field}[/{qualifier}]
		return parseSelfDescribingAgg(parts[1], value, "INT", record)

	case parts[0] == "agg_float":
		// agg_float/{type}/{field}[/{qualifier}]
		return parseSelfDescribingAgg(parts[1], value, "FLOAT", record)

	default:
		record.SelfDescribingKv = append(record.SelfDescribingKv, &pb.SelfDescribingEntry{
			Key:   key,
			Value: value,
		})
	}
	return nil
}

// parseSelfDescribingSketch parses the remainder after "sketch/" and appends a SketchEntry.
// Format: {type}/{field} — value is base64-encoded raw filter bytes.
// The type and data are re-encoded as msgpack to produce the BloomFilterSnapshot
// format that the ingestion pipeline expects.
func parseSelfDescribingSketch(remainder, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in sketch key")
	}
	sketchType, field := parts[0], parts[1]

	rawData, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	// Re-encode as msgpack BloomFilterSnapshot: {type: "<type>", data: <bytes>}
	snapshot := struct {
		Type string `msgpack:"type"`
		Data []byte `msgpack:"data"`
	}{
		Type: sketchType,
		Data: rawData,
	}
	snapshotBytes, err := msgpack.Marshal(&snapshot)
	if err != nil {
		return fmt.Errorf("encode sketch snapshot: %w", err)
	}

	record.Sketch = append(record.Sketch, &pb.SketchEntry{
		SketchKey: field,
		Data:      snapshotBytes,
	})
	return nil
}

// parseSelfDescribingDim parses the remainder after "dim/" and appends a DimEntry.
// Format: {typeSpec}/{field}
//
//	str{N}/{field}       → str, width N
//	str{N}utf8/{field}   → str_utf8, width N
//	int/{field}          → int
//	float/{field}        → float
//	bool/{field}         → bool
func parseSelfDescribingDim(remainder, value string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in dim key")
	}
	typeSpec, field := parts[0], parts[1]

	var dimVal *pb.DimensionValue
	switch {
	case typeSpec == "int":
		var intVal int64
		if _, err := fmt.Sscanf(value, "%d", &intVal); err != nil {
			return fmt.Errorf("parse int dim value: %w", err)
		}
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_IntVal{IntVal: intVal}}
	case typeSpec == "float":
		var floatVal float64
		if _, err := fmt.Sscanf(value, "%g", &floatVal); err != nil {
			return fmt.Errorf("parse float dim value: %w", err)
		}
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_FloatVal{FloatVal: floatVal}}
	case typeSpec == "bool":
		boolVal := value == "true" || value == "1"
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_BoolVal{BoolVal: boolVal}}
	case strings.HasSuffix(typeSpec, "utf8"):
		width := parseWidthFromTypeSpec(strings.TrimSuffix(typeSpec, "utf8"))
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_StrUtf8{
			StrUtf8: &pb.StringDimension{Value: value, MaxLength: int32(width)},
		}}
	default:
		// str{N} — default string type
		width := parseWidthFromTypeSpec(typeSpec)
		dimVal = &pb.DimensionValue{Value: &pb.DimensionValue_Str{
			Str: &pb.StringDimension{Value: value, MaxLength: int32(width)},
		}}
	}

	record.Dim = append(record.Dim, &pb.DimEntry{Key: field, Value: dimVal})
	return nil
}

// parseWidthFromTypeSpec extracts the numeric width from a type spec like "str128".
// Strips the "str" prefix if present. Defaults to 64 if parsing fails.
func parseWidthFromTypeSpec(spec string) int {
	spec = strings.TrimPrefix(spec, "str")
	var width int
	if _, err := fmt.Sscanf(spec, "%d", &width); err != nil || width <= 0 {
		return 64
	}
	return width
}

// parseSelfDescribingAgg parses the remainder after "agg_int/" or "agg_float/"
// and appends an AggEntry.
// Format: {type}/{field}[/{qualifier}]
func parseSelfDescribingAgg(remainder, value, valueType string, record *pb.MetadataRecord) error {
	parts := strings.SplitN(remainder, "/", 3)
	if len(parts) < 2 || parts[1] == "" {
		return fmt.Errorf("missing field name in agg key")
	}
	aggTypeStr := strings.ToUpper(parts[0])
	field := parts[1]
	var qualifier string
	if len(parts) == 3 {
		qualifier = parts[2]
	}

	aggType := pb.AggType_GTE
	if v, ok := pb.AggType_value[aggTypeStr]; ok {
		aggType = pb.AggType(v)
	}

	entry := &pb.AggEntry{
		Field:     field,
		Qualifier: qualifier,
		AggType:   aggType,
	}

	if valueType == "FLOAT" {
		var floatVal float64
		if _, err := fmt.Sscanf(value, "%g", &floatVal); err != nil {
			return fmt.Errorf("parse float agg value: %w", err)
		}
		entry.Value = &pb.AggEntry_FloatVal{FloatVal: floatVal}
	} else {
		var intVal int64
		if _, err := fmt.Sscanf(value, "%d", &intVal); err != nil {
			return fmt.Errorf("parse int agg value: %w", err)
		}
		entry.Value = &pb.AggEntry_IntVal{IntVal: intVal}
	}

	record.Agg = append(record.Agg, entry)
	return nil
}

// ConsumerConfig holds configuration for creating a Kafka consumer.
type ConsumerConfig struct {
	BootstrapServers string
	GroupID          string
	Topic            string
	TableName        string
	ExtraConfig      map[string]string
}

// BuildConfigMap constructs a kafka.ConfigMap from ConsumerConfig.
func BuildConfigMap(cfg ConsumerConfig) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}
	for k, v := range cfg.ExtraConfig {
		cm.SetKey(k, v)
	}
	return cm
}

// ParseBootstrapServers splits a comma-separated bootstrap servers string.
func ParseBootstrapServers(servers string) []string {
	parts := strings.Split(servers, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// ValidateConfig checks that required fields are set.
func ValidateConfig(cfg ConsumerConfig) error {
	if cfg.BootstrapServers == "" {
		return fmt.Errorf("kafka: bootstrap.servers is required")
	}
	if cfg.Topic == "" {
		return fmt.Errorf("kafka: topic is required")
	}
	if cfg.GroupID == "" {
		return fmt.Errorf("kafka: group.id is required")
	}
	return nil
}
