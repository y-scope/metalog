package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

// Run starts consuming from the Kafka topic until ctx is cancelled.
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
		c.log.Error("failed to create kafka consumer", zap.Error(err))
		return
	}
	defer consumer.Close()

	if err := consumer.Subscribe(c.topic, nil); err != nil {
		c.log.Error("failed to subscribe", zap.Error(err))
		return
	}

	c.log.Info("kafka consumer started",
		zap.String("bootstrapServers", c.bootstrapServers),
		zap.String("groupId", c.groupID),
	)

	for {
		select {
		case <-ctx.Done():
			c.drainFlushes()
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
		c.handleEvent(ctx, ev, &processed)

		for processed < maxPollBatch {
			ev = consumer.Poll(0) // non-blocking: returns nil if no buffered messages
			if ev == nil {
				break
			}
			c.handleEvent(ctx, ev, &processed)
		}

		if processed > 0 {
			c.log.Debug("poll batch processed", zap.Int("messages", processed))
		}
	}
}

func (c *Consumer) handleEvent(ctx context.Context, ev kafka.Event, processed *int) {
	switch e := ev.(type) {
	case *kafka.Message:
		c.handleMessage(ctx, e)
		*processed++
	case kafka.Error:
		if e.IsFatal() {
			c.log.Error("fatal kafka error", zap.Error(e))
		} else {
			c.log.Warn("kafka error", zap.Error(e))
		}
	}
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

// drainFlushes non-blockingly checks all pending flush channels. Completed
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
			c.lastOffsets[pf.partition] = pf.offset
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

	return record, nil
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
