package kafka

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/logutil"
)

// pollTimeoutMs is the timeout for the initial Poll call when no messages are buffered.
const pollTimeoutMs = 100

// maxPollBatch is the maximum number of messages to drain per poll cycle.
const maxPollBatch = 1000

// MessageSource consumes messages from a Kafka topic and submits them
// to the ingestion pipeline. Implementations handle offset management.
type MessageSource interface {
	Run(ctx context.Context)
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
	dataFL           *logutil.FailureLogger // throttles transform/convert failures

	// pendingFlushes tracks records submitted to the BatchingWriter but not
	// yet confirmed as flushed. Drained each poll cycle — no goroutines needed.
	pendingFlushes []pendingFlush

	pendingCommit []kafka.TopicPartition

	// Metrics (nil when telemetry is disabled).
	mConsumed      metric.Int64Counter
	mFailed        metric.Int64Counter
	mPollBatchSize metric.Int64Histogram
}

// Compile-time check that Consumer implements MessageSource.
var _ MessageSource = (*Consumer)(nil)

// NewConsumer creates a Kafka metadata consumer.
func NewConsumer(
	bootstrapServers, groupID, topic, tableName string,
	transformer MessageTransformer,
	service *ingestion.Service,
	log *zap.Logger,
) *Consumer {
	consumerLog := log.With(zap.String("topic", topic), zap.String("table", tableName))
	return &Consumer{
		bootstrapServers: bootstrapServers,
		groupID:          groupID,
		topic:            topic,
		tableName:        tableName,
		transformer:      transformer,
		service:          service,
		log:              consumerLog,
		dataFL:           logutil.NewFailureLogger(consumerLog, time.Minute),
	}
}

// SetMeter configures OpenTelemetry metrics for this consumer.
func (c *Consumer) SetMeter(m metric.Meter) {
	c.mConsumed, _ = m.Int64Counter("metalog.kafka.messages_consumed",
		metric.WithDescription("Kafka messages successfully consumed and submitted"),
		metric.WithUnit("{message}"))
	c.mFailed, _ = m.Int64Counter("metalog.kafka.messages_failed",
		metric.WithDescription("Kafka messages that failed transform, convert, or ingest"),
		metric.WithUnit("{message}"))
	c.mPollBatchSize, _ = m.Int64Histogram("metalog.kafka.poll_batch_size",
		metric.WithDescription("Number of messages processed per poll cycle"),
		metric.WithUnit("{message}"))
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
			if c.mPollBatchSize != nil {
				c.mPollBatchSize.Record(ctx, int64(processed),
					metric.WithAttributes(attribute.String("topic", c.topic)))
			}
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
// Uses the blocking SubmitWait path so no messages are dropped on channel full —
// the poll loop blocks until space opens, propagating backpressure to Kafka.
func (c *Consumer) handleMessage(ctx context.Context, msg *kafka.Message) {
	topicAttr := attribute.String("topic", c.topic)

	record, err := c.transformer.Transform(msg.Value)
	if err != nil {
		c.dataFL.Fail("transform failed", zap.Error(err))
		if c.mFailed != nil {
			c.mFailed.Add(ctx, 1, metric.WithAttributes(topicAttr, attribute.String("reason", "transform")))
		}
		return
	}

	rec, err := ingestion.ConvertRecord(record)
	if err != nil {
		c.dataFL.Fail("convert failed", zap.Error(err))
		if c.mFailed != nil {
			c.mFailed.Add(ctx, 1, metric.WithAttributes(topicAttr, attribute.String("reason", "convert")))
		}
		return
	}

	flushed := make(chan error, 1)

	if err := c.service.IngestWithCallbackWait(ctx, c.tableName, rec, flushed); err != nil {
		c.log.Warn("ingest failed",
			zap.Int32("partition", msg.TopicPartition.Partition),
			zap.Any("offset", msg.TopicPartition.Offset),
			zap.Error(err),
		)
		if c.mFailed != nil {
			c.mFailed.Add(ctx, 1, metric.WithAttributes(topicAttr, attribute.String("reason", "ingest")))
		}
		return
	}

	if c.mConsumed != nil {
		c.mConsumed.Add(ctx, 1, metric.WithAttributes(topicAttr))
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
	deadlineTimer := time.NewTimer(5 * time.Second)
	defer deadlineTimer.Stop()
	deadline := deadlineTimer.C
	for i, pf := range c.pendingFlushes {
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
				zap.Int("remaining", len(c.pendingFlushes)-i))
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

