package franzgo

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/logutil"
)

// pendingFlush tracks a submitted record awaiting DB flush confirmation.
type pendingFlush struct {
	flushed   chan error
	topic     string
	partition int32
	offset    int64
}

// Consumer reads metadata records from a Kafka topic using franz-go (pure Go)
// and submits them through the IngestionService.
type Consumer struct {
	bootstrapServers string
	groupID          string
	topic            string
	tableName        string
	transformer      kafka.MessageTransformer
	service          *ingestion.Service
	log              *zap.Logger
	dataFL           *logutil.FailureLogger
	dlq              kafka.DeadLetterHandler

	pendingFlushes []pendingFlush
	// pendingCommit tracks the highest flushed offset per partition for commit.
	pendingCommit map[topicPartition]int64

	mConsumed      metric.Int64Counter
	mFailed        metric.Int64Counter
	mPollBatchSize metric.Int64Histogram
}

type topicPartition struct {
	topic     string
	partition int32
}

// Compile-time check that Consumer implements MessageSource.
var _ kafka.MessageSource = (*Consumer)(nil)

// NewConsumer creates a franz-go based Kafka metadata consumer.
func NewConsumer(
	bootstrapServers, groupID, topic, tableName string,
	transformer kafka.MessageTransformer,
	service *ingestion.Service,
	log *zap.Logger,
) *Consumer {
	consumerLog := log.With(zap.String("topic", topic), zap.String("table", tableName))
	c := &Consumer{
		bootstrapServers: bootstrapServers,
		groupID:          groupID,
		topic:            topic,
		tableName:        tableName,
		transformer:      transformer,
		service:          service,
		log:              consumerLog,
		dataFL:           logutil.NewFailureLogger(consumerLog, time.Minute),
		pendingCommit:    make(map[topicPartition]int64),
	}
	c.SetMeter(noop.Meter{})
	return c
}

// SetDeadLetterHandler sets a handler for messages that fail transform or
// convert. Must be called before Run — not safe for concurrent use.
func (c *Consumer) SetDeadLetterHandler(h kafka.DeadLetterHandler) {
	c.dlq = h
}

// SetMeter configures OpenTelemetry metrics for this consumer.
// Must be called before Run — not safe for concurrent use.
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
func (c *Consumer) Run(ctx context.Context) {
	seeds := strings.Split(c.bootstrapServers, ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(c.groupID),
		kgo.ConsumeTopics(c.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(1<<20), // 1MB per fetch
	)
	if err != nil {
		c.log.Error("failed to create kafka client",
			zap.String("bootstrapServers", c.bootstrapServers),
			zap.Error(err))
		return
	}
	defer client.Close()

	c.log.Info("kafka consumer started",
		zap.String("bootstrapServers", c.bootstrapServers),
		zap.String("groupId", c.groupID),
	)

	for {
		select {
		case <-ctx.Done():
			c.drainFlushesBlocking()
			c.commitPendingSync(client)
			c.log.Info("kafka consumer stopped")
			return
		default:
		}

		c.drainFlushes()
		c.commitPending(ctx, client)

		// PollRecords returns at most DefaultBatchSize records per poll,
		// matching the DB UPSERT batch size. Remaining records stay in
		// franz-go's internal buffer for the next poll cycle.
		fetches := client.PollRecords(ctx, config.DefaultBatchSize)
		if fetches.IsClientClosed() {
			return
		}

		var fatalErr bool
		fetches.EachError(func(_ string, _ int32, err error) {
			c.log.Error("kafka fetch error", zap.Error(err))
			fatalErr = true
		})
		if fatalErr {
			c.drainFlushes()
			c.commitPending(ctx, client)
			return
		}

		processed := 0
		fetches.EachRecord(func(record *kgo.Record) {
			c.handleRecord(ctx, record)
			processed++
		})

		if processed > 0 {
			c.log.Debug("poll batch processed", zap.Int("messages", processed))
			c.mPollBatchSize.Record(ctx, int64(processed),
				metric.WithAttributes(attribute.String("topic", c.topic)))
		}
	}
}

// handleRecord transforms and ingests a single Kafka record.
func (c *Consumer) handleRecord(ctx context.Context, rec *kgo.Record) {
	topicAttr := attribute.String("topic", c.topic)

	converted, err := c.transformer.Transform(rec.Value)
	if err != nil {
		c.dataFL.Fail("transform failed", zap.Error(err))
		c.mFailed.Add(ctx, 1, metric.WithAttributes(topicAttr, attribute.String("reason", "transform")))
		if c.dlq != nil {
			c.dlq.Handle(ctx, c.topic, rec.Partition, rec.Offset, rec.Value, "transform", err)
		}
		return
	}

	flushed := make(chan error, 1)

	if err := c.service.IngestWithCallbackWait(ctx, c.tableName, converted, flushed); err != nil {
		c.log.Warn("ingest failed",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err),
		)
		c.mFailed.Add(ctx, 1, metric.WithAttributes(topicAttr, attribute.String("reason", "ingest")))
		return
	}

	c.mConsumed.Add(ctx, 1, metric.WithAttributes(topicAttr))

	c.pendingFlushes = append(c.pendingFlushes, pendingFlush{
		flushed:   flushed,
		topic:     rec.Topic,
		partition: rec.Partition,
		offset:    rec.Offset,
	})
}

// drainFlushesBlocking waits for all pending flushes to complete (with a timeout)
// before shutdown. Failed flushes are skipped (offset not committed), which means
// the next consumer startup will re-deliver those records. This is safe because
// the DB uses idempotent UPSERTs — re-processing a record is a no-op.
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
					zap.Int64("offset", pf.offset),
					zap.Error(err),
				)
				continue
			}
			tp := topicPartition{topic: pf.topic, partition: pf.partition}
			if existing, ok := c.pendingCommit[tp]; !ok || pf.offset+1 > existing {
				c.pendingCommit[tp] = pf.offset + 1
			}
		case <-deadline:
			c.log.Error("shutdown drain timeout, uncommitted flushes remain",
				zap.Int("remaining", len(c.pendingFlushes)-i))
			c.pendingFlushes = nil
			return
		}
	}
	c.pendingFlushes = nil
}

// drainFlushes checks all pending flush channels without blocking.
func (c *Consumer) drainFlushes() {
	remaining := c.pendingFlushes[:0]
	for _, pf := range c.pendingFlushes {
		select {
		case err := <-pf.flushed:
			if err != nil {
				c.log.Warn("flush failed, skipping offset commit",
					zap.Int32("partition", pf.partition),
					zap.Int64("offset", pf.offset),
					zap.Error(err),
				)
				continue
			}
			tp := topicPartition{topic: pf.topic, partition: pf.partition}
			if existing, ok := c.pendingCommit[tp]; !ok || pf.offset+1 > existing {
				c.pendingCommit[tp] = pf.offset + 1
			}
		default:
			remaining = append(remaining, pf)
		}
	}
	c.pendingFlushes = remaining
}

// commitPending fires an async offset commit to Kafka and clears the
// pending map immediately.
//
// Why async (CommitOffsets) instead of sync (CommitOffsetsSync):
//   - This runs in the hot poll loop. A sync commit would block the loop
//     on a broker round-trip (1–10ms) every cycle, reducing throughput.
//   - If the async commit fails (broker error, rebalance), the offsets are
//     lost from pendingCommit but NOT from the broker — they were simply
//     not advanced. On next poll, the consumer re-processes those messages
//     and re-submits them. The UPSERT is idempotent, so re-delivery is safe.
//   - Shutdown uses commitPendingSync (below) which blocks until the broker
//     acknowledges, ensuring offsets are persisted before the client closes.
//
// Do NOT change this to CommitOffsetsSync without measuring throughput impact.
func (c *Consumer) commitPending(ctx context.Context, client *kgo.Client) {
	if len(c.pendingCommit) == 0 {
		return
	}

	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for tp, offset := range c.pendingCommit {
		if offsets[tp.topic] == nil {
			offsets[tp.topic] = make(map[int32]kgo.EpochOffset)
		}
		offsets[tp.topic][tp.partition] = kgo.EpochOffset{
			Epoch:  -1, // unknown epoch
			Offset: offset,
		}
	}

	// Clear before commit — safe because at-least-once delivery is guaranteed
	// by re-processing on the next poll if the commit fails. See comment above.
	for k := range c.pendingCommit {
		delete(c.pendingCommit, k)
	}

	client.CommitOffsets(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			c.log.Warn("async offset commit failed (will re-process on next poll)", zap.Error(err))
		}
	})
}

// commitPendingSync commits pending offsets synchronously. Used only during
// shutdown to ensure offsets reach the broker before client.Close().
// Unlike commitPending (async, fire-and-forget), this blocks until the broker
// acknowledges. The map is cleared before the call because shutdown is
// fire-and-forget — there is no next cycle to retry on failure.
func (c *Consumer) commitPendingSync(client *kgo.Client) {
	if len(c.pendingCommit) == 0 {
		return
	}

	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for tp, offset := range c.pendingCommit {
		if offsets[tp.topic] == nil {
			offsets[tp.topic] = make(map[int32]kgo.EpochOffset)
		}
		offsets[tp.topic][tp.partition] = kgo.EpochOffset{
			Epoch:  -1,
			Offset: offset,
		}
	}
	for k := range c.pendingCommit {
		delete(c.pendingCommit, k)
	}

	// CommitOffsetsSync cancels any active async commits, begins a commit
	// that cannot be canceled, and blocks until complete. Use a fresh
	// context since the parent ctx is already cancelled.
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client.CommitOffsetsSync(commitCtx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			c.log.Warn("shutdown offset commit failed", zap.Error(err))
		}
	})
}
