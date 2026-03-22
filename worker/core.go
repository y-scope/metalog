package worker

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/taskqueue"
	"github.com/y-scope/metalog/timeutil"
)

// TaskCompleter handles task lifecycle operations (complete/fail).
type TaskCompleter interface {
	CompleteTask(ctx context.Context, taskID int64, output []byte) (int64, error)
	FailTask(ctx context.Context, taskID int64) (int64, error)
}

// Archiver creates archives from IR files.
type Archiver interface {
	CreateArchive(ctx context.Context,
		irBackend string, irBuckets []string, irPaths []string,
		archiveBackend, archiveBucket, archivePath string,
	) (int64, error)

	// DeleteArchive removes an archive from object storage. Used for
	// orphan cleanup when archive creation fails after a partial upload.
	DeleteArchive(ctx context.Context, backend, bucket, path string) error
}

// Core is the worker task execution loop.
type Core struct {
	taskQueue      TaskCompleter
	archiveCreator Archiver
	prefetcher     *Prefetcher
	log            *zap.Logger

	mTaskDuration  metric.Float64Histogram
	mTaskCompleted metric.Int64Counter
}

// NewCore creates a worker Core.
func NewCore(
	taskQueue TaskCompleter,
	archiveCreator Archiver,
	prefetcher *Prefetcher,
	log *zap.Logger,
) *Core {
	c := &Core{
		taskQueue:      taskQueue,
		archiveCreator: archiveCreator,
		prefetcher:     prefetcher,
		log:            log,
	}
	c.initMetrics(noop.Meter{})
	return c
}

// SetMeter configures OpenTelemetry metrics. Must be called before Run.
func (c *Core) SetMeter(m metric.Meter) { c.initMetrics(m) }

func (c *Core) initMetrics(m metric.Meter) {
	c.mTaskDuration, _ = m.Float64Histogram("metalog.worker.task_duration_seconds",
		metric.WithDescription("Time to execute a single task"), metric.WithUnit("s"))
	c.mTaskCompleted, _ = m.Int64Counter("metalog.worker.tasks_completed",
		metric.WithDescription("Tasks completed"), metric.WithUnit("{task}"))
}

// Run processes tasks from the prefetcher until ctx is canceled.
func (c *Core) Run(ctx context.Context) {
	for task := range c.prefetcher.Tasks() {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		c.executeTask(ctx, task)
		c.mTaskDuration.Record(ctx, time.Since(start).Seconds())
	}
}

func (c *Core) executeTask(ctx context.Context, task *taskqueue.Task) {
	log := c.log.With(zap.Int64("taskId", task.TaskID))

	if task.Version != taskqueue.TaskPayloadVersion {
		log.Error("unsupported task version",
			zap.Uint8("version", task.Version),
			zap.Uint8("expected", taskqueue.TaskPayloadVersion),
		)
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after version mismatch", zap.Error(fErr))
		}
		return
	}

	payload, err := taskqueue.UnmarshalPayload(task.Input)
	if err != nil {
		log.Error("unmarshal payload failed", zap.Error(err))
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after unmarshal error", zap.Error(fErr))
		}
		return
	}

	cons := payload.Consolidation
	if cons == nil {
		log.Error("payload missing consolidation data")
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after invalid payload", zap.Error(fErr))
		}
		return
	}

	if len(cons.IRBuckets) == 0 || len(cons.IRPaths) == 0 {
		log.Error("invalid payload: missing IR buckets or paths",
			zap.Int("irBuckets", len(cons.IRBuckets)),
			zap.Int("irPaths", len(cons.IRPaths)),
		)
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after invalid payload", zap.Error(fErr))
		}
		return
	}

	// Create archive
	sizeBytes, err := c.archiveCreator.CreateArchive(ctx,
		cons.IRBackend, cons.IRBuckets, cons.IRPaths,
		cons.ArchiveBackend, cons.ArchiveBucket, cons.ArchivePath,
	)
	if err != nil {
		log.Error("archive creation failed", zap.Error(err))

		// Clean up any partially uploaded archive (idempotent — no-op if not uploaded).
		// Use a fresh context: the worker context may already be cancelled if
		// CreateArchive failed due to shutdown, but we still want to delete.
		delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if delErr := c.archiveCreator.DeleteArchive(delCtx, cons.ArchiveBackend, cons.ArchiveBucket, cons.ArchivePath); delErr != nil {
			log.Warn("orphan archive cleanup failed", zap.Error(delErr))
		}
		delCancel()

		result := &taskqueue.TaskResult{Error: err.Error()}
		output, marshalErr := taskqueue.MarshalResult(result)
		if marshalErr != nil {
			log.Warn("marshal error result failed", zap.Error(marshalErr))
		}
		if _, cErr := c.taskQueue.CompleteTask(ctx, task.TaskID, output); cErr != nil {
			log.Warn("complete task with error result failed — task will be reclaimed", zap.Error(cErr))
		}
		return
	}

	// Complete task with result
	result := &taskqueue.TaskResult{
		ArchivePath:      cons.ArchivePath,
		ArchiveSizeBytes: sizeBytes,
		CreatedAt:        timeutil.EpochNanos(),
	}
	output, err := taskqueue.MarshalResult(result)
	if err != nil {
		log.Error("marshal result failed", zap.Error(err))
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after marshal error", zap.Error(fErr))
		}
		return
	}

	if _, cErr := c.taskQueue.CompleteTask(ctx, task.TaskID, output); cErr != nil {
		log.Error("complete task failed", zap.Error(cErr))
		return
	}
	c.mTaskCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
	log.Debug("task completed", zap.String("archivePath", cons.ArchivePath), zap.Int64("sizeBytes", sizeBytes))
}
