package worker

import (
	"context"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/timeutil"
)

// TaskCompleter handles task lifecycle operations (complete/fail).
type TaskCompleter interface {
	CompleteTask(ctx context.Context, taskID int64, output []byte) (int64, error)
	FailTask(ctx context.Context, taskID int64) (int64, error)
}

// Archiver creates archives from IR files.
type Archiver interface {
	CreateArchive(ctx context.Context,
		irBackend, irBucket string, irPaths []string,
		archiveBackend, archiveBucket, archivePath string,
	) (int64, error)
}

// Core is the worker task execution loop.
type Core struct {
	taskQueue      TaskCompleter
	archiveCreator Archiver
	prefetcher     *Prefetcher
	log            *zap.Logger
}

// NewCore creates a worker Core.
func NewCore(
	taskQueue TaskCompleter,
	archiveCreator Archiver,
	prefetcher *Prefetcher,
	log *zap.Logger,
) *Core {
	return &Core{
		taskQueue:      taskQueue,
		archiveCreator: archiveCreator,
		prefetcher:     prefetcher,
		log:            log,
	}
}

// Run processes tasks from the prefetcher until ctx is cancelled.
func (c *Core) Run(ctx context.Context) {
	for task := range c.prefetcher.Tasks() {
		if ctx.Err() != nil {
			return
		}
		c.executeTask(ctx, task)
	}
}

func (c *Core) executeTask(ctx context.Context, task *taskqueue.Task) {
	log := c.log.With(zap.Int64("taskId", task.TaskID))

	payload, err := taskqueue.UnmarshalPayload(task.Input)
	if err != nil {
		log.Error("unmarshal payload failed", zap.Error(err))
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after unmarshal error", zap.Error(fErr))
		}
		return
	}

	if len(payload.IRBuckets) == 0 || len(payload.IRPaths) == 0 {
		log.Error("invalid payload: missing IR buckets or paths")
		if _, fErr := c.taskQueue.FailTask(ctx, task.TaskID); fErr != nil {
			log.Error("fail task after invalid payload", zap.Error(fErr))
		}
		return
	}

	// Create archive
	sizeBytes, err := c.archiveCreator.CreateArchive(ctx,
		payload.IRBackend, payload.IRBuckets[0], payload.IRPaths,
		payload.ArchiveBackend, payload.ArchiveBucket, payload.ArchivePath,
	)
	if err != nil {
		log.Error("archive creation failed", zap.Error(err))
		result := &taskqueue.TaskResult{Error: err.Error()}
		output, _ := taskqueue.MarshalResult(result)
		if _, cErr := c.taskQueue.CompleteTask(ctx, task.TaskID, output); cErr != nil {
			log.Error("complete task with error result failed", zap.Error(cErr))
		}
		return
	}

	// Complete task with result
	result := &taskqueue.TaskResult{
		ArchivePath:      payload.ArchivePath,
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
	log.Info("task completed", zap.String("archivePath", payload.ArchivePath), zap.Int64("sizeBytes", sizeBytes))
}
