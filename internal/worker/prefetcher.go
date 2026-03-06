package worker

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/taskqueue"
)

// Prefetcher batch-claims tasks from the database and feeds them into a channel.
type Prefetcher struct {
	taskQueue *taskqueue.Queue
	workerID  string
	batchSize int
	tasks     chan *taskqueue.Task
	done      chan struct{} // closed when Run() returns
	log       *zap.Logger
}

// NewPrefetcher creates a Prefetcher.
func NewPrefetcher(tq *taskqueue.Queue, workerID string, batchSize int, log *zap.Logger) *Prefetcher {
	return &Prefetcher{
		taskQueue: tq,
		workerID:  workerID,
		batchSize: batchSize,
		tasks:     make(chan *taskqueue.Task, batchSize*2),
		done:      make(chan struct{}),
		log:       log,
	}
}

// Tasks returns the channel from which workers consume tasks.
func (pf *Prefetcher) Tasks() <-chan *taskqueue.Task {
	return pf.tasks
}

// Done returns a channel that is closed when the prefetcher's Run loop exits.
// Workers can select on this to detect when no more tasks will arrive.
func (pf *Prefetcher) Done() <-chan struct{} {
	return pf.done
}

// Run polls for tasks until ctx is cancelled, then closes the task channel.
func (pf *Prefetcher) Run(ctx context.Context) {
	defer close(pf.done)
	defer close(pf.tasks)

	backoff := config.DefaultWorkerPollInterval

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		claimed, err := pf.taskQueue.ClaimTasks(ctx, "", pf.workerID, pf.batchSize)
		if err != nil {
			pf.log.Error("claim tasks failed", zap.Error(err))
			sleep(ctx, backoff)
			continue
		}

		if len(claimed) == 0 {
			sleep(ctx, backoff)
			if backoff < config.DefaultWorkerBackoffMax {
				backoff = backoff * 2
				if backoff > config.DefaultWorkerBackoffMax {
					backoff = config.DefaultWorkerBackoffMax
				}
			}
			continue
		}

		backoff = config.DefaultWorkerPollInterval

		for _, task := range claimed {
			select {
			case pf.tasks <- task:
			case <-ctx.Done():
				return
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
