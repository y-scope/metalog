package node

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/worker"
)

// drainTimeout is how long workers have to finish their current task after
// the prefetcher stops claiming new ones.
const drainTimeout = 30 * time.Second

// WorkerUnit manages a pool of worker goroutines sharing a single TaskPrefetcher.
type WorkerUnit struct {
	numWorkers int
	nodeID     string
	shared     *SharedResources
	prefetcher *worker.Prefetcher
	log        *zap.Logger

	// prefetchCtx/prefetchCancel control the prefetcher only.
	// Workers use workerCtx which is canceled after drain timeout.
	prefetchCtx    context.Context
	prefetchCancel context.CancelFunc
	workerCtx      context.Context
	workerCancel   context.CancelFunc
	wg             sync.WaitGroup
}

// NewWorkerUnit creates a worker unit derived from the given parent context.
func NewWorkerUnit(parent context.Context, numWorkers int, nodeID string, shared *SharedResources, log *zap.Logger) *WorkerUnit {
	prefetchCtx, prefetchCancel := context.WithCancel(parent)
	workerCtx, workerCancel := context.WithCancel(parent)
	return &WorkerUnit{
		numWorkers:     numWorkers,
		nodeID:         nodeID,
		shared:         shared,
		log:            log.With(zap.String("unit", "worker")),
		prefetchCtx:    prefetchCtx,
		prefetchCancel: prefetchCancel,
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
	}
}

// Start begins the prefetcher and worker goroutines.
func (u *WorkerUnit) Start() {
	u.log.Info("starting worker unit", zap.Int("workers", u.numWorkers))

	db := u.shared.EffectiveWorkerDB()
	tq := taskqueue.NewQueue(db, u.log)

	u.prefetcher = worker.NewPrefetcher(tq, u.nodeID, config.DefaultTaskClaimBatchSize, u.log)

	// Start prefetcher (uses prefetchCtx — stops claiming on Stop)
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.prefetcher.Run(u.prefetchCtx)
	}()

	// Start worker goroutines (use workerCtx — get drain timeout)
	for i := 0; i < u.numWorkers; i++ {
		u.wg.Add(1)
		go func(id int) {
			defer u.wg.Done()
			core := worker.NewCore(
				tq, u.shared.ArchiveCreator, u.prefetcher,
				u.log.With(zap.Int("workerId", id)),
			)
			core.Run(u.workerCtx)
		}(i)
	}

	u.log.Info("worker unit started")
}

// Stop performs a two-phase shutdown:
//  1. Stop the prefetcher (no more DB claims). Workers drain remaining
//     tasks from the channel and finish their current task.
//  2. After drainTimeout, cancel the worker context to force-stop any
//     long-running task.
func (u *WorkerUnit) Stop() {
	u.log.Info("stopping worker unit: draining in-flight tasks",
		zap.Duration("drainTimeout", drainTimeout))

	// Phase 1: stop prefetcher. This closes the task channel, so workers
	// will exit their range loop once the channel is drained.
	u.prefetchCancel()

	// Wait for workers to finish (with timeout).
	done := make(chan struct{})
	go func() {
		u.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		u.log.Info("worker unit stopped (drained cleanly)")
	case <-time.After(drainTimeout):
		u.log.Warn("drain timeout exceeded, force-stopping workers")
		u.workerCancel()
		u.wg.Wait()
		u.log.Info("worker unit stopped (forced)")
	}
}
