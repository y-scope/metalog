package consolidation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/storage"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/timeutil"
)

// maxBackpressureDepth is the maximum number of pending+processing tasks
// before the planner skips creating new consolidation tasks.
const maxBackpressureDepth = 100

// Planner runs the consolidation planning loop for a single table.
type Planner struct {
	db              *sql.DB
	tableName       string
	policy          Policy
	inFlight        *InFlightSet
	taskQueue       *taskqueue.Queue
	fileRecs        *metastore.FileRecords
	storageRegistry *storage.Registry
	interval        time.Duration
	log             *zap.Logger
}

// NewPlanner creates a Planner.
func NewPlanner(
	db *sql.DB,
	tableName string,
	policy Policy,
	inFlight *InFlightSet,
	taskQueue *taskqueue.Queue,
	storageRegistry *storage.Registry,
	interval time.Duration,
	log *zap.Logger,
) (*Planner, error) {
	fr, err := metastore.NewFileRecords(db, tableName, log)
	if err != nil {
		return nil, fmt.Errorf("new planner: %w", err)
	}
	return &Planner{
		db:              db,
		tableName:       tableName,
		policy:          policy,
		inFlight:        inFlight,
		taskQueue:       taskQueue,
		fileRecs:        fr,
		storageRegistry: storageRegistry,
		interval:        interval,
		log:             log.With(zap.String("table", tableName)),
	}, nil
}

// Run executes the planning loop until ctx is cancelled.
func (p *Planner) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.log.Info("planner stopped")
			return
		case <-ticker.C:
			if err := p.planOnce(ctx); err != nil {
				// Suppress errors caused by context cancellation during shutdown.
				if ctx.Err() != nil {
					p.log.Info("planner stopped")
					return
				}
				p.log.Warn("planning cycle failed", zap.Error(err))
			}
		}
	}
}

func (p *Planner) planOnce(ctx context.Context) error {
	// 1. Process completed tasks (marks ARCHIVE_CLOSED, deletes source IR files)
	if err := p.processCompletedTasks(ctx); err != nil {
		return fmt.Errorf("process completed: %w", err)
	}

	// 2. Reclaim stale tasks
	if err := p.reclaimStaleTasks(ctx); err != nil {
		return fmt.Errorf("reclaim stale: %w", err)
	}

	// 3. Clean up old completed/failed tasks
	if err := p.cleanupOldTasks(ctx); err != nil {
		p.log.Warn("cleanup old tasks failed", zap.Error(err))
	}

	// 4. Backpressure check — skip creating new tasks if queue is deep
	counts, err := p.taskQueue.GetTaskCounts(ctx, p.tableName)
	if err != nil {
		return fmt.Errorf("backpressure check: %w", err)
	}
	activeDepth := counts.Pending + counts.Processing
	if activeDepth >= maxBackpressureDepth {
		p.log.Debug("backpressure: skipping task creation",
			zap.Int("pending", counts.Pending),
			zap.Int("processing", counts.Processing),
		)
		return nil
	}

	// 5. Find consolidation-pending files
	candidates, err := p.fileRecs.FindConsolidationPending(ctx)
	if err != nil {
		return fmt.Errorf("find pending: %w", err)
	}

	if len(candidates) == 0 {
		return nil
	}

	// 6. Apply policy to group files
	groups := p.policy.SelectFiles(candidates)

	// 7. Create tasks for each group
	for _, group := range groups {
		irPaths := make([]string, 0, len(group))
		for _, rec := range group {
			if rec.ClpIRPath.Valid {
				irPaths = append(irPaths, rec.ClpIRPath.String)
			}
		}

		if !p.inFlight.TryAdd(irPaths) {
			continue
		}

		payload := &taskqueue.TaskPayload{
			TableName: p.tableName,
			IRPaths:   irPaths,
		}
		for _, rec := range group {
			payload.FileIDs = append(payload.FileIDs, rec.ID)
			if payload.MinTimestamp == 0 || rec.MinTimestamp < payload.MinTimestamp {
				payload.MinTimestamp = rec.MinTimestamp
			}
		}
		if len(group) > 0 && group[0].ClpIRStorageBackend.Valid {
			payload.IRBackend = group[0].ClpIRStorageBackend.String
		}
		if len(group) > 0 && group[0].ClpIRBucket.Valid {
			payload.IRBuckets = make([]string, len(group))
			for i, rec := range group {
				if rec.ClpIRBucket.Valid {
					payload.IRBuckets[i] = rec.ClpIRBucket.String
				}
			}
		}

		input, err := taskqueue.MarshalPayload(payload)
		if err != nil {
			p.inFlight.Remove(irPaths)
			p.log.Error("marshal payload failed", zap.Error(err))
			continue
		}

		taskID, err := p.taskQueue.CreateTask(ctx, p.tableName, input)
		if err != nil {
			p.inFlight.Remove(irPaths)
			return fmt.Errorf("create task: %w", err)
		}

		p.log.Info("created consolidation task",
			zap.Int64("taskId", taskID),
			zap.Int("files", len(group)),
		)
	}

	return nil
}

func (p *Planner) processCompletedTasks(ctx context.Context) error {
	// Single query fetches both input and output — avoids N+1.
	rows, err := p.db.QueryContext(ctx,
		"SELECT task_id, input, output FROM "+taskqueue.TableName+
			" WHERE table_name = ? AND state = 'completed' AND output IS NOT NULL LIMIT 100",
		p.tableName,
	)
	if err != nil {
		return fmt.Errorf("process completed: query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var taskID int64
		var input, output []byte
		if err := rows.Scan(&taskID, &input, &output); err != nil {
			return fmt.Errorf("process completed: scan: %w", err)
		}

		result, err := taskqueue.UnmarshalResult(output)
		if err != nil {
			p.log.Error("unmarshal task result failed", zap.Int64("taskId", taskID), zap.Error(err))
			continue
		}

		if result.Error != "" {
			p.log.Warn("task completed with error", zap.Int64("taskId", taskID), zap.String("error", result.Error))
			continue
		}

		payload, err := taskqueue.UnmarshalPayload(input)
		if err != nil {
			p.log.Error("unmarshal task payload failed", zap.Int64("taskId", taskID), zap.Error(err))
			continue
		}

		// Mark files as ARCHIVE_CLOSED
		err = p.fileRecs.MarkArchiveClosed(ctx,
			payload.IRPaths,
			result.ArchivePath,
			payload.ArchiveBackend,
			payload.ArchiveBucket,
			result.ArchiveSizeBytes,
			timeutil.EpochNanos(),
		)
		if err != nil {
			p.log.Error("mark archive closed failed", zap.Int64("taskId", taskID), zap.Error(err))
		}

		// Delete source IR files from storage (best-effort)
		p.deleteIRFiles(ctx, payload)

		// Remove from in-flight set
		p.inFlight.Remove(payload.IRPaths)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("process completed: rows: %w", err)
	}
	return nil
}

// deleteIRFiles removes source IR files from storage after successful archiving.
func (p *Planner) deleteIRFiles(ctx context.Context, payload *taskqueue.TaskPayload) {
	if p.storageRegistry == nil || payload.IRBackend == "" {
		return
	}
	backend, err := p.storageRegistry.Get(payload.IRBackend)
	if err != nil {
		p.log.Warn("IR deletion: unknown backend", zap.String("backend", payload.IRBackend))
		return
	}
	for i, irPath := range payload.IRPaths {
		bucket := ""
		if i < len(payload.IRBuckets) {
			bucket = payload.IRBuckets[i]
		}
		if bucket == "" || irPath == "" {
			continue
		}
		if err := backend.Delete(ctx, bucket, irPath); err != nil {
			p.log.Warn("failed to delete IR file",
				zap.String("path", irPath), zap.String("bucket", bucket), zap.Error(err))
		}
	}
}

func (p *Planner) cleanupOldTasks(ctx context.Context) error {
	n, err := p.taskQueue.CleanupOldTasks(ctx, p.tableName, config.DefaultTaskCleanupAge)
	if err != nil {
		return err
	}
	if n > 0 {
		p.log.Info("cleaned up old tasks", zap.Int64("deleted", n))
	}
	return nil
}

func (p *Planner) reclaimStaleTasks(ctx context.Context) error {
	staleTimeoutSec := int(config.DefaultTaskStaleTimeout.Seconds())
	staleTasks, err := p.taskQueue.FindStaleTasks(ctx, p.tableName, staleTimeoutSec)
	if err != nil {
		return fmt.Errorf("reclaim stale tasks: %w", err)
	}

	for _, task := range staleTasks {
		if err := p.taskQueue.ReclaimTask(ctx, task.TaskID, task.RetryCount); err != nil {
			p.log.Error("reclaim task failed", zap.Int64("taskId", task.TaskID), zap.Error(err))
		}
	}
	return nil
}
