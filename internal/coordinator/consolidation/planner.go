package consolidation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/logutil"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/timeutil"
	"github.com/y-scope/metalog/storage"
)

// maxBackpressureDepth is the maximum number of pending+processing tasks
// before the planner skips creating new consolidation tasks.
const maxBackpressureDepth = 100

// ColumnResolver resolves logical dimension/aggregation keys to physical column names.
// Satisfied by schema.ColumnRegistry.
type ColumnResolver interface {
	ResolveDim(dimKey string) string
	ResolveAgg(aggKey, aggValue, aggType string) string
}

// Planner runs the consolidation planning loop for a single table.
type Planner struct {
	db              *sql.DB
	tableName       string
	policy          Policy
	inFlight        *InFlightSet
	taskQueue       *taskqueue.Queue
	fileRecs        *metastore.FileRecords
	storageRegistry *storage.Registry
	archiveBackend  string
	archiveBucket   string
	interval           time.Duration
	failureLogInterval time.Duration
	staleThreshold     time.Duration // 0 disables stuck-file promotion
	resolver           ColumnResolver
	log                *zap.Logger
}

// NewPlanner creates a Planner. Column resolution happens per-cycle in planOnce
// so that newly-registered columns are picked up without restarting the planner.
func NewPlanner(
	db *sql.DB,
	tableName string,
	isMariaDB bool,
	policy Policy,
	inFlight *InFlightSet,
	taskQueue *taskqueue.Queue,
	resolver ColumnResolver,
	storageRegistry *storage.Registry,
	archiveBackend string,
	archiveBucket string,
	interval time.Duration,
	failureLogInterval time.Duration,
	staleThreshold time.Duration,
	log *zap.Logger,
) (*Planner, error) {
	fr, err := metastore.NewFileRecords(db, tableName, isMariaDB, log)
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
		archiveBackend:  archiveBackend,
		archiveBucket:   archiveBucket,
		interval:           interval,
		failureLogInterval: failureLogInterval,
		staleThreshold:     staleThreshold,
		resolver:           resolver,
		log:                log.With(zap.String("table", tableName)),
	}, nil
}

// Run executes the planning loop until ctx is canceled.
func (p *Planner) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	fl := logutil.NewFailureLogger(p.log, p.failureLogInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.planOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				fl.Fail("planning cycle failed", zap.Error(err))
			} else {
				fl.OK()
			}
		}
	}
}

func (p *Planner) planOnce(ctx context.Context) error {
	// --- Task queue maintenance ---

	// 1. Finalize completed tasks (mark files ARCHIVE_CLOSED, delete source IR).
	if err := p.processCompletedTasks(ctx); err != nil {
		return fmt.Errorf("process completed: %w", err)
	}

	// 2. Re-queue abandoned tasks (claimed by a worker that crashed before finishing).
	if err := p.reclaimStaleTasks(ctx); err != nil {
		return fmt.Errorf("reclaim stale: %w", err)
	}

	// 3. Backpressure check — skip creating new tasks if queue is deep.
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

	// --- Candidate discovery ---

	// 4. Promote stuck files (IR_ARCHIVE_BUFFERING whose data is stale → CONSOLIDATION_PENDING).
	p.promoteStuckBuffering(ctx)

	// 5. Find all CONSOLIDATION_PENDING files (includes any just-promoted).
	candidates, err := p.findCandidates(ctx)
	if err != nil {
		return fmt.Errorf("find candidates: %w", err)
	}

	if len(candidates) == 0 {
		return nil
	}

	// --- Task creation ---

	// 6. Apply policy to group files.
	groups := p.policy.SelectFiles(candidates)

	// 7. Create tasks for each group.
	for _, group := range groups {
		irPaths := make([]string, 0, len(group))
		for _, rec := range group {
			if rec.ClpIRPath.Valid {
				irPaths = append(irPaths, rec.ClpIRPath.String)
			}
		}

		if len(irPaths) == 0 {
			continue
		}

		if !p.inFlight.TryAdd(irPaths) {
			continue
		}

		payload := &taskqueue.TaskPayload{
			TableName:      p.tableName,
			IRPaths:        irPaths,
			ArchiveBackend: p.archiveBackend,
			ArchiveBucket:  p.archiveBucket,
		}
		for _, rec := range group {
			payload.FileIDs = append(payload.FileIDs, rec.ID)
			// MinTimestamp == 0 is treated as uninitialized (not a valid epoch-zero timestamp).
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

		p.log.Debug("created consolidation task",
			zap.Int64("taskId", taskID),
			zap.Int("files", len(group)),
		)
	}

	return nil
}

// findCandidates resolves the policy's required columns and queries for
// CONSOLIDATION_PENDING files with those columns populated.
func (p *Planner) findCandidates(ctx context.Context) ([]*metastore.FileRecord, error) {
	dimMappings, aggMappings := p.resolveColumnMappings()
	return p.fileRecs.FindConsolidationPending(ctx, dimMappings, aggMappings)
}

// resolveColumnMappings resolves the policy's required dims and aggs to physical
// column names using the current registry state. Unresolvable keys are skipped
// (the column may not exist yet; it will be picked up on a future cycle).
func (p *Planner) resolveColumnMappings() (dimMappings, aggMappings []metastore.ColumnMapping) {
	for _, dimKey := range p.policy.RequiredDims() {
		physCol := p.resolver.ResolveDim(dimKey)
		if physCol == "" {
			p.log.Debug("dim key not yet resolvable, skipping",
				zap.String("dimKey", dimKey))
			continue
		}
		dimMappings = append(dimMappings, metastore.ColumnMapping{
			PhysicalCol: physCol,
			LogicalKey:  dimKey,
		})
	}
	for _, agg := range p.policy.RequiredAggs() {
		physCol := p.resolver.ResolveAgg(agg.Key, agg.Value, agg.Type)
		if physCol == "" {
			p.log.Debug("agg key not yet resolvable, skipping",
				zap.String("aggKey", agg.Key),
				zap.String("aggType", agg.Type))
			continue
		}
		aggMappings = append(aggMappings, metastore.ColumnMapping{
			PhysicalCol: physCol,
			LogicalKey:  agg.Key,
		})
	}
	return
}

// promoteStuckBuffering transitions IR_ARCHIVE_BUFFERING files older than
// staleThreshold to CONSOLIDATION_PENDING so the next FindConsolidationPending
// picks them up. Errors are logged and swallowed — promotion is best-effort.
func (p *Planner) promoteStuckBuffering(ctx context.Context) {
	if p.staleThreshold <= 0 {
		return
	}

	staleBeforeNanos := timeutil.EpochNanos() - p.staleThreshold.Nanoseconds()
	n, err := p.fileRecs.PromoteStuckBuffering(ctx, staleBeforeNanos)
	if err != nil {
		p.log.Warn("promote stuck buffering failed", zap.Error(err))
		return
	}
	if n > 0 {
		p.log.Info("promoted stuck buffering files", zap.Int64("promoted", n))
	}
}

func (p *Planner) processCompletedTasks(ctx context.Context) error {
	// Collect all completed tasks first, then close the cursor before processing.
	// This avoids holding a DB connection open during potentially slow storage operations.
	type completedTask struct {
		taskID int64
		input  []byte
		output []byte
	}

	query, qArgs, err := sq.Select("task_id", "input", "output").
		From(taskqueue.TableName).
		Where(sq.Eq{
			"table_name": p.tableName,
			"state":      []string{"completed", "failed", "dead_letter"},
		}).
		Limit(100).
		ToSql()
	if err != nil {
		return fmt.Errorf("process completed: build query: %w", err)
	}
	rows, err := p.db.QueryContext(ctx, query, qArgs...)
	if err != nil {
		return fmt.Errorf("process completed: query: %w", err)
	}

	var tasks []completedTask
	for rows.Next() {
		var t completedTask
		if err := rows.Scan(&t.taskID, &t.input, &t.output); err != nil {
			rows.Close()
			return fmt.Errorf("process completed: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("process completed: rows: %w", err)
	}

	for _, t := range tasks {
		payload, err := taskqueue.UnmarshalPayload(t.input)
		if err != nil {
			p.log.Error("unmarshal task payload failed", zap.Int64("taskId", t.taskID), zap.Error(err))
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		// Failed/dead-letter tasks: free in-flight paths, delete task, skip archive update.
		if t.output == nil {
			p.inFlight.Remove(payload.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		result, err := taskqueue.UnmarshalResult(t.output)
		if err != nil {
			p.log.Error("unmarshal task result failed", zap.Int64("taskId", t.taskID), zap.Error(err))
			p.inFlight.Remove(payload.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		if result.Error != "" {
			p.log.Warn("task completed with error", zap.Int64("taskId", t.taskID), zap.String("error", result.Error))
			p.inFlight.Remove(payload.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
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
			p.log.Error("mark archive closed failed", zap.Int64("taskId", t.taskID), zap.Error(err))
			// Keep paths in inFlight — removing them would allow
			// FindConsolidationPending to re-queue the same files as a
			// duplicate task. The next cycle will retry MarkArchiveClosed
			// against the still-completed task.
			continue
		}

		// Delete source IR files from storage (best-effort)
		p.deleteIRFiles(ctx, payload)

		// Remove from in-flight set and mark task processed
		p.inFlight.Remove(payload.IRPaths)
		p.markTaskProcessed(ctx, t.taskID)
	}

	// Catch-all: delete leaked terminal task rows that a previous cycle failed to
	// clean up (e.g. planner crashed between finalization and deletion). The 24h
	// age gate avoids racing with the processing loop above.
	if n, err := p.taskQueue.CleanupOldTasks(ctx, p.tableName, config.DefaultTaskCleanupAge); err != nil {
		p.log.Warn("cleanup old tasks failed", zap.Error(err))
	} else if n > 0 {
		p.log.Debug("cleaned up old tasks", zap.Int64("deleted", n))
	}

	return nil
}

// markTaskProcessed deletes a terminal task after the planner has fully processed it.
func (p *Planner) markTaskProcessed(ctx context.Context, taskID int64) {
	query, args, _ := sq.Delete(taskqueue.TableName).
		Where(sq.Eq{
			"task_id": taskID,
			"state":   []string{"completed", "failed", "dead_letter"},
		}).
		ToSql()
	_, err := p.db.ExecContext(ctx, query, args...)
	if err != nil {
		p.log.Warn("delete processed task failed", zap.Int64("taskId", taskID), zap.Error(err))
	}
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

func (p *Planner) reclaimStaleTasks(ctx context.Context) error {
	staleTasks, err := p.taskQueue.FindStaleTasks(ctx, p.tableName, config.DefaultTaskStaleTimeout)
	if err != nil {
		return fmt.Errorf("reclaim stale tasks: %w", err)
	}

	for _, task := range staleTasks {
		if err := p.taskQueue.ReclaimTask(ctx, task.TaskID); err != nil {
			p.log.Error("reclaim task failed", zap.Int64("taskId", task.TaskID), zap.Error(err))
		}
	}
	return nil
}
