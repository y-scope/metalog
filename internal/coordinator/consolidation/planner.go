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

// terminalTaskBatchSize is the maximum number of completed/failed/dead-letter
// tasks to process per planning cycle.
const terminalTaskBatchSize = 100

// ColumnResolver resolves logical dimension/aggregation keys to physical column names.
// Satisfied by schema.ColumnRegistry.
type ColumnResolver interface {
	ResolveDim(dimKey string) string
	ResolveAgg(aggKey, aggValue, aggType string) string
}

// fileRecordStore is the subset of metastore.FileRecords used by the planner.
type fileRecordStore interface {
	FindConsolidationPending(ctx context.Context, dimMappings, aggMappings []metastore.ColumnMapping) ([]*metastore.FileRecord, error)
	PromoteStuckBuffering(ctx context.Context, staleBeforeNanos int64) (int64, error)
	MarkArchiveClosed(ctx context.Context, irPaths []string, archivePath, archiveBackend, archiveBucket string, archiveSizeBytes, archiveCreatedAt int64) error
}

// terminalTask is a completed, failed, or dead-letter task ready for processing.
type terminalTask struct {
	taskID int64
	input  []byte
	output []byte
}

// taskStore is the subset of task queue operations used by the planner.
type taskStore interface {
	CreateTask(ctx context.Context, tableName string, version uint8, input []byte) (int64, error)
	CleanupOldTasks(ctx context.Context, tableName string, maxAge time.Duration) (int64, error)
	FindStaleTasks(ctx context.Context, tableName string, timeout time.Duration) ([]*taskqueue.Task, error)
	ReclaimTask(ctx context.Context, taskID int64) error
	FindTerminalTasks(ctx context.Context, tableName string, limit int) ([]terminalTask, error)
	DeleteTerminalTask(ctx context.Context, taskID int64) error
	CountActiveTasks(ctx context.Context, tableName string) (int, error)
}

// storageDeleter deletes objects from storage backends.
type storageDeleter interface {
	Delete(ctx context.Context, bucket, path string) error
}

// storageResolver resolves a backend name to a storageDeleter.
type storageResolver interface {
	Get(name string) (storageDeleter, error)
}

// Planner runs the consolidation planning loop for a single table.
type Planner struct {
	tableName       string
	policy          Policy
	inFlight        *InFlightSet
	tasks           taskStore
	fileRecs        fileRecordStore
	storageResolver storageResolver
	archiveBackend  string
	archiveBucket   string
	interval           time.Duration
	failureLogInterval time.Duration
	staleThreshold     time.Duration // 0 disables stuck-file promotion
	activeTaskCount    int           // in-memory counter for backpressure (pending + processing)
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

	var sr storageResolver
	if storageRegistry != nil {
		sr = &registryAdapter{reg: storageRegistry}
	}

	return &Planner{
		tableName:       tableName,
		policy:          policy,
		inFlight:        inFlight,
		tasks:           &queueAdapter{db: db, q: taskQueue},
		fileRecs:        fr,
		storageResolver: sr,
		archiveBackend:  archiveBackend,
		archiveBucket:   archiveBucket,
		interval:           interval,
		failureLogInterval: failureLogInterval,
		staleThreshold:     staleThreshold,
		resolver:           resolver,
		log:                log.With(zap.String("table", tableName)),
	}, nil
}

// queueAdapter wraps *taskqueue.Queue and *sql.DB to satisfy taskStore.
// The raw SQL operations (FindTerminalTasks, DeleteTerminalTask) use the DB
// directly because the Queue type doesn't expose these methods.
type queueAdapter struct {
	db *sql.DB
	q  *taskqueue.Queue
}

func (a *queueAdapter) CreateTask(ctx context.Context, tableName string, version uint8, input []byte) (int64, error) {
	return a.q.CreateTask(ctx, tableName, version, input)
}
func (a *queueAdapter) CleanupOldTasks(ctx context.Context, tableName string, maxAge time.Duration) (int64, error) {
	return a.q.CleanupOldTasks(ctx, tableName, maxAge)
}
func (a *queueAdapter) FindStaleTasks(ctx context.Context, tableName string, timeout time.Duration) ([]*taskqueue.Task, error) {
	return a.q.FindStaleTasks(ctx, tableName, timeout)
}
func (a *queueAdapter) ReclaimTask(ctx context.Context, taskID int64) error {
	return a.q.ReclaimTask(ctx, taskID)
}

func (a *queueAdapter) CountActiveTasks(ctx context.Context, tableName string) (int, error) {
	counts, err := a.q.GetTaskCounts(ctx, tableName)
	if err != nil {
		return 0, err
	}
	return counts.Pending + counts.Processing, nil
}

func (a *queueAdapter) FindTerminalTasks(ctx context.Context, tableName string, limit int) ([]terminalTask, error) {
	query, args, err := sq.Select("task_id", "input", "output").
		From(taskqueue.TableName).
		Where(sq.Eq{
			"table_name": tableName,
			"state":      []string{"completed", "failed", "dead_letter"},
		}).
		Limit(uint64(limit)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("find terminal tasks: build query: %w", err)
	}
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find terminal tasks: %w", err)
	}
	defer rows.Close()

	var tasks []terminalTask
	for rows.Next() {
		var t terminalTask
		if err := rows.Scan(&t.taskID, &t.input, &t.output); err != nil {
			return nil, fmt.Errorf("find terminal tasks: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (a *queueAdapter) DeleteTerminalTask(ctx context.Context, taskID int64) error {
	query, args, _ := sq.Delete(taskqueue.TableName).
		Where(sq.Eq{
			"task_id": taskID,
			"state":   []string{"completed", "failed", "dead_letter"},
		}).
		ToSql()
	_, err := a.db.ExecContext(ctx, query, args...)
	return err
}

// registryAdapter wraps *storage.Registry to satisfy storageResolver.
type registryAdapter struct {
	reg *storage.Registry
}

func (a *registryAdapter) Get(name string) (storageDeleter, error) {
	return a.reg.Get(name)
}

// Run executes the planning loop until ctx is canceled.
func (p *Planner) Run(ctx context.Context) {
	// Seed activeTaskCount from the DB so backpressure is accurate after restart.
	if count, err := p.tasks.CountActiveTasks(ctx, p.tableName); err != nil {
		p.log.Warn("failed to seed active task count, starting from 0", zap.Error(err))
	} else {
		p.activeTaskCount = count
	}

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
	if p.activeTaskCount >= maxBackpressureDepth {
		p.log.Debug("backpressure: skipping task creation",
			zap.Int("activeTaskCount", p.activeTaskCount),
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
		irPaths := make([]string, 0, len(group.Records))
		for _, rec := range group.Records {
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

		archiveBackend := group.ArchiveBackend
		if archiveBackend == "" {
			archiveBackend = p.archiveBackend
		}
		archiveBucket := group.ArchiveBucket
		if archiveBucket == "" {
			archiveBucket = p.archiveBucket
		}

		cons := &taskqueue.ConsolidationPayload{
			IRPaths:        irPaths,
			ArchiveBackend: archiveBackend,
			ArchiveBucket:  archiveBucket,
			ArchivePath:    group.ArchivePath,
		}
		for _, rec := range group.Records {
			cons.FileIDs = append(cons.FileIDs, rec.ID)
			// MinTimestamp == 0 is treated as uninitialized (not a valid epoch-zero timestamp).
			if cons.MinTimestamp == 0 || rec.MinTimestamp < cons.MinTimestamp {
				cons.MinTimestamp = rec.MinTimestamp
			}
		}
		if len(group.Records) > 0 && group.Records[0].ClpIRStorageBackend.Valid {
			cons.IRBackend = group.Records[0].ClpIRStorageBackend.String
		}
		if len(group.Records) > 0 && group.Records[0].ClpIRBucket.Valid {
			cons.IRBuckets = make([]string, len(group.Records))
			for i, rec := range group.Records {
				if rec.ClpIRBucket.Valid {
					cons.IRBuckets[i] = rec.ClpIRBucket.String
				}
			}
		}
		payload := &taskqueue.TaskPayload{
			TableName:     p.tableName,
			Consolidation: cons,
		}

		input, err := taskqueue.MarshalPayload(payload)
		if err != nil {
			p.inFlight.Remove(irPaths)
			p.log.Error("marshal payload failed", zap.Error(err))
			continue
		}

		taskID, err := p.tasks.CreateTask(ctx, p.tableName, taskqueue.TaskPayloadVersion, input)
		if err != nil {
			p.inFlight.Remove(irPaths)
			return fmt.Errorf("create task: %w", err)
		}
		p.activeTaskCount++

		p.log.Debug("created consolidation task",
			zap.Int64("taskId", taskID),
			zap.Int("files", len(group.Records)),
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
// Returns nil, nil if no resolver is configured (e.g., in tests).
func (p *Planner) resolveColumnMappings() ([]metastore.ColumnMapping, []metastore.ColumnMapping) {
	if p.resolver == nil {
		return nil, nil
	}
	var dimMappings, aggMappings []metastore.ColumnMapping
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
	return dimMappings, aggMappings
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
	tasks, err := p.tasks.FindTerminalTasks(ctx, p.tableName, terminalTaskBatchSize)
	if err != nil {
		return fmt.Errorf("process completed: %w", err)
	}

	for _, t := range tasks {
		payload, err := taskqueue.UnmarshalPayload(t.input)
		if err != nil {
			p.log.Error("unmarshal task payload failed", zap.Int64("taskId", t.taskID), zap.Error(err))
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		cons := payload.Consolidation
		if cons == nil {
			p.log.Error("task payload missing consolidation data", zap.Int64("taskId", t.taskID))
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		// Failed/dead-letter tasks: free in-flight paths, delete task, skip archive update.
		if t.output == nil {
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		result, err := taskqueue.UnmarshalResult(t.output)
		if err != nil {
			p.log.Error("unmarshal task result failed", zap.Int64("taskId", t.taskID), zap.Error(err))
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		if result.Error != "" {
			p.log.Warn("task completed with error", zap.Int64("taskId", t.taskID), zap.String("error", result.Error))
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.taskID)
			continue
		}

		// Mark files as ARCHIVE_CLOSED
		err = p.fileRecs.MarkArchiveClosed(ctx,
			cons.IRPaths,
			result.ArchivePath,
			cons.ArchiveBackend,
			cons.ArchiveBucket,
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
		p.deleteIRFiles(ctx, cons)

		// Remove from in-flight set and mark task processed
		p.inFlight.Remove(cons.IRPaths)
		p.markTaskProcessed(ctx, t.taskID)
	}

	// Catch-all: delete leaked terminal task rows that a previous cycle failed to
	// clean up (e.g. planner crashed between finalization and deletion). The 24h
	// age gate avoids racing with the processing loop above.
	if n, err := p.tasks.CleanupOldTasks(ctx, p.tableName, config.DefaultTaskCleanupAge); err != nil {
		p.log.Warn("cleanup old tasks failed", zap.Error(err))
	} else if n > 0 {
		p.log.Debug("cleaned up old tasks", zap.Int64("deleted", n))
	}

	return nil
}

// markTaskProcessed deletes a terminal task after the planner has fully processed it
// and decrements the in-memory active task counter on success.
func (p *Planner) markTaskProcessed(ctx context.Context, taskID int64) {
	if err := p.tasks.DeleteTerminalTask(ctx, taskID); err != nil {
		p.log.Warn("delete processed task failed", zap.Int64("taskId", taskID), zap.Error(err))
		return
	}
	if p.activeTaskCount > 0 {
		p.activeTaskCount--
	}
}

// deleteIRFiles removes source IR files from storage after successful archiving.
func (p *Planner) deleteIRFiles(ctx context.Context, cons *taskqueue.ConsolidationPayload) {
	if p.storageResolver == nil || cons.IRBackend == "" {
		return
	}
	backend, err := p.storageResolver.Get(cons.IRBackend)
	if err != nil {
		p.log.Warn("IR deletion: unknown backend", zap.String("backend", cons.IRBackend))
		return
	}
	for i, irPath := range cons.IRPaths {
		bucket := ""
		if i < len(cons.IRBuckets) {
			bucket = cons.IRBuckets[i]
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
	staleTasks, err := p.tasks.FindStaleTasks(ctx, p.tableName, config.DefaultTaskStaleTimeout)
	if err != nil {
		return fmt.Errorf("reclaim stale tasks: %w", err)
	}

	for _, task := range staleTasks {
		if err := p.tasks.ReclaimTask(ctx, task.TaskID); err != nil {
			p.log.Error("reclaim task failed", zap.Int64("taskId", task.TaskID), zap.Error(err))
		}
	}
	return nil
}
