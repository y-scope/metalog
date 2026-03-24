package consolidation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/logutil"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/taskqueue"
	"github.com/y-scope/metalog/timeutil"
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

// taskStore is the subset of task queue operations used by the planner.
type taskStore interface {
	CreateTasks(ctx context.Context, tableName string, version uint8, inputs [][]byte) (int64, error)
	CleanupOldTasks(ctx context.Context, tableName string, maxAge time.Duration) (int64, error)
	FindStaleTasks(ctx context.Context, tableName string, timeout time.Duration) ([]*taskqueue.Task, error)
	ReclaimTask(ctx context.Context, taskID int64) error
	FindTerminalTasks(ctx context.Context, tableName string, limit int) ([]taskqueue.TerminalTask, error)
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

	mTasksCreated   metric.Int64Counter
	mTasksCompleted metric.Int64Counter
	mPlanDuration   metric.Float64Histogram
}

// PlannerConfig holds configuration for creating a Planner.
type PlannerConfig struct {
	DB              *sql.DB
	TableName       string
	IsMariaDB       bool
	Policy          Policy
	InFlight        *InFlightSet
	TaskQueue       *taskqueue.Queue
	Resolver        ColumnResolver
	StorageRegistry *storage.Registry
	ArchiveBackend  string
	ArchiveBucket   string
	Interval        time.Duration
	FailureLogInterval time.Duration
	StaleThreshold  time.Duration
	Log             *zap.Logger
}

// NewPlanner creates a Planner. Column resolution happens per-cycle in planOnce
// so that newly-registered columns are picked up without restarting the planner.
func NewPlanner(cfg PlannerConfig) (*Planner, error) {
	if cfg.FailureLogInterval <= 0 {
		cfg.FailureLogInterval = time.Minute
	}

	fr, err := metastore.NewFileRecords(cfg.DB, cfg.TableName, cfg.IsMariaDB, cfg.Log)
	if err != nil {
		return nil, fmt.Errorf("new planner: %w", err)
	}

	var sr storageResolver
	if cfg.StorageRegistry != nil {
		sr = &registryAdapter{reg: cfg.StorageRegistry}
	}

	p := &Planner{
		tableName:       cfg.TableName,
		policy:          cfg.Policy,
		inFlight:        cfg.InFlight,
		tasks:           &queueAdapter{q: cfg.TaskQueue},
		fileRecs:        fr,
		storageResolver: sr,
		archiveBackend:  cfg.ArchiveBackend,
		archiveBucket:   cfg.ArchiveBucket,
		interval:           cfg.Interval,
		failureLogInterval: cfg.FailureLogInterval,
		staleThreshold:     cfg.StaleThreshold,
		resolver:           cfg.Resolver,
		log:                cfg.Log.With(zap.String("table", cfg.TableName)),
	}
	p.initMetrics(noop.Meter{})
	return p, nil
}

// SetMeter configures OpenTelemetry metrics. Must be called before Run.
func (p *Planner) SetMeter(m metric.Meter) { p.initMetrics(m) }

func (p *Planner) initMetrics(m metric.Meter) {
	p.mTasksCreated, _ = m.Int64Counter("metalog.consolidation.tasks_created",
		metric.WithDescription("Consolidation tasks created"), metric.WithUnit("{task}"))
	p.mTasksCompleted, _ = m.Int64Counter("metalog.consolidation.tasks_completed",
		metric.WithDescription("Consolidation tasks completed"), metric.WithUnit("{task}"))
	p.mPlanDuration, _ = m.Float64Histogram("metalog.consolidation.plan_duration_seconds",
		metric.WithDescription("Time per planning cycle"), metric.WithUnit("s"))
}

// queueAdapter wraps *taskqueue.Queue to satisfy taskStore.
type queueAdapter struct {
	q *taskqueue.Queue
}

func (a *queueAdapter) CreateTasks(ctx context.Context, tableName string, version uint8, inputs [][]byte) (int64, error) {
	return a.q.CreateTasks(ctx, tableName, version, inputs)
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
func (a *queueAdapter) FindTerminalTasks(ctx context.Context, tableName string, limit int) ([]taskqueue.TerminalTask, error) {
	return a.q.FindTerminalTasks(ctx, tableName, limit)
}
func (a *queueAdapter) DeleteTerminalTask(ctx context.Context, taskID int64) error {
	return a.q.DeleteTerminalTask(ctx, taskID)
}
func (a *queueAdapter) CountActiveTasks(ctx context.Context, tableName string) (int, error) {
	counts, err := a.q.GetTaskCounts(ctx, tableName)
	if err != nil {
		return 0, err
	}
	return counts.Pending + counts.Processing, nil
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
			planStart := time.Now()
			tableAttr := attribute.String("table", p.tableName)
			if err := p.planOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				fl.Fail("planning cycle failed", zap.Error(err))
			} else {
				fl.OK()
			}
			p.mPlanDuration.Record(ctx, time.Since(planStart).Seconds(), metric.WithAttributes(tableAttr))
		}
	}
}

func (p *Planner) planOnce(ctx context.Context) error {
	// --- Task queue maintenance ---

	// 1. Finalize terminal tasks (mark files ARCHIVE_CLOSED, delete source IR).
	if err := p.processTerminalTasks(ctx); err != nil {
		return fmt.Errorf("process terminal: %w", err)
	}

	// 2. Re-queue abandoned tasks (claimed by a worker that crashed before finishing).
	if err := p.reclaimStaleTasks(ctx); err != nil {
		return fmt.Errorf("reclaim stale: %w", err)
	}

	// 3. Backpressure check — skip creating new tasks if queue is deep.
	if p.activeTaskCount >= maxBackpressureDepth {
		p.log.Info("backpressure: skipping task creation",
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

	// 7. Build payloads and batch-insert tasks.
	var inputs [][]byte
	var groupPaths [][]string // IR paths per group, for rollback on failure
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

		payload := p.buildPayload(group, irPaths)
		if payload == nil {
			p.inFlight.Remove(irPaths)
			p.log.Warn("skipping group: missing IR bucket info")
			continue
		}
		input, err := taskqueue.MarshalPayload(payload)
		if err != nil {
			p.inFlight.Remove(irPaths)
			p.log.Error("marshal payload failed", zap.Error(err))
			continue
		}
		inputs = append(inputs, input)
		groupPaths = append(groupPaths, irPaths)
	}

	if len(inputs) == 0 {
		return nil
	}

	n, err := p.tasks.CreateTasks(ctx, p.tableName, taskqueue.TaskPayloadVersion, inputs)
	if err != nil {
		// Roll back all in-flight paths since the batch failed.
		for _, paths := range groupPaths {
			p.inFlight.Remove(paths)
		}
		return fmt.Errorf("create tasks: %w", err)
	}
	p.activeTaskCount += int(n)
	p.mTasksCreated.Add(ctx, n, metric.WithAttributes(attribute.String("table", p.tableName)))

	p.log.Info("created consolidation tasks",
		zap.Int64("count", n),
		zap.Int("groups", len(inputs)),
	)

	return nil
}

// buildPayload constructs a TaskPayload from a FileGroup and its extracted IR paths.
func (p *Planner) buildPayload(group FileGroup, irPaths []string) *taskqueue.TaskPayload {
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
		if cons.MinTimestamp == 0 || rec.MinTimestamp < cons.MinTimestamp {
			cons.MinTimestamp = rec.MinTimestamp
		}
	}
	if len(group.Records) > 0 && group.Records[0].ClpIRStorageBackend.Valid {
		cons.IRBackend = group.Records[0].ClpIRStorageBackend.String
	}
	cons.IRBuckets = make([]string, len(group.Records))
	for i, rec := range group.Records {
		if rec.ClpIRBucket.Valid {
			cons.IRBuckets[i] = rec.ClpIRBucket.String
		}
	}

	return &taskqueue.TaskPayload{
		TableName:     p.tableName,
		Consolidation: cons,
	}
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

func (p *Planner) processTerminalTasks(ctx context.Context) error {
	tasks, err := p.tasks.FindTerminalTasks(ctx, p.tableName, terminalTaskBatchSize)
	if err != nil {
		return fmt.Errorf("process completed: %w", err)
	}

	for _, t := range tasks {
		payload, err := taskqueue.UnmarshalPayload(t.Input)
		if err != nil {
			p.log.Error("unmarshal task payload failed", zap.Int64("taskId", t.TaskID), zap.Error(err))
			p.markTaskProcessed(ctx, t.TaskID)
			continue
		}

		cons := payload.Consolidation
		if cons == nil {
			p.log.Error("task payload missing consolidation data", zap.Int64("taskId", t.TaskID))
			p.markTaskProcessed(ctx, t.TaskID)
			continue
		}

		// Failed/dead-letter tasks: free in-flight paths, delete task, skip archive update.
		if t.Output == nil {
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.TaskID)
			continue
		}

		result, err := taskqueue.UnmarshalResult(t.Output)
		if err != nil {
			p.log.Error("unmarshal task result failed", zap.Int64("taskId", t.TaskID), zap.Error(err))
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.TaskID)
			continue
		}

		if result.Error != "" {
			p.log.Warn("task completed with error", zap.Int64("taskId", t.TaskID), zap.String("error", result.Error))
			p.inFlight.Remove(cons.IRPaths)
			p.markTaskProcessed(ctx, t.TaskID)
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
			p.log.Error("mark archive closed failed", zap.Int64("taskId", t.TaskID), zap.Error(err))
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
		p.markTaskProcessed(ctx, t.TaskID)
	}

	// Catch-all: delete leaked terminal task rows that a previous cycle failed to
	// clean up (e.g. planner crashed between finalization and deletion). The 24h
	// age gate avoids racing with the processing loop above.
	if n, err := p.tasks.CleanupOldTasks(ctx, p.tableName, config.DefaultTaskCleanupAge); err != nil {
		p.log.Warn("cleanup old tasks failed", zap.Error(err))
	} else if n > 0 {
		p.log.Info("cleaned up old tasks", zap.Int64("deleted", n))
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
