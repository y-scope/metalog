package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator"
	"github.com/y-scope/metalog/coordinator/consolidation"
	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/coordinator/retention"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/taskqueue"
)

// partitionMaintenanceInterval is how often partition lookahead/cleanup runs.
const partitionMaintenanceInterval = time.Hour

// aliasRefreshInterval is how often alias_column values are re-read from the DB.
// Frequent enough that admin-set aliases propagate quickly; lightweight (SELECT only).
const aliasRefreshInterval = time.Minute

// CoordinatorUnit manages coordinator goroutines for a single table.
type CoordinatorUnit struct {
	tableName     string
	tableCfg      metastore.TableConfig
	shared        *Resources
	writer        *ingestion.BatchingWriter
	planner           *consolidation.Planner
	retentionStrategy retention.Strategy
	partition         *schema.PartitionManager
	registry          *schema.ColumnRegistry
	progress          *coordinator.ProgressTracker
	kafkaAdapter      KafkaAdapter
	log               *zap.Logger

	parentCtx context.Context // preserved for Restart
	ctxMu     sync.Mutex      // protects ctx and cancel
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewCoordinatorUnit creates a coordinator unit for a table.
// tableID is the UUID from the _table registry, used to derive a unique Kafka consumer
// group ID across environments sharing the same Kafka cluster.
func NewCoordinatorUnit(
	ctx context.Context,
	tableName string,
	tableID string,
	tableCfg metastore.TableConfig,
	shared *Resources,
	writer *ingestion.BatchingWriter,
	ingestSvc *ingestion.Service,
	kafkaFactory KafkaAdapterFactory,
	log *zap.Logger,
) (*CoordinatorUnit, error) {
	reg, err := schema.NewColumnRegistry(ctx, shared.DB, tableName, shared.IsMariaDB, log)
	if err != nil {
		return nil, fmt.Errorf("new coordinator unit: column registry: %w", err)
	}

	writer.SetRegistry(tableName, reg)
	shared.SetColumnRegistry(tableName, reg)

	// Consolidation planner (conditional on feature flag).
	var planner *consolidation.Planner
	if tableCfg.Consolidation.Enabled {
		inFlight := consolidation.NewInFlightSet()

		policy, err := consolidation.CreatePolicyChain(tableCfg.Consolidation.Policies)
		if err != nil {
			return nil, fmt.Errorf("new coordinator unit: create policy chain: %w", err)
		}

		taskQueue := taskqueue.NewQueue(shared.DB, log)

		staleThreshold := 60 * time.Minute
		if tableCfg.Consolidation.StaleBufferingMins > 0 {
			staleThreshold = time.Duration(tableCfg.Consolidation.StaleBufferingMins) * time.Minute
		} else if tableCfg.Consolidation.StaleBufferingMins < 0 {
			staleThreshold = 0 // disabled
		}

		planner, err = consolidation.NewPlanner(consolidation.PlannerConfig{
			DB:                 shared.DB,
			TableName:          tableName,
			IsMariaDB:          shared.IsMariaDB,
			Policy:             policy,
			InFlight:           inFlight,
			TaskQueue:          taskQueue,
			Resolver:           reg,
			StorageRegistry:    shared.StorageRegistry,
			ArchiveBackend:     shared.ArchiveBackend,
			ArchiveBucket:      shared.ArchiveBucket,
			Interval:           config.DefaultPlannerInterval,
			FailureLogInterval: shared.FailureLogInterval,
			StaleThreshold:     staleThreshold,
			Log:                log,
		})
		if err != nil {
			return nil, fmt.Errorf("new coordinator unit: planner: %w", err)
		}
	}

	// Retention strategy — always created; conditionally started in Start().
	retTypeName := tableCfg.Retention.Type
	if retTypeName == "" {
		retTypeName = "default"
	}
	retStrategy, err := retention.CreateStrategy(retTypeName, retention.Deps{
		DB:                 shared.DB,
		TableName:          tableName,
		IsMariaDB:          shared.IsMariaDB,
		StorageRegistry:    shared.StorageRegistry,
		FailureLogInterval: shared.FailureLogInterval,
		Log:                log,
	})
	if err != nil {
		return nil, fmt.Errorf("new coordinator unit: retention strategy: %w", err)
	}

	partMgr := schema.NewPartitionManager(shared.DB, tableName, 7, 90, log)

	// Create Kafka adapter if configured — routes through IngestionService
	// for proper dim/agg column resolution.
	var kafkaAdapter KafkaAdapter
	if kafkaFactory != nil {
		adapter, err := kafkaFactory(tableName, tableID, tableCfg, ingestSvc, log)
		if errors.Is(err, ErrKafkaNotConfigured) {
			// Table doesn't use Kafka — skip adapter setup.
		} else if err != nil {
			return nil, fmt.Errorf("new coordinator unit: %w", err)
		} else {
			kafkaAdapter = adapter
		}
	}

	progress := coordinator.NewProgressTracker(config.DefaultProgressStallTimeout, log)

	childCtx, cancel := context.WithCancel(ctx)

	return &CoordinatorUnit{
		tableName:        tableName,
		tableCfg:         tableCfg,
		shared:           shared,
		writer:           writer,
		planner:           planner,
		retentionStrategy: retStrategy,
		partition:        partMgr,
		registry:         reg,
		progress:         progress,
		kafkaAdapter:     kafkaAdapter,
		log:              log.With(zap.String("unit", "coordinator"), zap.String("table", tableName)),
		parentCtx:     ctx,
		ctx:           childCtx,
		cancel:        cancel,
	}, nil
}

// TableConfig returns the per-table configuration loaded at startup.
func (u *CoordinatorUnit) TableConfig() metastore.TableConfig {
	return u.tableCfg
}

// IsStalled returns true if the coordinator has not made progress within the stall timeout.
func (u *CoordinatorUnit) IsStalled() bool {
	return u.progress.IsStalled()
}

// Restart stops and restarts the coordinator goroutines. Returns an error
// if the parent context is already cancelled (node shutting down), so the
// caller can release the table assignment for another node to claim.
func (u *CoordinatorUnit) Restart() error {
	u.log.Warn("restarting stalled coordinator")
	u.Stop()
	if u.parentCtx.Err() != nil {
		return fmt.Errorf("restart %s: parent context cancelled", u.tableName)
	}
	u.ctxMu.Lock()
	u.ctx, u.cancel = context.WithCancel(u.parentCtx)
	u.ctxMu.Unlock()
	u.progress.RecordProgress()
	u.Start()
	return nil
}

// Start begins the coordinator goroutines.
func (u *CoordinatorUnit) Start() {
	u.log.Info("starting coordinator unit")

	// Snapshot ctx under the mutex so goroutine closures capture a stable value.
	// Without this, Restart() writing u.ctx races with goroutines reading it.
	u.ctxMu.Lock()
	ctx := u.ctx
	u.ctxMu.Unlock()

	// Planner goroutine (nil when consolidation_enabled=false)
	if u.planner != nil {
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			u.planner.Run(ctx)
		}()
	}

	// Partition maintenance goroutine
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.runPartitionMaintenance(ctx)
	}()

	// Alias refresh goroutine
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.runAliasRefresh(ctx)
	}()

	// Retention cleanup goroutine (conditional on retention.enabled)
	if u.tableCfg.Retention.Enabled {
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			u.retentionStrategy.Run(ctx)
		}()
	}

	// Column recycler goroutine
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.registry.RunRecycler(ctx)
	}()

	// Kafka adapter goroutine — if the adapter exits unexpectedly (fatal
	// Kafka error), cancel the coordinator so the reconciliation loop can
	// restart it. Without this, other goroutines (partition maintenance,
	// alias refresh) mask the failure and IsStalled() never fires.
	if u.kafkaAdapter != nil {
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			u.kafkaAdapter.Start(ctx)
			if ctx.Err() == nil {
				u.log.Error("kafka adapter exited unexpectedly, cancelling coordinator")
				u.cancel()
			}
		}()
	}

	u.log.Info("coordinator unit started")
}

// Stop signals all goroutines to stop and waits for completion.
func (u *CoordinatorUnit) Stop() {
	u.log.Info("stopping coordinator unit")
	// Stop the Kafka adapter before cancelling the context so push-based
	// adapters can deregister cleanly while the goroutine is still running.
	if u.kafkaAdapter != nil {
		u.kafkaAdapter.Stop()
	}
	u.ctxMu.Lock()
	cancel := u.cancel
	u.ctxMu.Unlock()
	cancel()
	u.wg.Wait()
	u.log.Info("coordinator unit stopped")
}

func (u *CoordinatorUnit) runAliasRefresh(ctx context.Context) {
	ticker := time.NewTicker(aliasRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.progress.RecordProgress()
			if err := u.registry.RefreshAliases(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				u.log.Warn("alias refresh failed", zap.Error(err))
			}
		}
	}
}

func (u *CoordinatorUnit) runPartitionMaintenance(ctx context.Context) {
	// Run once on startup
	if err := u.partition.RunMaintenance(ctx); err != nil {
		u.log.Warn("initial partition maintenance failed", zap.Error(err))
	}
	u.progress.RecordProgress()

	ticker := time.NewTicker(partitionMaintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.progress.RecordProgress()
			if err := u.partition.RunMaintenance(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				u.log.Warn("partition maintenance failed", zap.Error(err))
			}
		}
	}
}
