package node

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/coordinator/ingestion"
	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/health"
	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/storage"
)

// Node is the top-level orchestrator that manages coordinator and worker units.
type Node struct {
	cfg       *config.NodeConfig
	nodeID    string
	shared    *SharedResources
	registry  *CoordinatorRegistry
	writer    *ingestion.BatchingWriter
	ingestSvc *ingestion.Service

	coordMu      sync.Mutex
	coordinators map[string]*CoordinatorUnit
	workerUnit   *WorkerUnit
	healthSrv    *health.Server

	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewNode creates and initializes a Node from configuration.
func NewNode(cfg *config.NodeConfig, log *zap.Logger) (*Node, error) {
	nodeID := cfg.ResolveNodeID()
	log = log.With(zap.String("nodeId", nodeID))

	// Create primary pool (nil if not configured, e.g. replica-only API server)
	var pool *sql.DB
	if cfg.Database.Primary.Host != "" {
		var err error
		pool, err = db.NewPool(cfg.Database.Primary)
		if err != nil {
			return nil, fmt.Errorf("create primary pool: %w", err)
		}
	}
	success := false
	defer func() {
		if !success && pool != nil {
			pool.Close()
		}
	}()

	// Create replica pool if configured
	var readPool *sql.DB
	if cfg.Database.Replica != nil && cfg.Database.Replica.Host != "" {
		var err error
		readPool, err = db.NewPool(*cfg.Database.Replica)
		if err != nil {
			return nil, fmt.Errorf("create replica pool: %w", err)
		}
		log.Info("replica database pool created",
			zap.String("host", cfg.Database.Replica.Host),
			zap.Int("poolSize", cfg.Database.Replica.PoolSize))
	}
	defer func() {
		if !success && readPool != nil {
			readPool.Close()
		}
	}()

	// Detect database type from whichever pool is available
	detectPool := pool
	if detectPool == nil {
		detectPool = readPool
	}
	dbType, versionStr, err := db.DetectDatabaseType(context.Background(), detectPool)
	if err != nil {
		log.Warn("failed to detect database type, assuming MySQL", zap.Error(err))
	} else {
		log.Info("detected database", zap.String("type", dbType.String()), zap.String("version", versionStr))
	}
	isMariaDB := dbType == db.DatabaseTypeMariaDB

	// Set up storage registry
	storageReg := storage.NewRegistry()
	for name, backendCfg := range cfg.Storage.Backends {
		typeName := backendCfg.Type
		if typeName == "" {
			typeName = "s3"
		}
		backend, err := storage.CreateBackend(typeName, backendCfg.ToMap())
		if err != nil {
			log.Warn("failed to create storage backend", zap.String("name", name), zap.Error(err))
			continue
		}
		storageReg.Register(name, backend)
	}

	// Create compressor (only when workers are enabled)
	var compressor *storage.ClpCompressor
	if cfg.Worker.Concurrency > 0 && cfg.Worker.ClpBinaryPath != "" {
		compressor = storage.NewClpCompressor(
			cfg.Worker.ClpBinaryPath,
			time.Duration(cfg.Worker.ClpProcessTimeoutSeconds)*time.Second,
			log,
		)
	}

	// Create archive creator
	archiveCreator := storage.NewArchiveCreator(storageReg, compressor, log)

	shared := &SharedResources{
		DB:              pool,
		ReadDB:          readPool,
		StorageRegistry: storageReg,
		ArchiveCreator:  archiveCreator,
		ArchiveBackend:  cfg.Storage.DefaultBackend,
		ArchiveBucket:   cfg.Storage.Backends[cfg.Storage.DefaultBackend].Bucket,
		IsMariaDB:       isMariaDB,
		Log:             log,
	}

	cr := NewCoordinatorRegistry(pool, nodeID, isMariaDB, log)

	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:          cfg,
		nodeID:       nodeID,
		shared:       shared,
		registry:     cr,
		coordinators: make(map[string]*CoordinatorUnit),
		log:          log,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Health server
	if cfg.Health.Enabled {
		n.healthSrv = health.NewServer(cfg.Health.Port, log)
	}

	success = true
	return n, nil
}

// Start initializes the node based on configuration. Coordinator and worker
// subsystems only start when configured (coordinator.enabled + primary DB).
// Tables are discovered from DB assignments — registered via admin API.
// A node with only a replica DB and gRPC enabled runs as a read-only API server.
func (n *Node) Start() error {
	ctx := n.ctx

	// Coordinator and ingestion subsystems require primary DB
	if n.cfg.HasCoordinator() {
		if err := n.registry.EnsureSystemTables(ctx); err != nil {
			return err
		}
		if err := schema.NewBaseSchemaValidator(n.shared.DB, n.log).Validate(ctx); err != nil {
			return fmt.Errorf("base schema validation failed: %w", err)
		}
		if err := n.registry.ValidateSchemaReady(ctx); err != nil {
			return err
		}

		n.writer = ingestion.NewBatchingWriter(n.ctx, n.shared.DB, n.shared.IsMariaDB, n.log)
		n.ingestSvc = ingestion.NewService(n.writer, n.log)

		// Resume coordinators for tables assigned to this node in the DB
		// (registered via admin API, assigned by reconciliation).
		assigned, err := n.registry.GetAssignedTables(ctx)
		if err != nil {
			n.log.Warn("failed to get assigned tables from DB", zap.Error(err))
		} else {
			for _, t := range assigned {
				n.log.Info("resuming coordinator for assigned table", zap.String("table", t))
				if err := n.startCoordinator(t); err != nil {
					n.log.Error("failed to start coordinator", zap.String("table", t), zap.Error(err))
				}
			}
		}

		// Signal initial liveness
		if n.cfg.Coordinator.HAStrategy == config.HAStrategyHeartbeat {
			if err := n.registry.SendHeartbeat(ctx); err != nil {
				n.log.Warn("initial heartbeat failed", zap.Error(err))
			}
		} else {
			if err := n.registry.RenewLeases(ctx, time.Duration(n.cfg.Coordinator.LeaseTTLSeconds)*time.Second); err != nil {
				n.log.Warn("initial lease renewal failed", zap.Error(err))
			}
		}

		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.runLiveness()
		}()

		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.runReconciliation()
		}()
	}

	// Workers require primary DB
	if n.cfg.Worker.Concurrency > 0 {
		n.workerUnit = NewWorkerUnit(n.ctx, n.cfg.Worker.Concurrency, n.nodeID, n.shared, n.log)
		n.workerUnit.Start()
	}

	// Health server
	if n.healthSrv != nil {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			if err := n.healthSrv.Start(); err != nil {
				n.log.Error("health server error", zap.Error(err))
			}
		}()
		n.healthSrv.SetReady(true)
	}

	n.log.Info("node started",
		zap.Int("coordinators", len(n.coordinators)),
		zap.Int("workers", n.cfg.Worker.Concurrency),
	)
	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	n.log.Info("stopping node")

	// Mark not ready
	if n.healthSrv != nil {
		n.healthSrv.SetReady(false)
	}

	// Cancel background goroutines (reconciliation, liveness, stall checker)
	n.cancel()

	// Wait for background goroutines to finish first — this ensures
	// reconciliation (which may call Restart()) completes before we
	// stop coordinators, preventing a WaitGroup reuse panic.
	n.wg.Wait()

	// Stop coordinators (safe now — no concurrent Restart() calls)
	n.coordMu.Lock()
	coords := make(map[string]*CoordinatorUnit, len(n.coordinators))
	for k, v := range n.coordinators {
		coords[k] = v
	}
	n.coordMu.Unlock()
	for name, cu := range coords {
		cu.Stop()
		if err := n.registry.ReleaseTable(context.Background(), name); err != nil {
			n.log.Warn("failed to release table", zap.String("table", name), zap.Error(err))
		}
	}

	// Stop batching writer
	if n.writer != nil {
		n.writer.Stop()
	}

	// Stop workers
	if n.workerUnit != nil {
		n.workerUnit.Stop()
	}

	// Stop health server
	if n.healthSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		n.healthSrv.Stop(shutdownCtx)
	}

	// Deregister node from _node_registry (skip when no primary DB, e.g. API-only nodes)
	if n.shared.DB != nil {
		if err := n.registry.DeregisterNode(context.Background()); err != nil {
			n.log.Warn("failed to deregister node", zap.Error(err))
		}
	}

	// Release shared resources
	n.shared.Close()

	n.log.Info("node stopped")
}

// Writer returns the batching writer for gRPC ingestion.
func (n *Node) Writer() *ingestion.BatchingWriter {
	return n.writer
}

// IngestionService returns the shared ingestion service.
func (n *Node) IngestionService() *ingestion.Service {
	return n.ingestSvc
}

// Shared returns the node's shared resources.
func (n *Node) Shared() *SharedResources {
	return n.shared
}

// NodeID returns the node's identifier.
func (n *Node) NodeID() string {
	return n.nodeID
}

func (n *Node) startCoordinator(tableName string) error {
	// Ensure the data table exists (idempotent). This covers reconciliation
	// paths where a table was registered via admin API but never provisioned.
	if err := schema.EnsureTable(n.ctx, n.shared.DB, tableName, n.shared.IsMariaDB, n.cfg.Coordinator.TableCompression, n.log); err != nil {
		return fmt.Errorf("start coordinator %s: ensure table: %w", tableName, err)
	}

	// Read unified table config blob from _table_config.
	tableCfg, err := n.registry.GetTableConfig(n.ctx, tableName)
	if err != nil {
		return fmt.Errorf("start coordinator %s: %w", tableName, err)
	}

	if tableCfg.Kafka.Enabled && tableCfg.Kafka.Topic == "" {
		n.log.Warn("kafka enabled but no topic configured — coordinator will run without Kafka consumer",
			zap.String("table", tableName))
	}

	tableID, err := n.registry.GetTableID(n.ctx, tableName)
	if err != nil {
		return err
	}

	n.log.Info("starting coordinator",
		zap.String("table", tableName),
		zap.Bool("kafka", tableCfg.Kafka.Enabled),
		zap.Bool("consolidation", tableCfg.Consolidation.Enabled),
		zap.Bool("retention", tableCfg.Retention.Enabled),
		zap.String("retentionType", tableCfg.Retention.Type),
	)

	cu, err := NewCoordinatorUnit(n.ctx, tableName, tableID, tableCfg, n.shared, n.writer, n.ingestSvc, n.log)
	if err != nil {
		return err
	}
	cu.Start()
	n.coordMu.Lock()
	n.coordinators[tableName] = cu
	n.coordMu.Unlock()
	return nil
}

func (n *Node) runLiveness() {
	var interval time.Duration
	if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
		interval = time.Duration(n.cfg.Coordinator.LeaseRenewalIntervalSeconds) * time.Second
	} else {
		interval = time.Duration(n.cfg.Coordinator.HeartbeatIntervalSeconds) * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
				if err := n.registry.RenewLeases(n.ctx, time.Duration(n.cfg.Coordinator.LeaseTTLSeconds)*time.Second); err != nil {
					n.log.Warn("lease renewal failed", zap.Error(err))
				}
			} else {
				if err := n.registry.SendHeartbeat(n.ctx); err != nil {
					n.log.Warn("heartbeat failed", zap.Error(err))
				}
			}
		}
	}
}

func (n *Node) runReconciliation() {
	interval := time.Duration(n.cfg.Coordinator.ReconciliationIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.reconcile()
		}
	}
}

func (n *Node) reconcile() {
	ctx := n.ctx

	// Step 1: Claim orphans from dead nodes
	var orphansClaimed []string
	var err error
	if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
		orphansClaimed, err = n.registry.ClaimOrphansLease(ctx, time.Duration(n.cfg.Coordinator.LeaseTTLSeconds)*time.Second)
	} else {
		orphansClaimed, err = n.registry.ClaimOrphansHeartbeat(ctx, time.Duration(n.cfg.Coordinator.DeadNodeThresholdSeconds)*time.Second)
	}
	if err != nil {
		n.log.Warn("orphan claim failed", zap.Error(err))
	}
	for _, t := range orphansClaimed {
		if err := n.startCoordinator(t); err != nil {
			n.log.Error("failed to start coordinator for orphan",
				zap.String("table", t), zap.Error(err))
			if relErr := n.registry.ReleaseTable(ctx, t); relErr != nil {
				n.log.Warn("failed to release orphan after start failure",
					zap.String("table", t), zap.Error(relErr))
			}
		}
	}

	// Step 2: Claim unassigned tables (fair-share)
	unassigned, err := n.registry.GetUnassignedTables(ctx)
	if err != nil {
		n.log.Warn("get unassigned tables failed", zap.Error(err))
		return
	}
	for _, t := range unassigned {
		n.coordMu.Lock()
		_, exists := n.coordinators[t]
		n.coordMu.Unlock()
		if exists {
			continue
		}
		ok, err := n.registry.ClaimTable(ctx, t)
		if err != nil {
			n.log.Warn("claim unassigned table failed", zap.String("table", t), zap.Error(err))
			continue
		}
		if ok {
			if err := n.startCoordinator(t); err != nil {
				n.log.Error("failed to start coordinator for claimed table",
					zap.String("table", t), zap.Error(err))
				if relErr := n.registry.ReleaseTable(ctx, t); relErr != nil {
					n.log.Warn("failed to release table after start failure",
						zap.String("table", t), zap.Error(relErr))
				}
			}
		}
	}

	// Step 3: Watchdog — restart stalled coordinators.
	// Collect stalled coordinators under lock, then restart outside lock.
	var stalled []*CoordinatorUnit
	n.coordMu.Lock()
	for name, cu := range n.coordinators {
		if cu.IsStalled() {
			n.log.Warn("coordinator stalled, restarting", zap.String("table", name))
			stalled = append(stalled, cu)
		}
	}
	n.coordMu.Unlock()
	for _, cu := range stalled {
		cu.Restart()
	}

	// Step 4: Ownership verification — stop coordinators for lost assignments,
	// start coordinators for new assignments
	assigned, err := n.registry.GetAssignedTables(ctx)
	if err != nil {
		n.log.Warn("get assigned tables failed", zap.Error(err))
		return
	}
	assignedSet := make(map[string]bool, len(assigned))
	for _, t := range assigned {
		assignedSet[t] = true
	}

	// Collect coordinators to stop, then release the lock before stopping them.
	// cu.Stop() blocks on wg.Wait() which can take seconds under load.
	var toStopUnits []*CoordinatorUnit
	var toStopNames []string
	n.coordMu.Lock()
	for name, cu := range n.coordinators {
		if !assignedSet[name] {
			n.log.Warn("assignment lost, stopping coordinator", zap.String("table", name))
			toStopUnits = append(toStopUnits, cu)
			toStopNames = append(toStopNames, name)
		}
	}
	n.coordMu.Unlock()

	for _, cu := range toStopUnits {
		cu.Stop()
	}

	n.coordMu.Lock()
	for _, name := range toStopNames {
		delete(n.coordinators, name)
	}
	// Identify newly assigned tables that need coordinators.
	var toStart []string
	for _, t := range assigned {
		if _, running := n.coordinators[t]; !running {
			toStart = append(toStart, t)
		}
	}
	n.coordMu.Unlock()

	// Start coordinators outside the lock.
	for _, t := range toStart {
		n.log.Info("new assignment detected, starting coordinator", zap.String("table", t))
		if err := n.startCoordinator(t); err != nil {
			n.log.Error("failed to start coordinator for assignment",
				zap.String("table", t), zap.Error(err))
		}
	}
}
