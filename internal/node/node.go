package node

import (
	"context"
	"database/sql"
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

	// Create DB pool
	pool, err := db.NewPool(cfg.Node.Database)
	if err != nil {
		return nil, err
	}

	// Detect database type
	dbType, versionStr, err := db.DetectDatabaseType(context.Background(), pool)
	if err != nil {
		log.Warn("failed to detect database type, assuming MySQL", zap.Error(err))
	} else {
		log.Info("detected database", zap.String("type", dbType.String()), zap.String("version", versionStr))
	}
	isMariaDB := dbType == db.DatabaseTypeMariaDB

	// Create optional worker DB pool
	var workerPool *sql.DB
	if cfg.Worker.Database != nil {
		wPool, err := db.NewPool(*cfg.Worker.Database)
		if err != nil {
			pool.Close()
			return nil, err
		}
		workerPool = wPool
	}

	// Set up storage registry
	storageReg := storage.NewRegistry()
	for name, backendCfg := range cfg.Node.Storage.Backends {
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

	// Create compressor
	var compressor *storage.ClpCompressor
	if cfg.Node.Storage.ClpBinaryPath != "" {
		compressor = storage.NewClpCompressor(
			cfg.Node.Storage.ClpBinaryPath,
			time.Duration(cfg.Node.Storage.ClpProcessTimeoutSeconds)*time.Second,
			log,
		)
	}

	// Create archive creator
	archiveCreator := storage.NewArchiveCreator(storageReg, compressor, log)

	shared := &SharedResources{
		DB:              pool,
		WorkerDB:        workerPool,
		StorageRegistry: storageReg,
		ArchiveCreator:  archiveCreator,
		ArchiveBackend:  cfg.Node.Storage.DefaultBackend,
		ArchiveBucket:   cfg.Node.Storage.ArchiveBucket,
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
	if cfg.Node.Health.Enabled {
		n.healthSrv = health.NewServer(cfg.Node.Health.Port, log)
	}

	return n, nil
}

// Start initializes system tables, provisions configured tables, claims them,
// starts coordinator and worker units, and begins background goroutines.
// Uses the node's internal context for all operations.
func (n *Node) Start() error {
	ctx := n.ctx

	// Ensure system tables
	if err := n.registry.EnsureSystemTables(ctx); err != nil {
		return err
	}

	// Validate schema is in expected state
	if err := n.registry.ValidateSchemaReady(ctx); err != nil {
		return err
	}

	// Register configured tables
	if err := n.registry.UpsertTables(ctx, n.cfg.Tables); err != nil {
		return err
	}

	// Provision physical tables
	for _, t := range n.cfg.Tables {
		if err := schema.EnsureTable(ctx, n.shared.DB, t.Name, n.shared.IsMariaDB, n.cfg.Node.Storage.TableCompression, n.log); err != nil {
			n.log.Error("failed to provision table", zap.String("table", t.Name), zap.Error(err))
			continue
		}
	}

	// Create batching writer and ingestion service
	n.writer = ingestion.NewBatchingWriter(n.ctx, n.shared.DB, n.log)
	n.ingestSvc = ingestion.NewService(n.writer, n.log)

	// Claim and start coordinators for configured tables
	for _, t := range n.cfg.Tables {
		claimed, err := n.registry.ClaimTable(ctx, t.Name)
		if err != nil {
			n.log.Error("failed to claim table", zap.String("table", t.Name), zap.Error(err))
			continue
		}
		if !claimed {
			n.log.Info("table already claimed by another node", zap.String("table", t.Name))
			continue
		}
		if err := n.startCoordinator(t.Name); err != nil {
			n.log.Error("failed to start coordinator", zap.String("table", t.Name), zap.Error(err))
		}
	}

	// Start workers
	if n.cfg.Worker.NumWorkers > 0 {
		n.workerUnit = NewWorkerUnit(n.ctx, n.cfg.Worker.NumWorkers, n.nodeID, n.shared, n.log)
		n.workerUnit.Start()
	}

	// Signal initial liveness before reconciliation so other nodes see us
	if n.cfg.Node.CoordinatorHAStrategy == config.HAStrategyHeartbeat {
		if err := n.registry.SendHeartbeat(ctx); err != nil {
			n.log.Warn("initial heartbeat failed", zap.Error(err))
		}
	} else {
		if err := n.registry.RenewLeases(ctx, n.cfg.Node.LeaseTTLSeconds); err != nil {
			n.log.Warn("initial lease renewal failed", zap.Error(err))
		}
	}

	// Start liveness goroutine (heartbeat or lease renewal)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runLiveness()
	}()

	// Start reconciliation goroutine
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runReconciliation()
	}()

	// Start health server
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
		zap.Int("workers", n.cfg.Worker.NumWorkers),
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

	// Cancel background goroutines
	n.cancel()

	// Stop coordinators
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

	// Wait for background goroutines
	n.wg.Wait()

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
	// Look up Kafka config for this table
	var kafkaCfg config.TableKafkaConfig
	for _, t := range n.cfg.Tables {
		if t.Name == tableName {
			kafkaCfg = t.Kafka
			break
		}
	}

	cu, err := NewCoordinatorUnit(n.ctx, tableName, kafkaCfg, n.shared, n.writer, n.ingestSvc, n.log)
	if err != nil {
		return err
	}
	n.coordMu.Lock()
	n.coordinators[tableName] = cu
	n.coordMu.Unlock()
	cu.Start()
	return nil
}

func (n *Node) runLiveness() {
	var interval time.Duration
	if n.cfg.Node.CoordinatorHAStrategy == config.HAStrategyLease {
		interval = time.Duration(n.cfg.Node.LeaseRenewalIntervalSeconds) * time.Second
	} else {
		interval = time.Duration(n.cfg.Node.HeartbeatIntervalSeconds) * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if n.cfg.Node.CoordinatorHAStrategy == config.HAStrategyLease {
				if err := n.registry.RenewLeases(n.ctx, n.cfg.Node.LeaseTTLSeconds); err != nil {
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
	interval := time.Duration(n.cfg.Node.ReconciliationIntervalSeconds) * time.Second
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
	if n.cfg.Node.CoordinatorHAStrategy == config.HAStrategyLease {
		orphansClaimed, err = n.registry.ClaimOrphansLease(ctx, n.cfg.Node.LeaseTTLSeconds)
	} else {
		orphansClaimed, err = n.registry.ClaimOrphansHeartbeat(ctx, n.cfg.Node.DeadNodeThresholdSeconds)
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
