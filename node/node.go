package node

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/health"
	"github.com/y-scope/metalog/logutil"
	"github.com/y-scope/metalog/node/registry"
	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/storage"
	"github.com/y-scope/metalog/telemetry"
)

// Node is the top-level orchestrator that manages coordinator and worker units.
type Node struct {
	cfg          *config.NodeConfig
	nodeID       string
	shared       *Resources
	registry     *registry.Registry
	writer       *ingestion.BatchingWriter
	ingestSvc    *ingestion.Service
	kafkaFactory KafkaAdapterFactory

	coordMu      sync.Mutex
	coordinators map[string]*CoordinatorUnit
	workerUnit   *WorkerUnit
	healthSrv    *health.Server

	// reconcileReg overrides the registry for reconciliation (testing only).
	reconcileReg reconcileRegistry
	// startCoordinatorFn overrides startCoordinator for testing.
	startCoordinatorFn func(tableName string) error

	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewNode creates and initializes a Node from configuration.
func NewNode(cfg *config.NodeConfig, log *zap.Logger, opts ...NodeOption) (*Node, error) {
	nodeID := cfg.ResolveNodeID()
	if nodeID == "" {
		return nil, fmt.Errorf("node ID is empty: set %s env var or configure nodeIdEnvVar", cfg.Coordinator.NodeIDEnvVar)
	}
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
	if detectPool == nil {
		return nil, fmt.Errorf("no database configured (both primary and replica are nil)")
	}
	dbType, versionStr, err := db.DetectDatabaseType(context.Background(), detectPool)
	if err != nil {
		return nil, fmt.Errorf("detect database type: %w", err)
	}
	log.Info("detected database", zap.String("type", dbType.String()), zap.String("version", versionStr))
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
			if name == cfg.Storage.DefaultBackend {
				return nil, fmt.Errorf("create default storage backend %q: %w", name, err)
			}
			log.Warn("failed to create storage backend, skipping", zap.String("name", name), zap.Error(err))
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

	// Create telemetry provider
	var telemetryProvider *telemetry.Provider
	if cfg.Telemetry.Enabled {
		var err error
		telemetryProvider, err = telemetry.NewProvider(cfg.Telemetry)
		if err != nil {
			return nil, fmt.Errorf("create telemetry provider: %w", err)
		}
		log.Info("telemetry enabled", zap.String("exporter", cfg.Telemetry.Exporter))
	}

	shared := &Resources{
		DB:                 pool,
		ReadDB:             readPool,
		StorageRegistry:    storageReg,
		ArchiveCreator:     archiveCreator,
		ArchiveBackend:     cfg.Storage.DefaultBackend,
		ArchiveBucket:      cfg.Storage.Backends[cfg.Storage.DefaultBackend].Bucket,
		IsMariaDB:          isMariaDB,
		FailureLogInterval: cfg.Logging.FailureLogInterval(),
		Telemetry:          telemetryProvider,
		Log:                log,
	}

	cr := registry.New(pool, nodeID, isMariaDB, log)

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

	for _, opt := range opts {
		opt(n)
	}

	// Health server with DB readiness check and metrics endpoint
	if cfg.Health.Enabled {
		n.healthSrv = health.NewServer(cfg.Health.Port, log)
		if pool != nil {
			n.healthSrv.AddChecker(&health.DBChecker{DB: pool})
		}
		if telemetryProvider != nil {
			if h := telemetryProvider.Handler(); h != nil {
				n.healthSrv.SetMetricsHandler(h)
				log.Info("metrics endpoint registered", zap.Int("port", cfg.Health.Port), zap.String("path", "/metrics"))
			}
		}
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

		var bwOpts []ingestion.BatchingWriterOption
		if n.shared.Telemetry != nil {
			bwOpts = append(bwOpts, ingestion.WithMeter(n.shared.Telemetry.Meter("metalog.ingestion")))
		}
		n.writer = ingestion.NewBatchingWriter(n.ctx, n.shared.DB, n.shared.IsMariaDB, n.log, bwOpts...)
		n.ingestSvc = ingestion.NewService(n.writer, n.cfg.GRPC.IsBlockingIngestion(), n.log)

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
				n.log.Error("health HTTP server failed", zap.Int("port", n.cfg.Health.Port), zap.Error(err))
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
func (n *Node) Shared() *Resources {
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

	cu, err := NewCoordinatorUnit(n.ctx, tableName, tableID, tableCfg, n.shared, n.writer, n.ingestSvc, n.kafkaFactory, n.log)
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

	fl := logutil.NewFailureLogger(n.log, n.shared.FailureLogInterval)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			var err error
			if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
				err = n.registry.RenewLeases(n.ctx, time.Duration(n.cfg.Coordinator.LeaseTTLSeconds)*time.Second)
			} else {
				err = n.registry.SendHeartbeat(n.ctx)
			}
			if err != nil {
				fl.Fail("liveness failed", zap.Error(err))
			} else {
				fl.OK()
			}
		}
	}
}

func (n *Node) runReconciliation() {
	interval := time.Duration(n.cfg.Coordinator.ReconciliationIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fl := logutil.NewFailureLogger(n.log, n.shared.FailureLogInterval)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if err := n.reconcile(); err != nil {
				fl.Fail("reconciliation failed", zap.Error(err))
			} else {
				fl.OK()
			}
		}
	}
}

func (n *Node) getReconcileRegistry() reconcileRegistry {
	if n.reconcileReg != nil {
		return n.reconcileReg
	}
	return n.registry
}

func (n *Node) doStartCoordinator(tableName string) error {
	if n.startCoordinatorFn != nil {
		return n.startCoordinatorFn(tableName)
	}
	return n.startCoordinator(tableName)
}

func (n *Node) reconcile() error {
	ctx := n.ctx
	reg := n.getReconcileRegistry()

	// Step 1: Claim orphans from dead nodes
	var orphansClaimed []string
	var err error
	if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
		orphansClaimed, err = reg.ClaimOrphansLease(ctx, time.Duration(n.cfg.Coordinator.LeaseTTLSeconds)*time.Second)
	} else {
		orphansClaimed, err = reg.ClaimOrphansHeartbeat(ctx, time.Duration(n.cfg.Coordinator.DeadNodeThresholdSeconds)*time.Second)
	}
	if err != nil {
		n.log.Warn("orphan claim failed", zap.Error(err))
	}
	for _, t := range orphansClaimed {
		n.coordMu.Lock()
		_, exists := n.coordinators[t]
		n.coordMu.Unlock()
		if exists {
			continue
		}
		if err := n.doStartCoordinator(t); err != nil {
			n.log.Error("failed to start coordinator for orphan",
				zap.String("table", t), zap.Error(err))
			if relErr := reg.ReleaseTable(ctx, t); relErr != nil {
				n.log.Warn("failed to release orphan after start failure",
					zap.String("table", t), zap.Error(relErr))
			}
		}
	}

	// Step 2: Claim unassigned tables with fair-share limit.
	// Compute how many tables this node should own so that tables are
	// distributed evenly. If we already own our share, skip claiming
	// and leave the rest for other nodes.
	unassigned, err := reg.GetUnassignedTables(ctx)
	if err != nil {
		return fmt.Errorf("get unassigned tables: %w", err)
	}
	if len(unassigned) > 0 {
		var activeNodes int
		var err error
		if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
			activeNodes, err = reg.CountActiveNodesLease(ctx)
		} else {
			deadThreshold := time.Duration(n.cfg.Coordinator.DeadNodeThresholdSeconds) * time.Second
			activeNodes, err = reg.CountActiveNodesHeartbeat(ctx, deadThreshold)
		}
		if err != nil {
			n.log.Warn("count active nodes failed", zap.Error(err))
			activeNodes = 1 // fall back to claiming all
		}
		if activeNodes < 1 {
			activeNodes = 1
		}
		myTables, err := reg.CountMyTables(ctx)
		if err != nil {
			n.log.Warn("count my tables failed", zap.Error(err))
			myTables = 0
		}
		totalAssigned, err := reg.CountAssignedTables(ctx)
		if err != nil {
			n.log.Warn("count assigned tables failed", zap.Error(err))
			totalAssigned = 0
		}
		totalTables := totalAssigned + len(unassigned)
		fairShare := (totalTables + activeNodes - 1) / activeNodes // ceil division

		for _, t := range unassigned {
			if myTables >= fairShare {
				n.log.Debug("fair-share reached, deferring remaining tables",
					zap.Int("myTables", myTables), zap.Int("fairShare", fairShare))
				break
			}
			n.coordMu.Lock()
			_, exists := n.coordinators[t]
			n.coordMu.Unlock()
			if exists {
				continue
			}
			var leaseTTL time.Duration
			if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
				leaseTTL = time.Duration(n.cfg.Coordinator.LeaseTTLSeconds) * time.Second
			}
			ok, err := reg.ClaimTable(ctx, t, leaseTTL)
			if err != nil {
				n.log.Debug("claim unassigned table failed", zap.String("table", t), zap.Error(err))
				continue
			}
			if ok {
				myTables++
				if err := n.doStartCoordinator(t); err != nil {
					n.log.Error("failed to start coordinator for claimed table",
						zap.String("table", t), zap.Error(err))
					myTables--
					if relErr := reg.ReleaseTable(ctx, t); relErr != nil {
						n.log.Warn("failed to release table after start failure",
							zap.String("table", t), zap.Error(relErr))
					}
				}
			}
		}
	}

	// Step 3: Watchdog — restart stalled coordinators.
	// Collect stalled coordinators under lock, then restart outside lock.
	type stalledEntry struct {
		name string
		cu   *CoordinatorUnit
	}
	var stalled []stalledEntry
	n.coordMu.Lock()
	for name, cu := range n.coordinators {
		if cu.IsStalled() {
			n.log.Warn("coordinator stalled, restarting", zap.String("table", name))
			stalled = append(stalled, stalledEntry{name, cu})
		}
	}
	n.coordMu.Unlock()
	for _, s := range stalled {
		if err := s.cu.Restart(); err != nil {
			n.log.Error("restart failed, releasing assignment",
				zap.String("table", s.name), zap.Error(err))
			n.coordMu.Lock()
			delete(n.coordinators, s.name)
			n.coordMu.Unlock()
			if relErr := reg.ReleaseTable(ctx, s.name); relErr != nil {
				n.log.Warn("failed to release table after restart failure",
					zap.String("table", s.name), zap.Error(relErr))
			}
		}
	}

	// Step 4: Ownership verification — stop coordinators for lost assignments,
	// start coordinators for new assignments
	assigned, err := reg.GetAssignedTables(ctx)
	if err != nil {
		return fmt.Errorf("get assigned tables: %w", err)
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
		if err := n.doStartCoordinator(t); err != nil {
			n.log.Error("failed to start coordinator for assignment",
				zap.String("table", t), zap.Error(err))
		}
	}
	return nil
}
