// Package node provides the top-level orchestrator for metalog processes.
//
// A [Node] manages the full lifecycle of coordinator and worker units:
// provisioning tables, claiming ownership via [registry.Registry],
// starting per-table [CoordinatorUnit] instances and a shared [WorkerUnit]
// pool, and running background goroutines for liveness (heartbeat or lease
// renewal) and reconciliation (orphan reclaim, stall detection).
//
// [Resources] holds database pools, storage backends, and column
// registries shared across all units within a node.
package node

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/health"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/logutil"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node/registry"
	"github.com/y-scope/metalog/schema"
	"github.com/y-scope/metalog/telemetry"
)

// Node is the top-level orchestrator that manages coordinator and worker units.
type Node struct {
	ctx                  context.Context
	reconcileReg         reconcileRegistry
	healthSrv            *health.Server
	writer               *ingestion.BatchingWriter
	ingestSvc            *ingestion.Service
	kafkaFactory         KafkaAdapterFactory
	shared               *Resources
	coordinators         map[string]*CoordinatorUnit
	externalDB           *sql.DB
	registry             *registry.Registry
	log                  *zap.Logger
	cfg                  *config.NodeConfig
	externalReadDB       *sql.DB
	kafkaUnits           map[string]*KafkaIngestionUnit
	cancel               context.CancelFunc
	startCoordinatorFn   func(tableName string) error
	externalTelemetry    *telemetry.Provider
	nodeID               string
	archiveBackend       string
	archiveBucket        string
	wg                   sync.WaitGroup
	kafkaMu              sync.Mutex
	coordMu              sync.Mutex
	hasTelemetryProvider bool
	hasExternalDB        bool
	hasExternalReadDB    bool
}

// NewNode creates and initializes a Node from configuration.
func NewNode(cfg *config.NodeConfig, log *zap.Logger, opts ...NodeOption) (*Node, error) {
	nodeID, err := cfg.ResolveNodeID()
	if err != nil {
		return nil, fmt.Errorf("resolve node ID: %w", err)
	}
	log = log.With(zap.String("nodeId", nodeID))

	ctx, cancel := context.WithCancel(context.Background())

	// Create the node early so options can be applied before pool creation.
	n := &Node{
		cfg:          cfg,
		nodeID:       nodeID,
		coordinators: make(map[string]*CoordinatorUnit),
		kafkaUnits:   make(map[string]*KafkaIngestionUnit),
		log:          log,
		ctx:          ctx,
		cancel:       cancel,
	}
	for _, opt := range opts {
		opt(n)
	}

	// Resolve kafka driver from config if not set via WithKafkaAdapterFactory.
	// Needed for both coordinator nodes (BatchingWriter) and dedicated Kafka
	// consumer nodes (KafkaIngestionUnit via source reconciliation).
	if n.kafkaFactory == nil {
		driverName := cfg.KafkaDriver
		if driverName == "" {
			driverName = "franzgo"
		}
		var factory kafka.AdapterFactory
		factory, err = kafka.GetDriver(driverName)
		if err != nil {
			return nil, fmt.Errorf("kafka driver: %w", err)
		}
		n.kafkaFactory = factory
	}

	// Create primary pool: use external (from WithDB) or create from config.
	var pool *sql.DB
	var poolOwned bool
	if n.hasExternalDB {
		pool = n.externalDB
	} else if cfg.Database.Primary.Host != "" {
		pool, err = db.NewPool(cfg.Database.Primary)
		if err != nil {
			return nil, fmt.Errorf("create primary pool: %w", err)
		}
		poolOwned = true
	}
	success := false
	defer func() {
		if !success && poolOwned && pool != nil {
			_ = pool.Close()
		}
	}()

	// Create replica pool: use external (from WithReadDB) or create from config.
	var readPool *sql.DB
	var readPoolOwned bool
	if n.hasExternalReadDB {
		readPool = n.externalReadDB
	} else if cfg.Database.Replica != nil && cfg.Database.Replica.Host != "" {
		readPool, err = db.NewPool(*cfg.Database.Replica)
		if err != nil {
			return nil, fmt.Errorf("create replica pool: %w", err)
		}
		readPoolOwned = true
		log.Info("replica database pool created",
			zap.String("host", cfg.Database.Replica.Host),
			zap.Int("poolSize", cfg.Database.Replica.PoolSize))
	}
	defer func() {
		if !success && readPoolOwned && readPool != nil {
			_ = readPool.Close()
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

	// Create telemetry provider (or use externally provided one)
	var telemetryProvider *telemetry.Provider

	shared := &Resources{
		DB:                 pool,
		ReadDB:             readPool,
		IsMariaDB:          isMariaDB,
		FailureLogInterval: cfg.Logging.FailureLogInterval(),
		Telemetry:          telemetryProvider,
		Log:                log,
		dbOwned:            poolOwned,
		readDBOwned:        readPoolOwned,
	}

	cr := registry.New(pool, nodeID, isMariaDB, log)

	n.shared = shared
	n.registry = cr

	// Resolve telemetry provider: use external (from WithTelemetryProvider)
	// or create from config. WithTelemetryProvider(nil) explicitly suppresses
	// config-based creation.
	if n.hasTelemetryProvider {
		telemetryProvider = n.externalTelemetry
	} else if cfg.Telemetry.Enabled {
		var err error
		telemetryProvider, err = telemetry.NewProvider(cfg.Telemetry)
		if err != nil {
			return nil, fmt.Errorf("create telemetry provider: %w", err)
		}
		log.Info("telemetry enabled", zap.String("exporter", cfg.Telemetry.Exporter))
	}
	n.shared.Telemetry = telemetryProvider

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

	// Kafka source reconciliation — runs independently of coordinator ownership
	// but requires writer + ingestSvc (which are created when coordinator is enabled).
	if n.kafkaFactory != nil && n.writer != nil && n.ingestSvc != nil {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.runKafkaSourceReconciliation()
		}()
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

	// Stop Kafka ingestion units
	n.kafkaMu.Lock()
	kafkaUnits := make(map[string]*KafkaIngestionUnit, len(n.kafkaUnits))
	for k, v := range n.kafkaUnits {
		kafkaUnits[k] = v
	}
	n.kafkaMu.Unlock()
	for key, ku := range kafkaUnits {
		ku.Stop()
		tbl, src := splitKafkaKey(key)
		if err := n.registry.ReleaseKafkaSource(context.Background(), tbl, src); err != nil {
			n.log.Warn("failed to release kafka source", zap.String("key", key), zap.Error(err))
		}
	}

	// Stop batching writer
	if n.writer != nil {
		n.writer.Stop()
	}


	// Stop health server
	if n.healthSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n.healthSrv.Stop(shutdownCtx)
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

	tableID, err := n.registry.GetTableID(n.ctx, tableName)
	if err != nil {
		return err
	}

	n.log.Info("starting coordinator",
		zap.String("table", tableName),
		zap.Bool("consolidation", tableCfg.Consolidation.Enabled),
		zap.Bool("retention", tableCfg.Retention.Enabled),
		zap.String("retentionType", tableCfg.Retention.Type),
	)

	cu, err := NewCoordinatorUnit(n.ctx, tableName, tableID, tableCfg, n.shared, n.writer, n.ingestSvc, n.log)
	if err != nil {
		return err
	}

	n.coordMu.Lock()
	if _, exists := n.coordinators[tableName]; exists {
		n.coordMu.Unlock()
		// Another goroutine started this coordinator concurrently. Stop
		// the duplicate to prevent a leaked goroutine.
		cu.Stop()
		n.log.Debug("coordinator already running, discarding duplicate", zap.String("table", tableName))
		return nil
	}
	n.coordinators[tableName] = cu
	n.coordMu.Unlock()

	cu.Start()
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
		err = n.doStartCoordinator(t)
		if err != nil {
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
		var myTables int
		myTables, err = reg.CountMyTables(ctx)
		if err != nil {
			n.log.Warn("count my tables failed", zap.Error(err))
			myTables = 0
		}
		var totalAssigned int
		totalAssigned, err = reg.CountAssignedTables(ctx)
		if err != nil {
			n.log.Warn("count assigned tables failed", zap.Error(err))
			totalAssigned = 0
		}
		totalTables := totalAssigned + len(unassigned)
		fairShare := (totalTables + activeNodes - 1) / activeNodes // ceil division

		for _, t := range unassigned {
			if myTables >= fairShare {
				n.log.Info("fair-share reached, deferring remaining tables",
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
			var ok bool
			ok, err = reg.ClaimTable(ctx, t, leaseTTL)
			if err != nil {
				n.log.Warn("claim unassigned table failed", zap.String("table", t), zap.Error(err))
				continue
			}
			if ok {
				myTables++
				err = n.doStartCoordinator(t)
				if err != nil {
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
		cu   *CoordinatorUnit
		name string
	}
	var stalled []stalledEntry
	n.coordMu.Lock()
	for name, cu := range n.coordinators {
		if cu.IsStalled() {
			n.log.Warn("coordinator stalled, restarting", zap.String("table", name))
			stalled = append(stalled, stalledEntry{cu: cu, name: name})
		}
	}
	n.coordMu.Unlock()
	for _, s := range stalled {
		err = s.cu.Restart()
		if err != nil {
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

// --- Kafka source reconciliation ---

// kafkaKeySep is the separator for kafka unit map keys. Using NUL byte
// avoids collisions with any valid table_name or source_name value.
const kafkaKeySep = "\x00"

func kafkaKey(tableName, sourceName string) string {
	return tableName + kafkaKeySep + sourceName
}

func splitKafkaKey(key string) (tableName, sourceName string) {
	idx := strings.Index(key, kafkaKeySep)
	if idx < 0 {
		return key, ""
	}
	return key[:idx], key[idx+len(kafkaKeySep):]
}

func (n *Node) runKafkaSourceReconciliation() {
	interval := time.Duration(n.cfg.Coordinator.ReconciliationIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fl := logutil.NewFailureLogger(n.log, n.shared.FailureLogInterval)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if err := n.reconcileKafkaSources(); err != nil {
				fl.Fail("kafka source reconciliation failed", zap.Error(err))
			} else {
				fl.OK()
			}
		}
	}
}

func (n *Node) reconcileKafkaSources() error {
	ctx := n.ctx

	// Reclaim orphan sources from dead nodes (heartbeat mode).
	// In lease mode, expired leases are handled by GetUnclaimedKafkaSources.
	if n.cfg.Coordinator.HAStrategy == config.HAStrategyHeartbeat {
		deadThreshold := time.Duration(n.cfg.Coordinator.DeadNodeThresholdSeconds) * time.Second
		if err := n.registry.ClaimOrphanKafkaSourcesHeartbeat(ctx, deadThreshold); err != nil {
			n.log.Warn("claim orphan kafka sources failed", zap.Error(err))
		}
	}

	// Discover unclaimed sources and claim those matching our env.
	unclaimed, err := n.registry.GetUnclaimedKafkaSources(ctx)
	if err != nil {
		return fmt.Errorf("get unclaimed kafka sources: %w", err)
	}

	var leaseTTL time.Duration
	if n.cfg.Coordinator.HAStrategy == config.HAStrategyLease {
		leaseTTL = time.Duration(n.cfg.Coordinator.LeaseTTLSeconds) * time.Second
	}

	for _, src := range unclaimed {
		if !metastore.MatchesEnv(src.RequiredEnv) {
			continue
		}
		var ok bool
		ok, err = n.registry.ClaimKafkaSource(ctx, src.TableName, src.SourceName, leaseTTL)
		if err != nil {
			n.log.Warn("claim kafka source failed",
				zap.String("table", src.TableName), zap.String("source", src.SourceName), zap.Error(err))
			continue
		}
		if ok {
			err = n.startKafkaUnit(src)
			if err != nil {
				n.log.Error("failed to start kafka unit",
					zap.String("table", src.TableName), zap.String("source", src.SourceName), zap.Error(err))
				if relErr := n.registry.ReleaseKafkaSource(ctx, src.TableName, src.SourceName); relErr != nil {
					n.log.Warn("release kafka source after start failure",
						zap.String("table", src.TableName), zap.String("source", src.SourceName), zap.Error(relErr))
				}
			}
		}
	}

	// Ownership verification: stop units for sources no longer assigned to us,
	// and start units for sources assigned but not yet running (crash recovery).
	myAssignments, err := n.registry.GetMyKafkaSources(ctx)
	if err != nil {
		return fmt.Errorf("get my kafka sources: %w", err)
	}
	assignedSet := make(map[string]bool, len(myAssignments))
	for _, a := range myAssignments {
		assignedSet[kafkaKey(a.TableName, a.SourceName)] = true
	}

	// Stop units that lost their assignment.
	n.kafkaMu.Lock()
	var toStop []string
	for key := range n.kafkaUnits {
		if !assignedSet[key] {
			toStop = append(toStop, key)
		}
	}
	n.kafkaMu.Unlock()

	for _, key := range toStop {
		n.kafkaMu.Lock()
		ku, ok := n.kafkaUnits[key]
		if ok {
			delete(n.kafkaUnits, key)
		}
		n.kafkaMu.Unlock()
		if ok {
			n.log.Warn("kafka source assignment lost, stopping", zap.String("key", key))
			ku.Stop()
		}
	}

	// Start units for assignments not yet running (e.g., after crash recovery
	// where kafkaUnits map is empty but DB assignments persist).
	for _, a := range myAssignments {
		key := kafkaKey(a.TableName, a.SourceName)
		n.kafkaMu.Lock()
		_, running := n.kafkaUnits[key]
		n.kafkaMu.Unlock()
		if !running {
			// Need to fetch the full source config to create the adapter.
			sources, err := n.registry.GetMyKafkaSourceConfigs(ctx, a.TableName, a.SourceName)
			if err != nil {
				n.log.Warn("cannot restart kafka unit: failed to fetch source config",
					zap.String("table", a.TableName), zap.String("source", a.SourceName), zap.Error(err))
				continue
			}
			if len(sources) == 0 {
				n.log.Warn("cannot restart kafka unit: source config not found in DB",
					zap.String("table", a.TableName), zap.String("source", a.SourceName))
				continue
			}
			if err := n.startKafkaUnit(sources[0]); err != nil {
				n.log.Error("failed to restart kafka unit",
					zap.String("table", a.TableName), zap.String("source", a.SourceName), zap.Error(err))
			}
		}
	}

	// Renew leases for owned sources (after ownership verification).
	if leaseTTL > 0 {
		if err := n.registry.RenewKafkaSourceLeases(ctx, leaseTTL); err != nil {
			n.log.Warn("renew kafka source leases failed", zap.Error(err))
		}
	}

	return nil
}

func (n *Node) startKafkaUnit(src *metastore.KafkaSource) error {
	key := kafkaKey(src.TableName, src.SourceName)

	n.kafkaMu.Lock()
	if _, exists := n.kafkaUnits[key]; exists {
		n.kafkaMu.Unlock()
		return nil
	}
	n.kafkaMu.Unlock()

	// Ensure writer and ingestion service exist.
	if n.writer == nil || n.ingestSvc == nil {
		return fmt.Errorf("kafka source %s: writer/ingestSvc not initialized (coordinator not enabled?)", key)
	}

	tableID, err := n.registry.GetTableID(n.ctx, src.TableName)
	if err != nil {
		return fmt.Errorf("kafka source %s: get table ID: %w", key, err)
	}

	ku, err := NewKafkaIngestionUnit(n.ctx, src.TableName, tableID, src, n.kafkaFactory, n.ingestSvc, n.log)
	if err != nil {
		return fmt.Errorf("kafka source %s: create unit: %w", key, err)
	}

	n.kafkaMu.Lock()
	if _, exists := n.kafkaUnits[key]; exists {
		n.kafkaMu.Unlock()
		// Another goroutine started this unit concurrently. Cancel the
		// context but don't call Stop() — the adapter was never started.
		ku.cancel()
		n.log.Debug("kafka unit already running, discarding duplicate", zap.String("key", key))
		return nil
	}
	n.kafkaUnits[key] = ku
	n.kafkaMu.Unlock()

	ku.Start()

	n.log.Info("kafka ingestion unit started",
		zap.String("table", src.TableName),
		zap.String("source", src.SourceName),
		zap.String("topic", src.Topic),
	)
	return nil
}
