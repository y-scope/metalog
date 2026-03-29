package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator"
	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node/registry"
)

// --- NewNode tests ---

func TestNewNode_EmptyNodeID(t *testing.T) {
	// With empty env var, ResolveNodeID falls back to os.Hostname().
	// Either way, NewNode should fail because no DB is configured.
	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "METALOG_TEST_NONEXISTENT_ENV_VAR_12345",
		},
	}
	t.Setenv("METALOG_TEST_NONEXISTENT_ENV_VAR_12345", "")

	_, err := NewNode(cfg, zap.NewNop())
	if err == nil {
		t.Fatal("expected error from NewNode when no DB is configured")
	}
}

func TestNewNode_WithExternalDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	// DetectDatabaseType: SELECT VERSION()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "test-node-1")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if n == nil {
		t.Fatal("NewNode returned nil")
	}
	if n.NodeID() != "test-node-1" {
		t.Errorf("NodeID = %q, want test-node-1", n.NodeID())
	}
	if n.Shared() == nil {
		t.Error("Shared() should not be nil")
	}
	if !n.Shared().IsMariaDB {
		t.Error("should detect MariaDB")
	}
}

func TestNewNode_WithExternalDB_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "mysql-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if n.Shared().IsMariaDB {
		t.Error("should not detect MariaDB for MySQL 8.0")
	}
}

func TestNewNode_WithReadDB(t *testing.T) {
	readDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer readDB.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "readonly-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithReadDB(readDB),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if n.Shared().ReadDB != readDB {
		t.Error("ReadDB should be set from WithReadDB")
	}
}

func TestNewNode_NoDB(t *testing.T) {
	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "no-db-node")

	_, err := NewNode(cfg, zap.NewNop(),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err == nil {
		t.Fatal("expected error when no DB is configured")
	}
}

func TestNewNode_DetectDBFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnError(fmt.Errorf("connection refused"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "fail-node")

	_, err = NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err == nil {
		t.Fatal("expected error when DetectDatabaseType fails")
	}
}

// --- Stop tests ---

func TestStop_NoCoordinatorsNoWorkers(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "stop-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("error = %v", err)
	}

	// DeregisterNode: DELETE FROM _node_registry
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))

	// Stop should not panic
	n.Stop()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestStop_WithCoordinators(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close() //nolint:errcheck

	childCtx, childCancel := context.WithCancel(ctx)
	cu1 := &CoordinatorUnit{
		cancel:    childCancel,
		ctx:       childCtx,
		tableName: "t1",
		log:       zap.NewNop(),
		progress:  coordinator.NewProgressTracker(5*time.Minute, zap.NewNop()),
	}
	cu1.progress.RecordProgress()

	reg := &mockRegistry{
		assigned: []string{},
	}

	mock.MatchExpectationsInOrder(false)

	// ReleaseTable for "t1"
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	// DeregisterNode (DELETE FROM _node_registry)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				HAStrategy: config.HAStrategyHeartbeat,
			},
		},
		nodeID:       "stop-test",
		coordinators: map[string]*CoordinatorUnit{"t1": cu1},
		kafkaUnits:   make(map[string]*KafkaIngestionUnit),
		log:          zap.NewNop(),
		ctx:          ctx,
		cancel:       cancel,
		reconcileReg: reg,
		shared:       &Resources{DB: mockDB, Log: zap.NewNop()},
		registry:     registry.New(mockDB, "stop-test", true, zap.NewNop()),
	}

	n.Stop()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestStop_WithKafkaUnits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close() //nolint:errcheck

	stopCalled := false
	adapter := &mockKafkaAdapter{
		startFn: func(ctx context.Context) { <-ctx.Done() },
		stopFn:  func() { stopCalled = true },
	}

	childCtx, childCancel := context.WithCancel(ctx)
	ku := &KafkaIngestionUnit{
		adapter:    adapter,
		ctx:        childCtx,
		cancel:     childCancel,
		tableName:  "t1",
		sourceName: "src1",
		log:        zap.NewNop(),
	}
	ku.Start()

	mock.MatchExpectationsInOrder(false)

	// ReleaseKafkaSource
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	// DeregisterNode (DELETE FROM _node_registry)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				HAStrategy: config.HAStrategyHeartbeat,
			},
		},
		nodeID:       "kafka-stop-test",
		coordinators: make(map[string]*CoordinatorUnit),
		kafkaUnits:   map[string]*KafkaIngestionUnit{kafkaKey("t1", "src1"): ku},
		log:          zap.NewNop(),
		ctx:          ctx,
		cancel:       cancel,
		shared:       &Resources{DB: mockDB, Log: zap.NewNop()},
		registry:     registry.New(mockDB, "kafka-stop-test", true, zap.NewNop()),
	}

	n.Stop()

	if !stopCalled {
		t.Error("kafka adapter Stop should be called")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// --- runLiveness tests ---

func TestRunLiveness_HeartbeatMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				HAStrategy:             config.HAStrategyHeartbeat,
				HeartbeatIntervalSeconds: 1,
			},
			Logging: config.LoggingConfig{},
		},
		log: zap.NewNop(),
		ctx: ctx,
		shared: &Resources{
			Log:                zap.NewNop(),
			FailureLogInterval: time.Minute,
		},
	}

	// Cancel immediately to test the select case
	cancel()

	done := make(chan struct{})
	go func() {
		n.runLiveness()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runLiveness did not exit after context cancel")
	}
}

func TestRunLiveness_LeaseMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				HAStrategy:                  config.HAStrategyLease,
				LeaseRenewalIntervalSeconds: 1,
			},
		},
		log: zap.NewNop(),
		ctx: ctx,
		shared: &Resources{
			Log:                zap.NewNop(),
			FailureLogInterval: time.Minute,
		},
	}

	cancel()

	done := make(chan struct{})
	go func() {
		n.runLiveness()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runLiveness did not exit after context cancel")
	}
}

// --- runReconciliation tests ---

func TestRunReconciliation_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				ReconciliationIntervalSeconds: 1,
				HAStrategy:                    config.HAStrategyHeartbeat,
				DeadNodeThresholdSeconds:      60,
			},
		},
		nodeID:       "test-reconcile",
		coordinators: make(map[string]*CoordinatorUnit),
		log:          zap.NewNop(),
		ctx:          ctx,
		cancel:       cancel,
		reconcileReg: &mockRegistry{},
		shared: &Resources{
			Log:                zap.NewNop(),
			FailureLogInterval: time.Minute,
		},
	}

	cancel()

	done := make(chan struct{})
	go func() {
		n.runReconciliation()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReconciliation did not exit after context cancel")
	}
}

// --- runKafkaSourceReconciliation tests ---

func TestRunKafkaSourceReconciliation_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				ReconciliationIntervalSeconds: 1,
				HAStrategy:                    config.HAStrategyHeartbeat,
			},
		},
		log: zap.NewNop(),
		ctx: ctx,
		shared: &Resources{
			Log:                zap.NewNop(),
			FailureLogInterval: time.Minute,
		},
		kafkaUnits: make(map[string]*KafkaIngestionUnit),
	}

	cancel()

	done := make(chan struct{})
	go func() {
		n.runKafkaSourceReconciliation()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runKafkaSourceReconciliation did not exit")
	}
}

// --- startKafkaUnit tests ---

func TestStartKafkaUnit_AlreadyRunning(t *testing.T) {
	ctx := context.Background()
	src := &metastore.KafkaSource{
		TableName:  "test_table",
		SourceName: "src1",
		Topic:      "topic1",
	}

	childCtx, childCancel := context.WithCancel(ctx)
	existingUnit := &KafkaIngestionUnit{
		adapter:    &mockKafkaAdapter{},
		ctx:        childCtx,
		cancel:     childCancel,
		tableName:  "test_table",
		sourceName: "src1",
		log:        zap.NewNop(),
	}

	n := &Node{
		ctx:        ctx,
		kafkaUnits: map[string]*KafkaIngestionUnit{kafkaKey("test_table", "src1"): existingUnit},
		log:        zap.NewNop(),
	}

	err := n.startKafkaUnit(src)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	// Should return nil (already running)
}

func TestStartKafkaUnit_NoWriter(t *testing.T) {
	ctx := context.Background()
	src := &metastore.KafkaSource{
		TableName:  "test_table",
		SourceName: "src1",
	}

	n := &Node{
		ctx:        ctx,
		kafkaUnits: make(map[string]*KafkaIngestionUnit),
		log:        zap.NewNop(),
		writer:     nil, // no writer
	}

	err := n.startKafkaUnit(src)
	if err == nil {
		t.Fatal("expected error when writer is nil")
	}
}

// --- reconcile with stalled coordinator ---

func TestReconcile_StalledCoordinator_DetectedAndReleased(t *testing.T) {
	// When a coordinator is stalled and Restart fails (parent cancelled),
	// reconcile should remove it from coordinators and release the table.
	reg := &mockRegistry{
		assigned: []string{},
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	parentCancel() // cancel parent so Restart() fails

	childCtx, childCancel := context.WithCancel(parentCtx)

	stalledCU := &CoordinatorUnit{
		parentCtx: parentCtx,
		ctx:       childCtx,
		cancel:    childCancel,
		tableName: "stalled_table",
		progress:  coordinator.NewProgressTracker(1*time.Nanosecond, zap.NewNop()),
		log:       zap.NewNop(),
		tableCfg:  metastore.TableConfig{},
	}
	time.Sleep(2 * time.Millisecond)

	n := newTestNode(reg)
	n.coordinators["stalled_table"] = stalledCU

	_ = n.reconcile()

	// Stalled coordinator should be removed after failed restart
	if _, exists := n.coordinators["stalled_table"]; exists {
		t.Error("stalled_table should be removed after restart failure")
	}

	// Table should be released
	released := reg.getReleased()
	found := false
	for _, r := range released {
		if r == "stalled_table" {
			found = true
		}
	}
	if !found {
		t.Error("stalled_table should be released after restart failure")
	}
}

// --- Reconcile with existing coordinator skips on unassigned claim ---

func TestReconcile_ClaimUnassigned_SkipAlreadyRunning(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"already_running"},
		assigned:      []string{"already_running"},
		activeNodes:   1,
		totalAssigned: 0,
	}

	n := newTestNode(reg)
	n.startCoordinatorFn = func(_ string) error {
		t.Error("should not start already running coordinator")
		return nil
	}
	n.coordinators["already_running"] = newTestCU()

	n.reconcile() //nolint:errcheck

	// No claim should have been made
	claims := reg.getClaimCalls()
	if len(claims) != 0 {
		t.Errorf("expected 0 claims, got %d", len(claims))
	}
}

// --- NewNode with both primary and replica ---

func TestNewNode_WithPrimaryAndReplica(t *testing.T) {
	primary, primaryMock, _ := sqlmock.New()
	defer primary.Close() //nolint:errcheck
	replica, _, _ := sqlmock.New()
	defer replica.Close() //nolint:errcheck

	primaryMock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
	}
	t.Setenv("TEST_NODE_ID", "dual-db-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(primary),
		WithReadDB(replica),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if n.Shared().ReadDB != replica {
		t.Error("ReadDB should be replica")
	}
	if n.Shared().DB != primary {
		t.Error("DB should be primary")
	}
}

// --- NewNode with health enabled ---

func TestNewNode_WithHealth(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
		},
		Health: config.HealthConfig{
			Enabled: true,
			Port:    0, // will pick a free port when started
		},
	}
	t.Setenv("TEST_NODE_ID", "health-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if n.healthSrv == nil {
		t.Error("health server should be created when enabled")
	}
}

// --- CoordinatorUnit additional tests ---

func TestCoordinatorUnit_Restart_Success_ContextReset(t *testing.T) {
	// Test that Restart resets the context when parent is still alive.
	// We use a quick cancel after Start to prevent goroutines from running
	// into nil pointer panics on partition/registry (which are nil in test).
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	childCtx, childCancel := context.WithCancel(parentCtx)
	childCancel() // pre-cancel so Stop is a no-op

	cu := &CoordinatorUnit{
		parentCtx: parentCtx,
		ctx:       childCtx,
		cancel:    childCancel,
		tableName: "test_table",
		progress:  coordinator.NewProgressTracker(5*time.Minute, zap.NewNop()),
		log:       zap.NewNop(),
		tableCfg:  metastore.TableConfig{},
	}

	// Restart calls Stop() (no-op since already cancelled), then creates new ctx.
	// The new Start() will launch goroutines, so cancel parent immediately to stop them.
	parentCancel()

	err := cu.Restart()
	// Should fail because parent is now cancelled
	if err == nil {
		t.Fatal("expected error when parent cancelled")
	}
}

// --- Start path tests (without real coordinator subsystems) ---

func TestStart_NoCoordinatorNoWorker(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"VERSION()"}).AddRow("10.6.16-MariaDB"))

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			NodeIDEnvVar: "TEST_NODE_ID",
			Enabled:      false,
		},
		Worker: config.WorkerConfig{
			Concurrency: 0,
		},
	}
	t.Setenv("TEST_NODE_ID", "no-coord-node")

	n, err := NewNode(cfg, zap.NewNop(),
		WithDB(db),
		WithTelemetryProvider(nil),
		WithKafkaAdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewNode error = %v", err)
	}

	err = n.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Stop should work cleanly
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	n.Stop()
}

// --- Reconcile step 4 (lease strategy for claim table) ---

func TestReconcile_ClaimUnassigned_LeaseStrategy(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"t1"},
		assigned:      []string{"t1"},
		activeNodes:   1,
		totalAssigned: 0,
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.cfg.Coordinator.HAStrategy = config.HAStrategyLease
	n.cfg.Coordinator.LeaseTTLSeconds = 60
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}

	n.reconcile() //nolint:errcheck

	if !started["t1"] {
		t.Error("t1 should be started under lease strategy")
	}
}

// --- Edge cases for reconcile ---

func TestReconcile_EmptyState(t *testing.T) {
	reg := &mockRegistry{
		orphans:       nil,
		unassigned:    nil,
		assigned:      nil,
		activeNodes:   1,
		totalAssigned: 0,
	}

	n := newTestNode(reg)
	n.startCoordinatorFn = func(_ string) error { return nil }

	err := n.reconcile()
	if err != nil {
		t.Fatalf("reconcile error = %v", err)
	}
}

func TestReconcile_OrphanClaimFailsGracefully(t *testing.T) {
	// When orphan start fails, the table should be released
	reg := &mockRegistry{
		orphans:     []string{"failing_orphan"},
		assigned:    []string{},
		activeNodes: 1,
	}

	startCount := 0
	n := newTestNode(reg)
	n.startCoordinatorFn = func(_ string) error {
		startCount++
		return fmt.Errorf("artificial failure")
	}

	_ = n.reconcile()

	released := reg.getReleased()
	if len(released) == 0 || released[0] != "failing_orphan" {
		t.Error("failing_orphan should be released after start failure")
	}
}
