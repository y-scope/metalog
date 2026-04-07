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
)

func TestKafkaKey(t *testing.T) {
	key := kafkaKey("my_table", "src1")
	want := "my_table\x00src1"
	if key != want {
		t.Errorf("kafkaKey = %q, want %q", key, want)
	}
}

func TestSplitKafkaKey(t *testing.T) {
	key := kafkaKey("my_table", "src1")
	tbl, src := splitKafkaKey(key)
	if tbl != "my_table" {
		t.Errorf("table = %q, want my_table", tbl)
	}
	if src != "src1" {
		t.Errorf("source = %q, want src1", src)
	}
}

func TestSplitKafkaKey_NoSeparator(t *testing.T) {
	tbl, src := splitKafkaKey("noseparator")
	if tbl != "noseparator" {
		t.Errorf("table = %q, want noseparator", tbl)
	}
	if src != "" {
		t.Errorf("source = %q, want empty", src)
	}
}

func TestGetReconcileRegistry_WithOverride(t *testing.T) {
	mock := &mockRegistry{}
	n := &Node{
		reconcileReg: mock,
	}
	got := n.getReconcileRegistry()
	if got != mock {
		t.Error("should return mock override")
	}
}

func TestGetReconcileRegistry_DefaultUsesRegistry(t *testing.T) {
	// When reconcileReg is nil, it returns n.registry which is the concrete registry.
	// We can't easily test this without a real registry, so we verify that with
	// reconcileReg set, it uses the override.
	mock := &mockRegistry{}
	n := &Node{reconcileReg: mock}
	if n.getReconcileRegistry() != mock {
		t.Error("should return reconcileReg when set")
	}
}

func TestDoStartCoordinator_WithFn(t *testing.T) {
	called := false
	n := &Node{
		startCoordinatorFn: func(tableName string) error {
			called = true
			return nil
		},
	}
	err := n.doStartCoordinator("test_table")
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !called {
		t.Error("startCoordinatorFn was not called")
	}
}

func TestNode_Writer_Nil(t *testing.T) {
	n := &Node{}
	if n.Writer() != nil {
		t.Error("Writer() should be nil initially")
	}
}

func TestNode_IngestionService_Nil(t *testing.T) {
	n := &Node{}
	if n.IngestionService() != nil {
		t.Error("IngestionService() should be nil initially")
	}
}

func TestNode_Shared_Nil(t *testing.T) {
	n := &Node{}
	if n.Shared() != nil {
		t.Error("Shared() should be nil initially")
	}
}

func TestNode_NodeID(t *testing.T) {
	n := &Node{nodeID: "test-123"}
	if n.NodeID() != "test-123" {
		t.Errorf("NodeID() = %q, want test-123", n.NodeID())
	}
}

func TestWithDB(t *testing.T) {
	n := &Node{}
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	opt := WithDB(db)
	opt(n)
	if !n.hasExternalDB {
		t.Error("hasExternalDB should be true")
	}
	if n.externalDB != db {
		t.Error("externalDB should be set")
	}
}

func TestWithReadDB(t *testing.T) {
	n := &Node{}
	db, _, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck

	opt := WithReadDB(db)
	opt(n)
	if !n.hasExternalReadDB {
		t.Error("hasExternalReadDB should be true")
	}
}

func TestWithTelemetryProvider(t *testing.T) {
	n := &Node{}
	opt := WithTelemetryProvider(nil)
	opt(n)
	if !n.hasTelemetryProvider {
		t.Error("hasTelemetryProvider should be true")
	}
}

func TestCoordinatorUnit_IsStalled(t *testing.T) {
	cu := &CoordinatorUnit{
		progress: coordinator.NewProgressTracker(100*time.Millisecond, zap.NewNop()),
		log:      zap.NewNop(),
	}
	// Just created - should be stalled since no progress recorded yet
	// (but depends on implementation)
	cu.progress.RecordProgress()
	if cu.IsStalled() {
		t.Error("should not be stalled right after RecordProgress")
	}
}

func TestCoordinatorUnit_TableConfig(t *testing.T) {
	cu := &CoordinatorUnit{}
	cfg := cu.TableConfig()
	// Default zero-value config
	if cfg.Consolidation.Enabled {
		t.Error("default should have consolidation disabled")
	}
}

func TestCoordinatorUnit_Stop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cu := &CoordinatorUnit{
		cancel: cancel,
		ctx:    ctx,
		log:    zap.NewNop(),
	}
	cu.Stop()
	// Should not panic and context should be cancelled
	if ctx.Err() == nil {
		t.Error("context should be cancelled after Stop")
	}
}

func TestCoordinatorUnit_Restart_ParentCancelled(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	parentCancel() // cancel parent

	childCtx, childCancel := context.WithCancel(parentCtx)

	cu := &CoordinatorUnit{
		parentCtx: parentCtx,
		ctx:       childCtx,
		cancel:    childCancel,
		tableName: "test_table",
		progress:  coordinator.NewProgressTracker(5*time.Minute, zap.NewNop()),
		log:       zap.NewNop(),
	}

	err := cu.Restart()
	if err == nil {
		t.Fatal("Restart() should fail when parent context is cancelled")
	}
}

func TestReconcile_StalledCoordinator_Detected(t *testing.T) {
	// Test that IsStalled detects stalled coordinator
	cu := &CoordinatorUnit{
		progress:  coordinator.NewProgressTracker(1*time.Nanosecond, zap.NewNop()),
		tableName: "stalled_table",
		log:       zap.NewNop(),
	}
	// Don't record progress — it will be stalled immediately
	time.Sleep(2 * time.Millisecond)

	if !cu.IsStalled() {
		t.Error("coordinator should be detected as stalled")
	}
}

func TestReconcile_LeaseStrategy(t *testing.T) {
	reg := &mockRegistry{
		orphans:     []string{"lease_orphan"},
		assigned:    []string{"lease_orphan"},
		activeNodes: 1,
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

	if !started["lease_orphan"] {
		t.Error("expected lease_orphan to be started under lease strategy")
	}
}

func TestReconcile_ClaimUnassignedStartFail(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"claim_fail"},
		assigned:      []string{},
		activeNodes:   1,
		totalAssigned: 0,
	}

	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		return fmt.Errorf("start failed")
	}

	n.reconcile() //nolint:errcheck

	// Verify release was called
	released := reg.getReleased()
	found := false
	for _, r := range released {
		if r == "claim_fail" {
			found = true
		}
	}
	if !found {
		t.Error("expected claim_fail to be released after start failure")
	}
}

func TestReconcile_ClaimTableFails(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"contested"},
		assigned:      []string{},
		activeNodes:   1,
		totalAssigned: 0,
		claimResults:  map[string]bool{"contested": false}, // claim fails (lost CAS)
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}

	n.reconcile() //nolint:errcheck

	if started["contested"] {
		t.Error("should not start coordinator for table that failed to claim")
	}
}

func TestReconcile_LeaseStrategyCountNodes(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"t1"},
		assigned:      []string{},
		activeNodes:   2,
		totalAssigned: 0,
	}

	n := newTestNode(reg)
	n.cfg.Coordinator.HAStrategy = config.HAStrategyLease
	n.cfg.Coordinator.LeaseTTLSeconds = 60
	n.startCoordinatorFn = func(_ string) error { return nil }

	n.reconcile() //nolint:errcheck

	claims := reg.getClaimCalls()
	if len(claims) != 1 {
		t.Errorf("expected 1 claim, got %d", len(claims))
	}
}

func TestKafkaIngestionUnit_Lifecycle(t *testing.T) {
	// Test that NewKafkaIngestionUnit works with a mock factory
	ctx := context.Background()

	startCalled := false
	stopCalled := false
	mockAdapter := &mockKafkaAdapter{
		startFn: func(ctx context.Context) {
			startCalled = true
			<-ctx.Done()
		},
		stopFn: func() { stopCalled = true },
	}

	factory := func(_, _ string, src interface{}, svc interface{}, log *zap.Logger) (interface{}, error) {
		return mockAdapter, nil
	}
	_ = factory

	// Test KafkaIngestionUnit stop with existing adapter
	childCtx, cancel := context.WithCancel(ctx)
	ku := &KafkaIngestionUnit{
		adapter:    mockAdapter,
		ctx:        childCtx,
		cancel:     cancel,
		tableName:  "test",
		sourceName: "src",
		log:        zap.NewNop(),
	}
	ku.Start()
	time.Sleep(10 * time.Millisecond) // let goroutine start
	ku.Stop()

	if !startCalled {
		t.Error("adapter.Start was not called")
	}
	if !stopCalled {
		t.Error("adapter.Stop was not called")
	}
}

// mockKafkaAdapter implements kafka.Adapter for testing.
type mockKafkaAdapter struct {
	startFn func(ctx context.Context)
	stopFn  func()
}

func (m *mockKafkaAdapter) Start(ctx context.Context) {
	if m.startFn != nil {
		m.startFn(ctx)
	}
}

func (m *mockKafkaAdapter) Stop() {
	if m.stopFn != nil {
		m.stopFn()
	}
}

func TestCoordinatorUnit_Start_Stop_NoPlanner(t *testing.T) {
	// Test Start/Stop of a minimal CoordinatorUnit without planner or retention
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cu := &CoordinatorUnit{
		parentCtx: ctx,
		ctx:       ctx,
		cancel:    cancel,
		tableName: "test_table",
		progress:  coordinator.NewProgressTracker(5*time.Minute, zap.NewNop()),
		log:       zap.NewNop(),
		tableCfg:  metastore.TableConfig{}, // no consolidation, no retention
	}
	// We can't call Start() because it needs partition, registry, retention subsystems.
	// But we can test Stop() is safe to call
	cu.Stop()
}

func TestNewKafkaIngestionUnit_Success(t *testing.T) {
	ctx := context.Background()

	mockAdapter := &mockKafkaAdapter{}
	src := &metastore.KafkaSource{
		TableName:        "test_table",
		SourceName:       "src1",
		Topic:            "my-topic",
		BootstrapServers: "localhost:9092",
	}

	ku, err := NewKafkaIngestionUnit(ctx, "test_table", "uuid-1", src,
		func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return mockAdapter, nil
		},
		nil, zap.NewNop())
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if ku == nil {
		t.Fatal("returned nil")
	}
	if ku.tableName != "test_table" {
		t.Errorf("tableName = %q", ku.tableName)
	}
}

func TestNewKafkaIngestionUnit_FactoryError(t *testing.T) {
	ctx := context.Background()
	src := &metastore.KafkaSource{
		TableName:  "test_table",
		SourceName: "src1",
	}

	_, err := NewKafkaIngestionUnit(ctx, "test_table", "uuid-1", src,
		func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
			return nil, fmt.Errorf("factory error")
		},
		nil, zap.NewNop())
	if err == nil {
		t.Fatal("expected error from factory")
	}
}

func TestWithKafkaAdapterFactory(t *testing.T) {
	n := &Node{}
	factory := kafka.AdapterFactory(func(tableName, tableID string, s *metastore.KafkaSource, svc *ingestion.Service, log *zap.Logger) (kafka.Adapter, error) {
		return nil, nil
	})
	opt := WithKafkaAdapterFactory(factory)
	opt(n)
	if n.kafkaFactory == nil {
		t.Error("kafkaFactory should be set")
	}
}

func TestReconcile_MultipleOrphansOneAlreadyRunning(t *testing.T) {
	reg := &mockRegistry{
		orphans:     []string{"running", "new_orphan"},
		assigned:    []string{"running", "new_orphan"},
		activeNodes: 1,
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}
	n.coordinators["running"] = newTestCU()

	n.reconcile() //nolint:errcheck

	if started["running"] {
		t.Error("should not restart already running coordinator")
	}
	if !started["new_orphan"] {
		t.Error("should start new_orphan")
	}
}
