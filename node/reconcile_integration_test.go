package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator"
)

// mockRegistry implements reconcileRegistry for testing.
type mockRegistry struct {
	mu               sync.Mutex
	orphans          []string
	unassigned       []string
	assigned         []string
	activeNodes      int
	myTables         int
	totalAssigned    int
	claimResults     map[string]bool // table → claim success
	released         []string
	claimTableCalls  []string
}

func (m *mockRegistry) ClaimOrphansLease(_ context.Context, _ time.Duration) ([]string, error) {
	return m.orphans, nil
}
func (m *mockRegistry) ClaimOrphansHeartbeat(_ context.Context, _ time.Duration) ([]string, error) {
	return m.orphans, nil
}
func (m *mockRegistry) GetUnassignedTables(_ context.Context) ([]string, error) {
	return m.unassigned, nil
}
func (m *mockRegistry) CountActiveNodesLease(_ context.Context) (int, error) {
	return m.activeNodes, nil
}
func (m *mockRegistry) CountActiveNodesHeartbeat(_ context.Context, _ time.Duration) (int, error) {
	return m.activeNodes, nil
}
func (m *mockRegistry) CountMyTables(_ context.Context) (int, error) {
	return m.myTables, nil
}
func (m *mockRegistry) CountAssignedTables(_ context.Context) (int, error) {
	return m.totalAssigned, nil
}
func (m *mockRegistry) ClaimTable(_ context.Context, tableName string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimTableCalls = append(m.claimTableCalls, tableName)
	if m.claimResults != nil {
		return m.claimResults[tableName], nil
	}
	return true, nil
}
func (m *mockRegistry) GetAssignedTables(_ context.Context) ([]string, error) {
	return m.assigned, nil
}
func (m *mockRegistry) ReleaseTable(_ context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, tableName)
	return nil
}
func (m *mockRegistry) getClaimCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.claimTableCalls))
	copy(result, m.claimTableCalls)
	return result
}
func (m *mockRegistry) getReleased() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.released))
	copy(result, m.released)
	return result
}

// newTestCU creates a minimal CoordinatorUnit for reconciliation testing.
// The progress tracker is initialized so IsStalled() returns false.
// The context and cancel are set so Stop() doesn't panic.
func newTestCU() *CoordinatorUnit {
	_, cancel := context.WithCancel(context.Background())
	cu := &CoordinatorUnit{
		progress: coordinator.NewProgressTracker(5*time.Minute, zap.NewNop()),
		cancel:   cancel,
		log:      zap.NewNop(),
	}
	cu.progress.RecordProgress() // mark as recently active
	return cu
}

// newTestNode creates a minimal Node for reconciliation testing.
func newTestNode(reg *mockRegistry) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(map[string]bool)
	n := &Node{
		cfg: &config.NodeConfig{
			Coordinator: config.CoordinatorConfig{
				HAStrategy:               config.HAStrategyHeartbeat,
				DeadNodeThresholdSeconds: 60,
			},
		},
		nodeID:       "test-node",
		coordinators: make(map[string]*CoordinatorUnit),
		log:          zap.NewNop(),
		ctx:          ctx,
		cancel:       cancel,
		reconcileReg: reg,
		startCoordinatorFn: func(tableName string) error {
			started[tableName] = true
			return nil
		},
	}
	// Expose started map via closure for assertions
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}
	return n
}

func TestReconcile_ClaimsOrphansAndStartsCoordinators(t *testing.T) {
	reg := &mockRegistry{
		orphans:    []string{"orphan_a", "orphan_b"},
		assigned:   []string{"orphan_a", "orphan_b"},
		activeNodes: 1,
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}

	n.reconcile()

	if !started["orphan_a"] {
		t.Error("expected orphan_a to be started")
	}
	if !started["orphan_b"] {
		t.Error("expected orphan_b to be started")
	}
}

func TestReconcile_SkipsAlreadyRunningOrphans(t *testing.T) {
	reg := &mockRegistry{
		orphans:    []string{"already_running"},
		assigned:   []string{"already_running"},
		activeNodes: 1,
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}
	// Pre-populate as already running
	n.coordinators["already_running"] = newTestCU()

	n.reconcile()

	if started["already_running"] {
		t.Error("should not restart already running coordinator")
	}
}

func TestReconcile_FairShareLimitsClaims(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"t1", "t2", "t3", "t4", "t5", "t6"},
		assigned:      []string{},
		activeNodes:   3,
		myTables:      0,
		totalAssigned: 0,
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}

	n.reconcile()

	// fairShare = ceil(6/3) = 2; should claim at most 2
	claimed := reg.getClaimCalls()
	if len(claimed) > 2 {
		t.Errorf("claimed %d tables, want <= 2 (fair share)", len(claimed))
	}
}

func TestReconcile_FairShareAlreadyAtLimit(t *testing.T) {
	reg := &mockRegistry{
		unassigned:    []string{"t1", "t2"},
		assigned:      []string{},
		activeNodes:   2,
		myTables:      3, // already over fair share
		totalAssigned: 4,
	}

	n := newTestNode(reg)
	n.startCoordinatorFn = func(_ string) error { return nil }

	n.reconcile()

	// fairShare = ceil(6/2) = 3; myTables already 3 → should claim 0
	claimed := reg.getClaimCalls()
	if len(claimed) != 0 {
		t.Errorf("claimed %d tables, want 0 (already at fair share)", len(claimed))
	}
}

func TestReconcile_OwnershipVerification_StopsLostAssignment(t *testing.T) {
	reg := &mockRegistry{
		assigned: []string{"table_a"}, // only table_a assigned
	}

	stopped := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(_ string) error { return nil }

	// table_b is running but not assigned — should be stopped
	n.coordinators["table_a"] = newTestCU()
	n.coordinators["table_b"] = newTestCU()

	// We can't easily call Stop() on a zero-value CoordinatorUnit, so track
	// the ownership verification logic by checking the coordinators map after
	// reconcile. The Stop() call on a zero-value CU is a no-op (no goroutines).
	n.reconcile()

	if _, running := n.coordinators["table_b"]; running {
		stopped["table_b"] = false
	} else {
		stopped["table_b"] = true
	}

	if !stopped["table_b"] {
		t.Error("table_b should be removed from coordinators after losing assignment")
	}
	if _, running := n.coordinators["table_a"]; !running {
		t.Error("table_a should still be running")
	}
}

func TestReconcile_OwnershipVerification_StartsNewAssignment(t *testing.T) {
	reg := &mockRegistry{
		assigned: []string{"existing", "new_table"},
	}

	started := map[string]bool{}
	n := newTestNode(reg)
	n.startCoordinatorFn = func(tableName string) error {
		started[tableName] = true
		return nil
	}
	n.coordinators["existing"] = newTestCU()

	n.reconcile()

	if !started["new_table"] {
		t.Error("new_table should be started")
	}
	if started["existing"] {
		t.Error("existing should not be restarted")
	}
}
