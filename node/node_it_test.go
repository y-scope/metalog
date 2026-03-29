package node

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/testutil"
)

func TestIT_NewNode_RealMariaDB(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			Enabled:                  true,
			HAStrategy:               config.HAStrategyHeartbeat,
			HeartbeatIntervalSeconds: 30,
			DeadNodeThresholdSeconds: 180,
		},
	}

	n, err := NewNode(cfg, zap.NewNop(), WithDB(mc.DB))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.nodeID == "" {
		t.Error("nodeID should not be empty")
	}
}

func TestIT_Start_CoordinatorEnabled_NoTables(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			Enabled:                       true,
			HAStrategy:                    config.HAStrategyHeartbeat,
			HeartbeatIntervalSeconds:      1,
			DeadNodeThresholdSeconds:      10,
			ReconciliationIntervalSeconds: 1,
		},
	}

	n, err := NewNode(cfg, zap.NewNop(), WithDB(mc.DB))
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	n.Stop()
}

func TestIT_Stop_WithActiveCoordinator(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, "stop_test")

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			Enabled:                       true,
			HAStrategy:                    config.HAStrategyHeartbeat,
			HeartbeatIntervalSeconds:      1,
			DeadNodeThresholdSeconds:      10,
			ReconciliationIntervalSeconds: 1,
		},
	}

	n, err := NewNode(cfg, zap.NewNop(), WithDB(mc.DB))
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	// Let reconciliation claim the table
	time.Sleep(2 * time.Second)

	n.Stop()

	// Stop should complete without panic or hang.
	// Coordinator map may retain entries (ownership released in DB, not map).
}

func TestIT_DeregisterNode(t *testing.T) {
	mc := testutil.SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)

	cfg := &config.NodeConfig{
		Coordinator: config.CoordinatorConfig{
			Enabled:                       true,
			HAStrategy:                    config.HAStrategyHeartbeat,
			HeartbeatIntervalSeconds:      1,
			DeadNodeThresholdSeconds:      10,
			ReconciliationIntervalSeconds: 60,
		},
	}

	n, err := NewNode(cfg, zap.NewNop(), WithDB(mc.DB))
	if err != nil {
		t.Fatal(err)
	}
	if startErr := n.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	n.Stop()

	// Verify node was deregistered
	var count int
	err = mc.DB.QueryRow("SELECT COUNT(*) FROM _node_registry WHERE node_id = ?", n.nodeID).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("node should be deregistered, got count=%d", count)
	}
}
