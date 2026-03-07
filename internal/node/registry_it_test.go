//go:build integration

package node_test

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/node"
	"github.com/y-scope/metalog/internal/testutil"
)

func setupRegistryIT(t *testing.T) (*testutil.MariaDBContainer, *node.CoordinatorRegistry) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)

	log := zap.NewNop()
	cr := node.NewCoordinatorRegistry(mc.DB, "node-1", true, log)
	return mc, cr
}

func TestCoordinatorRegistry_EnsureSystemTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// EnsureSystemTables is already handled by LoadSchema, but calling it
	// again should be idempotent
	err := cr.EnsureSystemTables(ctx)
	if err != nil {
		t.Fatalf("EnsureSystemTables() error = %v", err)
	}
}

func TestCoordinatorRegistry_UpsertTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	tables := []config.TableConfig{
		{Name: "logs_app", DisplayName: "Application Logs", Kafka: config.TableKafkaConfig{
			Topic: "app-ir", BootstrapServers: "kafka:9092",
		}},
		{Name: "logs_infra", DisplayName: "Infrastructure Logs", Kafka: config.TableKafkaConfig{
			Topic: "infra-ir", BootstrapServers: "kafka:9092",
		}},
	}

	err := cr.UpsertTables(ctx, tables)
	if err != nil {
		t.Fatalf("UpsertTables() error = %v", err)
	}

	// Verify tables registered
	allTables, err := cr.GetAllRegisteredTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(allTables) != 2 {
		t.Errorf("registered tables = %d, want 2", len(allTables))
	}

	// Idempotent: call again
	err = cr.UpsertTables(ctx, tables)
	if err != nil {
		t.Fatalf("UpsertTables() second call error = %v", err)
	}
}

func TestCoordinatorRegistry_ClaimAndRelease(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Register a table first
	cr.UpsertTables(ctx, []config.TableConfig{
		{Name: "claim_test", DisplayName: "Claim Test"},
	})

	// Claim
	claimed, err := cr.ClaimTable(ctx, "claim_test")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Error("ClaimTable() = false, want true")
	}

	// Claiming again should fail (already claimed by this node)
	claimed2, err := cr.ClaimTable(ctx, "claim_test")
	if err != nil {
		t.Fatal(err)
	}
	if claimed2 {
		t.Error("ClaimTable() second call = true, want false (already claimed)")
	}

	// Release
	err = cr.ReleaseTable(ctx, "claim_test")
	if err != nil {
		t.Fatal(err)
	}

	// Now it should be unassigned
	unassigned, err := cr.GetUnassignedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, name := range unassigned {
		if name == "claim_test" {
			found = true
		}
	}
	if !found {
		t.Error("claim_test should be unassigned after release")
	}
}

func TestCoordinatorRegistry_GetAssignedTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	cr.UpsertTables(ctx, []config.TableConfig{
		{Name: "assigned_a", DisplayName: "A"},
		{Name: "assigned_b", DisplayName: "B"},
		{Name: "unassigned_c", DisplayName: "C"},
	})

	cr.ClaimTable(ctx, "assigned_a")
	cr.ClaimTable(ctx, "assigned_b")

	assigned, err := cr.GetAssignedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(assigned) != 2 {
		t.Errorf("assigned tables = %d, want 2", len(assigned))
	}
}

func TestCoordinatorRegistry_SendHeartbeat(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// First heartbeat (inserts)
	err := cr.SendHeartbeat(ctx)
	if err != nil {
		t.Fatalf("SendHeartbeat() error = %v", err)
	}

	// Second heartbeat (updates)
	err = cr.SendHeartbeat(ctx)
	if err != nil {
		t.Fatalf("SendHeartbeat() second call error = %v", err)
	}

	// Verify node exists in registry
	var nodeID string
	err = mc.DB.QueryRowContext(ctx,
		"SELECT node_id FROM _node_registry WHERE node_id = ?", "node-1").Scan(&nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != "node-1" {
		t.Errorf("node_id = %q, want node-1", nodeID)
	}
}

func TestCoordinatorRegistry_ReleaseAllTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	cr.UpsertTables(ctx, []config.TableConfig{
		{Name: "release_all_a", DisplayName: "A"},
		{Name: "release_all_b", DisplayName: "B"},
	})
	cr.ClaimTable(ctx, "release_all_a")
	cr.ClaimTable(ctx, "release_all_b")

	err := cr.ReleaseAllTables(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assigned, _ := cr.GetAssignedTables(ctx)
	if len(assigned) != 0 {
		t.Errorf("assigned tables after ReleaseAll = %d, want 0", len(assigned))
	}
}

func TestCoordinatorRegistry_ClaimByDifferentNodes(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	ctx := context.Background()
	log := zap.NewNop()

	node1 := node.NewCoordinatorRegistry(mc.DB, "node-1", true, log)
	node2 := node.NewCoordinatorRegistry(mc.DB, "node-2", true, log)

	node1.UpsertTables(ctx, []config.TableConfig{
		{Name: "contested", DisplayName: "Contested"},
	})

	// Node 1 claims
	claimed1, err := node1.ClaimTable(ctx, "contested")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed1 {
		t.Error("node-1 should claim successfully")
	}

	// Node 2 tries to claim — should fail
	claimed2, err := node2.ClaimTable(ctx, "contested")
	if err != nil {
		t.Fatal(err)
	}
	if claimed2 {
		t.Error("node-2 should not claim (already held by node-1)")
	}

	// Node 1 releases
	node1.ReleaseTable(ctx, "contested")

	// Now node 2 can claim
	claimed3, err := node2.ClaimTable(ctx, "contested")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed3 {
		t.Error("node-2 should claim after node-1 release")
	}
}

func TestCoordinatorRegistry_ClaimOrphansFromDeadNodes(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	ctx := context.Background()
	log := zap.NewNop()

	node1 := node.NewCoordinatorRegistry(mc.DB, "dead-node", true, log)
	node2 := node.NewCoordinatorRegistry(mc.DB, "alive-node", true, log)

	// Dead node registers and claims a table
	node1.UpsertTables(ctx, []config.TableConfig{
		{Name: "orphan_table", DisplayName: "Orphan"},
	})
	node1.SendHeartbeat(ctx)
	node1.ClaimTable(ctx, "orphan_table")

	// Set dead-node's heartbeat to the past (simulate death)
	mc.DB.ExecContext(ctx,
		"UPDATE _node_registry SET last_heartbeat_at = UNIX_TIMESTAMP() - 300 WHERE node_id = ?",
		"dead-node")

	// Alive node sends heartbeat and claims orphans
	node2.SendHeartbeat(ctx)
	claimed, err := node2.ClaimOrphansHeartbeat(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Errorf("claimed orphans = %d, want 1", len(claimed))
	}

	// Verify alive-node owns the table now
	assigned, _ := node2.GetAssignedTables(ctx)
	found := false
	for _, name := range assigned {
		if name == "orphan_table" {
			found = true
		}
	}
	if !found {
		t.Error("alive-node should own orphan_table after claiming orphans")
	}
}
