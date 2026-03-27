//go:build integration

package node_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/node/registry"
	"github.com/y-scope/metalog/testutil"
)

func setupRegistryIT(t *testing.T) (*testutil.MariaDBContainer, *registry.Registry) {
	t.Helper()
	mc := testutil.SetupMariaDB(t)
	mc.LoadSchema(t)

	log := zap.NewNop()
	cr := registry.New(mc.DB, "node-1", true, log)
	return mc, cr
}

// registerTestTable inserts a table into the registry, assignment, and
// Kafka config tables. This replaces UpsertTables for test setup — tables are
// now registered via admin API in production.
func registerTestTable(t *testing.T, db *sql.DB, name, displayName string) {
	t.Helper()
	ctx := context.Background()

	_, err := db.ExecContext(ctx,
		"INSERT IGNORE INTO _table (table_name, display_name) VALUES (?, ?)",
		name, displayName)
	if err != nil {
		t.Fatalf("insert _table %s: %v", name, err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT IGNORE INTO _table_assignment (table_name) VALUES (?)",
		name)
	if err != nil {
		t.Fatalf("insert _table_assignment %s: %v", name, err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT IGNORE INTO _table_config (table_name, config) VALUES (?, ?)",
		name, nil)
	if err != nil {
		t.Fatalf("insert _table_config %s: %v", name, err)
	}
}

func TestRegistry_EnsureSystemTables(t *testing.T) {
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

func TestRegistry_RegisterAndList(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)

	registerTestTable(t, mc.DB, "logs_app", "Application Logs")
	registerTestTable(t, mc.DB, "logs_infra", "Infrastructure Logs")

	// Verify tables registered
	allTables, err := cr.GetAllRegisteredTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allTables) != 2 {
		t.Errorf("registered tables = %d, want 2", len(allTables))
	}

	// Idempotent: insert again
	registerTestTable(t, mc.DB, "logs_app", "Application Logs")
	allTables, err = cr.GetAllRegisteredTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allTables) != 2 {
		t.Errorf("registered tables after re-insert = %d, want 2", len(allTables))
	}
}

func TestRegistry_ClaimAndRelease(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	registerTestTable(t, mc.DB, "claim_test", "Claim Test")

	// Claim
	claimed, err := cr.ClaimTable(ctx, "claim_test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Error("ClaimTable() = false, want true")
	}

	// Claiming again should fail (already claimed by this node)
	claimed2, err := cr.ClaimTable(ctx, "claim_test", 0)
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

func TestRegistry_GetAssignedTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	registerTestTable(t, mc.DB, "assigned_a", "A")
	registerTestTable(t, mc.DB, "assigned_b", "B")
	registerTestTable(t, mc.DB, "unassigned_c", "C")

	cr.ClaimTable(ctx, "assigned_a", 0)
	cr.ClaimTable(ctx, "assigned_b", 0)

	assigned, err := cr.GetAssignedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(assigned) != 2 {
		t.Errorf("assigned tables = %d, want 2", len(assigned))
	}
}

func TestRegistry_SendHeartbeat(t *testing.T) {
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

func TestRegistry_ReleaseAllTables(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	registerTestTable(t, mc.DB, "release_all_a", "A")
	registerTestTable(t, mc.DB, "release_all_b", "B")
	cr.ClaimTable(ctx, "release_all_a", 0)
	cr.ClaimTable(ctx, "release_all_b", 0)

	err := cr.ReleaseAllTables(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assigned, _ := cr.GetAssignedTables(ctx)
	if len(assigned) != 0 {
		t.Errorf("assigned tables after ReleaseAll = %d, want 0", len(assigned))
	}
}

func TestRegistry_ClaimByDifferentNodes(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	ctx := context.Background()
	log := zap.NewNop()

	node1 := registry.New(mc.DB, "node-1", true, log)
	node2 := registry.New(mc.DB, "node-2", true, log)

	registerTestTable(t, mc.DB, "contested", "Contested")

	// Node 1 claims
	claimed1, err := node1.ClaimTable(ctx, "contested", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed1 {
		t.Error("node-1 should claim successfully")
	}

	// Node 2 tries to claim — should fail
	claimed2, err := node2.ClaimTable(ctx, "contested", 0)
	if err != nil {
		t.Fatal(err)
	}
	if claimed2 {
		t.Error("node-2 should not claim (already held by node-1)")
	}

	// Node 1 releases
	node1.ReleaseTable(ctx, "contested")

	// Now node 2 can claim
	claimed3, err := node2.ClaimTable(ctx, "contested", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed3 {
		t.Error("node-2 should claim after node-1 release")
	}
}

// Kafka config is no longer in TableConfig — see _kafka_source table instead.

func TestRegistry_ClaimOrphansFromDeadNodes(t *testing.T) {
	mc := testutil.SetupMariaDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	ctx := context.Background()
	log := zap.NewNop()

	node1 := registry.New(mc.DB, "dead-node", true, log)
	node2 := registry.New(mc.DB, "alive-node", true, log)

	// Dead node registers and claims a table
	registerTestTable(t, mc.DB, "orphan_table", "Orphan")
	node1.SendHeartbeat(ctx)
	node1.ClaimTable(ctx, "orphan_table", 0)

	// Set dead-node's heartbeat to the past (simulate death)
	pastNanos := time.Now().Add(-300 * time.Second).UnixNano()
	mc.DB.ExecContext(ctx,
		"UPDATE _node_registry SET last_heartbeat_at = ? WHERE node_id = ?",
		pastNanos, "dead-node")

	// Alive node sends heartbeat and claims orphans
	node2.SendHeartbeat(ctx)
	claimed, err := node2.ClaimOrphansHeartbeat(ctx, 60*time.Second)
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
