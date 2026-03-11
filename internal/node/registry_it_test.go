//go:build integration

package node_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/metastore"
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

// registerTestTable inserts a table into the registry, assignment, and optionally
// Kafka config tables. This replaces UpsertTables for test setup — tables are
// now registered via admin API in production.
func registerTestTable(t *testing.T, db *sql.DB, name, displayName string, kafkaTopic, kafkaBootstrapServers string) {
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

	// Write config blob with Kafka settings if provided.
	var configBlob []byte
	if kafkaTopic != "" {
		cfg := metastore.DefaultTableConfig()
		cfg.Kafka = &metastore.KafkaConfig{
			Topic:            kafkaTopic,
			BootstrapServers: kafkaBootstrapServers,
		}
		configBlob, err = metastore.EncodeTableConfig(cfg)
		if err != nil {
			t.Fatalf("encode config for %s: %v", name, err)
		}
	}

	_, err = db.ExecContext(ctx,
		"INSERT IGNORE INTO _table_config (table_name, config) VALUES (?, ?)",
		name, configBlob)
	if err != nil {
		t.Fatalf("insert _table_config %s: %v", name, err)
	}
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

func TestCoordinatorRegistry_RegisterAndList(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)

	registerTestTable(t, mc.DB, "logs_app", "Application Logs", "app-ir", "kafka:9092")
	registerTestTable(t, mc.DB, "logs_infra", "Infrastructure Logs", "infra-ir", "kafka:9092")

	// Verify tables registered
	allTables, err := cr.GetAllRegisteredTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allTables) != 2 {
		t.Errorf("registered tables = %d, want 2", len(allTables))
	}

	// Idempotent: insert again
	registerTestTable(t, mc.DB, "logs_app", "Application Logs", "app-ir", "kafka:9092")
	allTables, err = cr.GetAllRegisteredTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allTables) != 2 {
		t.Errorf("registered tables after re-insert = %d, want 2", len(allTables))
	}
}

func TestCoordinatorRegistry_ClaimAndRelease(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	registerTestTable(t, mc.DB, "claim_test", "Claim Test", "", "")

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

	registerTestTable(t, mc.DB, "assigned_a", "A", "", "")
	registerTestTable(t, mc.DB, "assigned_b", "B", "", "")
	registerTestTable(t, mc.DB, "unassigned_c", "C", "", "")

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

	registerTestTable(t, mc.DB, "release_all_a", "A", "", "")
	registerTestTable(t, mc.DB, "release_all_b", "B", "", "")
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

	registerTestTable(t, mc.DB, "contested", "Contested", "", "")

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

func TestCoordinatorRegistry_GetTableConfig_Kafka(t *testing.T) {
	mc, cr := setupRegistryIT(t)
	defer mc.Teardown(t)
	ctx := context.Background()

	// Register table with Kafka config embedded in config blob
	registerTestTable(t, mc.DB, "kafka_test", "Kafka Test", "test-topic", "kafka:9092")

	cfg, err := cr.GetTableConfig(ctx, "kafka_test")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka == nil {
		t.Fatal("Kafka config is nil, want non-nil")
	}
	if cfg.Kafka.Topic != "test-topic" {
		t.Errorf("Kafka.Topic = %q, want test-topic", cfg.Kafka.Topic)
	}
	if cfg.Kafka.BootstrapServers != "kafka:9092" {
		t.Errorf("Kafka.BootstrapServers = %q, want kafka:9092", cfg.Kafka.BootstrapServers)
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
	registerTestTable(t, mc.DB, "orphan_table", "Orphan", "", "")
	node1.SendHeartbeat(ctx)
	node1.ClaimTable(ctx, "orphan_table")

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
