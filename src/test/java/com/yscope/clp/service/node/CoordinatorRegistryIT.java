package com.yscope.clp.service.node;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.testutil.AbstractMariaDBTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CoordinatorRegistry Integration Tests")
class CoordinatorRegistryIT extends AbstractMariaDBTest {

  // ==========================================
  // Helper Methods
  // ==========================================

  /** Create a minimal table definition for testing. */
  private NodeConfig.TableDefinition createTableDef(String name) {
    NodeConfig.TableDefinition def = new NodeConfig.TableDefinition();
    def.setName(name);
    def.setDisplayName("Display " + name);
    def.setActive(true);
    return def;
  }

  /** Create a table definition with Kafka config. */
  private NodeConfig.TableDefinition createTableDefWithKafka(String name, String topic) {
    NodeConfig.TableDefinition def = createTableDef(name);
    NodeConfig.TableKafkaDefinition kafka = new NodeConfig.TableKafkaDefinition();
    kafka.setTopic(topic);
    kafka.setBootstrapServers("localhost:9092");
    def.setKafka(kafka);
    return def;
  }

  /** Register a table directly in _table and _table_assignment for testing. */
  private void registerTableWithAssignment(String tableName, String nodeId) throws SQLException {
    registerTable(tableName);
    try (Connection conn = getDataSource().getConnection()) {
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO _table_assignment (table_name, node_id, node_assigned_at, assignment_updated_at) "
                  + "VALUES (?, ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())")) {
        ps.setString(1, tableName);
        if (nodeId != null) {
          ps.setString(2, nodeId);
        } else {
          ps.setNull(2, java.sql.Types.VARCHAR);
        }
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.prepareStatement("INSERT INTO _table_config (table_name) VALUES (?)")) {
        ps.setString(1, tableName);
        ps.executeUpdate();
      }
    }
  }

  /** Register a node in the node registry with a heartbeat. */
  private void registerNode(String nodeId) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO _node_registry (node_id, last_heartbeat_at) VALUES (?, UNIX_TIMESTAMP())")) {
      ps.setString(1, nodeId);
      ps.executeUpdate();
    }
  }

  /** Set a node's heartbeat to N seconds ago (to simulate dead nodes). */
  private void setHeartbeatAge(String nodeId, int secondsAgo) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE _node_registry SET last_heartbeat_at = UNIX_TIMESTAMP() - ? WHERE node_id = ?")) {
      ps.setInt(1, secondsAgo);
      ps.setString(2, nodeId);
      ps.executeUpdate();
    }
  }

  /** Get the node_id assigned to a table. */
  private String getAssignedNodeId(String tableName) throws SQLException {
    try (Connection conn = getDataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT node_id FROM _table_assignment WHERE table_name = ?")) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString("node_id");
        }
        return null;
      }
    }
  }

  // ==========================================
  // initializeSchema
  // ==========================================

  @Nested
  @DisplayName("initializeSchema")
  class InitializeSchema {

    @Test
    void createsRegistryTables() throws SQLException {
      // Schema is loaded by AbstractMariaDBTest, but initializeSchema should be idempotent
      CoordinatorRegistry.initializeSchema(getDataSource());

      // Verify key tables exist by querying them
      try (Connection conn = getDataSource().getConnection();
          Statement stmt = conn.createStatement()) {
        // These should not throw
        stmt.executeQuery("SELECT COUNT(*) FROM _table");
        stmt.executeQuery("SELECT COUNT(*) FROM _table_assignment");
        stmt.executeQuery("SELECT COUNT(*) FROM _table_config");
        stmt.executeQuery("SELECT COUNT(*) FROM _node_registry");
      }
    }
  }

  // ==========================================
  // upsertTables
  // ==========================================

  @Nested
  @DisplayName("upsertTables")
  class UpsertTables {

    @Test
    void insertsTableDefinition() throws SQLException {
      NodeConfig.TableDefinition def = createTableDefWithKafka("clp_logs", "logs-topic");
      CoordinatorRegistry.upsertTables(getDataSource(), List.of(def));

      // Verify _table row
      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement("SELECT display_name FROM _table WHERE table_name = ?")) {
        ps.setString(1, "clp_logs");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Display clp_logs", rs.getString("display_name"));
        }
      }

      // Verify _table_assignment row
      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT node_id FROM _table_assignment WHERE table_name = ?")) {
        ps.setString(1, "clp_logs");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertNull(rs.getString("node_id")); // Not yet claimed
        }
      }
    }

    @Test
    void idempotent() throws SQLException {
      NodeConfig.TableDefinition def = createTableDefWithKafka("clp_logs", "logs-topic");
      CoordinatorRegistry.upsertTables(getDataSource(), List.of(def));
      CoordinatorRegistry.upsertTables(getDataSource(), List.of(def));

      // Should have exactly one row
      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement("SELECT COUNT(*) FROM _table WHERE table_name = ?")) {
        ps.setString(1, "clp_logs");
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          assertEquals(1, rs.getInt(1));
        }
      }
    }

    @Test
    void nullList_noop() throws SQLException {
      assertDoesNotThrow(() -> CoordinatorRegistry.upsertTables(getDataSource(), null));
    }

    @Test
    void emptyList_noop() throws SQLException {
      assertDoesNotThrow(() -> CoordinatorRegistry.upsertTables(getDataSource(), List.of()));
    }
  }

  // ==========================================
  // claimOneUnassignedTable
  // ==========================================

  @Nested
  @DisplayName("claimOneUnassignedTable")
  class ClaimOneUnassignedTable {

    @Test
    void returnsTableName() throws SQLException {
      registerTableWithAssignment("clp_logs", null); // unassigned
      registerNode("node-1");

      String claimed =
          CoordinatorRegistry.claimOneUnassignedTable(getDataSource(), "node-1", null);

      assertEquals("clp_logs", claimed);
      assertEquals("node-1", getAssignedNodeId("clp_logs"));
    }

    @Test
    void noneAvailable_returnsNull() throws SQLException {
      // No tables registered
      String claimed =
          CoordinatorRegistry.claimOneUnassignedTable(getDataSource(), "node-1", null);
      assertNull(claimed);
    }

    @Test
    void alreadyClaimed_skipped() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1"); // already assigned

      String claimed =
          CoordinatorRegistry.claimOneUnassignedTable(getDataSource(), "node-2", null);

      assertNull(claimed);
    }
  }

  // ==========================================
  // getAssignedCoordinators
  // ==========================================

  @Nested
  @DisplayName("getAssignedCoordinators")
  class GetAssignedCoordinators {

    @Test
    void returnsUnitsForNode() throws SQLException {
      NodeConfig.TableDefinition def = createTableDefWithKafka("clp_logs", "logs-topic");
      CoordinatorRegistry.upsertTables(getDataSource(), List.of(def));

      // Claim the table
      CoordinatorRegistry.claimOneUnassignedTable(getDataSource(), "node-1", null);

      List<NodeConfig.UnitDefinition> units =
          CoordinatorRegistry.getAssignedCoordinators(getDataSource(), "node-1");

      assertEquals(1, units.size());
      assertEquals("coordinator-clp_logs", units.get(0).getName());
      assertEquals("coordinator", units.get(0).getType());
      assertTrue(units.get(0).isEnabled());
    }
  }

  // ==========================================
  // heartbeat
  // ==========================================

  @Nested
  @DisplayName("heartbeat")
  class Heartbeat {

    @Test
    void insertsOrUpdates() throws SQLException {
      CoordinatorRegistry.heartbeat(getDataSource(), "node-1");

      // Verify node exists
      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT last_heartbeat_at FROM _node_registry WHERE node_id = ?")) {
        ps.setString(1, "node-1");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(rs.getLong("last_heartbeat_at") > 0);
        }
      }

      // Second call should update, not insert
      CoordinatorRegistry.heartbeat(getDataSource(), "node-1");

      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT COUNT(*) FROM _node_registry WHERE node_id = ?")) {
        ps.setString(1, "node-1");
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          assertEquals(1, rs.getInt(1));
        }
      }
    }
  }

  // ==========================================
  // findOrphanedTables
  // ==========================================

  @Nested
  @DisplayName("findOrphanedTables")
  class FindOrphanedTables {

    @Test
    void findsDeadNodeTables() throws SQLException {
      registerTableWithAssignment("clp_logs", "dead-node");
      registerNode("dead-node");
      setHeartbeatAge("dead-node", 600); // 10 min ago

      List<CoordinatorRegistry.OrphanedTable> orphans =
          CoordinatorRegistry.findOrphanedTables(getDataSource(), 180);

      assertEquals(1, orphans.size());
      assertEquals("clp_logs", orphans.get(0).tableName());
      assertEquals("dead-node", orphans.get(0).deadOwnerId());
    }

    @Test
    void liveNodeNotOrphaned() throws SQLException {
      registerTableWithAssignment("clp_logs", "alive-node");
      registerNode("alive-node"); // fresh heartbeat

      List<CoordinatorRegistry.OrphanedTable> orphans =
          CoordinatorRegistry.findOrphanedTables(getDataSource(), 180);

      assertTrue(orphans.isEmpty());
    }
  }

  // ==========================================
  // claimOrphan
  // ==========================================

  @Nested
  @DisplayName("claimOrphan")
  class ClaimOrphan {

    @Test
    void succeeds() throws SQLException {
      registerTableWithAssignment("clp_logs", "dead-node");

      boolean claimed =
          CoordinatorRegistry.claimOrphan(getDataSource(), "clp_logs", "dead-node", "new-node");

      assertTrue(claimed);
      assertEquals("new-node", getAssignedNodeId("clp_logs"));
    }

    @Test
    void wrongOwner_fails() throws SQLException {
      registerTableWithAssignment("clp_logs", "actual-owner");

      boolean claimed =
          CoordinatorRegistry.claimOrphan(getDataSource(), "clp_logs", "wrong-owner", "new-node");

      assertFalse(claimed);
      assertEquals("actual-owner", getAssignedNodeId("clp_logs"));
    }
  }

  // ==========================================
  // calculateFairShare
  // ==========================================

  @Nested
  @DisplayName("calculateFairShare")
  class CalculateFairShare {

    @Test
    void singleNode() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");
      registerTableWithAssignment("clp_metrics", "node-1");
      registerNode("node-1");

      int fairShare = CoordinatorRegistry.calculateFairShare(getDataSource(), 180);

      assertEquals(2, fairShare); // 2 tables / 1 node = 2
    }

    @Test
    void multipleNodes() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");
      registerTableWithAssignment("clp_metrics", "node-2");
      registerTableWithAssignment("clp_traces", "node-1");
      registerNode("node-1");
      registerNode("node-2");

      int fairShare = CoordinatorRegistry.calculateFairShare(getDataSource(), 180);

      // ceil(3 / 2) = 2
      assertEquals(2, fairShare);
    }

    @Test
    void noActiveTables_returnsMaxValue() throws SQLException {
      registerNode("node-1");

      int fairShare = CoordinatorRegistry.calculateFairShare(getDataSource(), 180);

      assertEquals(Integer.MAX_VALUE, fairShare);
    }
  }

  // ==========================================
  // countOwnedTables
  // ==========================================

  @Nested
  @DisplayName("countOwnedTables")
  class CountOwnedTables {

    @Test
    void countsCorrectly() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");
      registerTableWithAssignment("clp_metrics", "node-1");
      registerTableWithAssignment("clp_traces", "node-2");

      assertEquals(2, CoordinatorRegistry.countOwnedTables(getDataSource(), "node-1"));
      assertEquals(1, CoordinatorRegistry.countOwnedTables(getDataSource(), "node-2"));
      assertEquals(0, CoordinatorRegistry.countOwnedTables(getDataSource(), "node-3"));
    }
  }

  // ==========================================
  // revertClaim
  // ==========================================

  @Nested
  @DisplayName("revertClaim")
  class RevertClaim {

    @Test
    void setsNodeIdNull() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");

      CoordinatorRegistry.revertClaim(getDataSource(), "clp_logs", "node-1");

      assertNull(getAssignedNodeId("clp_logs"));
    }

    @Test
    void wrongNode_noEffect() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");

      CoordinatorRegistry.revertClaim(getDataSource(), "clp_logs", "wrong-node");

      assertEquals("node-1", getAssignedNodeId("clp_logs"));
    }
  }

  // ==========================================
  // getAssignedTableNames
  // ==========================================

  @Nested
  @DisplayName("getAssignedTableNames")
  class GetAssignedTableNames {

    @Test
    void returnsNames() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");
      registerTableWithAssignment("clp_metrics", "node-1");
      registerTableWithAssignment("clp_traces", "node-2");

      Set<String> names = CoordinatorRegistry.getAssignedTableNames(getDataSource(), "node-1");

      assertEquals(Set.of("clp_logs", "clp_metrics"), names);
    }

    @Test
    void noAssignments_returnsEmpty() throws SQLException {
      Set<String> names = CoordinatorRegistry.getAssignedTableNames(getDataSource(), "node-1");
      assertTrue(names.isEmpty());
    }
  }

  // ==========================================
  // renewLeases
  // ==========================================

  @Nested
  @DisplayName("renewLeases")
  class RenewLeases {

    @Test
    void updatesExpiry() throws SQLException {
      registerTableWithAssignment("clp_logs", "node-1");
      registerTableWithAssignment("clp_metrics", "node-1");

      int renewed = CoordinatorRegistry.renewLeases(getDataSource(), "node-1", 180);

      assertEquals(2, renewed);

      // Verify lease_expiry is set
      try (Connection conn = getDataSource().getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT lease_expiry FROM _table_assignment WHERE table_name = ?")) {
        ps.setString(1, "clp_logs");
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(rs.getLong("lease_expiry") > 0);
        }
      }
    }
  }
}
