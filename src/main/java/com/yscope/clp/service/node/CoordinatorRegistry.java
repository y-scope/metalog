package com.yscope.clp.service.node;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads coordinator configuration and assignments from the database.
 *
 * <p>Replaces the static {@code units:} section of node.yaml with database-driven coordinator
 * discovery. On startup, the Node calls this class to:
 *
 * <ol>
 *   <li>Create the {@code _table} registry (idempotent)
 *   <li>Query for tables assigned to this node
 *   <li>Convert each row into a {@link NodeConfig.UnitDefinition} for the existing unit lifecycle
 * </ol>
 *
 * <p>The config mapping strategy converts DB rows into the same {@code Map<String, Object>}
 * structure that {@link CoordinatorUnitConfig#fromMap(Map)} expects, so no changes are needed to
 * the config parser or unit classes.
 *
 * @see CoordinatorUnitConfig#fromMap(Map)
 */
public class CoordinatorRegistry {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorRegistry.class);

  /** Prefix used to derive Kafka consumer group ID from table_name and table_id. */
  private static final String KAFKA_GROUP_PREFIX = "clp-coordinator-";

  private static final String QUERY_UNASSIGNED_TABLES =
      "SELECT a.table_name FROM _table_assignment a "
          + "JOIN _table t ON a.table_name = t.table_name "
          + "WHERE a.node_id IS NULL AND t.active = true";

  private static final String CLAIM_TABLE =
      "UPDATE _table_assignment "
          + "SET node_id = ?, node_assigned_at = UNIX_TIMESTAMP(), assignment_updated_at = UNIX_TIMESTAMP() "
          + "WHERE table_name = ? AND node_id IS NULL";

  private static final String QUERY_ASSIGNED_TABLES =
      "SELECT t.table_id, t.table_name, "
          + "k.kafka_bootstrap_servers, k.kafka_topic, k.record_transformer, "
          + "c.kafka_poller_enabled, "
          + "c.deletion_enabled, c.consolidation_enabled, "
          + "c.retention_cleanup_enabled, c.retention_cleanup_interval_ms, "
          + "c.partition_manager_enabled, c.partition_maintenance_interval_ms, "
          + "c.policy_hot_reload_enabled, c.index_hot_reload_enabled, "
          + "c.schema_evolution_enabled, c.schema_evolution_max_dim_columns, "
          + "c.schema_evolution_max_count_columns, "
          + "c.loop_interval_ms, c.storage_deletion_delay_ms, "
          + "c.policy_config_path, c.index_config_path "
          + "FROM _table t "
          + "LEFT JOIN _table_kafka k ON t.table_name = k.table_name "
          + "JOIN _table_config c ON t.table_name = c.table_name "
          + "JOIN _table_assignment a ON t.table_name = a.table_name "
          + "WHERE a.node_id = ? AND t.active = true";

  private static final String QUERY_ACTIVE_PARTITIONED_TABLES =
      "SELECT t.table_name FROM _table t "
          + "JOIN _table_config c ON t.table_name = c.table_name "
          + "WHERE t.active = true AND c.partition_manager_enabled = true";

  private static final String HEARTBEAT_SQL =
      "INSERT INTO _node_registry (node_id, last_heartbeat_at) "
          + "VALUES (?, UNIX_TIMESTAMP()) "
          + "ON DUPLICATE KEY UPDATE last_heartbeat_at = UNIX_TIMESTAMP()";

  private static final String FIND_ORPHANED_TABLES =
      "SELECT a.table_name, a.node_id FROM _table_assignment a "
          + "JOIN _table t ON a.table_name = t.table_name "
          + "LEFT JOIN _node_registry n ON a.node_id = n.node_id "
          + "WHERE t.active = true AND a.node_id IS NOT NULL "
          + "AND (n.node_id IS NULL OR n.last_heartbeat_at < UNIX_TIMESTAMP() - ?)";

  private static final String CLAIM_ORPHAN_SQL =
      "UPDATE _table_assignment "
          + "SET node_id = ?, assignment_updated_at = UNIX_TIMESTAMP() "
          + "WHERE table_name = ? AND node_id = ?";

  private static final String COUNT_ACTIVE_TABLES_WITH_ASSIGNMENT =
      "SELECT COUNT(*) FROM _table_assignment a "
          + "JOIN _table t ON a.table_name = t.table_name "
          + "WHERE t.active = true AND a.node_id IS NOT NULL";

  private static final String COUNT_ACTIVE_NODES =
      "SELECT COUNT(*) FROM _node_registry WHERE last_heartbeat_at >= UNIX_TIMESTAMP() - ?";

  private static final String COUNT_OWNED_TABLES =
      "SELECT COUNT(*) FROM _table_assignment WHERE node_id = ?";

  private static final String UPDATE_PROGRESS_SQL =
      "UPDATE _table_assignment SET last_progress_at = UNIX_TIMESTAMP() "
          + "WHERE table_name = ? AND node_id = ?";

  private static final String REVERT_CLAIM_SQL =
      "UPDATE _table_assignment "
          + "SET node_id = NULL, assignment_updated_at = UNIX_TIMESTAMP() "
          + "WHERE table_name = ? AND node_id = ?";

  // ==================== Lease mode SQL ====================

  private static final String RENEW_LEASES_SQL =
      "UPDATE _table_assignment SET lease_expiry = UNIX_TIMESTAMP() + ? WHERE node_id = ?";

  private static final String FIND_EXPIRED_LEASES_SQL =
      "SELECT a.table_name, a.node_id FROM _table_assignment a "
          + "JOIN _table t ON a.table_name = t.table_name "
          + "WHERE t.active = true AND a.node_id IS NOT NULL "
          + "AND (a.lease_expiry < UNIX_TIMESTAMP() OR a.lease_expiry IS NULL)";

  private static final String CLAIM_EXPIRED_LEASE_SQL =
      "UPDATE _table_assignment "
          + "SET node_id = ?, lease_expiry = UNIX_TIMESTAMP() + ?, assignment_updated_at = UNIX_TIMESTAMP() "
          + "WHERE table_name = ? AND (lease_expiry < UNIX_TIMESTAMP() OR lease_expiry IS NULL)";

  private static final String COUNT_ACTIVE_NODES_FROM_LEASES =
      "SELECT COUNT(DISTINCT node_id) FROM _table_assignment "
          + "WHERE lease_expiry >= UNIX_TIMESTAMP() AND node_id IS NOT NULL";

  private static final String CLAIM_UNASSIGNED_WITH_LEASE_SQL =
      "UPDATE _table_assignment "
          + "SET node_id = ?, node_assigned_at = UNIX_TIMESTAMP(), "
          + "assignment_updated_at = UNIX_TIMESTAMP(), lease_expiry = UNIX_TIMESTAMP() + ? "
          + "WHERE table_name = ? AND node_id IS NULL";

  private CoordinatorRegistry() {}

  /**
   * Create the {@code _table} registry if it doesn't exist.
   *
   * <p>Reads the DDL from {@code schema/schema.sql} on the classpath and executes the {@code CREATE
   * TABLE IF NOT EXISTS _table} statement. This is idempotent.
   *
   * @param dataSource Database connection source
   * @throws SQLException if table creation fails
   */
  public static void initializeSchema(DataSource dataSource) throws SQLException {
    try (InputStream is =
        CoordinatorRegistry.class.getClassLoader().getResourceAsStream("schema/schema.sql")) {
      if (is == null) {
        logger.warn("schema/schema.sql not found on classpath, skipping coordination schema init");
        return;
      }

      String sql;
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        // Strip SQL line comments before splitting to avoid splitting on
        // semicolons inside comments (e.g., "projection only; use hash")
        sql =
            reader
                .lines()
                .filter(line -> !line.stripLeading().startsWith("--"))
                .collect(Collectors.joining("\n"));
      }

      // Split into individual statements and execute only coordination table DDL
      // and schema migration ALTER statements for coordination tables
      String[] statements = sql.split(";");
      int executed = 0;

      try (Connection conn = dataSource.getConnection();
          Statement stmt = conn.createStatement()) {
        for (String s : statements) {
          String trimmed = s.trim();
          if (trimmed.isEmpty()) {
            continue;
          }
          if (trimmed.contains("CREATE TABLE") && isCoordinationTableDdl(trimmed)) {
            stmt.execute(trimmed);
            executed++;
          } else if (isCoordinationMigration(trimmed)) {
            stmt.execute(trimmed);
            executed++;
          }
        }
      }

      logger.info("Coordination schema initialized: executed {} CREATE TABLE statements", executed);

    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to initialize coordination schema", e);
    }
  }

  /** Check if a DDL statement creates coordination tables (_table*, _node_registry, registries). */
  private static boolean isCoordinationTableDdl(String sql) {
    return sql.contains("CREATE TABLE IF NOT EXISTS _table")
        || sql.contains("CREATE TABLE IF NOT EXISTS _task_queue")
        || sql.contains("CREATE TABLE IF NOT EXISTS _node_registry")
        || sql.contains("CREATE TABLE IF NOT EXISTS _dim_registry")
        || sql.contains("CREATE TABLE IF NOT EXISTS _agg_registry")
        || sql.contains("CREATE TABLE IF NOT EXISTS _sketch_registry");
  }

  /** Check if a statement is a schema migration for coordination tables. */
  private static boolean isCoordinationMigration(String sql) {
    return sql.contains("ALTER TABLE _table_config") || sql.contains("ALTER TABLE _task_queue");
  }

  /**
   * UPSERT table definitions from YAML config into the {@code _table*} sub-tables.
   *
   * <p>Only explicitly specified (non-null) fields are updated; omitted fields keep their DB
   * defaults or existing values. All writes for each table run in a single transaction.
   *
   * @param dataSource Database connection source
   * @param tables Table definitions from YAML config (may be null or empty)
   * @throws SQLException if any UPSERT fails
   * @throws IllegalArgumentException if a table definition is missing required fields
   */
  public static void upsertTables(DataSource dataSource, List<NodeConfig.TableDefinition> tables)
      throws SQLException {
    if (tables == null || tables.isEmpty()) {
      return;
    }

    for (NodeConfig.TableDefinition table : tables) {
      validateTableDefinition(table);
    }

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        for (NodeConfig.TableDefinition table : tables) {
          upsertTable(conn, table);
        }
        conn.commit();
        logger.info("Upserted {} table(s) from config", tables.size());
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    }
  }

  private static void validateTableDefinition(NodeConfig.TableDefinition table) {
    if (table.getName() == null || table.getName().isBlank()) {
      throw new IllegalArgumentException("Table definition missing required field: name");
    }
    if (table.getKafka() != null
        && (table.getKafka().getTopic() == null || table.getKafka().getTopic().isBlank())) {
      throw new IllegalArgumentException(
          "Table '" + table.getName() + "' missing required field: kafka.topic");
    }
  }

  private static void upsertTable(Connection conn, NodeConfig.TableDefinition table)
      throws SQLException {
    String name = table.getName();
    String displayName = table.getDisplayName() != null ? table.getDisplayName() : name;

    // 1. _table: identity
    upsertTableIdentity(conn, name, displayName, table.getActive());

    // 2. _table_kafka: routing (optional — omit for gRPC-only tables)
    if (table.getKafka() != null) {
      upsertTableKafka(conn, name, table.getKafka());
    }

    // 3. _table_config: feature flags (INSERT IGNORE + conditional UPDATE)
    upsertTableConfig(conn, name, table);

    // 4. _table_assignment: insert-only, never overwrite existing assignment
    try (PreparedStatement ps =
        conn.prepareStatement("INSERT IGNORE INTO _table_assignment (table_name) VALUES (?)")) {
      ps.setString(1, name);
      ps.executeUpdate();
    }

    // 5. _sketch_registry: pre-populate 32 AVAILABLE slots (idempotent)
    prepopulateSketchSlots(conn, name);

    logger.info("Upserted table '{}' (display='{}')", name, displayName);
  }

  private static void upsertTableIdentity(
      Connection conn, String name, String displayName, Boolean active) throws SQLException {
    List<String> columns = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    columns.add("table_name");
    params.add(name);

    columns.add("display_name");
    params.add(displayName);

    addIfNonNull(columns, params, "active", active);

    // table_name is the PK (index 0), not included in ON DUPLICATE KEY UPDATE
    executeDynamicUpsert(conn, "_table", columns, params, 1);
  }

  private static void upsertTableKafka(
      Connection conn, String name, NodeConfig.TableKafkaDefinition kafka) throws SQLException {
    List<String> columns = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    columns.add("table_name");
    params.add(name);

    columns.add("kafka_topic");
    params.add(kafka.getTopic());

    addIfNonNull(columns, params, "kafka_bootstrap_servers", kafka.getBootstrapServers());
    addIfNonNull(columns, params, "record_transformer", kafka.getRecordTransformer());

    // table_name is the PK (index 0), not included in ON DUPLICATE KEY UPDATE
    executeDynamicUpsert(conn, "_table_kafka", columns, params, 1);
  }

  /**
   * Execute a dynamic INSERT ... ON DUPLICATE KEY UPDATE statement.
   *
   * <p>Columns at indices {@code [0, updateStartIndex)} are insert-only (typically the PK). Columns
   * at indices {@code [updateStartIndex, columns.size())} are included in both the INSERT and the
   * ON DUPLICATE KEY UPDATE clause.
   *
   * @param conn Database connection
   * @param table Target table name
   * @param columns Column names (parallel with params)
   * @param params Parameter values (parallel with columns)
   * @param updateStartIndex First column index to include in the UPDATE clause
   */
  private static void executeDynamicUpsert(
      Connection conn,
      String table,
      List<String> columns,
      List<Object> params,
      int updateStartIndex)
      throws SQLException {
    List<String> updateClauses = new ArrayList<>();
    for (int i = updateStartIndex; i < columns.size(); i++) {
      String col = columns.get(i);
      updateClauses.add(col + " = VALUES(" + col + ")");
    }

    String placeholders = String.join(", ", columns.stream().map(c -> "?").toList());
    String sql =
        "INSERT INTO "
            + table
            + " ("
            + String.join(", ", columns)
            + ") VALUES ("
            + placeholders
            + ") ON DUPLICATE KEY UPDATE "
            + String.join(", ", updateClauses);

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        ps.setObject(i + 1, params.get(i));
      }
      ps.executeUpdate();
    }
  }

  private static void upsertTableConfig(
      Connection conn, String name, NodeConfig.TableDefinition table) throws SQLException {
    // Step 1: Ensure row exists with DB defaults
    try (PreparedStatement ps =
        conn.prepareStatement("INSERT IGNORE INTO _table_config (table_name) VALUES (?)")) {
      ps.setString(1, name);
      ps.executeUpdate();
    }

    // Step 2: Update only non-null fields
    List<String> setClauses = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    addSetClauseIfNonNull(
        setClauses, params, "kafka_poller_enabled", table.getKafkaPollerEnabled());
    addSetClauseIfNonNull(setClauses, params, "deletion_enabled", table.getDeletionEnabled());
    addSetClauseIfNonNull(
        setClauses, params, "consolidation_enabled", table.getConsolidationEnabled());
    addSetClauseIfNonNull(
        setClauses, params, "retention_cleanup_enabled", table.getRetentionCleanupEnabled());
    addSetClauseIfNonNull(
        setClauses, params, "retention_cleanup_interval_ms", table.getRetentionCleanupIntervalMs());
    addSetClauseIfNonNull(
        setClauses, params, "partition_manager_enabled", table.getPartitionManagerEnabled());
    addSetClauseIfNonNull(
        setClauses,
        params,
        "partition_maintenance_interval_ms",
        table.getPartitionMaintenanceIntervalMs());
    addSetClauseIfNonNull(
        setClauses, params, "policy_hot_reload_enabled", table.getPolicyHotReloadEnabled());
    addSetClauseIfNonNull(
        setClauses, params, "index_hot_reload_enabled", table.getIndexHotReloadEnabled());
    addSetClauseIfNonNull(
        setClauses, params, "schema_evolution_enabled", table.getSchemaEvolutionEnabled());
    addSetClauseIfNonNull(
        setClauses,
        params,
        "schema_evolution_max_dim_columns",
        table.getSchemaEvolutionMaxDimColumns());
    addSetClauseIfNonNull(
        setClauses,
        params,
        "schema_evolution_max_count_columns",
        table.getSchemaEvolutionMaxCountColumns());
    addSetClauseIfNonNull(setClauses, params, "loop_interval_ms", table.getLoopIntervalMs());
    addSetClauseIfNonNull(
        setClauses, params, "storage_deletion_delay_ms", table.getStorageDeletionDelayMs());
    addSetClauseIfNonNull(setClauses, params, "policy_config_path", table.getPolicyConfigPath());
    addSetClauseIfNonNull(setClauses, params, "index_config_path", table.getIndexConfigPath());

    if (setClauses.isEmpty()) {
      return;
    }

    String sql =
        "UPDATE _table_config SET " + String.join(", ", setClauses) + " WHERE table_name = ?";
    params.add(name);

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        ps.setObject(i + 1, params.get(i));
      }
      ps.executeUpdate();
    }
  }

  /** Number of pre-allocated sketch slots in the SET column and _sketch_registry. */
  private static final int SKETCH_SLOT_COUNT = 32;

  /**
   * Pre-populate 32 AVAILABLE sketch slots in _sketch_registry for a table.
   *
   * <p>Uses INSERT IGNORE so this is idempotent — existing slots are not overwritten.
   */
  private static void prepopulateSketchSlots(Connection conn, String tableName)
      throws SQLException {
    String sql =
        "INSERT IGNORE INTO _sketch_registry (table_name, set_member, status) VALUES (?, ?, 'AVAILABLE')";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= SKETCH_SLOT_COUNT; i++) {
        ps.setString(1, tableName);
        ps.setString(2, String.format("s%02d", i));
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  /**
   * Add a column name and value to the lists if the value is non-null.
   *
   * <p>Used by the dynamic UPSERT builders to conditionally include optional fields.
   */
  private static void addIfNonNull(
      List<String> columns, List<Object> params, String column, Object value) {
    if (value != null) {
      columns.add(column);
      params.add(value);
    }
  }

  /**
   * Add a {@code column = ?} SET clause and value to the lists if the value is non-null.
   *
   * <p>Used by the conditional UPDATE in {@link #upsertTableConfig}.
   */
  private static void addSetClauseIfNonNull(
      List<String> setClauses, List<Object> params, String column, Object value) {
    if (value != null) {
      setClauses.add(column + " = ?");
      params.add(value);
    }
  }

  /**
   * Attempt to claim one unassigned active table for this node (fight-for-master).
   *
   * <p>Queries for a single table where {@code node_id IS NULL} and {@code active = true}, then
   * attempts to claim it via an atomic UPDATE. The {@code WHERE node_id IS NULL} condition in the
   * UPDATE ensures that if two nodes race, only the first one succeeds (the second sees {@code
   * affected_rows = 0}).
   *
   * <p>Callers should invoke this in a loop, recomputing fair share between each call. This
   * staggered approach gives concurrent nodes time to become visible — especially in lease mode,
   * where a node's first claim creates its first visible lease.
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier claiming the table
   * @param leaseTtlSeconds If non-null, also sets {@code lease_expiry} on claim (lease mode)
   * @return The claimed table name, or null if no unassigned tables or claim lost the race
   * @throws SQLException if the query or update fails
   */
  public static String claimOneUnassignedTable(
      DataSource dataSource, String nodeId, Integer leaseTtlSeconds) throws SQLException {

    // Find one unassigned table
    String tableName = null;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(QUERY_UNASSIGNED_TABLES + " LIMIT 1")) {
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          tableName = rs.getString("table_name");
        }
      }
    }

    if (tableName == null) {
      return null;
    }

    // Attempt atomic claim
    String claimSql = leaseTtlSeconds != null ? CLAIM_UNASSIGNED_WITH_LEASE_SQL : CLAIM_TABLE;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(claimSql)) {
      if (leaseTtlSeconds != null) {
        ps.setString(1, nodeId);
        ps.setInt(2, leaseTtlSeconds);
        ps.setString(3, tableName);
      } else {
        ps.setString(1, nodeId);
        ps.setString(2, tableName);
      }
      int affected = ps.executeUpdate();
      if (affected == 1) {
        logger.info("Claimed unassigned table '{}' for node '{}'", tableName, nodeId);
        return tableName;
      } else {
        logger.debug(
            "Table '{}' already claimed by another node, skipping for node '{}'",
            tableName,
            nodeId);
        return null;
      }
    }
  }

  /**
   * Query the database for tables assigned to a specific node.
   *
   * <p>Queries the {@code _table*} sub-tables (joined on {@code table_name}) to find all active
   * tables assigned to the given node. Each row is converted into a {@link
   * NodeConfig.UnitDefinition} using column-based config mapping:
   *
   * <ol>
   *   <li>Read typed columns directly from the result set (no JSON parsing)
   *   <li>Map {@code table_name} → key {@code "table"}, {@code kafka_*} → nested {@code "kafka"}
   *       map
   *   <li>Derive {@code kafka.groupId} from table_name and table_id
   *   <li>The resulting map is compatible with {@link CoordinatorUnitConfig#fromMap(Map)}
   * </ol>
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier to look up assignments for
   * @return List of UnitDefinitions (coordinators and workers) assigned to this node
   * @throws SQLException if the query fails
   */
  public static List<NodeConfig.UnitDefinition> getAssignedCoordinators(
      DataSource dataSource, String nodeId) throws SQLException {

    List<NodeConfig.UnitDefinition> units = new ArrayList<>();

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(QUERY_ASSIGNED_TABLES)) {

      ps.setString(1, nodeId);

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String tableId = rs.getString("table_id");
          String tableName = rs.getString("table_name");
          String kafkaGroupId = KAFKA_GROUP_PREFIX + tableName + "-" + tableId;

          Map<String, Object> config = buildConfigMap(rs, kafkaGroupId);

          NodeConfig.UnitDefinition coordDef = new NodeConfig.UnitDefinition();
          coordDef.setName("coordinator-" + tableName);
          coordDef.setType("coordinator");
          coordDef.setEnabled(true);
          coordDef.setConfig(config);
          units.add(coordDef);

          logger.debug(
              "Discovered coordinator: table={}, topic={}", tableName, rs.getString("kafka_topic"));
        }
      }
    }

    logger.debug("Found {} units assigned to node '{}'", units.size(), nodeId);
    return units;
  }

  /**
   * Query all active tables that have partition management enabled.
   *
   * <p>Used by the node-level partition maintenance thread to run cooperative maintenance across
   * all tables, not just locally owned ones.
   *
   * @param dataSource Database connection source
   * @return List of table names with partition management enabled
   * @throws SQLException if the query fails
   */
  public static List<String> getActivePartitionedTables(DataSource dataSource) throws SQLException {
    List<String> tables = new ArrayList<>();

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(QUERY_ACTIVE_PARTITIONED_TABLES);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        tables.add(rs.getString("table_name"));
      }
    }

    return tables;
  }

  /**
   * Build a config map from result set columns.
   *
   * <p>Maps typed DB columns (snake_case) to camelCase config keys, producing a map compatible with
   * {@link CoordinatorUnitConfig#fromMap(Map)}.
   */
  private static Map<String, Object> buildConfigMap(ResultSet rs, String kafkaGroupId)
      throws SQLException {
    Map<String, Object> config = new HashMap<>();
    config.put("table", rs.getString("table_name"));

    // Kafka routing (may be null for gRPC-only tables with no _table_kafka row)
    Map<String, Object> kafkaMap = new HashMap<>();
    String bs = rs.getString("kafka_bootstrap_servers");
    String topic = rs.getString("kafka_topic");
    if (bs != null) kafkaMap.put("bootstrapServers", bs);
    if (topic != null) kafkaMap.put("topic", topic);
    kafkaMap.put("groupId", kafkaGroupId);
    String recordTransformer = rs.getString("record_transformer");
    if (recordTransformer != null) kafkaMap.put("recordTransformer", recordTransformer);
    config.put("kafka", kafkaMap);

    // Feature flags (snake_case DB → camelCase config)
    // kafkaPollerEnabled is false for gRPC-only tables (no topic → no poller)
    config.put("kafkaPollerEnabled", topic != null && rs.getBoolean("kafka_poller_enabled"));
    config.put("deletionEnabled", rs.getBoolean("deletion_enabled"));
    config.put("consolidationEnabled", rs.getBoolean("consolidation_enabled"));
    config.put("retentionCleanupEnabled", rs.getBoolean("retention_cleanup_enabled"));
    config.put("retentionCleanupIntervalMs", rs.getLong("retention_cleanup_interval_ms"));
    config.put("partitionManagerEnabled", rs.getBoolean("partition_manager_enabled"));
    config.put("partitionMaintenanceIntervalMs", rs.getLong("partition_maintenance_interval_ms"));
    config.put("policyHotReloadEnabled", rs.getBoolean("policy_hot_reload_enabled"));
    config.put("indexHotReloadEnabled", rs.getBoolean("index_hot_reload_enabled"));
    config.put("schemaEvolutionEnabled", rs.getBoolean("schema_evolution_enabled"));
    config.put("schemaEvolutionMaxDimColumns", rs.getInt("schema_evolution_max_dim_columns"));
    config.put("schemaEvolutionMaxCountColumns", rs.getInt("schema_evolution_max_count_columns"));
    config.put("loopIntervalMs", rs.getLong("loop_interval_ms"));
    config.put("storageDeletionDelayMs", rs.getLong("storage_deletion_delay_ms"));

    // Nullable string paths
    String policyPath = rs.getString("policy_config_path");
    if (policyPath != null) config.put("policyConfigPath", policyPath);
    String indexPath = rs.getString("index_config_path");
    if (indexPath != null) config.put("indexConfigPath", indexPath);

    return config;
  }

  // ==================== Heartbeat & Self-Healing (Phase 2) ====================

  /**
   * Record representing an orphaned table that belongs to a dead node.
   *
   * @param tableName the name of the orphaned table
   * @param deadOwnerId the node ID of the dead owner
   */
  public record OrphanedTable(String tableName, String deadOwnerId) {}

  /**
   * Register this node or update its heartbeat timestamp.
   *
   * <p>Uses INSERT ON DUPLICATE KEY UPDATE to atomically register a new node or refresh an existing
   * node's heartbeat. The timestamp uses the database server's UNIX_TIMESTAMP() for consistency
   * across nodes with potentially unsynchronized clocks.
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier
   * @throws SQLException if the update fails
   */
  public static void heartbeat(DataSource dataSource, String nodeId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(HEARTBEAT_SQL)) {
      ps.setString(1, nodeId);
      ps.executeUpdate();
      logger.debug("Heartbeat sent for node '{}'", nodeId);
    }
  }

  /**
   * Find orphaned tables whose owners are dead or missing from the node registry.
   *
   * <p>A table is orphaned when:
   *
   * <ul>
   *   <li>It has a non-null node_id in _table_assignment, AND
   *   <li>That node either doesn't exist in _node_registry (never heartbeated), OR
   *   <li>The node's last heartbeat is older than the dead threshold
   * </ul>
   *
   * @param dataSource Database connection source
   * @param deadThresholdSeconds Seconds since last heartbeat before a node is considered dead
   * @return List of orphaned tables with their dead owner IDs
   * @throws SQLException if the query fails
   */
  public static List<OrphanedTable> findOrphanedTables(
      DataSource dataSource, int deadThresholdSeconds) throws SQLException {
    List<OrphanedTable> orphans = new ArrayList<>();

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_ORPHANED_TABLES)) {
      ps.setInt(1, deadThresholdSeconds);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          orphans.add(new OrphanedTable(rs.getString("table_name"), rs.getString("node_id")));
        }
      }
    }

    return orphans;
  }

  /**
   * Atomically claim an orphaned table from a dead owner.
   *
   * <p>The atomic claim uses {@code WHERE node_id = deadOwnerId} to ensure only one node succeeds
   * when multiple nodes race to claim the same orphan. If the table was already claimed by another
   * node, this method returns false.
   *
   * @param dataSource Database connection source
   * @param tableName The orphaned table to claim
   * @param deadOwnerId The node ID of the dead owner (must match for claim to succeed)
   * @param newOwnerId The node ID of the new owner claiming the table
   * @return true if the claim succeeded (affected_rows = 1), false otherwise
   * @throws SQLException if the update fails
   */
  public static boolean claimOrphan(
      DataSource dataSource, String tableName, String deadOwnerId, String newOwnerId)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(CLAIM_ORPHAN_SQL)) {
      ps.setString(1, newOwnerId);
      ps.setString(2, tableName);
      ps.setString(3, deadOwnerId);
      int affected = ps.executeUpdate();
      return affected == 1;
    }
  }

  /**
   * Calculate the fair share of tables per active node.
   *
   * <p>Fair share = ceil(total_active_assigned_tables / active_nodes). This ensures tables are
   * distributed roughly evenly across the cluster. Nodes should only claim orphans if their current
   * load is below this threshold.
   *
   * <p>In heartbeat mode ({@code deadThresholdSeconds != null}), active nodes are counted from
   * {@code _node_registry}. In lease mode ({@code deadThresholdSeconds == null}), active nodes are
   * counted as distinct owners with non-expired leases in {@code _table_assignment}.
   *
   * @param dataSource Database connection source
   * @param deadThresholdSeconds Heartbeat threshold (seconds), or null for lease mode
   * @return The fair share, or Integer.MAX_VALUE if no active nodes (caller can claim all)
   * @throws SQLException if the query fails
   */
  public static int calculateFairShare(DataSource dataSource, Integer deadThresholdSeconds)
      throws SQLException {
    int totalTables = 0;
    int activeNodes = 0;

    try (Connection conn = dataSource.getConnection()) {
      // Count active tables with assignment
      try (PreparedStatement ps = conn.prepareStatement(COUNT_ACTIVE_TABLES_WITH_ASSIGNMENT);
          ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          totalTables = rs.getInt(1);
        }
      }

      if (deadThresholdSeconds != null) {
        // Heartbeat mode: count active nodes from _node_registry
        try (PreparedStatement ps = conn.prepareStatement(COUNT_ACTIVE_NODES)) {
          ps.setInt(1, deadThresholdSeconds);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              activeNodes = rs.getInt(1);
            }
          }
        }
      } else {
        // Lease mode: count distinct owners with active leases
        try (PreparedStatement ps = conn.prepareStatement(COUNT_ACTIVE_NODES_FROM_LEASES);
            ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            activeNodes = rs.getInt(1);
          }
        }
      }
    }

    if (activeNodes == 0 || totalTables == 0) {
      // No active nodes, or no tables yet assigned to any node: claim freely
      return Integer.MAX_VALUE;
    }

    // Ceiling division: (a + b - 1) / b
    return (totalTables + activeNodes - 1) / activeNodes;
  }

  /**
   * Count the number of tables owned by a specific node.
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier
   * @return The number of tables assigned to this node
   * @throws SQLException if the query fails
   */
  public static int countOwnedTables(DataSource dataSource, String nodeId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNT_OWNED_TABLES)) {
      ps.setString(1, nodeId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1);
        }
        return 0;
      }
    }
  }

  /**
   * Revert a claim by setting node_id to NULL.
   *
   * <p>Used when a CoordinatorUnit fails to start after claiming a table (Edge Case 6.6). The table
   * becomes unassigned so another node can claim it via fight-for-master.
   *
   * @param dataSource Database connection source
   * @param tableName The table to revert
   * @param nodeId The node ID that currently owns the table (must match for revert to succeed)
   * @throws SQLException if the update fails
   */
  public static void revertClaim(DataSource dataSource, String tableName, String nodeId)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(REVERT_CLAIM_SQL)) {
      ps.setString(1, tableName);
      ps.setString(2, nodeId);
      int affected = ps.executeUpdate();
      if (affected == 1) {
        logger.info("Reverted claim for table '{}' (node='{}')", tableName, nodeId);
      } else {
        logger.debug(
            "Revert claim for '{}' affected 0 rows (ownership already changed)", tableName);
      }
    }
  }

  /**
   * Update the progress timestamp for a coordinator.
   *
   * <p>Called by the watchdog when a coordinator is healthy. The {@code AND node_id = ?} clause
   * prevents a coordinator that lost ownership from overwriting the new owner's progress.
   *
   * @param dataSource Database connection source
   * @param tableName The table being coordinated
   * @param nodeId The node ID that owns the table (must match for update to succeed)
   * @return true if the update succeeded (affected_rows = 1), false otherwise
   * @throws SQLException if the update fails
   */
  public static boolean updateProgress(DataSource dataSource, String tableName, String nodeId)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(UPDATE_PROGRESS_SQL)) {
      ps.setString(1, tableName);
      ps.setString(2, nodeId);
      return ps.executeUpdate() == 1;
    }
  }

  /**
   * Get the set of table names currently assigned to a specific node.
   *
   * <p>Used by ownership verification to detect when a running unit has lost its assignment.
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier
   * @return Set of table names assigned to this node
   * @throws SQLException if the query fails
   */
  public static Set<String> getAssignedTableNames(DataSource dataSource, String nodeId)
      throws SQLException {
    Set<String> tables = new HashSet<>();
    String sql = "SELECT table_name FROM _table_assignment WHERE node_id = ?";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, nodeId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString("table_name"));
        }
      }
    }

    return tables;
  }

  // ==================== Lease Mode (Phase 2 alternative) ====================

  /**
   * Renew leases for all tables owned by this node.
   *
   * <p>Sets {@code lease_expiry = NOW() + ttlSeconds} for every row where {@code node_id} matches.
   * Called periodically by the lease renewal thread.
   *
   * @param dataSource Database connection source
   * @param nodeId The node identifier
   * @param ttlSeconds Lease duration in seconds
   * @return Number of leases renewed
   * @throws SQLException if the update fails
   */
  public static int renewLeases(DataSource dataSource, String nodeId, int ttlSeconds)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(RENEW_LEASES_SQL)) {
      ps.setInt(1, ttlSeconds);
      ps.setString(2, nodeId);
      int renewed = ps.executeUpdate();
      logger.debug("Renewed {} lease(s) for node '{}' (ttl={}s)", renewed, nodeId, ttlSeconds);
      return renewed;
    }
  }

  /**
   * Find tables with expired or missing leases.
   *
   * <p>A table has an expired lease when:
   *
   * <ul>
   *   <li>It has a non-null node_id (currently owned), AND
   *   <li>{@code lease_expiry < NOW()} (lease expired), OR
   *   <li>{@code lease_expiry IS NULL} (leftover from heartbeat mode after a mode switch)
   * </ul>
   *
   * @param dataSource Database connection source
   * @return List of tables with expired leases and their current owner IDs
   * @throws SQLException if the query fails
   */
  public static List<OrphanedTable> findExpiredLeases(DataSource dataSource) throws SQLException {
    List<OrphanedTable> expired = new ArrayList<>();

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_EXPIRED_LEASES_SQL);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        expired.add(new OrphanedTable(rs.getString("table_name"), rs.getString("node_id")));
      }
    }

    return expired;
  }

  /**
   * Atomically claim a table whose lease has expired.
   *
   * <p>The {@code WHERE lease_expiry < NOW() OR lease_expiry IS NULL} condition ensures only one
   * node succeeds when multiple nodes race to claim the same expired lease. If the lease was
   * renewed by the original owner (or claimed by another node) between detection and this call, the
   * update affects zero rows and returns false.
   *
   * @param dataSource Database connection source
   * @param tableName The table to claim
   * @param newOwnerId The node ID of the new owner
   * @param ttlSeconds Lease duration for the new owner
   * @return true if the claim succeeded (affected_rows = 1), false otherwise
   * @throws SQLException if the update fails
   */
  public static boolean claimExpiredLease(
      DataSource dataSource, String tableName, String newOwnerId, int ttlSeconds)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(CLAIM_EXPIRED_LEASE_SQL)) {
      ps.setString(1, newOwnerId);
      ps.setInt(2, ttlSeconds);
      ps.setString(3, tableName);
      int affected = ps.executeUpdate();
      return affected == 1;
    }
  }
}
