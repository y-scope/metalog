package com.yscope.metalog.coordinator;

import com.yscope.metalog.metastore.schema.TableProvisioner;
import com.yscope.metalog.node.CoordinatorRegistry;
import com.yscope.metalog.node.NodeConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers metadata tables at runtime without requiring a node restart or config file edit.
 *
 * <p>Orchestrates the full registration workflow:
 *
 * <ol>
 *   <li>Check whether the table already exists (for the {@code created} flag)
 *   <li>UPSERT registry rows via {@link CoordinatorRegistry#upsertTables}
 *   <li>Provision the physical table via {@link TableProvisioner#ensureTable}
 * </ol>
 *
 * <p>All operations are fully idempotent — safe to call multiple times for the same table.
 */
public class TableRegistrationService {

  private static final Logger LOG = LoggerFactory.getLogger(TableRegistrationService.class);

  private final DataSource dataSource;

  public TableRegistrationService(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Result of a table registration attempt.
   *
   * @param tableName the registered table name
   * @param created true if the table was newly created, false if it already existed
   */
  public record RegistrationResult(String tableName, boolean created) {}

  /**
   * Register a table from its definition.
   *
   * @param definition the table definition (name, kafka config, feature flags)
   * @return result indicating whether the table was newly created
   * @throws SQLException if any database operation fails
   * @throws IllegalArgumentException if the definition is missing required fields
   */
  public RegistrationResult register(NodeConfig.TableDefinition definition) throws SQLException {
    String tableName = definition.getName();

    boolean alreadyExists = tableExists(tableName);

    CoordinatorRegistry.upsertTables(dataSource, List.of(definition));
    TableProvisioner.ensureTable(dataSource, tableName);

    LOG.info(
        "Table '{}' registered (created={})", tableName, !alreadyExists);

    return new RegistrationResult(tableName, !alreadyExists);
  }

  /**
   * Check whether a physical table exists in the current database.
   *
   * @param tableName table name to check
   * @return true if the table exists
   * @throws SQLException if the query fails
   */
  public boolean tableExists(String tableName) throws SQLException {
    String sql =
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }
}
