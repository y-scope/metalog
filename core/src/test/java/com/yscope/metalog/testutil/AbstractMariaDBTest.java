package com.yscope.metalog.testutil;

import com.yscope.metalog.common.config.db.DSLContextFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.MariaDBContainer;

/**
 * Base class for integration tests that require a real MariaDB instance.
 *
 * <p>Provides a shared MariaDB container (one per JVM) and per-test database isolation via {@code
 * CREATE DATABASE test_<uuid>}. Subclasses get a fresh {@link DSLContext} and {@link
 * HikariDataSource} for each test, with the full schema loaded.
 */
@Testcontainers
public abstract class AbstractMariaDBTest {

  @Container
  static final MariaDBContainer<?> MARIADB =
      new MariaDBContainer<>("mariadb:10.3.32")
          .withUsername("root")
          .withPassword("")  // Empty password with MARIADB_ALLOW_EMPTY_ROOT_PASSWORD
          .withEnv("MARIADB_ALLOW_EMPTY_ROOT_PASSWORD", "yes")  // Avoids entrypoint user creation bug
          .withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_bin");

  private HikariDataSource dataSource;
  private DSLContext dsl;
  private String testDbName;

  @BeforeEach
  void setUpDatabase() throws Exception {
    testDbName = "test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);

    // Create isolated database using root connection with allowMultiQueries for schema loading
    String rootUrl = MARIADB.getJdbcUrl().replace("/test", "");
    try (Connection conn =
        DriverManager.getConnection(rootUrl, MARIADB.getUsername(), MARIADB.getPassword())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE DATABASE " + testDbName);
      }
    }

    // Load schema using allowMultiQueries to handle multi-statement SQL
    String baseUrl = MARIADB.getJdbcUrl().replace("/test", "/" + testDbName);
    String separator = baseUrl.contains("?") ? "&" : "?";
    String schemaUrl = baseUrl + separator + "allowMultiQueries=true";
    try (Connection conn =
        DriverManager.getConnection(schemaUrl, MARIADB.getUsername(), MARIADB.getPassword())) {
      String schemaSql = loadSchema();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(schemaSql);
      }
    }

    // Create HikariCP pool for the test database
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(MARIADB.getJdbcUrl().replace("/test", "/" + testDbName));
    config.setUsername(MARIADB.getUsername());
    config.setPassword(MARIADB.getPassword());
    config.setMaximumPoolSize(2);
    config.setMinimumIdle(1);
    dataSource = new HikariDataSource(config);

    dsl = DSLContextFactory.create(dataSource);
  }

  @AfterEach
  void tearDownDatabase() {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
    // Drop the test database to free resources
    try {
      String rootUrl = MARIADB.getJdbcUrl().replace("/test", "");
      try (Connection conn =
          DriverManager.getConnection(rootUrl, MARIADB.getUsername(), MARIADB.getPassword())) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DROP DATABASE IF EXISTS " + testDbName);
        }
      }
    } catch (SQLException e) {
      // Best-effort cleanup; container will be destroyed anyway
    }
  }

  protected DSLContext getDsl() {
    return dsl;
  }

  protected HikariDataSource getDataSource() {
    return dataSource;
  }

  protected String getTestDbName() {
    return testDbName;
  }

  /**
   * Register a table in the _table registry. Many metastore classes require a valid _table entry
   * due to foreign key constraints (e.g., _task_queue, _dim_registry, _agg_registry).
   *
   * <p>Creates the table from the _clp_template and registers it in the _table registry.
   *
   * @param tableName the table name to register
   */
  protected void registerTable(String tableName) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      // Create table from _clp_template
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " LIKE _clp_template");
      }

      // Register in _table registry
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO _table (table_name, display_name) VALUES (?, ?)")) {
        ps.setString(1, tableName);
        ps.setString(2, tableName);
        ps.executeUpdate();
      }
    }
  }

  private static String loadSchema() throws IOException {
    try (InputStream is =
        AbstractMariaDBTest.class.getResourceAsStream("/schema/schema.sql")) {
      if (is == null) {
        throw new IOException("schema.sql not found on classpath");
      }
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
