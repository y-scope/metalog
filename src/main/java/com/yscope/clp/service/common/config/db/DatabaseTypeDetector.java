package com.yscope.clp.service.common.config.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to detect database type and capabilities from a connection.
 *
 * <p>Supports MySQL, MariaDB, and MySQL-compatible cloud databases:
 *
 * <ul>
 *   <li>MySQL: {@code 8.0.33}
 *   <li>MariaDB: {@code 10.6.12-MariaDB}
 *   <li>Amazon Aurora: {@code 8.0.mysql_aurora.3.02.0}
 *   <li>TiDB: {@code 5.7.25-TiDB-v6.1.0}
 *   <li>Percona: {@code 8.0.32-24-Percona Server}
 *   <li>Amazon RDS, Google Cloud SQL, Azure: Report as standard MySQL
 * </ul>
 *
 * <p>All MySQL-compatible databases use the same SQL syntax (VIRTUAL columns, ON DUPLICATE KEY
 * UPDATE, RANGE partitioning) and work with this schema.
 */
public final class DatabaseTypeDetector {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseTypeDetector.class);

  /**
   * Database type categories. All types except MARIADB are MySQL-compatible and use MySQL syntax.
   */
  public enum DatabaseType {
    MYSQL, // Standard MySQL (including RDS, Cloud SQL, Azure)
    MARIADB, // MariaDB
    AURORA, // Amazon Aurora (MySQL-compatible)
    TIDB, // TiDB (MySQL-compatible distributed DB)
    PERCONA, // Percona Server (MySQL fork)
    VITESS, // Vitess/PlanetScale (MySQL-compatible)
    UNKNOWN
  }

  /** Compression algorithms for InnoDB page compression. */
  public enum CompressionType {
    LZ4, // Default - MySQL 8.0+, MariaDB 10.3+
    NONE // No compression (TiDB, Vitess)
  }

  /** Holds detected database information including type, version, and capabilities. */
  public static class DatabaseInfo {
    private final DatabaseType type;
    private final String versionString;
    private final int majorVersion;
    private final int minorVersion;
    private final CompressionType recommendedCompression;

    DatabaseInfo(DatabaseType type, String versionString, int majorVersion, int minorVersion) {
      this.type = type;
      this.versionString = versionString;
      this.majorVersion = majorVersion;
      this.minorVersion = minorVersion;
      this.recommendedCompression = determineCompression(type, majorVersion, minorVersion);
    }

    public DatabaseType getType() {
      return type;
    }

    public String getVersionString() {
      return versionString;
    }

    public int getMajorVersion() {
      return majorVersion;
    }

    public int getMinorVersion() {
      return minorVersion;
    }

    public CompressionType getRecommendedCompression() {
      return recommendedCompression;
    }

    public boolean supportsPartitioning() {
      return type != DatabaseType.VITESS;
    }

    public boolean supportsLz4() {
      return recommendedCompression == CompressionType.LZ4;
    }

    private static CompressionType determineCompression(DatabaseType type, int major, int minor) {
      return switch (type) {
        case TIDB, VITESS -> CompressionType.NONE;
        default -> CompressionType.LZ4;
      };
    }

    @Override
    public String toString() {
      return String.format(
          "%s %d.%d (compression: %s, partitioning: %s)",
          toDisplayName(type),
          majorVersion,
          minorVersion,
          recommendedCompression,
          supportsPartitioning() ? "yes" : "no");
    }
  }

  private DatabaseTypeDetector() {
    // Utility class
  }

  /**
   * Detect database type from a DataSource.
   *
   * @param dataSource The DataSource to check
   * @return Detected database type
   */
  public static DatabaseType detect(DataSource dataSource) {
    return detectInfo(dataSource).getType();
  }

  /**
   * Detect full database info including type, version, and capabilities.
   *
   * @param dataSource The DataSource to check
   * @return DatabaseInfo with type, version, and capability information
   */
  public static DatabaseInfo detectInfo(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection()) {
      if (conn == null) {
        LOG.warn("DataSource returned null connection, cannot detect database type");
        return new DatabaseInfo(DatabaseType.UNKNOWN, "unknown", 0, 0);
      }
      return detectInfo(conn);
    } catch (SQLException e) {
      LOG.warn("Failed to detect database info", e);
      return new DatabaseInfo(DatabaseType.UNKNOWN, "unknown", 0, 0);
    }
  }

  /**
   * Detect database type from an existing connection.
   *
   * @param conn The connection to check (not closed by this method)
   * @return Detected database type
   */
  public static DatabaseType detect(Connection conn) {
    return detectInfo(conn).getType();
  }

  /**
   * Detect full database info from an existing connection.
   *
   * @param conn The connection to check (not closed by this method)
   * @return DatabaseInfo with type, version, and capability information
   */
  public static DatabaseInfo detectInfo(Connection conn) {
    try {
      // Query VERSION() for the most reliable detection
      String version = queryVersion(conn);
      if (version != null) {
        return detectInfoFromVersion(version);
      }

      // Fallback: Check DatabaseMetaData
      DatabaseMetaData meta = conn.getMetaData();
      String productName = meta.getDatabaseProductName();
      String productVersion = meta.getDatabaseProductVersion();

      LOG.debug(
          "Database metadata fallback - product: {}, version: {}", productName, productVersion);

      if (productVersion != null) {
        return detectInfoFromVersion(productVersion);
      }
      if (productName != null && productName.toLowerCase().contains("mariadb")) {
        return new DatabaseInfo(DatabaseType.MARIADB, "unknown", 10, 2);
      }

      return new DatabaseInfo(DatabaseType.MYSQL, "unknown", 8, 0);

    } catch (SQLException e) {
      LOG.warn("Failed to detect database info", e);
      return new DatabaseInfo(DatabaseType.UNKNOWN, "unknown", 0, 0);
    }
  }

  /** Query SELECT VERSION() from the database. */
  private static String queryVersion(Connection conn) {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
      if (rs.next()) {
        return rs.getString(1);
      }
    } catch (SQLException e) {
      LOG.warn("Failed to query VERSION()", e);
    }
    return null;
  }

  /**
   * Detect database info from version string.
   *
   * <p>Version string examples:
   *
   * <ul>
   *   <li>MySQL: {@code 8.0.33}
   *   <li>MariaDB: {@code 10.6.12-MariaDB}
   *   <li>Aurora: {@code 8.0.mysql_aurora.3.02.0}
   *   <li>TiDB: {@code 5.7.25-TiDB-v6.1.0}
   *   <li>Percona: {@code 8.0.32-24-Percona Server}
   *   <li>Vitess: {@code 5.7.9-vitess-...}
   * </ul>
   */
  private static DatabaseInfo detectInfoFromVersion(String version) {
    if (version == null) {
      return new DatabaseInfo(DatabaseType.UNKNOWN, "unknown", 0, 0);
    }

    String lower = version.toLowerCase();
    int[] parsed = parseVersion(version);

    DatabaseType type = detectTypeFromVersionString(lower);
    DatabaseInfo info = new DatabaseInfo(type, version, parsed[0], parsed[1]);
    LOG.info("Detected database: {}", info);
    return info;
  }

  private static DatabaseType detectTypeFromVersionString(String lowerVersion) {
    if (lowerVersion.contains("mariadb")) {
      return DatabaseType.MARIADB;
    } else if (lowerVersion.contains("aurora")) {
      return DatabaseType.AURORA;
    } else if (lowerVersion.contains("tidb")) {
      return DatabaseType.TIDB;
    } else if (lowerVersion.contains("percona")) {
      return DatabaseType.PERCONA;
    } else if (lowerVersion.contains("vitess")) {
      return DatabaseType.VITESS;
    }
    return DatabaseType.MYSQL;
  }

  /**
   * Parse major and minor version from version string. Handles formats like "8.0.33",
   * "10.6.12-MariaDB", "5.7.25-TiDB-v6.1.0".
   *
   * @param version The version string
   * @return int array with [major, minor], defaults to [8, 0] if parsing fails
   */
  private static int[] parseVersion(String version) {
    if (version == null || version.isEmpty()) {
      return new int[] {8, 0}; // Default to MySQL 8.0
    }

    try {
      // Extract the numeric part at the beginning (e.g., "8.0.33" from "8.0.33-something")
      String[] parts = version.split("[^0-9]+");
      int major = parts.length > 0 && !parts[0].isEmpty() ? Integer.parseInt(parts[0]) : 8;
      int minor = parts.length > 1 && !parts[1].isEmpty() ? Integer.parseInt(parts[1]) : 0;
      return new int[] {major, minor};
    } catch (NumberFormatException e) {
      LOG.warn("Failed to parse version '{}', defaulting to 8.0", version);
      return new int[] {8, 0};
    }
  }

  /**
   * Check if the database is MariaDB.
   *
   * @param dataSource The DataSource to check
   * @return true if MariaDB, false otherwise
   */
  public static boolean isMariaDB(DataSource dataSource) {
    return detect(dataSource) == DatabaseType.MARIADB;
  }

  /**
   * Check if the database is MySQL or MySQL-compatible.
   *
   * <p>Returns true for: MySQL, Aurora, TiDB, Percona, Vitess
   *
   * <p>Returns false for: MariaDB (has some syntax differences)
   *
   * @param dataSource The DataSource to check
   * @return true if MySQL or MySQL-compatible, false otherwise
   */
  public static boolean isMySQLCompatible(DataSource dataSource) {
    return switch (detect(dataSource)) {
      case MYSQL, AURORA, TIDB, PERCONA, VITESS -> true;
      default -> false;
    };
  }

  /**
   * Get a simple type name for logging/config purposes.
   *
   * <p>Used by ServiceConfig to determine feature support:
   *
   * <ul>
   *   <li>"mysql" - Standard MySQL, Aurora, TiDB, Percona (partition management enabled)
   *   <li>"mariadb" - MariaDB (partition management enabled)
   *   <li>"vitess" - Vitess (partition management auto-disabled)
   * </ul>
   *
   * @param type The detected database type
   * @return "mysql", "mariadb", or "vitess"
   */
  public static String toSimpleTypeName(DatabaseType type) {
    return switch (type) {
      case MARIADB -> "mariadb";
      case VITESS -> "vitess";
      default -> "mysql";
    };
  }

  /**
   * Check if the database type supports MySQL RANGE partitioning.
   *
   * @param type The detected database type
   * @return true if partitioning is supported, false for Vitess
   */
  public static boolean supportsPartitioning(DatabaseType type) {
    // Vitess manages sharding at its own layer - MySQL partitions not recommended
    return type != DatabaseType.VITESS;
  }

  /**
   * Get a descriptive name for the database type.
   *
   * @param type The detected database type
   * @return Human-readable name (e.g., "Amazon Aurora", "TiDB")
   */
  public static String toDisplayName(DatabaseType type) {
    return switch (type) {
      case MYSQL -> "MySQL";
      case MARIADB -> "MariaDB";
      case AURORA -> "Amazon Aurora";
      case TIDB -> "TiDB";
      case PERCONA -> "Percona Server";
      case VITESS -> "Vitess";
      case UNKNOWN -> "Unknown";
    };
  }
}
