package com.yscope.metalog.common.config.db;

/**
 * Builder for constructing JDBC URLs with consistent parameter formatting.
 *
 * <p>Provides fluent API for building MariaDB/MySQL JDBC URLs with common connection parameters for
 * batching, prepared statement caching, and compression.
 *
 * <p>Example usage:
 *
 * <pre>
 * String url = new JdbcUrlBuilder("localhost", 3306, "mydb")
 *     .useSSL(false)
 *     .rewriteBatchedStatements(true)
 *     .build();
 * </pre>
 */
public final class JdbcUrlBuilder {
  private final StringBuilder url;
  private boolean hasParams = false;

  /**
   * Create a new JDBC URL builder for MariaDB/MySQL.
   *
   * @param host Database host
   * @param port Database port
   * @param database Database name
   */
  public JdbcUrlBuilder(String host, int port, String database) {
    this.url = new StringBuilder(String.format("jdbc:mariadb://%s:%d/%s", host, port, database));
  }

  /** Enable or disable SSL for the connection. */
  public JdbcUrlBuilder useSSL(boolean enabled) {
    return addParam("useSSL", enabled);
  }

  /**
   * Enable batched statement rewriting for efficient bulk inserts. Critical for performance with
   * batch INSERT operations.
   */
  public JdbcUrlBuilder rewriteBatchedStatements(boolean enabled) {
    return addParam("rewriteBatchedStatements", enabled);
  }

  /** Enable prepared statement caching. */
  public JdbcUrlBuilder cachePrepStmts(boolean enabled) {
    return addParam("cachePrepStmts", enabled);
  }

  /** Set the prepared statement cache size. */
  public JdbcUrlBuilder prepStmtCacheSize(int size) {
    return addParam("prepStmtCacheSize", size);
  }

  /** Set the maximum SQL length to cache. */
  public JdbcUrlBuilder prepStmtCacheSqlLimit(int limit) {
    return addParam("prepStmtCacheSqlLimit", limit);
  }

  /** Enable protocol compression. */
  public JdbcUrlBuilder useCompression(boolean enabled) {
    return addParam("useCompression", enabled);
  }

  /** Auto-create the database if it doesn't exist on first connection. */
  public JdbcUrlBuilder createDatabaseIfNotExist(boolean enabled) {
    return addParam("createDatabaseIfNotExist", enabled);
  }

  private JdbcUrlBuilder addParam(String key, Object value) {
    url.append(hasParams ? "&" : "?").append(key).append("=").append(value);
    hasParams = true;
    return this;
  }

  /** Build the final JDBC URL string. */
  public String build() {
    return url.toString();
  }

  /**
   * Build a fully optimized JDBC URL for production use.
   *
   * <p>Includes all performance optimizations:
   *
   * <ul>
   *   <li>Batch statement rewriting
   *   <li>Prepared statement caching
   *   <li>Protocol compression
   * </ul>
   *
   * @param host Database host
   * @param port Database port
   * @param database Database name
   * @return Optimized JDBC URL
   */
  public static String buildOptimized(String host, int port, String database) {
    return new JdbcUrlBuilder(host, port, database)
        .useSSL(false)
        .rewriteBatchedStatements(true)
        .cachePrepStmts(true)
        .prepStmtCacheSize(250)
        .prepStmtCacheSqlLimit(2048)
        .useCompression(true)
        .build();
  }

  /**
   * Build a simple JDBC URL with basic optimizations.
   *
   * <p>Suitable for simpler use cases without all optimizations.
   *
   * @param host Database host
   * @param port Database port
   * @param database Database name
   * @return Simple JDBC URL
   */
  public static String buildSimple(String host, int port, String database) {
    return new JdbcUrlBuilder(host, port, database)
        .useSSL(false)
        .createDatabaseIfNotExist(true)
        .rewriteBatchedStatements(true)
        .cachePrepStmts(true)
        .prepStmtCacheSize(250)
        .build();
  }
}
