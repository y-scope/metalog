package com.yscope.metalog.common.config.db;

import com.yscope.metalog.common.config.ServiceConfig;
import com.yscope.metalog.common.config.Timeouts;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Factory for creating HikariDataSource instances with consistent configuration.
 *
 * <p>Centralizes database connection pool creation to ensure:
 *
 * <ul>
 *   <li>Consistent timeout and pool settings across the application
 *   <li>Single point of configuration for connection parameters
 *   <li>Easy testing and maintenance
 * </ul>
 */
public final class DataSourceFactory {
  private DataSourceFactory() {}

  /**
   * Create a HikariDataSource from ServiceConfig.
   *
   * @param config Application configuration
   * @return Configured HikariDataSource
   */
  public static HikariDataSource create(ServiceConfig config) {
    return create(
        config.getJdbcUrl(),
        config.getDatabaseUser(),
        config.getDatabasePassword(),
        config.getDatabasePoolMinIdle(),
        config.getDatabasePoolMaxSize(),
        "Coordinator");
  }

  /**
   * Create a HikariDataSource from DatabaseConfig.
   *
   * @param dbConfig Database configuration from node config
   * @param poolName Name for the connection pool (for monitoring)
   * @return Configured HikariDataSource
   */
  public static HikariDataSource create(DatabaseConfig dbConfig, String poolName) {
    return create(
        dbConfig.getJdbcUrl(),
        dbConfig.getUser(),
        dbConfig.getPassword(),
        dbConfig.getPoolMinIdle(),
        dbConfig.getPoolSize(),
        poolName,
        dbConfig.getConnectionTimeout(),
        dbConfig.getIdleTimeout());
  }

  /**
   * Create a HikariDataSource with standard timeouts.
   *
   * @param jdbcUrl JDBC URL for the database
   * @param user Database username
   * @param password Database password
   * @param minIdle Minimum idle connections
   * @param maxPoolSize Maximum pool size
   * @param poolName Name for the connection pool
   * @return Configured HikariDataSource
   */
  public static HikariDataSource create(
      String jdbcUrl, String user, String password, int minIdle, int maxPoolSize, String poolName) {
    return create(
        jdbcUrl,
        user,
        password,
        minIdle,
        maxPoolSize,
        poolName,
        Timeouts.CONNECTION_TIMEOUT_MS,
        Timeouts.IDLE_TIMEOUT_MS);
  }

  /**
   * Create a HikariDataSource with custom timeouts.
   *
   * @param jdbcUrl JDBC URL for the database
   * @param user Database username
   * @param password Database password
   * @param minIdle Minimum idle connections
   * @param maxPoolSize Maximum pool size
   * @param poolName Name for the connection pool
   * @param connectionTimeoutMs Connection timeout in milliseconds
   * @param idleTimeoutMs Idle timeout in milliseconds
   * @return Configured HikariDataSource
   */
  public static HikariDataSource create(
      String jdbcUrl,
      String user,
      String password,
      int minIdle,
      int maxPoolSize,
      String poolName,
      long connectionTimeoutMs,
      long idleTimeoutMs) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(user);
    config.setPassword(password);
    config.setMinimumIdle(minIdle);
    config.setMaximumPoolSize(maxPoolSize);
    config.setConnectionTimeout(connectionTimeoutMs);
    config.setIdleTimeout(idleTimeoutMs);
    config.setPoolName(poolName);
    return new HikariDataSource(config);
  }
}
