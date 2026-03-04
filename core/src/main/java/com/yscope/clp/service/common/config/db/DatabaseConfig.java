package com.yscope.clp.service.common.config.db;

/** Database connection pool configuration. */
public class DatabaseConfig {
  private String host = "localhost";
  private int port = 3306;
  private String database = "metalog_metastore";
  private String user = "root";
  private String password = "";

  /** Maximum pool size (shared across all units). */
  private int poolSize = 20;

  /** Minimum idle connections. */
  private int poolMinIdle = 5;

  /** Connection timeout in milliseconds. */
  private long connectionTimeout = 30000;

  /** Idle timeout in milliseconds. */
  private long idleTimeout = 600000;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public int getPoolSize() {
    return poolSize;
  }

  public void setPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }

  public int getPoolMinIdle() {
    return poolMinIdle;
  }

  public void setPoolMinIdle(int poolMinIdle) {
    this.poolMinIdle = poolMinIdle;
  }

  public long getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setConnectionTimeout(long connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  public void setIdleTimeout(long idleTimeout) {
    this.idleTimeout = idleTimeout;
  }

  /** Build JDBC URL from configuration. */
  public String getJdbcUrl() {
    return JdbcUrlBuilder.buildSimple(host, port, database);
  }
}
