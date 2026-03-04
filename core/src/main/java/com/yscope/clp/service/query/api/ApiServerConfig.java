package com.yscope.clp.service.query.api;

import com.yscope.clp.service.common.config.db.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;

/** API Server configuration. */
public record ApiServerConfig(
    int grpcPort,
    DataSourceConfig dataSource,
    CacheConfig cache,
    TimeoutConfig timeout,
    StreamingConfig streaming) {

  public static ApiServerConfig fromEnvironment() {
    return new ApiServerConfig(
        getEnvInt("API_GRPC_PORT", 9090),
        DataSourceConfig.fromEnvironment(),
        CacheConfig.fromEnvironment(),
        TimeoutConfig.fromEnvironment(),
        StreamingConfig.fromEnvironment());
  }

  /**
   * Build an ApiServerConfig from the node's own database configuration.
   *
   * <p>Used by {@link com.yscope.clp.service.node.NodeMain} when the API server is embedded
   * in-process. Derives JDBC URL, username, and password from the node's {@link DatabaseConfig}
   * so no separate {@code DB_JDBC_URL}/{@code DB_PASSWORD} environment variables are required.
   * Cache, timeout, and streaming settings still respect environment variables.
   *
   * @param db the node's database configuration
   * @param grpcPort the port for the embedded API server's gRPC endpoint
   */
  public static ApiServerConfig fromNodeDatabaseConfig(DatabaseConfig db, int grpcPort) {
    DataSourceConfig dsConfig =
        new DataSourceConfig(db.getJdbcUrl(), db.getUser(), db.getPassword(), db.getPoolSize(), true);
    return new ApiServerConfig(
        grpcPort,
        dsConfig,
        CacheConfig.fromEnvironment(),
        TimeoutConfig.fromEnvironment(),
        StreamingConfig.fromEnvironment());
  }

  private static int getEnvInt(String name, int defaultValue) {
    String value = System.getenv(name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  /** Database connection configuration for read replicas. */
  public record DataSourceConfig(
      String jdbcUrl, String username, String password, int poolSize, boolean readOnly) {
    public static DataSourceConfig fromEnvironment() {
      return new DataSourceConfig(
          System.getenv().getOrDefault("DB_JDBC_URL", "jdbc:mariadb://localhost:3306/metalog_metastore"),
          System.getenv().getOrDefault("DB_USERNAME", "root"),
          System.getenv().getOrDefault("DB_PASSWORD", ""),
          getEnvInt("DB_POOL_SIZE", 20),
          true // Always read-only for API server
          );
    }

    private static int getEnvInt(String name, int defaultValue) {
      String value = System.getenv(name);
      return value != null ? Integer.parseInt(value) : defaultValue;
    }

    public HikariConfig toHikariConfig() {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(jdbcUrl);
      config.setUsername(username);
      config.setPassword(password);
      config.setMaximumPoolSize(poolSize);
      config.setReadOnly(readOnly);
      config.setPoolName("api-server-pool");
      return config;
    }
  }

  /** Cache configuration. */
  public record CacheConfig(
      long maxSize,
      int schemaTtlSeconds,
      int queryTtlSeconds,
      int fileTtlSeconds,
      int sketchTtlSeconds) {
    public static CacheConfig fromEnvironment() {
      return new CacheConfig(
          getEnvLong("CACHE_MAX_SIZE", 100_000),
          getEnvInt("CACHE_SCHEMA_TTL_SECONDS", 300), // 5 min
          getEnvInt("CACHE_QUERY_TTL_SECONDS", 10), // 10 sec
          getEnvInt("CACHE_FILE_TTL_SECONDS", 30), // 30 sec
          getEnvInt("CACHE_SKETCH_TTL_SECONDS", 60) // 60 sec
          );
    }

    private static int getEnvInt(String name, int defaultValue) {
      String value = System.getenv(name);
      return value != null ? Integer.parseInt(value) : defaultValue;
    }

    private static long getEnvLong(String name, long defaultValue) {
      String value = System.getenv(name);
      return value != null ? Long.parseLong(value) : defaultValue;
    }
  }

  /**
   * Timeout configuration for API requests.
   *
   * <p>All timeouts support per-request override via request headers/parameters.
   */
  public record TimeoutConfig(
      /** Default request timeout in milliseconds */
      long requestTimeoutMs,

      /** Cursor TTL - how long a cursor remains valid before expiring */
      long cursorTtlMs,

      /**
       * Maximum time the server waits for the client's receive buffer to drain when full. Only
       * triggers if the client is completely stuck — a slow consumer never hits this.
       */
      long streamIdleTimeoutMs,

      /** Query execution timeout - maximum time for database query */
      long queryTimeoutMs) {
    public static TimeoutConfig fromEnvironment() {
      return new TimeoutConfig(
          getEnvLong("API_REQUEST_TIMEOUT_MS", 30_000), // 30 sec default
          getEnvLong("API_CURSOR_TTL_MS", 300_000), // 5 min cursor validity
          getEnvLong("API_STREAM_IDLE_TIMEOUT_MS", 60_000), // 1 min idle timeout
          getEnvLong("API_QUERY_TIMEOUT_MS", 120_000) // 2 min query timeout
          );
    }

    private static long getEnvLong(String name, long defaultValue) {
      String value = System.getenv(name);
      return value != null ? Long.parseLong(value) : defaultValue;
    }
  }

  /** Streaming configuration for gRPC streaming RPCs. */
  public record StreamingConfig(
      /** Number of rows to fetch per DB page in keyset pagination */
      int dbPageSize) {
    public static StreamingConfig fromEnvironment() {
      return new StreamingConfig(getEnvInt("API_STREAMING_DB_PAGE_SIZE", 1000));
    }

    private static int getEnvInt(String name, int defaultValue) {
      String value = System.getenv(name);
      return value != null ? Integer.parseInt(value) : defaultValue;
    }
  }
}
