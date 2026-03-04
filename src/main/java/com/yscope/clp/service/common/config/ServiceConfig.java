package com.yscope.clp.service.common.config;

import com.yscope.clp.service.common.config.db.JdbcUrlBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service configuration loader.
 *
 * <p>Loads configuration from application.properties and environment variables. Environment
 * variables override properties file values.
 */
public class ServiceConfig {
  private static final Logger logger = LoggerFactory.getLogger(ServiceConfig.class);
  private final Properties props;

  public ServiceConfig() {
    this.props = new Properties();
    loadProperties();
  }

  private void loadProperties() {
    // Load from classpath
    try (InputStream is =
        getClass().getClassLoader().getResourceAsStream("config/application.properties")) {
      if (is != null) {
        props.load(is);
        logger.info("Loaded configuration from application.properties");
      } else {
        logger.warn("application.properties not found, using defaults");
      }
    } catch (IOException e) {
      logger.error("Failed to load application.properties", e);
    }
  }

  private String get(String key) {
    // Priority: Environment variable > System property > Properties file
    String envKey = key.toUpperCase().replace('.', '_');
    String envValue = System.getenv(envKey);
    if (envValue != null) {
      return envValue;
    }
    // Check system properties (used by tests)
    String sysProp = System.getProperty(key);
    if (sysProp != null) {
      return sysProp;
    }
    return props.getProperty(key);
  }

  private String get(String key, String defaultValue) {
    String value = get(key);
    return value != null ? value : defaultValue;
  }

  private int getInt(String key, int defaultValue) {
    String value = get(key);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  private long getLong(String key, long defaultValue) {
    String value = get(key);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  // Kafka Configuration
  public String getKafkaBootstrapServers() {
    return get("kafka.bootstrap.servers", "localhost:9092");
  }

  public String getKafkaGroupId() {
    return get("kafka.group.id", "coordinator");
  }

  public String getKafkaTopic() {
    return get("kafka.topic", "clp-metadata");
  }

  public int getKafkaMaxPollRecords() {
    return getInt("kafka.consumer.max.poll.records", 500);
  }

  // Database Configuration (supports MySQL, MariaDB, and cloud variants)
  // Database type and capabilities are auto-detected from connection

  // Cached detected database info (set after first connection via auto-detection)
  private volatile com.yscope.clp.service.common.config.db.DatabaseTypeDetector.DatabaseInfo
      detectedDatabaseInfo = null;

  /**
   * Get the detected database type as a simple string.
   *
   * <p>Type is auto-detected from the database connection by querying VERSION().
   *
   * @return "mysql", "mariadb", or "vitess"
   */
  public String getDatabaseType() {
    if (detectedDatabaseInfo != null) {
      return com.yscope.clp.service.common.config.db.DatabaseTypeDetector.toSimpleTypeName(
          detectedDatabaseInfo.getType());
    }
    return "mysql";
  }

  /**
   * Get the full detected database info including version and capabilities.
   *
   * @return DatabaseInfo or null if not yet detected
   */
  public com.yscope.clp.service.common.config.db.DatabaseTypeDetector.DatabaseInfo
      getDetectedDatabaseInfo() {
    return detectedDatabaseInfo;
  }

  /**
   * Set the detected database info after auto-detection. Called by startup code after connecting to
   * the database.
   *
   * @param info The detected database info
   */
  public void setDetectedDatabaseInfo(
      com.yscope.clp.service.common.config.db.DatabaseTypeDetector.DatabaseInfo info) {
    this.detectedDatabaseInfo = info;
    logger.info("Database detected: {}", info);
  }

  /**
   * @deprecated Use {@link
   *     #setDetectedDatabaseInfo(com.yscope.clp.service.common.config.db.DatabaseTypeDetector.DatabaseInfo)}
   *     instead.
   */
  @Deprecated
  public void setDetectedDatabaseType(String type) {
    // Legacy compatibility - create minimal info
    logger.info("Database type set (legacy): {}", type);
  }

  /** Check if the connected database is MariaDB. */
  public boolean isMariaDB() {
    return "mariadb".equals(getDatabaseType());
  }

  /**
   * Get the compression type to use for tables.
   *
   * <p>Priority:
   *
   * <ol>
   *   <li>User-configured value (db.compression)
   *   <li>Auto-detected (LZ4 for MySQL/MariaDB, none for TiDB/Vitess)
   * </ol>
   *
   * @return "lz4" or "none"
   */
  public String getCompression() {
    // User override takes priority
    String userOverride = get("db.compression");
    if (userOverride != null && !userOverride.isEmpty()) {
      String normalized = userOverride.toLowerCase().trim();
      if ("lz4".equals(normalized) || "none".equals(normalized)) {
        return normalized;
      }
      logger.warn("Invalid db.compression value '{}', using auto-detected", userOverride);
    }

    // Fall back to auto-detected
    if (detectedDatabaseInfo != null) {
      return detectedDatabaseInfo.getRecommendedCompression().name().toLowerCase();
    }
    return "lz4"; // Default to LZ4 for MySQL 8.0+
  }

  /**
   * Get the SQL compression clause for CREATE TABLE.
   *
   * @return SQL clause like "COMPRESSION='lz4'" or empty string for none
   */
  public String getCompressionClause() {
    String compression = getCompression();
    if ("none".equals(compression)) {
      return "";
    }
    return "COMPRESSION='" + compression + "'";
  }

  /** Check if LZ4 compression is being used. */
  public boolean isLz4Compression() {
    return "lz4".equals(getCompression());
  }

  public String getDatabaseHost() {
    return get("db.host", "localhost");
  }

  public int getDatabasePort() {
    return getInt("db.port", 3306);
  }

  public String getDatabaseName() {
    return get("db.database", "clp_metastore");
  }

  public String getDatabaseUser() {
    return get("db.user", "root");
  }

  public String getDatabasePassword() {
    return get("db.password", "password");
  }

  public String getDatabaseTable() {
    return get("db.table", "clp_table");
  }

  /**
   * Build JDBC URL using MariaDB Connector/J (works with both MySQL and MariaDB servers).
   *
   * <p>Database type is auto-detected after connection via {@link
   * #setDetectedDatabaseInfo(DatabaseTypeDetector.DatabaseInfo)}.
   */
  public String getJdbcUrl() {
    return JdbcUrlBuilder.buildOptimized(getDatabaseHost(), getDatabasePort(), getDatabaseName());
  }

  public int getDatabasePoolMaxSize() {
    return getInt("db.pool.maximum.size", 10);
  }

  public int getDatabasePoolMinIdle() {
    return getInt("db.pool.minimum.idle", 2);
  }

  // Storage Configuration (multi-backend)

  public String getStorageDefaultBackend() {
    return get("storage.default.backend", "minio");
  }

  public String getStorageIrBucket() {
    return get("storage.ir.bucket", get("minio.ir.bucket", "clp-ir"));
  }

  public String getStorageArchiveBucket() {
    return get("storage.archive.bucket", get("minio.archive.bucket", "clp-archives"));
  }

  /**
   * Build a map of backend name to ObjectStorageConfig from properties.
   *
   * <p>Supports new multi-backend format (storage.backends.NAME.*) and falls back to legacy minio.*
   * properties as a single "minio" backend.
   */
  public Map<String, ObjectStorageConfig> getStorageBackends() {
    Map<String, ObjectStorageConfig> backends = new LinkedHashMap<>();

    // Check for new-style multi-backend properties
    String minioEndpoint = get("storage.backends.minio.endpoint");
    if (minioEndpoint != null) {
      // New-style properties exist — scan for all backends
      // For now, support "minio" backend from properties
      backends.put("minio", buildBackendFromProperties("storage.backends.minio"));
      return backends;
    }

    // Fall back to legacy minio.* properties as a single "minio" backend
    ObjectStorageConfig legacy = new ObjectStorageConfig();
    legacy.setEndpoint(get("minio.endpoint", "http://localhost:9000"));
    legacy.setAccessKey(get("minio.access.key", "minioadmin"));
    legacy.setSecretKey(get("minio.secret.key", "minioadmin"));
    legacy.setRegion(get("minio.region", "us-east-1"));
    legacy.setForcePathStyle(true);
    backends.put("minio", legacy);
    return backends;
  }

  private ObjectStorageConfig buildBackendFromProperties(String prefix) {
    ObjectStorageConfig config = new ObjectStorageConfig();
    config.setEndpoint(get(prefix + ".endpoint", "http://localhost:9000"));
    config.setAccessKey(get(prefix + ".access.key", "minioadmin"));
    config.setSecretKey(get(prefix + ".secret.key", "minioadmin"));
    config.setRegion(get(prefix + ".region", "us-east-1"));
    String forcePathStyle = get(prefix + ".force.path.style", "true");
    config.setForcePathStyle(Boolean.parseBoolean(forcePathStyle));
    return config;
  }

  // Policy Configuration
  public String getPolicyType() {
    return get("policy.type", "spark");
  }

  public long getSparkTimeoutHours() {
    return getLong("policy.spark.trigger.timeout.hours", 4);
  }

  public long getSparkTargetSizeMb() {
    return getLong("policy.spark.target.archive.size.mb", 64);
  }

  // Coordinator Configuration
  public long getCoordinatorLoopIntervalMs() {
    return getLong("coordinator.loop.interval.ms", 5000);
  }

  public long getTaskTimeoutMinutes() {
    return getLong("coordinator.task.timeout.minutes", 10);
  }

  /**
   * Maximum number of expired files to delete per deletion run.
   *
   * <p>Limits the batch size to avoid long-running transactions and excessive storage API calls in
   * a single deletion cycle.
   *
   * <p>Default: 1000
   */
  public int getDeletionBatchSize() {
    return getInt("coordinator.deletion.batch.size", 1000);
  }

  /**
   * Enable deletion of expired files.
   *
   * <p>When disabled, expired files remain in MySQL and storage until manually cleaned up. Useful
   * for testing or when retention is managed externally.
   *
   * <p>Default: false (disabled for initial POC)
   */
  public boolean isDeletionEnabled() {
    String value = get("coordinator.deletion.enabled", "false");
    return Boolean.parseBoolean(value);
  }

  // Worker Configuration (separate JVM processes that poll database directly)

  public int getWorkerThreadCount() {
    // Default to 4 threads - safe for most environments.
    // In containers, set worker.threads explicitly based on cgroup CPU limit.
    return getInt("worker.threads", 4);
  }

  public int getWorkerDbPoolMin() {
    return getInt("worker.db.pool.min", 1);
  }

  public int getWorkerDbPoolMax() {
    return getInt("worker.db.pool.max", 3);
  }

  public long getWorkerDbPoolIdleTimeoutMs() {
    return getLong("worker.db.pool.idle.timeout.ms", 600000); // 10 minutes default
  }

  public long getWorkerPollIntervalMs() {
    return getLong("worker.poll.interval.ms", 5000);
  }

  public String getWorkerId() {
    String workerId = get("worker.id");
    if (workerId != null) {
      return workerId;
    }
    // Fallback: generate from hostname if available
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (java.net.UnknownHostException e) {
      return "worker-" + System.currentTimeMillis();
    }
  }

  // CLP Binary Configuration

  public String getClpBinaryPath() {
    return get("clp.binary.path", "/usr/bin/clp-s");
  }

  public long getClpProcessTimeoutSeconds() {
    return getLong("clp.process.timeout.seconds", 300);
  }

  // Logging
  public String getLogLevel() {
    return get("log.level", "INFO");
  }

  // Schema Evolution Configuration
  // See: docs/guides/evolve-schema.md (Schema Evolution)

  /**
   * Enable automatic schema evolution for dimension and count columns. When enabled, new dim_* and
   * count_* fields in metadata records will automatically trigger ALTER TABLE ADD COLUMN.
   *
   * <p>Default: false (disabled for safety - manual schema changes recommended for production)
   */
  public boolean isSchemaEvolutionEnabled() {
    String value = get("schema.evolution.enabled", "false");
    return Boolean.parseBoolean(value);
  }

  /**
   * Maximum number of dimension columns allowed per table. Prevents unbounded schema growth.
   *
   * <p>Default: 50
   */
  public int getSchemaEvolutionMaxDimColumns() {
    return getInt("schema.evolution.max.dim.columns", 50);
  }

  /**
   * Maximum number of count columns allowed per table. Prevents unbounded schema growth.
   *
   * <p>Default: 20
   */
  public int getSchemaEvolutionMaxCountColumns() {
    return getInt("schema.evolution.max.count.columns", 20);
  }

  // Graceful Shutdown Configuration

  /**
   * Grace period in milliseconds to wait for in-flight tasks to complete during shutdown.
   *
   * <p>Default: 30000 (30 seconds)
   */
  public long getShutdownGracePeriodMs() {
    return getLong("coordinator.shutdown.grace.period.ms", 30000);
  }

  // Storage Deletion Thread Configuration

  /**
   * Delay in milliseconds between storage deletion operations. Used to rate-limit deletion requests
   * to object storage.
   *
   * <p>Default: 100 (10 deletions per second)
   */
  public long getStorageDeletionDelayMs() {
    return getLong("coordinator.storage.deletion.delay.ms", 100);
  }

  // Partition Manager Configuration

  /**
   * Interval in milliseconds between partition maintenance runs.
   *
   * <p>Default: 3600000 (1 hour)
   */
  public long getPartitionMaintenanceIntervalMs() {
    return getLong("coordinator.partition.maintenance.interval.ms", 3600000);
  }

  /**
   * Number of days to create partitions ahead of current date.
   *
   * <p>Default: 7
   */
  public int getPartitionLookaheadDays() {
    return getInt("coordinator.partition.lookahead.days", 7);
  }

  /**
   * Age in days after which empty partitions can be dropped.
   *
   * <p>Default: 30
   */
  public int getPartitionCleanupAgeDays() {
    return getInt("coordinator.partition.cleanup.age.days", 30);
  }

  /**
   * Row count threshold below which partitions are considered sparse. Sparse partitions may be
   * candidates for merging.
   *
   * <p>Default: 1000
   */
  public long getPartitionSparseRowThreshold() {
    return getLong("coordinator.partition.sparse.row.threshold", 1000);
  }

  // Policy Configuration Paths

  /**
   * Path to the policy YAML configuration file. If not set, uses classpath resource policy.yaml.
   *
   * <p>Default: null (use classpath)
   */
  public String getPolicyConfigPath() {
    return get("coordinator.policy.config.path");
  }

  /**
   * Path to the index YAML configuration file. If not set, uses classpath resource index.yaml.
   *
   * <p>Default: null (use classpath)
   */
  public String getIndexConfigPath() {
    return get("coordinator.index.config.path");
  }

  /**
   * Path to the schema SQL file. If not set, uses classpath resource schema.sql.
   *
   * <p>Default: null (use classpath schema.sql)
   *
   * <p>Environment variable: COORDINATOR_SCHEMA_CONFIG_PATH
   */
  public String getSchemaConfigPath() {
    return get("coordinator.schema.config.path");
  }

  /**
   * Enable hot-reload of policy configuration.
   *
   * <p>Default: true
   */
  public boolean isPolicyHotReloadEnabled() {
    String value = get("coordinator.policy.hot.reload.enabled", "true");
    return Boolean.parseBoolean(value);
  }

  /**
   * Enable hot-reload of index configuration.
   *
   * <p>Default: true
   */
  public boolean isIndexHotReloadEnabled() {
    String value = get("coordinator.index.hot.reload.enabled", "true");
    return Boolean.parseBoolean(value);
  }

  // ============================================================
  // Feature Flags
  // ============================================================
  //
  // For minimal POC mode (Kafka → validation → database only):
  //   COORDINATOR_CONSOLIDATION_ENABLED=false
  //   COORDINATOR_DELETION_ENABLED=false
  //
  // This disables consolidation planning and file deletion,
  // leaving only: Kafka consumption → processing → batch write.
  //

  /**
   * Enable consolidation (planning and execution).
   *
   * <p>When disabled:
   *
   * <ul>
   *   <li>Planner thread does not create consolidation tasks
   *   <li>Files remain in IR state, never consolidated to archives
   *   <li>Useful for POC/testing of ingestion pipeline only
   * </ul>
   *
   * <p>Default: false (disabled for initial POC)
   */
  public boolean isConsolidationEnabled() {
    String value = get("coordinator.consolidation.enabled", "false");
    return Boolean.parseBoolean(value);
  }

  // 4-Thread Architecture Configuration

  /**
   * Enable the partition manager thread.
   *
   * <p>The partition manager creates lookahead partitions and merges sparse partitions for MySQL
   * RANGE partitioning. This is only needed for direct MySQL/MariaDB deployments.
   *
   * <p>Automatically disabled when Vitess is detected, as Vitess manages sharding at its own layer.
   * Can be explicitly overridden via config.
   *
   * <p>Default: true for MySQL/MariaDB, false for Vitess
   */
  public boolean isPartitionManagerThreadEnabled() {
    String explicit = get("coordinator.partition.manager.thread.enabled");
    if (explicit != null) {
      // Explicit config overrides auto-detection
      return Boolean.parseBoolean(explicit);
    }
    // Auto-disable for Vitess (and other non-partition-supporting systems)
    return isPartitioningSupported();
  }

  /**
   * Check if the detected database supports MySQL RANGE partitioning.
   *
   * <p>Returns true for direct MySQL, MariaDB, Aurora, Percona, TiDB. Returns false for Vitess
   * (manages sharding at its own layer).
   */
  public boolean isPartitioningSupported() {
    if (detectedDatabaseInfo != null) {
      return detectedDatabaseInfo.supportsPartitioning();
    }
    // Default to true until detection runs
    return true;
  }

  // ============================================================
  // 5-Thread Architecture Configuration
  // ============================================================

  /**
   * Interval in milliseconds between planner cycles.
   *
   * <p>The Planner thread runs on a timer, querying the database for pending files and creating
   * consolidation tasks.
   *
   * <p>Default: 5000 (5 seconds)
   */
  public long getPlannerIntervalMs() {
    return getLong("coordinator.planner.interval.ms", 5000);
  }

  /**
   * High watermark for backpressure on task creation.
   *
   * <p>When the pending task count in the database exceeds this threshold, the Planner pauses task
   * creation to allow the system to catch up.
   *
   * <p>Default: 1000
   */
  public int getBackpressureHighWatermark() {
    return getInt("coordinator.backpressure.high.watermark", 1000);
  }

  /**
   * Interval in milliseconds between Writer thread loop iterations.
   *
   * <p>Default: 100
   */
  public long getWriterLoopIntervalMs() {
    return getLong("coordinator.writer.loop.interval.ms", 100);
  }

  /**
   * Timeout in milliseconds for tasks stuck in processing.
   *
   * <p>If a worker doesn't report completion within this time, the task is considered stale and
   * reclaimed by the Planner.
   *
   * <p>Default: 600000 (10 minutes)
   */
  public long getTaskTimeoutMs() {
    return getLong("coordinator.task.timeout.ms", 600000);
  }

  // ============================================================
  // Health Check Configuration
  // ============================================================

  /**
   * Enable health check HTTP server.
   *
   * <p>When enabled, exposes:
   *
   * <ul>
   *   <li>{@code /health/live} - Liveness probe (is the process alive?)
   *   <li>{@code /health/ready} - Readiness probe (is the process ready for traffic?)
   * </ul>
   *
   * <p>Default: true
   */
  public boolean isHealthCheckEnabled() {
    String value = get("coordinator.health.enabled", "true");
    return Boolean.parseBoolean(value);
  }

  /**
   * HTTP port for health check endpoints.
   *
   * <p>Default: 8081
   */
  public int getHealthCheckPort() {
    return getInt("coordinator.health.port", 8081);
  }
}
