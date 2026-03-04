package com.yscope.metalog.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yscope.metalog.common.config.ObjectStorageConfig;
import com.yscope.metalog.common.config.db.DatabaseConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Root configuration model for a multi-unit Node.
 *
 * <p>Coordinator assignments are driven by the database (via {@code CoordinatorRegistry}). Worker
 * threads are configured at the node level via the {@code worker} section.
 *
 * <p>Example node.yaml:
 *
 * <pre>
 * node:
 *   name: coordinator-node-1
 *   database:
 *     host: localhost
 *     port: 3306
 *     database: metalog_metastore
 *     user: root
 *     password: password
 *     poolSize: 20
 *   storage:
 *     defaultBackend: minio
 *     clpBinaryPath: /usr/bin/clp-s
 *     backends:
 *       minio:
 *         endpoint: http://localhost:9000
 *         accessKey: minioadmin
 *         secretKey: minioadmin
 *         forcePathStyle: true
 *   health:
 *     enabled: true
 *     port: 8081
 *   grpc:
 *     enabled: true
 *     port: 9090
 *     services:
 *       ingestion: true
 *       query: true
 *       catalog: true
 *       admin: true
 *   ingestion:
 *     kafka:
 *       recordsPerBatch: 500
 *       maxQueuedRecords: 5000
 *       flushTimeoutMs: 1000
 *     grpc:
 *       recordsPerBatch: 1000
 *       maxQueuedRecords: 5000
 *       flushTimeoutMs: 1000
 *   reconciliationIntervalSeconds: 60   # how often to claim unassigned tables
 *
 * worker:
 *   numWorkers: 4    # 0 = coordinator-only node
 *   # Optional: separate database for workers (e.g., read replica).
 *   # If omitted, workers use node.database.
 *   # database:
 *   #   host: replica-db
 *   #   port: 3306
 *   #   database: metalog_metastore
 *   #   user: reader
 *   #   password: reader-secret
 *   #   poolSize: 10
 *   #   poolMinIdle: 2
 * </pre>
 */
public class NodeConfig {

  @JsonProperty("node")
  private NodeSettings node = new NodeSettings();

  @JsonProperty("units")
  private List<UnitDefinition> units = new ArrayList<>();

  @JsonProperty("worker")
  private WorkerConfig worker = new WorkerConfig();

  @JsonProperty("tables")
  private List<TableDefinition> tables = new ArrayList<>();

  public NodeSettings getNode() {
    return node;
  }

  public void setNode(NodeSettings node) {
    this.node = node;
  }

  public List<UnitDefinition> getUnits() {
    return units;
  }

  public void setUnits(List<UnitDefinition> units) {
    this.units = units;
  }

  public WorkerConfig getWorker() {
    return worker;
  }

  public void setWorker(WorkerConfig worker) {
    this.worker = worker;
  }

  public List<TableDefinition> getTables() {
    return tables;
  }

  public void setTables(List<TableDefinition> tables) {
    this.tables = tables;
  }

  /** Node-level settings including shared resource configuration. */
  public static class NodeSettings {
    private String name = "coordinator-node";

    @JsonProperty("nodeIdEnvVar")
    private String nodeIdEnvVar = "HOSTNAME";

    @JsonProperty("database")
    private DatabaseConfig database = new DatabaseConfig();

    @JsonProperty("storage")
    private StorageConfig storage = new StorageConfig();

    @JsonProperty("health")
    private HealthConfig health = new HealthConfig();

    @JsonProperty("grpc")
    private GrpcConfig grpc = new GrpcConfig();

    @JsonProperty("ingestion")
    private IngestionConfig ingestion = new IngestionConfig();

    /** How often the node checks for new/unassigned tables (seconds). */
    @JsonProperty("reconciliationIntervalSeconds")
    private long reconciliationIntervalSeconds = 60;

    /** How often the node sends heartbeats to _node_registry (seconds). */
    @JsonProperty("heartbeatIntervalSeconds")
    private int heartbeatIntervalSeconds = 30;

    /** How long since last heartbeat before a node is considered dead (seconds). */
    @JsonProperty("deadNodeThresholdSeconds")
    private int deadNodeThresholdSeconds = 180; // 3 minutes = 6 missed heartbeats

    /** How often to scan for orphaned tables from dead nodes (seconds). */
    @JsonProperty("orphanScanIntervalSeconds")
    private int orphanScanIntervalSeconds = 60;

    /** How often to log throughput stats for each coordinator (seconds). 0 = disabled. */
    @JsonProperty("throughputLogIntervalSeconds")
    private int throughputLogIntervalSeconds = 60;

    /** HA strategy for coordinators: "heartbeat" (default) or "lease". */
    @JsonProperty("coordinatorHaStrategy")
    private String coordinatorHaStrategy = "heartbeat";

    /** Lease TTL in seconds (lease mode only). */
    @JsonProperty("leaseTtlSeconds")
    private int leaseTtlSeconds = 180;

    /** How often to renew leases in seconds (lease mode only, must be < leaseTtlSeconds). */
    @JsonProperty("leaseRenewalIntervalSeconds")
    private int leaseRenewalIntervalSeconds = 30;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getNodeIdEnvVar() {
      return nodeIdEnvVar;
    }

    public void setNodeIdEnvVar(String nodeIdEnvVar) {
      this.nodeIdEnvVar = nodeIdEnvVar;
    }

    public DatabaseConfig getDatabase() {
      return database;
    }

    public void setDatabase(DatabaseConfig database) {
      this.database = database;
    }

    public StorageConfig getStorage() {
      return storage;
    }

    public void setStorage(StorageConfig storage) {
      this.storage = storage;
    }

    public HealthConfig getHealth() {
      return health;
    }

    public void setHealth(HealthConfig health) {
      this.health = health;
    }

    public GrpcConfig getGrpc() {
      return grpc;
    }

    public void setGrpc(GrpcConfig grpc) {
      this.grpc = grpc;
    }

    public IngestionConfig getIngestion() {
      return ingestion;
    }

    public void setIngestion(IngestionConfig ingestion) {
      this.ingestion = ingestion;
    }

    public long getReconciliationIntervalSeconds() {
      return reconciliationIntervalSeconds;
    }

    public void setReconciliationIntervalSeconds(long reconciliationIntervalSeconds) {
      this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    public int getHeartbeatIntervalSeconds() {
      return heartbeatIntervalSeconds;
    }

    public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
      this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    public int getDeadNodeThresholdSeconds() {
      return deadNodeThresholdSeconds;
    }

    public void setDeadNodeThresholdSeconds(int deadNodeThresholdSeconds) {
      this.deadNodeThresholdSeconds = deadNodeThresholdSeconds;
    }

    public int getOrphanScanIntervalSeconds() {
      return orphanScanIntervalSeconds;
    }

    public void setOrphanScanIntervalSeconds(int orphanScanIntervalSeconds) {
      this.orphanScanIntervalSeconds = orphanScanIntervalSeconds;
    }

    public int getThroughputLogIntervalSeconds() {
      return throughputLogIntervalSeconds;
    }

    public void setThroughputLogIntervalSeconds(int throughputLogIntervalSeconds) {
      this.throughputLogIntervalSeconds = throughputLogIntervalSeconds;
    }

    public String getCoordinatorHaStrategy() {
      return coordinatorHaStrategy;
    }

    public void setCoordinatorHaStrategy(String coordinatorHaStrategy) {
      this.coordinatorHaStrategy = coordinatorHaStrategy;
    }

    public int getLeaseTtlSeconds() {
      return leaseTtlSeconds;
    }

    public void setLeaseTtlSeconds(int leaseTtlSeconds) {
      this.leaseTtlSeconds = leaseTtlSeconds;
    }

    public int getLeaseRenewalIntervalSeconds() {
      return leaseRenewalIntervalSeconds;
    }

    public void setLeaseRenewalIntervalSeconds(int leaseRenewalIntervalSeconds) {
      this.leaseRenewalIntervalSeconds = leaseRenewalIntervalSeconds;
    }
  }

  /**
   * Storage configuration with multi-backend support.
   *
   * <p>Backends can be any type (S3-compatible, HTTP, filesystem, etc.) Each backend is configured
   * with a map containing its type and type-specific parameters.
   *
   * <p>Bucket/namespace information is NOT specified here:
   *
   * <ul>
   *   <li>For reads: bucket/namespace is stored in the database per-record (each record knows where
   *       it lives)
   *   <li>For writes: bucket/namespace is determined by the policy configuration (retention,
   *       consolidation rules)
   * </ul>
   *
   * <p>Example YAML:
   *
   * <pre>{@code
   * storage:
   *   defaultBackend: tb
   *   backends:
   *     tb:
   *       type: tb
   *       baseUrl: http://localhost:19617
   *     gcs:
   *       type: gcs
   *       endpoint: https://storage.googleapis.com
   *       projectId: my-project
   * }</pre>
   */
  public static class StorageConfig {
    private String defaultBackend = "minio";
    private Map<String, Map<String, Object>> backends = new LinkedHashMap<>();
    private String clpBinaryPath = "/usr/bin/clp-s";
    private long clpProcessTimeoutSeconds = 300;

    public String getDefaultBackend() {
      return defaultBackend;
    }

    public void setDefaultBackend(String defaultBackend) {
      this.defaultBackend = defaultBackend;
    }

    public Map<String, Map<String, Object>> getBackends() {
      return backends;
    }

    public void setBackends(Map<String, Map<String, Object>> backends) {
      this.backends = backends;
    }

    public String getClpBinaryPath() {
      return clpBinaryPath;
    }

    public void setClpBinaryPath(String clpBinaryPath) {
      this.clpBinaryPath = clpBinaryPath;
    }

    public long getClpProcessTimeoutSeconds() {
      return clpProcessTimeoutSeconds;
    }

    public void setClpProcessTimeoutSeconds(long clpProcessTimeoutSeconds) {
      this.clpProcessTimeoutSeconds = clpProcessTimeoutSeconds;
    }
  }

  /**
   * Health check server configuration.
   *
   * <p>Configures the HTTP health check endpoints:
   *
   * <ul>
   *   <li>{@code /health/live} - Liveness probe
   *   <li>{@code /health/ready} - Readiness probe
   * </ul>
   */
  public static class HealthConfig {
    /** Enable health check HTTP server. */
    private boolean enabled = true;

    /** Port for health check endpoints. */
    private int port = 8081;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }
  }

  /**
   * Unified gRPC server configuration.
   *
   * <p>Controls the single gRPC server (started by {@code ServerMain} in the grpc-server module)
   * with per-service toggles. All services share one port.
   *
   * <p>Example:
   *
   * <pre>
   * node:
   *   grpc:
   *     enabled: true
   *     port: 9090
   *     services:
   *       ingestion: true
   *       query: true
   *       catalog: true
   *       admin: true
   * </pre>
   */
  public static class GrpcConfig {
    private boolean enabled = true;
    private int port = 9090;

    @JsonProperty("services")
    private GrpcServicesConfig services = new GrpcServicesConfig();

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public GrpcServicesConfig getServices() {
      return services;
    }

    public void setServices(GrpcServicesConfig services) {
      this.services = services;
    }

    public boolean isIngestionEnabled() {
      return services.isIngestion();
    }

    public boolean isQueryEnabled() {
      return services.isQuery();
    }

    public boolean isCatalogEnabled() {
      return services.isCatalog();
    }

    public boolean isAdminEnabled() {
      return services.isAdmin();
    }
  }

  /** Per-service toggles for the unified gRPC server. */
  public static class GrpcServicesConfig {
    private boolean ingestion = true;
    private boolean query = true;
    private boolean catalog = true;
    private boolean admin = true;

    public boolean isIngestion() {
      return ingestion;
    }

    public void setIngestion(boolean ingestion) {
      this.ingestion = ingestion;
    }

    public boolean isQuery() {
      return query;
    }

    public void setQuery(boolean query) {
      this.query = query;
    }

    public boolean isCatalog() {
      return catalog;
    }

    public void setCatalog(boolean catalog) {
      this.catalog = catalog;
    }

    public boolean isAdmin() {
      return admin;
    }

    public void setAdmin(boolean admin) {
      this.admin = admin;
    }
  }

  /**
   * Shared worker pool configuration.
   *
   * <p>Workers claim tasks from {@code _task_queue} across all tables. Set {@code numWorkers} to 0
   * for coordinator-only nodes.
   *
   * <p>The {@code NUM_WORKERS} environment variable overrides the YAML value.
   */
  public static class WorkerConfig {
    private int numWorkers = 0;

    @JsonProperty("database")
    private DatabaseConfig database;

    public int getNumWorkers() {
      String env = System.getenv("NUM_WORKERS");
      if (env != null) {
        try {
          return Integer.parseInt(env);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Invalid integer value for NUM_WORKERS env var: " + env, e);
        }
      }
      return numWorkers;
    }

    public void setNumWorkers(int numWorkers) {
      this.numWorkers = numWorkers;
    }

    /**
     * Optional separate database configuration for workers (e.g., read replica).
     *
     * @return database config, or null to use node.database
     */
    public DatabaseConfig getDatabase() {
      return database;
    }

    public void setDatabase(DatabaseConfig database) {
      this.database = database;
    }
  }

  /**
   * Declarative table registration from YAML config.
   *
   * <p>Tables defined here are auto-UPSERTed into the {@code _table*} sub-tables on startup. Only
   * explicitly specified fields are updated; omitted (null) fields keep their DB defaults or
   * existing values.
   *
   * <p>All optional fields use boxed types so Jackson leaves omitted fields as {@code null},
   * distinguishing "user set false" from "not specified".
   */
  public static class TableDefinition {
    private String name;
    private String displayName;
    private Boolean active;

    @JsonProperty("kafka")
    private TableKafkaDefinition kafka;

    // Feature config overrides (all nullable = don't touch DB value)
    private Boolean kafkaPollerEnabled;
    private Boolean deletionEnabled;
    private Boolean consolidationEnabled;
    private Boolean retentionCleanupEnabled;
    private Integer retentionCleanupIntervalMs;
    private Boolean partitionManagerEnabled;
    private Integer partitionMaintenanceIntervalMs;
    private Boolean policyHotReloadEnabled;
    private Boolean indexHotReloadEnabled;
    private Boolean schemaEvolutionEnabled;
    private Integer schemaEvolutionMaxDimColumns;
    private Integer schemaEvolutionMaxCountColumns;
    private Integer loopIntervalMs;
    private Integer storageDeletionDelayMs;
    private String policyConfigPath;
    private String indexConfigPath;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }

    public Boolean getActive() {
      return active;
    }

    public void setActive(Boolean active) {
      this.active = active;
    }

    public TableKafkaDefinition getKafka() {
      return kafka;
    }

    public void setKafka(TableKafkaDefinition kafka) {
      this.kafka = kafka;
    }

    public Boolean getKafkaPollerEnabled() {
      return kafkaPollerEnabled;
    }

    public void setKafkaPollerEnabled(Boolean kafkaPollerEnabled) {
      this.kafkaPollerEnabled = kafkaPollerEnabled;
    }

    public Boolean getDeletionEnabled() {
      return deletionEnabled;
    }

    public void setDeletionEnabled(Boolean deletionEnabled) {
      this.deletionEnabled = deletionEnabled;
    }

    public Boolean getConsolidationEnabled() {
      return consolidationEnabled;
    }

    public void setConsolidationEnabled(Boolean consolidationEnabled) {
      this.consolidationEnabled = consolidationEnabled;
    }

    public Boolean getRetentionCleanupEnabled() {
      return retentionCleanupEnabled;
    }

    public void setRetentionCleanupEnabled(Boolean retentionCleanupEnabled) {
      this.retentionCleanupEnabled = retentionCleanupEnabled;
    }

    public Integer getRetentionCleanupIntervalMs() {
      return retentionCleanupIntervalMs;
    }

    public void setRetentionCleanupIntervalMs(Integer retentionCleanupIntervalMs) {
      this.retentionCleanupIntervalMs = retentionCleanupIntervalMs;
    }

    public Boolean getPartitionManagerEnabled() {
      return partitionManagerEnabled;
    }

    public void setPartitionManagerEnabled(Boolean partitionManagerEnabled) {
      this.partitionManagerEnabled = partitionManagerEnabled;
    }

    public Integer getPartitionMaintenanceIntervalMs() {
      return partitionMaintenanceIntervalMs;
    }

    public void setPartitionMaintenanceIntervalMs(Integer partitionMaintenanceIntervalMs) {
      this.partitionMaintenanceIntervalMs = partitionMaintenanceIntervalMs;
    }

    public Boolean getPolicyHotReloadEnabled() {
      return policyHotReloadEnabled;
    }

    public void setPolicyHotReloadEnabled(Boolean policyHotReloadEnabled) {
      this.policyHotReloadEnabled = policyHotReloadEnabled;
    }

    public Boolean getIndexHotReloadEnabled() {
      return indexHotReloadEnabled;
    }

    public void setIndexHotReloadEnabled(Boolean indexHotReloadEnabled) {
      this.indexHotReloadEnabled = indexHotReloadEnabled;
    }

    public Boolean getSchemaEvolutionEnabled() {
      return schemaEvolutionEnabled;
    }

    public void setSchemaEvolutionEnabled(Boolean schemaEvolutionEnabled) {
      this.schemaEvolutionEnabled = schemaEvolutionEnabled;
    }

    public Integer getSchemaEvolutionMaxDimColumns() {
      return schemaEvolutionMaxDimColumns;
    }

    public void setSchemaEvolutionMaxDimColumns(Integer schemaEvolutionMaxDimColumns) {
      this.schemaEvolutionMaxDimColumns = schemaEvolutionMaxDimColumns;
    }

    public Integer getSchemaEvolutionMaxCountColumns() {
      return schemaEvolutionMaxCountColumns;
    }

    public void setSchemaEvolutionMaxCountColumns(Integer schemaEvolutionMaxCountColumns) {
      this.schemaEvolutionMaxCountColumns = schemaEvolutionMaxCountColumns;
    }

    public Integer getLoopIntervalMs() {
      return loopIntervalMs;
    }

    public void setLoopIntervalMs(Integer loopIntervalMs) {
      this.loopIntervalMs = loopIntervalMs;
    }

    public Integer getStorageDeletionDelayMs() {
      return storageDeletionDelayMs;
    }

    public void setStorageDeletionDelayMs(Integer storageDeletionDelayMs) {
      this.storageDeletionDelayMs = storageDeletionDelayMs;
    }

    public String getPolicyConfigPath() {
      return policyConfigPath;
    }

    public void setPolicyConfigPath(String policyConfigPath) {
      this.policyConfigPath = policyConfigPath;
    }

    public String getIndexConfigPath() {
      return indexConfigPath;
    }

    public void setIndexConfigPath(String indexConfigPath) {
      this.indexConfigPath = indexConfigPath;
    }
  }

  /** Kafka routing configuration for a table definition. */
  public static class TableKafkaDefinition {
    private String topic;
    private String bootstrapServers;
    private String recordTransformer;

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public String getRecordTransformer() {
      return recordTransformer;
    }

    public void setRecordTransformer(String recordTransformer) {
      this.recordTransformer = recordTransformer;
    }
  }

  /**
   * Ingestion batching configuration for Kafka and gRPC paths.
   *
   * <p>Each path has its own {@code BatchingWriter} instance with independent settings. Example:
   *
   * <pre>
   * node:
   *   ingestion:
   *     kafka:
   *       recordsPerBatch: 500
   *       maxQueuedRecords: 5000
   *       flushTimeoutMs: 1000
   *     grpc:
   *       recordsPerBatch: 1000
   *       maxQueuedRecords: 10000
   *       flushTimeoutMs: 1000
   * </pre>
   */
  public static class IngestionConfig {
    @JsonProperty("kafka")
    private BatchingConfig kafka = new BatchingConfig(500, 5000, 1000);

    @JsonProperty("grpc")
    private BatchingConfig grpc = new BatchingConfig(1000, 10000, 1000);

    public BatchingConfig getKafka() {
      return kafka;
    }

    public void setKafka(BatchingConfig kafka) {
      this.kafka = kafka;
    }

    public BatchingConfig getGrpc() {
      return grpc;
    }

    public void setGrpc(BatchingConfig grpc) {
      this.grpc = grpc;
    }
  }

  /**
   * Per-path batching settings for a {@code BatchingWriter} instance.
   *
   * <ul>
   *   <li>{@code recordsPerBatch}: target DB batch size — consumer accumulates up to this many
   *       records from the queue before issuing one multi-row INSERT. Defaults to 500 for Kafka
   *       (one poll), 1000 for gRPC (matches {@code MAX_MULTI_ROW_INSERT}).
   *   <li>{@code maxQueuedRecords}: max pending submissions the queue can hold per table before
   *       backpressure kicks in. For gRPC this is the number of concurrent in-flight requests that
   *       can be buffered; for Kafka it is rarely reached since there is one consumer thread.
   *   <li>{@code flushTimeoutMs}: max time the consumer waits for the first record before looping.
   *       Ensures partial batches are flushed promptly even at low ingestion rates.
   * </ul>
   */
  public static class BatchingConfig {
    private int recordsPerBatch;
    private int maxQueuedRecords;
    private long flushTimeoutMs;

    public BatchingConfig() {
      this(1, 5000, 1000);
    }

    public BatchingConfig(int recordsPerBatch, int maxQueuedRecords, long flushTimeoutMs) {
      this.recordsPerBatch = recordsPerBatch;
      this.maxQueuedRecords = maxQueuedRecords;
      this.flushTimeoutMs = flushTimeoutMs;
    }

    public int getRecordsPerBatch() {
      return recordsPerBatch;
    }

    public void setRecordsPerBatch(int recordsPerBatch) {
      this.recordsPerBatch = recordsPerBatch;
    }

    public int getMaxQueuedRecords() {
      return maxQueuedRecords;
    }

    public void setMaxQueuedRecords(int maxQueuedRecords) {
      this.maxQueuedRecords = maxQueuedRecords;
    }

    public long getFlushTimeoutMs() {
      return flushTimeoutMs;
    }

    public void setFlushTimeoutMs(long flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
    }
  }

  /** Unit definition within the node configuration. */
  public static class UnitDefinition {
    private String name;
    private String type; // "coordinator" or "worker"
    private boolean enabled = true;

    /** Unit-specific configuration as a map (parsed by unit implementation). */
    @JsonProperty("config")
    private Map<String, Object> config;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public Map<String, Object> getConfig() {
      return config;
    }

    public void setConfig(Map<String, Object> config) {
      this.config = config;
    }
  }
}
