package com.yscope.clp.service.node;

import com.yscope.clp.service.common.config.YamlConfigLoader;
import com.yscope.clp.service.common.config.db.DataSourceFactory;
import com.yscope.clp.service.common.config.db.DatabaseConfig;
import com.yscope.clp.service.common.health.HealthCheckServer;
import com.yscope.clp.service.common.storage.ArchiveCreator;
import com.yscope.clp.service.common.storage.ClpCompressor;
import com.yscope.clp.service.common.storage.StorageBackendFactory;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.yscope.clp.service.coordinator.ProgressTracker;
import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.yscope.clp.service.coordinator.ingestion.IngestionService;
import com.yscope.clp.service.coordinator.ingestion.MetadataPollerFactory;
import com.yscope.clp.service.coordinator.ingestion.server.IngestionServer;
import com.yscope.clp.service.coordinator.ingestion.server.IngestionServerFactory;
import com.yscope.clp.service.metastore.schema.PartitionManager;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node that hosts multiple independent units (Coordinators and Workers) in the same JVM.
 *
 * <p>The Node manages:
 *
 * <ul>
 *   <li>Shared resources (HikariDataSource, StorageRegistry) used by all units
 *   <li>Unit lifecycle (creation, start, stop, close)
 *   <li>DB-driven unit discovery via {@link CoordinatorRegistry}
 * </ul>
 *
 * <p>Architecture:
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────┐
 * │                     Node JVM                        │
 * ├─────────────────────────────────────────────────────┤
 * │  SharedResources (owned by Node):                   │
 * │  - HikariDataSource (poolSize=20, shared)           │
 * │  - StorageRegistry (thread-safe, shared)             │
 * ├─────────────────────────────────────────────────────┤
 * │  Coordinators (discovered from DB):                 │
 * │  ┌─────────────────┐  ┌─────────────────┐          │
 * │  │ CoordinatorUnit │  │ CoordinatorUnit │          │
 * │  │ - KafkaConsumer │  │ - KafkaConsumer │          │
 * │  │ - table: spark  │  │ - table: flink  │          │
 * │  └─────────────────┘  └─────────────────┘          │
 * │  Shared worker pool (from node.yaml):               │
 * │  ┌─────────────────────────────────────┐            │
 * │  │ WorkerUnit (shared, all tables)     │            │
 * │  │ - 4 workers, polls _task_queue      │            │
 * │  └─────────────────────────────────────┘            │
 * └─────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>Startup:
 *
 * <ol>
 *   <li>Create shared resources (DataSource, StorageRegistry)
 *   <li>Initialize coordination schema (_table registry)
 *   <li>Query DB for coordinators assigned to this node
 *   <li>If worker.numWorkers &gt; 0, add a shared WorkerUnit
 *   <li>Create units from definitions
 * </ol>
 */
public class Node implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Node.class);

  /** How often the node-level partition maintenance runs (1 hour). */
  private static final long PARTITION_MAINTENANCE_INTERVAL_HOURS = 1;

  /** Default partition lookahead days for node-level maintenance. */
  private static final int DEFAULT_LOOKAHEAD_DAYS = 7;

  /** Default partition cleanup age days for node-level maintenance. */
  private static final int DEFAULT_CLEANUP_AGE_DAYS = 90;

  /** Default sparse row threshold for node-level maintenance. */
  private static final long DEFAULT_SPARSE_ROW_THRESHOLD = 1000;

  /** How often the watchdog checks coordinator health (seconds). */
  private static final long WATCHDOG_INTERVAL_SECONDS = 60;

  /** Age threshold (ms) before logging a stall warning. */
  private static final long STALL_WARN_THRESHOLD_MS = 50_000;

  /** Age threshold (ms) before restarting a stalled coordinator. */
  private static final long STALL_RESTART_THRESHOLD_MS = 100_000;

  /** Window (ms) within which a second stall triggers release instead of restart. */
  private static final long STALL_RECURRENCE_WINDOW_MS = 300_000;

  private final String name;
  private final String nodeId;
  private final YamlConfigLoader<NodeConfig> yamlConfig;
  private volatile NodeConfig config;
  private final SharedResources sharedResources;
  private final Map<String, Unit> unitsByName = new ConcurrentHashMap<>();
  private volatile boolean nodeStarted = false;
  private BatchingWriter kafkaBatchingWriter;
  private BatchingWriter grpcBatchingWriter;
  private final IngestionServerFactory ingestionServerFactory;
  private final MetadataPollerFactory metadataPollerFactory;
  private IngestionServer ingestionServer;
  private IngestionService ingestionService;
  private HealthCheckServer healthCheckServer;
  private ScheduledExecutorService coordinatorHaExecutor;
  private ScheduledExecutorService partitionMaintenanceExecutor;
  private ScheduledExecutorService reconciliationExecutor;
  private ScheduledExecutorService watchdogExecutor;
  private final Object unitLock = new Object();
  private final Map<String, Long> lastRestartTime = new ConcurrentHashMap<>();

  /**
   * Create a Node with the given configuration.
   *
   * <p>Initializes shared resources, creates coordination tables, and discovers units from the
   * database. The YAML config provides node-level settings (database, storage, health) but units
   * come from the DB.
   *
   * @param yamlConfig YAML configuration with hot-reload support
   * @param ingestionServerFactory factory called during {@link #start()} when gRPC ingestion is
   *     enabled; use {@link IngestionServerFactory#disabled()} to reject built-in server creation
   * @param metadataPollerFactory factory for creating metadata pollers; use {@link
   *     MetadataPollerFactory#disabled()} for nodes without a messaging backend
   * @throws SQLException if coordination schema initialization or unit discovery fails
   */
  public Node(
      YamlConfigLoader<NodeConfig> yamlConfig,
      IngestionServerFactory ingestionServerFactory,
      MetadataPollerFactory metadataPollerFactory)
      throws SQLException {
    this.yamlConfig = yamlConfig;
    this.config = yamlConfig.get();
    this.name = config.getNode().getName();
    this.nodeId = resolveNodeId(config.getNode());
    this.ingestionServerFactory = ingestionServerFactory;
    this.metadataPollerFactory = metadataPollerFactory;
    this.sharedResources = createSharedResources(config);
    initNode();
  }

  /**
   * Create a Node directly from configuration.
   *
   * @param config Node configuration
   * @param ingestionServerFactory factory called during {@link #start()} when gRPC ingestion is
   *     enabled; use {@link IngestionServerFactory#disabled()} to reject built-in server creation
   * @param metadataPollerFactory factory for creating metadata pollers; use {@link
   *     MetadataPollerFactory#disabled()} for nodes without a messaging backend
   * @throws SQLException if coordination schema initialization or unit discovery fails
   */
  public Node(
      NodeConfig config,
      IngestionServerFactory ingestionServerFactory,
      MetadataPollerFactory metadataPollerFactory)
      throws SQLException {
    this.yamlConfig = null;
    this.config = config;
    this.name = config.getNode().getName();
    this.nodeId = resolveNodeId(config.getNode());
    this.ingestionServerFactory = ingestionServerFactory;
    this.metadataPollerFactory = metadataPollerFactory;
    this.sharedResources = createSharedResources(config);
    initNode();
  }

  /**
   * Shared initialization logic for both constructors.
   *
   * <p>Initializes coordination schema, registers node heartbeat, upserts YAML-declared tables,
   * claims unassigned tables, discovers assigned coordinators, and reconciles units.
   */
  private void initNode() throws SQLException {
    NodeConfig.NodeSettings settings = config.getNode();
    String haStrategy = settings.getCoordinatorHaStrategy();
    boolean leaseMode = "lease".equals(haStrategy);

    // Validate HA strategy config
    if (!"heartbeat".equals(haStrategy) && !"lease".equals(haStrategy)) {
      throw new IllegalArgumentException(
          "Invalid coordinatorHaStrategy: '" + haStrategy + "' (must be 'heartbeat' or 'lease')");
    }
    if (leaseMode && settings.getLeaseRenewalIntervalSeconds() >= settings.getLeaseTtlSeconds()) {
      throw new IllegalArgumentException(
          "leaseRenewalIntervalSeconds ("
              + settings.getLeaseRenewalIntervalSeconds()
              + ") must be less than leaseTtlSeconds ("
              + settings.getLeaseTtlSeconds()
              + ")");
    }

    logger.info("Creating Node: {} (nodeId={}, haStrategy={})", name, nodeId, haStrategy);

    // Initialize coordination tables (idempotent)
    CoordinatorRegistry.initializeSchema(sharedResources.getDataSource());

    // Auto-upsert tables from YAML config (before claiming)
    CoordinatorRegistry.upsertTables(sharedResources.getDataSource(), config.getTables());

    if (leaseMode) {
      // Lease mode: renew leases for already-owned tables first (critical for heartbeat→lease
      // transition — prevents other nodes from treating our tables as expired during startup).
      CoordinatorRegistry.renewLeases(
          sharedResources.getDataSource(), nodeId, settings.getLeaseTtlSeconds());
    } else {
      // Heartbeat mode: register in _node_registry immediately
      CoordinatorRegistry.heartbeat(sharedResources.getDataSource(), nodeId);
      logger.info("Registered node '{}' in _node_registry", nodeId);
    }

    // Claim unassigned tables (staggered fight-for-master) — recompute fair share between each
    // claim so concurrent nodes can become visible, especially in lease mode where a node's first
    // claim creates its first visible lease.
    Integer leaseTtl = leaseMode ? settings.getLeaseTtlSeconds() : null;
    Integer deadThreshold = leaseMode ? null : settings.getDeadNodeThresholdSeconds();
    claimUnassignedWithFairShare(leaseTtl, deadThreshold);

    // Discover coordinator units from database (now includes newly claimed)
    List<NodeConfig.UnitDefinition> dbUnits =
        CoordinatorRegistry.getAssignedCoordinators(sharedResources.getDataSource(), nodeId);

    // Add shared worker unit if configured
    addWorkerUnitDefinition(config, dbUnits);

    // Create BatchingWriters before reconcileUnits so CoordinatorUnits receive non-null writers.
    // Writer threads are started lazily on first submit, so early creation is safe.
    NodeConfig.IngestionConfig ingestionConfig = config.getNode().getIngestion();
    NodeConfig.BatchingConfig kafkaBatching = ingestionConfig.getKafka();
    NodeConfig.BatchingConfig grpcBatching = ingestionConfig.getGrpc();
    kafkaBatchingWriter =
        new BatchingWriter(
            sharedResources.getDataSource(),
            kafkaBatching.getMaxQueuedRecords(),
            kafkaBatching.getRecordsPerBatch(),
            kafkaBatching.getFlushTimeoutMs());
    grpcBatchingWriter =
        new BatchingWriter(
            sharedResources.getDataSource(),
            grpcBatching.getMaxQueuedRecords(),
            grpcBatching.getRecordsPerBatch(),
            grpcBatching.getFlushTimeoutMs());

    reconcileUnits(dbUnits);

    logger.info("Node {} created with {} units", name, unitsByName.size());
  }

  /**
   * Resolve the node ID used for table assignment in {@code _table_assignment.node_id}.
   *
   * <p>Resolution order:
   *
   * <ol>
   *   <li>If {@code node.nodeIdEnvVar} is configured, read that environment variable
   *   <li>Otherwise, fall back to the local hostname
   * </ol>
   *
   * @param nodeSettings node configuration containing the optional env var name
   * @return the resolved node ID
   */
  private static String resolveNodeId(NodeConfig.NodeSettings nodeSettings) {
    String envVarName = nodeSettings.getNodeIdEnvVar();
    if (envVarName != null && !envVarName.isBlank()) {
      String value = System.getenv(envVarName);
      if (value != null && !value.isBlank()) {
        logger.info("Resolved nodeId from env var {}={}", envVarName, value);
        return value;
      }
      logger.warn(
          "nodeIdEnvVar '{}' is configured but not set in environment, falling back to hostname",
          envVarName);
    }

    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      logger.info("Resolved nodeId from hostname: {}", hostname);
      return hostname;
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
          "Cannot resolve nodeId: environment variable not set and hostname lookup failed. "
              + "Configure node.nodeIdEnvVar or ensure hostname is resolvable.",
          e);
    }
  }

  /** Create shared resources from configuration. */
  private SharedResources createSharedResources(NodeConfig config) {
    DatabaseConfig dbConfig = config.getNode().getDatabase();
    NodeConfig.StorageConfig storageConfig = config.getNode().getStorage();

    // Create shared HikariDataSource (coordinator / master)
    logger.info(
        "Connecting to database: url={}, user={}, password={}",
        dbConfig.getJdbcUrl(),
        dbConfig.getUser(),
        obfuscatePassword(dbConfig.getPassword()));
    HikariDataSource dataSource = DataSourceFactory.create(dbConfig, "Node-" + name);
    // Verify connection works
    try (var conn = dataSource.getConnection()) {
      logger.info(
          "Database connection verified: poolSize={}, poolMinIdle={}",
          dbConfig.getPoolSize(),
          dbConfig.getPoolMinIdle());
    } catch (SQLException e) {
      dataSource.close();
      logger.error("Failed to verify database connection", e);
      throw new RuntimeException("Database connection failed", e);
    }

    // Optional separate worker DataSource for replica routing
    HikariDataSource workerDataSource = null;
    DatabaseConfig workerDbConfig = config.getWorker().getDatabase();
    if (workerDbConfig != null) {
      try {
        workerDataSource = DataSourceFactory.create(workerDbConfig, "Worker-" + name);
        logger.info(
            "Created worker HikariDataSource: host={}, poolSize={}, poolMinIdle={}",
            workerDbConfig.getHost(),
            workerDbConfig.getPoolSize(),
            workerDbConfig.getPoolMinIdle());
      } catch (RuntimeException e) {
        dataSource.close();
        throw e;
      }
    }

    // Create shared StorageRegistry only if backends are configured
    // Coordinator-only nodes (numWorkers=0) don't need storage
    StorageRegistry storageRegistry = null;
    ArchiveCreator archiveCreator = null;
    if (!storageConfig.getBackends().isEmpty()) {
      try {
        storageRegistry =
            StorageBackendFactory.createRegistry(
                storageConfig.getDefaultBackend(), storageConfig.getBackends());

        // Create archive creator and wire CLP compressor
        archiveCreator =
            new ArchiveCreator(
                storageRegistry.getIrBackend(), storageRegistry.getArchiveBackend());
        archiveCreator.setClpCompressor(
            new ClpCompressor(
                storageConfig.getClpBinaryPath(), storageConfig.getClpProcessTimeoutSeconds()));

        logger.info(
            "Created shared StorageRegistry: defaultBackend={}, backends={}, clpBinaryPath={}",
            storageConfig.getDefaultBackend(),
            storageConfig.getBackends().keySet(),
            storageConfig.getClpBinaryPath());
      } catch (RuntimeException e) {
        if (workerDataSource != null) {
          workerDataSource.close();
        }
        dataSource.close();
        throw e;
      }
    } else {
      logger.info("StorageRegistry skipped (no backends configured, coordinator-only mode)");
    }

    return new SharedResources(dataSource, workerDataSource, storageRegistry, archiveCreator);
  }

  /**
   * Add a shared WorkerUnit definition if {@code worker.numWorkers > 0}.
   *
   * <p>The shared worker pool claims tasks from all tables in {@code _task_queue}, unlike the old
   * per-table workers. Worker count comes from node.yaml (overridable via {@code NUM_WORKERS} env
   * var).
   */
  private static void addWorkerUnitDefinition(
      NodeConfig config, List<NodeConfig.UnitDefinition> units) {
    int numWorkers = config.getWorker().getNumWorkers();
    if (numWorkers > 0) {
      Map<String, Object> workerConfig = new HashMap<>();
      workerConfig.put("numWorkers", numWorkers);

      NodeConfig.UnitDefinition workerDef = new NodeConfig.UnitDefinition();
      workerDef.setName("worker");
      workerDef.setType("worker");
      workerDef.setEnabled(true);
      workerDef.setConfig(workerConfig);
      units.add(workerDef);

      logger.info("Adding shared worker unit: numWorkers={}", numWorkers);
    }
  }

  /**
   * Start the node and all its units.
   *
   * @throws Exception if startup fails
   */
  public void start() throws Exception {
    logger.info("Starting Node: {}", name);

    // Note: storage connectivity is validated lazily on first I/O operation

    // Start all units
    for (Unit unit : unitsByName.values()) {
      logger.info("Starting unit: {} (type={})", unit.getName(), unit.getType());
      unit.start();
    }

    // Mark node as started so reconciliation will start new units
    nodeStarted = true;

    // Create the framework-agnostic ingestion service (always available via getIngestionService()
    // so integrators can wire their own transport layer).
    ingestionService = new IngestionService(grpcBatchingWriter);

    // Start ingestion server (if enabled and factory supports it)
    NodeConfig.GrpcConfig grpcConfig = config.getNode().getGrpc();
    if (grpcConfig.isEnabled() && grpcConfig.isIngestionEnabled()) {
      try {
        ingestionServer = ingestionServerFactory.create(grpcConfig.getPort(), ingestionService);
        ingestionServer.start();
      } catch (UnsupportedOperationException e) {
        // Factory is disabled (headless mode) — ingestion handled elsewhere
        logger.debug("Built-in ingestion server disabled: {}", e.getMessage());
      }
    }

    // Start background threads (HA strategy determines heartbeat vs lease renewal)
    if ("lease".equals(config.getNode().getCoordinatorHaStrategy())) {
      startCoordinatorHaThread(
          "lease-renewal",
          config.getNode().getLeaseRenewalIntervalSeconds(),
          this::runLeaseRenewal);
    } else {
      startCoordinatorHaThread(
          "node-heartbeat", config.getNode().getHeartbeatIntervalSeconds(), this::runHeartbeat);
    }
    startReconciliationThread();
    startPartitionMaintenanceThread();
    startWatchdogThread();

    // Start health check server (if enabled)
    NodeConfig.HealthConfig healthConfig = config.getNode().getHealth();
    if (healthConfig.isEnabled()) {
      startHealthCheckServer(healthConfig);
    } else {
      logger.info("Health check server disabled");
    }

    logger.info("Node {} started with {} units", name, unitsByName.size());
  }

  /** Start the health check HTTP server. */
  private void startHealthCheckServer(NodeConfig.HealthConfig healthConfig) {
    try {
      healthCheckServer = new HealthCheckServer(healthConfig.getPort());

      // Liveness check: are all units running?
      healthCheckServer.addLivenessCheck(
          "units-running",
          () -> {
            for (Unit unit : unitsByName.values()) {
              if (!unit.isRunning()) {
                return false;
              }
            }
            return true;
          });

      // Readiness check: can we connect to the database?
      healthCheckServer.addReadinessCheck(
          "database",
          () -> {
            try (var conn = sharedResources.getDataSource().getConnection()) {
              return conn.isValid(1);
            } catch (Exception e) {
              return false;
            }
          });

      healthCheckServer.start();
      logger.info("Health check server started on port {}", healthConfig.getPort());
    } catch (IOException e) {
      logger.error("Failed to start health check server on port {}", healthConfig.getPort(), e);
    }
  }

  /** Stop the node and all its units. */
  public void stop() {
    logger.info("Stopping Node: {}", name);

    // Stop background threads
    if (coordinatorHaExecutor != null) {
      coordinatorHaExecutor.shutdownNow();
      logger.info("Coordinator HA thread stopped");
    }
    if (reconciliationExecutor != null) {
      reconciliationExecutor.shutdownNow();
      try {
        reconciliationExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      logger.info("Reconciliation thread stopped");
    }
    if (partitionMaintenanceExecutor != null) {
      partitionMaintenanceExecutor.shutdownNow();
      try {
        partitionMaintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      logger.info("Partition maintenance thread stopped");
    }
    if (watchdogExecutor != null) {
      watchdogExecutor.shutdownNow();
      try {
        watchdogExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      logger.info("Watchdog thread stopped");
    }

    // Stop health check server (signal that node is no longer ready)
    if (healthCheckServer != null) {
      healthCheckServer.stop();
      healthCheckServer = null;
    }

    // 1. Stop accepting new work from both sources.
    if (ingestionServer != null) {
      ingestionServer.stop();
      ingestionServer = null;
    }
    for (Unit unit : unitsByName.values()) {
      if ("coordinator".equals(unit.getType())) {
        logger.info("Stopping coordinator unit: {}", unit.getName());
        unit.stop();
      }
    }

    // 2. Signal both writers to stop, then drain in parallel.
    stopBatchingWriter(grpcBatchingWriter, "grpc");
    stopBatchingWriter(kafkaBatchingWriter, "kafka");
    grpcBatchingWriter = null;
    kafkaBatchingWriter = null;

    // 3. Workers last — let coordinators finish creating tasks first.
    for (Unit unit : unitsByName.values()) {
      if ("worker".equals(unit.getType())) {
        logger.info("Stopping worker unit: {}", unit.getName());
        unit.stop();
      }
    }

    logger.info("Node {} stopped", name);
  }

  /** Close the node and all resources. */
  @Override
  public void close() {
    logger.info("Closing Node: {}", name);

    // Stop if not already stopped
    stop();

    // Close all units
    for (Unit unit : unitsByName.values()) {
      try {
        unit.close();
      } catch (Exception e) {
        logger.error("Error closing unit: {}", unit.getName(), e);
      }
    }
    unitsByName.clear();

    // Close shared resources last
    sharedResources.close();

    // Close config if we own it
    if (yamlConfig != null) {
      yamlConfig.close();
    }

    logger.info("Node {} closed", name);
  }

  /**
   * Start a background thread that periodically claims unassigned tables and reconciles running
   * units with the database.
   *
   * <p>This ensures tables added after startup are picked up without requiring a node restart. The
   * fight-for-master semantics in {@link CoordinatorRegistry#claimUnassignedTables} are race-safe.
   */
  private void startReconciliationThread() {
    long intervalSeconds = config.getNode().getReconciliationIntervalSeconds();
    reconciliationExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "unit-reconciliation");
              t.setDaemon(true);
              return t;
            });
    reconciliationExecutor.scheduleWithFixedDelay(
        this::runPeriodicReconciliation, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    logger.info("Reconciliation thread started (interval={}s)", intervalSeconds);
  }

  /**
   * Start the HA background thread (heartbeat or lease renewal, depending on mode).
   *
   * @param threadName Name for the daemon thread
   * @param intervalSeconds Scheduling interval
   * @param task The runnable to execute periodically
   */
  private void startCoordinatorHaThread(String threadName, int intervalSeconds, Runnable task) {
    coordinatorHaExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, threadName);
              t.setDaemon(true);
              return t;
            });
    coordinatorHaExecutor.scheduleWithFixedDelay(task, 0, intervalSeconds, TimeUnit.SECONDS);
    logger.info("{} thread started (interval={}s)", threadName, intervalSeconds);
  }

  /** Send a heartbeat to the _node_registry table. */
  private void runHeartbeat() {
    try {
      CoordinatorRegistry.heartbeat(sharedResources.getDataSource(), nodeId);
    } catch (SQLException e) {
      logger.warn("Heartbeat failed, will retry next cycle", e);
    }
  }

  /** Renew leases for all tables owned by this node. */
  private void runLeaseRenewal() {
    try {
      CoordinatorRegistry.renewLeases(
          sharedResources.getDataSource(), nodeId, config.getNode().getLeaseTtlSeconds());
    } catch (SQLException e) {
      logger.warn("Lease renewal failed, will retry next cycle", e);
    }
  }

  /**
   * Start a background thread that periodically checks coordinator health and reports progress.
   *
   * <p>For each running CoordinatorUnit, the watchdog:
   *
   * <ol>
   *   <li>If healthy: updates {@code last_progress_at} in the database
   *   <li>If stalled &lt; restart threshold: logs a warning
   *   <li>If stalled &gt;= restart threshold: restarts the coordinator
   *   <li>If stalled again within recurrence window: releases the assignment
   * </ol>
   */
  private void startWatchdogThread() {
    watchdogExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "coordinator-watchdog");
              t.setDaemon(true);
              return t;
            });
    watchdogExecutor.scheduleAtFixedRate(
        this::runWatchdog, WATCHDOG_INTERVAL_SECONDS, WATCHDOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    logger.info("Watchdog thread started (interval={}s)", WATCHDOG_INTERVAL_SECONDS);
  }

  /**
   * Check coordinator health and escalate stalls (warn → restart → release).
   *
   * <p>Uses {@link #unitLock} to snapshot units, then executes I/O-bound actions (DB calls, unit
   * stop/close) outside the lock to avoid blocking reconciliation.
   */
  private void runWatchdog() {
    // Phase 1: Under lock, snapshot units and evaluate actions
    List<String> progressTables = new ArrayList<>();
    List<String> releaseUnits = new ArrayList<>();
    List<String> releaseTables = new ArrayList<>();
    List<String> restartUnits = new ArrayList<>();

    synchronized (unitLock) {
      for (Unit unit : new ArrayList<>(unitsByName.values())) {
        if (!(unit instanceof CoordinatorUnit cu) || !cu.isRunning()) {
          continue;
        }

        String tableName = cu.getTableName();
        ProgressTracker tracker = cu.getProgressTracker();

        if (tracker.isHealthy(STALL_WARN_THRESHOLD_MS)) {
          progressTables.add(tableName);
          continue;
        }

        var stalest = tracker.getStalestThread();
        long ageMs = stalest != null ? stalest.getValue() : 0;

        if (ageMs < STALL_RESTART_THRESHOLD_MS) {
          String threadName = stalest != null ? stalest.getKey() : "unknown";
          logger.warn("[{}] Thread '{}' stalled for {}ms", tableName, threadName, ageMs);
          continue;
        }

        Long lastRestart = lastRestartTime.get(tableName);
        if (lastRestart != null
            && System.currentTimeMillis() - lastRestart < STALL_RECURRENCE_WINDOW_MS) {
          logger.error("[{}] Recurrent stall, releasing assignment", tableName);
          releaseUnits.add(cu.getName());
          releaseTables.add(tableName);
        } else {
          logger.warn(
              "[{}] Restarting stalled coordinator (thread '{}' stalled {}ms)",
              tableName,
              stalest.getKey(),
              ageMs);
          lastRestartTime.put(tableName, System.currentTimeMillis());
          restartUnits.add(cu.getName());
        }
      }
    }

    // Phase 2: Execute I/O-bound actions outside lock
    for (String tableName : progressTables) {
      try {
        CoordinatorRegistry.updateProgress(sharedResources.getDataSource(), tableName, nodeId);
      } catch (SQLException e) {
        logger.warn("Failed to update progress for {}", tableName, e);
      }
    }

    for (int i = 0; i < releaseUnits.size(); i++) {
      stopAndRemoveUnit(releaseUnits.get(i));
      try {
        CoordinatorRegistry.revertClaim(
            sharedResources.getDataSource(), releaseTables.get(i), nodeId);
      } catch (SQLException e) {
        logger.error("[{}] Failed to release assignment", releaseTables.get(i), e);
      }
    }

    // Restart by removing the unit; reconciliation will recreate it from DB.
    for (String unitName : restartUnits) {
      stopAndRemoveUnit(unitName);
    }
  }

  /** Claim unassigned tables and reconcile running units with DB assignments. */
  private void runPeriodicReconciliation() {
    try {
      NodeConfig.NodeSettings settings = config.getNode();
      boolean leaseMode = "lease".equals(settings.getCoordinatorHaStrategy());

      // 1. Find and claim tables from dead/expired owners
      Integer deadThreshold = leaseMode ? null : settings.getDeadNodeThresholdSeconds();
      if (leaseMode) {
        List<CoordinatorRegistry.OrphanedTable> expired =
            CoordinatorRegistry.findExpiredLeases(sharedResources.getDataSource());
        claimCandidateTables(
            expired,
            deadThreshold,
            c ->
                CoordinatorRegistry.claimExpiredLease(
                    sharedResources.getDataSource(),
                    c.tableName(),
                    nodeId,
                    settings.getLeaseTtlSeconds()),
            "expired lease");
      } else {
        List<CoordinatorRegistry.OrphanedTable> orphans =
            CoordinatorRegistry.findOrphanedTables(sharedResources.getDataSource(), deadThreshold);
        claimCandidateTables(
            orphans,
            deadThreshold,
            c ->
                CoordinatorRegistry.claimOrphan(
                    sharedResources.getDataSource(), c.tableName(), c.deadOwnerId(), nodeId),
            "orphan");
      }

      // 2. Claim any unassigned tables (staggered fight-for-master)
      Integer leaseTtl = leaseMode ? settings.getLeaseTtlSeconds() : null;
      claimUnassignedWithFairShare(leaseTtl, deadThreshold);

      // 3. Reconcile running units with DB assignments
      List<NodeConfig.UnitDefinition> dbUnits =
          CoordinatorRegistry.getAssignedCoordinators(sharedResources.getDataSource(), nodeId);
      addWorkerUnitDefinition(config, dbUnits);
      reconcileUnits(dbUnits);
    } catch (SQLException e) {
      logger.error("Error during periodic reconciliation", e);
    }
  }

  /**
   * Claim unassigned tables using staggered fair-share: claim one table at a time, recomputing fair
   * share between each claim. This gives concurrent nodes time to become visible — especially in
   * lease mode, where a node's first claim creates its first visible lease.
   *
   * @param leaseTtlSeconds If non-null, sets lease_expiry on claim (lease mode)
   * @param deadThresholdSeconds Heartbeat threshold for fair share, or null for lease mode
   */
  private void claimUnassignedWithFairShare(Integer leaseTtlSeconds, Integer deadThresholdSeconds)
      throws SQLException {
    int claimed = 0;
    while (true) {
      int fairShare =
          CoordinatorRegistry.calculateFairShare(
              sharedResources.getDataSource(), deadThresholdSeconds);
      int myLoad = CoordinatorRegistry.countOwnedTables(sharedResources.getDataSource(), nodeId);

      if (myLoad >= fairShare) {
        logger.debug(
            "At or above fair share (fair_share={}, my_load={}), stopping unassigned claiming",
            fairShare,
            myLoad);
        break;
      }

      String tableName =
          CoordinatorRegistry.claimOneUnassignedTable(
              sharedResources.getDataSource(), nodeId, leaseTtlSeconds);
      if (tableName == null) {
        break; // No more unassigned tables, or lost race
      }

      claimed++;

      // Signal liveness after first claim so other nodes can see us in lease mode
      if (leaseTtlSeconds != null) {
        CoordinatorRegistry.renewLeases(sharedResources.getDataSource(), nodeId, leaseTtlSeconds);
      }

      // Short random delay to reduce collisions with concurrent nodes
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(1000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    if (claimed > 0) {
      logger.info("Claimed {} unassigned table(s) for node '{}'", claimed, nodeId);
    }
  }

  /** Functional interface for claiming a single candidate table (may throw SQLException). */
  @FunctionalInterface
  private interface TableClaimer {
    boolean claim(CoordinatorRegistry.OrphanedTable candidate) throws SQLException;
  }

  /**
   * Common claiming loop for both orphaned tables (heartbeat mode) and expired leases (lease mode).
   *
   * <p>Applies staggered fair-share load balancing: recomputes fair share between each claim so
   * concurrent nodes can become visible. A random delay (0-5s) before each claim reduces collision
   * probability when multiple nodes race.
   *
   * @param candidates The orphaned/expired tables to attempt claiming
   * @param deadThresholdSeconds Heartbeat threshold for fair share calculation, or null for lease
   *     mode
   * @param claimer Function that attempts to claim a single candidate
   * @param label Description for logging (e.g., "orphan", "expired lease")
   */
  private void claimCandidateTables(
      List<CoordinatorRegistry.OrphanedTable> candidates,
      Integer deadThresholdSeconds,
      TableClaimer claimer,
      String label)
      throws SQLException {
    if (candidates.isEmpty()) {
      return;
    }

    int claimed = 0;
    for (CoordinatorRegistry.OrphanedTable candidate : candidates) {
      // Recompute fair share before each claim attempt
      int fairShare =
          CoordinatorRegistry.calculateFairShare(
              sharedResources.getDataSource(), deadThresholdSeconds);
      int myLoad = CoordinatorRegistry.countOwnedTables(sharedResources.getDataSource(), nodeId);

      if (myLoad >= fairShare) {
        logger.debug(
            "At or above fair share (fair_share={}, my_load={}), stopping {} claiming",
            fairShare,
            myLoad,
            label);
        break;
      }

      // Random delay (0-5s) to reduce collision probability
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(5000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      if (claimer.claim(candidate)) {
        logger.info(
            "Claimed {} '{}' from node '{}' (fair_share={}, my_load={})",
            label,
            candidate.tableName(),
            candidate.deadOwnerId(),
            fairShare,
            myLoad + 1);
        claimed++;
      } else {
        logger.debug("{} '{}' already claimed by another node", label, candidate.tableName());
      }
    }

    if (claimed > 0) {
      logger.info("Claimed {} {}(s)", claimed, label);
    }
  }

  /**
   * Start a background thread that periodically runs partition maintenance for ALL active tables
   * with partition management enabled.
   *
   * <p>This provides redundancy: if the owning coordinator's node dies, partition maintenance
   * continues from surviving nodes. Advisory locks inside {@link PartitionManager#runMaintenance()}
   * prevent concurrent DDL.
   */
  private void startPartitionMaintenanceThread() {
    partitionMaintenanceExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "partition-maintenance-global");
              t.setDaemon(true);
              return t;
            });
    partitionMaintenanceExecutor.scheduleWithFixedDelay(
        this::runGlobalPartitionMaintenance,
        PARTITION_MAINTENANCE_INTERVAL_HOURS,
        PARTITION_MAINTENANCE_INTERVAL_HOURS,
        TimeUnit.HOURS);
    logger.info(
        "Global partition maintenance thread started (interval={}h)",
        PARTITION_MAINTENANCE_INTERVAL_HOURS);
  }

  /**
   * Run partition maintenance for all active tables with partition management enabled.
   *
   * <p>Each table's maintenance is wrapped in an advisory lock, so concurrent calls from multiple
   * nodes are safe — only one wins, the rest skip.
   */
  private void runGlobalPartitionMaintenance() {
    try {
      List<String> tables =
          CoordinatorRegistry.getActivePartitionedTables(sharedResources.getDataSource());
      logger.debug("Running global partition maintenance for {} tables", tables.size());
      for (String tableName : tables) {
        try {
          PartitionManager pm =
              new PartitionManager(
                  sharedResources.getDslContext(),
                  tableName,
                  DEFAULT_LOOKAHEAD_DAYS,
                  DEFAULT_CLEANUP_AGE_DAYS,
                  DEFAULT_SPARSE_ROW_THRESHOLD);
          pm.runMaintenance();
        } catch (Exception e) {
          logger.error("Partition maintenance failed for table '{}'", tableName, e);
        }
      }
    } catch (SQLException e) {
      logger.error("Error in global partition maintenance", e);
    }
  }

  /**
   * Reconcile running units with a list of desired unit definitions.
   *
   * <p>Actions:
   *
   * <ul>
   *   <li>Start new units (in list but not running)
   *   <li>Stop removed units (running but not in list)
   *   <li>Stop disabled units (enabled: false)
   * </ul>
   *
   * <p>Uses {@link #unitLock} to protect map mutations, then starts/stops units outside the lock to
   * avoid holding it during I/O.
   *
   * @param unitDefs Desired unit definitions (from DB or YAML)
   */
  private void reconcileUnits(List<NodeConfig.UnitDefinition> unitDefs) {
    List<Unit> unitsToStart = new ArrayList<>();
    List<Unit> unitsToStop = new ArrayList<>();

    synchronized (unitLock) {
      Set<String> desiredUnits = new HashSet<>();

      for (NodeConfig.UnitDefinition unitDef : unitDefs) {
        String unitName = unitDef.getName();

        if (!unitDef.isEnabled()) {
          Unit removed = unitsByName.remove(unitName);
          if (removed != null) {
            unitsToStop.add(removed);
          }
          continue;
        }

        desiredUnits.add(unitName);

        if (!unitsByName.containsKey(unitName)) {
          try {
            Unit unit =
                UnitFactory.createUnit(
                    unitDef, sharedResources, kafkaBatchingWriter, metadataPollerFactory);
            unitsByName.put(unitName, unit);
            logger.info("Created unit: {} (type={})", unitName, unit.getType());

            if (nodeStarted) {
              unitsToStart.add(unit);
            }
          } catch (Exception e) {
            logger.error("Failed to create unit: {}", unitName, e);
            if (unitName.startsWith("coordinator-")) {
              String tableName = unitName.substring("coordinator-".length());
              revertClaim(tableName);
            }
          }
        }
      }

      // Collect units no longer desired (ownership lost or table deactivated)
      Set<String> toRemove = new HashSet<>(unitsByName.keySet());
      toRemove.removeAll(desiredUnits);
      for (String unitName : toRemove) {
        if (unitName.startsWith("coordinator-")) {
          String tableName = unitName.substring("coordinator-".length());
          logger.warn("Lost ownership of table '{}', stopping unit", tableName);
        }
        Unit removed = unitsByName.remove(unitName);
        if (removed != null) {
          unitsToStop.add(removed);
        }
      }
    }

    // Stop removed units outside the lock before starting new ones
    for (Unit unit : unitsToStop) {
      logger.info("Stopping removed unit: {}", unit.getName());
      try {
        unit.stop();
        unit.close();
      } catch (Exception e) {
        logger.error("Error stopping unit: {}", unit.getName(), e);
      }
    }

    // Start new units outside the lock
    for (Unit unit : unitsToStart) {
      try {
        unit.start();
        logger.info("Started new unit: {} (type={})", unit.getName(), unit.getType());
      } catch (Exception e) {
        logger.error("Failed to start unit: {}", unit.getName(), e);
        synchronized (unitLock) {
          unitsByName.remove(unit.getName());
        }
        if (unit.getName().startsWith("coordinator-")) {
          String tableName = unit.getName().substring("coordinator-".length());
          revertClaim(tableName);
        }
      }
    }
  }

  /** Stop and remove a unit by name. Removes from map under lock, stops outside. */
  private void stopAndRemoveUnit(String unitName) {
    Unit unit;
    synchronized (unitLock) {
      unit = unitsByName.remove(unitName);
    }
    if (unit != null) {
      logger.info("Stopping removed unit: {}", unitName);
      try {
        unit.stop();
        unit.close();
      } catch (Exception e) {
        logger.error("Error stopping unit: {}", unitName, e);
      }
    }
  }

  /** Signal a BatchingWriter to stop and wait for its per-table threads to drain. */
  private static final long BATCHING_WRITER_DRAIN_TIMEOUT_MS = 30_000;

  private static void stopBatchingWriter(BatchingWriter writer, String label) {
    if (writer == null) {
      return;
    }
    writer.stop();
    writer.join(BATCHING_WRITER_DRAIN_TIMEOUT_MS);
    logger.info("BatchingWriter ({}) drained", label);
  }

  /**
   * Revert a table claim by setting node_id to NULL.
   *
   * <p>Called when a CoordinatorUnit fails to start (Edge Case 6.6). The table becomes unassigned
   * so another node can claim it.
   */
  private void revertClaim(String tableName) {
    try {
      CoordinatorRegistry.revertClaim(sharedResources.getDataSource(), tableName, nodeId);
    } catch (SQLException e) {
      logger.error("Failed to revert claim for table '{}'", tableName, e);
    }
  }

  /** Get the node name (from config). */
  public String getName() {
    return name;
  }

  /** Get the node ID (hostname) used for table assignment. */
  public String getNodeId() {
    return nodeId;
  }

  /** Get all units. */
  public Map<String, Unit> getUnits() {
    return Map.copyOf(unitsByName);
  }

  /** Get a unit by name. */
  public Unit getUnit(String name) {
    return unitsByName.get(name);
  }

  /** Get the shared resources. */
  public SharedResources getSharedResources() {
    return sharedResources;
  }

  /** Get the ingestion service (available after {@link #start()}). */
  public IngestionService getIngestionService() {
    return ingestionService;
  }

  /**
   * Obfuscate a password for logging purposes.
   *
   * @param password the password to obfuscate
   * @return "****" if password is non-null and non-empty, otherwise "(not set)"
   */
  private static String obfuscatePassword(String password) {
    if (password == null || password.isEmpty()) {
      return "(not set)";
    }
    return "****";
  }
}
