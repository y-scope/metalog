package com.yscope.clp.service.node;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.common.config.Defaults;
import com.yscope.clp.service.common.config.IndexConfig;
import com.yscope.clp.service.common.config.YamlConfigLoader;
import com.yscope.clp.service.common.storage.StorageException;
import com.yscope.clp.service.coordinator.ProgressTracker;
import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.yscope.clp.service.coordinator.ingestion.MetadataPoller;
import com.yscope.clp.service.coordinator.ingestion.MetadataPollerFactory;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformer;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformerFactory;
import com.yscope.clp.service.coordinator.workflow.consolidation.ArchivePathGenerator;
import com.yscope.clp.service.coordinator.workflow.consolidation.InFlightSet;
import com.yscope.clp.service.coordinator.workflow.consolidation.Planner;
import com.yscope.clp.service.coordinator.workflow.consolidation.Policy;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyConfig;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyFactory;
import com.yscope.clp.service.metastore.FileRecords;
import com.yscope.clp.service.metastore.TaskQueue;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.schema.DynamicIndexManager;
import com.yscope.clp.service.metastore.schema.PartitionManager;
import com.yscope.clp.service.metastore.schema.TableProvisioner;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator as a Unit that can be hosted within a Node.
 *
 * <p>The CoordinatorUnit wraps the Coordinator logic but accepts shared resources (DataSource,
 * StorageRegistry) from the Node instead of creating its own.
 *
 * <p>Unit-owned resources:
 *
 * <ul>
 *   <li>MetadataPoller - each coordinator has its own topic/group
 *   <li>FileRecords (uses shared DataSource)
 *   <li>All coordinator threads
 *   <li>YAML config watchers
 * </ul>
 *
 * <p>Shared resources (from Node):
 *
 * <ul>
 *   <li>HikariDataSource
 *   <li>StorageRegistry
 * </ul>
 */
public class CoordinatorUnit implements Unit {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorUnit.class);

  // Unit identity
  private final String name;
  private final String instanceId;
  private final CoordinatorUnitConfig unitConfig;

  // Shared resources (owned by Node)
  private final SharedResources sharedResources;

  // Unit-owned components
  private MetadataPoller metadataPoller;
  private FileRecords repository;
  private TaskQueue taskQueue;
  private PartitionManager partitionManager;
  private DynamicIndexManager indexManager;
  private InFlightSet inFlightSet;
  private ArchivePathGenerator archivePathGenerator;
  private volatile List<Policy> policies;

  // Shared batching writer (owned by Node)
  private final BatchingWriter batchingWriter;

  // Factory for creating metadata pollers
  private final MetadataPollerFactory metadataPollerFactory;

  // YAML configuration with hot-reload
  private YamlConfigLoader<PolicyConfig> policyConfig;
  private YamlConfigLoader<IndexConfig> indexConfig;

  // Threading
  private BlockingQueue<String> deletionQueue;
  private ExecutorService backgroundPoller;
  private Planner planner;
  private Thread plannerThread;
  private Thread storageDeletionThread;
  private Thread retentionCleanupThread;

  // Running state
  private final AtomicBoolean running = new AtomicBoolean(false);

  // Progress tracking for watchdog
  private final ProgressTracker progressTracker = new ProgressTracker();

  // Throughput tracking
  private final AtomicLong recordsPolled = new AtomicLong(0);
  private volatile long lastThroughputLogTime = 0;
  private volatile long lastThroughputLogCount = 0;

  /**
   * Create a CoordinatorUnit.
   *
   * @param definition Unit definition from configuration
   * @param sharedResources Resources shared across all units
   * @param batchingWriter Shared batching writer for database writes
   * @param metadataPollerFactory Factory for creating metadata pollers
   */
  public CoordinatorUnit(
      NodeConfig.UnitDefinition definition,
      SharedResources sharedResources,
      BatchingWriter batchingWriter,
      MetadataPollerFactory metadataPollerFactory) {
    this.name = definition.getName();
    this.instanceId = UUID.randomUUID().toString();
    this.sharedResources = sharedResources;
    this.batchingWriter = batchingWriter;
    this.metadataPollerFactory = metadataPollerFactory;
    this.unitConfig = CoordinatorUnitConfig.fromMap(definition.getConfig());

    logger.info(
        "Created CoordinatorUnit: name={}, instanceId={}, table={}",
        name,
        instanceId,
        unitConfig.getTable());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return "coordinator";
  }

  @Override
  public void start() throws Exception {
    if (running.getAndSet(true)) {
      logger.warn("CoordinatorUnit {} is already running", name);
      return;
    }

    try {
      doStart();
    } catch (Exception e) {
      // Clean up any partially initialized resources and threads.
      // Keep running=true so close() calls stop() to shut down any threads that were started.
      logger.error("[{}] Start failed, cleaning up partially initialized resources", name, e);
      close();
      throw e;
    }
  }

  /** Internal start logic, separated so that start() can wrap it with cleanup-on-failure. */
  private void doStart() throws Exception {
    logger.info(
        "Starting CoordinatorUnit: {} (instanceId={}, table={}, kafkaTopic={}, kafkaGroup={})",
        name,
        instanceId,
        unitConfig.getTable(),
        unitConfig.getKafka().getTopic(),
        unitConfig.getKafka().getGroupId());

    // Initialize deletion queue unconditionally — used by Planner and storageDeletionLoop
    // independently of Kafka ingestion
    this.deletionQueue = new LinkedBlockingQueue<>(Defaults.DELETION_QUEUE_CAPACITY);

    // Initialize ingestion infrastructure (only needed when poller is enabled)
    boolean ingestionEnabled = unitConfig.isKafkaPollerEnabled();
    if (ingestionEnabled) {

      RecordTransformer recordTransformer =
          RecordTransformerFactory.create(unitConfig.getRecordTransformer());
      CoordinatorUnitConfig.KafkaConfig kafkaConfig = unitConfig.getKafka();
      this.metadataPoller =
          metadataPollerFactory.create(
              kafkaConfig.getBootstrapServers(),
              kafkaConfig.getGroupId(),
              kafkaConfig.getTopic(),
              500, // max poll records
              recordTransformer);
    }

    // Initialize repository using shared DataSource
    this.repository = new FileRecords(sharedResources.getDataSource(), unitConfig.getTable());

    // Auto-provision table (idempotent: dialect-aware DDL, registry rows, partitions)
    TableProvisioner.ensureTable(sharedResources.getDataSource(), unitConfig.getTable());

    // Initialize consolidation components (only needed when consolidation is enabled)
    if (unitConfig.isConsolidationEnabled()) {
      this.taskQueue = new TaskQueue(sharedResources.getDslContext());
      this.inFlightSet = new InFlightSet();
      this.archivePathGenerator = new ArchivePathGenerator(unitConfig.getTable(), instanceId);
    }

    // Initialize partition manager
    this.partitionManager =
        new PartitionManager(
            sharedResources.getDslContext(),
            unitConfig.getTable(),
            7, // lookahead days
            90, // cleanup age days
            1000 // sparse row threshold
            );

    // Initialize index manager
    this.indexManager =
        new DynamicIndexManager(sharedResources.getDslContext(), unitConfig.getTable());

    // Initialize policies (only needed when consolidation is enabled)
    if (unitConfig.isConsolidationEnabled()) {
      this.policies = initializePolicies();
    }

    // Note: storage connectivity is validated lazily on first I/O operation

    // Recovery
    recoverFromRestart();

    // Initialize index configuration
    initializeIndexConfig();

    // Ensure lookahead partitions (if partitioning is supported)
    if (unitConfig.isPartitionManagerEnabled()) {
      try {
        int created = partitionManager.ensureLookaheadPartitions();
        if (created > 0) {
          logger.info("[{}] Startup: Created {} lookahead partitions", name, created);
        }
      } catch (SQLException e) {
        logger.warn("[{}] Failed to create lookahead partitions (may not be supported)", name, e);
      }
    }

    // Start policy hot-reload if enabled (only when consolidation is enabled)
    if (unitConfig.isConsolidationEnabled()
        && unitConfig.isPolicyHotReloadEnabled()
        && policyConfig != null) {
      policyConfig.startWatching(
          (oldConfig, newConfig) -> {
            Map<String, Policy> oldPolicyMap = PolicyFactory.buildPolicyMap(policies);
            policies = PolicyFactory.createPoliciesWithStateTransfer(newConfig, oldPolicyMap);
            if (planner != null) {
              planner.setPolicies(policies);
            }
            logger.info("[{}] Policy configuration reloaded: {} policies", name, policies.size());
          });
    }

    // Thread 1: Background Kafka poller (if enabled)
    if (unitConfig.isKafkaPollerEnabled()) {
      progressTracker.register("poller");
      this.backgroundPoller =
          Executors.newSingleThreadExecutor(
              r -> {
                Thread t = new Thread(r, "KafkaPoller-" + name);
                t.setDaemon(true);
                return t;
              });

      backgroundPoller.submit(
          () -> {
            logger.info("[{}] Thread 1 (Kafka poller) started", name);
            lastThroughputLogTime = System.currentTimeMillis();
            lastThroughputLogCount = 0;
            int throughputIntervalMs = unitConfig.getThroughputLogIntervalSeconds() * 1000;

            while (running.get()) {
              try {
                // Record progress BEFORE blocking poll to avoid false stall detection
                progressTracker.recordProgress("poller");
                List<FileRecord> files = metadataPoller.poll(Duration.ofSeconds(5));
                if (!files.isEmpty()) {
                  logger.debug("[{}] Polled {} messages", name, files.size());
                  CompletableFuture<BatchingWriter.WriteResult> future =
                      batchingWriter.submitBlocking(unitConfig.getTable(), files);
                  future
                      .thenRun(
                          () -> {
                            // Commit offsets after successful DB write
                            Map<Integer, Long> offsets = computeMaxOffsets(files);
                            if (!offsets.isEmpty()) {
                              metadataPoller.commitOffsets(offsets);
                            }
                          })
                      .exceptionally(
                          ex -> {
                            logger.error(
                                "[{}] DB write failed, Kafka offsets not committed ({} files dropped)",
                                name,
                                files.size(),
                                ex);
                            return null;
                          });
                  recordsPolled.addAndGet(files.size());
                }

                // Periodic throughput logging (checked every poll, not just when records arrive)
                if (throughputIntervalMs > 0) {
                  long now = System.currentTimeMillis();
                  long elapsed = now - lastThroughputLogTime;
                  if (elapsed >= throughputIntervalMs) {
                    long currentCount = recordsPolled.get();
                    long recordsInInterval = currentCount - lastThroughputLogCount;
                    double rate = recordsInInterval / (elapsed / 1000.0);
                    // Include partition/offset info for observability
                    Map<Integer, Long> offsets = metadataPoller.getLastPolledOffsets();
                    String offsetSummary =
                        offsets.entrySet().stream()
                            .sorted(Comparator.comparingInt(Map.Entry::getKey))
                            .map(e -> "p" + e.getKey() + "=" + e.getValue())
                            .collect(java.util.stream.Collectors.joining(", "));
                    logger.info(
                        "[{}] Throughput: {} records/sec total across {} partitions [{}]",
                        unitConfig.getTable(),
                        String.format("%.1f", rate),
                        offsets.size(),
                        offsetSummary);
                    lastThroughputLogTime = now;
                    lastThroughputLogCount = currentCount;
                  }
                }
                progressTracker.recordProgress("poller");
              } catch (InterruptedException e) {
                logger.info("[{}] Kafka poller interrupted", name);
                Thread.currentThread().interrupt();
                break;
              } catch (RuntimeException e) {
                logger.error("[{}] Error in Kafka poller", name, e);
              }
            }
            logger.info("[{}] Thread 1 (Kafka poller) stopped", name);
          });
    } else {
      logger.info("[{}] Thread 1 (Kafka poller) disabled", name);
    }

    // Thread 3: Planner thread (if consolidation is enabled)
    if (unitConfig.isConsolidationEnabled()) {
      progressTracker.register("planner");
      planner =
          new Planner(
              createServiceConfigAdapter(),
              unitConfig.getTable(),
              repository,
              taskQueue,
              inFlightSet,
              archivePathGenerator,
              sharedResources.getStorageRegistry(),
              policies,
              deletionQueue,
              progressTracker);
      plannerThread = new Thread(planner, "Planner-" + name);
      plannerThread.start();
      logger.info("[{}] Thread 3 (Planner) started", name);
    } else {
      logger.info("[{}] Thread 3 (Planner) disabled (consolidation disabled)", name);
    }

    // Thread 4: Storage deletion thread (if deletion is enabled and storage is configured)
    if (unitConfig.isDeletionEnabled() && sharedResources.getStorageRegistry() != null) {
      progressTracker.register("deleter");
      storageDeletionThread = new Thread(this::storageDeletionLoop, "StorageDeletion-" + name);
      storageDeletionThread.start();
      logger.info("[{}] Thread 4 (Storage deletion) started", name);
    } else if (unitConfig.isDeletionEnabled()) {
      logger.info("[{}] Thread 4 (Storage deletion) skipped (no storage client configured)", name);
    } else {
      logger.info("[{}] Thread 4 (Storage deletion) disabled", name);
    }

    // Thread 5: Retention cleanup (if enabled) — partition management moved to node-level
    if (unitConfig.isRetentionCleanupEnabled()) {
      progressTracker.register("retention-cleanup");
      retentionCleanupThread = new Thread(this::retentionCleanupLoop, "RetentionCleanup-" + name);
      retentionCleanupThread.start();
      logger.info("[{}] Thread 5 (Retention cleanup) started", name);
    } else {
      logger.info("[{}] Thread 5 (Retention cleanup) disabled", name);
    }

    logger.info(
        "[{}] CoordinatorUnit started with {} active threads",
        name,
        (unitConfig.isKafkaPollerEnabled() ? 1 : 0)
            + (unitConfig.isConsolidationEnabled() ? 1 : 0)
            + (unitConfig.isDeletionEnabled() ? 1 : 0)
            + (unitConfig.isRetentionCleanupEnabled() ? 1 : 0));
  }

  @Override
  public void stop() {
    if (!running.getAndSet(false)) {
      logger.warn("CoordinatorUnit {} is not running", name);
      return;
    }

    logger.info("Stopping CoordinatorUnit: {} (instanceId={})", name, instanceId);
    long shutdownStart = System.currentTimeMillis();

    // Phase 1: Signal all threads to stop
    logger.debug("[{}] Phase 1: Signaling all threads to stop", name);
    if (planner != null) {
      planner.stop();
    }

    // Phase 2: Stop Kafka poller
    logger.debug("[{}] Phase 2: Stopping Kafka poller", name);
    if (backgroundPoller != null) {
      backgroundPoller.shutdownNow();
      try {
        backgroundPoller.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Phase 3: Wait for Planner thread to exit
    logger.debug("[{}] Phase 3: Waiting for Planner thread to exit", name);
    try {
      if (plannerThread != null && plannerThread.isAlive()) {
        plannerThread.interrupt();
        plannerThread.join(5000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Phase 4: Stop remaining threads
    logger.debug("[{}] Phase 4: Stopping remaining threads", name);
    if (storageDeletionThread != null && storageDeletionThread.isAlive()) {
      storageDeletionThread.interrupt();
    }
    if (retentionCleanupThread != null && retentionCleanupThread.isAlive()) {
      retentionCleanupThread.interrupt();
    }

    try {
      if (storageDeletionThread != null && storageDeletionThread.isAlive()) {
        storageDeletionThread.join(2000);
      }
      if (retentionCleanupThread != null && retentionCleanupThread.isAlive()) {
        retentionCleanupThread.join(2000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Clear registered threads to prevent stale entries on restart
    progressTracker.clear();

    long totalShutdownTime = System.currentTimeMillis() - shutdownStart;
    logger.info("[{}] CoordinatorUnit stopped (shutdown took {}ms)", name, totalShutdownTime);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void close() {
    // Stop if still running
    if (running.get()) {
      stop();
    }

    // Close unit-owned resources
    logger.debug("[{}] Phase 5: Closing unit-owned resources", name);

    if (policyConfig != null) {
      policyConfig.close();
    }
    if (indexConfig != null) {
      indexConfig.close();
    }
    if (metadataPoller != null) {
      metadataPoller.close();
    }
    if (repository != null) {
      repository.close(); // Won't close shared DataSource
    }

    logger.info("[{}] CoordinatorUnit closed", name);
  }

  /** Initialize policies from YAML config or fall back to default. */
  private List<Policy> initializePolicies() {
    String policyPath = unitConfig.getPolicyConfigPath();

    try {
      if (policyPath != null) {
        policyConfig =
            new YamlConfigLoader<>(PolicyConfig.class, Paths.get(policyPath), "config/policy.yaml");
      } else {
        policyConfig = new YamlConfigLoader<>(PolicyConfig.class, "config/policy.yaml");
      }

      List<Policy> yamlPolicies = PolicyFactory.createPolicies(policyConfig.get());
      if (!yamlPolicies.isEmpty()) {
        logger.info("[{}] Loaded {} policies from YAML configuration", name, yamlPolicies.size());
        return yamlPolicies;
      }
    } catch (YamlConfigLoader.ConfigException e) {
      logger.warn("[{}] Failed to load YAML policy config", name, e);
    }

    // Return empty list - will need default policy creation
    logger.info("[{}] Using default policies", name);
    return new ArrayList<>();
  }

  /**
   * Initialize database schema by executing schema.sql from classpath.
   *
   * <p>Reads {@code schema/schema.sql}, replaces the hardcoded table name with the unit's
   * configured table name, and executes each statement. All statements use {@code CREATE TABLE IF
   * NOT EXISTS}, making this idempotent.
   *
   * <p>Also creates the shared {@code _task_queue} table.
   */
  /** Initialize index configuration and reconcile with database. */
  private void initializeIndexConfig() {
    String indexPath = unitConfig.getIndexConfigPath();

    try {
      if (indexPath != null) {
        indexConfig =
            new YamlConfigLoader<>(IndexConfig.class, Paths.get(indexPath), "index/index.yaml");
      } else {
        indexConfig = new YamlConfigLoader<>(IndexConfig.class, "index/index.yaml");
      }

      DynamicIndexManager.ReconcileResult result = indexManager.reconcile(indexConfig.get());
      if (!result.created.isEmpty() || !result.dropped.isEmpty()) {
        logger.info(
            "[{}] Index reconciliation: created={}, dropped={}",
            name,
            result.created,
            result.dropped);
      }

      if (unitConfig.isIndexHotReloadEnabled()) {
        indexConfig.startWatching(
            (oldConfig, newConfig) -> {
              logger.info("[{}] Index configuration changed, reconciling", name);
              DynamicIndexManager.ReconcileResult reloadResult = indexManager.reconcile(newConfig);
              logger.info("[{}] Index reconciliation after reload: {}", name, reloadResult);
            });
      }
    } catch (YamlConfigLoader.ConfigException e) {
      logger.warn("[{}] Failed to load index config, skipping dynamic index management", name, e);
    }
  }

  /** Recover from coordinator restart. */
  private void recoverFromRestart() throws InterruptedException {
    logger.info("[{}] Starting recovery from restart...", name);

    if (!unitConfig.isConsolidationEnabled()) {
      logger.info(
          "[{}] Consolidation disabled, using Kafka consumer group offset for recovery", name);
      return;
    }

    try {
      String tableName = unitConfig.getTable();

      // Wait for MySQL UNIX_TIMESTAMP() to advance past the current second.
      // UNIX_TIMESTAMP() returns integer seconds, so sleeping 1100ms guarantees
      // we cross the second boundary even in the worst case (called at X.999).
      taskQueue.getDatabaseTimestamp(); // verify DB connectivity
      Thread.sleep(1100);

      // Delete all tasks except dead_letter
      int deleted = taskQueue.deleteAllExceptDeadLetter(tableName);
      if (deleted > 0) {
        logger.info("[{}] Deleted {} tasks during recovery", name, deleted);
      }

      // Kafka consumer group resumes from last committed offset automatically
      // (group ID: clp-coordinator-{table_name}-{table_id})
      logger.info("[{}] Kafka consumer group will resume from last committed offset", name);

      // Clear InFlightSet
      inFlightSet.clear();

      logger.info("[{}] Recovery complete", name);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (SQLException e) {
      logger.error("[{}] Error during recovery, continuing with empty state", name, e);
      inFlightSet.clear();
    }
  }

  /** Storage deletion thread loop. */
  private void storageDeletionLoop() {
    long deletionDelayMs = unitConfig.getStorageDeletionDelayMs();

    while (running.get()) {
      try {
        String path = deletionQueue.poll(100, TimeUnit.MILLISECONDS);
        if (path != null) {
          deletePathWithRateLimit(path, deletionDelayMs);
        }
        progressTracker.recordProgress("deleter");
      } catch (InterruptedException e) {
        logger.info("[{}] Storage deletion thread interrupted", name);
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException e) {
        logger.error("[{}] Error in storage deletion loop", name, e);
      }
    }

    logger.info("[{}] Thread 4 (Storage deletion) stopped", name);
  }

  /** Delete a single path with rate limiting. */
  private void deletePathWithRateLimit(String path, long delayMs) throws InterruptedException {
    try {
      sharedResources.getStorageRegistry().getIrBackend().deleteFile(path);
      logger.debug("[{}] Deleted file: {}", name, path);
    } catch (StorageException e) {
      logger.warn("[{}] Failed to delete file {}", name, path, e);
    }

    if (delayMs > 0) {
      Thread.sleep(delayMs);
    }
  }

  /**
   * Retention cleanup thread loop.
   *
   * <p>Periodically deletes expired rows from the database using {@link
   * FileRecords#deleteExpiredFiles(long)}. This is database-only cleanup and does not touch object
   * storage.
   */
  private void retentionCleanupLoop() {
    long intervalMs = unitConfig.getRetentionCleanupIntervalMs();
    // Sleep in small chunks to avoid appearing stalled to the watchdog
    long sleepChunkMs = 10_000; // 10 seconds

    while (running.get()) {
      try {
        // Sleep in chunks while recording progress periodically
        long remainingSleepMs = intervalMs;
        while (remainingSleepMs > 0 && running.get()) {
          progressTracker.recordProgress("retention-cleanup");
          long sleepMs = Math.min(sleepChunkMs, remainingSleepMs);
          Thread.sleep(sleepMs);
          remainingSleepMs -= sleepMs;
        }
        if (!running.get()) break;
        long now = Timestamps.nowNanos();
        int deleted = repository.deleteExpiredFiles(now);
        if (deleted > 0) {
          logger.info("[{}] Retention cleanup: deleted {} expired rows", name, deleted);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("[{}] Error in retention cleanup", name, e);
      }
    }
  }

  /** Get the coordinator instance ID. */
  public String getInstanceId() {
    return instanceId;
  }

  /** Get the table name managed by this coordinator. */
  public String getTableName() {
    return unitConfig.getTable();
  }

  /** Get the progress tracker for health monitoring. */
  public ProgressTracker getProgressTracker() {
    return progressTracker;
  }

  /**
   * Compute per-partition max offsets from a batch of FileRecords.
   *
   * @param files batch of files with source partition/offset metadata
   * @return map of partition → max offset for Kafka commit
   */
  private static Map<Integer, Long> computeMaxOffsets(List<FileRecord> files) {
    Map<Integer, Long> offsets = new HashMap<>();
    for (FileRecord file : files) {
      if (file.hasSourceOffset() && file.hasSourcePartition()) {
        offsets.merge(file.getSourcePartition(), file.getSourceOffset(), Math::max);
      }
    }
    return offsets;
  }

  /**
   * Create an ServiceConfig adapter for components that require it.
   *
   * <p>This creates a minimal ServiceConfig that proxies to our unit configuration. Only the
   * methods actually used by Planner are implemented.
   */
  private com.yscope.clp.service.common.config.ServiceConfig createServiceConfigAdapter() {
    return new ServiceConfigAdapter(unitConfig);
  }

  /** Adapter class that provides ServiceConfig interface backed by CoordinatorUnitConfig. */
  private static class ServiceConfigAdapter
      extends com.yscope.clp.service.common.config.ServiceConfig {

    private final CoordinatorUnitConfig unitConfig;

    ServiceConfigAdapter(CoordinatorUnitConfig unitConfig) {
      this.unitConfig = unitConfig;
    }

    @Override
    public String getDatabaseTable() {
      return unitConfig.getTable();
    }

    @Override
    public long getWriterLoopIntervalMs() {
      return unitConfig.getLoopIntervalMs();
    }

    @Override
    public long getPlannerIntervalMs() {
      return unitConfig.getLoopIntervalMs();
    }

    @Override
    public long getTaskTimeoutMs() {
      return 300000; // 5 minutes default
    }
  }
}
