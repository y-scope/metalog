package com.yscope.clp.service.node;

import com.yscope.clp.service.common.config.Timeouts;
import com.yscope.clp.service.metastore.TaskQueue;
import com.yscope.clp.service.worker.TaskPrefetcher;
import com.yscope.clp.service.worker.WorkerCore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker as a Unit that can be hosted within a Node.
 *
 * <p>The WorkerUnit wraps the Worker logic but accepts shared resources (DataSource, StorageRegistry)
 * from the Node instead of creating its own.
 *
 * <p>Unit-owned resources:
 *
 * <ul>
 *   <li>Worker threads
 *   <li>TaskQueue (uses shared DataSource)
 * </ul>
 *
 * <p>Shared resources (from Node):
 *
 * <ul>
 *   <li>HikariDataSource
 *   <li>StorageRegistry
 * </ul>
 */
public class WorkerUnit implements Unit {
  private static final Logger logger = LoggerFactory.getLogger(WorkerUnit.class);

  // Unit identity
  private final String name;
  private final String workerInstanceId;
  private final WorkerUnitConfig unitConfig;

  // Shared resources (owned by Node)
  private final SharedResources sharedResources;

  // Unit-owned components
  private TaskPrefetcher prefetcher;
  private WorkerCore workerCore;
  private final List<Thread> workerThreads = new ArrayList<>();
  private CountDownLatch shutdownLatch;

  // Running state
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Create a WorkerUnit.
   *
   * @param definition Unit definition from configuration
   * @param sharedResources Resources shared across all units
   */
  public WorkerUnit(NodeConfig.UnitDefinition definition, SharedResources sharedResources) {
    this.name = definition.getName();
    this.workerInstanceId = "node-" + UUID.randomUUID().toString().substring(0, 8);
    this.sharedResources = sharedResources;
    this.unitConfig = WorkerUnitConfig.fromMap(definition.getConfig());

    logger.info(
        "Created WorkerUnit: name={}, workerInstanceId={}, numWorkers={}",
        name,
        workerInstanceId,
        unitConfig.getNumWorkers());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return "worker";
  }

  @Override
  public void start() throws Exception {
    if (running.getAndSet(true)) {
      logger.warn("WorkerUnit {} is already running", name);
      return;
    }

    try {
      int threadCount = unitConfig.getNumWorkers();
      logger.info(
          "Starting {} workers for unit: {} (workerInstanceId={})",
          threadCount,
          name,
          workerInstanceId);

      // Initialize task queue using shared DSLContext
      TaskQueue taskQueue = new TaskQueue(sharedResources.getWorkerDslContext());

      this.prefetcher =
          new TaskPrefetcher(
              taskQueue,
              "prefetch-" + workerInstanceId,
              unitConfig.getPrefetchQueueSize());
      prefetcher.start();

      this.workerCore =
          new WorkerCore(
              taskQueue,
              sharedResources.getArchiveCreator(),
              sharedResources.getStorageRegistry() != null
                  ? sharedResources.getStorageRegistry().getArchiveBackend()
                  : null,
              (id) -> prefetcher.poll(5000),
              "[" + name + "] ");

      // Initialize shutdown latch
      this.shutdownLatch = new CountDownLatch(threadCount);

      // Start worker threads
      for (int i = 0; i < threadCount; i++) {
        String workerId = workerInstanceId + "-t" + i;
        Thread thread = new Thread(() -> workerLoop(workerId), "worker-" + name + "-" + workerId);
        thread.setDaemon(false);
        workerThreads.add(thread);
        thread.start();
      }

      logger.info("[{}] All {} workers started", name, threadCount);
    } catch (Exception e) {
      logger.error("[{}] Start failed, cleaning up partially initialized resources", name, e);
      close();
      throw e;
    }
  }

  @Override
  public void stop() {
    if (!running.getAndSet(false)) {
      logger.warn("WorkerUnit {} is not running", name);
      return;
    }

    logger.info("Stopping WorkerUnit: {} ({} threads)", name, workerThreads.size());

    // Stop prefetch thread and reset any queued tasks back to pending
    if (prefetcher != null) {
      prefetcher.shutdown();
    }

    // Interrupt worker threads (unblocks any blocked queue.poll())
    for (Thread thread : workerThreads) {
      thread.interrupt();
    }

    // Wait for threads to finish (with timeout)
    try {
      boolean terminated = shutdownLatch.await(Timeouts.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!terminated) {
        logger.warn(
            "[{}] Some worker threads did not terminate within {} seconds",
            name,
            Timeouts.SHUTDOWN_TIMEOUT_SECONDS);
      }
    } catch (InterruptedException e) {
      logger.warn("[{}] Interrupted while waiting for worker threads to stop", name);
      Thread.currentThread().interrupt();
    }

    logger.info("[{}] WorkerUnit stopped", name);
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

    // Clear worker threads list
    workerThreads.clear();

    // Note: We do NOT close the shared DataSource or StorageRegistry

    logger.info("[{}] WorkerUnit closed", name);
  }

  private void workerLoop(String workerId) {
    try {
      workerCore.runLoop(workerId, running);
    } finally {
      shutdownLatch.countDown();
      logger.info("[{}] [{}] Worker thread stopped", name, workerId);
    }
  }

  /** Get the worker instance ID. */
  public String getNodeId() {
    return workerInstanceId;
  }

  /** Get the number of workers. */
  public int getNumWorkers() {
    return unitConfig.getNumWorkers();
  }
}
