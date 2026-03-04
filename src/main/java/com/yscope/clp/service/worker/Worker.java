package com.yscope.clp.service.worker;

import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.common.config.Timeouts;
import com.yscope.clp.service.common.config.db.DataSourceFactory;
import com.yscope.clp.service.common.health.HealthCheckServer;
import com.yscope.clp.service.common.storage.ArchiveCreator;
import com.yscope.clp.service.common.storage.ClpCompressor;
import com.yscope.clp.service.common.storage.StorageBackendFactory;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.yscope.clp.service.metastore.TaskQueue;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker process that executes aggregation tasks using a thread pool.
 *
 * <p>Architecture: Single JVM with N worker threads sharing:
 *
 * <ul>
 *   <li>HikariCP connection pool (N+2 connections)
 *   <li>S3 storage client (thread-safe)
 *   <li>TaskQueue (thread-safe)
 * </ul>
 *
 * <p>Each thread independently:
 *
 * <ol>
 *   <li>Claims task using SELECT ... FOR UPDATE + UPDATE
 *   <li>Downloads IR files from storage
 *   <li>Executes clp-s compression
 *   <li>Uploads archive to storage
 *   <li>Marks task complete
 * </ol>
 *
 * <p>Benefits:
 *
 * <ul>
 *   <li>Shared connection pool limits database connections
 *   <li>Single S3 client with connection reuse
 *   <li>Centralized configuration and shutdown handling
 * </ul>
 *
 * <p>Run as separate JVM:
 *
 * <pre>
 * java -cp clp-service.jar com.yscope.clp.service.worker.Worker
 * </pre>
 */
public class Worker {
  private static final Logger logger = LoggerFactory.getLogger(Worker.class);

  private final String nodeId;
  private final String tableName;
  private final int threadCount;
  private final HikariDataSource dataSource;
  private final StorageRegistry storageRegistry;
  private final ArchiveCreator archiveCreator;
  private final WorkerCore core;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Thread> workerThreads = new ArrayList<>();
  private final CountDownLatch shutdownLatch;

  public Worker(ServiceConfig config) {
    this.nodeId = "node-" + UUID.randomUUID().toString().substring(0, 8);
    this.tableName = config.getDatabaseTable();
    this.threadCount = config.getWorkerThreadCount();

    // Create shared database connection pool (small fixed size - DB ops are brief)
    this.dataSource = createDataSource(config);
    TaskQueue taskQueue = new TaskQueue(dataSource);

    // Create shared storage registry (thread-safe)
    this.storageRegistry =
        StorageBackendFactory.createObjectStorageRegistry(
            config.getStorageDefaultBackend(),
            config.getStorageIrBucket(),
            config.getStorageArchiveBucket(),
            config.getStorageBackends());

    // Create archive creator and wire CLP compressor
    this.archiveCreator =
        new ArchiveCreator(storageRegistry.getIrBackend(), storageRegistry.getArchiveBackend());
    archiveCreator.setClpCompressor(
        new ClpCompressor(config.getClpBinaryPath(), config.getClpProcessTimeoutSeconds()));

    this.core =
        new WorkerCore(
            taskQueue,
            archiveCreator,
            storageRegistry.getArchiveBackend(),
            (id) -> taskQueue.claimTask(tableName, id),
            "");

    this.shutdownLatch = new CountDownLatch(threadCount);

    logger.info(
        "Worker node {} initialized: {} threads, db pool {}-{}, database {}:{}",
        nodeId,
        threadCount,
        config.getWorkerDbPoolMin(),
        config.getWorkerDbPoolMax(),
        config.getDatabaseHost(),
        config.getDatabasePort());
  }

  private HikariDataSource createDataSource(ServiceConfig config) {
    // Small pool - DB operations are brief (claim/complete),
    // threads spend most time on I/O (download/compress/upload)
    return DataSourceFactory.create(
        config.getJdbcUrl(),
        config.getDatabaseUser(),
        config.getDatabasePassword(),
        config.getWorkerDbPoolMin(),
        config.getWorkerDbPoolMax(),
        "Worker-" + nodeId,
        Timeouts.CONNECTION_TIMEOUT_MS,
        config.getWorkerDbPoolIdleTimeoutMs());
  }

  /** Start all worker threads. */
  public void start() {
    logger.info("Starting {} worker threads on node {}", threadCount, nodeId);

    for (int i = 0; i < threadCount; i++) {
      String workerId = nodeId + "-t" + i;
      Thread thread = new Thread(() -> workerLoop(workerId), "worker-" + workerId);
      thread.setDaemon(false);
      workerThreads.add(thread);
      thread.start();
    }

    logger.info("All {} worker threads started", threadCount);
  }

  private void workerLoop(String workerId) {
    try {
      core.runLoop(workerId, running);
    } finally {
      shutdownLatch.countDown();
      logger.info("[{}] Worker thread stopped", workerId);
    }
  }

  /** Gracefully stop all worker threads. */
  public void stop() {
    logger.info("Stopping worker node {} ({} threads)...", nodeId, threadCount);

    // Signal all threads to stop
    running.set(false);

    // Interrupt all threads
    for (Thread thread : workerThreads) {
      thread.interrupt();
    }

    // Wait for threads to finish (with timeout)
    try {
      boolean terminated = shutdownLatch.await(Timeouts.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!terminated) {
        logger.warn(
            "Some worker threads did not terminate within {} seconds",
            Timeouts.SHUTDOWN_TIMEOUT_SECONDS);
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for worker threads to stop");
      Thread.currentThread().interrupt();
    }

    // Close connection pool
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      logger.info("Connection pool closed");
    }

    // Close storage registry (releases S3 async client thread pools)
    if (storageRegistry != null) {
      storageRegistry.close();
      logger.info("Storage registry closed");
    }

    logger.info("Worker node {} stopped", nodeId);
  }

  public String getNodeId() {
    return nodeId;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public boolean isRunning() {
    return running.get();
  }

  public static void main(String[] args) {
    ServiceConfig config = new ServiceConfig();
    Worker worker = new Worker(config);

    // Start health check server (standalone mode only)
    HealthCheckServer healthCheckServer = null;
    if (config.isHealthCheckEnabled()) {
      healthCheckServer = startHealthCheckServer(config, worker);
    }

    // Shutdown hook for graceful termination
    final HealthCheckServer finalHealthServer = healthCheckServer;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Shutdown signal received");
                  if (finalHealthServer != null) {
                    finalHealthServer.stop();
                  }
                  worker.stop();
                }));

    worker.start();

    // Block main thread until all worker threads complete
    try {
      worker.shutdownLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Start the health check HTTP server (standalone mode only).
   *
   * <p>When running as a WorkerUnit inside a Node, the Node handles health checks.
   */
  private static HealthCheckServer startHealthCheckServer(ServiceConfig config, Worker worker) {
    try {
      HealthCheckServer server = new HealthCheckServer(config.getHealthCheckPort());

      // Liveness: are worker threads running?
      server.addLivenessCheck("worker-running", worker::isRunning);

      // Readiness: can we connect to the database?
      server.addReadinessCheck(
          "database",
          () -> {
            try (var conn = worker.dataSource.getConnection()) {
              return conn.isValid(1);
            } catch (Exception e) {
              return false;
            }
          });

      server.start();
      logger.info("Health check server started on port {}", config.getHealthCheckPort());
      return server;
    } catch (IOException e) {
      logger.error(
          "Failed to start health check server on port {}", config.getHealthCheckPort(), e);
      return null;
    }
  }
}
