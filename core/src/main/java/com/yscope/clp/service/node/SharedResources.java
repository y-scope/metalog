package com.yscope.clp.service.node;

import com.yscope.clp.service.common.config.db.DSLContextFactory;
import com.yscope.clp.service.common.storage.ArchiveCreator;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;

/**
 * Container for resources shared across all units in a Node.
 *
 * <p>These resources are owned by the Node and must NOT be closed by individual units. The Node is
 * responsible for closing these resources during shutdown, after all units have been stopped.
 *
 * <p>Shared resources include:
 *
 * <ul>
 *   <li>{@link HikariDataSource} - Database connection pool for coordinators (master)
 *   <li>{@link HikariDataSource} - Optional database connection pool for workers (replica)
 *   <li>{@link DSLContext} - jOOQ DSL context with auto-detected dialect
 *   <li>{@link StorageRegistry} - Named storage backends shared by all units
 *   <li>{@link ArchiveCreator} - Archive creation orchestrator shared by workers
 * </ul>
 *
 * <p>Benefits of sharing:
 *
 * <ul>
 *   <li>Reduced connection count - single pool instead of N pools
 *   <li>Consistent connection reuse across units
 *   <li>Simplified resource management
 * </ul>
 */
public class SharedResources implements AutoCloseable {

  private final HikariDataSource dataSource;
  private final HikariDataSource workerDataSource;
  private final DSLContext dslContext;
  private final DSLContext workerDslContext;
  private final StorageRegistry storageRegistry;
  private final ArchiveCreator archiveCreator;

  /**
   * Create shared resources container without a separate worker DataSource.
   *
   * @param dataSource Shared database connection pool (used by all units)
   * @param storageRegistry Shared storage registry
   * @param archiveCreator Shared archive creator (may be null for coordinator-only nodes)
   */
  public SharedResources(
      HikariDataSource dataSource,
      StorageRegistry storageRegistry,
      ArchiveCreator archiveCreator) {
    this(dataSource, null, storageRegistry, archiveCreator);
  }

  /**
   * Create shared resources container with an optional separate worker DataSource.
   *
   * @param dataSource Database connection pool for coordinators (master)
   * @param workerDataSource Optional database connection pool for workers (replica), or null
   * @param storageRegistry Shared storage registry (may be null for coordinator-only nodes)
   * @param archiveCreator Shared archive creator (may be null for coordinator-only nodes)
   */
  public SharedResources(
      HikariDataSource dataSource,
      HikariDataSource workerDataSource,
      StorageRegistry storageRegistry,
      ArchiveCreator archiveCreator) {
    this.dataSource = dataSource;
    this.workerDataSource = workerDataSource;
    this.dslContext = DSLContextFactory.create(dataSource);
    this.workerDslContext =
        workerDataSource != null ? DSLContextFactory.create(workerDataSource) : dslContext;
    this.storageRegistry = storageRegistry;
    this.archiveCreator = archiveCreator;
  }

  /**
   * Get the database connection pool for coordinators (master).
   *
   * @return HikariCP data source
   */
  public HikariDataSource getDataSource() {
    return dataSource;
  }

  /**
   * Get the database connection pool for workers. Falls back to the coordinator DataSource if no
   * separate worker DataSource is configured.
   *
   * @return HikariCP data source for worker use
   */
  public HikariDataSource getWorkerDataSource() {
    return workerDataSource != null ? workerDataSource : dataSource;
  }

  /**
   * Get the jOOQ DSLContext with auto-detected dialect.
   *
   * @return DSLContext backed by the coordinator DataSource
   */
  public DSLContext getDslContext() {
    return dslContext;
  }

  /**
   * Get the jOOQ DSLContext for workers. Falls back to the coordinator DSLContext if no separate
   * worker DataSource is configured.
   *
   * @return DSLContext backed by the worker DataSource
   */
  public DSLContext getWorkerDslContext() {
    return workerDslContext;
  }

  /**
   * Get the shared storage registry.
   *
   * @return Storage registry with named backends, or null if storage is not configured
   */
  public StorageRegistry getStorageRegistry() {
    return storageRegistry;
  }

  /**
   * Get the shared archive creator.
   *
   * @return Archive creator for building archives from IR files, or null if not configured
   */
  public ArchiveCreator getArchiveCreator() {
    return archiveCreator;
  }

  /**
   * Close all shared resources.
   *
   * <p>This should only be called by the Node after all units have been stopped.
   */
  @Override
  public void close() {
    if (workerDataSource != null
        && workerDataSource != dataSource
        && !workerDataSource.isClosed()) {
      workerDataSource.close();
    }
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
    if (storageRegistry != null) {
      storageRegistry.close();
    }
  }
}
