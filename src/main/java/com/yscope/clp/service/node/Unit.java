package com.yscope.clp.service.node;

/**
 * Interface for a unit that can be hosted within a Node.
 *
 * <p>Units represent independent execution contexts (coordinators or workers) that share common
 * resources (database pool, storage client) but maintain independent threading and lifecycle.
 *
 * <p>Units are created by {@link UnitFactory} based on configuration and are managed by the {@link
 * Node} which handles startup, shutdown, and hot-reload of units.
 *
 * <p>Resource ownership:
 *
 * <ul>
 *   <li>Shared resources (DataSource, StorageRegistry) - owned by Node
 *   <li>Unit-specific resources (Kafka consumers, threads) - owned by Unit
 * </ul>
 */
public interface Unit extends AutoCloseable {

  /**
   * Get the unique name of this unit.
   *
   * @return Unit name from configuration
   */
  String getName();

  /**
   * Get the type of this unit.
   *
   * @return "coordinator" or "worker"
   */
  String getType();

  /**
   * Start the unit.
   *
   * <p>This method should:
   *
   * <ul>
   *   <li>Initialize unit-specific resources
   *   <li>Start all unit threads
   *   <li>Return quickly (not block)
   * </ul>
   *
   * @throws Exception if startup fails
   */
  void start() throws Exception;

  /**
   * Signal the unit to stop.
   *
   * <p>This method should:
   *
   * <ul>
   *   <li>Signal all threads to stop
   *   <li>Wait for threads to finish (with timeout)
   *   <li>Return when the unit is stopped
   * </ul>
   */
  void stop();

  /**
   * Check if the unit is currently running.
   *
   * @return true if the unit is running
   */
  boolean isRunning();

  /**
   * Close unit-owned resources.
   *
   * <p>This method should only close resources owned by the unit (e.g., Kafka consumers). It should
   * NOT close shared resources like the DataSource or StorageRegistry.
   */
  @Override
  void close();
}
