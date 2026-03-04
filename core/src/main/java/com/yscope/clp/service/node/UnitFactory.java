package com.yscope.clp.service.node;

import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.yscope.clp.service.coordinator.ingestion.MetadataPollerFactory;

/**
 * Factory for creating Unit instances from configuration.
 *
 * <p>Creates the appropriate Unit implementation based on the unit type specified in the
 * configuration.
 */
public class UnitFactory {

  /**
   * Create a Unit from a definition.
   *
   * @param definition Unit definition from configuration
   * @param sharedResources Resources shared across all units
   * @param batchingWriter Shared batching writer for database writes
   * @param metadataPollerFactory Factory for creating metadata pollers
   * @return The created Unit instance
   * @throws IllegalArgumentException if the unit type is unknown
   */
  public static Unit createUnit(
      NodeConfig.UnitDefinition definition,
      SharedResources sharedResources,
      BatchingWriter batchingWriter,
      MetadataPollerFactory metadataPollerFactory) {
    String type = definition.getType();
    if (type == null) {
      throw new IllegalArgumentException("Unit type is required for unit: " + definition.getName());
    }

    return switch (type.toLowerCase()) {
      case "coordinator" ->
          new CoordinatorUnit(definition, sharedResources, batchingWriter, metadataPollerFactory);
      case "worker" -> new WorkerUnit(definition, sharedResources);
      default ->
          throw new IllegalArgumentException(
              "Unknown unit type: " + type + " for unit: " + definition.getName());
    };
  }
}
