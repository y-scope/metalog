package com.yscope.clp.service.coordinator.ingestion.server;

import com.yscope.clp.service.coordinator.ingestion.IngestionService;

/** Factory for creating {@link IngestionServer} instances, injectable into Node. */
@FunctionalInterface
public interface IngestionServerFactory {
  IngestionServer create(int port, IngestionService service);

  /** Factory that rejects server creation (for integrators providing their own transport). */
  static IngestionServerFactory disabled() {
    return (port, service) -> {
      throw new UnsupportedOperationException(
          "Built-in ingestion server is disabled; set grpcIngestion.enabled=false"
              + " or provide a custom IngestionServerFactory");
    };
  }
}
