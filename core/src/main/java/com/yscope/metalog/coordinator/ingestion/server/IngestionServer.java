package com.yscope.metalog.coordinator.ingestion.server;

import java.io.IOException;

/** Transport-agnostic interface for an ingestion server managed by Node. */
public interface IngestionServer {
  /**
   * Start the server and begin accepting requests.
   *
   * @throws IOException if the server fails to bind or start
   */
  void start() throws IOException;

  /** Stop the server gracefully, draining in-flight requests before returning. */
  void stop();
}
