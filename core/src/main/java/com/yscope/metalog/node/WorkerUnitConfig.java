package com.yscope.metalog.node;

import java.util.Map;

/**
 * Configuration specific to a WorkerUnit.
 *
 * <p>Parsed from the unit's config map in node.yaml:
 *
 * <pre>
 * worker:
 *   numWorkers: 4
 *   prefetchQueueSize: 5
 * </pre>
 */
public class WorkerUnitConfig {

  private int numWorkers = 4;
  private int prefetchQueueSize = 5;

  /** Parse configuration from a map. */
  public static WorkerUnitConfig fromMap(Map<String, Object> config) {
    WorkerUnitConfig result = new WorkerUnitConfig();
    ConfigMapParser parser = new ConfigMapParser(config);
    result.numWorkers = parser.getInt("numWorkers", result.numWorkers);
    result.prefetchQueueSize = parser.getInt("prefetchQueueSize", result.prefetchQueueSize);
    return result;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public void setNumWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
  }

  public int getPrefetchQueueSize() {
    return prefetchQueueSize;
  }

  public void setPrefetchQueueSize(int prefetchQueueSize) {
    this.prefetchQueueSize = prefetchQueueSize;
  }
}
