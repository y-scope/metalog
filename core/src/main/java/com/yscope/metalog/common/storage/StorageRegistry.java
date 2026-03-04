package com.yscope.metalog.common.storage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Named registry of storage backends with dedicated IR and archive accessors.
 *
 * <p>Provides typed access to the two primary storage roles (IR files and archives) plus named
 * lookup for per-record routing when records come from different backends.
 */
public class StorageRegistry implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(StorageRegistry.class);

  private final StorageBackend irBackend;
  private final StorageBackend archiveBackend;
  private final Map<String, StorageBackend> allBackends;

  /**
   * Create a StorageRegistry.
   *
   * @param irBackend backend for reading/writing IR files
   * @param archiveBackend backend for reading/writing archive files
   * @param allBackends map of all named backends (must include irBackend and archiveBackend)
   */
  public StorageRegistry(
      StorageBackend irBackend,
      StorageBackend archiveBackend,
      Map<String, StorageBackend> allBackends) {
    this.irBackend = irBackend;
    this.archiveBackend = archiveBackend;
    this.allBackends = Collections.unmodifiableMap(new LinkedHashMap<>(allBackends));
  }

  /** Get the backend for IR files. */
  public StorageBackend getIrBackend() {
    return irBackend;
  }

  /** Get the backend for archive files. */
  public StorageBackend getArchiveBackend() {
    return archiveBackend;
  }

  /**
   * Look up a backend by name.
   *
   * @param name backend name
   * @return the backend
   * @throws IllegalArgumentException if no backend with that name exists
   */
  public StorageBackend getBackend(String name) {
    StorageBackend backend = allBackends.get(name);
    if (backend == null) {
      throw new IllegalArgumentException(
          "Unknown storage backend: " + name + " (known: " + allBackends.keySet() + ")");
    }
    return backend;
  }

  /** Get all registered backends. */
  public Map<String, StorageBackend> getAllBackends() {
    return allBackends;
  }

  @Override
  public void close() {
    for (var entry : allBackends.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
        logger.warn("Error closing backend: {}", entry.getKey(), e);
      }
    }
    logger.info("StorageRegistry closed ({} backends)", allBackends.size());
  }
}
