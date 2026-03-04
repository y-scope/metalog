package com.yscope.clp.service.coordinator.ingestion;

import com.yscope.clp.service.metastore.model.FileRecord;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Abstraction over a metadata record source (e.g., Kafka consumer).
 *
 * <p>Implementations poll for metadata records, track per-partition offsets, and support
 * asynchronous offset commits. The core module programs against this interface so it has no direct
 * dependency on any specific messaging system.
 */
public interface MetadataPoller extends AutoCloseable {

  /**
   * Poll for metadata records.
   *
   * @param timeout maximum time to block waiting for records
   * @return list of parsed and validated file records; may be empty
   */
  List<FileRecord> poll(Duration timeout);

  /**
   * Request per-partition offset commits. Thread-safe; can be called from any thread.
   *
   * <p>Each entry maps a partition number to the last offset that has been fully processed.
   *
   * @param offsets map of partition number to last successfully processed offset
   */
  void commitOffsets(Map<Integer, Long> offsets);

  /**
   * Get the last polled offsets per partition.
   *
   * @return map of partition number to last polled offset
   */
  Map<Integer, Long> getLastPolledOffsets();

  /**
   * Check if the poller is connected to its backend.
   *
   * @return true if connected and ready to poll
   */
  boolean isConnected();

  /** Close the poller and release resources. */
  @Override
  void close();
}
