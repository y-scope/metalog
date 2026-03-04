package com.yscope.clp.service.coordinator.ingestion;

import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformer;

/**
 * Factory for creating {@link MetadataPoller} instances.
 *
 * <p>The core module uses this factory to create pollers without depending on any specific
 * messaging library. The concrete implementation (e.g., Kafka) is provided by a separate module.
 */
public interface MetadataPollerFactory {

  /**
   * Create a new metadata poller.
   *
   * @param bootstrapServers connection string for the messaging backend
   * @param groupId consumer group identifier
   * @param topic topic or channel to consume from
   * @param maxPollRecords maximum records to return per poll
   * @param transformer transformer for converting raw payloads to {@link
   *     com.yscope.clp.service.metastore.model.FileRecord}
   * @return a new poller instance
   */
  MetadataPoller create(
      String bootstrapServers,
      String groupId,
      String topic,
      int maxPollRecords,
      RecordTransformer transformer);

  /**
   * Returns a factory that throws {@link UnsupportedOperationException} on every {@code create}
   * call. Use this for nodes that do not require a metadata poller (e.g., headless mode).
   */
  static MetadataPollerFactory disabled() {
    return (bootstrapServers, groupId, topic, maxPollRecords, transformer) -> {
      throw new UnsupportedOperationException(
          "MetadataPoller is disabled; no messaging module is on the classpath");
    };
  }
}
