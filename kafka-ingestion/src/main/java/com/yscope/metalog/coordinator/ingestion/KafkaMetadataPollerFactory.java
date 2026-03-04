package com.yscope.metalog.coordinator.ingestion;

import com.yscope.metalog.coordinator.ingestion.record.RecordTransformer;

/** {@link MetadataPollerFactory} that creates Kafka-backed {@link MetadataConsumer} instances. */
public class KafkaMetadataPollerFactory implements MetadataPollerFactory {

  @Override
  public MetadataPoller create(
      String bootstrapServers,
      String groupId,
      String topic,
      int maxPollRecords,
      RecordTransformer transformer) {
    return new MetadataConsumer(bootstrapServers, groupId, topic, maxPollRecords, transformer);
  }
}
