package com.yscope.clp.service.coordinator.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformer;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformer.RecordTransformException;
import com.yscope.clp.service.coordinator.ingestion.record.RecordTransformerFactory;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.schema.RecordStateValidator;
import com.yscope.clp.service.metastore.schema.RecordStateValidator.ValidationResult;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka-based implementation of {@link MetadataPoller}. Consumes metadata messages from Kafka. */
public class MetadataConsumer implements MetadataPoller {
  private static final Logger logger = LoggerFactory.getLogger(MetadataConsumer.class);

  private final KafkaConsumer<String, byte[]> consumer;
  private final ObjectMapper objectMapper;
  private final RecordStateValidator validator;
  private final RecordTransformer recordTransformer;
  private final String topic;

  // Track the last successfully polled offset per partition for watermark calculation
  private final Map<TopicPartition, Long> lastPolledOffsets = new HashMap<>();

  // Cache assigned partitions (KafkaConsumer is not thread-safe, so we cache this)
  private volatile Set<TopicPartition> cachedAssignment = Collections.emptySet();

  // Pending per-partition offsets to commit (set by caller, committed by poll thread)
  private final java.util.concurrent.atomic.AtomicReference<Map<Integer, Long>>
      pendingCommitOffsets =
          new java.util.concurrent.atomic.AtomicReference<>(Collections.emptyMap());

  // Metrics for monitoring
  private long recordsDroppedParsing = 0;
  private long recordsDroppedValidation = 0;

  /**
   * Create a MetadataConsumer with ServiceConfig.
   *
   * @param config Application configuration
   */
  public MetadataConsumer(ServiceConfig config) {
    this(
        config.getKafkaBootstrapServers(),
        config.getKafkaGroupId(),
        config.getKafkaTopic(),
        config.getKafkaMaxPollRecords(),
        RecordTransformerFactory.create("default"));
  }

  /**
   * Create a MetadataConsumer with explicit parameters.
   *
   * <p>Used by the Node architecture where each CoordinatorUnit can have its own Kafka consumer
   * configuration.
   *
   * @param bootstrapServers Kafka bootstrap servers
   * @param groupId Consumer group ID
   * @param topic Kafka topic to consume from
   * @param maxPollRecords Maximum records to poll at once
   * @param recordTransformer Transformer for converting Kafka payloads to FileRecord; if null, uses
   *     default transformer via {@link RecordTransformerFactory}
   */
  public MetadataConsumer(
      String bootstrapServers,
      String groupId,
      String topic,
      int maxPollRecords,
      RecordTransformer recordTransformer) {
    this.topic = topic;
    this.objectMapper = new ObjectMapper();
    // Use snake_case to match database column naming (e.g., min_timestamp, application_id)
    this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    this.validator = new RecordStateValidator();
    this.recordTransformer =
        recordTransformer != null ? recordTransformer : RecordTransformerFactory.create("default");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    logger.info(
        "Connecting to Kafka: bootstrapServers={}, groupId={}, topic={}",
        bootstrapServers,
        groupId,
        topic);

    this.consumer = new KafkaConsumer<String, byte[]>(props);
    this.consumer.subscribe(Collections.singletonList(topic));

    logger.info("Kafka consumer initialized, will verify connection on first poll");
  }

  /** {@inheritDoc} */
  @Override
  public List<FileRecord> poll(Duration timeout) {
    // Commit any pending offset before polling (single-threaded access to consumer)
    commitPendingOffset();

    ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
    List<FileRecord> files = new ArrayList<>();

    // Cache assignment on each poll
    cachedAssignment = consumer.assignment();

    int droppedParsing = 0;
    int droppedValidation = 0;

    for (ConsumerRecord<String, byte[]> record : records) {
      try {
        FileRecord file = recordTransformer.transform(record.value(), objectMapper);
        // Track the source offset and partition for watermark calculation
        file.setSourceOffset(record.offset());
        file.setSourcePartition(record.partition());

        // Update last polled offset for this partition
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        lastPolledOffsets.put(tp, record.offset());

        // Validate record before accepting
        ValidationResult validationResult = validator.validate(file);
        if (!validationResult.isValid()) {
          droppedValidation++;
          recordsDroppedValidation++;
          logger.debug(
              "Dropping invalid record from partition={}, offset={}, path={}: {}",
              record.partition(),
              record.offset(),
              file.getIrPath(),
              validationResult.getErrors());
          continue; // Drop invalid record
        }

        // Log warnings but still accept the record
        if (!validationResult.getWarnings().isEmpty()) {
          logger.debug(
              "Record has warnings from partition={}, offset={}, path={}: {}",
              record.partition(),
              record.offset(),
              file.getIrPath(),
              validationResult.getWarnings());
        }

        files.add(file);

        logger.trace(
            "Parsed and validated message from partition={}, offset={}, path={}",
            record.partition(),
            record.offset(),
            file.getIrPath());
      } catch (RecordTransformException | RuntimeException e) {
        droppedParsing++;
        recordsDroppedParsing++;
        logger.debug(
            "Dropping unparseable message from partition={}, offset={}",
            record.partition(),
            record.offset(),
            e);
      }
    }

    if (droppedParsing > 0 || droppedValidation > 0) {
      logger.warn(
          "Dropped {} records in batch (parse={}, validation={})",
          droppedParsing + droppedValidation,
          droppedParsing,
          droppedValidation);
    }

    return files;
  }

  /**
   * Commit offsets after successful processing.
   *
   * @deprecated Use {@link #commitOffsets(Map)} for safe watermark-based commits
   */
  @Deprecated
  public void commitSync() {
    consumer.commitSync();
  }

  /**
   * Request per-partition offset commits. Thread-safe; can be called from any thread. The actual
   * commit happens in the poll thread before the next poll.
   *
   * <p>Each entry maps a partition number to the last offset that has been fully processed. The
   * committed Kafka offset will be (offset + 1).
   *
   * @param newOffsets Map of partition to last successfully processed offset
   */
  @Override
  public void commitOffsets(Map<Integer, Long> newOffsets) {
    if (newOffsets.isEmpty()) {
      return;
    }
    pendingCommitOffsets.updateAndGet(
        current -> {
          Map<Integer, Long> merged = new HashMap<>(current);
          newOffsets.forEach((partition, offset) -> merged.merge(partition, offset, Math::max));
          return merged;
        });
    logger.debug("Queued offsets for commit: {}", newOffsets);
  }

  /** Commit any pending per-partition offsets. Called from the poll thread only. */
  private void commitPendingOffset() {
    Map<Integer, Long> offsets = pendingCommitOffsets.getAndSet(Collections.emptyMap());
    if (offsets.isEmpty()) {
      return;
    }

    Set<TopicPartition> assignment = cachedAssignment;
    if (assignment.isEmpty()) {
      // Re-queue if no assignment yet
      final Map<Integer, Long> toRequeue = offsets;
      pendingCommitOffsets.updateAndGet(
          current -> {
            Map<Integer, Long> merged = new HashMap<>(current);
            toRequeue.forEach((p, o) -> merged.merge(p, o, Math::max));
            return merged;
          });
      return;
    }

    Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
      TopicPartition tp = new TopicPartition(topic, entry.getKey());
      kafkaOffsets.put(tp, new OffsetAndMetadata(entry.getValue() + 1));
    }

    consumer.commitSync(kafkaOffsets);
    logger.debug("Committed per-partition offsets: {}", offsets);
  }

  /** {@inheritDoc} */
  @Override
  public Map<Integer, Long> getLastPolledOffsets() {
    Map<Integer, Long> result = new HashMap<>();
    lastPolledOffsets.forEach((tp, offset) -> result.put(tp.partition(), offset));
    return result;
  }

  @Override
  public void close() {
    consumer.close();
  }

  /**
   * Check if the consumer is connected to Kafka.
   *
   * <p>Uses cached assignment from last poll. Returns true if the consumer has been assigned
   * partitions. Note: this is a lightweight check that doesn't make a network call.
   *
   * @return true if consumer has partition assignment
   */
  @Override
  public boolean isConnected() {
    return !cachedAssignment.isEmpty();
  }

  /** Get count of records dropped due to parsing errors. */
  public long getRecordsDroppedParsing() {
    return recordsDroppedParsing;
  }

  /** Get count of records dropped due to validation errors. */
  public long getRecordsDroppedValidation() {
    return recordsDroppedValidation;
  }

  /** Get total count of dropped records (parsing + validation). */
  public long getTotalRecordsDropped() {
    return recordsDroppedParsing + recordsDroppedValidation;
  }

  /**
   * Seek to a specific offset on all assigned partitions.
   *
   * <p>Used during coordinator recovery to rewind to the minimum source position from the task
   * queue.
   *
   * @param offset The offset to seek to
   */
  public void seekToOffset(long offset) {
    // Trigger a poll to get assignment (required before seeking; zero duration may not populate it)
    consumer.poll(Duration.ofMillis(100));
    Set<TopicPartition> assignment = consumer.assignment();

    if (assignment.isEmpty()) {
      logger.warn("No partitions assigned, cannot seek to offset {}", offset);
      return;
    }

    for (TopicPartition tp : assignment) {
      consumer.seek(tp, offset);
      logger.info("Seeked partition {} to offset {}", tp, offset);
    }
  }

  /**
   * Seek to the end of all assigned partitions.
   *
   * <p>Used during coordinator recovery when there are no tasks to recover, meaning we can start
   * fresh from the end of the topic.
   */
  public void seekToEnd() {
    // Trigger a poll to get assignment (required before seeking; zero duration may not populate it)
    consumer.poll(Duration.ofMillis(100));
    Set<TopicPartition> assignment = consumer.assignment();

    if (assignment.isEmpty()) {
      logger.warn("No partitions assigned, cannot seek to end");
      return;
    }

    consumer.seekToEnd(assignment);
    logger.info("Seeked {} partitions to end", assignment.size());
  }
}
