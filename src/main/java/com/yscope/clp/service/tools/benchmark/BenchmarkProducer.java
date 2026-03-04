package com.yscope.clp.service.tools.benchmark;

import com.yscope.clp.service.common.Timestamps;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yscope.clp.service.coordinator.grpc.proto.AggEntry;
import com.yscope.clp.service.coordinator.grpc.proto.AggType;
import com.yscope.clp.service.coordinator.grpc.proto.ArchiveFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.DimEntry;
import com.yscope.clp.service.coordinator.grpc.proto.DimensionValue;
import com.yscope.clp.service.coordinator.grpc.proto.FileFields;
import com.yscope.clp.service.coordinator.grpc.proto.IrFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataRecord;
import com.yscope.clp.service.coordinator.grpc.proto.SelfDescribingEntry;
import com.yscope.clp.service.coordinator.grpc.proto.StringDimension;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark producer that generates configurable number of metadata records to Kafka.
 *
 * <p>Usage:
 *
 * <pre>
 * java -cp coordinator.jar com.yscope.consolidation.benchmark.BenchmarkProducer \
 *   --records 10000 \
 *   --batch-size 100 \
 *   --apps 10 \
 *   --mode proto-structured
 * </pre>
 *
 * <p>Environment variables: - KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092) - KAFKA_TOPIC
 * (default: clp-metadata)
 */
public class BenchmarkProducer {

  /** Wire format used to encode each Kafka message. */
  public enum Mode {
    /** JSON payload — keys use {@code /} self-describing format (e.g. {@code dim/str128/app}). */
    JSON,
    /**
     * Serialized {@link MetadataRecord} proto with structured {@code DimEntry}/{@code AggEntry}.
     */
    PROTO_STRUCTURED,
    /** Serialized {@link MetadataRecord} proto with {@code SelfDescribingEntry} {@code /} keys. */
    PROTO_COMPAT
  }

  private static final Logger logger = LoggerFactory.getLogger(BenchmarkProducer.class);

  private final KafkaProducer<String, byte[]> producer;
  private final ObjectMapper objectMapper;
  private final String topic;
  private final AtomicLong sentCount = new AtomicLong(0);
  private final AtomicLong ackedCount = new AtomicLong(0);

  public BenchmarkProducer(String bootstrapServers, String topic) {
    this.topic = topic;
    this.objectMapper = new ObjectMapper();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1"); // Wait for leader ack
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16KB batches
    props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Wait up to 5ms for batching
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer

    this.producer = new KafkaProducer<>(props);
    logger.info(
        "BenchmarkProducer initialized: bootstrapServers={}, topic={}", bootstrapServers, topic);
  }

  /**
   * Generate and send records to Kafka.
   *
   * @param totalRecords Total number of records to send
   * @param numApps Number of distinct application IDs to use
   * @param batchSize Records to send before logging progress
   * @param mode wire format for each message payload
   * @return Statistics about the run
   */
  public BenchmarkStats produce(int totalRecords, int numApps, int batchSize, Mode mode) {
    logger.info(
        "Starting benchmark: records={}, apps={}, batchSize={}, mode={}",
        totalRecords,
        numApps,
        batchSize,
        mode);

    long startTime = System.currentTimeMillis();
    long baseTimestamp = Timestamps.nowNanos();
    int logInterval = Math.max(totalRecords / 10, 50000);
    AtomicLong sendErrors = new AtomicLong(0);

    for (int i = 0; i < totalRecords; i++) {
      try {
        String appId = "benchmark-app-" + (i % numApps);
        String executorId = "executor-" + (i % 10);
        String host = "host-" + (i % 5) + ".example.com";
        String irPath = String.format("s3://clp-ir/%s/%s/file-%d.clp.zst", appId, executorId, i);

        long minTs = baseTimestamp - Timestamps.NANOS_PER_DAY - i * Timestamps.NANOS_PER_SECOND;
        long maxTs = minTs + 300 * Timestamps.NANOS_PER_SECOND;

        byte[] payload = buildPayload(appId, executorId, host, irPath, minTs, maxTs, mode);

        producer.send(
            new ProducerRecord<>(topic, appId, payload),
            (metadata, exception) -> {
              if (exception != null) {
                if (sendErrors.getAndIncrement() == 0) {
                  logger.error("Kafka send failed (further failures counted only)", exception);
                }
              } else {
                ackedCount.incrementAndGet();
              }
            });

        sentCount.incrementAndGet();

        if ((i + 1) % logInterval == 0) {
          double percent = (i + 1) * 100.0 / totalRecords;
          logger.info(
              "Progress: {}/{} records sent ({}%)",
              i + 1, totalRecords, String.format("%.1f", percent));
        }

      } catch (Exception e) {
        logger.error("Error producing record {}", i, e);
      }
    }

    // Flush and wait for all acks
    producer.flush();
    long endTime = System.currentTimeMillis();

    // Wait for all acks (with timeout)
    long waitStart = System.currentTimeMillis();
    while (ackedCount.get() < totalRecords && System.currentTimeMillis() - waitStart < 30000) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    long duration = endTime - startTime;
    double throughput = (totalRecords * 1000.0) / duration;

    BenchmarkStats stats =
        new BenchmarkStats(totalRecords, sentCount.get(), ackedCount.get(), duration, throughput);

    if (sendErrors.get() > 0) {
      logger.error("Kafka send errors: {} records failed to send", sendErrors.get());
    }
    logger.info("Benchmark complete: {}", stats);
    return stats;
  }

  private byte[] buildPayload(
      String appId,
      String executorId,
      String host,
      String irPath,
      long minTs,
      long maxTs,
      Mode mode)
      throws Exception {
    int recordCount = 1000 + (int) (Math.random() * 9000);
    int countFatal = (int) (Math.random() * 5);
    int countError = countFatal + (int) (Math.random() * 50);
    int countWarn = countError + (int) (Math.random() * 200);
    int countInfo = countWarn + (int) (Math.random() * 500);
    int countDebug = countInfo + (int) (Math.random() * (recordCount - countInfo));
    recordCount = Math.max(recordCount, countDebug);

    if (mode == Mode.JSON) {
      ObjectNode message =
          createMetadataMessage(
              irPath,
              appId,
              executorId,
              host,
              minTs,
              maxTs,
              recordCount,
              countDebug,
              countInfo,
              countWarn,
              countError,
              countFatal);
      return objectMapper.writeValueAsBytes(message);
    }

    FileFields fileFields =
        FileFields.newBuilder()
            .setState("IR_ARCHIVE_BUFFERING")
            .setMinTimestamp(minTs)
            .setMaxTimestamp(maxTs)
            .setRawSizeBytes(1024 * 1024 + (long) (Math.random() * 10 * 1024 * 1024))
            .setRecordCount(recordCount)
            .setIr(
                IrFileInfo.newBuilder()
                    .setClpIrStorageBackend("s3")
                    .setClpIrBucket("clp-ir")
                    .setClpIrPath(irPath)
                    .setClpIrSizeBytes(512 * 1024 + (long) (Math.random() * 5 * 1024 * 1024))
                    .build())
            .setArchive(
                ArchiveFileInfo.newBuilder()
                    .setClpArchiveStorageBackend("")
                    .setClpArchiveBucket("")
                    .setClpArchivePath("")
                    .setClpArchiveCreatedAt(0)
                    .setClpArchiveSizeBytes(0)
                    .build())
            .setRetentionDays(30)
            .setExpiresAt(0)
            .build();

    MetadataRecord.Builder builder = MetadataRecord.newBuilder().setFile(fileFields);

    if (mode == Mode.PROTO_STRUCTURED) {
      builder
          .addAgg(aggEntry("level", "debug", AggType.GTE, countDebug))
          .addAgg(aggEntry("level", "info", AggType.GTE, countInfo))
          .addAgg(aggEntry("level", "warn", AggType.GTE, countWarn))
          .addAgg(aggEntry("level", "error", AggType.GTE, countError))
          .addAgg(aggEntry("level", "fatal", AggType.GTE, countFatal))
          .addDim(strDimEntry("application_id", appId, 64))
          .addDim(strDimEntry("executor_id", executorId, 64))
          .addDim(strDimEntry("host", host, 64));
    } else {
      builder
          .addSelfDescribingKv(sdEntry("agg_int/gte/level/debug", String.valueOf(countDebug)))
          .addSelfDescribingKv(sdEntry("agg_int/gte/level/info", String.valueOf(countInfo)))
          .addSelfDescribingKv(sdEntry("agg_int/gte/level/warn", String.valueOf(countWarn)))
          .addSelfDescribingKv(sdEntry("agg_int/gte/level/error", String.valueOf(countError)))
          .addSelfDescribingKv(sdEntry("agg_int/gte/level/fatal", String.valueOf(countFatal)))
          .addSelfDescribingKv(sdEntry("dim/str64/application_id", appId))
          .addSelfDescribingKv(sdEntry("dim/str64/executor_id", executorId))
          .addSelfDescribingKv(sdEntry("dim/str64/host", host));
    }

    return builder.build().toByteArray();
  }

  private ObjectNode createMetadataMessage(
      String irPath,
      String appId,
      String executorId,
      String host,
      long minTimestamp,
      long maxTimestamp,
      int recordCount,
      int countDebug,
      int countInfo,
      int countWarn,
      int countError,
      int countFatal) {
    ObjectNode node = objectMapper.createObjectNode();

    // IR location
    node.put("ir_storage_backend", "s3");
    node.put("ir_bucket", "clp-ir");
    node.put("ir_path", irPath);

    // Archive location (empty for new IR files)
    node.put("archive_storage_backend", "");
    node.put("archive_bucket", "");
    node.put("archive_path", "");

    // State
    node.put("state", "IR_ARCHIVE_BUFFERING");

    // Timestamps
    node.put("min_timestamp", minTimestamp);
    node.put("max_timestamp", maxTimestamp);
    node.put("archive_created_at", 0);

    // Sizes
    node.put("raw_size_bytes", 1024 * 1024 + (int) (Math.random() * 10 * 1024 * 1024));
    node.put("ir_size_bytes", 512 * 1024 + (int) (Math.random() * 5 * 1024 * 1024));

    node.put("record_count", recordCount);

    // Retention
    node.put("retention_days", 30);
    node.put("expires_at", 0);

    ObjectNode dimensions = objectMapper.createObjectNode();
    dimensions.put("dim/str128/application_id", appId);
    dimensions.put("dim/str128/executor_id", executorId);
    dimensions.put("dim/str128/host", host);
    node.set("dimensions", dimensions);

    ObjectNode counts = objectMapper.createObjectNode();
    counts.put("agg_int/gte/level/debug", countDebug);
    counts.put("agg_int/gte/level/info", countInfo);
    counts.put("agg_int/gte/level/warn", countWarn);
    counts.put("agg_int/gte/level/error", countError);
    counts.put("agg_int/gte/level/fatal", countFatal);
    node.set("counts", counts);

    return node;
  }

  private static AggEntry aggEntry(String field, String qualifier, AggType type, long value) {
    return AggEntry.newBuilder()
        .setField(field)
        .setQualifier(qualifier)
        .setAggType(type)
        .setIntVal(value)
        .build();
  }

  private static DimEntry strDimEntry(String key, String value, int maxLength) {
    return DimEntry.newBuilder()
        .setKey(key)
        .setValue(
            DimensionValue.newBuilder()
                .setStr(
                    StringDimension.newBuilder().setValue(value).setMaxLength(maxLength).build())
                .build())
        .build();
  }

  private static SelfDescribingEntry sdEntry(String key, String value) {
    return SelfDescribingEntry.newBuilder().setKey(key).setValue(value).build();
  }

  public void close() {
    producer.close();
  }

  public static class BenchmarkStats {
    public final int totalRecords;
    public final long sentCount;
    public final long ackedCount;
    public final long durationMs;
    public final double throughput;

    public BenchmarkStats(
        int totalRecords, long sentCount, long ackedCount, long durationMs, double throughput) {
      this.totalRecords = totalRecords;
      this.sentCount = sentCount;
      this.ackedCount = ackedCount;
      this.durationMs = durationMs;
      this.throughput = throughput;
    }

    @Override
    public String toString() {
      return String.format(
          "BenchmarkStats{records=%d, sent=%d, acked=%d, duration=%dms, throughput=%.0f rec/sec}",
          totalRecords, sentCount, ackedCount, durationMs, throughput);
    }
  }

  public static void main(String[] args) {
    int records = 10000;
    int apps = 10;
    int batchSize = 1000;
    Mode mode = Mode.JSON;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--records":
        case "-r":
          records = Integer.parseInt(args[++i]);
          break;
        case "--apps":
        case "-a":
          apps = Integer.parseInt(args[++i]);
          break;
        case "--batch-size":
        case "-b":
          batchSize = Integer.parseInt(args[++i]);
          break;
        case "--mode":
        case "-m":
          mode = Mode.valueOf(args[++i].toUpperCase().replace('-', '_'));
          break;
        case "--help":
        case "-h":
          System.out.println("Usage: BenchmarkProducer [options]");
          System.out.println(
              "  --records, -r <num>                    Number of records (default: 10000)");
          System.out.println(
              "  --apps, -a <num>                       Distinct app IDs (default: 10)");
          System.out.println(
              "  --batch-size, -b <num>                 Progress log interval (default: 1000)");
          System.out.println(
              "  --mode, -m json|proto-structured|proto-compat  Wire format (default: json)");
          System.out.println("Environment variables:");
          System.out.println("  KAFKA_BOOTSTRAP_SERVERS  Kafka brokers (default: localhost:9092)");
          System.out.println("  KAFKA_TOPIC              Topic name (default: clp-metadata)");
          System.exit(0);
          break;
        default:
          logger.error("Unknown argument: {}", args[i]);
          System.exit(1);
      }
    }

    String bootstrapServers =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "clp-metadata");

    BenchmarkProducer producer = new BenchmarkProducer(bootstrapServers, topic);
    try {
      BenchmarkStats stats = producer.produce(records, apps, batchSize, mode);

      // Output machine-readable stats
      logger.info("=== PRODUCER BENCHMARK RESULTS ===");
      logger.info("Records produced: {}", stats.totalRecords);
      logger.info("Records acked: {}", stats.ackedCount);
      logger.info("Duration: {} ms", stats.durationMs);
      logger.info("Producer throughput: {} records/sec", String.format("%.0f", stats.throughput));
      logger.info("==================================");

    } finally {
      producer.close();
    }
  }
}
