package com.yscope.clp.service.tools.benchmark;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.coordinator.grpc.proto.AggEntry;
import com.yscope.clp.service.coordinator.grpc.proto.AggType;
import com.yscope.clp.service.coordinator.grpc.proto.ArchiveFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.DimEntry;
import com.yscope.clp.service.coordinator.grpc.proto.DimensionValue;
import com.yscope.clp.service.coordinator.grpc.proto.FileFields;
import com.yscope.clp.service.coordinator.grpc.proto.IngestRequest;
import com.yscope.clp.service.coordinator.grpc.proto.IngestResponse;
import com.yscope.clp.service.coordinator.grpc.proto.IrFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataIngestionServiceGrpc;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataRecord;
import com.yscope.clp.service.coordinator.grpc.proto.SelfDescribingEntry;
import com.yscope.clp.service.coordinator.grpc.proto.StringDimension;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark producer that sends synthetic metadata records via gRPC {@code Ingest} RPC.
 *
 * <p>Uses the async stub with a configurable concurrency limit (semaphore) so many requests are
 * in-flight simultaneously. This exercises the coordinator's {@code BatchingWriter} queue and DB
 * batching — records from concurrent in-flight requests queue up together and are written in bulk.
 *
 * <p>Usage:
 *
 * <pre>
 * java -cp metalog.jar com.yscope.clp.service.tools.benchmark.GrpcBenchmarkProducer \
 *   --records 10000 \
 *   --concurrency 5000 \
 *   --apps 10 \
 *   --mode structured
 * </pre>
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code GRPC_HOST} — coordinator host (default: {@code localhost})
 *   <li>{@code GRPC_PORT} — coordinator gRPC port (default: {@code 9091})
 *   <li>{@code TABLE_NAME} — target metadata table (default: {@code clp_table})
 * </ul>
 */
public class GrpcBenchmarkProducer {

  /** Wire format used to encode dimensions and aggregations in each record. */
  public enum Mode {
    /** Structured {@code DimEntry} / {@code AggEntry} proto fields. */
    STRUCTURED,
    /** {@code SelfDescribingEntry} with {@code /}-separated key convention. */
    COMPAT
  }

  private static final Logger logger = LoggerFactory.getLogger(GrpcBenchmarkProducer.class);

  private final ManagedChannel channel;
  private final MetadataIngestionServiceGrpc.MetadataIngestionServiceStub asyncStub;
  private final String tableName;

  public GrpcBenchmarkProducer(String host, int port, String tableName) {
    this.tableName = tableName;
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.asyncStub = MetadataIngestionServiceGrpc.newStub(channel);
    logger.info(
        "GrpcBenchmarkProducer initialized: host={}, port={}, table={}", host, port, tableName);
  }

  /**
   * Send synthetic records to the coordinator via gRPC with bounded concurrency.
   *
   * @param totalRecords total records to send
   * @param numApps number of distinct application IDs to simulate
   * @param concurrency max number of RPCs in flight at once
   * @param mode wire format for dim/agg encoding
   * @return statistics about the run
   */
  public BenchmarkStats produce(int totalRecords, int numApps, int concurrency, Mode mode)
      throws InterruptedException {
    logger.info(
        "Starting gRPC benchmark: records={}, apps={}, concurrency={}, mode={}",
        totalRecords,
        numApps,
        concurrency,
        mode);

    long startTime = System.currentTimeMillis();
    long baseTimestamp = Timestamps.nowNanos();
    int logInterval = Math.max(totalRecords / 10, 1000);

    AtomicLong totalAccepted = new AtomicLong(0);
    AtomicLong totalRejected = new AtomicLong(0);
    AtomicLong totalErrors = new AtomicLong(0);

    Semaphore inFlight = new Semaphore(concurrency);
    CountDownLatch done = new CountDownLatch(totalRecords);

    for (int i = 0; i < totalRecords; i++) {
      inFlight.acquire();

      IngestRequest request =
          IngestRequest.newBuilder()
              .setTableName(tableName)
              .setRecord(buildRecord(i, numApps, baseTimestamp, mode))
              .build();

      final int recordIndex = i;
      asyncStub.ingest(
          request,
          new StreamObserver<IngestResponse>() {
            @Override
            public void onNext(IngestResponse response) {
              if (response.getAccepted()) {
                totalAccepted.incrementAndGet();
              } else {
                totalRejected.incrementAndGet();
              }
            }

            @Override
            public void onError(Throwable t) {
              if (totalErrors.incrementAndGet() == 1) {
                logger.warn("gRPC ingest call failed (further errors counted only)", t);
              }
              inFlight.release();
              done.countDown();
            }

            @Override
            public void onCompleted() {
              inFlight.release();
              done.countDown();
            }
          });

      int sent = i + 1;
      if (sent % logInterval == 0 || sent == totalRecords) {
        logger.info(
            "Progress: {}/{} records sent ({}%)",
            sent, totalRecords, String.format("%.1f", sent * 100.0 / totalRecords));
      }
    }

    done.await(120, TimeUnit.SECONDS);

    long durationMs = System.currentTimeMillis() - startTime;
    double throughput = durationMs > 0 ? totalRecords * 1000.0 / durationMs : 0;
    BenchmarkStats stats =
        new BenchmarkStats(
            totalRecords,
            totalAccepted.get(),
            totalRejected.get(),
            totalErrors.get(),
            durationMs,
            throughput);
    logger.info("gRPC benchmark complete: {}", stats);
    return stats;
  }

  private MetadataRecord buildRecord(int idx, int numApps, long baseTimestamp, Mode mode) {
    String appId = "benchmark-app-" + (idx % numApps);
    String executorId = "executor-" + (idx % 10);
    String host = "host-" + (idx % 5) + ".example.com";
    String irPath = String.format("s3://clp-ir/%s/%s/file-%d.clp.zst", appId, executorId, idx);

    long minTs = baseTimestamp - Timestamps.NANOS_PER_DAY - idx * Timestamps.NANOS_PER_SECOND;
    long maxTs = minTs + 300 * Timestamps.NANOS_PER_SECOND;

    int recordCount = 1000 + (int) (Math.random() * 9000);
    int countFatal = (int) (Math.random() * 5);
    int countError = countFatal + (int) (Math.random() * 50);
    int countWarn = countError + (int) (Math.random() * 200);
    int countInfo = countWarn + (int) (Math.random() * 500);
    int countDebug = countInfo + (int) (Math.random() * (recordCount - countInfo));
    recordCount = Math.max(recordCount, countDebug);

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

    if (mode == Mode.STRUCTURED) {
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

    return builder.build();
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

  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Statistics from a benchmark run. */
  public static class BenchmarkStats {
    public final int totalRecords;
    public final long accepted;
    public final long rejected;
    public final long errors;
    public final long durationMs;
    public final double throughput;

    public BenchmarkStats(
        int totalRecords,
        long accepted,
        long rejected,
        long errors,
        long durationMs,
        double throughput) {
      this.totalRecords = totalRecords;
      this.accepted = accepted;
      this.rejected = rejected;
      this.errors = errors;
      this.durationMs = durationMs;
      this.throughput = throughput;
    }

    @Override
    public String toString() {
      return String.format(
          "GrpcBenchmarkStats{records=%d, accepted=%d, rejected=%d, errors=%d,"
              + " duration=%dms, throughput=%.0f rec/sec}",
          totalRecords, accepted, rejected, errors, durationMs, throughput);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    String host = System.getenv().getOrDefault("GRPC_HOST", "localhost");
    int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "9091"));
    String table = System.getenv().getOrDefault("TABLE_NAME", "clp_table");
    int records = 10000;
    int apps = 10;
    int concurrency = 5000;
    Mode mode = Mode.STRUCTURED;

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
        case "--concurrency":
        case "-c":
          concurrency = Integer.parseInt(args[++i]);
          break;
        case "--mode":
        case "-m":
          mode = Mode.valueOf(args[++i].toUpperCase());
          break;
        case "--help":
        case "-h":
          System.out.println("Usage: GrpcBenchmarkProducer [options]");
          System.out.println("  --records, -r <num>         Records to send (default: 10000)");
          System.out.println("  --apps, -a <num>            Distinct app IDs (default: 10)");
          System.out.println("  --concurrency, -c <num>     Max concurrent RPCs (default: 5000)");
          System.out.println("  --mode, -m structured|compat  Wire format (default: structured)");
          System.out.println("Environment variables:");
          System.out.println("  GRPC_HOST    Target host (default: localhost)");
          System.out.println("  GRPC_PORT    Target port (default: 9091)");
          System.out.println("  TABLE_NAME   Metadata table (default: clp_table)");
          System.exit(0);
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          System.exit(1);
      }
    }

    GrpcBenchmarkProducer producer = new GrpcBenchmarkProducer(host, port, table);
    try {
      BenchmarkStats stats = producer.produce(records, apps, concurrency, mode);

      logger.info("=== GRPC BENCHMARK RESULTS ===");
      logger.info("Records requested : {}", stats.totalRecords);
      logger.info("Records accepted  : {}", stats.accepted);
      logger.info("Records rejected  : {}", stats.rejected);
      logger.info("RPC errors        : {}", stats.errors);
      logger.info("Duration          : {} ms", stats.durationMs);
      logger.info("Throughput        : {} rec/s", String.format("%.0f", stats.throughput));
      logger.info("==============================");
    } finally {
      producer.close();
    }
  }
}
