package com.yscope.clp.service.coordinator.ingestion;

import com.yscope.clp.service.common.config.db.DSLContextFactory;
import com.yscope.clp.service.metastore.FileRecords;
import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.schema.ColumnRegistry;
import com.yscope.clp.service.metastore.schema.SchemaEvolution;
import com.yscope.clp.service.metastore.schema.TableProvisioner;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer that routes submissions to per-table writer threads.
 *
 * <p>Each table gets its own dedicated background thread with its own queue, so writes to different
 * tables never block each other. Within a table, writes are batched using a poll+drainTo strategy.
 *
 * <p>Each {@link #submit} call returns a {@link CompletableFuture} that completes after the DB
 * commit. Callers use the future to trigger downstream actions (e.g., gRPC ACK, Kafka offset
 * commit).
 *
 * <p>Node creates separate instances for Kafka and gRPC traffic so they never contend.
 */
public class BatchingWriter {
  private static final Logger logger = LoggerFactory.getLogger(BatchingWriter.class);

  private static final long DEFAULT_SUBMIT_TIMEOUT_MS = 60_000;

  private final ConcurrentHashMap<String, TableWriter> tableWriters = new ConcurrentHashMap<>();
  private final HikariDataSource dataSource;
  private final DSLContext dsl;
  private final int queueSlots;
  private final int maxBatchSize;
  private final long flushTimeoutMs;
  private final long submitTimeoutMs;
  private volatile boolean running = true;

  /**
   * @param dataSource shared connection pool
   * @param maxQueuedSubmissions maximum number of pending submit() calls to buffer per table;
   *     excess submissions are rejected with {@link QueueFullException}
   * @param maxRecordsPerBatch target DB batch size; the consumer drains up to this many records per
   *     write (e.g., 500 for Kafka polled batches, 1000 for gRPC single-record submissions)
   * @param flushTimeoutMs maximum time in ms to wait before flushing a partial batch
   */
  public BatchingWriter(
      HikariDataSource dataSource,
      int maxQueuedSubmissions,
      int maxRecordsPerBatch,
      long flushTimeoutMs) {
    this(
        dataSource,
        maxQueuedSubmissions,
        maxRecordsPerBatch,
        flushTimeoutMs,
        DEFAULT_SUBMIT_TIMEOUT_MS);
  }

  /**
   * @param dataSource shared connection pool
   * @param maxQueuedSubmissions maximum number of pending submit() calls to buffer per table;
   *     excess submissions are rejected with {@link QueueFullException}
   * @param maxRecordsPerBatch target DB batch size; the consumer drains up to this many records per
   *     write (e.g., 500 for Kafka polled batches, 1000 for gRPC single-record submissions)
   * @param flushTimeoutMs maximum time in ms to wait before flushing a partial batch
   * @param submitTimeoutMs maximum time in ms for blocking submit to wait for queue space
   */
  public BatchingWriter(
      HikariDataSource dataSource,
      int maxQueuedSubmissions,
      int maxRecordsPerBatch,
      long flushTimeoutMs,
      long submitTimeoutMs) {
    this.dataSource = dataSource;
    this.dsl = DSLContextFactory.create(dataSource);
    this.queueSlots = maxQueuedSubmissions;
    this.maxBatchSize = maxRecordsPerBatch;
    this.flushTimeoutMs = flushTimeoutMs;
    this.submitTimeoutMs = submitTimeoutMs;
  }

  /**
   * Submit a batch of records for writing (non-blocking). Returns a failed future with {@link
   * QueueFullException} if the queue is full. Use this for gRPC where immediate backpressure is
   * preferred.
   *
   * @param tableName the target metadata table
   * @param files the records to write
   * @return a future that completes after DB commit with the write result
   */
  public CompletableFuture<WriteResult> submit(String tableName, List<FileRecord> files) {
    if (files.isEmpty()) {
      return CompletableFuture.completedFuture(new WriteResult(0, 0));
    }
    if (!running) {
      return CompletableFuture.failedFuture(new IllegalStateException("BatchingWriter stopped"));
    }
    return getOrCreateWriter(tableName).submit(files);
  }

  /**
   * Submit a batch of records for writing (blocking). Blocks until queue has space. Use this for
   * Kafka where records must not be dropped — blocking naturally throttles the poller.
   *
   * @param tableName the target metadata table
   * @param files the records to write
   * @return a future that completes after DB commit with the write result
   * @throws InterruptedException if the thread is interrupted while waiting for queue space
   */
  public CompletableFuture<WriteResult> submitBlocking(String tableName, List<FileRecord> files)
      throws InterruptedException {
    if (files.isEmpty()) {
      return CompletableFuture.completedFuture(new WriteResult(0, 0));
    }
    if (!running) {
      return CompletableFuture.failedFuture(new IllegalStateException("BatchingWriter stopped"));
    }
    return getOrCreateWriter(tableName).submitBlocking(files);
  }

  private TableWriter getOrCreateWriter(String tableName) {
    // Fast path: avoid lambda allocation for existing writers.
    TableWriter existing = tableWriters.get(tableName);
    if (existing != null) {
      return existing;
    }
    if (!running) {
      throw new IllegalStateException("BatchingWriter stopped");
    }
    // computeIfAbsent is key-level atomic — only one thread executes the lambda per key.
    // Provisioning and writer creation happen as a single unit, eliminating the race where
    // a thread could create a writer before another thread's ensureTable() completes.
    return tableWriters.computeIfAbsent(
        tableName,
        name -> {
          if (!running) {
            throw new IllegalStateException("BatchingWriter stopped");
          }
          try {
            TableProvisioner.ensureTable(dataSource, name);
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            logger.warn("Auto-provision failed for table {}, writes may fail", name, e);
          }
          TableWriter tw =
              new TableWriter(
                  name, dataSource, dsl, queueSlots, maxBatchSize, flushTimeoutMs,
                  submitTimeoutMs);
          Thread t = new Thread(tw, "table-writer-" + name);
          t.setDaemon(true);
          t.start();
          logger.info(
              "Started writer thread for table {} (queueSlots={}, maxBatchSize={}, "
                  + "flushTimeoutMs={}, submitTimeoutMs={})",
              name,
              queueSlots,
              maxBatchSize,
              flushTimeoutMs,
              submitTimeoutMs);
          return tw;
        });
  }

  /** Stop all per-table writer threads and drain remaining submissions. */
  public void stop() {
    running = false;
    for (TableWriter tw : tableWriters.values()) {
      tw.stop();
    }
  }

  /**
   * Wait for all per-table writer threads to finish.
   *
   * @param timeoutMs maximum time to wait per thread
   */
  public void join(long timeoutMs) {
    for (TableWriter tw : tableWriters.values()) {
      tw.join(timeoutMs);
    }
  }

  /** Check if the writer is accepting submissions. */
  public boolean isRunning() {
    return running;
  }

  /** Result of a batch write operation. */
  public record WriteResult(int accepted, int rowsAffected) {}

  /** Thrown when the per-table write queue is full (backpressure signal). */
  public static class QueueFullException extends RuntimeException {
    public QueueFullException(String message) {
      super(message);
    }
  }

  // ========================================================================
  // Per-table writer thread
  // ========================================================================

  /**
   * Dedicated writer thread for a single table. Has its own queue so writes to this table are
   * independent of other tables.
   */
  static class TableWriter implements Runnable {
    private final String tableName;
    private final BlockingQueue<PendingBatch> queue;
    private final FileRecords repository;
    private final SchemaEvolution schemaEvolution;
    private final ColumnRegistry columnRegistry;
    private final int maxBatchSize;
    private final long flushTimeoutMs;
    private final long submitTimeoutMs;
    private volatile boolean running = true;
    private volatile Thread thread;

    TableWriter(
        String tableName,
        HikariDataSource dataSource,
        DSLContext dsl,
        int queueSlots,
        int maxBatchSize,
        long flushTimeoutMs,
        long submitTimeoutMs) {
      this.queue = new ArrayBlockingQueue<>(queueSlots);
      this.tableName = tableName;
      this.maxBatchSize = maxBatchSize;
      this.flushTimeoutMs = flushTimeoutMs;
      this.submitTimeoutMs = submitTimeoutMs;
      this.repository = new FileRecords(dataSource, dsl, tableName);

      ColumnRegistry cr = null;
      try {
        cr = new ColumnRegistry(dsl, tableName);
      } catch (Exception e) {
        logger.warn("Column registry unavailable for table {}", tableName, e);
      }
      this.columnRegistry = cr;

      SchemaEvolution se = null;
      try {
        se = new SchemaEvolution(dsl, tableName, true, cr);
      } catch (Exception e) {
        logger.warn("Schema evolution disabled for table {}", tableName, e);
      }
      this.schemaEvolution = se;
    }

    /** Non-blocking submit. Returns failed future if queue is full. */
    CompletableFuture<WriteResult> submit(List<FileRecord> files) {
      CompletableFuture<WriteResult> future = new CompletableFuture<>();
      if (!queue.offer(new PendingBatch(files, future))) {
        future.completeExceptionally(
            new QueueFullException("Write queue full for table " + tableName));
      }
      return future;
    }

    /** Blocking submit. Waits until queue has space, up to the configured submit timeout. */
    CompletableFuture<WriteResult> submitBlocking(List<FileRecord> files)
        throws InterruptedException {
      CompletableFuture<WriteResult> future = new CompletableFuture<>();
      if (!queue.offer(new PendingBatch(files, future), submitTimeoutMs, TimeUnit.MILLISECONDS)) {
        future.completeExceptionally(
            new QueueFullException(
                "Write queue full for table "
                    + tableName
                    + " (timed out after "
                    + submitTimeoutMs
                    + "ms)"));
      }
      return future;
    }

    @Override
    public void run() {
      thread = Thread.currentThread();
      logger.info("TableWriter[{}] started", tableName);

      while (running) {
        try {
          // Block until the first record arrives — no idle timeout needed.
          PendingBatch first = queue.take();

          // Accumulate additional records for up to flushTimeoutMs after the first arrives,
          // or until maxBatchSize records are collected — whichever comes first.
          List<PendingBatch> batch = new ArrayList<>();
          batch.add(first);
          int recordCount = first.files.size();
          long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(flushTimeoutMs);
          while (recordCount < maxBatchSize) {
            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) {
              break;
            }
            PendingBatch next = queue.poll(remainingNs, TimeUnit.NANOSECONDS);
            if (next == null) {
              break;
            }
            batch.add(next);
            recordCount += next.files.size();
          }

          List<FileRecord> allFiles = new ArrayList<>();
          for (PendingBatch pb : batch) {
            allFiles.addAll(pb.files);
          }

          try {
            int rows = upsertWithEvolution(allFiles);
            logger.debug(
                "TableWriter[{}]: wrote {} files ({} rows)", tableName, allFiles.size(), rows);
            for (PendingBatch pb : batch) {
              int batchRows =
                  allFiles.size() > 0
                      ? (int) Math.round((double) rows * pb.files.size() / allFiles.size())
                      : 0;
              pb.future.complete(new WriteResult(pb.files.size(), batchRows));
            }
          } catch (Exception e) {
            logger.error("TableWriter[{}]: failed to write batch", tableName, e);
            for (PendingBatch pb : batch) {
              pb.future.completeExceptionally(e);
            }
          }
        } catch (InterruptedException e) {
          logger.info("TableWriter[{}] interrupted, shutting down", tableName);
          Thread.currentThread().interrupt();
          break;
        } catch (RuntimeException e) {
          logger.error("Error in TableWriter[{}]", tableName, e);
        }
      }

      // Drain remaining
      List<PendingBatch> remaining = new ArrayList<>();
      queue.drainTo(remaining);
      for (PendingBatch pb : remaining) {
        pb.future.completeExceptionally(new IllegalStateException("TableWriter stopped"));
      }

      logger.info("TableWriter[{}] stopped", tableName);
    }

    void stop() {
      running = false;
      Thread t = thread;
      if (t != null) {
        t.interrupt();
      }
    }

    void join(long timeoutMs) {
      Thread t = thread;
      if (t != null && t.isAlive()) {
        try {
          t.join(timeoutMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private int upsertWithEvolution(List<FileRecord> files) throws SQLException {
      if (schemaEvolution != null && schemaEvolution.isEnabled() && columnRegistry != null) {
        return upsertWithRegistry(files);
      }
      return repository.upsertBatch(files);
    }

    private int upsertWithRegistry(List<FileRecord> files) throws SQLException {
      // Collect dim field specs and original-key-to-dimKey mapping
      Map<String, ColumnRegistry.DimSpec> dimSpecs = new LinkedHashMap<>();
      Map<String, String> originalDimKeyToDimKey = new LinkedHashMap<>();
      collectDimFieldSpecs(files, dimSpecs, originalDimKeyToDimKey);

      // Collect agg field specs and original-key-to-compositeKey mapping
      Map<String, ColumnRegistry.AggSpec> aggSpecs = new LinkedHashMap<>();
      Map<String, String> originalAggKeyToCompositeKey = new LinkedHashMap<>();
      collectAggFieldSpecs(files, aggSpecs, originalAggKeyToCompositeKey);

      // Resolve dimKey -> placeholder column via registry
      Map<String, String> dimKeyToColumn = schemaEvolution.evolveSchemaWithRegistry(dimSpecs);

      // Resolve compositeKey -> placeholder column via registry
      Map<String, String> aggKeyToColumn =
          aggSpecs.isEmpty() ? Map.of() : schemaEvolution.evolveAggsWithRegistry(aggSpecs);

      // Build originalKey -> placeholder column for dim SQL translation
      Map<String, String> dimFieldToColumn = new LinkedHashMap<>();
      for (Map.Entry<String, String> e : originalDimKeyToDimKey.entrySet()) {
        String column = dimKeyToColumn.get(e.getValue());
        if (column != null) {
          dimFieldToColumn.put(e.getKey(), column);
        }
      }

      // Build originalKey -> placeholder column for agg SQL translation
      Map<String, String> aggFieldToColumn = new LinkedHashMap<>();
      for (Map.Entry<String, String> e : originalAggKeyToCompositeKey.entrySet()) {
        String column = aggKeyToColumn.get(e.getValue());
        if (column != null) {
          aggFieldToColumn.put(e.getKey(), column);
        }
      }

      // Build ordered placeholder column lists
      List<String> dimPlaceholders = new ArrayList<>(dimFieldToColumn.values());
      dimPlaceholders.sort(String::compareTo);
      List<String> aggPlaceholders = new ArrayList<>(aggFieldToColumn.values());
      aggPlaceholders.sort(String::compareTo);

      // Identify which agg placeholder columns hold float values
      Set<String> floatAggPlaceholders = new HashSet<>();
      for (Map.Entry<String, String> e : originalAggKeyToCompositeKey.entrySet()) {
        String column = aggKeyToColumn.get(e.getValue());
        ColumnRegistry.AggSpec spec = aggSpecs.get(e.getValue());
        if (column != null && spec != null && spec.valueType() == AggValueType.FLOAT) {
          floatAggPlaceholders.add(column);
        }
      }

      return repository.upsertBatchWithTranslation(
          files,
          dimPlaceholders,
          dimFieldToColumn,
          aggPlaceholders,
          aggFieldToColumn,
          floatAggPlaceholders);
    }

    /**
     * Collect dimension field specs from a batch of files.
     *
     * <p>Handles two key formats:
     *
     * <ul>
     *   <li>Composite keys ({@code fieldName\0baseType\0width}, e.g. {@code
     *       application_id\0str\064}) produced by {@link ProtoConverter} from structured {@code
     *       DimEntry}. The NUL separator makes these unambiguous and bypasses all other matching.
     *   <li>Self-describing keys ({@code dim/typeSpec/fieldName}, e.g. {@code dim/str64/service},
     *       {@code dim/bool/is_cloud}) from gRPC compat and Kafka producers.
     * </ul>
     *
     * @param files the batch of IR files
     * @param dimSpecs output: fieldName -> DimSpec for registry resolution
     * @param originalKeyToDimKey output: original FileRecord key -> registry fieldName
     */
    private static void collectDimFieldSpecs(
        List<FileRecord> files,
        Map<String, ColumnRegistry.DimSpec> dimSpecs,
        Map<String, String> originalKeyToDimKey) {
      for (FileRecord file : files) {
        if (file.getDimensions() == null) {
          continue;
        }
        for (Map.Entry<String, Object> entry : file.getDimensions().entrySet()) {
          String originalKey = entry.getKey();
          if (originalKeyToDimKey.containsKey(originalKey)) {
            continue;
          }

          // Composite format: fieldName\0baseType\0width (from ProtoConverter structured path)
          if (originalKey.indexOf('\0') >= 0) {
            String[] parts = originalKey.split("\0", 3);
            String fieldName = parts[0];
            String baseType = parts.length > 1 ? parts[1] : "str";
            int width = parts.length > 2 && !parts[2].isEmpty() ? Integer.parseInt(parts[2]) : 0;
            originalKeyToDimKey.put(originalKey, fieldName);
            dimSpecs.putIfAbsent(fieldName, new ColumnRegistry.DimSpec(baseType, width));
            continue;
          }

          // Self-describing format: dim/typeSpec/fieldName
          if (originalKey.startsWith("dim/")) {
            String[] parts = originalKey.split("/", 3);
            if (parts.length == 3) {
              String typeSpec = parts[1];
              String fieldName = parts[2];
              String baseType = parseLegacyBaseType(typeSpec);
              int width = parseLegacyWidth(typeSpec);
              originalKeyToDimKey.put(originalKey, fieldName);
              dimSpecs.putIfAbsent(fieldName, new ColumnRegistry.DimSpec(baseType, width));
            } else {
              logger.warn(
                  "Dropping dimension '{}': expected dim/typeSpec/fieldName format", originalKey);
            }
            continue;
          }

          logger.warn(
              "Dropping dimension '{}': key must use dim/typeSpec/fieldName format"
                  + " (e.g. dim/str64/field, dim/int/field, dim/bool/field)",
              originalKey);
        }
      }
    }

    /**
     * Collect aggregate field specs from a batch of files.
     *
     * <p>Handles two key formats from {@link FileRecord#getCounts()} and {@link
     * FileRecord#getFloatCounts()}:
     *
     * <ul>
     *   <li>Composite keys ({@code AGGTYPE\0field\0qualifier}) produced by {@link ProtoConverter}
     *       from structured {@code AggEntry} and {@code SelfDescribingEntry} agg fields. The NUL
     *       separator makes these unambiguous and bypasses pattern matching.
     *   <li>Self-describing keys ({@code agg_int/aggtype/field[/qualifier]} and {@code
     *       agg_float/aggtype/field[/qualifier]}) from gRPC compat and Kafka producers.
     * </ul>
     *
     * @param files the batch of IR files
     * @param aggSpecs output: compositeKey -> AggSpec for registry resolution
     * @param originalKeyToCompositeKey output: original FileRecord key -> registry composite key
     */
    private static void collectAggFieldSpecs(
        List<FileRecord> files,
        Map<String, ColumnRegistry.AggSpec> aggSpecs,
        Map<String, String> originalKeyToCompositeKey) {
      for (FileRecord file : files) {
        // INT agg keys from counts map
        if (!file.getCounts().isEmpty()) {
          for (Map.Entry<String, Long> entry : file.getCounts().entrySet()) {
            String originalKey = entry.getKey();
            if (originalKeyToCompositeKey.containsKey(originalKey)) {
              continue;
            }

            // Composite format: AGGTYPE\0field\0qualifier[\0aliasCol] (from ProtoConverter)
            if (originalKey.indexOf('\0') >= 0) {
              originalKeyToCompositeKey.put(originalKey, originalKey);
              String[] parts = originalKey.split("\0", 4);
              AggregationType aggType =
                  parts.length > 0 ? AggregationType.valueOf(parts[0]) : AggregationType.EQ;
              String field = parts.length > 1 ? parts[1] : "";
              String qualifier = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
              String aliasColumn = parts.length > 3 && !parts[3].isEmpty() ? parts[3] : null;
              aggSpecs.putIfAbsent(
                  originalKey,
                  new ColumnRegistry.AggSpec(field, qualifier, aggType, AggValueType.INT, aliasColumn));
              continue;
            }

            // Self-describing format: agg_int/aggtype/field[/qualifier]
            if (originalKey.startsWith("agg_int/")) {
              String[] parts = originalKey.split("/", 4);
              if (parts.length >= 3) {
                String typePart = parts[1];
                String field = parts[2];
                String qualifier = parts.length == 4 ? parts[3] : null;
                try {
                  AggregationType aggType = AggregationType.valueOf(typePart.toUpperCase());
                  String compositeKey =
                      aggType.name() + "\0" + field + "\0" + (qualifier != null ? qualifier : "");
                  originalKeyToCompositeKey.put(originalKey, compositeKey);
                  aggSpecs.putIfAbsent(
                      compositeKey,
                      new ColumnRegistry.AggSpec(field, qualifier, aggType, AggValueType.INT));
                } catch (IllegalArgumentException e) {
                  logger.warn("Dropping agg '{}': unknown agg_type '{}'", originalKey, typePart);
                }
              }
              continue;
            }

            logger.warn(
                "Dropping int agg '{}': key must use agg_int/aggtype/field[/qualifier]"
                    + " format (e.g. agg_int/gte/level/debug, agg_int/eq/record_count)",
                originalKey);
          }
        }

        // FLOAT agg keys from floatCounts map
        if (!file.getFloatCounts().isEmpty()) {
          for (String originalKey : file.getFloatCounts().keySet()) {
            if (originalKeyToCompositeKey.containsKey(originalKey)) {
              continue;
            }

            // Composite format: AGGTYPE\0field\0qualifier[\0aliasCol] (from ProtoConverter)
            if (originalKey.indexOf('\0') >= 0) {
              originalKeyToCompositeKey.put(originalKey, originalKey);
              String[] parts = originalKey.split("\0", 4);
              AggregationType aggType =
                  parts.length > 0 ? AggregationType.valueOf(parts[0]) : AggregationType.EQ;
              String field = parts.length > 1 ? parts[1] : "";
              String qualifier = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
              String aliasColumn = parts.length > 3 && !parts[3].isEmpty() ? parts[3] : null;
              aggSpecs.putIfAbsent(
                  originalKey,
                  new ColumnRegistry.AggSpec(field, qualifier, aggType, AggValueType.FLOAT, aliasColumn));
              continue;
            }

            // Self-describing format: agg_float/aggtype/field[/qualifier]
            if (originalKey.startsWith("agg_float/")) {
              String[] parts = originalKey.split("/", 4);
              if (parts.length >= 3) {
                String typePart = parts[1];
                String field = parts[2];
                String qualifier = parts.length == 4 ? parts[3] : null;
                try {
                  AggregationType aggType = AggregationType.valueOf(typePart.toUpperCase());
                  String compositeKey =
                      aggType.name() + "\0" + field + "\0" + (qualifier != null ? qualifier : "");
                  originalKeyToCompositeKey.put(originalKey, compositeKey);
                  aggSpecs.putIfAbsent(
                      compositeKey,
                      new ColumnRegistry.AggSpec(field, qualifier, aggType, AggValueType.FLOAT));
                } catch (IllegalArgumentException e) {
                  logger.warn("Dropping agg '{}': unknown agg_type '{}'", originalKey, typePart);
                }
              }
              continue;
            }

            logger.warn(
                "Dropping float agg '{}': key must use agg_float/aggtype/field[/qualifier]"
                    + " format (e.g. agg_float/avg/latency, agg_float/avg/latency/p99)",
                originalKey);
          }
        }
      }
    }

    private static final Pattern LEGACY_WIDTH_PATTERN = Pattern.compile("^str(\\d+)(?:utf8)?$");
    private static final Pattern LEGACY_STR_UTF8_PATTERN = Pattern.compile("^str\\d+utf8$");
    private static final Pattern LEGACY_STR_PATTERN = Pattern.compile("^str\\d+$");

    /**
     * Parse a type spec (e.g., "str128", "str64utf8", "bool", "int", "uint", "float") into a
     * registry base type.
     */
    private static String parseLegacyBaseType(String typeSpec) {
      if (LEGACY_STR_UTF8_PATTERN.matcher(typeSpec).matches()) {
        return "str_utf8";
      } else if (LEGACY_STR_PATTERN.matcher(typeSpec).matches()) {
        return "str";
      } else if (typeSpec.equals("bool")) {
        return "bool";
      } else if (typeSpec.equals("float")) {
        return "float";
      } else if (typeSpec.equals("int") || typeSpec.equals("uint")) {
        return "int";
      } else {
        return "int";
      }
    }

    /** Parse a legacy type spec width. Only string types have a width; others return 0. */
    private static int parseLegacyWidth(String typeSpec) {
      Matcher m = LEGACY_WIDTH_PATTERN.matcher(typeSpec);
      if (m.matches()) {
        return Integer.parseInt(m.group(1));
      }
      return 0;
    }

    record PendingBatch(List<FileRecord> files, CompletableFuture<WriteResult> future) {}
  }
}
