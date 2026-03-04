package com.yscope.clp.service.query.api.server.vertx.grpc;

import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.query.api.ApiServerConfig;
import com.yscope.clp.service.query.api.proto.grpc.AggEntry;
import com.yscope.clp.service.query.api.proto.grpc.CursorValue;
import com.yscope.clp.service.query.api.proto.grpc.QuerySplitsServiceGrpc;
import com.yscope.clp.service.query.api.proto.grpc.QueryStats;
import com.yscope.clp.service.query.api.proto.grpc.StreamSplitsRequest;
import com.yscope.clp.service.query.api.proto.grpc.StreamSplitsResponse;
import com.yscope.clp.service.query.core.splits.KeysetCursor;
import com.yscope.clp.service.query.core.splits.Order;
import com.yscope.clp.service.query.core.splits.OrderBySpec;
import com.yscope.clp.service.query.core.splits.ResolvedProjection;
import com.yscope.clp.service.query.core.splits.SketchExtractionResult;
import com.yscope.clp.service.query.core.splits.SketchPredicate;
import com.yscope.clp.service.query.core.splits.Split;
import com.yscope.clp.service.query.core.splits.SplitQueryEngine;
import com.yscope.clp.service.query.core.splits.SplitQueryEngine.PageFetchResult;
import com.yscope.clp.service.query.core.splits.SplitWithCursor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jooq.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for split queries.
 *
 * <p>Uses a prefetch queue to overlap DB fetching with gRPC streaming. A background thread fetches
 * pages from the database and pushes rows into a bounded queue. The gRPC thread consumes rows from
 * the queue. This avoids holding DB connections idle during gRPC backpressure and overlaps DB
 * network I/O with client streaming.
 */
public class QuerySplitsGrpcService extends QuerySplitsServiceGrpc.QuerySplitsServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(QuerySplitsGrpcService.class);

  private final SplitQueryEngine queryEngine;
  private final long streamIdleTimeoutMs;
  private final int dbPageSize;
  private final ExecutorService dbFetchExecutor;

  public QuerySplitsGrpcService(
      SplitQueryEngine queryEngine,
      ApiServerConfig.TimeoutConfig timeoutConfig,
      ApiServerConfig.StreamingConfig streamingConfig) {
    this.queryEngine = queryEngine;
    this.streamIdleTimeoutMs = timeoutConfig.streamIdleTimeoutMs();
    this.dbPageSize = streamingConfig.dbPageSize();
    this.dbFetchExecutor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "db-prefetch");
              t.setDaemon(true);
              return t;
            });
  }

  /** Shuts down the background DB fetch thread pool. */
  public void close() {
    dbFetchExecutor.shutdownNow();
  }

  /**
   * Stream split results from the database using a prefetch queue.
   *
   * <p>A background thread fetches pages from DB and pushes rows into a bounded queue. The gRPC
   * thread consumes from the queue and streams to the client. DB connections are released after
   * each page fetch.
   */
  @Override
  public void streamSplits(
      StreamSplitsRequest request, StreamObserver<StreamSplitsResponse> responseObserver) {
    ServerCallStreamObserver<StreamSplitsResponse> serverObserver =
        (ServerCallStreamObserver<StreamSplitsResponse>) responseObserver;

    AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicBoolean clientCancelled = new AtomicBoolean(false);
    AtomicInteger sequence = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    Object readyLock = new Object();

    // Handle client cancellation - signal readyLock to unblock any waiting thread
    serverObserver.setOnCancelHandler(
        () -> {
          LOG.debug("Client cancelled Split stream after {} results", sequence.get());
          clientCancelled.set(true);
          cancelled.set(true);
          synchronized (readyLock) {
            readyLock.notifyAll();
          }
        });

    // Handle backpressure - signal readyLock when client is ready for more data
    serverObserver.setOnReadyHandler(
        () -> {
          synchronized (readyLock) {
            readyLock.notifyAll();
          }
        });

    try {
      List<OrderBySpec> orderBy =
          queryEngine.prepareOrderBy(request.getTable(), mapOrderBy(request.getOrderByList()));
      for (OrderBySpec spec : orderBy) {
        if (!SplitQueryEngine.INDEXED_SORT_COLUMNS.contains(spec.column())) {
          if (!request.getAllowUnindexedSort()) {
            throw new IllegalArgumentException(
                "Sort column \""
                    + spec.column()
                    + "\" is not indexed. Use an indexed column (min_timestamp, max_timestamp)"
                    + " or set allow_unindexed_sort=true to allow a full table scan.");
          }
          LOG.warn(
              "Unindexed sort on table {}: column \"{}\" — full table scan required",
              request.getTable(),
              spec.column());
        }
      }
      long effectiveIdleTimeoutMs =
          request.getStreamIdleTimeoutMs() > 0
              ? request.getStreamIdleTimeoutMs()
              : streamIdleTimeoutMs;

      final KeysetCursor initialCursor;
      if (request.hasCursor()) {
        var protoCursor = request.getCursor();
        if (protoCursor.getValuesCount() != orderBy.size()) {
          throw new IllegalArgumentException(
              "cursor.values has "
                  + protoCursor.getValuesCount()
                  + " entries but order_by has "
                  + orderBy.size());
        }
        initialCursor = mapCursor(protoCursor);
      } else {
        initialCursor = null;
      }

      // Client result cap: 0 = unlimited, N > 0 = return at most N results
      long clientLimit = request.getLimit();

      LOG.debug(
          "Starting prefetch stream: table={}, clientLimit={}, orderBy={}, cursor={}, "
              + "stateFilter={}, dbPageSize={}",
          request.getTable(),
          clientLimit == 0 ? "unlimited" : clientLimit,
          orderBy,
          initialCursor != null ? "(present)" : "(none)",
          request.getStateFilterList(),
          dbPageSize);

      // Resolve, validate, and translate the filter expression once before starting the producer.
      // Doing this on the gRPC thread means invalid filters return INVALID_ARGUMENT immediately
      // rather than surfacing as INTERNAL from the background producer thread.
      SketchExtractionResult sketchResult =
          queryEngine.prepareSketchPredicates(request.getFilterExpression());
      Condition filterCondition =
          queryEngine.prepareFilterCondition(request.getTable(), sketchResult.cleanedExpression());
      List<SketchPredicate> sketchPredicates = sketchResult.predicates();
      ResolvedProjection projection =
          queryEngine.prepareProjection(request.getTable(), request.getProjectionList());

      AtomicLong remainingCount = new AtomicLong(clientLimit); // 0 = unlimited
      AtomicLong totalScanned = new AtomicLong(0);
      AtomicBoolean producerDone = new AtomicBoolean(false);
      AtomicReference<Exception> producerError = new AtomicReference<>();

      // Prefetch queue: capacity 2 * dbPageSize allows the producer to stay one page ahead
      ArrayBlockingQueue<SplitWithCursor> splitQueue = new ArrayBlockingQueue<>(2 * dbPageSize);

      // Background DB fetcher: fetches pages and pushes rows into the queue
      Future<?> fetcherFuture =
          dbFetchExecutor.submit(
              () ->
                  runProducer(
                      request.getTable(),
                      request.getStateFilterList(),
                      filterCondition,
                      sketchPredicates,
                      projection,
                      orderBy,
                      initialCursor,
                      remainingCount,
                      cancelled,
                      splitQueue,
                      totalScanned,
                      producerDone,
                      producerError));

      // Foreground gRPC consumer: drain queue and stream to client
      try {
        while (!cancelled.get()) {
          SplitWithCursor item;
          try {
            item = splitQueue.poll(100, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }

          if (item == null) {
            if (producerDone.get() && splitQueue.isEmpty()) {
              break; // producer finished and queue drained
            }
            Exception err = producerError.get();
            if (err != null) {
              throw err;
            }
            continue; // poll timeout, retry
          }

          if (clientLimit > 0 && remainingCount.get() == 0) {
            break;
          }

          if (!waitForReady(serverObserver, cancelled, readyLock, effectiveIdleTimeoutMs)) {
            clientCancelled.set(true);
            break;
          }

          StreamSplitsResponse.Builder responseBuilder =
              StreamSplitsResponse.newBuilder()
                  .setSplit(convertSplit(item.split()))
                  .setSequence(sequence.incrementAndGet())
                  .setDone(false);

          if (request.getIncludeCursor()) {
            responseBuilder.setCursor(buildProtoCursor(item.cursor()));
          }

          try {
            serverObserver.onNext(responseBuilder.build());
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.CANCELLED) {
              LOG.debug("Stream cancelled during onNext");
              clientCancelled.set(true);
              cancelled.set(true);
              break;
            }
            throw e;
          }

          if (remainingCount.get() > 0) {
            remainingCount.decrementAndGet();
          }
        }
      } finally {
        cancelled.set(true);
        fetcherFuture.cancel(true);
      }

      // Check for producer error after consumer loop
      Exception err = producerError.get();
      if (err != null) {
        throw err;
      }

      if (!clientCancelled.get()) {
        StreamSplitsResponse finalResponse =
            StreamSplitsResponse.newBuilder()
                .setSequence(sequence.get())
                .setDone(true)
                .setStats(
                    QueryStats.newBuilder()
                        .setSplitsScanned(totalScanned.get())
                        .setSplitsMatched(sequence.get())
                        .build())
                .build();
        serverObserver.onNext(finalResponse);
        serverObserver.onCompleted();

        LOG.debug(
            "Prefetch stream completed: {} results, {} scanned, {} ms",
            sequence.get(),
            totalScanned.get(),
            System.currentTimeMillis() - startTime);
      }

    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid StreamSplits request", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    } catch (Exception e) {
      LOG.error("Split stream failed", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Query failed: " + e.getMessage()).asRuntimeException());
    }
  }

  /**
   * Background DB fetch producer. Fetches pages using keyset pagination and pushes rows into the
   * queue. Uses blocking {@code put()} for natural backpressure when the queue fills.
   */
  private void runProducer(
      String table,
      List<String> stateFilter,
      Condition filterCondition,
      List<SketchPredicate> sketchPredicates,
      ResolvedProjection projection,
      List<OrderBySpec> orderBy,
      KeysetCursor initialCursor,
      AtomicLong remainingCount,
      AtomicBoolean cancelled,
      ArrayBlockingQueue<SplitWithCursor> splitQueue,
      AtomicLong totalScanned,
      AtomicBoolean producerDone,
      AtomicReference<Exception> producerError) {
    try {
      KeysetCursor cursor = initialCursor;
      while (!cancelled.get()) {
        long remaining = remainingCount.get();
        long pageSize = (remaining > 0) ? Math.min(dbPageSize, remaining) : dbPageSize;

        PageFetchResult result =
            queryEngine.fetchPage(
                table,
                stateFilter,
                filterCondition,
                pageSize,
                orderBy,
                cursor,
                sketchPredicates,
                projection);
        // DB connection released here

        totalScanned.addAndGet(result.dbRowsScanned());

        if (result.splits().isEmpty()) {
          // All rows were sketch-pruned, but the DB may have more pages.
          // Use lastDbCursor to advance past the pruned page.
          if (result.dbRowsScanned() >= pageSize && result.lastDbCursor() != null) {
            cursor = result.lastDbCursor();
            continue;
          }
          break; // DB actually exhausted (fewer rows than page size)
        }

        for (SplitWithCursor item : result.splits()) {
          if (cancelled.get()) {
            break;
          }
          splitQueue.put(item);
        }

        if (result.dbRowsScanned() < pageSize) {
          break; // DB returned fewer rows than requested — data exhausted
        }

        cursor = result.splits().get(result.splits().size() - 1).cursor();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      if (!cancelled.get()) {
        producerError.set(e);
      }
    } finally {
      producerDone.set(true);
    }
  }

  // --- Proto mapping helpers ---

  private static List<OrderBySpec> mapOrderBy(
      List<com.yscope.clp.service.query.api.proto.grpc.OrderBy> protoList) {
    if (protoList == null || protoList.isEmpty()) {
      throw new IllegalArgumentException("order_by is required (must have at least one field)");
    }
    List<OrderBySpec> result = new ArrayList<>(protoList.size());
    for (var f : protoList) {
      if (f.getColumn().isEmpty()) {
        throw new IllegalArgumentException("order_by column must not be empty");
      }
      if (f.getColumn().equals("id")) {
        throw new IllegalArgumentException(
            "\"id\" must not appear in order_by — it is the implicit final tiebreaker");
      }
      result.add(new OrderBySpec(f.getColumn(), mapOrder(f.getOrder())));
    }
    return result;
  }

  private static Order mapOrder(com.yscope.clp.service.query.api.proto.grpc.Order proto) {
    return switch (proto) {
      case ORDER_ASC -> Order.ASC;
      case ORDER_DESC -> Order.DESC;
      default -> throw new IllegalArgumentException("order must be ORDER_ASC or ORDER_DESC, not UNSPECIFIED");
    };
  }

  private static KeysetCursor mapCursor(
      com.yscope.clp.service.query.api.proto.grpc.KeysetCursor proto) {
    List<Object> values = new ArrayList<>(proto.getValuesCount());
    for (var cv : proto.getValuesList()) {
      values.add(
          switch (cv.getValueCase()) {
            case INT_VAL -> cv.getIntVal();
            case FLOAT_VAL -> cv.getFloatVal();
            case STR_VAL -> cv.getStrVal();
            default -> throw new IllegalArgumentException("cursor value has no value set");
          });
    }
    return new KeysetCursor(values, proto.getId());
  }

  private static com.yscope.clp.service.query.api.proto.grpc.KeysetCursor buildProtoCursor(
      KeysetCursor cursor) {
    var builder =
        com.yscope.clp.service.query.api.proto.grpc.KeysetCursor.newBuilder().setId(cursor.id());
    for (Object val : cursor.sortValues()) {
      builder.addValues(toCursorValue(val));
    }
    return builder.build();
  }

  private static CursorValue toCursorValue(Object val) {
    if (val instanceof Long l) {
      return CursorValue.newBuilder().setIntVal(l).build();
    } else if (val instanceof Integer i) {
      // JDBC returns Integer for signed INT columns; upcast to Long for the cursor.
      return CursorValue.newBuilder().setIntVal(i.longValue()).build();
    } else if (val instanceof Double d) {
      return CursorValue.newBuilder().setFloatVal(d).build();
    } else {
      return CursorValue.newBuilder().setStrVal(val != null ? val.toString() : "").build();
    }
  }

  // --- Split conversion helpers ---

  private static com.yscope.clp.service.query.api.proto.grpc.Split convertSplit(Split split) {
    var builder =
        com.yscope.clp.service.query.api.proto.grpc.Split.newBuilder()
            .setId(split.id())
            .setClpIrPath(nullToEmpty(split.objectPath()))
            .setClpIrStorageBackend(nullToEmpty(split.objectStorageBackend()))
            .setClpIrBucket(nullToEmpty(split.objectBucket()))
            .setClpArchivePath(nullToEmpty(split.archivePath()))
            .setClpArchiveStorageBackend(nullToEmpty(split.archiveStorageBackend()))
            .setClpArchiveBucket(nullToEmpty(split.archiveBucket()))
            .setMinTimestamp(split.minTimestamp())
            .setMaxTimestamp(split.maxTimestamp())
            .setState(nullToEmpty(split.state()))
            .setRecordCount(split.recordCount())
            .setSizeBytes(split.sizeBytes());

    if (split.dimensions() != null) {
      builder.putAllDimensions(split.dimensions());
    }
    if (split.aggs() != null) {
      for (var a : split.aggs()) {
        var aggBuilder =
            AggEntry.newBuilder()
                .setKey(a.key())
                .setValue(a.value() != null ? a.value() : "")
                .setAggregationType(GrpcConverters.toProtoAggregationType(a.aggregationType()));
        if (a.valueType() == AggValueType.FLOAT) {
          aggBuilder.setFloatValue(a.floatResult());
        } else {
          aggBuilder.setIntValue(a.intResult());
        }
        builder.addAggs(aggBuilder.build());
      }
    }

    return builder.build();
  }

  private static String nullToEmpty(String value) {
    return value != null ? value : "";
  }

  /**
   * Block until the client's receive buffer has drained (gRPC flow control ready), or until
   * cancelled/timed out.
   *
   * <p>A slow-but-steady consumer never triggers the timeout — it only fires if the client's buffer
   * remains continuously full for {@code timeoutMs} milliseconds (frozen/crashed client).
   *
   * @return true if ready to receive, false if cancelled or client appears stuck
   */
  private boolean waitForReady(
      ServerCallStreamObserver<?> observer,
      AtomicBoolean cancelled,
      Object readyLock,
      long timeoutMs) {
    if (observer.isReady()) {
      return true;
    }

    synchronized (readyLock) {
      long deadline = System.currentTimeMillis() + timeoutMs;
      while (!observer.isReady() && !cancelled.get()) {
        long remainingMs = deadline - System.currentTimeMillis();
        if (remainingMs <= 0) {
          LOG.warn(
              "Client receive buffer has been full for {} ms — client appears stuck, aborting stream",
              timeoutMs);
          cancelled.set(true);
          return false;
        }
        try {
          readyLock.wait(remainingMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    return !cancelled.get();
  }
}
