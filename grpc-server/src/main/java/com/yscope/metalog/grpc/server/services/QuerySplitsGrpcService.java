package com.yscope.metalog.grpc.server.services;

import com.yscope.metalog.metastore.model.AggValueType;
import com.yscope.metalog.query.api.ApiServerConfig;
import com.yscope.metalog.query.api.proto.grpc.AggEntry;
import com.yscope.metalog.query.api.proto.grpc.CursorValue;
import com.yscope.metalog.query.api.proto.grpc.QuerySplitsServiceGrpc;
import com.yscope.metalog.query.api.proto.grpc.QueryStats;
import com.yscope.metalog.query.api.proto.grpc.StreamSplitsRequest;
import com.yscope.metalog.query.api.proto.grpc.StreamSplitsResponse;
import com.yscope.metalog.query.core.splits.KeysetCursor;
import com.yscope.metalog.query.core.splits.Order;
import com.yscope.metalog.query.core.splits.OrderBySpec;
import com.yscope.metalog.query.core.splits.PreparedFilter;
import com.yscope.metalog.query.core.splits.ResolvedProjection;
import com.yscope.metalog.query.core.splits.SketchExtractionResult;
import com.yscope.metalog.query.core.splits.SketchPredicate;
import com.yscope.metalog.query.core.splits.Split;
import com.yscope.metalog.query.core.splits.SplitQueryEngine;
import com.yscope.metalog.query.core.splits.SplitQueryEngine.StreamingResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for split queries.
 *
 * <p>Thin API layer that handles proto conversion and gRPC streaming mechanics. The heavy lifting
 * (pagination, prefetching, backpressure) is done by {@link SplitQueryEngine#streamSplitsAsync}.
 */
public class QuerySplitsGrpcService extends QuerySplitsServiceGrpc.QuerySplitsServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(QuerySplitsGrpcService.class);

  private final SplitQueryEngine queryEngine;
  private final long streamIdleTimeoutMs;
  private final int dbPageSize;

  public QuerySplitsGrpcService(
      SplitQueryEngine queryEngine,
      ApiServerConfig.TimeoutConfig timeoutConfig,
      ApiServerConfig.StreamingConfig streamingConfig) {
    this.queryEngine = queryEngine;
    this.streamIdleTimeoutMs = timeoutConfig.streamIdleTimeoutMs();
    this.dbPageSize = streamingConfig.dbPageSize();
  }

  /**
   * Stream split results using async pagination from core.
   *
   * <p>This method handles proto conversion and gRPC backpressure. The business logic (pagination,
   * prefetching) is in {@link SplitQueryEngine#streamSplitsAsync}.
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
      // Prepare query parameters (proto → domain objects)
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

      long clientLimit = request.getLimit(); // 0 = unlimited

      LOG.debug(
          "Starting split stream: table={}, clientLimit={}, orderBy={}, cursor={}, "
              + "stateFilter={}, dbPageSize={}",
          request.getTable(),
          clientLimit == 0 ? "unlimited" : clientLimit,
          orderBy,
          initialCursor != null ? "(present)" : "(none)",
          request.getStateFilterList(),
          dbPageSize);

      // Resolve, validate, and translate the filter expression
      SketchExtractionResult sketchResult =
          queryEngine.prepareSketchPredicates(request.getFilterExpression());
      PreparedFilter filterCondition =
          queryEngine.prepareFilterCondition(request.getTable(), sketchResult.cleanedExpression());
      List<SketchPredicate> sketchPredicates = sketchResult.predicates();
      ResolvedProjection projection =
          queryEngine.prepareProjection(request.getTable(), request.getProjectionList());

      // Stream splits using core business logic
      StreamingResult result =
          queryEngine.streamSplitsAsync(
              request.getTable(),
              request.getStateFilterList(),
              filterCondition,
              orderBy,
              initialCursor,
              sketchPredicates,
              projection,
              dbPageSize,
              clientLimit,
              split -> {
                // Wait for client to be ready (backpressure)
                if (!waitForReady(serverObserver, cancelled, readyLock, effectiveIdleTimeoutMs)) {
                  clientCancelled.set(true);
                  return false;
                }

                // Convert domain object → proto
                StreamSplitsResponse.Builder responseBuilder =
                    StreamSplitsResponse.newBuilder()
                        .setSplit(convertSplit(split.split()))
                        .setSequence(sequence.incrementAndGet())
                        .setDone(false);

                if (request.getIncludeCursor()) {
                  responseBuilder.setCursor(buildProtoCursor(split.cursor()));
                }

                try {
                  serverObserver.onNext(responseBuilder.build());
                } catch (StatusRuntimeException e) {
                  if (e.getStatus().getCode() == Status.Code.CANCELLED) {
                    LOG.debug("Stream cancelled during onNext");
                    clientCancelled.set(true);
                    cancelled.set(true);
                    return false;
                  }
                  throw e;
                }

                return !cancelled.get();
              },
              cancelled);

      // Send final response with stats
      if (!clientCancelled.get()) {
        StreamSplitsResponse finalResponse =
            StreamSplitsResponse.newBuilder()
                .setSequence(sequence.get())
                .setDone(true)
                .setStats(
                    QueryStats.newBuilder()
                        .setSplitsScanned(result.splitsScanned())
                        .setSplitsMatched(result.splitsMatched())
                        .build())
                .build();
        serverObserver.onNext(finalResponse);
        serverObserver.onCompleted();

        LOG.debug(
            "Split stream completed: {} results, {} scanned, {} ms",
            sequence.get(),
            result.splitsScanned(),
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


  // --- Proto mapping helpers ---

  private static List<OrderBySpec> mapOrderBy(
      List<com.yscope.metalog.query.api.proto.grpc.OrderBy> protoList) {
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

  private static Order mapOrder(com.yscope.metalog.query.api.proto.grpc.Order proto) {
    return switch (proto) {
      case ORDER_ASC -> Order.ASC;
      case ORDER_DESC -> Order.DESC;
      default -> throw new IllegalArgumentException("order must be ORDER_ASC or ORDER_DESC, not UNSPECIFIED");
    };
  }

  private static KeysetCursor mapCursor(
      com.yscope.metalog.query.api.proto.grpc.KeysetCursor proto) {
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

  private static com.yscope.metalog.query.api.proto.grpc.KeysetCursor buildProtoCursor(
      KeysetCursor cursor) {
    var builder =
        com.yscope.metalog.query.api.proto.grpc.KeysetCursor.newBuilder().setId(cursor.id());
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

  private static com.yscope.metalog.query.api.proto.grpc.Split convertSplit(Split split) {
    var builder =
        com.yscope.metalog.query.api.proto.grpc.Split.newBuilder()
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
