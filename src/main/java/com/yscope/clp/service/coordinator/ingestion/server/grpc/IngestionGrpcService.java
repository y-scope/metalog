package com.yscope.clp.service.coordinator.ingestion.server.grpc;

import com.yscope.clp.service.coordinator.grpc.proto.IngestRequest;
import com.yscope.clp.service.coordinator.grpc.proto.IngestResponse;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataIngestionServiceGrpc;
import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.yscope.clp.service.coordinator.ingestion.IngestionService;
import com.yscope.clp.service.coordinator.ingestion.IngestionService.IngestionResult;
import com.yscope.clp.service.coordinator.ingestion.ProtoConverter;
import com.yscope.clp.service.metastore.model.FileRecord;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC adapter for metadata ingestion.
 *
 * <p>Converts proto {@link com.yscope.clp.service.coordinator.grpc.proto.MetadataRecord} to domain
 * {@link FileRecord}, delegates to {@link IngestionService} for validation and batching, and maps
 * the result back to a proto response.
 */
public class IngestionGrpcService
    extends MetadataIngestionServiceGrpc.MetadataIngestionServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(IngestionGrpcService.class);
  private static final long WRITE_TIMEOUT_SECONDS = 10;

  private final IngestionService ingestionService;

  public IngestionGrpcService(IngestionService ingestionService) {
    this.ingestionService = ingestionService;
  }

  @Override
  public void ingest(IngestRequest request, StreamObserver<IngestResponse> responseObserver) {
    String tableName = request.getTableName();

    FileRecord file;
    try {
      file = ProtoConverter.toFileRecord(request.getRecord());
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      return;
    }

    try {
      ingestionService
          .ingest(tableName, List.of(file))
          .orTimeout(WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  handleError(tableName, error, responseObserver);
                } else {
                  respondWith(responseObserver, result);
                }
              });
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  private void handleError(
      String tableName, Throwable error, StreamObserver<IngestResponse> responseObserver) {
    // Unwrap CompletionException to get the real cause (CompletableFuture wraps exceptions)
    Throwable cause = error;
    while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof BatchingWriter.QueueFullException) {
      logger.warn("Write queue full for table {}", tableName);
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED.withDescription(cause.getMessage()).asRuntimeException());
    } else if (cause instanceof TimeoutException) {
      logger.error("Write timed out for table {} after {}s", tableName, WRITE_TIMEOUT_SECONDS);
      responseObserver.onError(
          Status.DEADLINE_EXCEEDED
              .withDescription("Write timed out after " + WRITE_TIMEOUT_SECONDS + "s")
              .asRuntimeException());
    } else {
      logger.error("Ingest write failed for table {}", tableName, cause);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Database write failed: " + cause.getMessage())
              .withCause(cause)
              .asRuntimeException());
    }
  }

  private static void respondWith(
      StreamObserver<IngestResponse> responseObserver, IngestionResult result) {
    boolean accepted = result.recordsAccepted() > 0;
    String error =
        accepted
            ? ""
            : result.recordErrors().values().stream().findFirst().orElse("validation failed");
    responseObserver.onNext(
        IngestResponse.newBuilder().setAccepted(accepted).setError(error).build());
    responseObserver.onCompleted();
  }
}
