package com.yscope.clp.service.coordinator.ingestion.server.grpc;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.yscope.clp.service.coordinator.grpc.proto.FileFields;
import com.yscope.clp.service.coordinator.grpc.proto.IngestRequest;
import com.yscope.clp.service.coordinator.grpc.proto.IngestResponse;
import com.yscope.clp.service.coordinator.grpc.proto.IrFileInfo;
import com.yscope.clp.service.coordinator.grpc.proto.MetadataRecord;
import com.yscope.clp.service.coordinator.ingestion.BatchingWriter;
import com.yscope.clp.service.coordinator.ingestion.IngestionService;
import com.yscope.clp.service.coordinator.ingestion.IngestionService.IngestionResult;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@DisplayName("IngestionGrpcService")
class IngestionGrpcServiceTest {

  private IngestionService ingestionService;
  private IngestionGrpcService grpcService;

  @BeforeEach
  void setUp() {
    ingestionService = mock(IngestionService.class);
    grpcService = new IngestionGrpcService(ingestionService);
  }

  private IngestRequest buildValidRequest(String tableName) {
    return IngestRequest.newBuilder()
        .setTableName(tableName)
        .setRecord(
            MetadataRecord.newBuilder()
                .setFile(
                    FileFields.newBuilder()
                        .setState("IR_ARCHIVE_BUFFERING")
                        .setMinTimestamp(1000)
                        .setMaxTimestamp(2000)
                        .setIr(IrFileInfo.newBuilder().setClpIrPath("test/path.clp"))))
        .build();
  }

  // ==========================================
  // Success Path
  // ==========================================

  @Nested
  @DisplayName("Success path")
  class SuccessPath {

    @Test
    void success_returnsAccepted() {
      var result = new IngestionResult(1, 0, Map.of(), 1);
      when(ingestionService.ingest(eq("clp_logs"), anyList()))
          .thenReturn(CompletableFuture.completedFuture(result));

      @SuppressWarnings("unchecked")
      StreamObserver<IngestResponse> observer = mock(StreamObserver.class);
      grpcService.ingest(buildValidRequest("clp_logs"), observer);

      ArgumentCaptor<IngestResponse> captor = ArgumentCaptor.forClass(IngestResponse.class);
      verify(observer, timeout(2000)).onNext(captor.capture());
      verify(observer, timeout(2000)).onCompleted();

      org.junit.jupiter.api.Assertions.assertTrue(captor.getValue().getAccepted());
    }
  }

  // ==========================================
  // Error Handling
  // ==========================================

  @Nested
  @DisplayName("Error handling")
  class ErrorHandling {

    @Test
    void queueFull_resourceExhausted() {
      CompletableFuture<IngestionResult> future = new CompletableFuture<>();
      future.completeExceptionally(new BatchingWriter.QueueFullException("Queue full"));
      when(ingestionService.ingest(eq("clp_logs"), anyList())).thenReturn(future);

      @SuppressWarnings("unchecked")
      StreamObserver<IngestResponse> observer = mock(StreamObserver.class);
      grpcService.ingest(buildValidRequest("clp_logs"), observer);

      verify(observer, timeout(2000)).onError(any(StatusRuntimeException.class));
      verify(observer, never()).onNext(any());
    }

    @Test
    void timeout_deadlineExceeded() {
      CompletableFuture<IngestionResult> future = new CompletableFuture<>();
      future.completeExceptionally(new TimeoutException("timed out"));
      when(ingestionService.ingest(eq("clp_logs"), anyList())).thenReturn(future);

      @SuppressWarnings("unchecked")
      StreamObserver<IngestResponse> observer = mock(StreamObserver.class);
      grpcService.ingest(buildValidRequest("clp_logs"), observer);

      verify(observer, timeout(2000)).onError(any(StatusRuntimeException.class));
    }

    @Test
    void unexpectedError_internalStatus() {
      CompletableFuture<IngestionResult> future = new CompletableFuture<>();
      future.completeExceptionally(new RuntimeException("unexpected error"));
      when(ingestionService.ingest(eq("clp_logs"), anyList())).thenReturn(future);

      @SuppressWarnings("unchecked")
      StreamObserver<IngestResponse> observer = mock(StreamObserver.class);
      grpcService.ingest(buildValidRequest("clp_logs"), observer);

      verify(observer, timeout(2000)).onError(any(StatusRuntimeException.class));
    }

    @Test
    void invalidTableName_invalidArgument() {
      when(ingestionService.ingest(eq(""), anyList()))
          .thenThrow(new IllegalArgumentException("Table name cannot be empty"));

      @SuppressWarnings("unchecked")
      StreamObserver<IngestResponse> observer = mock(StreamObserver.class);

      grpcService.ingest(buildValidRequest(""), observer);

      verify(observer).onError(any(StatusRuntimeException.class));
    }
  }

}
