package com.yscope.metalog.grpc.server.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.metalog.metastore.model.AggValueType;
import com.yscope.metalog.metastore.model.AggregationType;
import com.yscope.metalog.query.api.proto.grpc.ListAggsRequest;
import com.yscope.metalog.query.api.proto.grpc.ListAggsResponse;
import com.yscope.metalog.query.api.proto.grpc.ListDimensionsRequest;
import com.yscope.metalog.query.api.proto.grpc.ListDimensionsResponse;
import com.yscope.metalog.query.api.proto.grpc.ListSketchesRequest;
import com.yscope.metalog.query.api.proto.grpc.ListSketchesResponse;
import com.yscope.metalog.query.api.proto.grpc.ListTablesRequest;
import com.yscope.metalog.query.api.proto.grpc.ListTablesResponse;
import com.yscope.metalog.query.core.QueryService;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@DisplayName("MetadataGrpcService")
class MetadataGrpcServiceTest {

  private QueryService queryService;
  private MetadataGrpcService service;

  @BeforeEach
  void setUp() {
    queryService = mock(QueryService.class);
    service = new MetadataGrpcService(queryService);
  }

  // ==========================================
  // listTables
  // ==========================================

  @Nested
  @DisplayName("listTables")
  class ListTables {

    @Test
    void returnsTableNames() {
      when(queryService.listTables()).thenReturn(List.of("clp_logs", "clp_metrics"));

      @SuppressWarnings("unchecked")
      StreamObserver<ListTablesResponse> observer = mock(StreamObserver.class);
      service.listTables(ListTablesRequest.getDefaultInstance(), observer);

      ArgumentCaptor<ListTablesResponse> captor = ArgumentCaptor.forClass(ListTablesResponse.class);
      verify(observer).onNext(captor.capture());
      verify(observer).onCompleted();
      verify(observer, never()).onError(any());

      assertEquals(List.of("clp_logs", "clp_metrics"), captor.getValue().getTablesList());
    }

    @Test
    void emptyList() {
      when(queryService.listTables()).thenReturn(List.of());

      @SuppressWarnings("unchecked")
      StreamObserver<ListTablesResponse> observer = mock(StreamObserver.class);
      service.listTables(ListTablesRequest.getDefaultInstance(), observer);

      ArgumentCaptor<ListTablesResponse> captor = ArgumentCaptor.forClass(ListTablesResponse.class);
      verify(observer).onNext(captor.capture());
      verify(observer).onCompleted();

      assertTrue(captor.getValue().getTablesList().isEmpty());
    }

    @Test
    void queryServiceThrows_sendsError() {
      when(queryService.listTables()).thenThrow(new RuntimeException("DB connection failed"));

      @SuppressWarnings("unchecked")
      StreamObserver<ListTablesResponse> observer = mock(StreamObserver.class);
      service.listTables(ListTablesRequest.getDefaultInstance(), observer);

      verify(observer, never()).onNext(any());
      verify(observer, never()).onCompleted();
      verify(observer).onError(any(StatusRuntimeException.class));
    }
  }

  // ==========================================
  // listDimensions
  // ==========================================

  @Nested
  @DisplayName("listDimensions")
  class ListDimensions {

    @Test
    void returnsDimensionDetails() {
      when(queryService.listDimensionDetails("clp_logs"))
          .thenReturn(
              List.of(
                  new QueryService.DimensionDetail("service", "str", 128, "dim_f01"),
                  new QueryService.DimensionDetail("zone", "str", 64, null)));

      @SuppressWarnings("unchecked")
      StreamObserver<ListDimensionsResponse> observer = mock(StreamObserver.class);
      var request = ListDimensionsRequest.newBuilder().setTable("clp_logs").build();
      service.listDimensions(request, observer);

      ArgumentCaptor<ListDimensionsResponse> captor =
          ArgumentCaptor.forClass(ListDimensionsResponse.class);
      verify(observer).onNext(captor.capture());
      verify(observer).onCompleted();

      var dims = captor.getValue().getDimensionsList();
      assertEquals(2, dims.size());
      assertEquals("service", dims.get(0).getName());
      assertEquals("str", dims.get(0).getType());
      assertEquals(128, dims.get(0).getWidth());
    }

  }

  // ==========================================
  // listAggs
  // ==========================================

  @Nested
  @DisplayName("listAggs")
  class ListAggs {

    @Test
    void returnsAggDetails() {
      when(queryService.listAggDetails("clp_logs"))
          .thenReturn(
              List.of(
                  new QueryService.AggDetail(
                      "level", "error", AggregationType.GTE, AggValueType.INT, "agg_f01")));

      @SuppressWarnings("unchecked")
      StreamObserver<ListAggsResponse> observer = mock(StreamObserver.class);
      var request = ListAggsRequest.newBuilder().setTable("clp_logs").build();
      service.listAggs(request, observer);

      ArgumentCaptor<ListAggsResponse> captor = ArgumentCaptor.forClass(ListAggsResponse.class);
      verify(observer).onNext(captor.capture());
      verify(observer).onCompleted();

      var aggs = captor.getValue().getAggsList();
      assertEquals(1, aggs.size());
      assertEquals("level", aggs.get(0).getName());
      assertEquals("error", aggs.get(0).getValue());
      assertEquals(
          com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_GTE,
          aggs.get(0).getAggregationType());
      assertEquals(
          com.yscope.metalog.query.api.proto.grpc.AggValueType.AGG_VALUE_TYPE_INT,
          aggs.get(0).getValueType());
    }
  }

  // ==========================================
  // listSketches
  // ==========================================

  @Nested
  @DisplayName("listSketches")
  class ListSketches {

    @Test
    void returnsSketchDetails() {
      when(queryService.listSketches("clp_logs"))
          .thenReturn(List.of(new QueryService.SketchDetail("service_sketch")));

      @SuppressWarnings("unchecked")
      StreamObserver<ListSketchesResponse> observer = mock(StreamObserver.class);
      var request = ListSketchesRequest.newBuilder().setTable("clp_logs").build();
      service.listSketches(request, observer);

      ArgumentCaptor<ListSketchesResponse> captor =
          ArgumentCaptor.forClass(ListSketchesResponse.class);
      verify(observer).onNext(captor.capture());
      verify(observer).onCompleted();

      assertEquals(1, captor.getValue().getSketchesList().size());
      assertEquals("service_sketch", captor.getValue().getSketches(0).getName());
    }
  }
}
