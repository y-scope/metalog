package com.yscope.metalog.query.api.server.vertx.grpc;

import com.yscope.metalog.query.api.proto.grpc.AggInfo;
import com.yscope.metalog.query.api.proto.grpc.DimensionInfo;
import com.yscope.metalog.query.api.proto.grpc.ListAggsRequest;
import com.yscope.metalog.query.api.proto.grpc.ListAggsResponse;
import com.yscope.metalog.query.api.proto.grpc.ListDimensionsRequest;
import com.yscope.metalog.query.api.proto.grpc.ListDimensionsResponse;
import com.yscope.metalog.query.api.proto.grpc.ListSketchesRequest;
import com.yscope.metalog.query.api.proto.grpc.ListSketchesResponse;
import com.yscope.metalog.query.api.proto.grpc.ListTablesRequest;
import com.yscope.metalog.query.api.proto.grpc.ListTablesResponse;
import com.yscope.metalog.query.api.proto.grpc.MetadataServiceGrpc;
import com.yscope.metalog.query.api.proto.grpc.SketchInfo;
import com.yscope.metalog.query.core.QueryService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service for schema introspection.
 *
 * <p>All responses use resolved logical names — internal placeholder column names are never
 * exposed.
 */
public class MetadataGrpcService extends MetadataServiceGrpc.MetadataServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataGrpcService.class);

  private final QueryService queryService;

  public MetadataGrpcService(QueryService queryService) {
    this.queryService = queryService;
  }

  @Override
  public void listTables(
      ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
    try {
      var tables = queryService.listTables();
      responseObserver.onNext(ListTablesResponse.newBuilder().addAllTables(tables).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("ListTables failed", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Failed to list tables").asRuntimeException());
    }
  }

  @Override
  public void listDimensions(
      ListDimensionsRequest request, StreamObserver<ListDimensionsResponse> responseObserver) {
    try {
      var details = queryService.listDimensionDetails(request.getTable());
      var builder = ListDimensionsResponse.newBuilder();
      for (var detail : details) {
        builder.addDimensions(
            DimensionInfo.newBuilder()
                .setName(detail.name())
                .setType(detail.type())
                .setWidth(detail.width())
                .setAliasColumn(detail.aliasColumn() != null ? detail.aliasColumn() : "")
                .build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("ListDimensions failed for table={}", request.getTable(), e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Failed to list dimensions").asRuntimeException());
    }
  }

  @Override
  public void listAggs(ListAggsRequest request, StreamObserver<ListAggsResponse> responseObserver) {
    try {
      var details = queryService.listAggDetails(request.getTable());
      var builder = ListAggsResponse.newBuilder();
      for (var detail : details) {
        builder.addAggs(
            AggInfo.newBuilder()
                .setName(detail.name())
                .setValue(detail.value() != null ? detail.value() : "")
                .setAggregationType(GrpcConverters.toProtoAggregationType(detail.aggregationType()))
                .setValueType(toProtoAggValueType(detail.valueType()))
                .setAliasColumn(detail.aliasColumn() != null ? detail.aliasColumn() : "")
                .build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("ListAggs failed for table={}", request.getTable(), e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Failed to list aggs").asRuntimeException());
    }
  }

  private static com.yscope.metalog.query.api.proto.grpc.AggValueType toProtoAggValueType(
      com.yscope.metalog.metastore.model.AggValueType t) {
    return switch (t) {
      case INT -> com.yscope.metalog.query.api.proto.grpc.AggValueType.AGG_VALUE_TYPE_INT;
      case FLOAT -> com.yscope.metalog.query.api.proto.grpc.AggValueType.AGG_VALUE_TYPE_FLOAT;
    };
  }

  @Override
  public void listSketches(
      ListSketchesRequest request, StreamObserver<ListSketchesResponse> responseObserver) {
    try {
      var details = queryService.listSketches(request.getTable());
      var builder = ListSketchesResponse.newBuilder();
      for (var detail : details) {
        builder.addSketches(SketchInfo.newBuilder().setName(detail.name()).build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("ListSketches failed for table={}", request.getTable(), e);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Failed to list sketches").asRuntimeException());
    }
  }
}
