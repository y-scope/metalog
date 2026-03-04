package com.yscope.clp.service.query.api.server.vertx.grpc;

import com.yscope.clp.service.metastore.model.AggregationType;

/** Shared proto conversion utilities for the gRPC service layer. */
final class GrpcConverters {

  private GrpcConverters() {}

  static com.yscope.clp.service.query.api.proto.grpc.AggregationType toProtoAggregationType(
      AggregationType t) {
    return switch (t) {
      case EQ -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_EQ;
      case GTE -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_GTE;
      case GT -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_GT;
      case LTE -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_LTE;
      case LT -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_LT;
      case SUM -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_SUM;
      case AVG -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_AVG;
      case MIN -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_MIN;
      case MAX -> com.yscope.clp.service.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_MAX;
    };
  }
}
