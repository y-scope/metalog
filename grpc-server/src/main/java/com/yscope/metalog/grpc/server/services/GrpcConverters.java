package com.yscope.metalog.grpc.server.services;

import com.yscope.metalog.metastore.model.AggregationType;

/** Shared proto conversion utilities for the gRPC service layer. */
final class GrpcConverters {

  private GrpcConverters() {}

  static com.yscope.metalog.query.api.proto.grpc.AggregationType toProtoAggregationType(
      AggregationType t) {
    return switch (t) {
      case EQ -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_EQ;
      case GTE -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_GTE;
      case GT -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_GT;
      case LTE -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_LTE;
      case LT -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_LT;
      case SUM -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_SUM;
      case AVG -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_AVG;
      case MIN -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_MIN;
      case MAX -> com.yscope.metalog.query.api.proto.grpc.AggregationType.AGGREGATION_TYPE_MAX;
    };
  }
}
