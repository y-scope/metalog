package com.yscope.clp.service.metastore.model;

/**
 * Aggregation type for pre-computed aggregate columns.
 *
 * <p>Comparison types ({@code EQ}, {@code GTE}, {@code GT}, {@code LTE}, {@code LT}) are used for
 * integer count/threshold aggregations (e.g. {@code agg_int/gte/level/error}). {@code SUM},
 * {@code AVG}, {@code MIN}, {@code MAX} are used for float aggregations (e.g. {@code
 * agg_float/sum/cpu/usage}).
 */
public enum AggregationType {
  EQ,
  GTE,
  GT,
  LTE,
  LT,
  SUM,
  AVG,
  MIN,
  MAX
}
