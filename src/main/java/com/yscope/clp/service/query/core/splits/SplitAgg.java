package com.yscope.clp.service.query.core.splits;

import com.yscope.clp.service.metastore.model.AggValueType;
import com.yscope.clp.service.metastore.model.AggregationType;

/**
 * A pre-computed aggregate value with its registry metadata.
 *
 * @param key field name being aggregated (e.g., "level")
 * @param value value qualifier (e.g., "warn", "error"); null if no qualifier
 * @param aggregationType the aggregation comparison/function type
 * @param valueType physical column value type
 * @param intResult the integer result (valid when valueType == INT)
 * @param floatResult the float result (valid when valueType == FLOAT)
 */
public record SplitAgg(
    String key,
    String value,
    AggregationType aggregationType,
    AggValueType valueType,
    long intResult,
    double floatResult) {}
