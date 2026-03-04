package com.yscope.clp.service.query.core.splits;

/** A column name and sort direction for a single ORDER BY field. */
public record OrderBySpec(String column, Order order) {}
