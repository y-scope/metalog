package com.yscope.clp.service.query.core.splits;

import java.util.Set;

/**
 * A predicate to evaluate against a probabilistic sketch stored in the metadata table.
 *
 * <p>{@code field} is the logical sketch field name (e.g., {@code "user_id"}). {@code values} is
 * the set of string values to test membership for — the sketch must be checked for any of them.
 */
public record SketchPredicate(String field, Set<String> values) {}
