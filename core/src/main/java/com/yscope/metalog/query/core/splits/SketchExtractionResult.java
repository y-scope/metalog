package com.yscope.metalog.query.core.splits;

import java.util.List;

/**
 * Result of extracting {@code __SKETCH.*} predicates from a filter expression.
 *
 * <p>{@code cleanedExpression} is the original expression with all {@code __SKETCH.*} terms
 * removed; null or empty if the entire expression consisted of sketch predicates. {@code
 * predicates} is the list of extracted sketch predicates.
 */
public record SketchExtractionResult(String cleanedExpression, List<SketchPredicate> predicates) {}
