package com.yscope.clp.service.query.core.splits;

import java.util.Set;

/**
 * Evaluates probabilistic sketch predicates against a metadata row's sketch data.
 *
 * <p>Returns {@code true} (keep the row) when the sketch indicates the file might contain any of
 * the requested values — including when no sketch is available. Returns {@code false} only when the
 * sketch definitively proves none of the values are present (prune the row).
 *
 * <p>Sketch filtering is best-effort pruning only; false positives are expected. The query engine
 * always confirms survivors for exact results.
 */
public interface SketchEvaluator {

  /**
   * Test whether a file row might contain any of {@code values} for the given sketch {@code field}.
   *
   * @param field logical sketch field name (e.g., {@code "user_id"})
   * @param values set of string values to test for membership
   * @param sketchesSet value of the {@code sketches} column (which sketches are present), or null
   * @param extData raw bytes of the {@code ext} MEDIUMBLOB column, or null
   * @return {@code true} to keep the row, {@code false} to prune it
   */
  boolean maybeContains(String field, Set<String> values, String sketchesSet, byte[] extData);
}
