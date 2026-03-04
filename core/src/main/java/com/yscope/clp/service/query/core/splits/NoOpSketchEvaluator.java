package com.yscope.clp.service.query.core.splits;

import java.util.Set;

/**
 * Stub {@link SketchEvaluator} that never prunes any row.
 *
 * <p>Used until real sketch deserialization is implemented. Returning {@code true} unconditionally
 * preserves correctness — the query engine always confirms results, so no false negatives are
 * introduced.
 */
public class NoOpSketchEvaluator implements SketchEvaluator {

  @Override
  public boolean maybeContains(
      String field, Set<String> values, String sketchesSet, byte[] extData) {
    return true;
  }
}
