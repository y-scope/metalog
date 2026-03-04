package com.yscope.metalog.query.core.splits;

import org.jooq.Condition;

/**
 * Opaque wrapper around a pre-built jOOQ {@link Condition} so that the jOOQ type does not appear in
 * the public API of {@link SplitQueryEngine}.
 *
 * <p>Instances are created by {@link SplitQueryEngine#prepareFilterCondition} and passed back into
 * {@link SplitQueryEngine#fetchPage} or {@link SplitQueryEngine#streamPage}. The wrapped condition
 * is package-private — downstream consumers cannot access it.
 */
public final class PreparedFilter {
  final Condition condition;

  PreparedFilter(Condition condition) {
    this.condition = condition;
  }
}
