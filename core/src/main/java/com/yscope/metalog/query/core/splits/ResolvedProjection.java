package com.yscope.metalog.query.core.splits;

import java.util.Set;

/**
 * The set of columns to include in a split query result.
 *
 * <p>Each field is either {@code null} (include all columns of that group) or a specific set of
 * physical column names to include. An empty set means no columns from that group.
 */
public record ResolvedProjection(
    Set<String> fileColumns, // null = all; non-null = include exactly these physical names
    Set<String> dimColumns, // null = all; non-null = include exactly these (e.g., dim_f01)
    Set<String> aggColumns) // null = all; non-null = include exactly these (e.g., agg_f01)
{
  /** Sentinel value representing "all columns" — same as omitting projection entirely. */
  public static final ResolvedProjection ALL = new ResolvedProjection(null, null, null);

  /** Returns {@code true} if all three groups are unconstrained (null). */
  public boolean isAll() {
    return fileColumns == null && dimColumns == null && aggColumns == null;
  }
}
