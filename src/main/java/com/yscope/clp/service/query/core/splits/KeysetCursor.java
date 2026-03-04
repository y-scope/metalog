package com.yscope.clp.service.query.core.splits;

import java.util.List;

/**
 * Keyset pagination cursor.
 *
 * <p>Tracks the position of a row in the sort order so the next query can resume from there.
 *
 * @param sortValues Values of the ORDER BY fields for this row, in the same order as the {@code
 *     order_by} list. Each value is a {@link Long}, {@link Double}, or {@link String} matching the
 *     column's JDBC type.
 * @param id Row id — always the final tiebreaker.
 */
public record KeysetCursor(List<Object> sortValues, long id) {}
