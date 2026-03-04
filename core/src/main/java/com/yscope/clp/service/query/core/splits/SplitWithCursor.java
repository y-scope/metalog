package com.yscope.clp.service.query.core.splits;

/** A split paired with its keyset pagination cursor. */
public record SplitWithCursor(Split split, KeysetCursor cursor) {}
