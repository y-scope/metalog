package com.yscope.metalog.query.core.splits;

/** A split paired with its keyset pagination cursor. */
public record SplitWithCursor(Split split, KeysetCursor cursor) {}
