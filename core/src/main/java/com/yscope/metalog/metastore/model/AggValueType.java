package com.yscope.metalog.metastore.model;

/**
 * Value type for aggregate columns, determining the physical column type.
 *
 * <p>{@code INT} maps to {@code BIGINT NULL}. {@code FLOAT} maps to {@code DOUBLE NULL}.
 */
public enum AggValueType {
  INT,
  FLOAT
}
