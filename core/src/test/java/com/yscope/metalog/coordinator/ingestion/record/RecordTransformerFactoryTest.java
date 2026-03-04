package com.yscope.metalog.coordinator.ingestion.record;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("RecordTransformerFactory")
class RecordTransformerFactoryTest {

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"default"})
  void create_defaultOrBlank_returnsTransformer(String name) {
    // In OSS, "default" resolves to DefaultRecordTransformer via classpath scanning
    // In Uber, "default" may not be registered (Uber uses explicit transformers)
    if (RecordTransformerFactory.getRegisteredNames().contains("default")) {
      RecordTransformer t = RecordTransformerFactory.create(name);
      assertNotNull(t, "Factory should return a transformer for 'default'");
      assertTrue(
          t.getClass().getSimpleName().contains("Default"),
          "Expected default transformer, got: " + t.getClass().getName());
    } else {
      // In environments without DefaultRecordTransformer, requesting "default" should fail
      assertThrows(
          IllegalArgumentException.class,
          () -> RecordTransformerFactory.create(name),
          "Should throw when 'default' transformer is not registered");
    }
  }

  @Test
  void create_unknown_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class, () -> RecordTransformerFactory.create("nonexistent"));
  }

  @Test
  void getRegisteredNames_returnsNonEmptySet() {
    // OSS will have "default", Uber will have custom transformers (spark, athena, etc.)
    assertFalse(
        RecordTransformerFactory.getRegisteredNames().isEmpty(),
        "Factory should discover at least one transformer");
  }
}
