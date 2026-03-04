package com.yscope.metalog.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link TaskResult} serialization round-trip. */
class TaskResultTest {

  @Test
  void testSerializeDeserialize_roundTrip() {
    TaskResult original = new TaskResult(123456789L, 1700000000L);

    byte[] bytes = original.serialize();
    TaskResult deserialized = TaskResult.deserialize(bytes);

    assertEquals(original, deserialized);
  }

  @Test
  void testSerializeDeserialize_zeroValues() {
    // Boundary: zero-length msgpack values compress and decompress correctly
    TaskResult original = new TaskResult(0, 0);
    TaskResult deserialized = TaskResult.deserialize(original.serialize());
    assertEquals(original, deserialized);
  }

  @Test
  void testSerializeDeserialize_largeValues() {
    // Boundary: Long.MAX_VALUE fits in 8-byte msgpack encoding
    TaskResult original = new TaskResult(Long.MAX_VALUE, Long.MAX_VALUE);
    TaskResult deserialized = TaskResult.deserialize(original.serialize());
    assertEquals(Long.MAX_VALUE, deserialized.getArchiveSizeBytes());
    assertEquals(Long.MAX_VALUE, deserialized.getArchiveCreatedAt());
  }
}
