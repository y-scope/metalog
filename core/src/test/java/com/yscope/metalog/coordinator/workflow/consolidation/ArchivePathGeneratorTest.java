package com.yscope.metalog.coordinator.workflow.consolidation;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ArchivePathGenerator Unit Tests")
class ArchivePathGeneratorTest {

  private static final String TABLE = "spark";
  private static final String INSTANCE_ID = "node-abc123";

  // ==========================================
  // Constructor
  // ==========================================

  @Test
  void testConstructor_nullTable_throws() {
    assertThrows(NullPointerException.class, () -> new ArchivePathGenerator(null, INSTANCE_ID));
  }

  @Test
  void testConstructor_nullInstanceId_throws() {
    assertThrows(NullPointerException.class, () -> new ArchivePathGenerator(TABLE, null));
  }

  // ==========================================
  // generate(taskId, Instant)
  // ==========================================

  @Nested
  @DisplayName("generate with explicit timestamp")
  class Generate {

    @Test
    void testGenerate_producesCorrectFormat() {
      ArchivePathGenerator gen = new ArchivePathGenerator(TABLE, INSTANCE_ID);
      // 2024-01-15 12:30:00 UTC
      Instant ts = LocalDate.of(2024, 1, 15).atStartOfDay().toInstant(ZoneOffset.UTC);
      String path = gen.generate("task-001", ts);
      assertEquals("spark/2024/01/15/node-abc123/task-001.clp", path);
    }
  }

  // ==========================================
  // extractInstanceId
  // ==========================================

  @Nested
  @DisplayName("extractInstanceId")
  class ExtractInstanceId {

    @Test
    void testExtract_wellFormedPath() {
      String path = "spark/2024/01/15/node-abc123/task-001.clp";
      assertEquals("node-abc123", ArchivePathGenerator.extractInstanceId(path));
    }

    @Test
    void testExtract_null_returnsNull() {
      assertNull(ArchivePathGenerator.extractInstanceId(null));
    }

    @Test
    void testExtract_empty_returnsNull() {
      assertNull(ArchivePathGenerator.extractInstanceId(""));
    }

    @Test
    void testExtract_tooFewSegments_returnsNull() {
      // Only 5 segments — needs at least 6
      assertNull(ArchivePathGenerator.extractInstanceId("spark/2024/01/15/task.clp"));
    }

    @Test
    void testExtract_extraSegments_returnsSecondToLast() {
      // 7 segments — second-to-last is still the instance ID
      String path = "prefix/spark/2024/01/15/node-abc123/task-001.clp";
      assertEquals("node-abc123", ArchivePathGenerator.extractInstanceId(path));
    }
  }

  // ==========================================
  // isFromInstance
  // ==========================================

  @Nested
  @DisplayName("isFromInstance")
  class IsFromInstance {

    @Test
    void testIsFromInstance_matching_returnsTrue() {
      String path = "spark/2024/01/15/node-abc123/task-001.clp";
      assertTrue(ArchivePathGenerator.isFromInstance(path, "node-abc123"));
    }

    @Test
    void testIsFromInstance_different_returnsFalse() {
      String path = "spark/2024/01/15/node-abc123/task-001.clp";
      assertFalse(ArchivePathGenerator.isFromInstance(path, "node-xyz789"));
    }

    @Test
    void testIsFromInstance_nullPath_returnsFalse() {
      assertFalse(ArchivePathGenerator.isFromInstance(null, "node-abc123"));
    }
  }
}
