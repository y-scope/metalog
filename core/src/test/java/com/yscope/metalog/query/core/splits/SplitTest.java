package com.yscope.metalog.query.core.splits;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Split")
class SplitTest {

  private Split createSplit(String objectPath, String archivePath) {
    return new Split(
        1L,
        objectPath,
        "s3",
        "ir-bucket",
        archivePath,
        "s3",
        "archive-bucket",
        1000L,
        2000L,
        "IR_CLOSED",
        Map.of("dim/str128/service", "web", "dim/str64/zone", "us-east"),
        List.of(),
        100L,
        4096L);
  }

  // ==========================================
  // isArchive
  // ==========================================

  @Nested
  @DisplayName("isArchive")
  class IsArchive {

    @Test
    void trueWhenArchivePathSet() {
      Split split = createSplit("ir/path.clp", "archive/path.clp");
      assertTrue(split.isArchive());
    }

    @Test
    void falseWhenArchivePathNull() {
      Split split = createSplit("ir/path.clp", null);
      assertFalse(split.isArchive());
    }

    @Test
    void falseWhenArchivePathEmpty() {
      Split split = createSplit("ir/path.clp", "");
      assertFalse(split.isArchive());
    }
  }

  // ==========================================
  // effectivePath
  // ==========================================

  @Nested
  @DisplayName("effectivePath")
  class EffectivePath {

    @Test
    void returnsArchivePathWhenConsolidated() {
      Split split = createSplit("ir/path.clp", "archive/path.clp");
      assertEquals("archive/path.clp", split.effectivePath());
    }

    @Test
    void returnsObjectPathWhenNotConsolidated() {
      Split split = createSplit("ir/path.clp", null);
      assertEquals("ir/path.clp", split.effectivePath());
    }
  }

  // ==========================================
  // matchesDimensions
  // ==========================================

  @Nested
  @DisplayName("matchesDimensions")
  class MatchesDimensions {

    @Test
    void allMatch() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.matchesDimensions(Map.of("dim/str128/service", "web")));
    }

    @Test
    void partialMismatch() {
      Split split = createSplit("ir/path.clp", null);
      assertFalse(
          split.matchesDimensions(Map.of("dim/str128/service", "web", "dim/str64/zone", "eu-west")));
    }

    @Test
    void emptyFiltersReturnsTrue() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.matchesDimensions(Map.of()));
    }

    @Test
    void missingDimensionReturnsFalse() {
      Split split = createSplit("ir/path.clp", null);
      assertFalse(split.matchesDimensions(Map.of("dim/str128/unknown", "value")));
    }
  }

  // ==========================================
  // overlapsTimeRange
  // ==========================================

  @Nested
  @DisplayName("overlapsTimeRange")
  class OverlapsTimeRange {

    @Test
    void withinRange() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.overlapsTimeRange(500L, 1500L));
    }

    @Test
    void beforeRange() {
      Split split = createSplit("ir/path.clp", null);
      assertFalse(split.overlapsTimeRange(3000L, 4000L));
    }

    @Test
    void afterRange() {
      Split split = createSplit("ir/path.clp", null);
      assertFalse(split.overlapsTimeRange(0L, 500L));
    }

    @Test
    void nullBoundsReturnsTrue() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.overlapsTimeRange(null, null));
    }

    @Test
    void nullMinBound() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.overlapsTimeRange(null, 1500L));
    }

    @Test
    void nullMaxBound() {
      Split split = createSplit("ir/path.clp", null);
      assertTrue(split.overlapsTimeRange(500L, null));
    }
  }

  // ==========================================
  // getDimension
  // ==========================================

  @Nested
  @DisplayName("getDimension")
  class GetDimension {

    @Test
    void returnsValueForExistingKey() {
      Split split = createSplit("ir/path.clp", null);
      assertEquals("web", split.getDimension("dim/str128/service"));
    }

    @Test
    void returnsNullForMissingKey() {
      Split split = createSplit("ir/path.clp", null);
      assertNull(split.getDimension("nonexistent"));
    }
  }
}
