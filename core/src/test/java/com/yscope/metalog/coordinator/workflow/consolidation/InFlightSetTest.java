package com.yscope.metalog.coordinator.workflow.consolidation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("InFlightSet Unit Tests")
class InFlightSetTest {

  private InFlightSet set;

  @BeforeEach
  void setUp() {
    set = new InFlightSet();
  }

  // ==========================================
  // Add
  // ==========================================

  @Nested
  @DisplayName("add")
  class Add {

    @Test
    void testAdd_newEntry_returnsTrue() {
      assertTrue(set.add("/ir/file1.ir", "task-1"));
    }

    @Test
    void testAdd_duplicate_returnsFalse() {
      set.add("/ir/file1.ir", "task-1");
      assertFalse(set.add("/ir/file1.ir", "task-2"));
    }

    @Test
    void testAdd_preservesOriginalTaskId() {
      set.add("/ir/file1.ir", "task-1");
      set.add("/ir/file1.ir", "task-2");
      assertEquals("task-1", set.getTaskId("/ir/file1.ir"));
    }
  }

  // ==========================================
  // AddAll
  // ==========================================

  @Nested
  @DisplayName("addAll")
  class AddAll {

    @Test
    void testAddAll_allNew_returnsFullCount() {
      int added = set.addAll(List.of("/ir/a.ir", "/ir/b.ir", "/ir/c.ir"), "task-1");
      assertEquals(3, added);
      assertEquals(3, set.size());
    }

    @Test
    void testAddAll_someDuplicates_returnsNewCount() {
      set.add("/ir/a.ir", "task-1");
      int added = set.addAll(List.of("/ir/a.ir", "/ir/b.ir"), "task-2");
      assertEquals(1, added);
    }

    @Test
    void testAddAll_emptyCollection_returnsZero() {
      assertEquals(0, set.addAll(List.of(), "task-1"));
    }
  }

  // ==========================================
  // Remove
  // ==========================================

  @Nested
  @DisplayName("remove / removeAll")
  class Remove {

    @Test
    void testRemove_existing_returnsTrue() {
      set.add("/ir/file1.ir", "task-1");
      assertTrue(set.remove("/ir/file1.ir"));
      assertFalse(set.contains("/ir/file1.ir"));
    }

    @Test
    void testRemove_absent_returnsFalse() {
      assertFalse(set.remove("/ir/nonexistent.ir"));
    }

    @Test
    void testRemoveAll_countsRemovedOnly() {
      set.add("/ir/a.ir", "task-1");
      set.add("/ir/b.ir", "task-1");
      int removed = set.removeAll(List.of("/ir/a.ir", "/ir/b.ir", "/ir/c.ir"));
      assertEquals(2, removed);
      assertTrue(set.isEmpty());
    }
  }

  // ==========================================
  // RemoveAllForTask (rollback safety)
  // ==========================================

  @Nested
  @DisplayName("removeAllForTask")
  class RemoveAllForTask {

    @Test
    void testRemoveAllForTask_matchingTask_removes() {
      set.add("/ir/a.ir", "task-1");
      set.add("/ir/b.ir", "task-1");
      int removed = set.removeAllForTask(List.of("/ir/a.ir", "/ir/b.ir"), "task-1");
      assertEquals(2, removed);
      assertTrue(set.isEmpty());
    }

    @Test
    void testRemoveAllForTask_differentTask_doesNotRemove() {
      set.add("/ir/a.ir", "task-1");
      int removed = set.removeAllForTask(List.of("/ir/a.ir"), "task-2");
      assertEquals(0, removed);
      assertTrue(set.contains("/ir/a.ir"));
    }

    @Test
    void testRemoveAllForTask_mixedOwnership_removesOnlyMatching() {
      set.add("/ir/a.ir", "task-1");
      set.add("/ir/b.ir", "task-2");
      int removed = set.removeAllForTask(List.of("/ir/a.ir", "/ir/b.ir"), "task-1");
      assertEquals(1, removed);
      assertFalse(set.contains("/ir/a.ir"));
      assertTrue(set.contains("/ir/b.ir"));
    }
  }

  // ==========================================
  // Lookups
  // ==========================================

  @Nested
  @DisplayName("contains / getTaskId")
  class Lookups {

    @Test
    void testContains_absent_returnsFalse() {
      assertFalse(set.contains("/ir/file1.ir"));
    }

    @Test
    void testGetTaskId_absent_returnsNull() {
      assertNull(set.getTaskId("/ir/file1.ir"));
    }
  }

  // ==========================================
  // Size / isEmpty / clear
  // ==========================================

  @Nested
  @DisplayName("size / isEmpty / clear")
  class Lifecycle {

    @Test
    void testEmpty_initialState() {
      assertEquals(0, set.size());
      assertTrue(set.isEmpty());
    }

    @Test
    void testSize_reflectsEntries() {
      set.add("/ir/a.ir", "t1");
      set.add("/ir/b.ir", "t2");
      assertEquals(2, set.size());
      assertFalse(set.isEmpty());
    }

    @Test
    void testClear_emptiesSet() {
      set.add("/ir/a.ir", "t1");
      set.add("/ir/b.ir", "t2");
      set.clear();
      assertTrue(set.isEmpty());
      assertEquals(0, set.size());
    }

  }

  // ==========================================
  // getInFlightPaths
  // ==========================================

  @Test
  void testGetInFlightPaths_returnsSnapshot() {
    set.add("/ir/a.ir", "t1");
    set.add("/ir/b.ir", "t2");
    Set<String> paths = set.getInFlightPaths();
    assertEquals(Set.of("/ir/a.ir", "/ir/b.ir"), paths);

    // Snapshot is unmodifiable
    assertThrows(UnsupportedOperationException.class, () -> paths.add("/ir/c.ir"));
  }

  // ==========================================
  // Metrics
  // ==========================================

  @Nested
  @DisplayName("Metrics")
  class MetricsTests {

    @Test
    void testGetMetrics_reflectsSize() {
      set.add("/ir/a.ir", "t1");
      set.add("/ir/b.ir", "t2");
      InFlightSet.Metrics metrics = set.getMetrics();
      assertEquals(2, metrics.getSize());
    }

  }
}
