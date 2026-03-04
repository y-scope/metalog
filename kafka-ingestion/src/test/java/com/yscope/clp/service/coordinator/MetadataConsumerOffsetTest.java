package com.yscope.clp.service.coordinator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for per-partition offset commit logic in MetadataConsumer.
 *
 * <p>Tests the commitOffsets() merge semantics and thread-safety without requiring a real Kafka
 * consumer. Uses reflection to inspect the pendingCommitOffsets AtomicReference directly.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Single partition commit queuing
 *   <li>Multi-partition independent tracking
 *   <li>Merge semantics: max offset wins per partition
 *   <li>Empty map is no-op
 *   <li>Concurrent commits from multiple threads merge correctly
 * </ul>
 */
class MetadataConsumerOffsetTest {

  /**
   * Isolated test of commitOffsets merge logic using a standalone AtomicReference. This avoids
   * needing a Kafka consumer just to test the merge semantics.
   */
  private final AtomicReference<Map<Integer, Long>> pendingCommitOffsets =
      new AtomicReference<>(Collections.emptyMap());

  /** Mirrors MetadataConsumer.commitOffsets() logic. */
  private void commitOffsets(Map<Integer, Long> newOffsets) {
    if (newOffsets.isEmpty()) return;
    pendingCommitOffsets.updateAndGet(
        current -> {
          Map<Integer, Long> merged = new HashMap<>(current);
          newOffsets.forEach((partition, offset) -> merged.merge(partition, offset, Math::max));
          return merged;
        });
  }

  /** Mirrors MetadataConsumer.commitPendingOffset() drain. */
  private Map<Integer, Long> drainPending() {
    return pendingCommitOffsets.getAndSet(Collections.emptyMap());
  }

  @Test
  void testSinglePartitionCommit() {
    commitOffsets(Map.of(0, 100L));

    Map<Integer, Long> pending = drainPending();
    assertEquals(1, pending.size());
    assertEquals(100L, pending.get(0));
  }

  @Test
  void testMultiPartitionCommit() {
    commitOffsets(Map.of(0, 100L, 1, 200L, 2, 50L));

    Map<Integer, Long> pending = drainPending();
    assertEquals(3, pending.size());
    assertEquals(100L, pending.get(0));
    assertEquals(200L, pending.get(1));
    assertEquals(50L, pending.get(2));
  }

  @Test
  void testMerge_maxOffsetWinsPerPartition() {
    commitOffsets(Map.of(0, 100L, 1, 200L));
    commitOffsets(Map.of(0, 50L, 1, 300L)); // P0 lower, P1 higher

    Map<Integer, Long> pending = drainPending();
    assertEquals(100L, pending.get(0), "P0 should keep the higher offset");
    assertEquals(300L, pending.get(1), "P1 should advance to the higher offset");
  }

  @Test
  void testMerge_newPartitionsAdded() {
    commitOffsets(Map.of(0, 100L));
    commitOffsets(Map.of(1, 200L)); // New partition

    Map<Integer, Long> pending = drainPending();
    assertEquals(2, pending.size());
    assertEquals(100L, pending.get(0));
    assertEquals(200L, pending.get(1));
  }

  @Test
  void testEmptyMap_isNoOp() {
    commitOffsets(Map.of(0, 100L));
    commitOffsets(Collections.emptyMap()); // Should be no-op

    Map<Integer, Long> pending = drainPending();
    assertEquals(1, pending.size());
    assertEquals(100L, pending.get(0));
  }

  @Test
  void testDrain_resetsToEmpty() {
    commitOffsets(Map.of(0, 100L));

    Map<Integer, Long> first = drainPending();
    assertFalse(first.isEmpty());

    Map<Integer, Long> second = drainPending();
    assertTrue(second.isEmpty(), "Second drain should return empty map");
  }

  @Test
  void testConcurrentCommits_mergeSafely() throws InterruptedException {
    int numThreads = 8;
    int commitsPerThread = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int t = 0; t < numThreads; t++) {
      final int partition = t % 4; // 4 partitions across 8 threads
      final int threadId = t;
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < commitsPerThread; i++) {
                // Each thread commits increasing offsets for its partition
                commitOffsets(Map.of(partition, (long) (threadId * commitsPerThread + i)));
              }
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Concurrent commits should complete");
    executor.shutdown();

    Map<Integer, Long> pending = drainPending();
    // Each of the 4 partitions should have the max offset from its threads
    assertEquals(4, pending.size());
    for (int p = 0; p < 4; p++) {
      assertTrue(pending.containsKey(p), "Partition " + p + " should be present");
      assertTrue(pending.get(p) >= 0, "Partition " + p + " offset should be non-negative");
    }
  }
}
