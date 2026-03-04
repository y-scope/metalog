package com.yscope.metalog.coordinator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ProgressTracker}. */
class ProgressTrackerTest {

  @Test
  void emptyTracker_returnsZeroAge() {
    ProgressTracker tracker = new ProgressTracker();

    assertEquals(0, tracker.getStalestThreadAgeMs());
    assertNull(tracker.getStalestThread());
    assertTrue(tracker.isHealthy(0));
    assertTrue(tracker.getThreadAges().isEmpty());
  }

  @Test
  void register_setsInitialTimestamp() {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("poller");

    // Just registered, age should be very small
    assertTrue(tracker.getStalestThreadAgeMs() < 100);
    assertTrue(tracker.isHealthy(1000));
  }

  @Test
  void recordProgress_updatesTimestamp() throws InterruptedException {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("poller");

    Thread.sleep(50);
    long ageBefore = tracker.getStalestThreadAgeMs();
    assertTrue(ageBefore >= 50);

    tracker.recordProgress("poller");
    long ageAfter = tracker.getStalestThreadAgeMs();

    assertTrue(ageAfter < ageBefore, "Age should decrease after recording progress");
  }

  @Test
  void recordProgress_unregisteredThread_isIgnored() {
    ProgressTracker tracker = new ProgressTracker();

    // Should not throw
    tracker.recordProgress("unknown");

    assertEquals(0, tracker.getStalestThreadAgeMs());
  }

  @Test
  void getStalestThread_returnsOldestThread() throws InterruptedException {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("fast");
    tracker.register("slow");

    // Let time pass
    Thread.sleep(100);

    // Update "fast" but not "slow"
    tracker.recordProgress("fast");

    Map.Entry<String, Long> stalest = tracker.getStalestThread();
    assertNotNull(stalest);
    assertEquals("slow", stalest.getKey());
    assertTrue(stalest.getValue() >= 100);
  }

  @Test
  void isHealthy_returnsFalseWhenStale() throws InterruptedException {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("poller");

    assertTrue(tracker.isHealthy(200));

    Thread.sleep(150);

    assertFalse(tracker.isHealthy(100));
    assertTrue(tracker.isHealthy(500));
  }

  @Test
  void getThreadAges_returnsAllThreads() {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("poller");
    tracker.register("writer");
    tracker.register("planner");

    Map<String, Long> ages = tracker.getThreadAges();
    assertEquals(3, ages.size());
    assertTrue(ages.containsKey("poller"));
    assertTrue(ages.containsKey("writer"));
    assertTrue(ages.containsKey("planner"));
  }

  @Test
  void multipleThreads_independentProgress() throws InterruptedException {
    ProgressTracker tracker = new ProgressTracker();
    tracker.register("poller");
    tracker.register("writer");

    Thread.sleep(100);

    // Only update poller
    tracker.recordProgress("poller");

    Map<String, Long> ages = tracker.getThreadAges();
    assertTrue(ages.get("poller") < ages.get("writer"), "Poller should be fresher than writer");
  }
}
