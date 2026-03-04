package com.yscope.clp.service.coordinator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe per-thread progress tracker for coordinator health monitoring.
 *
 * <p>Each coordinator thread calls {@link #register(String)} before its loop starts, and {@link
 * #recordProgress(String)} at the end of each loop iteration. The Node's watchdog thread queries
 * {@link #getStalestThreadAgeMs()} to detect stalled coordinators.
 *
 * @see com.yscope.clp.service.node.Node
 */
public class ProgressTracker {
  private final ConcurrentHashMap<String, AtomicLong> threadProgress = new ConcurrentHashMap<>();

  /** Register a thread for tracking. Call before the thread's loop starts. */
  public void register(String threadName) {
    threadProgress.put(threadName, new AtomicLong(System.currentTimeMillis()));
  }

  /** Record that a thread completed a loop iteration. Call at end of loop body. */
  public void recordProgress(String threadName) {
    AtomicLong ts = threadProgress.get(threadName);
    if (ts != null) {
      ts.set(System.currentTimeMillis());
    }
  }

  /** Get age in ms of the stalest (oldest) registered thread. Returns 0 if no threads. */
  public long getStalestThreadAgeMs() {
    long now = System.currentTimeMillis();
    long oldest = 0;
    for (AtomicLong ts : threadProgress.values()) {
      long age = now - ts.get();
      if (age > oldest) {
        oldest = age;
      }
    }
    return oldest;
  }

  /** Get name and age of the stalest thread. Returns null if no threads registered. */
  public Map.Entry<String, Long> getStalestThread() {
    long now = System.currentTimeMillis();
    String stalestName = null;
    long stalestAge = 0;
    for (Map.Entry<String, AtomicLong> e : threadProgress.entrySet()) {
      long age = now - e.getValue().get();
      if (age > stalestAge) {
        stalestAge = age;
        stalestName = e.getKey();
      }
    }
    return stalestName != null ? Map.entry(stalestName, stalestAge) : null;
  }

  /** Check if all threads are within the threshold. */
  public boolean isHealthy(long thresholdMs) {
    return getStalestThreadAgeMs() <= thresholdMs;
  }

  /** Remove all registered threads. Call during unit shutdown to prevent stale entries. */
  public void clear() {
    threadProgress.clear();
  }

  /** Get snapshot of all thread ages in ms. */
  public Map<String, Long> getThreadAges() {
    long now = System.currentTimeMillis();
    Map<String, Long> ages = new LinkedHashMap<>();
    for (Map.Entry<String, AtomicLong> e : threadProgress.entrySet()) {
      ages.put(e.getKey(), now - e.getValue().get());
    }
    return ages;
  }
}
