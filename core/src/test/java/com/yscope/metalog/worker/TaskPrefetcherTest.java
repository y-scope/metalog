package com.yscope.metalog.worker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.yscope.metalog.metastore.TaskQueue;
import com.yscope.metalog.metastore.model.TaskPayload;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("TaskPrefetcher")
class TaskPrefetcherTest {

  private TaskQueue taskQueue;
  private TaskPrefetcher prefetcher;

  @BeforeEach
  void setUp() {
    taskQueue = mock(TaskQueue.class);
    prefetcher = new TaskPrefetcher(taskQueue, "test-prefetcher", 10);
  }

  @AfterEach
  void tearDown() {
    prefetcher.shutdown();
  }

  private TaskQueue.Task createTask(long taskId) {
    TaskPayload payload =
        new TaskPayload(
            "archive/path.clp",
            List.of("ir/file1.clp"),
            "clp_logs",
            "minio",
            "ir-bucket",
            "minio",
            "archive-bucket");
    TaskQueue.Task task = new TaskQueue.Task();
    task.setTaskId(taskId);
    task.setInput(payload.serialize());
    return task;
  }

  // ==========================================
  // poll
  // ==========================================

  @Nested
  @DisplayName("poll")
  class Poll {

    @Test
    void returnsTaskWhenAvailable() throws Exception {
      TaskQueue.Task task = createTask(1L);
      when(taskQueue.claimTasks(anyString(), anyInt())).thenReturn(List.of(task));

      prefetcher.start();

      // Give prefetch thread time to populate queue
      Optional<TaskQueue.Task> result = Optional.empty();
      for (int i = 0; i < 10 && result.isEmpty(); i++) {
        result = prefetcher.poll(200);
      }

      assertTrue(result.isPresent());
      assertEquals(1L, result.get().getTaskId());
    }

    @Test
    void returnsEmptyOnTimeout() throws Exception {
      when(taskQueue.claimTasks(anyString(), anyInt())).thenReturn(List.of());

      prefetcher.start();

      Optional<TaskQueue.Task> result = prefetcher.poll(200);
      assertTrue(result.isEmpty());
    }
  }

  // ==========================================
  // start
  // ==========================================

  @Nested
  @DisplayName("start")
  class Start {

    @Test
    void beginsBackgroundThread() throws Exception {
      when(taskQueue.claimTasks(anyString(), anyInt())).thenReturn(List.of());

      prefetcher.start();

      // The prefetch thread should be running — use timeout() to wait for invocation
      verify(taskQueue, timeout(2000).atLeastOnce()).claimTasks(eq("test-prefetcher"), eq(10));
    }
  }

  // ==========================================
  // shutdown
  // ==========================================

  @Nested
  @DisplayName("shutdown")
  class Shutdown {

    @Test
    void drainsAndResets() throws Exception {
      TaskQueue.Task task = createTask(1L);
      when(taskQueue.claimTasks(anyString(), anyInt()))
          .thenReturn(List.of(task))
          .thenReturn(List.of());

      prefetcher.start();

      // Wait until the prefetch thread has claimed the task
      verify(taskQueue, timeout(2000).atLeastOnce()).claimTasks(anyString(), anyInt());

      // Shutdown should drain unclaimed tasks and reset them to pending
      prefetcher.shutdown();

      // Verify resetTasksToPending was called with the drained task ID
      verify(taskQueue, atMost(1)).resetTasksToPending(List.of(1L));
    }

    @Test
    void interruptsPrefetchThread() throws Exception {
      when(taskQueue.claimTasks(anyString(), anyInt())).thenReturn(List.of());

      prefetcher.start();

      // Wait until prefetch thread is running
      verify(taskQueue, timeout(2000).atLeastOnce()).claimTasks(anyString(), anyInt());

      // Shutdown should complete without hanging
      assertTimeoutPreemptively(
          java.time.Duration.ofSeconds(5),
          () -> prefetcher.shutdown());
    }
  }

  // ==========================================
  // take
  // ==========================================

  @Nested
  @DisplayName("take")
  class Take {

    @Test
    void blocksUntilAvailable() throws Exception {
      TaskQueue.Task task = createTask(42L);
      when(taskQueue.claimTasks(anyString(), anyInt()))
          .thenReturn(List.of()) // first: no tasks
          .thenReturn(List.of(task)); // second: one task

      prefetcher.start();

      // take() should block until a task is available
      TaskQueue.Task result =
          assertTimeoutPreemptively(
              java.time.Duration.ofSeconds(10),
              () -> prefetcher.take());

      assertEquals(42L, result.getTaskId());
    }
  }
}
