package com.yscope.clp.service.worker;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.yscope.clp.service.common.storage.ArchiveCreator;
import com.yscope.clp.service.common.storage.StorageBackend;
import com.yscope.clp.service.common.storage.StorageException;
import com.yscope.clp.service.metastore.TaskQueue;
import com.yscope.clp.service.metastore.model.TaskPayload;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("WorkerCore")
class WorkerCoreTest {

  private TaskQueue taskQueue;
  private ArchiveCreator archiveCreator;
  private StorageBackend archiveBackend;
  private WorkerCore.TaskClaimer claimer;
  private WorkerCore workerCore;

  @BeforeEach
  void setUp() {
    taskQueue = mock(TaskQueue.class);
    archiveCreator = mock(ArchiveCreator.class);
    archiveBackend = mock(StorageBackend.class);
    claimer = mock(WorkerCore.TaskClaimer.class);
    workerCore = new WorkerCore(taskQueue, archiveCreator, archiveBackend, claimer, "");
  }

  private TaskQueue.Task createMockTask(long taskId) {
    TaskPayload payload =
        new TaskPayload(
            "archive/path.clp",
            List.of("ir/file1.clp", "ir/file2.clp"),
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
  // executeTask
  // ==========================================

  @Nested
  @DisplayName("executeTask")
  class ExecuteTask {

    @Test
    void success_completesTask() throws Exception {
      TaskQueue.Task task = createMockTask(1L);
      when(archiveCreator.createArchive(anyString(), anyList())).thenReturn(4096L);
      when(taskQueue.completeTask(eq(1L), any(byte[].class))).thenReturn(1);

      workerCore.executeTask("worker-1", task);

      verify(taskQueue).completeTask(eq(1L), any(byte[].class));
      verify(taskQueue, never()).failTask(anyLong());
    }

    @Test
    void archiveFails_failsTask() throws Exception {
      TaskQueue.Task task = createMockTask(2L);
      when(archiveCreator.createArchive(anyString(), anyList()))
          .thenThrow(new StorageException("S3 down"));
      when(taskQueue.failTask(2L)).thenReturn(1);

      workerCore.executeTask("worker-1", task);

      verify(taskQueue).failTask(2L);
      verify(taskQueue, never()).completeTask(anyLong(), any());
    }

    @Test
    void archiveFails_deletesOrphan() throws Exception {
      TaskQueue.Task task = createMockTask(3L);
      when(archiveCreator.createArchive(anyString(), anyList()))
          .thenThrow(new StorageException("S3 down"));
      when(taskQueue.failTask(3L)).thenReturn(1);

      workerCore.executeTask("worker-1", task);

      verify(archiveBackend).deleteFile("archive/path.clp");
    }

    @Test
    void completeFails_noThrow() throws Exception {
      TaskQueue.Task task = createMockTask(4L);
      when(archiveCreator.createArchive(anyString(), anyList())).thenReturn(4096L);
      when(taskQueue.completeTask(eq(4L), any(byte[].class)))
          .thenThrow(new SQLException("Connection lost"));

      // Should not throw - error is caught and logged, then failTask is called
      assertDoesNotThrow(() -> workerCore.executeTask("worker-1", task));
    }
  }

  // ==========================================
  // deleteOrphanArchive
  // ==========================================

  @Nested
  @DisplayName("deleteOrphanArchive")
  class DeleteOrphanArchive {

    @Test
    void callsArchiveBackend() throws Exception {
      workerCore.deleteOrphanArchive("worker-1", "orphan/path.clp");
      verify(archiveBackend).deleteFile("orphan/path.clp");
    }

    @Test
    void storageFailure_noThrow() throws Exception {
      doThrow(new StorageException("delete failed"))
          .when(archiveBackend)
          .deleteFile("orphan/path.clp");

      assertDoesNotThrow(() -> workerCore.deleteOrphanArchive("worker-1", "orphan/path.clp"));
    }
  }

  // ==========================================
  // runLoop
  // ==========================================

  @Nested
  @DisplayName("runLoop")
  class RunLoop {

    @Test
    void emptyQueue_respectsRunningFlag() throws Exception {
      AtomicBoolean running = new AtomicBoolean(false);
      when(claimer.claim(anyString())).thenReturn(Optional.empty());

      workerCore.runLoop("worker-1", running);

      // Loop should exit immediately since running is false
      verify(claimer, never()).claim(anyString());
    }

    @Test
    void interrupt_exitsLoop() throws Exception {
      AtomicBoolean running = new AtomicBoolean(true);
      when(claimer.claim(anyString())).thenThrow(new InterruptedException("interrupted"));

      Thread t =
          new Thread(
              () -> {
                workerCore.runLoop("worker-1", running);
              });
      t.start();
      t.join(2000);

      assertFalse(t.isAlive(), "Thread should have exited after interrupt");
    }

    @Test
    void claimsAndExecutes() throws Exception {
      AtomicBoolean running = new AtomicBoolean(true);
      TaskQueue.Task task = createMockTask(10L);

      when(claimer.claim("worker-1"))
          .thenReturn(Optional.of(task))
          .thenAnswer(
              inv -> {
                running.set(false);
                return Optional.empty();
              });
      when(archiveCreator.createArchive(anyString(), anyList())).thenReturn(4096L);
      when(taskQueue.completeTask(eq(10L), any(byte[].class))).thenReturn(1);

      workerCore.runLoop("worker-1", running);

      verify(taskQueue).completeTask(eq(10L), any(byte[].class));
    }
  }
}
