package com.yscope.clp.service.coordinator.workflow.consolidation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.common.storage.StorageBackend;
import com.yscope.clp.service.common.storage.StorageRegistry;
import com.yscope.clp.service.metastore.FileRecords;
import com.yscope.clp.service.metastore.TaskQueue;
import com.yscope.clp.service.metastore.model.ConsolidationTask;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.TaskPayload;
import com.yscope.clp.service.metastore.model.TaskResult;
import com.yscope.clp.service.testutil.FileRecordBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Planner Unit Tests")
class PlannerTest {

  private static final String TABLE_NAME = "clp_table";

  private ServiceConfig config;
  private FileRecords repository;
  private TaskQueue taskQueue;
  private InFlightSet inFlightSet;
  private ArchivePathGenerator archivePathGenerator;
  private StorageRegistry storageRegistry;
  private StorageBackend irBackend;
  private StorageBackend archiveBackend;
  private Policy policy;
  private Planner planner;

  @BeforeEach
  void setUp() {
    config = mock(ServiceConfig.class);
    repository = mock(FileRecords.class);
    taskQueue = mock(TaskQueue.class);
    inFlightSet = new InFlightSet();
    archivePathGenerator = new ArchivePathGenerator(TABLE_NAME, "test-instance");
    storageRegistry = mock(StorageRegistry.class);
    irBackend = mock(StorageBackend.class);
    archiveBackend = mock(StorageBackend.class);
    policy = mock(Policy.class);

    when(config.getPlannerIntervalMs()).thenReturn(100L);
    when(config.getBackpressureHighWatermark()).thenReturn(1000);
    when(config.getTaskTimeoutMs()).thenReturn(600_000L);
    when(storageRegistry.getIrBackend()).thenReturn(irBackend);
    when(storageRegistry.getArchiveBackend()).thenReturn(archiveBackend);
    when(irBackend.name()).thenReturn("minio");
    when(irBackend.namespace()).thenReturn("clp-ir");
    when(archiveBackend.name()).thenReturn("minio");
    when(archiveBackend.namespace()).thenReturn("clp-archives");

    planner =
        new Planner(
            config,
            TABLE_NAME,
            repository,
            taskQueue,
            inFlightSet,
            archivePathGenerator,
            storageRegistry,
            List.of(policy));
  }

  // ==========================================
  // Stale Task Reclaim
  // ==========================================

  @Nested
  @DisplayName("Stale Task Reclaim")
  class StaleTasks {

    @Test
    void testReclaimStaleTasks_reclaimsAndClearsInFlight() throws SQLException {
      TaskPayload payload =
          new TaskPayload(
              "archive/path", List.of("/ir/file1.ir", "/ir/file2.ir"),
              TABLE_NAME, "minio", "clp-ir", "minio", "clp-archives");

      TaskQueue.Task staleTask = new TaskQueue.Task();
      staleTask.setTaskId(1L);
      staleTask.setInput(payload.serialize());
      staleTask.setRetryCount(0);

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt()))
          .thenReturn(List.of(staleTask));
      when(taskQueue.reclaimTask(any(), anyInt())).thenReturn(2L);
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      // Pre-add the files to InFlightSet
      inFlightSet.addAll(List.of("/ir/file1.ir", "/ir/file2.ir"), "old-task");

      planner.runPlanningCycle();

      verify(taskQueue).reclaimTask(eq(staleTask), anyInt());
      assertEquals(0, inFlightSet.size());
    }

    @Test
    void testReclaimStaleTasks_sqlException_handledGracefully() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt()))
          .thenThrow(new SQLException("DB error"));
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      // Should not throw
      assertDoesNotThrow(() -> planner.runPlanningCycle());
    }
  }

  // ==========================================
  // Completed Task Processing
  // ==========================================

  @Nested
  @DisplayName("Completed Task Processing")
  class CompletedTasks {

    @Test
    void testProcessCompletedTasks_updatesMetadataAndCleansUp() throws SQLException {
      TaskPayload payload =
          new TaskPayload(
              "archive/path.clp", List.of("/ir/file1.ir"),
              TABLE_NAME, "minio", "clp-ir", "minio", "clp-archives");
      TaskResult result = new TaskResult(1024L, System.currentTimeMillis());

      TaskQueue.Task completedTask = new TaskQueue.Task();
      completedTask.setTaskId(10L);
      completedTask.setInput(payload.serialize());
      completedTask.setOutput(result.serialize());

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of(completedTask));
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      inFlightSet.add("/ir/file1.ir", "task-10");

      planner.runPlanningCycle();

      verify(repository).markArchiveClosed(
          eq(List.of("/ir/file1.ir")), eq("archive/path.clp"), eq(1024L), anyLong());
      verify(taskQueue).deleteTask(10L);
      assertFalse(inFlightSet.contains("/ir/file1.ir"));
    }

    @Test
    void testProcessCompletedTasks_nullInput_deletesTaskOnly() throws SQLException {
      TaskQueue.Task completedTask = new TaskQueue.Task();
      completedTask.setTaskId(10L);
      // No input set — getParsedInput() returns null

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of(completedTask));
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      planner.runPlanningCycle();

      verify(taskQueue).deleteTask(10L);
      verify(repository, never()).markArchiveClosed(any(), any(), anyLong(), anyLong());
    }

    @Test
    void testProcessCompletedTasks_withDeletionQueue_offersIrPaths() throws SQLException {
      LinkedBlockingQueue<String> deletionQueue = new LinkedBlockingQueue<>();
      planner =
          new Planner(
              config, TABLE_NAME, repository, taskQueue, inFlightSet,
              archivePathGenerator, storageRegistry, List.of(policy),
              deletionQueue, null);

      TaskPayload payload =
          new TaskPayload(
              "archive/path.clp", List.of("/ir/file1.ir", "/ir/file2.ir"),
              TABLE_NAME, "minio", "clp-ir", "minio", "clp-archives");
      TaskResult result = new TaskResult(2048L, System.currentTimeMillis());

      TaskQueue.Task completedTask = new TaskQueue.Task();
      completedTask.setTaskId(10L);
      completedTask.setInput(payload.serialize());
      completedTask.setOutput(result.serialize());

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of(completedTask));
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      planner.runPlanningCycle();

      assertEquals(2, deletionQueue.size());
      assertTrue(deletionQueue.contains("/ir/file1.ir"));
      assertTrue(deletionQueue.contains("/ir/file2.ir"));
    }

    @Test
    void testProcessCompletedTasks_sqlExceptionOnFind_handledGracefully() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenThrow(new SQLException("DB error"));
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      assertDoesNotThrow(() -> planner.runPlanningCycle());
    }
  }

  // ==========================================
  // Backpressure
  // ==========================================

  @Nested
  @DisplayName("Backpressure")
  class Backpressure {

    @Test
    void testBackpressure_aboveWatermark_skipsPendingFileQuery() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);

      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      counts.pending = 500;
      counts.processing = 501;
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      planner.runPlanningCycle();

      verify(repository, never()).findConsolidationPendingFiles();
    }

    @Test
    void testBackpressure_belowWatermark_proceedsNormally() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);

      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      counts.pending = 10;
      counts.processing = 5;
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      planner.runPlanningCycle();

      verify(repository).findConsolidationPendingFiles();
    }
  }

  // ==========================================
  // Normal Planning Cycle
  // ==========================================

  @Nested
  @DisplayName("Normal Planning")
  class NormalPlanning {

    @Test
    void testPlanningCycle_pendingFiles_createsTasksViaPolicies() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      FileRecord file2 = new FileRecordBuilder().withIrPath("/ir/file2.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1, file2));

      ConsolidationTask task =
          new ConsolidationTask("archive/path", List.of("/ir/file1.ir", "/ir/file2.ir"), "app-1");
      when(policy.shouldAggregate(anyList())).thenReturn(List.of(task));
      when(taskQueue.createTask(eq(TABLE_NAME), any(byte[].class))).thenReturn(100L);

      planner.runPlanningCycle();

      verify(taskQueue).createTask(eq(TABLE_NAME), any(byte[].class));
      assertEquals(2, inFlightSet.size());
    }

    @Test
    void testPlanningCycle_noPendingFiles_doesNotApplyPolicies() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of());

      planner.runPlanningCycle();

      verify(policy, never()).shouldAggregate(any());
    }

    @Test
    void testPlanningCycle_findPendingFilesThrows_handledGracefully() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);
      when(repository.findConsolidationPendingFiles()).thenThrow(new SQLException("DB error"));

      assertDoesNotThrow(() -> planner.runPlanningCycle());
    }
  }

  // ==========================================
  // InFlightSet Filtering
  // ==========================================

  @Nested
  @DisplayName("InFlightSet Filtering")
  class InFlightFiltering {

    @Test
    void testPlanningCycle_filesAlreadyInFlight_areExcluded() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      FileRecord file2 = new FileRecordBuilder().withIrPath("/ir/file2.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1, file2));

      // file1 is already in flight
      inFlightSet.add("/ir/file1.ir", "existing-task");

      when(policy.shouldAggregate(anyList())).thenReturn(List.of());

      planner.runPlanningCycle();

      // Policy should only receive file2
      verify(policy).shouldAggregate(argThat(files -> {
        List<FileRecord> fileList = (List<FileRecord>) files;
        return fileList.size() == 1 && fileList.get(0).getIrPath().equals("/ir/file2.ir");
      }));
    }

    @Test
    void testPlanningCycle_allInFlight_skipsPolicy() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1));
      inFlightSet.add("/ir/file1.ir", "existing-task");

      planner.runPlanningCycle();

      verify(policy, never()).shouldAggregate(any());
    }
  }

  // ==========================================
  // InFlightSet Race Condition
  // ==========================================

  @Nested
  @DisplayName("InFlightSet Race Condition")
  class InFlightRaceCondition {

    @Test
    void testSubmitTask_raceCondition_skipsTaskAndCleansUp() throws SQLException {
      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1));

      // Task wants both files, but file1 will be added to InFlightSet by another task
      // between eligibility check and addAll
      ConsolidationTask task =
          new ConsolidationTask("archive/path", List.of("/ir/file1.ir", "/ir/extra.ir"), "app-1");
      when(policy.shouldAggregate(anyList())).thenReturn(List.of(task));

      // Pre-add /ir/extra.ir so addAll returns fewer than expected
      inFlightSet.add("/ir/extra.ir", "other-task");

      planner.runPlanningCycle();

      // Task should not have been created because of race condition
      verify(taskQueue, never()).createTask(anyString(), any(byte[].class));
    }
  }

  // ==========================================
  // Policy Exception Isolation
  // ==========================================

  @Nested
  @DisplayName("Policy Exception Isolation")
  class PolicyExceptionIsolation {

    @Test
    void testPlanningCycle_onePolicyThrows_otherPoliciesStillRun() throws SQLException {
      Policy badPolicy = mock(Policy.class);
      Policy goodPolicy = mock(Policy.class);

      planner =
          new Planner(
              config, TABLE_NAME, repository, taskQueue, inFlightSet,
              archivePathGenerator, storageRegistry, List.of(badPolicy, goodPolicy));

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1));

      when(badPolicy.shouldAggregate(anyList())).thenThrow(new RuntimeException("policy error"));
      when(goodPolicy.shouldAggregate(anyList())).thenReturn(List.of());

      assertDoesNotThrow(() -> planner.runPlanningCycle());
      verify(goodPolicy).shouldAggregate(anyList());
    }
  }

  // ==========================================
  // Metrics
  // ==========================================

  @Nested
  @DisplayName("Metrics")
  class MetricsTests {

    @Test
    void testGetMetrics_returnsCorrectValues() throws SQLException {
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      counts.pending = 5;
      counts.processing = 3;
      counts.completed = 10;
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      inFlightSet.add("/ir/file1.ir", "task-1");
      inFlightSet.add("/ir/file2.ir", "task-2");

      Planner.Metrics metrics = planner.getMetrics();

      assertEquals(2, metrics.getInFlightSize());
      assertEquals(5, metrics.getPendingCount());
      assertEquals(3, metrics.getProcessingCount());
      assertEquals(10, metrics.getCompletedCount());
    }

    @Test
    void testGetMetrics_sqlException_returnsZeroedCounts() throws SQLException {
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenThrow(new SQLException("DB error"));
      inFlightSet.add("/ir/file1.ir", "task-1");

      Planner.Metrics metrics = planner.getMetrics();

      assertEquals(1, metrics.getInFlightSize());
      assertEquals(0, metrics.getPendingCount());
      assertEquals(0, metrics.getProcessingCount());
      assertEquals(0, metrics.getCompletedCount());
    }
  }

  // ==========================================
  // Stop and SetPolicies
  // ==========================================

  @Nested
  @DisplayName("Stop and SetPolicies")
  class StopAndSetPolicies {

    @Test
    void testStop_setsRunningToFalse() {
      assertTrue(planner.isRunning());
      planner.stop();
      assertFalse(planner.isRunning());
    }

    @Test
    void testSetPolicies_replacesPolicies() throws SQLException {
      Policy newPolicy = mock(Policy.class);
      planner.setPolicies(List.of(newPolicy));

      when(taskQueue.findStaleTasks(eq(TABLE_NAME), anyInt())).thenReturn(List.of());
      when(taskQueue.findCompletedTasks(TABLE_NAME)).thenReturn(List.of());
      when(taskQueue.cleanup(eq(TABLE_NAME), anyInt())).thenReturn(0);
      TaskQueue.TaskCounts counts = new TaskQueue.TaskCounts();
      when(taskQueue.getTaskCounts(TABLE_NAME)).thenReturn(counts);

      FileRecord file1 = new FileRecordBuilder().withIrPath("/ir/file1.ir").pending().build();
      when(repository.findConsolidationPendingFiles()).thenReturn(List.of(file1));
      when(newPolicy.shouldAggregate(anyList())).thenReturn(List.of());

      planner.runPlanningCycle();

      // Old policy should not be called, new policy should
      verify(policy, never()).shouldAggregate(any());
      verify(newPolicy).shouldAggregate(anyList());
    }
  }
}
