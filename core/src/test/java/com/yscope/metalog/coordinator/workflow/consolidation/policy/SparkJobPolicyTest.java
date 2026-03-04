package com.yscope.metalog.coordinator.workflow.consolidation.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import com.yscope.metalog.testutil.FileRecordBuilder;
import com.yscope.metalog.testutil.TestDataFactory;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SparkJobPolicy aggregation logic.
 *
 * <p>Validates: - Trigger logic (timeout, job complete) - Archive splitting based on size - Path
 * generation (format, UTC date, part numbering) - Compression ratio learning (EMA algorithm) - File
 * grouping by applicationId
 */
class SparkJobPolicyTest {

  private SparkJobPolicy policy;
  private static final long TIMEOUT_HOURS = 4L;
  private static final long TARGET_SIZE_MB = 100L;
  private static final long TEST_EPOCH = 1704067200_000_000_000L; // 2024-01-01 00:00:00 UTC (nanos)

  @BeforeEach
  void setUp() {
    policy = new SparkJobPolicy(TIMEOUT_HOURS, TARGET_SIZE_MB);
  }

  // Trigger logic tests (8 tests)

  @Test
  void testShouldAggregate_emptyList_returnsEmpty() {
    List<FileRecord> files = new ArrayList<>();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_allBuffering_noTrigger() {
    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder().withApplicationId("app-001").buffering().build(),
            new FileRecordBuilder().withApplicationId("app-001").buffering().build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty(), "Buffering files should not trigger aggregation");
  }

  @Test
  void testShouldAggregate_timeoutTrigger_createsTask() {
    long oldTimestamp = Timestamps.nowNanos() - (5 * Timestamps.NANOS_PER_HOUR); // 5 hours ago

    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withMinTimestamp(oldTimestamp)
                .pending()
                .build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    assertEquals("app-001", tasks.get(0).getApplicationId());
  }

  @Test
  void testShouldAggregate_jobComplete_threeFiles_triggers() {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob(); // 3 files

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    assertEquals(3, tasks.get(0).getFileCount());
  }

  @Test
  void testShouldAggregate_jobComplete_lessThanThreeFiles_noTrigger() {
    // Use recent timestamp to avoid timeout trigger (within 4 hour timeout)
    long recentTimestamp = Timestamps.nowNanos() - (2 * Timestamps.NANOS_PER_HOUR); // 2 hours ago

    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withMinTimestamp(recentTimestamp)
                .pending()
                .build(),
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withMinTimestamp(recentTimestamp)
                .pending()
                .build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty(), "Less than 3 files should not trigger job complete");
  }

  @Test
  void testShouldAggregate_mixedStates_onlyPendingConsidered() {
    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder().withApplicationId("app-001").buffering().build(),
            new FileRecordBuilder().withApplicationId("app-001").pending().build(),
            new FileRecordBuilder().withApplicationId("app-001").pending().build(),
            new FileRecordBuilder().withApplicationId("app-001").pending().build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    // Task should only include PENDING files
    assertEquals(3, tasks.get(0).getFileCount());
  }

  @Test
  void testShouldAggregate_multipleApps_separateTasks() {
    List<FileRecord> files = TestDataFactory.createMultiAppFiles();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Should create one task per app (if triggers are met)
    assertTrue(tasks.size() >= 1);

    // Verify each task belongs to a single app
    for (ConsolidationTask task : tasks) {
      assertNotNull(task.getApplicationId());
      assertTrue(task.getFileCount() > 0);
    }
  }

  // Archive splitting tests (8 tests)

  @Test
  void testArchiveSplitting_smallJob_singleArchive() {
    // 3 files * 10MB each = 30MB IR
    // With 3x compression = 10MB archive (under 100MB target)
    List<FileRecord> files =
        Arrays.asList(
            createFileWithSize("app-001", 10 * 1024 * 1024),
            createFileWithSize("app-001", 10 * 1024 * 1024),
            createFileWithSize("app-001", 10 * 1024 * 1024));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    assertFalse(tasks.get(0).getArchivePath().contains("-part"));
  }

  @Test
  void testArchiveSplitting_largeJob_multipleArchives() {
    // 100 files * 10MB each = 1000MB IR
    // With 3x compression = 333MB archive
    // Should split into 4 archives (333 / 100 = 3.33 -> ceil = 4)
    List<FileRecord> files = TestDataFactory.createFilesWithSize(10 * 1024 * 1024, 100);

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.size() > 1, "Large job should be split into multiple archives");

    // Verify all files are accounted for
    int totalFiles = tasks.stream().mapToInt(ConsolidationTask::getFileCount).sum();
    assertEquals(100, totalFiles);
  }

  @Test
  void testArchiveSplitting_estimatedSize_set() {
    List<FileRecord> files = TestDataFactory.createFilesWithSize(10 * 1024 * 1024, 10);

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    for (ConsolidationTask task : tasks) {
      assertTrue(task.getEstimatedArchiveSizeBytes() > 0, "Estimated archive size should be set");
    }
  }

  @Test
  void testArchiveSplitting_noSizeInfo_usesDefaultEstimate() {
    // Files with zero IR size
    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withIrSizeBytes(0L)
                .pending()
                .build(),
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withIrSizeBytes(0L)
                .pending()
                .build(),
            new FileRecordBuilder()
                .withApplicationId("app-001")
                .withIrSizeBytes(0L)
                .pending()
                .build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    assertTrue(tasks.get(0).getEstimatedArchiveSizeBytes() > 0);
  }

  @Test
  void testArchiveSplitting_slightlyOverTarget_twoArchives() {
    // Create 2 files that together compress to slightly over target size
    long targetBytes = TARGET_SIZE_MB * 1024 * 1024;
    long irSizePerFile = (long) (targetBytes * 3.0 * 0.6); // Each file ~55% of target

    List<FileRecord> files =
        Arrays.asList(
            createFileWithSize("app-001", irSizePerFile),
            createFileWithSize("app-001", irSizePerFile));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(2, tasks.size());
  }

  // Path generation tests (6 tests)

  @Test
  void testPathGeneration_format_correct() {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
    String path = tasks.get(0).getArchivePath();

    // Format: /clp-archives/spark/YYYY-MM-DD/app-id.clp
    assertTrue(path.startsWith("/clp-archives/spark/"));
    assertTrue(path.endsWith(".clp"));
  }

  @Test
  void testPathGeneration_utcDate_inPath() {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    String path = tasks.get(0).getArchivePath();
    String todayUTC = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);

    assertTrue(path.contains(todayUTC), "Path should contain today's UTC date");
  }

  @Test
  void testPathGeneration_singleArchive_noPart() {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    String path = tasks.get(0).getArchivePath();
    assertFalse(path.contains("-part"), "Single archive should not have part number");
    assertTrue(path.contains(TestDataFactory.SPARK_APP_001));
  }

  @Test
  void testPathGeneration_multipleArchives_hasPart() {
    List<FileRecord> files = TestDataFactory.createFilesWithSize(50 * 1024 * 1024, 50);

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    if (tasks.size() > 1) {
      for (ConsolidationTask task : tasks) {
        assertTrue(
            task.getArchivePath().contains("-part"), "Multi-part archives should have part number");
      }
    }
  }

  @Test
  void testPathGeneration_appIdInPath() {
    String appId = "custom-spark-app-123";
    List<FileRecord> files =
        Arrays.asList(
            createFileWithSize(appId, 10 * 1024 * 1024),
            createFileWithSize(appId, 10 * 1024 * 1024),
            createFileWithSize(appId, 10 * 1024 * 1024));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    String path = tasks.get(0).getArchivePath();
    assertTrue(path.contains(appId));
  }

  // Compression learning tests (6 tests)

  @Test
  void testCompressionLearning_initialRatio_isDefault() {
    double ratio = policy.getCompressionRatio("spark");

    assertEquals(3.0, ratio, 0.01);
  }

  @Test
  void testCompressionLearning_emaUpdate_correct() {
    // Start with default ratio of 3.0
    // Observe actual ratio of 5.0
    // EMA with learningRate=0.1: newRatio = 0.9 * 3.0 + 0.1 * 5.0 = 3.2

    ConsolidationTask task = TestDataFactory.createCompletedTask();
    task.setActualArchiveSizeBytes(20 * 1024 * 1024); // 20MB archive
    long totalIrSize = 100 * 1024 * 1024; // 100MB IR (5x compression)

    policy.learnFromResult(task, totalIrSize);

    double newRatio = policy.getCompressionRatio("spark");
    assertEquals(3.2, newRatio, 0.01);
  }

  @Test
  void testCompressionLearning_multipleUpdates_converges() {
    // Simulate multiple observations of 4.0x compression
    ConsolidationTask task = TestDataFactory.createCompletedTask();
    long irSize = 40 * 1024 * 1024;
    long archiveSize = 10 * 1024 * 1024; // 4x compression

    task.setActualArchiveSizeBytes(archiveSize);

    // Apply 10 updates
    for (int i = 0; i < 10; i++) {
      policy.learnFromResult(task, irSize);
    }

    double ratio = policy.getCompressionRatio("spark");
    // Should converge towards 4.0 (but not reach it with EMA)
    assertTrue(ratio > 3.5 && ratio < 4.0);
  }

  @Test
  void testCompressionLearning_zeroArchiveSize_noUpdate() {
    double initialRatio = policy.getCompressionRatio("spark");

    ConsolidationTask task = TestDataFactory.createCompletedTask();
    task.setActualArchiveSizeBytes(0); // Zero size

    policy.learnFromResult(task, 100 * 1024 * 1024);

    double afterRatio = policy.getCompressionRatio("spark");
    assertEquals(initialRatio, afterRatio, 0.01);
  }

  @Test
  void testCompressionLearning_zeroIrSize_noUpdate() {
    double initialRatio = policy.getCompressionRatio("spark");

    ConsolidationTask task = TestDataFactory.createCompletedTask();
    task.setActualArchiveSizeBytes(10 * 1024 * 1024);

    policy.learnFromResult(task, 0); // Zero IR size

    double afterRatio = policy.getCompressionRatio("spark");
    assertEquals(initialRatio, afterRatio, 0.01);
  }

  @Test
  void testCompressionLearning_customDefaultRatio() {
    policy.setDefaultCompressionRatio(5.0);

    double ratio = policy.getCompressionRatio("spark");
    assertEquals(5.0, ratio, 0.01);
  }

  // Grouping tests (2 tests)

  @Test
  void testGrouping_byApplicationId() {
    List<FileRecord> files = TestDataFactory.createMultiAppFiles();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Each task should contain files from only one app
    for (ConsolidationTask task : tasks) {
      String appId = task.getApplicationId();
      for (String path : task.getIrFilePaths()) {
        assertTrue(
            path.contains(appId) || path.contains("executor"),
            "All files in task should belong to same app");
      }
    }
  }

  @Test
  void testGrouping_onlyPendingFiles() {
    List<FileRecord> mixedFiles = TestDataFactory.createMixedStateFiles("app-001");

    List<ConsolidationTask> tasks = policy.shouldAggregate(mixedFiles);

    // Count PENDING files in input
    long pendingCount =
        mixedFiles.stream()
            .filter(f -> f.getState() == FileState.IR_ARCHIVE_CONSOLIDATION_PENDING)
            .count();

    // Count files in generated tasks
    long taskFileCount = tasks.stream().mapToLong(ConsolidationTask::getFileCount).sum();

    assertEquals(pendingCount, taskFileCount, "Tasks should only include PENDING files");
  }

  // Helper methods

  private FileRecord createFileWithSize(String appId, long sizeBytes) {
    return new FileRecordBuilder()
        .withApplicationId(appId)
        .withIrSizeBytes(sizeBytes)
        .withMinTimestamp(TEST_EPOCH)
        .pending()
        .build();
  }
}
