package com.yscope.metalog.coordinator.workflow.consolidation.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.testutil.FileRecordBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TimeWindowPolicy aggregation logic.
 *
 * <p>Validates: - Time window calculation (wall clock alignment) - Trigger logic (window close,
 * timeout) - Archive path generation - File grouping by dimension + time window
 */
class TimeWindowPolicyTest {

  private TimeWindowPolicy policy;
  private static final int WINDOW_INTERVAL_MINUTES = 15;
  private static final int TIMEOUT_HOURS = 1;
  private static final long TARGET_SIZE_MB = 64;
  private static final long TEST_EPOCH = 1704067200_000_000_000L; // 2024-01-01 00:00:00 UTC (nanos)

  @BeforeEach
  void setUp() {
    policy =
        new TimeWindowPolicy(
            "test-policy",
            "dim/str128/application_id", // Match the dimension set by
            // FileRecordBuilder.withApplicationId()
            WINDOW_INTERVAL_MINUTES,
            true, // alignToWallClock
            TIMEOUT_HOURS,
            TARGET_SIZE_MB);
  }

  // Window calculation tests

  @Test
  void testGetWindowStart_alignToWallClock_roundsDown() {
    // Timestamp at 12:07 should round down to 12:00
    long timestamp = TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR) + (7 * Timestamps.NANOS_PER_MINUTE); // 12:07
    long windowStart = policy.getWindowStart(timestamp);

    // Should be 12:00
    assertEquals(TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR), windowStart);
  }

  @Test
  void testGetWindowStart_exactBoundary_staysSame() {
    // Timestamp exactly at 12:15 should stay 12:15
    long timestamp = TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR) + (15 * Timestamps.NANOS_PER_MINUTE); // 12:15
    long windowStart = policy.getWindowStart(timestamp);

    assertEquals(timestamp, windowStart);
  }

  @Test
  void testGetWindowEnd_correct() {
    long windowStart = TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR); // 12:00
    long windowEnd = policy.getWindowEnd(windowStart);

    // 15 minute window, so end should be 12:15
    assertEquals(windowStart + (15 * Timestamps.NANOS_PER_MINUTE), windowEnd);
  }

  // Trigger tests

  @Test
  void testShouldAggregate_emptyList_returnsEmpty() {
    List<FileRecord> files = new ArrayList<>();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_windowClosed_triggers() {
    // Create file in a window that has closed
    // Window 12:00-12:15, current time is 12:20
    long windowStart = TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR); // 12:00
    long fileTimestamp = windowStart + (5 * Timestamps.NANOS_PER_MINUTE); // 12:05

    List<FileRecord> files =
        Arrays.asList(
            createFileWithTimestamp("service-a", fileTimestamp),
            createFileWithTimestamp("service-a", fileTimestamp + 60 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", fileTimestamp + 120 * Timestamps.NANOS_PER_SECOND));

    // Simulate time being after window close
    // Since we can't easily mock time, we use old enough timestamps
    // Align to 15-minute boundary so all files fall in the same window
    long now = Timestamps.nowNanos();
    long intervalNanos = WINDOW_INTERVAL_MINUTES * Timestamps.NANOS_PER_MINUTE;
    long oldWindowStart = ((now - (30 * Timestamps.NANOS_PER_MINUTE)) / intervalNanos) * intervalNanos;
    List<FileRecord> oldFiles =
        Arrays.asList(
            createFileWithTimestamp("service-a", oldWindowStart + 60 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", oldWindowStart + 120 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", oldWindowStart + 180 * Timestamps.NANOS_PER_SECOND));

    List<ConsolidationTask> tasks = policy.shouldAggregate(oldFiles);

    assertEquals(1, tasks.size());
  }

  @Test
  void testShouldAggregate_timeoutTrigger_triggers() {
    // Create files older than timeout
    long oldTimestamp = Timestamps.nowNanos() - (2 * Timestamps.NANOS_PER_HOUR); // 2 hours ago

    List<FileRecord> files = Arrays.asList(createFileWithTimestamp("service-a", oldTimestamp));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
  }

  @Test
  void testShouldAggregate_recentWindow_noTrigger() {
    // Anchor timestamp to the middle of the current 15-minute window so it can never
    // land in a just-closed window regardless of when the test runs.
    long now = Timestamps.nowNanos();
    long intervalNanos = WINDOW_INTERVAL_MINUTES * Timestamps.NANOS_PER_MINUTE;
    long currentWindowStart = (now / intervalNanos) * intervalNanos;
    long recentTimestamp = currentWindowStart + intervalNanos / 2;

    List<FileRecord> files = Arrays.asList(createFileWithTimestamp("service-a", recentTimestamp));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Should not trigger because window hasn't closed and no timeout
    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_onlyPendingFiles() {
    // Align to 15-minute window boundary to ensure all files in same window
    long now = Timestamps.nowNanos();
    long intervalNanos = WINDOW_INTERVAL_MINUTES * Timestamps.NANOS_PER_MINUTE;
    long oldWindowStart = ((now - (2 * Timestamps.NANOS_PER_HOUR)) / intervalNanos) * intervalNanos;

    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("service-a")
                .withMinTimestamp(oldWindowStart + 60 * Timestamps.NANOS_PER_SECOND)
                .buffering()
                .build(),
            createFileWithTimestamp("service-a", oldWindowStart + 120 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", oldWindowStart + 180 * Timestamps.NANOS_PER_SECOND));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Only 2 pending files, buffering should be excluded
    assertEquals(1, tasks.size());
    assertEquals(2, tasks.get(0).getFileCount());
  }

  // Grouping tests

  @Test
  void testShouldAggregate_groupsByDimensionAndWindow() {
    // Align to 15-minute window boundary to ensure all files in same window
    long now = Timestamps.nowNanos();
    long intervalNanos = WINDOW_INTERVAL_MINUTES * Timestamps.NANOS_PER_MINUTE;
    long oldWindowStart = ((now - (2 * Timestamps.NANOS_PER_HOUR)) / intervalNanos) * intervalNanos;

    List<FileRecord> files =
        Arrays.asList(
            // Service A, window 1
            createFileWithTimestamp("service-a", oldWindowStart + 60 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", oldWindowStart + 120 * Timestamps.NANOS_PER_SECOND),
            // Service B, same window
            createFileWithTimestamp("service-b", oldWindowStart + 180 * Timestamps.NANOS_PER_SECOND));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Should create 2 tasks (one per service)
    assertEquals(2, tasks.size());
  }

  @Test
  void testShouldAggregate_differentWindows_separateTasks() {
    // Files in different 15-minute windows
    long window1Start = Timestamps.nowNanos() - (3 * Timestamps.NANOS_PER_HOUR); // 3 hours ago
    long window2Start = window1Start + (30 * Timestamps.NANOS_PER_MINUTE); // 30 minutes later (different window)

    List<FileRecord> files =
        Arrays.asList(
            createFileWithTimestamp("service-a", window1Start + 60 * Timestamps.NANOS_PER_SECOND),
            createFileWithTimestamp("service-a", window2Start + 60 * Timestamps.NANOS_PER_SECOND));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Should create 2 tasks (one per window)
    assertEquals(2, tasks.size());
  }

  // Path generation tests

  @Test
  void testGenerateArchivePath_format_correct() {
    long windowStart = TEST_EPOCH + (12 * Timestamps.NANOS_PER_HOUR); // 12:00

    String path = policy.generateArchivePath("service-a", windowStart, 1, 1);

    assertTrue(path.startsWith("/clp-archives/service-a/"));
    assertTrue(path.contains("2024-01-01"));
    assertTrue(path.contains("12-00"));
    assertTrue(path.endsWith(".clp"));
  }

  @Test
  void testGenerateArchivePath_multiPart_includesPartNumber() {
    long windowStart = TEST_EPOCH;

    String path = policy.generateArchivePath("service-a", windowStart, 2, 3);

    assertTrue(path.contains("-part2"));
  }

  @Test
  void testGenerateArchivePath_singlePart_noPartNumber() {
    long windowStart = TEST_EPOCH;

    String path = policy.generateArchivePath("service-a", windowStart, 1, 1);

    assertFalse(path.contains("-part"));
  }

  @Test
  void testGenerateArchivePath_sanitizesDimensionValue() {
    long windowStart = TEST_EPOCH;

    String path = policy.generateArchivePath("service/name:special", windowStart, 1, 1);

    // Should replace special characters
    assertFalse(path.contains("/name"));
    assertFalse(path.contains(":"));
  }

  // Compression learning tests

  @Test
  void testLearnFromResult_updatesRatio() {
    double initialRatio = policy.getCompressionRatio();

    ConsolidationTask task =
        new ConsolidationTask("/test/path.clp", Arrays.asList("/ir/1", "/ir/2"), "service-a");
    task.markCompleted(20 * 1024 * 1024); // 20MB archive

    policy.learnFromResult(task, 100 * 1024 * 1024); // 100MB IR (5x ratio)

    double newRatio = policy.getCompressionRatio();
    assertTrue(newRatio > initialRatio);
  }

  @Test
  void testLearnFromResult_zeroValues_noUpdate() {
    double initialRatio = policy.getCompressionRatio();

    ConsolidationTask task =
        new ConsolidationTask("/test/path.clp", Arrays.asList("/ir/1"), "service-a");
    task.markCompleted(0); // Zero archive size

    policy.learnFromResult(task, 100 * 1024 * 1024);

    assertEquals(initialRatio, policy.getCompressionRatio(), 0.01);
  }

  // YAML configuration tests

  @Test
  void testCreateFromYamlDefinition() {
    PolicyConfig.PolicyDefinition definition = new PolicyConfig.PolicyDefinition();
    definition.setName("yaml-policy");
    definition.setType("timeWindow");
    definition.setGroupingKey("dim/str128/test");

    PolicyConfig.TriggerConfig triggers = new PolicyConfig.TriggerConfig();
    PolicyConfig.TimeWindowTrigger windowTrigger = new PolicyConfig.TimeWindowTrigger();
    windowTrigger.setIntervalMinutes(30);
    windowTrigger.setAlignToWallClock(false);
    triggers.setTimeWindow(windowTrigger);

    PolicyConfig.TimeoutTrigger timeoutTrigger = new PolicyConfig.TimeoutTrigger();
    timeoutTrigger.setHours(2);
    triggers.setTimeout(timeoutTrigger);
    definition.setTriggers(triggers);

    PolicyConfig.ArchiveSizeConfig archiveSize = new PolicyConfig.ArchiveSizeConfig();
    archiveSize.setTargetMb(128);
    definition.setArchiveSize(archiveSize);

    TimeWindowPolicy yamlPolicy = new TimeWindowPolicy(definition);

    assertEquals(30, yamlPolicy.getWindowIntervalMinutes());
    assertFalse(yamlPolicy.isAlignToWallClock());
    assertEquals(2, yamlPolicy.getTimeoutHours());
  }

  // State transfer tests

  @Test
  void testTransferState_preservesCompressionRatio() {
    // Train old policy
    TimeWindowPolicy oldPolicy = new TimeWindowPolicy("old", "dim", 15, true, 1, 64);
    ConsolidationTask task = new ConsolidationTask("/path.clp", Arrays.asList("/ir/1"), "svc");
    task.markCompleted(20 * 1024 * 1024);
    oldPolicy.learnFromResult(task, 100 * 1024 * 1024);

    double oldRatio = oldPolicy.getCompressionRatio();

    // Transfer to new policy
    TimeWindowPolicy newPolicy = new TimeWindowPolicy("new", "dim", 15, true, 1, 64);
    newPolicy.transferState(oldPolicy);

    assertEquals(oldRatio, newPolicy.getCompressionRatio(), 0.01);
  }

  // ========================================================================
  // Tier 2: YAML constructor default paths
  // ========================================================================

  @Test
  void testYamlConstructor_nullTimeWindowTrigger_defaultsTo15Minutes() {
    PolicyConfig.PolicyDefinition definition = new PolicyConfig.PolicyDefinition();
    definition.setName("defaults-policy");
    definition.setType("timeWindow");
    definition.setGroupingKey("dim/str128/test");

    PolicyConfig.TriggerConfig triggers = new PolicyConfig.TriggerConfig();
    // timeWindow is null — should default to 15 minutes
    triggers.setTimeout(new PolicyConfig.TimeoutTrigger());
    definition.setTriggers(triggers);

    TimeWindowPolicy p = new TimeWindowPolicy(definition);

    assertEquals(15, p.getWindowIntervalMinutes());
    assertFalse(p.isAlignToWallClock()); // null trigger → false
  }

  @Test
  void testYamlConstructor_nullTimeoutTrigger_defaultsTo1Hour() {
    PolicyConfig.PolicyDefinition definition = new PolicyConfig.PolicyDefinition();
    definition.setName("timeout-defaults");
    definition.setType("timeWindow");
    definition.setGroupingKey("dim/str128/test");

    PolicyConfig.TriggerConfig triggers = new PolicyConfig.TriggerConfig();
    PolicyConfig.TimeWindowTrigger wt = new PolicyConfig.TimeWindowTrigger();
    wt.setIntervalMinutes(30);
    triggers.setTimeWindow(wt);
    // timeout is null — should default to 1 hour
    definition.setTriggers(triggers);

    TimeWindowPolicy p = new TimeWindowPolicy(definition);

    assertEquals(1, p.getTimeoutHours());
  }

  // ========================================================================
  // Tier 2: shouldAggregate edge cases
  // ========================================================================

  @Test
  void testShouldAggregate_allNonPendingFiles_returnsEmpty() {
    long oldTimestamp = Timestamps.nowNanos() - (2 * Timestamps.NANOS_PER_HOUR);

    // All files are IR_BUFFERING, not CONSOLIDATION_PENDING
    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("service-a")
                .withMinTimestamp(oldTimestamp)
                .buffering()
                .build(),
            new FileRecordBuilder()
                .withApplicationId("service-a")
                .withMinTimestamp(oldTimestamp + 60 * Timestamps.NANOS_PER_SECOND)
                .buffering()
                .build());

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_multiArchiveSplit_whenTotalIrExceedsTarget() {
    long now = Timestamps.nowNanos();
    long intervalNanos = WINDOW_INTERVAL_MINUTES * Timestamps.NANOS_PER_MINUTE;
    long oldWindowStart = ((now - (2 * Timestamps.NANOS_PER_HOUR)) / intervalNanos) * intervalNanos;

    // Create files with large IR sizes that exceed target (64MB)
    // With default compression ratio of 3.0, 200MB IR → ~67MB archive → > 64MB target
    List<FileRecord> files = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      files.add(
          new FileRecordBuilder()
              .withApplicationId("service-a")
              .withMinTimestamp(oldWindowStart + (i * 10))
              .withMaxTimestamp(oldWindowStart + (i * 10) + 5)
              .withIrSizeBytes(20L * 1024 * 1024) // 20MB each = 200MB total
              .pending()
              .build());
    }

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // With 200MB IR / 3.0 ratio = ~67MB estimated, which exceeds 64MB target
    // Should split into at least 2 archives
    assertTrue(tasks.size() >= 2, "Expected multiple archives but got " + tasks.size());
  }

  // ========================================================================
  // Tier 2: sanitizePathComponent edge cases
  // ========================================================================

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.NullAndEmptySource
  void testGenerateArchivePath_nullOrEmptyDimValue_usesUnknown(String dimValue) {
    String path = policy.generateArchivePath(dimValue, TEST_EPOCH, 1, 1);
    assertTrue(path.contains("/unknown/"), "Expected 'unknown' in path: " + path);
  }

  // ========================================================================
  // Tier 2: Constructor validation
  // ========================================================================

  @Test
  void testConstructor_negativeWindowInterval_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TimeWindowPolicy("test", "dim", -1, true, 1, 64));
  }

  @Test
  void testConstructor_zeroWindowInterval_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TimeWindowPolicy("test", "dim", 0, true, 1, 64));
  }

  // Helper methods

  private FileRecord createFileWithTimestamp(String serviceId, long timestamp) {
    return new FileRecordBuilder()
        .withApplicationId(serviceId)
        .withMinTimestamp(timestamp)
        .withMaxTimestamp(timestamp + 300 * Timestamps.NANOS_PER_SECOND) // 5 minutes
        .pending()
        .build();
  }
}
