package com.yscope.clp.service.coordinator.workflow.consolidation.policy;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.metastore.model.ConsolidationTask;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.testutil.FileRecordBuilder;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for AuditPolicy aggregation logic.
 *
 * <p>Validates: - Size threshold trigger - Timeout trigger - Immutable compression ratio (no
 * learning) - Archive path generation
 */
class AuditPolicyTest {

  private AuditPolicy policy;
  private static final long SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024; // 10MB
  private static final int TIMEOUT_HOURS = 24;
  private static final long TARGET_SIZE_MB = 64;
  private static final double COMPRESSION_RATIO = 3.0;

  @BeforeEach
  void setUp() {
    policy =
        new AuditPolicy(
            "test-audit", SIZE_THRESHOLD_BYTES, TIMEOUT_HOURS, TARGET_SIZE_MB, COMPRESSION_RATIO);
  }

  // Trigger tests

  @Test
  void testShouldAggregate_emptyList_returnsEmpty() {
    List<FileRecord> files = new ArrayList<>();

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_sizeThreshold_triggers() {
    // Create files totaling more than threshold (10MB)
    List<FileRecord> files =
        Arrays.asList(
            createFileWithSize(5 * 1024 * 1024), // 5MB
            createFileWithSize(6 * 1024 * 1024) // 6MB = 11MB total
            );

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
  }

  @Test
  void testShouldAggregate_belowSizeThreshold_noTrigger() {
    // Recent files below threshold
    long recentTimestamp = Timestamps.nowNanos() - Timestamps.NANOS_PER_HOUR; // 1 hour ago

    List<FileRecord> files =
        Arrays.asList(
            createFileWithSizeAndTimestamp(2 * 1024 * 1024, recentTimestamp),
            createFileWithSizeAndTimestamp(3 * 1024 * 1024, recentTimestamp));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Below threshold and not timed out
    assertTrue(tasks.isEmpty());
  }

  @Test
  void testShouldAggregate_timeoutTrigger_triggers() {
    // Files older than timeout (24 hours)
    long oldTimestamp = Timestamps.nowNanos() - (25 * Timestamps.NANOS_PER_HOUR); // 25 hours ago

    List<FileRecord> files =
        Arrays.asList(createFileWithSizeAndTimestamp(1 * 1024 * 1024, oldTimestamp));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
  }

  @Test
  void testShouldAggregate_onlyPendingFiles() {
    long recentTimestamp = Timestamps.nowNanos() - Timestamps.NANOS_PER_HOUR;

    List<FileRecord> files =
        Arrays.asList(
            new FileRecordBuilder()
                .withApplicationId("audit")
                .withIrSizeBytes(8L * 1024 * 1024)
                .withMinTimestamp(recentTimestamp)
                .buffering() // Not pending
                .build(),
            createFileWithSizeAndTimestamp(8 * 1024 * 1024, recentTimestamp),
            createFileWithSizeAndTimestamp(5 * 1024 * 1024, recentTimestamp));

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    // Only 2 pending files (13MB total > 10MB threshold)
    assertEquals(1, tasks.size());
    assertEquals(2, tasks.get(0).getFileCount());
  }

  // Archive splitting tests

  @Test
  void testShouldAggregate_largeInput_splits() {
    // Create files that will require multiple archives
    // With 3x compression ratio and 64MB target:
    // 64MB * 3 = 192MB IR per archive
    // 500MB IR total should create ~3 archives
    List<FileRecord> files = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      files.add(createFileWithSize(10 * 1024 * 1024)); // 10MB each = 500MB total
    }

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertTrue(tasks.size() > 1, "Large input should be split into multiple archives");

    // Verify all files are accounted for
    int totalFiles = tasks.stream().mapToInt(ConsolidationTask::getFileCount).sum();
    assertEquals(50, totalFiles);
  }

  @Test
  void testShouldAggregate_smallInput_singleArchive() {
    List<FileRecord> files =
        Arrays.asList(
            createFileWithSize(15 * 1024 * 1024) // Single 15MB file > threshold
            );

    List<ConsolidationTask> tasks = policy.shouldAggregate(files);

    assertEquals(1, tasks.size());
  }

  // Path generation tests

  @Test
  void testGenerateArchivePath_format_correct() {
    String path = policy.generateArchivePath(1, 1);

    assertTrue(path.startsWith("/clp-archives/audit/"));
    assertTrue(path.endsWith(".clp"));

    // Should contain today's date
    String today = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);
    assertTrue(path.contains(today));
  }

  @Test
  void testGenerateArchivePath_multiPart_includesPartNumber() {
    String path = policy.generateArchivePath(2, 3);

    assertTrue(path.contains("-part2"));
  }

  @Test
  void testGenerateArchivePath_singlePart_noPartNumber() {
    String path = policy.generateArchivePath(1, 1);

    assertFalse(path.contains("-part"));
  }

  @Test
  void testGenerateArchivePath_uniquePaths() {
    String path1 = policy.generateArchivePath(1, 1);
    String path2 = policy.generateArchivePath(1, 1);

    // Should generate unique paths (contains UUID and timestamp)
    assertNotEquals(path1, path2);
  }

  // Immutable compression tests

  @Test
  void testLearnFromResult_immutable_noUpdate() {
    double initialRatio = policy.getCompressionRatio();

    ConsolidationTask task =
        new ConsolidationTask(
            "/clp-archives/audit/test.clp", Arrays.asList("/ir/1", "/ir/2"), "audit");
    task.markCompleted(10 * 1024 * 1024); // 10MB archive

    // Try to learn from 50MB IR (5x observed ratio)
    policy.learnFromResult(task, 50 * 1024 * 1024);

    // Ratio should NOT change for immutable audit policy
    assertEquals(initialRatio, policy.getCompressionRatio(), 0.01);
  }

  @Test
  void testIsImmutable_returnsTrue() {
    assertTrue(policy.isImmutable());
  }

  // YAML configuration tests

  @Test
  void testCreateFromYamlDefinition() {
    PolicyConfig.PolicyDefinition definition = new PolicyConfig.PolicyDefinition();
    definition.setName("yaml-audit");
    definition.setType("audit");
    definition.setImmutable(true);

    PolicyConfig.TriggerConfig triggers = new PolicyConfig.TriggerConfig();
    PolicyConfig.SizeThresholdTrigger sizeTrigger = new PolicyConfig.SizeThresholdTrigger();
    sizeTrigger.setMinBytes(20 * 1024 * 1024);
    triggers.setSizeThreshold(sizeTrigger);

    PolicyConfig.TimeoutTrigger timeoutTrigger = new PolicyConfig.TimeoutTrigger();
    timeoutTrigger.setHours(12);
    triggers.setTimeout(timeoutTrigger);
    definition.setTriggers(triggers);

    PolicyConfig.ArchiveSizeConfig archiveSize = new PolicyConfig.ArchiveSizeConfig();
    archiveSize.setTargetMb(128);
    definition.setArchiveSize(archiveSize);

    PolicyConfig.CompressionRatioConfig ratioConfig = new PolicyConfig.CompressionRatioConfig();
    ratioConfig.setInitialRatio(4.0);
    definition.setCompressionRatio(ratioConfig);

    AuditPolicy yamlPolicy = new AuditPolicy(definition);

    assertEquals(20 * 1024 * 1024, yamlPolicy.getSizeThresholdBytes());
    assertEquals(12, yamlPolicy.getTimeoutHours());
    assertEquals(128, yamlPolicy.getTargetArchiveSizeMb());
    assertEquals(4.0, yamlPolicy.getCompressionRatio(), 0.01);
    assertTrue(yamlPolicy.isImmutable());
  }

  @Test
  void testCreateFromYamlDefinition_defaults() {
    PolicyConfig.PolicyDefinition definition = new PolicyConfig.PolicyDefinition();
    definition.setName("minimal-audit");
    definition.setType("audit");

    AuditPolicy yamlPolicy = new AuditPolicy(definition);

    // Should use default values
    assertEquals(10 * 1024 * 1024, yamlPolicy.getSizeThresholdBytes());
    assertEquals(24, yamlPolicy.getTimeoutHours());
  }

  // Helper methods

  private FileRecord createFileWithSize(long sizeBytes) {
    long oldTimestamp =
        Timestamps.nowNanos() - (25 * Timestamps.NANOS_PER_HOUR); // Ensures timeout trigger
    return new FileRecordBuilder()
        .withApplicationId("audit")
        .withIrSizeBytes(sizeBytes)
        .withMinTimestamp(oldTimestamp)
        .pending()
        .build();
  }

  private FileRecord createFileWithSizeAndTimestamp(long sizeBytes, long timestamp) {
    return new FileRecordBuilder()
        .withApplicationId("audit")
        .withIrSizeBytes(sizeBytes)
        .withMinTimestamp(timestamp)
        .pending()
        .build();
  }
}
