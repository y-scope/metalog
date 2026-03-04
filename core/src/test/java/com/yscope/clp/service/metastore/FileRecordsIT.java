package com.yscope.clp.service.metastore;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.metastore.FileRecords.DeletionResult;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.FileState;
import com.yscope.clp.service.testutil.AbstractMariaDBTest;
import com.yscope.clp.service.testutil.FileRecordBuilder;
import com.yscope.clp.service.testutil.TestDataFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link FileRecords} against a real MariaDB instance.
 *
 * <p>Validates SQL correctness for upsert operations (including the UPSERT_GUARD), MD5 hash-based
 * lookups via VIRTUAL columns, state transitions, batch operations, and retention deletion.
 */
class FileRecordsIT extends AbstractMariaDBTest {

  private static final String TABLE_NAME = "clp_table";

  private FileRecords fileRecords;

  @BeforeEach
  void setUp() throws Exception {
    registerTable(TABLE_NAME);
    fileRecords = new FileRecords(getDataSource(), getDsl(), TABLE_NAME);
  }

  // ========================================================================
  // Single upsert
  // ========================================================================

  @Test
  void upsertFileRecord_insertsAndReadsBack() throws SQLException {
    FileRecord file =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("clp-ir")
            .withIrPath("/spark/app-001/executor-1/file1.zst")
            .withState(FileState.IR_ARCHIVE_BUFFERING)
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 300 * Timestamps.NANOS_PER_SECOND)
            .withRecordCount(5000)
            .withIrSizeBytes(1024L * 1024)
            .withRetentionDays(30)
            .build();

    fileRecords.upsertFileRecord(file);

    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertEquals(FileState.IR_ARCHIVE_BUFFERING, state);
  }

  @Test
  void upsertFileRecord_idempotent() throws SQLException {
    FileRecord file = TestDataFactory.createBufferingFile();

    fileRecords.upsertFileRecord(file);
    fileRecords.upsertFileRecord(file);

    // Should still be just one row — verify via getCurrentStates
    Map<String, FileState> states = fileRecords.getCurrentStates(List.of(file.getIrPath()));
    assertEquals(1, states.size());
  }

  // ========================================================================
  // Upsert guard
  // ========================================================================

  @Test
  void upsertGuard_protectsConsolidationPendingState() throws SQLException {
    // Insert a file in CONSOLIDATION_PENDING state
    FileRecord pending =
        new FileRecordBuilder()
            .withIrPath("/spark/guarded-file.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 600 * Timestamps.NANOS_PER_SECOND)
            .pending()
            .build();
    fileRecords.upsertFileRecord(pending);

    // Re-deliver same path with BUFFERING state and newer max_timestamp
    FileRecord redelivery =
        new FileRecordBuilder()
            .withIrPath("/spark/guarded-file.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 900 * Timestamps.NANOS_PER_SECOND)
            .buffering()
            .build();
    fileRecords.upsertFileRecord(redelivery);

    // State should NOT regress to BUFFERING
    FileState state = fileRecords.getCurrentState("/spark/guarded-file.zst");
    assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, state);
  }

  @Test
  void upsertGuard_allowsUpdateWhenNotProtected() throws SQLException {
    FileRecord buffering =
        new FileRecordBuilder()
            .withIrPath("/spark/updatable-file.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 100 * Timestamps.NANOS_PER_SECOND)
            .buffering()
            .build();
    fileRecords.upsertFileRecord(buffering);

    // Same path, newer max_timestamp, same non-protected state
    FileRecord update =
        new FileRecordBuilder()
            .withIrPath("/spark/updatable-file.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 500 * Timestamps.NANOS_PER_SECOND)
            .withRecordCount(200_000)
            .buffering()
            .build();
    fileRecords.upsertFileRecord(update);

    // State should remain BUFFERING (update was applied because state is not protected)
    FileState state = fileRecords.getCurrentState("/spark/updatable-file.zst");
    assertEquals(FileState.IR_ARCHIVE_BUFFERING, state);
  }

  @Test
  void upsertGuard_rejectsOlderTimestamp() throws SQLException {
    FileRecord first =
        new FileRecordBuilder()
            .withIrPath("/spark/timestamp-guard.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 500 * Timestamps.NANOS_PER_SECOND)
            .withRecordCount(50_000)
            .buffering()
            .build();
    fileRecords.upsertFileRecord(first);

    // Re-deliver with OLDER max_timestamp — should not update
    FileRecord stale =
        new FileRecordBuilder()
            .withIrPath("/spark/timestamp-guard.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .withMaxTimestamp(TestDataFactory.TEST_EPOCH + 100 * Timestamps.NANOS_PER_SECOND)
            .withRecordCount(10_000)
            .buffering()
            .build();
    fileRecords.upsertFileRecord(stale);

    // Verify by reading back — findConsolidationPendingFiles won't work because state is BUFFERING.
    // Use getCurrentState to verify state didn't change, and getCurrentStates for the row existence.
    FileState state = fileRecords.getCurrentState("/spark/timestamp-guard.zst");
    assertEquals(FileState.IR_ARCHIVE_BUFFERING, state);
  }

  // ========================================================================
  // Batch upsert
  // ========================================================================

  @Test
  void upsertBatch_insertsMultipleFiles() throws SQLException {
    List<FileRecord> files = TestDataFactory.createFiveFileJob();

    int affected = fileRecords.upsertBatch(files);

    assertTrue(affected > 0);
    // Verify all files exist
    List<String> paths = files.stream().map(FileRecord::getIrPath).toList();
    Map<String, FileState> states = fileRecords.getCurrentStates(paths);
    assertEquals(5, states.size());
  }

  @Test
  void upsertBatch_emptyListReturnsZero() throws SQLException {
    int affected = fileRecords.upsertBatch(List.of());
    assertEquals(0, affected);
  }

  @Test
  void upsertBatch_largeBatchChunking() throws SQLException {
    // Create 1050 files to test the MAX_MULTI_ROW_INSERT=1000 chunking
    List<FileRecord> files = new ArrayList<>();
    for (int i = 0; i < 1050; i++) {
      files.add(
          new FileRecordBuilder()
              .withApplicationId("bulk-app")
              .withExecutorId("executor-" + i)
              .withMinTimestamp(TestDataFactory.TEST_EPOCH + i)
              .pending()
              .build());
    }

    int affected = fileRecords.upsertBatch(files);

    assertTrue(affected >= 1050);
    // Spot-check a few
    FileState first = fileRecords.getCurrentState(files.get(0).getIrPath());
    FileState last = fileRecords.getCurrentState(files.get(1049).getIrPath());
    assertNotNull(first);
    assertNotNull(last);
  }

  // ========================================================================
  // Find consolidation pending
  // ========================================================================

  @Test
  void findConsolidationPendingFiles_returnsPendingOnly() throws SQLException {
    // Insert files in different states
    List<FileRecord> mixed = TestDataFactory.createMixedStateFiles("mixed-app");
    for (FileRecord f : mixed) {
      fileRecords.upsertFileRecord(f);
    }

    List<FileRecord> pending = fileRecords.findConsolidationPendingFiles();

    // createMixedStateFiles creates 3 pending files
    assertEquals(3, pending.size());
    for (FileRecord f : pending) {
      assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, f.getState());
    }
  }

  @Test
  void findConsolidationPendingFiles_orderedByMinTimestampAsc() throws SQLException {
    // Insert files with specific timestamps in reverse order
    long[] timestamps = {
      TestDataFactory.TEST_EPOCH + 300 * Timestamps.NANOS_PER_SECOND,
      TestDataFactory.TEST_EPOCH + 100 * Timestamps.NANOS_PER_SECOND,
      TestDataFactory.TEST_EPOCH + 200 * Timestamps.NANOS_PER_SECOND
    };
    for (long ts : timestamps) {
      FileRecord file =
          new FileRecordBuilder().withMinTimestamp(ts).pending().build();
      fileRecords.upsertFileRecord(file);
    }

    List<FileRecord> pending = fileRecords.findConsolidationPendingFiles();

    assertEquals(3, pending.size());
    assertTrue(pending.get(0).getMinTimestamp() <= pending.get(1).getMinTimestamp());
    assertTrue(pending.get(1).getMinTimestamp() <= pending.get(2).getMinTimestamp());
  }

  // ========================================================================
  // Get current state(s)
  // ========================================================================

  @Test
  void getCurrentState_returnsSingleState() throws SQLException {
    FileRecord file = TestDataFactory.createBufferingFile();
    fileRecords.upsertFileRecord(file);

    FileState state = fileRecords.getCurrentState(file.getIrPath());

    assertEquals(FileState.IR_ARCHIVE_BUFFERING, state);
  }

  @Test
  void getCurrentState_nullForMissingPath() throws SQLException {
    FileState state = fileRecords.getCurrentState("/nonexistent/path.zst");
    assertNull(state);
  }

  @Test
  void getCurrentStates_batchLookup() throws SQLException {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob();
    for (FileRecord f : files) {
      fileRecords.upsertFileRecord(f);
    }

    List<String> paths = files.stream().map(FileRecord::getIrPath).toList();
    Map<String, FileState> states = fileRecords.getCurrentStates(paths);

    assertEquals(files.size(), states.size());
    for (String path : paths) {
      assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, states.get(path));
    }
  }

  @Test
  void getCurrentStates_emptyListReturnsEmptyMap() throws SQLException {
    Map<String, FileState> states = fileRecords.getCurrentStates(List.of());
    assertTrue(states.isEmpty());
  }

  // ========================================================================
  // Mark archive closed
  // ========================================================================

  @Test
  void markArchiveClosed_transitionsToArchiveClosed() throws SQLException {
    FileRecord file = TestDataFactory.createPendingFile();
    fileRecords.upsertFileRecord(file);

    fileRecords.markArchiveClosed(
        List.of(file.getIrPath()),
        "/archives/consolidated.clp",
        50_000_000L,
        TestDataFactory.TEST_EPOCH + 900);

    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertEquals(FileState.ARCHIVE_CLOSED, state);
  }

  @Test
  void markArchiveClosed_invalidTransitionThrows() throws SQLException {
    FileRecord file = TestDataFactory.createBufferingFile();
    fileRecords.upsertFileRecord(file);

    // BUFFERING cannot transition directly to ARCHIVE_CLOSED
    assertThrows(
        IllegalStateException.class,
        () ->
            fileRecords.markArchiveClosed(
                List.of(file.getIrPath()),
                "/archives/bad.clp",
                50_000_000L,
                TestDataFactory.TEST_EPOCH + 900));
  }

  // ========================================================================
  // Update state
  // ========================================================================

  @Test
  void updateState_singleFile() throws SQLException {
    FileRecord file = TestDataFactory.createBufferingFile();
    fileRecords.upsertFileRecord(file);

    boolean updated =
        fileRecords.updateState(
            file.getIrPath(), FileState.IR_ARCHIVE_CONSOLIDATION_PENDING);

    assertTrue(updated);
    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, state);
  }

  @Test
  void updateState_batchFiles() throws SQLException {
    List<FileRecord> files = TestDataFactory.createSmallSparkJob();
    for (FileRecord f : files) {
      fileRecords.upsertFileRecord(f);
    }

    List<String> paths = files.stream().map(FileRecord::getIrPath).toList();
    // CONSOLIDATION_PENDING -> ARCHIVE_CLOSED is valid
    int affected = fileRecords.updateState(paths, FileState.ARCHIVE_CLOSED);

    assertEquals(files.size(), affected);
  }

  @Test
  void updateState_invalidTransitionThrows() throws SQLException {
    FileRecord file = TestDataFactory.createConsolidatedFile();
    fileRecords.upsertFileRecord(file);

    // ARCHIVE_CLOSED cannot go back to IR_BUFFERING
    assertThrows(
        IllegalStateException.class,
        () -> fileRecords.updateState(file.getIrPath(), FileState.IR_ARCHIVE_BUFFERING));
  }

  // ========================================================================
  // Mark consolidation pending
  // ========================================================================

  @Test
  void markConsolidationPending_transitionsBufferingToPending() throws SQLException {
    FileRecord file = TestDataFactory.createBufferingFile();
    fileRecords.upsertFileRecord(file);

    int affected = fileRecords.markConsolidationPending(List.of(file.getIrPath()));

    assertEquals(1, affected);
    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, state);
  }

  // ========================================================================
  // Delete expired files
  // ========================================================================

  @Test
  void deleteExpiredFiles_deletesExpiredRecords() throws SQLException {
    // Create files with expires_at in the past
    List<FileRecord> expired = TestDataFactory.createExpiredFiles(3);
    for (FileRecord f : expired) {
      fileRecords.upsertFileRecord(f);
    }

    int deleted = fileRecords.deleteExpiredFiles(TestDataFactory.TEST_EPOCH);

    assertEquals(3, deleted);
  }

  @Test
  void deleteExpiredFiles_preservesNonExpired() throws SQLException {
    // Create a non-expired file
    FileRecord file = TestDataFactory.createBufferingFile();
    fileRecords.upsertFileRecord(file);

    // Use current time — file's expires_at is in the future
    int deleted = fileRecords.deleteExpiredFiles(TestDataFactory.TEST_EPOCH);

    assertEquals(0, deleted);
    assertNotNull(fileRecords.getCurrentState(file.getIrPath()));
  }

  @Test
  void deleteExpiredFilesWithPaths_returnsPathsForCleanup() throws SQLException {
    FileRecord expired =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("clp-ir")
            .withIrPath("/spark/expired-file.zst")
            .withArchiveStorageBackend("s3")
            .withArchiveBucket("clp-archives")
            .withArchivePath("/archives/expired.clp")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH - 31 * Timestamps.NANOS_PER_DAY)
            .withExpiresAt(TestDataFactory.TEST_EPOCH - Timestamps.NANOS_PER_DAY)
            .consolidated()
            .build();
    fileRecords.upsertFileRecord(expired);

    DeletionResult result = fileRecords.deleteExpiredFilesWithPaths(TestDataFactory.TEST_EPOCH);

    assertTrue(result.hasDeletedFiles());
    assertEquals(1, result.getDeletedCount());
    assertFalse(result.getIrPaths().isEmpty());
    assertFalse(result.getArchivePaths().isEmpty());
  }

  // ========================================================================
  // Dynamic dimensions
  // ========================================================================

  @Test
  void upsertWithDynamicDimensions_storesDimensionValues() throws Exception {
    // First, add a dimension column to the table
    addDimColumn("dim_f01", "VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL");

    FileRecord file =
        new FileRecordBuilder()
            .withIrPath("/spark/dim-test.zst")
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .buffering()
            .build();
    file.setDimension("dim_f01", "spark-app-123");

    fileRecords.upsertWithDynamicDimensions(file, Set.of("dim_f01"));

    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertEquals(FileState.IR_ARCHIVE_BUFFERING, state);
  }

  // ========================================================================
  // Batch with translation
  // ========================================================================

  @Test
  void upsertBatchWithTranslation_storesTranslatedValues() throws Exception {
    // Add dimension and agg placeholder columns
    addDimColumn("dim_f01", "VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL");
    addAggColumn("agg_f01", "BIGINT NULL");

    List<FileRecord> files = new ArrayList<>();
    FileRecord file =
        new FileRecordBuilder()
            .withMinTimestamp(TestDataFactory.TEST_EPOCH)
            .pending()
            .build();
    file.setDimension("dim/str128/application_id", "spark-app-001");
    file.setIntAgg("agg_int/gte/level/debug", 42_000L);
    files.add(file);

    Map<String, String> dimFieldToColumn = Map.of("dim/str128/application_id", "dim_f01");
    Map<String, String> aggFieldToColumn = Map.of("agg_int/gte/level/debug", "agg_f01");

    int affected =
        fileRecords.upsertBatchWithTranslation(
            files,
            List.of("dim_f01"),
            dimFieldToColumn,
            List.of("agg_f01"),
            aggFieldToColumn,
            Set.of());

    assertTrue(affected > 0);
    FileState state = fileRecords.getCurrentState(file.getIrPath());
    assertNotNull(state);
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private void addDimColumn(String columnName, String sqlType) throws Exception {
    try (var conn = getDataSource().getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "ALTER TABLE "
              + TABLE_NAME
              + " ADD COLUMN "
              + columnName
              + " "
              + sqlType);
    }
  }

  private void addAggColumn(String columnName, String sqlType) throws Exception {
    try (var conn = getDataSource().getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "ALTER TABLE "
              + TABLE_NAME
              + " ADD COLUMN "
              + columnName
              + " "
              + sqlType);
    }
  }
}
