package com.yscope.clp.service.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.testutil.FileRecordBuilder;
import com.yscope.clp.service.testutil.TestDataFactory;
import org.junit.jupiter.api.Test;

/**
 * Tests for FileRecord domain model.
 *
 * <p>Validates: - Constructor initialization (timestamps, state, retention calculation) - Equality
 * based on 6-column composite primary key - Hash consistency with equals - isConsolidated() logic -
 * Retention calculation: expiresAt = minTimestamp + (retentionDays * NANOS_PER_DAY)
 */
class FileRecordTest {

  // Constructor tests

  @Test
  void testDefaultConstructor_createsEmptyFile() {
    FileRecord file = new FileRecord();

    assertNull(file.getIrPath());
    assertNull(file.getStringDimension("dim/str128/application_id"));
    assertEquals(0L, file.getMinTimestamp());
    // Size fields are nullable per design spec (NULL = unknown)
    assertNull(file.getRawSizeBytes());
    assertNull(file.getIrSizeBytes());
    assertNull(file.getArchiveSizeBytes());
  }

  @Test
  void testPathOnlyConstructor_initializesFields() {
    long beforeCreate = Timestamps.nowNanos();

    FileRecord file = new FileRecord("/spark/app-001/executor-1/file.clp");

    long afterCreate = Timestamps.nowNanos();

    // Check path
    assertEquals("/spark/app-001/executor-1/file.clp", file.getIrPath());

    // Dimensions are not set by constructor - use setDimension for dynamic fields
    assertNull(file.getStringDimension("dim/str128/application_id"));

    // Check state
    assertEquals(FileState.IR_ARCHIVE_BUFFERING, file.getState());

    // Check timestamps (within reasonable range)
    assertTrue(file.getMinTimestamp() >= beforeCreate);
    assertTrue(file.getMinTimestamp() <= afterCreate);
    assertEquals(file.getMinTimestamp(), file.getMaxTimestamp());

    // Check retention (default 30 days)
    assertEquals(30, file.getRetentionDays());
    assertEquals(file.getMinTimestamp() + (30 * Timestamps.NANOS_PER_DAY), file.getExpiresAt());

    // Check empty storage backends
    assertEquals("", file.getIrStorageBackend());
    assertEquals("", file.getIrBucket());
    assertEquals("", file.getArchiveStorageBackend());
    assertEquals("", file.getArchiveBucket());
    assertEquals("", file.getArchivePath());
  }

  // Equality tests (based on 6-column composite primary key)

  @Test
  void testEquals_sameCompositeKey_returnsTrue() {
    FileRecord file1 =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("bucket-1")
            .withIrPath("/path/to/file.clp")
            .withArchiveStorageBackend("s3")
            .withArchiveBucket("archives")
            .withArchivePath("/archive/file.clp")
            .build();

    FileRecord file2 =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("bucket-1")
            .withIrPath("/path/to/file.clp")
            .withArchiveStorageBackend("s3")
            .withArchiveBucket("archives")
            .withArchivePath("/archive/file.clp")
            .withApplicationId("different-app") // Different non-key field
            .build();

    assertEquals(file1, file2);
  }

  @Test
  void testEquals_differentIrPath_returnsFalse() {
    FileRecord file1 = new FileRecordBuilder().withIrPath("/path/to/file1.clp").build();

    FileRecord file2 = new FileRecordBuilder().withIrPath("/path/to/file2.clp").build();

    assertNotEquals(file1, file2);
  }

  @Test
  void testEquals_differentIrBackend_returnsFalse() {
    FileRecord file1 = new FileRecordBuilder().withIrStorageBackend("s3").build();

    FileRecord file2 = new FileRecordBuilder().withIrStorageBackend("minio").build();

    assertNotEquals(file1, file2);
  }

  @Test
  void testEquals_differentArchivePath_returnsFalse() {
    FileRecord file1 = new FileRecordBuilder().withArchivePath("/archive1.clp").build();

    FileRecord file2 = new FileRecordBuilder().withArchivePath("/archive2.clp").build();

    assertNotEquals(file1, file2);
  }

  // Hash code tests

  @Test
  void testHashCode_sameCompositeKey_sameHash() {
    FileRecord file1 =
        new FileRecordBuilder().withIrStorageBackend("s3").withIrPath("/path/file.clp").build();

    FileRecord file2 =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrPath("/path/file.clp")
            .withApplicationId("different-app")
            .build();

    assertEquals(file1.hashCode(), file2.hashCode());
  }

  // isConsolidated tests

  @Test
  void testIsConsolidated_emptyArchivePath_returnsFalse() {
    FileRecord file = new FileRecordBuilder().withArchivePath("").build();

    assertFalse(file.isConsolidated());
  }

  @Test
  void testIsConsolidated_nullArchivePath_returnsFalse() {
    FileRecord file = new FileRecordBuilder().withArchivePath("").build();
    file.setArchivePath(null);

    assertFalse(file.isConsolidated());
  }

  @Test
  void testIsConsolidated_hasArchivePath_returnsTrue() {
    FileRecord file =
        new FileRecordBuilder()
            .withArchivePath("/clp-archives/spark/2024-01-01/app-001.clp")
            .build();

    assertTrue(file.isConsolidated());
  }

  // Dynamic dimension type-safe accessor tests

  @Test
  void testGetStringDimension_returnsValue() {
    FileRecord file = new FileRecord();
    file.setDimension("dim/str128/service_name", "my-service");

    assertEquals("my-service", file.getStringDimension("dim/str128/service_name"));
  }

  @Test
  void testGetStringDimension_wrongType_returnsNull() {
    FileRecord file = new FileRecord();
    file.setDimension("dim/str128/service_name", 12345L);

    assertNull(file.getStringDimension("dim/str128/service_name"));
  }

  @Test
  void testGetDimension_notSet_returnsNull() {
    FileRecord file = new FileRecord();

    assertNull(file.getDimension("dim/str128/nonexistent"));
    assertNull(file.getStringDimension("dim/str128/nonexistent"));
  }

  // Retention calculation tests

  @Test
  void testRetention_defaultThirtyDays() {
    long minTimestamp = 1704067200_000_000_000L; // 2024-01-01 00:00:00 UTC (nanos)

    FileRecord file =
        new FileRecordBuilder().withMinTimestamp(minTimestamp).withRetentionDays(30).build();

    long expectedExpiry = minTimestamp + (30 * Timestamps.NANOS_PER_DAY);
    assertEquals(30, file.getRetentionDays());
    assertEquals(expectedExpiry, file.getExpiresAt());
  }

  @Test
  void testRetention_customRetentionDays() {
    long minTimestamp = 1704067200_000_000_000L;
    int retentionDays = 90;

    FileRecord file =
        new FileRecordBuilder()
            .withMinTimestamp(minTimestamp)
            .withRetentionDays(retentionDays)
            .build();

    long expectedExpiry = minTimestamp + (retentionDays * Timestamps.NANOS_PER_DAY);
    assertEquals(retentionDays, file.getRetentionDays());
    assertEquals(expectedExpiry, file.getExpiresAt());
  }

  @Test
  void testRetention_zeroRetention() {
    long minTimestamp = 1704067200_000_000_000L;

    FileRecord file =
        new FileRecordBuilder().withMinTimestamp(minTimestamp).withRetentionDays(0).build();

    assertEquals(0, file.getRetentionDays());
    assertEquals(minTimestamp, file.getExpiresAt());
  }

  // Source partition tracking tests

  @Test
  void testSourcePartition_defaultNotSet() {
    FileRecord file = new FileRecord();
    assertEquals(-1, file.getSourcePartition());
    assertFalse(file.hasSourcePartition());
  }

  @Test
  void testSourcePartition_setAndGet() {
    FileRecord file = new FileRecord();
    file.setSourcePartition(3);
    assertEquals(3, file.getSourcePartition());
    assertTrue(file.hasSourcePartition());
  }

  @Test
  void testSourcePartition_zeroIsValid() {
    FileRecord file = new FileRecord();
    file.setSourcePartition(0);
    assertTrue(file.hasSourcePartition());
  }

  @Test
  void testBuilder_withSourcePartition() {
    FileRecord file = new FileRecordBuilder().withSourceOffset(42).withSourcePartition(2).build();

    assertEquals(42, file.getSourceOffset());
    assertTrue(file.hasSourceOffset());
    assertEquals(2, file.getSourcePartition());
    assertTrue(file.hasSourcePartition());
  }

  // getCompositeKeyString tests

  @Test
  void testGetCompositeKeyString_formatCorrect() {
    FileRecord file =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("ir-bucket")
            .withIrPath("/ir/path")
            .withArchiveStorageBackend("s3")
            .withArchiveBucket("archive-bucket")
            .withArchivePath("/archive/path")
            .build();

    String pk = file.getCompositeKeyString();
    assertEquals("s3|ir-bucket|/ir/path|s3|archive-bucket|/archive/path", pk);
  }

  @Test
  void testGetCompositeKeyString_emptyArchiveFields() {
    FileRecord file =
        new FileRecordBuilder()
            .withIrStorageBackend("s3")
            .withIrBucket("ir-bucket")
            .withIrPath("/ir/path")
            .build();

    String pk = file.getCompositeKeyString();
    assertEquals("s3|ir-bucket|/ir/path|||", pk);
  }

  // Float count accessor tests

  @Test
  void testSetFloatCount_getReturnsValue() {
    FileRecord file = new FileRecord();
    file.setFloatCount("agg_float_latency_avg", 1.23);

    assertEquals(1.23, file.getFloatCount("agg_float_latency_avg"));
  }

  @Test
  void testGetFloatCount_notSet_returnsNull() {
    FileRecord file = new FileRecord();

    assertNull(file.getFloatCount("agg_float_latency_avg"));
  }

  @Test
  void testSetFloatCount_nan_throws() {
    FileRecord file = new FileRecord();
    assertThrows(
        IllegalArgumentException.class,
        () -> file.setFloatCount("agg_float_latency_avg", Double.NaN));
  }

  @Test
  void testSetFloatCount_infinity_throws() {
    FileRecord file = new FileRecord();
    assertThrows(
        IllegalArgumentException.class,
        () -> file.setFloatCount("agg_float_latency_avg", Double.POSITIVE_INFINITY));
    assertThrows(
        IllegalArgumentException.class,
        () -> file.setFloatCount("agg_float_latency_avg", Double.NEGATIVE_INFINITY));
  }

  @Test
  void testGetFloatCounts_unmodifiable() {
    FileRecord file = new FileRecord();
    file.setFloatCount("agg_float_latency_avg", 1.5);

    assertThrows(
        UnsupportedOperationException.class,
        () -> file.getFloatCounts().put("agg_float_response_sum", 2.0));
  }

  @Test
  void testSetFloatCounts_bulkPopulatesMap() {
    FileRecord file = new FileRecord();
    file.setFloatCounts(
        java.util.Map.of(
            "agg_float_latency_avg", 1.23,
            "agg_float_response_sum", 4.56));

    assertEquals(1.23, file.getFloatCount("agg_float_latency_avg"));
    assertEquals(4.56, file.getFloatCount("agg_float_response_sum"));
    assertEquals(2, file.getFloatCounts().size());
  }

  @Test
  void testSetFloatCounts_nullIsNoop() {
    FileRecord file = new FileRecord();
    file.setFloatCount("agg_float_latency_avg", 1.0);
    file.setFloatCounts(null);

    assertEquals(1, file.getFloatCounts().size());
  }

  @Test
  void testFloatCounts_jacksonDeserialize_snakeCaseKey() throws Exception {
    String json =
        "{\"irPath\":\"/test.clp\",\"state\":\"IR_ARCHIVE_BUFFERING\","
            + "\"float_counts\":{\"agg_float_latency_avg\":1.23,\"agg_float_response_sum\":4.56}}";

    FileRecord file = new ObjectMapper().readValue(json, FileRecord.class);

    assertEquals(1.23, file.getFloatCount("agg_float_latency_avg"));
    assertEquals(4.56, file.getFloatCount("agg_float_response_sum"));
  }

  @Test
  void testFloatCounts_jacksonSerialize_snakeCaseKey() throws Exception {
    FileRecord file = new FileRecord();
    file.setFloatCount("agg_float_latency_avg", 1.23);

    String json = new ObjectMapper().writeValueAsString(file);

    assertTrue(json.contains("\"float_counts\""));
    assertTrue(json.contains("\"agg_float_latency_avg\":1.23"));
  }

  // Builder integration tests

  @Test
  void testBuilder_bufferingPreset_correctState() {
    FileRecord file = new FileRecordBuilder().buffering().build();

    assertEquals(FileState.IR_ARCHIVE_BUFFERING, file.getState());
    assertEquals("", file.getArchivePath());
    assertEquals(0L, file.getArchiveCreatedAt());
  }

  @Test
  void testBuilder_pendingPreset_correctState() {
    FileRecord file = new FileRecordBuilder().pending().build();

    assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, file.getState());
    assertEquals("", file.getArchivePath());
  }

  @Test
  void testBuilder_consolidatedPreset_hasArchiveInfo() {
    FileRecord file = new FileRecordBuilder().consolidated().build();

    assertEquals(FileState.ARCHIVE_CLOSED, file.getState());
    assertNotEquals("", file.getArchivePath());
    assertTrue(file.getArchiveCreatedAt() > 0);
    assertTrue(file.getArchiveSizeBytes() > 0);
    assertTrue(file.isConsolidated());
  }
}
