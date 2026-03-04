package com.yscope.metalog.metastore.schema;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.common.Timestamps;
import com.yscope.metalog.metastore.model.FileRecord;
import com.yscope.metalog.metastore.model.FileState;
import com.yscope.metalog.metastore.schema.RecordStateValidator.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Unit tests for RecordStateValidator. */
@DisplayName("RecordStateValidator")
class RecordStateValidatorTest {

  private RecordStateValidator validator;
  private long now;

  @BeforeEach
  void setUp() {
    validator = new RecordStateValidator();
    now = Timestamps.nowNanos();
  }

  // ========================================================================
  // Basic Validation Tests
  // ========================================================================

  @Nested
  @DisplayName("Basic Validation")
  class BasicValidation {

    @Test
    @DisplayName("null record returns error")
    void nullRecord_returnsError() {
      ValidationResult result = validator.validate(null);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("null")));
    }

    @Test
    @DisplayName("null state returns error")
    void nullState_returnsError() {
      FileRecord file = new FileRecord();
      file.setState(null);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("State is required")));
    }

    @Test
    @DisplayName("min_timestamp = 0 returns error")
    void zeroMinTimestamp_returnsError() {
      FileRecord file = createValidIrBufferingFile();
      file.setMinTimestamp(0);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("min_timestamp")));
    }
  }

  // ========================================================================
  // IR-Only State Tests
  // ========================================================================

  @Nested
  @DisplayName("IR-Only States")
  class IrOnlyStates {

    @Test
    @DisplayName("IR_BUFFERING valid with minimal fields")
    void irBuffering_validWithMinimalFields() {
      FileRecord file = createValidIrBufferingFile();

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("IR_BUFFERING missing ir_path returns error")
    void irBuffering_missingIrPath_returnsError() {
      FileRecord file = createValidIrBufferingFile();
      file.setIrPath("");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("ir_path")));
    }

    @Test
    @DisplayName("IR_BUFFERING with archive fields returns error")
    void irBuffering_withArchiveFields_returnsError() {
      FileRecord file = createValidIrBufferingFile();
      file.setArchivePath("/some/archive/path");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream().anyMatch(e -> e.contains("Archive fields must be empty")));
    }

    @Test
    @DisplayName("IR_CLOSED requires max_timestamp")
    void irClosed_requiresMaxTimestamp() {
      FileRecord file = createValidIrClosedFile();
      file.setMaxTimestamp(0);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("max_timestamp")));
    }

    @Test
    @DisplayName("IR_CLOSED valid with all required fields")
    void irClosed_validWithRequiredFields() {
      FileRecord file = createValidIrClosedFile();

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("IR_CLOSED with url backend and no bucket passes")
    void irClosed_urlBackend_noBucket_passes() {
      FileRecord file = new FileRecord();
      file.setState(FileState.IR_CLOSED);
      file.setMinTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
      file.setMaxTimestamp(now - 50 * Timestamps.NANOS_PER_SECOND);
      file.setRecordCount(1000);
      file.setIrSizeBytes(50000L);
      file.setIrStorageBackend("http");
      file.setIrPath("https://r2.yscope.io/prod/logging/cockroachdb/abc.clp.zst");

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }
  }

  // ========================================================================
  // Archive-Only State Tests
  // ========================================================================

  @Nested
  @DisplayName("Archive-Only States")
  class ArchiveOnlyStates {

    @Test
    @DisplayName("ARCHIVE_CLOSED valid with all required fields")
    void archiveClosed_validWithRequiredFields() {
      FileRecord file = createValidArchiveClosedFile();

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("ARCHIVE_CLOSED missing archive_path returns error")
    void archiveClosed_missingArchivePath_returnsError() {
      FileRecord file = createValidArchiveClosedFile();
      file.setArchivePath("");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("archive_path")));
    }

    @Test
    @DisplayName("ARCHIVE_CLOSED missing archive_created_at returns error")
    void archiveClosed_missingArchiveCreatedAt_returnsError() {
      FileRecord file = createValidArchiveClosedFile();
      file.setArchiveCreatedAt(0);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("archive_created_at")));
    }

    @Test
    @DisplayName("ARCHIVE_CLOSED with IR fields returns error")
    void archiveClosed_withIrFields_returnsError() {
      FileRecord file = createValidArchiveClosedFile();
      file.setIrPath("/some/ir/path");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("IR fields must be empty")));
    }
  }

  // ========================================================================
  // IR+Archive State Tests
  // ========================================================================

  @Nested
  @DisplayName("IR+Archive States")
  class IrArchiveStates {

    @Test
    @DisplayName("IR_ARCHIVE_BUFFERING valid with IR fields only")
    void irArchiveBuffering_validWithIrFields() {
      FileRecord file = createValidIrArchiveBufferingFile();

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("IR_ARCHIVE_CONSOLIDATION_PENDING requires max_timestamp")
    void consolidationPending_requiresMaxTimestamp() {
      FileRecord file = createValidConsolidationPendingFile();
      file.setMaxTimestamp(0);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("max_timestamp")));
    }
  }

  // ========================================================================
  // State Transition Tests
  // ========================================================================

  @Nested
  @DisplayName("State Transitions")
  class StateTransitions {

    @Test
    @DisplayName("same state is valid transition")
    void sameState_isValid() {
      assertTrue(validator.isValidTransition(FileState.IR_BUFFERING, FileState.IR_BUFFERING));
    }

    @Test
    @DisplayName("IR_BUFFERING -> IR_CLOSED is valid")
    void irBuffering_to_irClosed_isValid() {
      assertTrue(validator.isValidTransition(FileState.IR_BUFFERING, FileState.IR_CLOSED));
    }

    @Test
    @DisplayName("IR_CLOSED -> IR_PURGING is valid")
    void irClosed_to_irPurging_isValid() {
      assertTrue(validator.isValidTransition(FileState.IR_CLOSED, FileState.IR_PURGING));
    }

    @Test
    @DisplayName("IR_BUFFERING -> ARCHIVE_CLOSED is invalid")
    void irBuffering_to_archiveClosed_isInvalid() {
      assertFalse(validator.isValidTransition(FileState.IR_BUFFERING, FileState.ARCHIVE_CLOSED));
    }

    @Test
    @DisplayName("IR_PURGING is terminal (no valid transitions)")
    void irPurging_isTerminal() {
      assertFalse(validator.isValidTransition(FileState.IR_PURGING, FileState.IR_CLOSED));
      assertFalse(validator.isValidTransition(FileState.IR_PURGING, FileState.ARCHIVE_CLOSED));
    }

    @Test
    @DisplayName("ARCHIVE_CLOSED -> ARCHIVE_PURGING is valid")
    void archiveClosed_to_archivePurging_isValid() {
      assertTrue(validator.isValidTransition(FileState.ARCHIVE_CLOSED, FileState.ARCHIVE_PURGING));
    }

    @Test
    @DisplayName("IR+Archive consolidation flow is valid")
    void irArchive_consolidationFlow_isValid() {
      assertTrue(
          validator.isValidTransition(
              FileState.IR_ARCHIVE_BUFFERING, FileState.IR_ARCHIVE_CONSOLIDATION_PENDING));
      // PENDING transitions directly to ARCHIVE_CLOSED when consolidation completes
      assertTrue(
          validator.isValidTransition(
              FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, FileState.ARCHIVE_CLOSED));
    }

    @Test
    @DisplayName("validateTransition returns error message for invalid transition")
    void validateTransition_invalidTransition_returnsError() {
      ValidationResult result =
          validator.validateTransition(FileState.IR_BUFFERING, FileState.ARCHIVE_CLOSED);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().get(0).contains("Invalid state transition"));
      assertTrue(result.getErrors().get(0).contains("Valid targets"));
    }
  }

  // ========================================================================
  // Time Constraint Tests
  // ========================================================================

  @Nested
  @DisplayName("Time Constraints")
  class TimeConstraints {

    @Test
    @DisplayName("min_timestamp in future returns error")
    void minTimestampInFuture_returnsError() {
      FileRecord file = createValidIrBufferingFile();
      file.setMinTimestamp(now + Timestamps.NANOS_PER_HOUR); // 1 hour in future

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("future")));
    }

    @Test
    @DisplayName("max_timestamp < min_timestamp returns error")
    void maxLessThanMin_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setMinTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
      file.setMaxTimestamp(now - 200 * Timestamps.NANOS_PER_SECOND); // Earlier than min

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("max_timestamp must be >= min_timestamp")));
    }

    @Test
    @DisplayName("archive_created_at < max_timestamp returns error")
    void archiveCreatedBeforeMaxTimestamp_returnsError() {
      FileRecord file = createValidArchiveClosedFile();
      file.setMaxTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
      file.setArchiveCreatedAt(now - 200 * Timestamps.NANOS_PER_SECOND); // Before max_timestamp

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("archive_created_at must be >= max_timestamp")));
    }

    @Test
    @DisplayName("expires_at mismatch returns warning")
    void expiresAtMismatch_returnsWarning() {
      FileRecord file = createValidIrClosedFile();
      file.setRetentionDays(30);
      file.setExpiresAt(file.getMinTimestamp() + 1000); // Wrong value

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid()); // Warning, not error
      assertFalse(result.getWarnings().isEmpty());
      assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("expires_at")));
    }
  }

  // ========================================================================
  // Count Hierarchy Tests
  // ========================================================================

  @Nested
  @DisplayName("Count Hierarchy")
  class CountHierarchy {

    @Test
    @DisplayName("valid count hierarchy passes")
    void validCountHierarchy_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setRecordCount(100);
      file.setIntAgg("agg_int/gte/level/debug", 80);
      file.setIntAgg("agg_int/gte/level/info", 60);
      file.setIntAgg("agg_int/gte/level/warn", 40);
      file.setIntAgg("agg_int/gte/level/error", 20);
      file.setIntAgg("agg_int/gte/level/fatal", 5);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("record_count < count_debug_plus returns error")
    void recordCountLessThanDebug_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setRecordCount(50);
      file.setIntAgg("agg_int/gte/level/debug", 80); // More than record_count

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("record_count must be >= count_debug_plus")));
    }

    @Test
    @DisplayName("count_debug_plus < count_info_plus returns error")
    void debugLessThanInfo_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setRecordCount(100);
      file.setIntAgg("agg_int/gte/level/debug", 30);
      file.setIntAgg("agg_int/gte/level/info", 50); // More than debug

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("count_debug_plus must be >= count_info_plus")));
    }

    @Test
    @DisplayName("full hierarchy violation detected")
    void fullHierarchyViolation_detected() {
      FileRecord file = createValidIrClosedFile();
      file.setRecordCount(10);
      file.setIntAgg("agg_int/gte/level/debug", 20);
      file.setIntAgg("agg_int/gte/level/info", 30);
      file.setIntAgg("agg_int/gte/level/warn", 40);
      file.setIntAgg("agg_int/gte/level/error", 50);
      file.setIntAgg("agg_int/gte/level/fatal", 60);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertEquals(5, result.getErrors().stream().filter(e -> e.contains("must be >=")).count());
    }
  }

  // ========================================================================
  // Dimension Type Validation Tests
  // ========================================================================

  @Nested
  @DisplayName("Dimension Type Validation")
  class DimensionTypeValidation {

    @Test
    @DisplayName("valid string dimension passes")
    void validStringDimension_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/str128/service_name", "my-service");

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("string dimension exceeding max length returns error")
    void stringDimensionExceedsMaxLength_returnsError() {
      FileRecord file = createValidIrClosedFile();
      // Create a string longer than 10 chars for dim/str10/*
      file.setDimension("dim/str10/short", "this-is-way-too-long");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("exceeds max")));
    }

    @Test
    @DisplayName("string dimension with wrong type returns error")
    void stringDimensionWrongType_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/str128/service_name", 12345); // Number instead of String

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("expected String")));
    }

    @Test
    @DisplayName("valid uint32 dimension passes")
    void validUint32Dimension_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/uint32/count", 1000L);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("uint32 dimension negative value returns error")
    void uint32DimensionNegative_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/uint32/count", -1L);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("out of uint32 range")));
    }

    @Test
    @DisplayName("uint32 dimension overflow returns error")
    void uint32DimensionOverflow_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/uint32/count", 5000000000L); // > 4294967295

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("out of uint32 range")));
    }

    @Test
    @DisplayName("uint8 dimension valid range passes")
    void uint8DimensionValidRange_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/uint8/level", 255);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("uint8 dimension overflow returns error")
    void uint8DimensionOverflow_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/uint8/level", 256);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("out of uint8 range")));
    }

    @Test
    @DisplayName("int32 dimension valid negative value passes")
    void int32DimensionNegative_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/int32/offset", -1000);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("bool dimension accepts boolean")
    void boolDimensionAcceptsBoolean() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/bool/active", true);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("bool dimension accepts number (0/1)")
    void boolDimensionAcceptsNumber() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/bool/active", 1);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("null dimension value is allowed")
    void nullDimensionValue_isAllowed() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/str128/service_name", null);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("count column type validation works")
    void countColumnTypeValidation_works() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("agg_int/eq/custom/metric", 1000L);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("agg column with non-numeric value returns error")
    void aggColumnNonNumeric_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("agg_int/eq/custom/metric", "not-a-number");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("expected Number")));
    }

    @Test
    @DisplayName("utf8 string dimension validates length correctly")
    void utf8StringDimension_validatesLength() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/str10utf8/name", "hello"); // 5 chars, OK

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("utf8 string dimension exceeding length returns error")
    void utf8StringDimensionExceedsLength_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setDimension("dim/str5utf8/name", "hello-world"); // > 5 chars

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("exceeds max")));
    }
  }

  // ========================================================================
  // Path and Bucket Validation Tests
  // ========================================================================

  @Nested
  @DisplayName("Path and Bucket Validation")
  class PathAndBucketValidation {

    @Test
    @DisplayName("valid path length passes")
    void validPathLength_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrPath("/path/to/valid/file.ir");

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("path at max length (1024 bytes) passes")
    void pathAtMaxLength_passes() {
      FileRecord file = createValidIrClosedFile();
      // Create a path that's exactly 1024 bytes
      StringBuilder path = new StringBuilder("/");
      while (path.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 1024) {
        path.append("a");
      }
      // Trim to exactly 1024 bytes
      while (path.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024) {
        path.deleteCharAt(path.length() - 1);
      }
      file.setIrPath(path.toString());

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("path exceeding 1024 bytes returns error")
    void pathExceedsMaxLength_returnsError() {
      FileRecord file = createValidIrClosedFile();
      // Create a path that's 1025 bytes
      StringBuilder path = new StringBuilder("/");
      while (path.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 1024) {
        path.append("a");
      }
      file.setIrPath(path.toString());

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("exceeds max length") && e.contains("1024")));
    }

    @Test
    @DisplayName("path with multi-byte UTF-8 characters validates byte length")
    void pathWithMultiByteChars_validatesByteLength() {
      FileRecord file = createValidIrClosedFile();
      // Unicode chars take multiple bytes in UTF-8
      // '日' is 3 bytes in UTF-8, so 342 chars = 1026 bytes
      StringBuilder path = new StringBuilder("/");
      for (int i = 0; i < 342; i++) {
        path.append("日");
      }
      file.setIrPath(path.toString());

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("exceeds max length")));
    }

    @Test
    @DisplayName("valid S3 bucket name passes")
    void validS3BucketName_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("my-bucket-123");

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("bucket with periods passes")
    void bucketWithPeriods_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("my.bucket.name");

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("bucket with uppercase returns error")
    void bucketWithUppercase_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("MyBucket");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("invalid format")));
    }

    @Test
    @DisplayName("bucket with underscore returns error")
    void bucketWithUnderscore_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("my_bucket");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("invalid format")));
    }

    @Test
    @DisplayName("bucket too short returns error")
    void bucketTooShort_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("ab"); // Less than 3 chars

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("too short")));
    }

    @Test
    @DisplayName("bucket too long returns error")
    void bucketTooLong_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("a".repeat(64)); // More than 63 chars

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("too long")));
    }

    @Test
    @DisplayName("bucket starting with hyphen returns error")
    void bucketStartingWithHyphen_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("-my-bucket");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("invalid format")));
    }

    @Test
    @DisplayName("bucket with consecutive periods returns error")
    void bucketWithConsecutivePeriods_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("my..bucket");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("consecutive periods")));
    }

    @Test
    @DisplayName("bucket formatted as IP address returns error")
    void bucketAsIpAddress_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setIrBucket("192.168.1.1");

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("IP address")));
    }

    @Test
    @DisplayName("archive bucket validation works")
    void archiveBucketValidation_works() {
      FileRecord file = createValidArchiveClosedFile();
      file.setArchiveBucket("Invalid_Bucket"); // Uppercase and underscore

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("archive_bucket") && e.contains("invalid format")));
    }

    @Test
    @DisplayName("archive path length validation works")
    void archivePathLengthValidation_works() {
      FileRecord file = createValidArchiveClosedFile();
      StringBuilder path = new StringBuilder("/");
      while (path.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 1024) {
        path.append("a");
      }
      file.setArchivePath(path.toString());

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("archive_path") && e.contains("exceeds max length")));
    }
  }

  // ========================================================================
  // Tier 2: State Transition Edge Cases
  // ========================================================================

  @Nested
  @DisplayName("State Transition Edge Cases")
  class StateTransitionEdgeCases {

    @Test
    @DisplayName("validateTransition with null fromState returns error")
    void validateTransition_nullFromState_returnsError() {
      ValidationResult result = validator.validateTransition(null, FileState.IR_CLOSED);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("required")));
    }

    @Test
    @DisplayName("validateTransition with null toState returns error")
    void validateTransition_nullToState_returnsError() {
      ValidationResult result = validator.validateTransition(FileState.IR_BUFFERING, null);
      assertFalse(result.isValid());
      assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("required")));
    }

    @Test
    @DisplayName("validateTransition same state is valid (idempotent)")
    void validateTransition_sameState_isValid() {
      ValidationResult result =
          validator.validateTransition(FileState.IR_CLOSED, FileState.IR_CLOSED);
      assertTrue(result.isValid());
    }

    @Test
    @DisplayName("isValidTransition with null returns false")
    void isValidTransition_null_returnsFalse() {
      assertFalse(validator.isValidTransition(null, FileState.IR_CLOSED));
      assertFalse(validator.isValidTransition(FileState.IR_BUFFERING, null));
    }
  }

  // ========================================================================
  // Tier 2: Time Constraint Edge Cases
  // ========================================================================

  @Nested
  @DisplayName("Time Constraint Edge Cases")
  class TimeConstraintEdgeCases {

    @Test
    @DisplayName("max_timestamp in future returns error")
    void maxTimestampInFuture_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setMaxTimestamp(now + Timestamps.NANOS_PER_HOUR); // 1 hour in future

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("max_timestamp") && e.contains("future")));
    }

    @Test
    @DisplayName("archive_created_at in future returns error")
    void archiveCreatedAtInFuture_returnsError() {
      FileRecord file = createValidArchiveClosedFile();
      file.setArchiveCreatedAt(now + Timestamps.NANOS_PER_HOUR); // 1 hour in future

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("archive_created_at") && e.contains("future")));
    }

    @Test
    @DisplayName("retentionDays > 0 but expiresAt == 0 does not warn")
    void retentionDaysWithZeroExpiresAt_noWarning() {
      FileRecord file = createValidIrClosedFile();
      file.setRetentionDays(30);
      file.setExpiresAt(0); // Not set

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
      assertTrue(
          result.getWarnings().stream().noneMatch(w -> w.contains("expires_at")),
          "Should not warn when expiresAt is 0");
    }
  }

  // ========================================================================
  // Tier 2: Count Field Edge Cases
  // ========================================================================

  @Nested
  @DisplayName("Count Field Edge Cases")
  class CountFieldEdgeCases {

    @Test
    @DisplayName("negative record_count returns error")
    void negativeRecordCount_returnsError() {
      FileRecord file = createValidIrClosedFile();
      file.setRecordCount(-1);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream().anyMatch(e -> e.contains("non-negative")));
    }
  }

  // ========================================================================
  // Tier 2: Backend Exemptions
  // ========================================================================

  @Nested
  @DisplayName("Backend Exemptions")
  class BackendExemptions {

    @Test
    @DisplayName("http backend with empty ir_bucket passes (annotation-driven)")
    void httpBackend_emptyIrBucket_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrStorageBackend("http");
      file.setIrBucket(null);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("local backend with empty ir_bucket passes (annotation-driven)")
    void localBackend_emptyIrBucket_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrStorageBackend("local");
      file.setIrBucket(null);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("s3 backend with empty bucket fails")
    void s3Backend_emptyBucket_fails() {
      FileRecord file = createValidIrClosedFile();
      file.setIrStorageBackend("s3");
      file.setIrBucket(null);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream().anyMatch(e -> e.contains("ir_bucket is required")));
    }

    @Test
    @DisplayName("unknown backend with empty bucket passes (lenient)")
    void unknownBackend_emptyBucket_passes() {
      FileRecord file = createValidIrClosedFile();
      file.setIrStorageBackend("unknown-future-backend");
      file.setIrBucket(null);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }

    @Test
    @DisplayName("http archive backend with empty bucket passes")
    void httpArchiveBackend_emptyBucket_passes() {
      FileRecord file = createValidArchiveClosedFile();
      file.setArchiveStorageBackend("http");
      file.setArchiveBucket(null);

      ValidationResult result = validator.validate(file);
      assertTrue(result.isValid(), "Expected valid but got: " + result);
    }
  }

  // ========================================================================
  // Tier 2: Mutual Exclusivity Edge Cases
  // ========================================================================

  @Nested
  @DisplayName("Mutual Exclusivity Edge Cases")
  class MutualExclusivityEdgeCases {

    @Test
    @DisplayName("IR-only state with archive_created_at > 0 returns error")
    void irOnlyState_withArchiveCreatedAt_returnsError() {
      FileRecord file = createValidIrBufferingFile();
      file.setArchiveCreatedAt(now - 10 * Timestamps.NANOS_PER_SECOND);

      ValidationResult result = validator.validate(file);
      assertFalse(result.isValid());
      assertTrue(
          result.getErrors().stream()
              .anyMatch(e -> e.contains("archive_created_at must be 0")));
    }
  }

  // ========================================================================
  // All States Coverage Test
  // ========================================================================

  @ParameterizedTest
  @EnumSource(FileState.class)
  @DisplayName("All states can be validated without exception")
  void allStates_canBeValidated(FileState state) {
    FileRecord file = createFileForState(state);
    assertDoesNotThrow(() -> validator.validate(file));
  }

  // ========================================================================
  // Helper Methods
  // ========================================================================

  private FileRecord createValidIrBufferingFile() {
    FileRecord file = new FileRecord();
    file.setState(FileState.IR_BUFFERING);
    file.setMinTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
    file.setIrStorageBackend("s3");
    file.setIrBucket("test-bucket");
    file.setIrPath("/path/to/ir/file.ir");
    return file;
  }

  private FileRecord createValidIrClosedFile() {
    FileRecord file = createValidIrBufferingFile();
    file.setState(FileState.IR_CLOSED);
    file.setMaxTimestamp(now - 50 * Timestamps.NANOS_PER_SECOND);
    file.setRecordCount(1000);
    file.setIrSizeBytes(50000L);
    return file;
  }

  private FileRecord createValidArchiveClosedFile() {
    FileRecord file = new FileRecord();
    file.setState(FileState.ARCHIVE_CLOSED);
    file.setMinTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
    file.setMaxTimestamp(now - 50 * Timestamps.NANOS_PER_SECOND);
    file.setArchiveStorageBackend("s3");
    file.setArchiveBucket("archive-bucket");
    file.setArchivePath("/path/to/archive.clp");
    file.setArchiveCreatedAt(now - 10 * Timestamps.NANOS_PER_SECOND);
    file.setRecordCount(5000);
    file.setArchiveSizeBytes(100000L);
    return file;
  }

  private FileRecord createValidIrArchiveBufferingFile() {
    FileRecord file = new FileRecord();
    file.setState(FileState.IR_ARCHIVE_BUFFERING);
    file.setMinTimestamp(now - 100 * Timestamps.NANOS_PER_SECOND);
    file.setIrStorageBackend("s3");
    file.setIrBucket("test-bucket");
    file.setIrPath("/path/to/ir/file.ir");
    return file;
  }

  private FileRecord createValidConsolidationPendingFile() {
    FileRecord file = createValidIrArchiveBufferingFile();
    file.setState(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING);
    file.setMaxTimestamp(now - 50 * Timestamps.NANOS_PER_SECOND);
    file.setRecordCount(1000);
    file.setIrSizeBytes(50000L);
    return file;
  }

  private FileRecord createFileForState(FileState state) {
    switch (state) {
      case IR_BUFFERING:
        return createValidIrBufferingFile();
      case IR_CLOSED:
      case IR_PURGING:
        FileRecord irClosed = createValidIrClosedFile();
        irClosed.setState(state);
        return irClosed;
      case ARCHIVE_CLOSED:
      case ARCHIVE_PURGING:
        FileRecord archiveClosed = createValidArchiveClosedFile();
        archiveClosed.setState(state);
        return archiveClosed;
      case IR_ARCHIVE_BUFFERING:
        return createValidIrArchiveBufferingFile();
      case IR_ARCHIVE_CONSOLIDATION_PENDING:
        return createValidConsolidationPendingFile();
      default:
        throw new IllegalArgumentException("Unknown state: " + state);
    }
  }
}
