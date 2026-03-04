package com.yscope.clp.service.metastore.schema;

import com.yscope.clp.service.common.Timestamps;
import com.yscope.clp.service.metastore.Columns;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.FileState;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates FileRecord records based on their lifecycle state.
 *
 * <p>Enforces:
 *
 * <ul>
 *   <li>Required fields per state
 *   <li>Valid state transitions (delegates to {@link FileState#canTransitionTo})
 *   <li>Time constraints (min/max/archive_created)
 *   <li>Count hierarchy (cumulative levels)
 *   <li>Mutual exclusivity (IR-only vs Archive-only)
 * </ul>
 *
 * <p>Based on: docs/concepts/metadata-schema.md
 */
public class RecordStateValidator {
  private static final Logger logger = LoggerFactory.getLogger(RecordStateValidator.class);

  // Tolerance for "not in future" check (5 minutes)
  private static final long FUTURE_TOLERANCE_NANOS = 300 * Timestamps.NANOS_PER_SECOND;

  // Path and bucket constraints (per S3 specification)
  // Object key: max 1024 bytes UTF-8 encoded
  private static final int MAX_PATH_BYTES = 1024;
  // Bucket names: lowercase letters, numbers, periods, hyphens only
  // Length 3-63, must start/end with letter or number, no consecutive periods
  private static final Pattern S3_BUCKET_PATTERN =
      Pattern.compile("^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$");
  private static final int MIN_BUCKET_LENGTH = 3;
  private static final int MAX_BUCKET_LENGTH = 63;
  // IP address pattern to reject
  private static final Pattern IP_ADDRESS_PATTERN =
      Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

  // Patterns for extracting type from dimension/aggregation keys.
  // Slash-format dimension: dim/{type}/{field} e.g., dim/str128/application_id, dim/bool/active
  private static final Pattern DIMENSION_PATTERN =
      Pattern.compile("^dim/(str\\d+(?:utf8)?|bool|u?int(?:8|16|32|64)|float(?:32|64))/(.+)$");
  // Slash-format aggregation: agg_(int|float)/{aggType}/{field}/{qualifier}
  private static final Pattern AGG_PATTERN =
      Pattern.compile("^agg_(int|float)/(\\w+)/(.+)$");
  // String type pattern: str{N} or str{N}utf8
  private static final Pattern STRING_TYPE_PATTERN = Pattern.compile("^str(\\d+)(utf8)?$");

  // Type ranges for validation
  private static final long UINT8_MAX = 255L;
  private static final long UINT16_MAX = 65535L;
  private static final long UINT32_MAX = 4294967295L;
  private static final long INT8_MIN = -128L;
  private static final long INT8_MAX = 127L;
  private static final long INT16_MIN = -32768L;
  private static final long INT16_MAX = 32767L;
  private static final long INT32_MIN = -2147483648L;
  private static final long INT32_MAX = 2147483647L;

  /** Validation result containing all errors found. */
  public static class ValidationResult {
    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();

    public void addError(String error) {
      errors.add(error);
    }

    public void addWarning(String warning) {
      warnings.add(warning);
    }

    public boolean isValid() {
      return errors.isEmpty();
    }

    public List<String> getErrors() {
      return List.copyOf(errors);
    }

    public List<String> getWarnings() {
      return List.copyOf(warnings);
    }

    @Override
    public String toString() {
      if (isValid() && warnings.isEmpty()) {
        return "ValidationResult: VALID";
      }
      StringBuilder sb = new StringBuilder();
      if (!errors.isEmpty()) {
        sb.append("Errors: ").append(errors);
      }
      if (!warnings.isEmpty()) {
        if (sb.length() > 0) sb.append("; ");
        sb.append("Warnings: ").append(warnings);
      }
      return sb.toString();
    }
  }

  /**
   * Validate a record for insert/update.
   *
   * @param file The FileRecord record to validate
   * @return ValidationResult with any errors/warnings
   */
  public ValidationResult validate(FileRecord file) {
    ValidationResult result = new ValidationResult();

    if (file == null) {
      result.addError("Record cannot be null");
      return result;
    }

    if (file.getState() == null) {
      result.addError("State is required");
      return result;
    }

    // Validate based on state
    validateRequiredFields(file, result);
    validateMutualExclusivity(file, result);
    validateTimeConstraints(file, result);
    validateCountHierarchy(file, result);
    validateDimensionTypes(file, result);

    return result;
  }

  /**
   * Validate a state transition.
   *
   * @param fromState Current state
   * @param toState Target state
   * @return ValidationResult with any errors
   */
  public ValidationResult validateTransition(FileState fromState, FileState toState) {
    ValidationResult result = new ValidationResult();

    if (fromState == null || toState == null) {
      result.addError("Both fromState and toState are required");
      return result;
    }

    if (fromState == toState) {
      // Same state is always valid (idempotent update)
      return result;
    }

    // Delegate to FileState's built-in transition validation
    if (!fromState.canTransitionTo(toState)) {
      result.addError(
          String.format(
              "Invalid state transition: %s -> %s. Valid targets: %s",
              fromState, toState, fromState.getValidTransitions()));
    }

    return result;
  }

  /** Check if a state transition is valid. */
  public boolean isValidTransition(FileState fromState, FileState toState) {
    if (fromState == null || toState == null) {
      return false;
    }
    if (fromState == toState) {
      return true;
    }
    return fromState.canTransitionTo(toState);
  }

  // ========================================================================
  // Required Fields Validation
  // ========================================================================

  private void validateRequiredFields(FileRecord file, ValidationResult result) {
    FileState state = file.getState();

    // Always required
    if (file.getMinTimestamp() <= 0) {
      result.addError("min_timestamp is required and must be > 0");
    }

    switch (state) {
      case IR_BUFFERING:
        validateIrFieldsRequired(file, result);
        // max_timestamp, sizes, counts optional during buffering
        break;

      case IR_CLOSED:
      case IR_PURGING:
        validateIrFieldsRequired(file, result);
        validateClosedStateFields(file, result, "IR");
        break;

      case ARCHIVE_CLOSED:
      case ARCHIVE_PURGING:
        validateArchiveFieldsRequired(file, result);
        validateClosedStateFields(file, result, "Archive");
        if (file.getArchiveCreatedAt() <= 0) {
          result.addError("archive_created_at is required for " + state);
        }
        break;

      case IR_ARCHIVE_BUFFERING:
        validateIrFieldsRequired(file, result);
        // max_timestamp, sizes, counts optional during buffering
        break;

      case IR_ARCHIVE_CONSOLIDATION_PENDING:
        validateIrFieldsRequired(file, result);
        validateClosedStateFields(file, result, "IR");
        break;
    }
  }

  private void validateIrFieldsRequired(FileRecord file, ValidationResult result) {
    if (isEmpty(file.getIrStorageBackend())) {
      result.addError("ir_storage_backend is required for IR states");
    }
    // ir_bucket is required unless storage backend is "http" or "local" where bucket
    // concept doesn't apply
    if (isEmpty(file.getIrBucket())
        && !"http".equals(file.getIrStorageBackend())
        && !"local".equals(file.getIrStorageBackend())) {
      result.addError(
          "ir_bucket is required for IR states (except for 'http' and 'local' backends)");
    } else if (!isEmpty(file.getIrBucket())) {
      validateBucketName(file.getIrBucket(), "ir_bucket", result);
    }
    if (isEmpty(file.getIrPath())) {
      result.addError("ir_path is required for IR states");
    } else {
      validatePathLength(file.getIrPath(), "ir_path", result);
    }
  }

  private void validateArchiveFieldsRequired(FileRecord file, ValidationResult result) {
    if (isEmpty(file.getArchiveStorageBackend())) {
      result.addError("archive_storage_backend is required for Archive states");
    }
    if (isEmpty(file.getArchiveBucket())
        && !"http".equals(file.getArchiveStorageBackend())
        && !"local".equals(file.getArchiveStorageBackend())) {
      result.addError(
          "archive_bucket is required for Archive states (except for 'http' and 'local' backends)");
    } else if (!isEmpty(file.getArchiveBucket())) {
      validateBucketName(file.getArchiveBucket(), "archive_bucket", result);
    }
    if (isEmpty(file.getArchivePath())) {
      result.addError("archive_path is required for Archive states");
    } else {
      validatePathLength(file.getArchivePath(), "archive_path", result);
    }
  }

  private void validateClosedStateFields(
      FileRecord file, ValidationResult result, String fileType) {
    if (file.getMaxTimestamp() <= 0) {
      result.addError("max_timestamp is required for closed " + fileType + " states");
    }
    if (file.getRecordCount() <= 0) {
      result.addWarning("record_count should be > 0 for closed states");
    }

    // Size validation based on file type
    if (fileType.equals("IR") || fileType.equals("Both")) {
      if (file.getIrSizeBytes() == null || file.getIrSizeBytes() <= 0) {
        result.addWarning("ir_size_bytes should be > 0 for closed IR states");
      }
    }
    if (fileType.equals("Archive") || fileType.equals("Both")) {
      if (file.getArchiveSizeBytes() == null || file.getArchiveSizeBytes() <= 0) {
        result.addWarning("archive_size_bytes should be > 0 for closed Archive states");
      }
    }
  }

  // ========================================================================
  // Mutual Exclusivity Validation
  // ========================================================================

  private void validateMutualExclusivity(FileRecord file, ValidationResult result) {
    FileState state = file.getState();

    // IR-only states should not have archive fields
    if (state.isIROnly()) {
      if (!isEmpty(file.getArchiveStorageBackend())
          || !isEmpty(file.getArchiveBucket())
          || !isEmpty(file.getArchivePath())) {
        result.addError("Archive fields must be empty for IR-only state: " + state);
      }
      if (file.getArchiveCreatedAt() > 0) {
        result.addError("archive_created_at must be 0 for IR-only state: " + state);
      }
    }

    // Archive-only states should not have IR fields
    if (state.isArchiveOnly()) {
      if (!isEmpty(file.getIrStorageBackend())
          || !isEmpty(file.getIrBucket())
          || !isEmpty(file.getIrPath())) {
        result.addError("IR fields must be empty for Archive-only state: " + state);
      }
    }
  }

  // ========================================================================
  // Time Constraints Validation
  // ========================================================================

  private void validateTimeConstraints(FileRecord file, ValidationResult result) {
    long now = Timestamps.nowNanos();
    long minTs = file.getMinTimestamp();
    long maxTs = file.getMaxTimestamp();
    long archiveCreatedAt = file.getArchiveCreatedAt();

    // min_timestamp not in future
    if (minTs > now + FUTURE_TOLERANCE_NANOS) {
      result.addError("min_timestamp cannot be in the future");
    }

    // max_timestamp >= min_timestamp (when set)
    if (maxTs > 0 && maxTs < minTs) {
      result.addError("max_timestamp must be >= min_timestamp");
    }

    // max_timestamp not in future
    if (maxTs > now + FUTURE_TOLERANCE_NANOS) {
      result.addError("max_timestamp cannot be in the future");
    }

    // archive_created_at >= max_timestamp (archive created after last event)
    if (archiveCreatedAt > 0 && maxTs > 0 && archiveCreatedAt < maxTs) {
      result.addError(
          "archive_created_at must be >= max_timestamp (archive created after last event)");
    }

    // archive_created_at not in future
    if (archiveCreatedAt > now + FUTURE_TOLERANCE_NANOS) {
      result.addError("archive_created_at cannot be in the future");
    }

    // expires_at consistency (if retention configured)
    if (file.getRetentionDays() > 0 && file.getExpiresAt() > 0) {
      long expectedExpiresAt = minTs + (file.getRetentionDays() * Timestamps.NANOS_PER_DAY);
      if (file.getExpiresAt() != expectedExpiresAt) {
        result.addWarning(
            String.format(
                "expires_at (%d) does not match min_timestamp + retention_days (%d)",
                file.getExpiresAt(), expectedExpiresAt));
      }
    }
  }

  // ========================================================================
  // Count Hierarchy Validation
  // ========================================================================

  private void validateCountHierarchy(FileRecord file, ValidationResult result) {
    long recordCount = file.getRecordCount();
    long debugPlus = file.getIntAggOrZero(Columns.AGG_INT_GTE_LEVEL_DEBUG);
    long infoPlus = file.getIntAggOrZero(Columns.AGG_INT_GTE_LEVEL_INFO);
    long warnPlus = file.getIntAggOrZero(Columns.AGG_INT_GTE_LEVEL_WARN);
    long errorPlus = file.getIntAggOrZero(Columns.AGG_INT_GTE_LEVEL_ERROR);
    long fatalPlus = file.getIntAggOrZero(Columns.AGG_INT_GTE_LEVEL_FATAL);

    // Validate cumulative hierarchy
    // record_count >= debug_plus >= info_plus >= warn_plus >= error_plus >= fatal_plus
    if (recordCount < debugPlus) {
      result.addError("record_count must be >= count_debug_plus");
    }
    if (debugPlus < infoPlus) {
      result.addError("count_debug_plus must be >= count_info_plus");
    }
    if (infoPlus < warnPlus) {
      result.addError("count_info_plus must be >= count_warn_plus");
    }
    if (warnPlus < errorPlus) {
      result.addError("count_warn_plus must be >= count_error_plus");
    }
    if (errorPlus < fatalPlus) {
      result.addError("count_error_plus must be >= count_fatal_plus");
    }

    // Non-negative
    if (recordCount < 0
        || debugPlus < 0
        || infoPlus < 0
        || warnPlus < 0
        || errorPlus < 0
        || fatalPlus < 0) {
      result.addError("Count values must be non-negative");
    }
  }

  // ========================================================================
  // Dimension and Count Type Validation
  // ========================================================================

  private void validateDimensionTypes(FileRecord file, ValidationResult result) {
    Map<String, Object> dimensions = file.getDimensions();
    if (dimensions == null || dimensions.isEmpty()) {
      return;
    }

    for (Map.Entry<String, Object> entry : dimensions.entrySet()) {
      String columnName = entry.getKey();
      Object value = entry.getValue();

      if (value == null) {
        continue; // Null values are allowed (nullable columns)
      }

      // Check if it's a dimension column
      Matcher dimMatcher = DIMENSION_PATTERN.matcher(columnName);
      if (dimMatcher.matches()) {
        String typeSpec = dimMatcher.group(1);
        validateValueType(columnName, typeSpec, value, result);
        continue;
      }

      // Check if it's an aggregation key (agg_int/... or agg_float/...)
      Matcher aggMatcher = AGG_PATTERN.matcher(columnName);
      if (aggMatcher.matches()) {
        String valueType = aggMatcher.group(1); // "int" or "float"
        // Aggregation values are always numeric; skip detailed type range validation
        if (!(value instanceof Number)) {
          result.addError(
              String.format(
                  "Column %s: expected Number but got %s",
                  columnName, value.getClass().getSimpleName()));
        }
      }
    }
  }

  /** Validate that a value matches the expected type specification. */
  private void validateValueType(
      String columnName, String typeSpec, Object value, ValidationResult result) {
    // String types: str{N} or str{N}utf8
    Matcher strMatcher = STRING_TYPE_PATTERN.matcher(typeSpec);
    if (strMatcher.matches()) {
      int maxLength = Integer.parseInt(strMatcher.group(1));
      if (!(value instanceof String)) {
        result.addError(
            String.format(
                "Column %s: expected String but got %s",
                columnName, value.getClass().getSimpleName()));
        return;
      }
      String strValue = (String) value;
      if (strValue.length() > maxLength) {
        result.addError(
            String.format(
                "Column %s: string length %d exceeds max %d",
                columnName, strValue.length(), maxLength));
      }
      return;
    }

    // Boolean type
    if (typeSpec.equals("bool")) {
      if (!(value instanceof Boolean) && !(value instanceof Number)) {
        result.addError(
            String.format(
                "Column %s: expected Boolean or Number but got %s",
                columnName, value.getClass().getSimpleName()));
      }
      return;
    }

    // Numeric types
    if (!(value instanceof Number)) {
      result.addError(
          String.format(
              "Column %s: expected Number but got %s",
              columnName, value.getClass().getSimpleName()));
      return;
    }

    long numValue = ((Number) value).longValue();
    validateNumericRange(columnName, typeSpec, numValue, result);
  }

  /** Validate numeric value is within the range for its type. */
  private void validateNumericRange(
      String columnName, String typeSpec, long value, ValidationResult result) {
    switch (typeSpec) {
      case "uint8":
        if (value < 0 || value > UINT8_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of uint8 range [0, %d]", columnName, value, UINT8_MAX));
        }
        break;
      case "uint16":
        if (value < 0 || value > UINT16_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of uint16 range [0, %d]",
                  columnName, value, UINT16_MAX));
        }
        break;
      case "uint32":
        if (value < 0 || value > UINT32_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of uint32 range [0, %d]",
                  columnName, value, UINT32_MAX));
        }
        break;
      case "int8":
        if (value < INT8_MIN || value > INT8_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of int8 range [%d, %d]",
                  columnName, value, INT8_MIN, INT8_MAX));
        }
        break;
      case "int16":
        if (value < INT16_MIN || value > INT16_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of int16 range [%d, %d]",
                  columnName, value, INT16_MIN, INT16_MAX));
        }
        break;
      case "int32":
        if (value < INT32_MIN || value > INT32_MAX) {
          result.addError(
              String.format(
                  "Column %s: value %d out of int32 range [%d, %d]",
                  columnName, value, INT32_MIN, INT32_MAX));
        }
        break;
      case "int64":
        // Long range - always valid for a long value
        break;
      case "float32":
      case "float64":
        // Float/double - numeric value is acceptable
        break;
      default:
        result.addWarning(
            String.format(
                "Column %s: unknown type spec '%s', skipping validation", columnName, typeSpec));
    }
  }

  // ========================================================================
  // Path and Bucket Validation
  // ========================================================================

  /**
   * Validate path length does not exceed max bytes. Uses UTF-8 encoding for byte length
   * calculation.
   */
  private void validatePathLength(String path, String fieldName, ValidationResult result) {
    if (path == null) {
      return;
    }
    int byteLength = path.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    if (byteLength > MAX_PATH_BYTES) {
      result.addError(
          String.format(
              "%s exceeds max length: %d bytes (max: %d bytes)",
              fieldName, byteLength, MAX_PATH_BYTES));
    }
  }

  /**
   * Validate bucket name follows S3 naming rules. - Only lowercase letters (a-z), numbers (0-9),
   * periods (.), hyphens (-) - Length 3-63 characters - Must start and end with letter or number -
   * No consecutive periods - Not formatted as IP address
   */
  private void validateBucketName(String bucket, String fieldName, ValidationResult result) {
    if (bucket == null) {
      return;
    }

    // Length check
    if (bucket.length() < MIN_BUCKET_LENGTH) {
      result.addError(
          String.format(
              "%s too short: %d chars (min: %d)", fieldName, bucket.length(), MIN_BUCKET_LENGTH));
      return;
    }
    if (bucket.length() > MAX_BUCKET_LENGTH) {
      result.addError(
          String.format(
              "%s too long: %d chars (max: %d)", fieldName, bucket.length(), MAX_BUCKET_LENGTH));
      return;
    }

    // Character and format check
    if (!S3_BUCKET_PATTERN.matcher(bucket).matches()) {
      result.addError(
          String.format(
              "%s invalid format: must use only lowercase a-z, 0-9, '.', '-' "
                  + "and start/end with letter or number",
              fieldName));
      return;
    }

    // No consecutive periods
    if (bucket.contains("..")) {
      result.addError(String.format("%s cannot contain consecutive periods", fieldName));
      return;
    }

    // Not IP address format
    if (IP_ADDRESS_PATTERN.matcher(bucket).matches()) {
      result.addError(String.format("%s cannot be formatted as IP address", fieldName));
    }
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private boolean isEmpty(String value) {
    return value == null || value.isEmpty();
  }
}
