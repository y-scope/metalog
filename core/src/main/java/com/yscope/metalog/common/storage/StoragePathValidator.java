package com.yscope.metalog.common.storage;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Validates S3/MinIO bucket names and object keys.
 *
 * <p>Bucket naming rules (S3/GCS/MinIO):
 *
 * <ul>
 *   <li>3-63 characters long
 *   <li>Lowercase letters, numbers, and hyphens only
 *   <li>Must start and end with letter or number
 *   <li>Cannot be formatted as IP address
 *   <li>Cannot contain consecutive periods or adjacent period and hyphen
 * </ul>
 *
 * <p>Object key rules (S3):
 *
 * <ul>
 *   <li>Max 1024 bytes (UTF-8 encoded)
 *   <li>Safe characters: alphanumeric, /, !, -, _, ., *, ', (, )
 *   <li>Cannot be empty
 *   <li>Cannot start with /
 * </ul>
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">S3
 *     Bucket Naming Rules</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html">S3 Object
 *     Keys</a>
 */
public final class StoragePathValidator {

  // Bucket name constraints
  public static final int BUCKET_MIN_LENGTH = 3;
  public static final int BUCKET_MAX_LENGTH = 63;

  // Object key constraints
  public static final int KEY_MAX_BYTES = 1024;

  // Bucket name pattern: lowercase letters, numbers, hyphens
  // Must start and end with alphanumeric
  private static final Pattern BUCKET_PATTERN =
      Pattern.compile("^[a-z0-9][a-z0-9\\-]{1,61}[a-z0-9]$");

  // Single character bucket (edge case for length 3)
  private static final Pattern BUCKET_SHORT_PATTERN = Pattern.compile("^[a-z0-9]{3}$");

  // IP address pattern (not allowed as bucket name)
  private static final Pattern IP_ADDRESS_PATTERN =
      Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

  // Disallowed patterns in bucket names
  private static final Pattern CONSECUTIVE_PERIODS = Pattern.compile("\\.\\.");
  private static final Pattern PERIOD_HYPHEN = Pattern.compile("\\.-|-\\.");

  private StoragePathValidator() {
    // Utility class
  }

  /**
   * Validate a bucket name.
   *
   * @param bucket The bucket name to validate
   * @return ValidationResult with success/failure and error message
   */
  public static ValidationResult validateBucket(String bucket) {
    if (bucket == null) {
      return ValidationResult.failure("Bucket name cannot be null");
    }

    if (bucket.isEmpty()) {
      return ValidationResult.failure("Bucket name cannot be empty");
    }

    int length = bucket.length();
    if (length < BUCKET_MIN_LENGTH) {
      return ValidationResult.failure(
          String.format("Bucket name too short: %d chars (min %d)", length, BUCKET_MIN_LENGTH));
    }

    if (length > BUCKET_MAX_LENGTH) {
      return ValidationResult.failure(
          String.format("Bucket name too long: %d chars (max %d)", length, BUCKET_MAX_LENGTH));
    }

    // Check for uppercase (common mistake)
    if (!bucket.equals(bucket.toLowerCase())) {
      return ValidationResult.failure("Bucket name must be lowercase: " + bucket);
    }

    // Check pattern
    boolean matchesPattern =
        (length == 3 && BUCKET_SHORT_PATTERN.matcher(bucket).matches())
            || (length > 3 && BUCKET_PATTERN.matcher(bucket).matches());

    if (!matchesPattern) {
      return ValidationResult.failure(
          "Bucket name contains invalid characters or format: "
              + bucket
              + " (must be lowercase alphanumeric and hyphens, start/end with alphanumeric)");
    }

    // Check for IP address format
    if (IP_ADDRESS_PATTERN.matcher(bucket).matches()) {
      return ValidationResult.failure("Bucket name cannot be formatted as IP address: " + bucket);
    }

    // Check for consecutive periods
    if (CONSECUTIVE_PERIODS.matcher(bucket).find()) {
      return ValidationResult.failure("Bucket name cannot contain consecutive periods: " + bucket);
    }

    // Check for period-hyphen adjacency
    if (PERIOD_HYPHEN.matcher(bucket).find()) {
      return ValidationResult.failure(
          "Bucket name cannot have adjacent period and hyphen: " + bucket);
    }

    return ValidationResult.success();
  }

  /**
   * Validate an object key (path).
   *
   * @param key The object key to validate
   * @return ValidationResult with success/failure and error message
   */
  public static ValidationResult validateObjectKey(String key) {
    if (key == null) {
      return ValidationResult.failure("Object key cannot be null");
    }

    if (key.isEmpty()) {
      return ValidationResult.failure("Object key cannot be empty");
    }

    // Check byte length (UTF-8)
    int byteLength = key.getBytes(StandardCharsets.UTF_8).length;
    if (byteLength > KEY_MAX_BYTES) {
      return ValidationResult.failure(
          String.format("Object key too long: %d bytes (max %d)", byteLength, KEY_MAX_BYTES));
    }

    // S3 keys should not start with /
    if (key.startsWith("/")) {
      return ValidationResult.failure("Object key should not start with '/': " + key);
    }

    // Check for problematic characters that could cause issues
    // Note: S3 allows most characters but these can cause problems
    if (key.contains("\\")) {
      return ValidationResult.failure("Object key should not contain backslash: " + key);
    }

    // Check for null bytes
    if (key.contains("\0")) {
      return ValidationResult.failure("Object key cannot contain null bytes");
    }

    // Check for control characters (0x00-0x1F except tab/newline which are rare but valid)
    for (char c : key.toCharArray()) {
      if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') {
        return ValidationResult.failure(
            String.format("Object key contains invalid control character: 0x%02X", (int) c));
      }
    }

    return ValidationResult.success();
  }

  /**
   * Validate both bucket and key together.
   *
   * @param bucket The bucket name
   * @param key The object key
   * @return ValidationResult with combined validation
   */
  public static ValidationResult validatePath(String bucket, String key) {
    ValidationResult bucketResult = validateBucket(bucket);
    if (!bucketResult.isValid()) {
      return bucketResult;
    }

    return validateObjectKey(key);
  }

  /**
   * Validate a bucket name and throw if invalid.
   *
   * @param bucket The bucket name to validate
   * @throws IllegalArgumentException if validation fails
   */
  public static void requireValidBucket(String bucket) {
    ValidationResult result = validateBucket(bucket);
    if (!result.isValid()) {
      throw new IllegalArgumentException(result.getErrorMessage());
    }
  }

  /**
   * Validate an object key and throw if invalid.
   *
   * @param key The object key to validate
   * @throws IllegalArgumentException if validation fails
   */
  public static void requireValidObjectKey(String key) {
    ValidationResult result = validateObjectKey(key);
    if (!result.isValid()) {
      throw new IllegalArgumentException(result.getErrorMessage());
    }
  }

  /**
   * Validate both bucket and key, throw if either is invalid.
   *
   * @param bucket The bucket name
   * @param key The object key
   * @throws IllegalArgumentException if validation fails
   */
  public static void requireValidPath(String bucket, String key) {
    requireValidBucket(bucket);
    requireValidObjectKey(key);
  }

  /** Result of a validation check. */
  public static final class ValidationResult {
    private final boolean valid;
    private final String errorMessage;

    private ValidationResult(boolean valid, String errorMessage) {
      this.valid = valid;
      this.errorMessage = errorMessage;
    }

    public static ValidationResult success() {
      return new ValidationResult(true, null);
    }

    public static ValidationResult failure(String message) {
      return new ValidationResult(false, message);
    }

    public boolean isValid() {
      return valid;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public String toString() {
      return valid ? "Valid" : "Invalid: " + errorMessage;
    }
  }
}
