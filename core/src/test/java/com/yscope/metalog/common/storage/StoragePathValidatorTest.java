package com.yscope.metalog.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for StoragePathValidator. */
class StoragePathValidatorTest {

  @Nested
  class BucketValidation {

    @ParameterizedTest
    @ValueSource(
        strings = {
          "my-bucket",
          "mybucket",
          "my-bucket-123",
          "123-bucket",
          "abc",
          "a1b",
          "bucket-with-many-hyphens-in-name",
          "logs",
          "clp-archives"
        })
    void validBucketNames(String bucket) {
      var result = StoragePathValidator.validateBucket(bucket);
      assertTrue(result.isValid(), "Expected valid: " + bucket + ", got: " + result);
    }

    @Test
    void nullBucket_invalid() {
      var result = StoragePathValidator.validateBucket(null);
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("null"));
    }

    @Test
    void emptyBucket_invalid() {
      var result = StoragePathValidator.validateBucket("");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("empty"));
    }

    @Test
    void tooShortBucket_invalid() {
      var result = StoragePathValidator.validateBucket("ab");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("too short"));
    }

    @Test
    void tooLongBucket_invalid() {
      String longBucket = "a".repeat(64);
      var result = StoragePathValidator.validateBucket(longBucket);
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("too long"));
    }

    @Test
    void maxLengthBucket_valid() {
      String maxBucket = "a" + "b".repeat(61) + "c"; // 63 chars
      var result = StoragePathValidator.validateBucket(maxBucket);
      assertTrue(result.isValid());
    }

    @Test
    void uppercaseBucket_invalid() {
      var result = StoragePathValidator.validateBucket("MyBucket");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("lowercase"));
    }

    @Test
    void bucketWithUnderscore_invalid() {
      var result = StoragePathValidator.validateBucket("my_bucket");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("invalid"));
    }

    @Test
    void bucketWithDot_invalid() {
      // Dots are technically allowed by S3 but cause SSL issues
      var result = StoragePathValidator.validateBucket("my.bucket");
      assertFalse(result.isValid());
    }

    @Test
    void bucketStartingWithHyphen_invalid() {
      var result = StoragePathValidator.validateBucket("-mybucket");
      assertFalse(result.isValid());
    }

    @Test
    void bucketEndingWithHyphen_invalid() {
      var result = StoragePathValidator.validateBucket("mybucket-");
      assertFalse(result.isValid());
    }

    @Test
    void ipAddressBucket_invalid() {
      // IP addresses contain dots which are not allowed
      var result = StoragePathValidator.validateBucket("192.168.1.1");
      assertFalse(result.isValid());
      // Fails pattern check (dots not allowed) before IP check
      assertTrue(result.getErrorMessage().contains("invalid"));
    }

    @Test
    void bucketWithSpecialChars_invalid() {
      var result = StoragePathValidator.validateBucket("my@bucket");
      assertFalse(result.isValid());
    }

    @Test
    void bucketWithSpace_invalid() {
      var result = StoragePathValidator.validateBucket("my bucket");
      assertFalse(result.isValid());
    }
  }

  @Nested
  class ObjectKeyValidation {

    @ParameterizedTest
    @ValueSource(
        strings = {
          "file.txt",
          "path/to/file.txt",
          "spark/app-001/executor-0/logs.clp.zst",
          "2024/01/15/data.json",
          "file-with-hyphens.txt",
          "file_with_underscores.txt",
          "file.multiple.dots.txt",
          "CamelCaseFile.txt",
          "file with spaces.txt",
          "file!special'chars().txt"
        })
    void validObjectKeys(String key) {
      var result = StoragePathValidator.validateObjectKey(key);
      assertTrue(result.isValid(), "Expected valid: " + key + ", got: " + result);
    }

    @Test
    void nullKey_invalid() {
      var result = StoragePathValidator.validateObjectKey(null);
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("null"));
    }

    @Test
    void emptyKey_invalid() {
      var result = StoragePathValidator.validateObjectKey("");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("empty"));
    }

    @Test
    void keyStartingWithSlash_invalid() {
      var result = StoragePathValidator.validateObjectKey("/path/to/file.txt");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("start with '/'"));
    }

    @Test
    void keyWithBackslash_invalid() {
      var result = StoragePathValidator.validateObjectKey("path\\to\\file.txt");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("backslash"));
    }

    @Test
    void keyTooLong_invalid() {
      String longKey = "a".repeat(1025);
      var result = StoragePathValidator.validateObjectKey(longKey);
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("too long"));
    }

    @Test
    void maxLengthKey_valid() {
      String maxKey = "a".repeat(1024);
      var result = StoragePathValidator.validateObjectKey(maxKey);
      assertTrue(result.isValid());
    }

    @Test
    void keyWithNullByte_invalid() {
      var result = StoragePathValidator.validateObjectKey("file\0name.txt");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("null"));
    }

    @Test
    void keyWithControlChar_invalid() {
      var result = StoragePathValidator.validateObjectKey("file\u0001name.txt");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("control character"));
    }

    @Test
    void keyWithUnicode_valid() {
      var result = StoragePathValidator.validateObjectKey("path/文件.txt");
      assertTrue(result.isValid());
    }

    @Test
    void unicodeKeyByteLengthCheck() {
      // Unicode characters take multiple bytes
      // 256 4-byte Unicode chars = 1024 bytes (exactly at limit)
      String unicodeKey = "𝄞".repeat(256); // Each is 4 bytes in UTF-8
      var result = StoragePathValidator.validateObjectKey(unicodeKey);
      assertTrue(result.isValid());

      // One more would exceed
      String tooLongUnicode = "𝄞".repeat(257);
      var result2 = StoragePathValidator.validateObjectKey(tooLongUnicode);
      assertFalse(result2.isValid());
    }
  }

  @Nested
  class CombinedValidation {

    @Test
    void validBucketAndKey_valid() {
      var result = StoragePathValidator.validatePath("my-bucket", "path/to/file.txt");
      assertTrue(result.isValid());
    }

    @Test
    void invalidBucket_failsFirst() {
      var result = StoragePathValidator.validatePath("INVALID", "path/to/file.txt");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("lowercase"));
    }

    @Test
    void validBucketInvalidKey_fails() {
      var result = StoragePathValidator.validatePath("my-bucket", "/invalid/key");
      assertFalse(result.isValid());
      assertTrue(result.getErrorMessage().contains("start with"));
    }
  }

  @Nested
  class RequireValidMethods {

    @Test
    void requireValidBucket_valid_noException() {
      assertDoesNotThrow(() -> StoragePathValidator.requireValidBucket("my-bucket"));
    }

    @Test
    void requireValidBucket_invalid_throwsException() {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> StoragePathValidator.requireValidBucket("INVALID"));
      assertTrue(ex.getMessage().contains("lowercase"));
    }

    @Test
    void requireValidObjectKey_valid_noException() {
      assertDoesNotThrow(() -> StoragePathValidator.requireValidObjectKey("path/to/file.txt"));
    }

    @Test
    void requireValidObjectKey_invalid_throwsException() {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> StoragePathValidator.requireValidObjectKey("/invalid"));
      assertTrue(ex.getMessage().contains("start with"));
    }

    @Test
    void requireValidPath_valid_noException() {
      assertDoesNotThrow(
          () -> StoragePathValidator.requireValidPath("my-bucket", "path/to/file.txt"));
    }

    @Test
    void requireValidPath_invalidBucket_throwsException() {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> StoragePathValidator.requireValidPath("", "path/to/file.txt"));
      assertTrue(ex.getMessage().contains("empty"));
    }
  }

  @Nested
  class ValidationResultTest {

    @Test
    void successResult() {
      var result = StoragePathValidator.ValidationResult.success();
      assertTrue(result.isValid());
      assertNull(result.getErrorMessage());
      assertEquals("Valid", result.toString());
    }

    @Test
    void failureResult() {
      var result = StoragePathValidator.ValidationResult.failure("Test error");
      assertFalse(result.isValid());
      assertEquals("Test error", result.getErrorMessage());
      assertEquals("Invalid: Test error", result.toString());
    }
  }
}
