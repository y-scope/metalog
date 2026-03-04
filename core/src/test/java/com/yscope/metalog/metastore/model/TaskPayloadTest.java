package com.yscope.metalog.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("TaskPayload Unit Tests")
class TaskPayloadTest {

  private static final String ARCHIVE_PATH = "spark/2024/01/15/node-1/task-001.clp";
  private static final String TABLE_NAME = "spark";
  private static final String IR_BACKEND = "minio";
  private static final String IR_BUCKET = "ir-bucket";
  private static final String ARCHIVE_BACKEND = "s3";
  private static final String ARCHIVE_BUCKET = "archive-bucket";

  private TaskPayload createPayload(List<String> irPaths) {
    return new TaskPayload(
        ARCHIVE_PATH, irPaths, TABLE_NAME, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND, ARCHIVE_BUCKET);
  }

  private TaskPayload createDefaultPayload() {
    return createPayload(List.of("/ir/file1.ir", "/ir/file2.ir"));
  }

  // ==========================================
  // Constructor validation
  // ==========================================

  @Nested
  @DisplayName("constructor")
  class Constructor {

    @Test
    void testNullArchivePath_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              new TaskPayload(
                  null, List.of(), TABLE_NAME, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND,
                  ARCHIVE_BUCKET));
    }

    @Test
    void testNullIrFilePaths_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              new TaskPayload(
                  ARCHIVE_PATH, null, TABLE_NAME, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND,
                  ARCHIVE_BUCKET));
    }

    @Test
    void testNullTableName_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              new TaskPayload(
                  ARCHIVE_PATH, List.of(), null, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND,
                  ARCHIVE_BUCKET));
    }

    @Test
    void testNullIrStorageBackend_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              new TaskPayload(
                  ARCHIVE_PATH, List.of(), TABLE_NAME, null, IR_BUCKET, ARCHIVE_BACKEND,
                  ARCHIVE_BUCKET));
    }

    @Test
    void testNullIrBucket_accepted() {
      TaskPayload payload =
          new TaskPayload(
              ARCHIVE_PATH, List.of(), TABLE_NAME, IR_BACKEND, null, ARCHIVE_BACKEND,
              ARCHIVE_BUCKET);
      assertNull(payload.getIrBucket());
    }

    @Test
    void testNullArchiveStorageBackend_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              new TaskPayload(
                  ARCHIVE_PATH, List.of(), TABLE_NAME, IR_BACKEND, IR_BUCKET, null,
                  ARCHIVE_BUCKET));
    }

    @Test
    void testNullArchiveBucket_accepted() {
      TaskPayload payload =
          new TaskPayload(
              ARCHIVE_PATH, List.of(), TABLE_NAME, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND,
              null);
      assertNull(payload.getArchiveBucket());
    }

    @Test
    void testIrFilePaths_defensiveCopy() {
      ArrayList<String> mutableList = new ArrayList<>(List.of("/ir/a.ir", "/ir/b.ir"));
      TaskPayload payload = createPayload(mutableList);
      mutableList.add("/ir/c.ir");
      assertEquals(2, payload.getFileCount());
    }

    @Test
    void testIrFilePaths_unmodifiable() {
      TaskPayload payload = createDefaultPayload();
      assertThrows(UnsupportedOperationException.class, () -> payload.getIrFilePaths().add("x"));
    }
  }

  // ==========================================
  // Serialize / Deserialize round-trip
  // ==========================================

  @Nested
  @DisplayName("serialize / deserialize")
  class Serialization {

    @Test
    void testRoundTrip_preservesAllFields() {
      TaskPayload original = createDefaultPayload();
      byte[] bytes = original.serialize();
      TaskPayload restored = TaskPayload.deserialize(bytes);
      assertEquals(original, restored);
    }

    @Test
    void testRoundTrip_emptyIrFilePaths() {
      TaskPayload original = createPayload(List.of());
      byte[] bytes = original.serialize();
      TaskPayload restored = TaskPayload.deserialize(bytes);
      assertEquals(original, restored);
      assertEquals(0, restored.getFileCount());
    }

    @Test
    void testRoundTrip_manyFiles() {
      List<String> paths = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        paths.add("/ir/file-" + i + ".ir");
      }
      TaskPayload original = createPayload(paths);
      byte[] bytes = original.serialize();
      TaskPayload restored = TaskPayload.deserialize(bytes);
      assertEquals(original, restored);
      assertEquals(100, restored.getFileCount());
    }

    @Test
    void testDeserialize_corruptBytes_throws() {
      assertThrows(RuntimeException.class, () -> TaskPayload.deserialize(new byte[] {0, 0, 0, 5, 1, 2}));
    }

    @Test
    void testDeserialize_negativeLengthPrefix_throws() {
      // 4-byte big-endian negative number
      byte[] bytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
      assertThrows(RuntimeException.class, () -> TaskPayload.deserialize(bytes));
    }
  }

  // ==========================================
  // equals / hashCode
  // ==========================================

  @Nested
  @DisplayName("equals / hashCode")
  class Equality {

    @Test
    void testEquals_sameFields_returnsTrue() {
      TaskPayload a = createDefaultPayload();
      TaskPayload b = createDefaultPayload();
      assertEquals(a, b);
      assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testEquals_differentArchivePath_returnsFalse() {
      TaskPayload a = createDefaultPayload();
      TaskPayload b =
          new TaskPayload(
              "other/path.clp",
              List.of("/ir/file1.ir", "/ir/file2.ir"),
              TABLE_NAME, IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND, ARCHIVE_BUCKET);
      assertNotEquals(a, b);
    }

    @Test
    void testEquals_differentIrPaths_returnsFalse() {
      TaskPayload a = createDefaultPayload();
      TaskPayload b = createPayload(List.of("/ir/different.ir"));
      assertNotEquals(a, b);
    }

    @Test
    void testEquals_differentTableName_returnsFalse() {
      TaskPayload a = createDefaultPayload();
      TaskPayload b =
          new TaskPayload(
              ARCHIVE_PATH,
              List.of("/ir/file1.ir", "/ir/file2.ir"),
              "other_table", IR_BACKEND, IR_BUCKET, ARCHIVE_BACKEND, ARCHIVE_BUCKET);
      assertNotEquals(a, b);
    }
  }

  // ==========================================
  // toString
  // ==========================================

  @Test
  void testToString_containsKeyFields() {
    TaskPayload payload = createDefaultPayload();
    String str = payload.toString();
    assertTrue(str.contains(ARCHIVE_PATH));
    assertTrue(str.contains("fileCount=2"));
    assertTrue(str.contains(TABLE_NAME));
  }
}
