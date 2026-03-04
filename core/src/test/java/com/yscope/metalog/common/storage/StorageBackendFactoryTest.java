package com.yscope.metalog.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("StorageBackendFactory")
class StorageBackendFactoryTest {

  @Test
  @DisplayName("getRegisteredTypes returns all built-in backend types")
  void testGetRegisteredTypes() {
    Set<String> types = StorageBackendFactory.getRegisteredTypes();

    assertTrue(types.contains("minio"), "should contain minio");
    assertTrue(types.contains("s3"), "should contain s3");
    assertTrue(types.contains("gcs"), "should contain gcs");
    assertTrue(types.contains("local"), "should contain local");
    assertTrue(types.contains("http"), "should contain http");
    assertEquals(5, types.size(), "should have exactly 5 built-in types");
  }

  @Test
  @DisplayName("getBackendClass returns correct class for known type")
  void testGetBackendClass_knownType() {
    assertEquals(HttpStorageBackend.class, StorageBackendFactory.getBackendClass("http"));
    assertEquals(FilesystemStorageBackend.class, StorageBackendFactory.getBackendClass("local"));
    assertEquals(S3StorageBackend.class, StorageBackendFactory.getBackendClass("s3"));
  }

  @Test
  @DisplayName("getBackendClass returns null for unknown type")
  void testGetBackendClass_unknownType() {
    assertNull(StorageBackendFactory.getBackendClass("unknown"));
  }

  @Test
  @DisplayName("getBackendClass is case-sensitive")
  void testGetBackendClass_caseSensitive() {
    assertNull(StorageBackendFactory.getBackendClass("HTTP"));
    assertNull(StorageBackendFactory.getBackendClass("Local"));
  }
}
