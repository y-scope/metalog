package com.yscope.clp.service.common.storage;

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
}
