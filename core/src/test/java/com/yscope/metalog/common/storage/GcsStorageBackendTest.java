package com.yscope.metalog.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@DisplayName("GcsStorageBackend")
class GcsStorageBackendTest {

  private final S3AsyncClient s3 = mock(S3AsyncClient.class);

  @Test
  void name_returnsGcs() {
    var backend = new GcsStorageBackend(s3, "test-bucket");
    assertEquals("gcs", backend.name());
  }

  @Test
  void namespace_returnsBucket() {
    var backend = new GcsStorageBackend(s3, "my-bucket");
    assertEquals("my-bucket", backend.namespace());
  }

  @Test
  void isInstanceOfObjectStorageBackend() {
    var backend = new GcsStorageBackend(s3, "test-bucket");
    assertInstanceOf(ObjectStorageBackend.class, backend);
  }
}
