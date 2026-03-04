package com.yscope.metalog.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@DisplayName("MinioStorageBackend")
class MinioStorageBackendTest {

  private final S3AsyncClient s3 = mock(S3AsyncClient.class);

  @Test
  void name_returnsMinio() {
    var backend = new MinioStorageBackend(s3, "test-bucket");
    assertEquals("minio", backend.name());
  }

  @Test
  void namespace_returnsBucket() {
    var backend = new MinioStorageBackend(s3, "my-bucket");
    assertEquals("my-bucket", backend.namespace());
  }

  @Test
  void isInstanceOfObjectStorageBackend() {
    var backend = new MinioStorageBackend(s3, "test-bucket");
    assertInstanceOf(ObjectStorageBackend.class, backend);
  }

  @Test
  void ownsClientConstructor_works() {
    var backend = new MinioStorageBackend(s3, "test-bucket", true);
    assertEquals("minio", backend.name());
  }
}
