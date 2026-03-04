package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@DisplayName("S3StorageBackend")
class S3StorageBackendTest {

  private final S3AsyncClient s3 = mock(S3AsyncClient.class);

  @Test
  void name_returnsS3() {
    var backend = new S3StorageBackend(s3, "test-bucket");
    assertEquals("s3", backend.name());
  }

  @Test
  void namespace_returnsBucket() {
    var backend = new S3StorageBackend(s3, "my-bucket");
    assertEquals("my-bucket", backend.namespace());
  }

  @Test
  void isInstanceOfObjectStorageBackend() {
    var backend = new S3StorageBackend(s3, "test-bucket");
    assertInstanceOf(ObjectStorageBackend.class, backend);
  }
}
