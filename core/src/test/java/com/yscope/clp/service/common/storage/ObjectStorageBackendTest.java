package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@DisplayName("ObjectStorageBackend Unit Tests")
class ObjectStorageBackendTest {

  private S3AsyncClient s3;
  private ObjectStorageBackend backend;

  private static final String BUCKET = "test-bucket";

  @BeforeEach
  void setUp() {
    s3 = mock(S3AsyncClient.class);
    backend = new ObjectStorageBackend("test", s3, BUCKET);
  }

  // ==========================================
  // Constructor Validation
  // ==========================================

  @Nested
  @DisplayName("Constructor Validation")
  class ConstructorValidation {

    @Test
    void testConstructor_validBucket_succeeds() {
      assertNotNull(new ObjectStorageBackend("test", s3, "valid-bucket"));
    }

    @Test
    void testConstructor_invalidBucket_throwsIllegalArgument() {
      assertThrows(
          IllegalArgumentException.class, () -> new ObjectStorageBackend("test", s3, ""));
    }
  }

  // ==========================================
  // name() and namespace()
  // ==========================================

  @Nested
  @DisplayName("Identity")
  class Identity {

    @Test
    void name_returnsBackendName() {
      assertEquals("test", backend.name());
    }

    @Test
    void namespace_returnsBucket() {
      assertEquals(BUCKET, backend.namespace());
    }
  }

  // ==========================================
  // Download To File
  // ==========================================

  @Nested
  @DisplayName("downloadToFile")
  class DownloadToFile {

    @TempDir Path tempDir;

    @SuppressWarnings("unchecked")
    @Test
    void testDownloadToFile_success() throws StorageException, ObjectNotFoundException {
      Path localPath = tempDir.resolve("downloaded.ir");
      when(s3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
          .thenReturn(CompletableFuture.completedFuture(GetObjectResponse.builder().build()));

      assertDoesNotThrow(() -> backend.downloadToFile("path/to/file.ir", localPath));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDownloadToFile_notFound_throwsObjectNotFoundException() {
      Path localPath = tempDir.resolve("downloaded.ir");
      when(s3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
          .thenReturn(
              CompletableFuture.failedFuture(
                  new CompletionException(
                      NoSuchKeyException.builder().message("not found").build())));

      assertThrows(
          ObjectNotFoundException.class,
          () -> backend.downloadToFile("path/to/missing.ir", localPath));
    }
  }

  // ==========================================
  // Upload From File
  // ==========================================

  @Nested
  @DisplayName("uploadFromFile")
  class UploadFromFile {

    @TempDir Path tempDir;

    @Test
    void testUploadFromFile_success_returnsFileSize() throws Exception {
      Path localPath = tempDir.resolve("upload.ir");
      Files.write(localPath, "test content".getBytes());

      when(s3.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
          .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));

      long size = backend.uploadFromFile("path/to/file.ir", localPath);

      assertEquals(Files.size(localPath), size);
    }

    @Test
    void testUploadFromFile_missingFile_throwsStorageException() {
      Path localPath = tempDir.resolve("nonexistent.ir");

      assertThrows(
          StorageException.class, () -> backend.uploadFromFile("path/to/file.ir", localPath));
    }
  }

  // ==========================================
  // Delete File
  // ==========================================

  @Nested
  @DisplayName("deleteFile")
  class DeleteFile {

    @Test
    void testDeleteFile_success() throws StorageException {
      when(s3.headObject(any(HeadObjectRequest.class)))
          .thenReturn(
              CompletableFuture.completedFuture(
                  HeadObjectResponse.builder().contentLength(100L).build()));
      when(s3.deleteObject(any(DeleteObjectRequest.class)))
          .thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));

      assertDoesNotThrow(() -> backend.deleteFile("path/to/file.ir"));
    }

    @Test
    void testDeleteFile_notFound_silentlySwallows() throws StorageException {
      when(s3.headObject(any(HeadObjectRequest.class)))
          .thenReturn(
              CompletableFuture.failedFuture(
                  new CompletionException(
                      NoSuchKeyException.builder().message("not found").build())));

      assertDoesNotThrow(() -> backend.deleteFile("path/to/missing.ir"));
    }
  }

  // ==========================================
  // Close
  // ==========================================

  @Nested
  @DisplayName("close")
  class Close {

    @Test
    void testClose_ownsClient_closesClient() {
      ObjectStorageBackend owning = new ObjectStorageBackend("test", s3, BUCKET, true);
      owning.close();
      verify(s3).close();
    }

    @Test
    void testClose_doesNotOwnClient_doesNotCloseClient() {
      backend.close();
      verify(s3, never()).close();
    }
  }
}
