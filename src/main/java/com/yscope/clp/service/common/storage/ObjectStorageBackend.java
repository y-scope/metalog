package com.yscope.clp.service.common.storage;

import com.yscope.clp.service.common.config.ObjectStorageConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * S3-compatible object storage backend (MinIO, AWS S3, GCS via S3 interop).
 *
 * <p>Each instance is bound to a single bucket. Two instances on the same endpoint can share the
 * same {@link S3AsyncClient} (created once via {@link #buildClient}) to avoid duplicate HTTP
 * connection pools.
 */
public class ObjectStorageBackend implements StorageBackend {
  private static final Logger logger = LoggerFactory.getLogger(ObjectStorageBackend.class);

  private final String backendName;
  private final S3AsyncClient client;
  private final String bucket;
  private final boolean ownsClient;

  /**
   * Create an ObjectStorageBackend bound to a specific bucket.
   *
   * @param name human-readable backend name (e.g., "minio")
   * @param client shared S3 async client (caller retains ownership unless {@code ownsClient} is
   *     true)
   * @param bucket bucket name to bind to
   * @param ownsClient if true, {@link #close()} will close the client
   */
  public ObjectStorageBackend(
      String name, S3AsyncClient client, String bucket, boolean ownsClient) {
    StoragePathValidator.requireValidBucket(bucket);
    this.backendName = name;
    this.client = client;
    this.bucket = bucket;
    this.ownsClient = ownsClient;
  }

  /**
   * Create an ObjectStorageBackend bound to a specific bucket (does not own the client).
   *
   * @param name human-readable backend name
   * @param client shared S3 async client
   * @param bucket bucket name
   */
  public ObjectStorageBackend(String name, S3AsyncClient client, String bucket) {
    this(name, client, bucket, false);
  }

  /**
   * Build a shared S3AsyncClient from configuration.
   *
   * <p>The returned client can be shared across multiple {@code ObjectStorageBackend} instances
   * bound to different buckets on the same endpoint.
   *
   * @param config object storage configuration
   * @return shared S3AsyncClient
   */
  public static S3AsyncClient buildClient(ObjectStorageConfig config) {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(config.getEndpoint()))
        .region(Region.of(config.getRegion()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey())))
        .serviceConfiguration(
            S3Configuration.builder().pathStyleAccessEnabled(config.isForcePathStyle()).build())
        .build();
  }

  @Override
  public String name() {
    return backendName;
  }

  @Override
  public String namespace() {
    return bucket;
  }

  @Override
  public void downloadToFile(String path, Path localPath)
      throws StorageException, ObjectNotFoundException {
    StoragePathValidator.requireValidObjectKey(path);
    try {
      client
          .getObject(
              GetObjectRequest.builder().bucket(bucket).key(path).build(),
              AsyncResponseTransformer.toFile(localPath))
          .join();
      logger.debug("Downloaded to file: bucket={}, path={}, localPath={}", bucket, path, localPath);
    } catch (CompletionException e) {
      Throwable cause = unwrap(e);
      if (cause instanceof NoSuchKeyException) {
        throw new ObjectNotFoundException("Object not found: " + bucket + "/" + path);
      }
      throw new StorageException("Failed to download file: " + bucket + "/" + path, cause);
    }
  }

  @Override
  public long uploadFromFile(String path, Path localPath) throws StorageException {
    StoragePathValidator.requireValidObjectKey(path);
    try {
      long size = Files.size(localPath);
      client
          .putObject(
              PutObjectRequest.builder().bucket(bucket).key(path).build(),
              AsyncRequestBody.fromFile(localPath))
          .join();
      logger.debug(
          "Uploaded from file: bucket={}, path={}, localPath={}, size={}",
          bucket, path, localPath, size);
      return size;
    } catch (IOException e) {
      throw new StorageException("Failed to read local file: " + localPath, e);
    } catch (CompletionException e) {
      throw new StorageException("Failed to upload file: " + bucket + "/" + path, unwrap(e));
    }
  }

  @Override
  public void deleteFile(String path) throws StorageException {
    try {
      deleteObject(path);
      logger.debug("Deleted file from storage: bucket={}, path={}", bucket, path);
    } catch (ObjectNotFoundException e) {
      logger.debug("File not found (already deleted): {}/{}", bucket, path);
    }
  }

  @Override
  public void close() {
    if (ownsClient) {
      try {
        client.close();
      } catch (Exception e) {
        logger.warn("Error closing S3 client for backend: {}", backendName, e);
      }
    }
    logger.info("ObjectStorageBackend closed (backend={}, bucket={})", backendName, bucket);
  }

  /** Get the underlying S3AsyncClient (for archive creation parallel downloads). */
  public S3AsyncClient getClient() {
    return client;
  }

  private void deleteObject(String path) throws StorageException, ObjectNotFoundException {
    try {
      client
          .headObject(HeadObjectRequest.builder().bucket(bucket).key(path).build())
          .join();
      client
          .deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(path).build())
          .join();
    } catch (CompletionException e) {
      Throwable cause = unwrap(e);
      if (cause instanceof NoSuchKeyException || isNotFound(cause)) {
        throw new ObjectNotFoundException("Object not found: " + bucket + "/" + path);
      }
      throw new StorageException("Failed to delete object: " + bucket + "/" + path, cause);
    }
  }

  private static Throwable unwrap(CompletionException e) {
    Throwable cause = e.getCause();
    return cause != null ? cause : e;
  }

  /**
   * Check if an exception represents a 404 Not Found response.
   *
   * <p>HEAD requests on non-existent keys throw S3Exception (not NoSuchKeyException) because HEAD
   * responses have no XML body to parse the error code from.
   */
  private static boolean isNotFound(Throwable cause) {
    if (cause instanceof S3Exception s3e) {
      return s3e.statusCode() == 404;
    }
    return false;
  }
}
