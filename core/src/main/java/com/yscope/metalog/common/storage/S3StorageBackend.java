package com.yscope.metalog.common.storage;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Amazon S3 storage backend.
 *
 * <p>Connects to AWS S3 (or any fully S3-compatible service) using the standard AWS SDK. Each
 * instance is bound to a single bucket.
 *
 * @see ObjectStorageBackend
 */
@StorageBackendType("s3")
public class S3StorageBackend extends ObjectStorageBackend {

  /**
   * Create an S3StorageBackend bound to a specific bucket.
   *
   * @param client shared S3 async client
   * @param bucket bucket name to bind to
   * @param ownsClient if true, {@link #close()} will close the client
   */
  public S3StorageBackend(S3AsyncClient client, String bucket, boolean ownsClient) {
    super("s3", client, bucket, ownsClient);
  }

  /**
   * Create an S3StorageBackend bound to a specific bucket (does not own the client).
   *
   * @param client shared S3 async client
   * @param bucket bucket name
   */
  public S3StorageBackend(S3AsyncClient client, String bucket) {
    super("s3", client, bucket);
  }
}
