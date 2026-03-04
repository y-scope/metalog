package com.yscope.metalog.common.storage;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Google Cloud Storage backend via S3-compatible interop.
 *
 * <p>GCS exposes an S3-compatible API (XML API) that allows the AWS SDK to interact with GCS
 * buckets. This backend connects to GCS through that interoperability layer.
 *
 * @see ObjectStorageBackend
 */
@StorageBackendType("gcs")
public class GcsStorageBackend extends ObjectStorageBackend {

  /**
   * Create a GcsStorageBackend bound to a specific bucket.
   *
   * @param client shared S3 async client
   * @param bucket bucket name to bind to
   * @param ownsClient if true, {@link #close()} will close the client
   */
  public GcsStorageBackend(S3AsyncClient client, String bucket, boolean ownsClient) {
    super("gcs", client, bucket, ownsClient);
  }

  /**
   * Create a GcsStorageBackend bound to a specific bucket (does not own the client).
   *
   * @param client shared S3 async client
   * @param bucket bucket name
   */
  public GcsStorageBackend(S3AsyncClient client, String bucket) {
    super("gcs", client, bucket);
  }
}
