package com.yscope.clp.service.common.storage;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * MinIO object storage backend.
 *
 * <p>MinIO is a self-hosted, S3-compatible object store. This backend uses the S3 API with
 * path-style access enabled to communicate with a MinIO server.
 *
 * @see ObjectStorageBackend
 */
@StorageBackendType("minio")
public class MinioStorageBackend extends ObjectStorageBackend {

  /**
   * Create a MinioStorageBackend bound to a specific bucket.
   *
   * @param client shared S3 async client
   * @param bucket bucket name to bind to
   * @param ownsClient if true, {@link #close()} will close the client
   */
  public MinioStorageBackend(S3AsyncClient client, String bucket, boolean ownsClient) {
    super("minio", client, bucket, ownsClient);
  }

  /**
   * Create a MinioStorageBackend bound to a specific bucket (does not own the client).
   *
   * @param client shared S3 async client
   * @param bucket bucket name
   */
  public MinioStorageBackend(S3AsyncClient client, String bucket) {
    super("minio", client, bucket);
  }
}
