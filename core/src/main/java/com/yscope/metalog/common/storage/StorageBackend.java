package com.yscope.metalog.common.storage;

import java.nio.file.Path;

/**
 * Abstract storage backend that hides the concept of "bucket" from callers.
 *
 * <p>Each instance is bound to a specific namespace at construction time:
 *
 * <ul>
 *   <li>Object storage: namespace = bucket name
 *   <li>Filesystem: namespace = base directory path
 *   <li>HTTP: namespace = null (no namespace concept)
 * </ul>
 *
 * <p>Callers interact with paths relative to the namespace — e.g., {@code
 * backend.downloadToFile("a/b", localPath)} rather than {@code client.getObject("my-bucket",
 * "a/b")}.
 */
public interface StorageBackend extends AutoCloseable {

  /** Human-readable name for this backend (e.g., "minio", "local", "http"). */
  String name();

  /**
   * The namespace this backend is bound to.
   *
   * @return bucket name (object storage), base path string (filesystem), or null (HTTP)
   */
  String namespace();

  /**
   * Download a file directly to a local path.
   *
   * @param path object key / relative path
   * @param localPath local file to write to
   * @throws StorageException if download fails
   * @throws ObjectNotFoundException if the file does not exist
   */
  void downloadToFile(String path, Path localPath) throws StorageException, ObjectNotFoundException;

  /**
   * Upload a local file to storage.
   *
   * @param path object key / relative path
   * @param localPath local file to upload
   * @return size of the uploaded file in bytes
   * @throws StorageException if upload fails
   */
  long uploadFromFile(String path, Path localPath) throws StorageException;

  /**
   * Delete a single file. Silently ignores files that do not exist.
   *
   * @param path object key / relative path
   * @throws StorageException if deletion fails (other than not-found)
   */
  void deleteFile(String path) throws StorageException;

  /** Release any resources held by this backend. */
  @Override
  void close();
}
