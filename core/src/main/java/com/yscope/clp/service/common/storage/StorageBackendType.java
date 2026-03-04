package com.yscope.clp.service.common.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the type name used to look up a {@link StorageBackend} implementation.
 *
 * <p>The factory scans for classes with this annotation and registers them automatically. To add a
 * new storage backend:
 *
 * <ol>
 *   <li>Implement {@link StorageBackend} (or extend {@link ObjectStorageBackend})
 *   <li>Annotate with {@code @StorageBackendType("your-type")}
 *   <li>Done — no other changes needed!
 * </ol>
 *
 * <p>Additional packages can be scanned by setting the {@code CLP_STORAGE_BACKEND_PACKAGES}
 * environment variable to a comma-separated list of package names.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @StorageBackendType("minio")
 * public class MinioStorageBackend extends ObjectStorageBackend { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StorageBackendType {
  /** The backend type name used in configuration. Case-sensitive during lookup. */
  String value();

  /**
   * Whether this backend requires a bucket for storage operations.
   *
   * <p>Object-storage backends (S3, GCS, Minio) require buckets, while URL-based or filesystem
   * backends do not. The metastore validator uses this to decide whether a null bucket is an error.
   *
   * <p>Defaults to {@code true} so existing object-storage backends need no change.
   */
  boolean requiresBucket() default true;
}
