package com.yscope.clp.service.common.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local filesystem storage backend.
 *
 * <p>Maps object keys to files under a base directory. The base path serves as the namespace
 * (analogous to a bucket in object storage).
 */
@StorageBackendType(value = "local", requiresBucket = false)
public class FilesystemStorageBackend implements StorageBackend {
  private static final Logger logger = LoggerFactory.getLogger(FilesystemStorageBackend.class);

  private final String backendName;
  private final Path basePath;

  /**
   * Create a FilesystemStorageBackend.
   *
   * @param name human-readable backend name (e.g., "local")
   * @param basePath base directory for all files
   */
  public FilesystemStorageBackend(String name, Path basePath) {
    this.backendName = name;
    this.basePath = basePath.toAbsolutePath().normalize();
  }

  /**
   * Create a FilesystemStorageBackend from a configuration map.
   *
   * <p>Used by {@link StorageBackendFactory} for reflection-based construction.
   *
   * @param name human-readable backend name
   * @param config configuration map; must contain {@code "basePath"}
   */
  public FilesystemStorageBackend(String name, Map<String, Object> config) {
    this(name, Path.of((String) config.get("basePath")));
  }

  @Override
  public String name() {
    return backendName;
  }

  @Override
  public String namespace() {
    return basePath.toString();
  }

  @Override
  public void downloadToFile(String path, Path localPath)
      throws StorageException, ObjectNotFoundException {
    Path resolved = resolve(path);
    try {
      Files.copy(resolved, localPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (NoSuchFileException e) {
      throw new ObjectNotFoundException("File not found: " + resolved);
    } catch (IOException e) {
      throw new StorageException("Failed to copy file: " + resolved + " -> " + localPath, e);
    }
  }

  @Override
  public long uploadFromFile(String path, Path localPath) throws StorageException {
    Path resolved = resolve(path);
    try {
      long size = Files.size(localPath);
      Files.createDirectories(resolved.getParent());
      Files.copy(localPath, resolved, StandardCopyOption.REPLACE_EXISTING);
      logger.debug("Uploaded file: {} -> {}, size={}", localPath, resolved, size);
      return size;
    } catch (IOException e) {
      throw new StorageException("Failed to upload file: " + localPath + " -> " + resolved, e);
    }
  }

  @Override
  public void deleteFile(String path) throws StorageException {
    Path resolved = resolve(path);
    try {
      Files.deleteIfExists(resolved);
      logger.debug("Deleted file: {}", resolved);
    } catch (IOException e) {
      throw new StorageException("Failed to delete file: " + resolved, e);
    }
  }

  @Override
  public void close() {
    logger.info("FilesystemStorageBackend closed (name={}, basePath={})", backendName, basePath);
  }

  /**
   * Resolve a relative path against the base directory, with path traversal protection.
   *
   * @throws IllegalArgumentException if the path escapes the base directory
   */
  private Path resolve(String path) {
    Path resolved = basePath.resolve(path).normalize();
    if (!resolved.startsWith(basePath)) {
      throw new IllegalArgumentException("Path traversal attempt: " + path);
    }
    return resolved;
  }
}
