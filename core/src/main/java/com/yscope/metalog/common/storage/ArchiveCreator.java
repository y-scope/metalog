package com.yscope.metalog.common.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Orchestrates archive creation from IR files.
 *
 * <p>Composes an IR backend (for reading IR files) with an archive backend (for writing the
 * resulting archive). Owns the parallel download, clp-s compression, POC placeholder, and temp
 * directory cleanup logic.
 *
 * <p>When a {@link ClpCompressor} is configured, downloads IR files in parallel (semaphore-gated at
 * {@value MAX_CONCURRENT_DOWNLOADS}), runs {@code clp-s c --single-file-archive}, and uploads the
 * result. Otherwise, falls back to a POC placeholder.
 */
public class ArchiveCreator {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveCreator.class);

  private static final int MAX_CONCURRENT_DOWNLOADS = 16;

  private final StorageBackend irBackend;
  private final StorageBackend archiveBackend;
  private ClpCompressor clpCompressor;

  /**
   * Create an ArchiveCreator.
   *
   * @param irBackend backend for reading IR files
   * @param archiveBackend backend for writing archive files
   */
  public ArchiveCreator(StorageBackend irBackend, StorageBackend archiveBackend) {
    this.irBackend = irBackend;
    this.archiveBackend = archiveBackend;
  }

  /**
   * Set the CLP compressor for real archive creation.
   *
   * @param compressor CLP compressor instance, or null for POC mode
   */
  public void setClpCompressor(ClpCompressor compressor) {
    this.clpCompressor = compressor;
  }

  /**
   * Create an archive from IR files.
   *
   * @param archivePath destination path for the archive (relative to archive backend)
   * @param irPaths list of IR file paths (relative to IR backend)
   * @return actual size of the created archive in bytes
   * @throws StorageException if archive creation fails
   * @throws InterruptedException if the thread is interrupted during download
   */
  public long createArchive(String archivePath, List<String> irPaths)
      throws StorageException, InterruptedException {
    if (clpCompressor == null) {
      return createArchivePlaceholder(archivePath, irPaths);
    }

    Path stagingDir = null;
    Path outputDir = null;
    try {
      stagingDir = Files.createTempDirectory("clp-staging-");
      outputDir = Files.createTempDirectory("clp-output-");

      downloadIrFilesParallel(irPaths, stagingDir);

      Path archiveFile = clpCompressor.compress(stagingDir, outputDir);

      long archiveSize = archiveBackend.uploadFromFile(archivePath, archiveFile);

      logger.info(
          "Created archive via clp-s: path={}, ir_files={}, archive_size={}",
          archivePath, irPaths.size(), archiveSize);

      return archiveSize;

    } catch (ClpCompressor.CompressorException e) {
      throw new StorageException("clp-s compression failed for " + archivePath, e);
    } catch (ObjectNotFoundException e) {
      throw new StorageException(
          "IR file not found during archive creation: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new StorageException(
          "Failed to create temp directories for archive: " + archivePath, e);
    } finally {
      deleteTempDir(stagingDir);
      deleteTempDir(outputDir);
    }
  }

  /**
   * Download IR files in parallel using a semaphore to limit concurrency.
   *
   * <p>If the IR backend is an {@link ObjectStorageBackend}, uses the underlying S3AsyncClient for
   * truly async parallel downloads. Otherwise, falls back to sequential downloads via the backend
   * interface.
   */
  private void downloadIrFilesParallel(List<String> irPaths, Path stagingDir)
      throws StorageException, ObjectNotFoundException, InterruptedException {
    if (irBackend instanceof ObjectStorageBackend objectBackend) {
      downloadIrFilesParallelS3(objectBackend, irPaths, stagingDir);
    } else {
      downloadIrFilesSequential(irPaths, stagingDir);
    }
  }

  /** S3-native parallel download using the async client directly. */
  private void downloadIrFilesParallelS3(
      ObjectStorageBackend objectBackend, List<String> irPaths, Path stagingDir)
      throws StorageException, ObjectNotFoundException, InterruptedException {
    S3AsyncClient client = objectBackend.getClient();
    String bucket = objectBackend.namespace();
    Semaphore permits = new Semaphore(MAX_CONCURRENT_DOWNLOADS);

    List<CompletableFuture<?>> futures = new ArrayList<>();
    for (String irPath : irPaths) {
      String localName = irPath.replace('/', '_');
      Path localPath = stagingDir.resolve(localName);
      permits.acquire();
      try {
        futures.add(
            client
                .getObject(
                    GetObjectRequest.builder().bucket(bucket).key(irPath).build(),
                    AsyncResponseTransformer.toFile(localPath))
                .whenComplete((r, e) -> permits.release()));
      } catch (RuntimeException e) {
        permits.release();
        throw e;
      }
    }

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof NoSuchKeyException) {
        throw new ObjectNotFoundException("IR file not found: " + cause.getMessage());
      }
      throw new StorageException("Failed to download IR files", cause);
    }
  }

  /** Sequential download fallback for non-S3 backends. */
  private void downloadIrFilesSequential(List<String> irPaths, Path stagingDir)
      throws StorageException, ObjectNotFoundException {
    for (String irPath : irPaths) {
      String localName = irPath.replace('/', '_');
      Path localPath = stagingDir.resolve(localName);
      irBackend.downloadToFile(irPath, localPath);
    }
  }

  /** POC placeholder archive creation. */
  private long createArchivePlaceholder(String archivePath, List<String> irPaths)
      throws StorageException {
    long totalIrSize = 0;
    List<byte[]> irContents = new ArrayList<>();

    Path tempFile = null;
    try {
      for (String irPath : irPaths) {
        try {
          Path irTemp = Files.createTempFile("ir-", ".tmp");
          try {
            irBackend.downloadToFile(irPath, irTemp);
            byte[] content = Files.readAllBytes(irTemp);
            irContents.add(content);
            totalIrSize += content.length;
          } finally {
            Files.deleteIfExists(irTemp);
          }
        } catch (ObjectNotFoundException e) {
          logger.debug("IR file not found (POC mode): {}", irPath);
          totalIrSize += 1024 * 1024;
        }
      }

      byte[] archiveContent = createArchiveContent(archivePath, irPaths, irContents, totalIrSize);

      tempFile = Files.createTempFile("archive-", ".tmp");
      Files.write(tempFile, archiveContent);
      archiveBackend.uploadFromFile(archivePath, tempFile);

      logger.info(
          "Created placeholder archive: path={}, ir_files={}, total_ir_size={}, archive_size={}",
          archivePath, irPaths.size(), totalIrSize, archiveContent.length);

      return archiveContent.length;
    } catch (IOException e) {
      throw new StorageException("Failed to create placeholder archive: " + archivePath, e);
    } finally {
      if (tempFile != null) {
        try {
          Files.deleteIfExists(tempFile);
        } catch (IOException e) {
          logger.warn("Failed to delete temp file: {}", tempFile, e);
        }
      }
    }
  }

  /** Create POC placeholder archive content. */
  private byte[] createArchiveContent(
      String archivePath, List<String> irPaths, List<byte[]> irContents, long totalIrSize) {
    StringBuilder metadata = new StringBuilder();
    metadata.append("CLP_ARCHIVE_POC_V1\n");
    metadata.append("archive_path: ").append(archivePath).append("\n");
    metadata.append("ir_file_count: ").append(irPaths.size()).append("\n");
    metadata.append("total_ir_size: ").append(totalIrSize).append("\n");
    metadata.append("ir_files_found: ").append(irContents.size()).append("\n");
    metadata.append("created_at: ").append(System.currentTimeMillis()).append("\n");
    metadata.append("---\n");
    for (String path : irPaths) {
      metadata.append("ir_file: ").append(path).append("\n");
    }
    metadata.append("---\n");

    byte[] header = metadata.toString().getBytes();

    if (irContents.isEmpty()) {
      return header;
    }

    int totalSize = header.length;
    for (byte[] content : irContents) {
      totalSize += content.length;
    }

    byte[] archive = new byte[totalSize];
    System.arraycopy(header, 0, archive, 0, header.length);

    int offset = header.length;
    for (byte[] content : irContents) {
      System.arraycopy(content, 0, archive, offset, content.length);
      offset += content.length;
    }

    return archive;
  }

  private void deleteTempDir(Path dir) {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try {
      Files.walkFileTree(
          dir,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              try {
                Files.delete(file);
              } catch (IOException e) {
                logger.warn("Failed to delete temp file: {}", file, e);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) {
              try {
                Files.delete(d);
              } catch (IOException e) {
                logger.warn("Failed to delete temp directory: {}", d, e);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      logger.warn("Failed to walk temp directory for cleanup: {}", dir, e);
    }
  }
}
