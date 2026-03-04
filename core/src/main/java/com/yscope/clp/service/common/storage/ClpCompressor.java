package com.yscope.clp.service.common.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subprocess wrapper for the {@code clp-s} compression binary.
 *
 * <p>Runs {@code clp-s c --single-file-archive} to compress IR files into a single CLP archive. The
 * binary is expected to be installed at the configured path (default: {@code /usr/bin/clp-s}).
 */
public class ClpCompressor {
  private static final Logger logger = LoggerFactory.getLogger(ClpCompressor.class);

  private final String binaryPath;
  private final long timeoutSeconds;

  public ClpCompressor(String binaryPath, long timeoutSeconds) {
    this.binaryPath = binaryPath;
    this.timeoutSeconds = timeoutSeconds;
  }

  /**
   * Compress IR files in a staging directory into a single CLP archive.
   *
   * @param stagingDir Directory containing downloaded IR files
   * @param outputDir Directory where clp-s will write the archive
   * @return Path to the generated archive file
   * @throws CompressorException if compression fails or times out
   */
  public Path compress(Path stagingDir, Path outputDir) throws CompressorException {
    ProcessBuilder pb =
        new ProcessBuilder(
            binaryPath, "c", "--single-file-archive", outputDir.toString(), stagingDir.toString());
    logger.debug("Running clp-s compression: staging={}, output={}", stagingDir, outputDir);

    Process process = null;
    try {
      process = pb.start();
      final Process proc = process;

      // Read stdout/stderr in background threads to avoid deadlock
      CompletableFuture<String> stdoutFuture =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return new String(proc.getInputStream().readAllBytes());
                } catch (IOException e) {
                  return "";
                }
              });
      CompletableFuture<String> stderrFuture =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return new String(proc.getErrorStream().readAllBytes());
                } catch (IOException e) {
                  return "";
                }
              });

      boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
      if (!finished) {
        process.destroyForcibly();
        // Close streams explicitly to unblock the background reader threads
        try {
          process.getInputStream().close();
        } catch (IOException ignored) {
        }
        try {
          process.getErrorStream().close();
        } catch (IOException ignored) {
        }
        throw new CompressorException("clp-s timed out after " + timeoutSeconds + " seconds");
      }

      String stdout;
      String stderr;
      try {
        stdout = stdoutFuture.get(5, TimeUnit.SECONDS);
        stderr = stderrFuture.get(5, TimeUnit.SECONDS);
      } catch (TimeoutException | ExecutionException e) {
        logger.warn("Failed to read process output after completion", e);
        stdout = "";
        stderr = "";
      }

      int exitCode = process.exitValue();
      if (exitCode != 0) {
        logger.error("clp-s failed (exit={}): stdout={}, stderr={}", exitCode, stdout, stderr);
        throw new CompressorException("clp-s exited with code " + exitCode + ": " + stderr);
      }

      logger.debug("clp-s completed successfully: stdout={}", stdout);

      return findSingleArchive(outputDir);

    } catch (IOException e) {
      throw new CompressorException("Failed to start clp-s process", e);
    } catch (InterruptedException e) {
      if (process != null) {
        process.destroyForcibly();
      }
      Thread.currentThread().interrupt();
      throw new CompressorException("clp-s process interrupted", e);
    }
  }

  /**
   * Find the single archive file that clp-s generates in the output directory.
   *
   * <p>clp-s creates a UUID-named file in the output directory.
   */
  private Path findSingleArchive(Path outputDir) throws CompressorException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputDir)) {
      Path archive = null;
      for (Path entry : stream) {
        if (Files.isRegularFile(entry)) {
          if (archive != null) {
            throw new CompressorException(
                "Expected single archive but found multiple files in " + outputDir);
          }
          archive = entry;
        }
      }
      if (archive == null) {
        throw new CompressorException("No archive file found in " + outputDir);
      }
      logger.debug("Found archive: {} (size={})", archive, Files.size(archive));
      return archive;
    } catch (IOException e) {
      throw new CompressorException("Failed to scan output directory: " + outputDir, e);
    }
  }

  /** Exception for clp-s compression errors. */
  public static class CompressorException extends Exception {
    public CompressorException(String message) {
      super(message);
    }

    public CompressorException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
