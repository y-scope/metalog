package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("ClpCompressor Unit Tests")
class ClpCompressorTest {

  @TempDir Path tempDir;

  private Path stagingDir;
  private Path outputDir;

  @BeforeEach
  void setUp() throws IOException {
    stagingDir = Files.createDirectory(tempDir.resolve("staging"));
    outputDir = Files.createDirectory(tempDir.resolve("output"));
  }

  private Path createScript(String name, String content) throws IOException {
    Path script = tempDir.resolve(name);
    Files.writeString(script, "#!/bin/bash\n" + content);
    Files.setPosixFilePermissions(script, Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE));
    return script;
  }

  // ==========================================
  // Successful Compression
  // ==========================================

  @Nested
  @DisplayName("Successful Compression")
  class SuccessfulCompression {

    @Test
    void testCompress_success_returnsOutputPath() throws Exception {
      // Script args: c --single-file-archive outputDir stagingDir → $3 is outputDir
      Path script = createScript("clp-s-ok", "touch \"$3/archive-uuid.clp\"");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 10);

      // Create a staging file to simulate IR input
      Files.writeString(stagingDir.resolve("input.ir"), "ir content");

      Path result = compressor.compress(stagingDir, outputDir);

      assertNotNull(result);
      assertTrue(Files.exists(result));
      assertEquals("archive-uuid.clp", result.getFileName().toString());
    }
  }

  // ==========================================
  // Non-Zero Exit Code
  // ==========================================

  @Nested
  @DisplayName("Non-Zero Exit Code")
  class NonZeroExitCode {

    @Test
    void testCompress_nonZeroExit_throwsCompressorException() throws Exception {
      Path script = createScript("clp-s-fail", "echo 'error message' >&2\nexit 1");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 10);

      ClpCompressor.CompressorException ex =
          assertThrows(
              ClpCompressor.CompressorException.class,
              () -> compressor.compress(stagingDir, outputDir));

      assertTrue(ex.getMessage().contains("exited with code 1"));
    }
  }

  // ==========================================
  // Timeout
  // ==========================================

  @Nested
  @DisplayName("Timeout")
  class Timeout {

    @Test
    void testCompress_timeout_throwsCompressorException() throws Exception {
      Path script = createScript("clp-s-slow", "sleep 60");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 1);

      ClpCompressor.CompressorException ex =
          assertThrows(
              ClpCompressor.CompressorException.class,
              () -> compressor.compress(stagingDir, outputDir));

      assertTrue(ex.getMessage().contains("timed out"));
    }
  }

  // ==========================================
  // Missing Binary
  // ==========================================

  @Nested
  @DisplayName("Missing Binary")
  class MissingBinary {

    @Test
    void testCompress_missingBinary_throwsCompressorException() {
      ClpCompressor compressor = new ClpCompressor("/nonexistent/clp-s", 10);

      assertThrows(
          ClpCompressor.CompressorException.class,
          () -> compressor.compress(stagingDir, outputDir));
    }
  }

  // ==========================================
  // findSingleArchive edge cases
  // ==========================================

  @Nested
  @DisplayName("findSingleArchive")
  class FindSingleArchive {

    @Test
    void testCompress_emptyOutputDir_throwsCompressorException() throws Exception {
      // Script succeeds but creates no output file (just touches a dir to be safe)
      Path script = createScript("clp-s-empty", "# no output\nexit 0");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 10);

      ClpCompressor.CompressorException ex =
          assertThrows(
              ClpCompressor.CompressorException.class,
              () -> compressor.compress(stagingDir, outputDir));

      assertTrue(ex.getMessage().contains("No archive file found"));
    }

    @Test
    void testCompress_multipleOutputFiles_throwsCompressorException() throws Exception {
      // Script creates two output files; $3 is outputDir
      Path script = createScript("clp-s-multi",
          "touch \"$3/archive1.clp\"\ntouch \"$3/archive2.clp\"");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 10);

      ClpCompressor.CompressorException ex =
          assertThrows(
              ClpCompressor.CompressorException.class,
              () -> compressor.compress(stagingDir, outputDir));

      assertTrue(ex.getMessage().contains("multiple files"));
    }
  }

  // ==========================================
  // Output content
  // ==========================================

  @Nested
  @DisplayName("Output Content")
  class OutputContent {

    @Test
    void testCompress_scriptReceivesCorrectArguments() throws Exception {
      // Script writes arguments to a file so we can verify them
      Path argsFile = tempDir.resolve("args.txt");
      Path script = createScript("clp-s-args",
          "echo \"$@\" > " + argsFile + "\ntouch \"$3/out.clp\"");

      ClpCompressor compressor = new ClpCompressor(script.toString(), 10);
      compressor.compress(stagingDir, outputDir);

      String args = Files.readString(argsFile);
      // clp-s is invoked as: binary c --single-file-archive outputDir stagingDir
      assertTrue(args.contains("c --single-file-archive"));
      assertTrue(args.contains(outputDir.toString()));
      assertTrue(args.contains(stagingDir.toString()));
    }
  }
}
