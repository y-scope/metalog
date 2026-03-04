package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FilesystemStorageBackend")
class FilesystemStorageBackendTest {

  @TempDir Path tempDir;
  private FilesystemStorageBackend backend;

  @BeforeEach
  void setUp() {
    backend = new FilesystemStorageBackend("local", tempDir);
  }

  @Nested
  @DisplayName("Identity")
  class Identity {

    @Test
    void name_returnsBackendName() {
      assertEquals("local", backend.name());
    }

    @Test
    void namespace_returnsBasePath() {
      assertEquals(tempDir.toAbsolutePath().normalize().toString(), backend.namespace());
    }
  }

  @Nested
  @DisplayName("downloadToFile / uploadFromFile")
  class DownloadUpload {

    @Test
    void uploadAndDownload_roundTrip() throws Exception {
      // Write a local source file
      Path source = tempDir.resolve("source.txt");
      Files.write(source, "upload me".getBytes());

      // Upload to the backend
      long size = backend.uploadFromFile("uploaded/file.txt", source);
      assertEquals(Files.size(source), size);

      // Download from the backend
      Path dest = tempDir.resolve("dest.txt");
      backend.downloadToFile("uploaded/file.txt", dest);
      assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(dest));
    }

    @Test
    void downloadToFile_notFound_throwsObjectNotFoundException() {
      Path dest = tempDir.resolve("dest.txt");
      assertThrows(
          ObjectNotFoundException.class, () -> backend.downloadToFile("nonexistent.txt", dest));
    }
  }

  @Nested
  @DisplayName("deleteFile")
  class Delete {

    @Test
    void deleteFile_removesFile() throws Exception {
      Path source = tempDir.resolve("source.txt");
      Files.write(source, "data".getBytes());
      backend.uploadFromFile("delete-me.txt", source);
      assertTrue(Files.exists(tempDir.resolve("delete-me.txt")));

      backend.deleteFile("delete-me.txt");
      assertFalse(Files.exists(tempDir.resolve("delete-me.txt")));
    }

    @Test
    void deleteFile_notFound_noThrow() {
      assertDoesNotThrow(() -> backend.deleteFile("nonexistent.txt"));
    }
  }

  @Nested
  @DisplayName("Path Traversal Protection")
  class PathTraversal {

    @Test
    void pathTraversal_throwsIllegalArgument() {
      Path dest = tempDir.resolve("out.txt");
      assertThrows(
          IllegalArgumentException.class,
          () -> backend.downloadToFile("../../etc/passwd", dest));
    }

    @Test
    void absolutePath_outside_throwsIllegalArgument() {
      Path dest = tempDir.resolve("out.txt");
      assertThrows(
          IllegalArgumentException.class, () -> backend.downloadToFile("../outside.txt", dest));
    }
  }
}
