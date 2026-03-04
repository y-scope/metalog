package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ArchiveCreator")
class ArchiveCreatorTest {

  private StorageBackend irBackend;
  private StorageBackend archiveBackend;
  private ArchiveCreator creator;

  @BeforeEach
  void setUp() {
    irBackend = mock(StorageBackend.class);
    archiveBackend = mock(StorageBackend.class);
    creator = new ArchiveCreator(irBackend, archiveBackend);
  }

  @Nested
  @DisplayName("POC placeholder mode (no ClpCompressor)")
  class PlaceholderMode {

    @Test
    void createArchive_writesPlaceholder() throws Exception {
      doAnswer(
              inv -> {
                Path localPath = inv.getArgument(1);
                Files.write(localPath, "content1".getBytes());
                return null;
              })
          .when(irBackend)
          .downloadToFile(eq("ir/file1.clp"), any(Path.class));
      doAnswer(
              inv -> {
                Path localPath = inv.getArgument(1);
                Files.write(localPath, "content2".getBytes());
                return null;
              })
          .when(irBackend)
          .downloadToFile(eq("ir/file2.clp"), any(Path.class));
      when(archiveBackend.uploadFromFile(eq("archive/out.clp"), any(Path.class))).thenReturn(100L);

      long size =
          creator.createArchive("archive/out.clp", List.of("ir/file1.clp", "ir/file2.clp"));

      assertTrue(size > 0);
      verify(archiveBackend).uploadFromFile(eq("archive/out.clp"), any(Path.class));
    }

    @Test
    void createArchive_missingIrFile_stillCreates() throws Exception {
      doAnswer(
              inv -> {
                Path localPath = inv.getArgument(1);
                Files.write(localPath, "data".getBytes());
                return null;
              })
          .when(irBackend)
          .downloadToFile(eq("ir/exists.clp"), any(Path.class));
      doThrow(new ObjectNotFoundException("not found"))
          .when(irBackend)
          .downloadToFile(eq("ir/missing.clp"), any(Path.class));
      when(archiveBackend.uploadFromFile(eq("archive/out.clp"), any(Path.class))).thenReturn(100L);

      long size =
          creator.createArchive("archive/out.clp", List.of("ir/exists.clp", "ir/missing.clp"));

      assertTrue(size > 0);
      verify(archiveBackend).uploadFromFile(eq("archive/out.clp"), any(Path.class));
    }

    @Test
    void createArchive_emptyIrList_writesHeaderOnly() throws Exception {
      when(archiveBackend.uploadFromFile(eq("archive/out.clp"), any(Path.class))).thenReturn(100L);

      long size = creator.createArchive("archive/out.clp", List.of());

      assertTrue(size > 0);
      verify(archiveBackend).uploadFromFile(eq("archive/out.clp"), any(Path.class));
    }
  }

  @Nested
  @DisplayName("Error handling")
  class ErrorHandling {

    @Test
    void storageError_throwsStorageException() throws Exception {
      doThrow(new StorageException("read failed"))
          .when(irBackend)
          .downloadToFile(anyString(), any(Path.class));

      assertThrows(
          StorageException.class,
          () -> creator.createArchive("archive/out.clp", List.of("ir/file.clp")));
    }

    @Test
    void writeError_throwsStorageException() throws Exception {
      doAnswer(
              inv -> {
                Path localPath = inv.getArgument(1);
                Files.write(localPath, "data".getBytes());
                return null;
              })
          .when(irBackend)
          .downloadToFile(anyString(), any(Path.class));
      when(archiveBackend.uploadFromFile(anyString(), any(Path.class)))
          .thenThrow(new StorageException("write failed"));

      assertThrows(
          StorageException.class,
          () -> creator.createArchive("archive/out.clp", List.of("ir/file.clp")));
    }
  }
}
