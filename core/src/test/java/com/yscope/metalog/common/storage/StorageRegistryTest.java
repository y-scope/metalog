package com.yscope.metalog.common.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("StorageRegistry")
class StorageRegistryTest {

  private StorageBackend irBackend;
  private StorageBackend archiveBackend;
  private StorageRegistry registry;

  @BeforeEach
  void setUp() {
    irBackend = mock(StorageBackend.class);
    archiveBackend = mock(StorageBackend.class);
    when(irBackend.name()).thenReturn("minio");
    when(archiveBackend.name()).thenReturn("minio");

    Map<String, StorageBackend> all = new LinkedHashMap<>();
    all.put("ir", irBackend);
    all.put("archive", archiveBackend);
    registry = new StorageRegistry(irBackend, archiveBackend, all);
  }

  @Nested
  @DisplayName("Accessors")
  class Accessors {

    @Test
    void getIrBackend_returnsIrBackend() {
      assertSame(irBackend, registry.getIrBackend());
    }

    @Test
    void getArchiveBackend_returnsArchiveBackend() {
      assertSame(archiveBackend, registry.getArchiveBackend());
    }

    @Test
    void getBackend_byName_returnsCorrect() {
      assertSame(irBackend, registry.getBackend("ir"));
      assertSame(archiveBackend, registry.getBackend("archive"));
    }

    @Test
    void getBackend_unknown_throwsIllegalArgument() {
      assertThrows(IllegalArgumentException.class, () -> registry.getBackend("unknown"));
    }

    @Test
    void getAllBackends_returnsUnmodifiable() {
      Map<String, StorageBackend> all = registry.getAllBackends();
      assertThrows(UnsupportedOperationException.class, () -> all.put("new", irBackend));
    }
  }

  @Nested
  @DisplayName("close")
  class Close {

    @Test
    void closesAllBackends() {
      registry.close();
      verify(irBackend).close();
      verify(archiveBackend).close();
    }

    @Test
    void closeSwallowsExceptions() {
      doThrow(new RuntimeException("close failed")).when(irBackend).close();
      assertDoesNotThrow(() -> registry.close());
      verify(archiveBackend).close();
    }
  }
}
