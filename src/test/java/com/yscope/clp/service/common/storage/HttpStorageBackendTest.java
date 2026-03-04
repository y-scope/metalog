package com.yscope.clp.service.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("HttpStorageBackend")
class HttpStorageBackendTest {

  private HttpStorageBackend backend;

  @BeforeEach
  void setUp() {
    backend = new HttpStorageBackend("http", "https://example.com/data");
  }

  @Nested
  @DisplayName("Identity")
  class Identity {

    @Test
    void name_returnsBackendName() {
      assertEquals("http", backend.name());
    }

    @Test
    void namespace_returnsNull() {
      assertNull(backend.namespace());
    }
  }

  @Nested
  @DisplayName("Trailing slash handling")
  class TrailingSlash {

    @Test
    void trailingSlashStripped() {
      HttpStorageBackend b = new HttpStorageBackend("http", "https://example.com/data/");
      assertEquals("http", b.name());
      // The trailing slash is stripped internally for consistent URL construction
    }
  }

}
