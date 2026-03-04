package com.yscope.metalog.query.core;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.metalog.query.api.ApiServerConfig;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CacheService")
class CacheServiceTest {

  private CacheService cacheService;

  @BeforeEach
  void setUp() {
    var cacheConfig = new ApiServerConfig.CacheConfig(100, 300, 60, 30, 60);
    cacheService = new CacheService(cacheConfig);
  }

  // ==========================================
  // Schema Cache
  // ==========================================

  @Nested
  @DisplayName("Schema cache")
  class SchemaCache {

    @Test
    void cachesOnFirstCall() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      String result1 = cacheService.getSchema("key1", () -> {
        loaderCalls.incrementAndGet();
        return "schema-data";
      });
      String result2 = cacheService.getSchema("key1", () -> {
        loaderCalls.incrementAndGet();
        return "schema-data-2";
      });

      assertEquals("schema-data", result1);
      assertEquals("schema-data", result2);
      assertEquals(1, loaderCalls.get(), "Loader should only be called once for same key");
    }

    @Test
    void differentKeysCallLoaderForEach() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      cacheService.getSchema("key1", () -> {
        loaderCalls.incrementAndGet();
        return "data1";
      });
      cacheService.getSchema("key2", () -> {
        loaderCalls.incrementAndGet();
        return "data2";
      });

      assertEquals(2, loaderCalls.get());
    }

    @Test
    void invalidateRemovesEntry() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      cacheService.getSchema("key1", () -> {
        loaderCalls.incrementAndGet();
        return "data";
      });

      cacheService.invalidateSchema("key1");

      cacheService.getSchema("key1", () -> {
        loaderCalls.incrementAndGet();
        return "data-new";
      });

      assertEquals(2, loaderCalls.get(), "Loader should be called again after invalidation");
    }
  }

  // ==========================================
  // File Cache
  // ==========================================

  @Nested
  @DisplayName("File cache")
  class FileCache {

    @Test
    void cachesOnFirstCall() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      String result1 = cacheService.getFile("f1", () -> {
        loaderCalls.incrementAndGet();
        return "file-data";
      });
      String result2 = cacheService.getFile("f1", () -> {
        loaderCalls.incrementAndGet();
        return "file-data-2";
      });

      assertEquals("file-data", result1);
      assertEquals("file-data", result2);
      assertEquals(1, loaderCalls.get());
    }

    @Test
    void invalidateRemovesEntry() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      cacheService.getFile("f1", () -> {
        loaderCalls.incrementAndGet();
        return "data";
      });

      cacheService.invalidateFile("f1");

      cacheService.getFile("f1", () -> {
        loaderCalls.incrementAndGet();
        return "data-new";
      });

      assertEquals(2, loaderCalls.get());
    }
  }

  // ==========================================
  // Query Cache
  // ==========================================

  @Nested
  @DisplayName("Query cache")
  class QueryCache {

    @Test
    void cachesOnFirstCall() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      Integer result1 = cacheService.getQuery("q1", () -> {
        loaderCalls.incrementAndGet();
        return 42;
      });
      Integer result2 = cacheService.getQuery("q1", () -> {
        loaderCalls.incrementAndGet();
        return 99;
      });

      assertEquals(42, result1);
      assertEquals(42, result2);
      assertEquals(1, loaderCalls.get());
    }
  }

  // ==========================================
  // Sketch Cache
  // ==========================================

  @Nested
  @DisplayName("Sketch cache")
  class SketchCache {

    @Test
    void cachesOnFirstCall() {
      AtomicInteger loaderCalls = new AtomicInteger(0);
      cacheService.getSketch("s1", () -> {
        loaderCalls.incrementAndGet();
        return "sketch";
      });
      cacheService.getSketch("s1", () -> {
        loaderCalls.incrementAndGet();
        return "sketch-2";
      });

      assertEquals(1, loaderCalls.get());
    }
  }

  // ==========================================
  // Stats
  // ==========================================

  @Nested
  @DisplayName("Cache stats")
  class Stats {

    @Test
    void returnsNonNullStats() {
      CacheService.CacheStats stats = cacheService.getStats();
      assertNotNull(stats);
      assertEquals(0, stats.schemaSize());
      assertEquals(0, stats.fileSize());
      assertEquals(0, stats.querySize());
      assertEquals(0, stats.sketchSize());
    }
  }
}
