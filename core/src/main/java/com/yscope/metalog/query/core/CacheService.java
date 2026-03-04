package com.yscope.metalog.query.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yscope.metalog.query.api.ApiServerConfig;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TTL-based caching using Caffeine.
 *
 * <p>Separate caches for different data types with appropriate TTLs:
 *
 * <ul>
 *   <li>Schema cache (5 min) - Rarely changes
 *   <li>File cache (30 sec) - May update during lifecycle
 *   <li>Query cache (10 sec) - Frequently changing
 * </ul>
 */
public class CacheService {

  private static final Logger LOG = LoggerFactory.getLogger(CacheService.class);

  private final Cache<String, Object> schemaCache;
  private final Cache<String, Object> fileCache;
  private final Cache<String, Object> queryCache;
  private final Cache<String, Object> sketchCache;

  public CacheService(ApiServerConfig.CacheConfig config) {
    this.schemaCache =
        Caffeine.newBuilder()
            .maximumSize(config.maxSize() / 10) // 10% for schema
            .expireAfterWrite(config.schemaTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();

    this.fileCache =
        Caffeine.newBuilder()
            .maximumSize(config.maxSize() / 2) // 50% for files
            .expireAfterWrite(config.fileTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();

    this.queryCache =
        Caffeine.newBuilder()
            .maximumSize(config.maxSize() / 10 * 4) // 40% for queries
            .expireAfterWrite(config.queryTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();

    this.sketchCache =
        Caffeine.newBuilder()
            .maximumSize(config.maxSize() / 10) // 10% for sketches
            .expireAfterWrite(config.sketchTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();

    LOG.info(
        "Cache initialized: schema={}s, file={}s, query={}s, sketch={}s, maxSize={}",
        config.schemaTtlSeconds(),
        config.fileTtlSeconds(),
        config.queryTtlSeconds(),
        config.sketchTtlSeconds(),
        config.maxSize());
  }

  /** Get or compute schema data. */
  @SuppressWarnings("unchecked")
  public <T> T getSchema(String key, Supplier<T> loader) {
    return (T) schemaCache.get(key, k -> loader.get());
  }

  /** Get or compute file metadata. */
  @SuppressWarnings("unchecked")
  public <T> T getFile(String key, Supplier<T> loader) {
    return (T) fileCache.get(key, k -> loader.get());
  }

  /** Get or compute query result. */
  @SuppressWarnings("unchecked")
  public <T> T getQuery(String key, Supplier<T> loader) {
    return (T) queryCache.get(key, k -> loader.get());
  }

  /** Get or compute sketch metadata. */
  @SuppressWarnings("unchecked")
  public <T> T getSketch(String key, Supplier<T> loader) {
    return (T) sketchCache.get(key, k -> loader.get());
  }

  /** Invalidate schema cache entry. */
  public void invalidateSchema(String key) {
    schemaCache.invalidate(key);
  }

  /** Invalidate file cache entry. */
  public void invalidateFile(String key) {
    fileCache.invalidate(key);
  }

  /** Get cache statistics for monitoring. */
  public CacheStats getStats() {
    return new CacheStats(
        schemaCache.stats().hitRate(),
        fileCache.stats().hitRate(),
        queryCache.stats().hitRate(),
        sketchCache.stats().hitRate(),
        schemaCache.estimatedSize(),
        fileCache.estimatedSize(),
        queryCache.estimatedSize(),
        sketchCache.estimatedSize());
  }

  public record CacheStats(
      double schemaHitRate,
      double fileHitRate,
      double queryHitRate,
      double sketchHitRate,
      long schemaSize,
      long fileSize,
      long querySize,
      long sketchSize) {}
}
