package com.yscope.metalog.metastore.schema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides cached {@link ColumnRegistry} instances with TTL-based refresh.
 *
 * <p>When the write path allocates new dim/count slots, the read path sees them after the TTL
 * expires and the registry is reloaded from the database.
 *
 * <p>Thread-safe: {@link ConcurrentHashMap#compute} serializes reloads per key, preventing
 * thundering-herd reloads when the TTL expires. Each cache entry uses a jittered TTL (±25%) so
 * entries loaded at the same time don't all expire simultaneously.
 *
 * <p>Failed loads are cached with a shorter TTL (5s) so transient DB errors don't block recovery
 * for the full refresh window.
 */
public class ColumnRegistryProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnRegistryProvider.class);
  private static final long DEFAULT_TTL_MS = 30_000; // 30 seconds
  private static final long FAILURE_TTL_MS = 5_000; // 5 seconds
  private static final double JITTER_FACTOR = 0.25; // ±25%

  private final DSLContext dsl;
  private final long ttlMs;
  private final long failureTtlMs;
  private final ConcurrentHashMap<String, CachedRegistry> cache = new ConcurrentHashMap<>();

  public ColumnRegistryProvider(DSLContext dsl) {
    this(dsl, DEFAULT_TTL_MS, FAILURE_TTL_MS);
  }

  public ColumnRegistryProvider(DSLContext dsl, long ttlMs) {
    this(dsl, ttlMs, FAILURE_TTL_MS);
  }

  public ColumnRegistryProvider(DSLContext dsl, long ttlMs, long failureTtlMs) {
    this.dsl = dsl;
    this.ttlMs = ttlMs;
    this.failureTtlMs = failureTtlMs;
  }

  /**
   * Get a ColumnRegistry for the given table. Returns a cached instance if still fresh, otherwise
   * reloads from the database.
   *
   * <p>Uses {@code compute()} to serialize reloads per key — only one thread queries the DB for a
   * given table at a time. Other threads for the same key block briefly; different keys are
   * uncontended.
   *
   * @return the registry, or null if the registry tables don't exist
   */
  public ColumnRegistry get(String table) {
    CachedRegistry entry =
        cache.compute(
            table,
            (key, cached) -> {
              long now = System.currentTimeMillis();
              if (cached != null && (now - cached.loadedAt) < cached.effectiveTtlMs) {
                return cached;
              }

              try {
                ColumnRegistry registry = new ColumnRegistry(dsl, key);
                return new CachedRegistry(registry, now, jitteredTtl(ttlMs));
              } catch (Exception e) {
                LOG.debug("Column registry unavailable for table {}: {}", key, e.getMessage());
                return new CachedRegistry(null, now, jitteredTtl(failureTtlMs));
              }
            });
    return entry.registry;
  }

  /**
   * Get the read-only view for the given table's registry. Convenience method for callers that only
   * need resolution (lookup) operations and no allocation.
   *
   * @return the reader, or null if the registry is unavailable
   */
  public ColumnRegistryReader getReader(String table) {
    ColumnRegistry registry = get(table);
    return registry != null ? registry.reader() : null;
  }

  /** Force evict a table's cached registry (e.g., after schema changes). */
  public void evict(String table) {
    cache.remove(table);
  }

  /** Apply ±25% jitter to a base TTL so entries don't expire in lockstep. */
  private static long jitteredTtl(long baseTtl) {
    double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 2 * JITTER_FACTOR;
    return (long) (baseTtl * jitter);
  }

  private record CachedRegistry(ColumnRegistry registry, long loadedAt, long effectiveTtlMs) {}
}
