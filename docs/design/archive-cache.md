# Archive Cache Design

[← Back to docs](../README.md)

## Problem

Metalog stores one row per file. Archive-level queries (e.g., "find all archives overlapping a time range") require `GROUP BY archive_path` which scans all file rows to aggregate. At scale (millions of archives, tens of millions of files), this becomes expensive for every search query.

## Current Approach

`StreamSplits` with `group_by=["archive_path"]` and caller-specified aggregations (`MIN(min_timestamp)`, `MAX(max_timestamp)`, etc.). This works but performs a full GROUP BY on every query.

## Why Not a Dedicated Archive Table with FK?

A normalized two-table design (archive table + file table with FK) was considered and rejected:

- **FK overhead**: Every file INSERT requires a parent-row lookup + shared lock on the archive row. At ~32K records/sec ingestion throughput, this is significant.
- **Write-path complexity**: Ingestion must coordinate upserts across two tables.
- **Schema complexity**: Two table schemas, two sets of indexes, two partition maintenance strategies.
- **Sync risk**: Keeping both tables consistent adds failure modes with minimal benefit over the single-table approach.

## Proposed Extension: Materialized Archive Cache Table

Instead of a FK relationship, maintain a **materialized cache** of archive-level aggregates. The ingestion write path stays simple (single table), and archive queries hit the cache instead of running GROUP BY.

### Schema

```sql
CREATE TABLE _archive_cache (
    archive_path       VARCHAR(1024) PRIMARY KEY,
    table_name         VARCHAR(64) NOT NULL,
    min_timestamp      BIGINT NOT NULL,
    max_timestamp      BIGINT NOT NULL,
    total_record_count BIGINT NOT NULL,
    total_raw_size     BIGINT NOT NULL,
    archive_size_bytes BIGINT NOT NULL,
    storage_backend    VARCHAR(32),
    bucket             VARCHAR(63),
    file_count         INT NOT NULL,
    last_updated_at    BIGINT NOT NULL,       -- epoch nanos
    INDEX idx_time (table_name, max_timestamp DESC)
);
```

### Refresh Strategy

After each BatchIngest flush, enqueue affected archive_paths for cache refresh. A background task periodically processes the queue:

1. Dequeue batch of dirty archive_paths
2. Run targeted GROUP BY only for those paths:
   ```sql
   SELECT archive_path, MIN(min_timestamp), MAX(max_timestamp), SUM(record_count), ...
   FROM <table> WHERE archive_path IN (?) GROUP BY archive_path
   ```
3. UPSERT into `_archive_cache`

This bounds the GROUP BY to a small set of recently-modified archives rather than the full table.

### Query Path

`StreamSplits` with `group_by` checks the cache table first. If the cache covers the request, return directly from the cache with standard keyset pagination (the cache table has its own indexed rows). Otherwise fall back to live GROUP BY on the file table.

### Consistency

Eventual — cache may be a few seconds behind ingestion. Acceptable for search queries since the archive must already be fully written and closed before it is searchable.

### Benefits

- Zero impact on ingestion write path (no FK, no two-table upsert)
- Archive queries become simple indexed lookups on `_archive_cache`
- Keyset pagination works on the cache table (each row has its own identity)
- Cache table is small (one row per archive vs N rows per archive in the file table)

## When to Implement

The GROUP BY approach is sufficient for most deployments. Consider implementing the archive cache when:

- Archive discovery queries exceed 100ms p99 latency
- The file table exceeds 10M rows
- Search query volume is high enough that GROUP BY becomes a bottleneck

## API Impact

None. The `StreamSplits` API with `group_by` stays the same. The cache is a server-side optimization — callers don't need to change.
