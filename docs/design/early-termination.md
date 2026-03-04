# Early Termination Design

[← Back to docs](../README.md)

Integration details, runnable examples, and advanced topics for the early termination query optimization. For the core algorithm and levels 1-3, see [Query Execution](../concepts/query-execution.md).

> **Column naming note:** SQL and DDL examples in this document use illustrative names (e.g., `dim_str128_service`, `agg_gte_level_error`) for readability. These are **not** the actual physical column names — in the database, columns use opaque sequential names (`dim_f01`, `agg_f01`, ...) with type and field mapping stored in `_dim_registry` and `_agg_registry`. See [Naming Conventions](../reference/naming-conventions.md) for current column naming and [Schema Evolution](../guides/evolve-schema.md) for column lifecycle.

---

## Runnable Example

<details>
<summary><strong>Runnable Java Example (EarlyTerminationDemo.java)</strong></summary>

Copy and run this self-contained Java file:

```java
import java.util.*;

/**
 * Early Termination Demo
 *
 * Compile: javac EarlyTerminationDemo.java
 * Run:     java EarlyTerminationDemo
 */
public class EarlyTerminationDemo {

    public static void main(String[] args) {
        List<FileMetadata> files = createSampleFiles();

        System.out.println("""
            EXAMPLE 1: No Filter (LIMIT 100)
            Matching count = totalCount
            """);
        runExample(files, 100, CountMode.TOTAL);

        System.out.println("""

            EXAMPLE 2: WHERE level = 'ERROR' (LIMIT 20)
            Matching count = errorPlusCount - fatalPlusCount
            """);
        runExample(files, 20, CountMode.ERROR_ONLY);

        System.out.println("""

            EXAMPLE 3: WHERE level IN ('ERROR', 'FATAL') (LIMIT 50)
            Matching count = errorPlusCount (cumulative, no subtraction)
            """);
        runExample(files, 50, CountMode.ERROR_AND_FATAL);
    }

    static List<FileMetadata> createSampleFiles() {
        return Arrays.asList(
            //              name       created  lastMod  total  error+  fatal+
            new FileMetadata("file01",  300,     450,     60,     25,      8),
            new FileMetadata("file02",  280,     420,     55,     22,      6),
            new FileMetadata("file03",  320,     400,     45,     18,      5),
            new FileMetadata("file04",  290,     380,     50,     20,      4),
            new FileMetadata("file05",  150,     270,     40,     15,      3),
            new FileMetadata("file06",  100,     250,     35,     12,      2),
            new FileMetadata("file07",   80,     200,     30,     10,      2),
            new FileMetadata("file08",   50,     150,     25,      8,      1)
        );
    }

    enum CountMode {
        TOTAL,           // No filter: use totalCount
        ERROR_ONLY,      // level = 'ERROR': use errorPlus - fatalPlus
        ERROR_AND_FATAL  // level >= 'ERROR': use errorPlus
    }

    static long getMatchingCount(FileMetadata file, CountMode mode) {
        return switch (mode) {
            case TOTAL -> file.totalCount;
            case ERROR_ONLY -> file.errorPlusCount - file.fatalPlusCount;
            case ERROR_AND_FATAL -> file.errorPlusCount;
        };
    }

    static void runExample(List<FileMetadata> files, int limit, CountMode mode) {
        List<FileMetadata> selected = selectFilesWithEarlyTermination(files, limit, mode);

        long totalMatching = selected.stream()
                .mapToLong(f -> getMatchingCount(f, mode))
                .sum();

        System.out.printf("RESULT: %d / %d files selected (%.0f%% pruned), %d events (need %d)%n",
                selected.size(), files.size(),
                100.0 * (files.size() - selected.size()) / files.size(),
                totalMatching, limit);
    }

    static List<FileMetadata> selectFilesWithEarlyTermination(
            List<FileMetadata> allFiles, int limit, CountMode mode) {

        List<FileMetadata> sortedFiles = new ArrayList<>(allFiles);
        sortedFiles.sort((a, b) -> Long.compare(b.lastModifiedAt, a.lastModifiedAt));

        List<FileMetadata> selectedFiles = new ArrayList<>();
        long cumulativeCount = 0;
        long watermark = Long.MAX_VALUE;
        long cutoffTs = Long.MIN_VALUE;

        for (FileMetadata file : sortedFiles) {
            long matchingCount = getMatchingCount(file, mode);

            if (file.lastModifiedAt < cutoffTs) {
                break;
            }

            selectedFiles.add(file);
            cumulativeCount += matchingCount;
            watermark = Math.min(watermark, file.createdAt);

            if (cutoffTs == Long.MIN_VALUE && cumulativeCount >= limit) {
                cutoffTs = watermark;
            }
        }

        return selectedFiles;
    }

    static class FileMetadata {
        final String name;
        final long createdAt;
        final long lastModifiedAt;
        final long totalCount;
        final long errorPlusCount;
        final long fatalPlusCount;

        FileMetadata(String name, long createdAt, long lastModifiedAt,
                     long totalCount, long errorPlusCount, long fatalPlusCount) {
            this.name = name;
            this.createdAt = createdAt;
            this.lastModifiedAt = lastModifiedAt;
            this.totalCount = totalCount;
            this.errorPlusCount = errorPlusCount;
            this.fatalPlusCount = fatalPlusCount;
        }
    }
}
```

</details>

---

## Presto Integration

**Where it fits:**

```
Presto Query Lifecycle:

1. Parser       - Parse SQL
2. Analyzer     - Resolve tables, columns
3. Optimizer    - Push down predicates
4. SplitManager - Early termination here (key optimization point)
5. Scheduler    - Distribute splits
6. Workers      - Scan files
```

**Eligibility:**

```
ELIGIBLE:
  SELECT * FROM t WHERE x='a' ORDER BY ts DESC LIMIT 100

NOT ELIGIBLE:
  SELECT * FROM t WHERE x='a'              -- no LIMIT
  SELECT * FROM t ORDER BY id LIMIT 100    -- not ordered by timestamp
  SELECT COUNT(*) FROM t WHERE x='a'       -- aggregation
```

**Split generation:**

```
User Query
    |
    v
+----------------------------------------------------------+
| 1. Query MySQL metadata                                   |
|    SELECT * FROM clp_ir_files                             |
|    WHERE dim_str128_service = 'auth'                      |
|      AND agg_gte_level_error > 0                         |
|    ORDER BY max_timestamp DESC                            |
+----------------------------------------------------------+
    |
    v
+----------------------------------------------------------+
| 2. Compute matching counts                                |
|    matching = gte_level_error - gte_level_fatal            |
+----------------------------------------------------------+
    |
    v
+----------------------------------------------------------+
| 3. Generate splits with early termination                 |
|    Split 1: File A (newest), matching=20                  |
|    Split 2: File B, matching=30                           |
|    ...accumulated=50+, cutoff reached, DONE               |
+----------------------------------------------------------+
```

**All operations at metadata level:**

| Operation | Accuracy |
|-----------|----------|
| Dimension filter | 100% exact (file-level constant) |
| Time range filter | 100% exact (known bounds) |
| Level filter | 100% exact (pre-computed) |
| Matching count | 100% exact (direct or derived) |

No estimation, no scanning, just exact counts from metadata.

---

## Streaming Cursors

**Why streaming matters:**

| Metric | Without Streaming | With Streaming |
|--------|-------------------|----------------|
| Files matching filters | 500 | 500 |
| Files actually needed | approximately 5 | approximately 5 |
| Rows transferred | 500 | approximately 5 |

```java
// Bad: loads ALL rows into memory
List<File> files = jdbc.query("SELECT ...", mapper);

// Good: streams rows on demand
stmt.setFetchSize(Integer.MIN_VALUE);  // MySQL streaming
ResultSet rs = stmt.executeQuery("SELECT ...");
while (rs.next()) {
    if (done) break;  // Only approximately 5 rows transferred
}
```

**Database configuration:**

| Database | Configuration |
|----------|---------------|
| MySQL | `setFetchSize(Integer.MIN_VALUE)` |
| PostgreSQL | `setFetchSize(100)` + `autoCommit=false` |
| Oracle | `setFetchSize(100)` |

---

## Configuration

| Mechanism | Use Case |
|-----------|----------|
| Catalog properties | Consistent defaults |
| Session properties | Per-query control |
| Separate catalogs | Explicit full-scan vs optimized |

**Catalog properties:**
```properties
clp.streaming-early-termination.enabled=true
clp.streaming-early-termination.max-limit=10000  # queries with LIMIT > max-limit fall back to full scan
```

**Session properties:**
```sql
SET SESSION clp.streaming_early_termination_enabled = true;
```

**Dual catalogs:**
```sql
SELECT COUNT(*) FROM clp.data WHERE level='ERROR';        -- full scan
SELECT * FROM clp_streaming.data ORDER BY ts DESC LIMIT 100;   -- optimized
```

---

## Edge Cases

| Case | Handling |
|------|----------|
| No aggregation column | Use `record_count` as upper bound (over-selects but correct) |
| ORDER BY ASC | Sort by `min_timestamp ASC`, track `max(max_timestamp)`, terminate when `min_timestamp > max` |
| Full overlap | No pruning possible; algorithm selects all files (correct) |
| LIMIT > total | Selects all files naturally |

---

## Metadata Schema Summary

```sql
-- Time range (for ordering and overlap pruning)
min_timestamp          INT UNSIGNED NOT NULL DEFAULT 0,
max_timestamp          INT UNSIGNED NOT NULL DEFAULT 0,

-- Total event count (for basic early termination)
record_count           INT UNSIGNED NOT NULL,

-- File-level dimensions (for filtered early termination)
-- Physical names are opaque placeholders: dim_f01, dim_f02, ...
-- Illustrative names used here for readability
dim_service     VARCHAR(128) NULL,
dim_host        VARCHAR(128) NULL,
dim_path        VARCHAR(1024) NULL,
dim_port        SMALLINT UNSIGNED NULL,

-- Per-field aggregations (for non-dimension filters)
-- Physical names are opaque placeholders: agg_f01, agg_f02, ...
-- Illustrative names used here for readability
agg_gte_level_warn   INT UNSIGNED NOT NULL DEFAULT 0,
agg_gte_level_error  INT UNSIGNED NOT NULL DEFAULT 0,
agg_gte_level_fatal  INT UNSIGNED NOT NULL DEFAULT 0,
```

**Semantic contracts:** The `dim_` and `agg_` column families signal invariants:
- **`dim_*`**: All rows in the file share the same value for this field.
- **`agg_*`**: The count accurately reflects the number of matching rows in the file.

These invariants must be maintained by the ingestion pipeline.

For current column naming (physical `dim_fNN`/`agg_fNN` names and registry-based type mapping), see [Naming Conventions](../reference/naming-conventions.md) and [Metadata Tables Reference](../reference/metadata-tables.md).

---

## Limitations

| Limitation | Implication |
|------------|-------------|
| **Time-ordered only** | `ORDER BY timestamp` only |
| **File-level dimensions** | `dim_*` requires all events share same value |
| **Explicit aggregation columns** | `agg_*` needs schema changes for new filters |
| **Cumulative ordering** | `_gte` assumes severity ordering |

**Custom filters fall back to scanning:**

Filters outside `dim_*` and `agg_*` (for example, `message LIKE '%error%'`) require scanning. However, they can be combined effectively:

```sql
WHERE application_id = 'app-123'      -- dim_*: 250K files reduced to 500 files
  AND level = 'ERROR'                 -- agg_*: 500 files reduced to 50 files
  AND message LIKE '%OutOfMemory%'    -- scan 50 files (acceptable)
```

Users investigating issues typically know the service, application, or host. These filters narrow the search before content filtering occurs.

## Beyond Logs: Other Data Types

The optimization applies to any data with these properties:

| Requirement | What It Means |
|-------------|---------------|
| Known time bounds | `min_timestamp` and `max_timestamp` tracked per file |
| File-level dimensions | For `dim_*` fields, all rows share the same value |
| Pre-computed counts | Counts for common filter values tracked during ingestion |

**Example: Metrics**

```sql
SELECT * FROM metrics
WHERE service = 'payments' AND latency_bucket = 'p99'
ORDER BY timestamp DESC LIMIT 50
```

**Example: Traces**

```sql
SELECT * FROM traces
WHERE service = 'checkout' AND status = 'ERROR'
ORDER BY timestamp DESC LIMIT 20
```

**Key insight:** The optimization does not require logs specifically; it requires data with the right properties.

---

## Future Optimization: Time Bucketing

At very large scale, add time bucketing as a pre-filtering layer.

**When to consider:**

| Trigger | Threshold |
|---------|-----------|
| Metadata table size | > 1M rows |
| Typical query time range | > 4 hours |
| Query latency p99 | Exceeds SLA |

Start without bucketing. Add it via schema evolution when needed.

**How it works:**

```
Phase 1: Select buckets (coarse)
  - Query bucket aggregates (24 rows for 1 day)
  - Apply watermark at bucket level

Phase 2: Select files (fine)
  - Query files from selected buckets only
  - Apply exact watermark algorithm
```

**Impact:**

| Query Range | Without Bucketing | With Bucketing | Reduction |
|-------------|-------------------|----------------|-----------|
| Last 1 hour | 500 | 500 | 0% |
| Last 24 hours | 12,000 | approximately 500 | 96% |
| Last 7 days | 84,000 | approximately 500 | 99% |

**Why it can wait:** All required data is already captured. Bucketing can be derived from existing data without requiring schema changes.

---

## Implementation Checklist

### Core Algorithm
- [ ] Query orders by `max_timestamp DESC`
- [ ] Termination check at top of loop: `file.max_timestamp < cutoff_ts`
- [ ] Watermark = `min(min_timestamp)` of selected files
- [ ] `cutoff_ts` locked once when `cumulative >= limit`
- [ ] Cursor closed on early termination

### Streaming
- [ ] MySQL: `setFetchSize(Integer.MIN_VALUE)`
- [ ] PostgreSQL: `setFetchSize(100)` + `autoCommit=false`

### Count Columns
- [ ] Subtraction for exact counts (`gte_level_error - gte_level_fatal`)
- [ ] Fallback to `record_count` when specific count missing

### Configuration
- [ ] Separate `clp` and `clp_streaming` catalogs
- [ ] `max-limit` configured

### Testing
- [ ] Unit test with mock files
- [ ] Edge cases: no matches, LIMIT > total, full overlap

---

## See Also

- [Query Execution](../concepts/query-execution.md) — Core algorithm, levels 1-3, problem statement
- [Metadata Tables Reference](../reference/metadata-tables.md) — Column schema and type conventions
- [Keyset Pagination](keyset-pagination.md) — How `SplitQueryEngine` pages through metadata using `(max_timestamp, id)` cursors
- [Platform Comparison](../reference/platform-comparison.md) — How early termination compares across systems
- [Performance Tuning](../operations/performance-tuning.md) — Benchmarks and tuning
