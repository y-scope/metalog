# Platform Comparison

[← Back to docs](../README.md)

Every platform makes trade-offs. This document explains what CLP optimizes for, what it doesn't, and when to use it versus alternatives.

> **Document Depth:** This is a comprehensive comparison document (~15 min read). For quick orientation, read the [Executive Summary](#executive-summary-why-clp-is-different) and [When to Use CLP](#when-to-use-clp). Detailed per-platform analyses are in the appendices for readers who want to go deeper.

**Related:**
- [Metadata Tables Reference](metadata-tables.md)
- [Early Termination Design](../design/early-termination.md) — Algorithm details

> **Column naming note:** Examples in this document use illustrative names (e.g., `dim_str128_service`, `agg_gte_level_error`) for readability. These are not the actual physical column names — in the database, columns use opaque sequential names (`dim_f01`, `agg_f01`, ...) mapped through `_dim_registry`/`_agg_registry`. See [Naming Conventions](naming-conventions.md) and [Schema Evolution](../guides/evolve-schema.md).

---

## Overview

CLP is purpose-built for **schema-less time-series data**—logs, events, traces, telemetry—where:
- Schema varies per record (different fields, types, nesting)
- Time-ordered queries dominate ("most recent N matching events")
- Storage cost matters at scale (petabytes of data)
- Semantic understanding enables smarter search

### Key Design Choices

| # | Differentiator | What It Means | Impact |
|---|----------------|---------------|--------|
| 1 | **Semantic Compression (~32x)** | Extract log types, store template once | 63,500 messages → ~37 templates |
| 2 | **Schema Tree (MPT)** | Preserve structure + extraction provenance | Query any path, expose to LLMs |
| 3 | **Metastore-Level Early Termination** | Pre-computed `agg_*` per file | Skip files entirely, not just prune |
| 4 | **Multi-ERT Architecture** | Multiple schemas in one file | Schema-less with columnar efficiency |
| 5 | **Object Storage First** | Sequential reads, O(1) API calls | 100x cost reduction vs Parquet seeks |

#### 1. Semantic Compression (~32x)

```
Raw logs → CLP extracts: "Connection to <ip>:<port> failed after <int>ms"
         → Template stored ONCE, variables separately
         → ~32x compression (vs ~5-10x for Parquet/Nimble)
```

#### 2. Schema Tree with Meaning & Provenance

```
CLP schema tree:
  msg                    ← original message
  ├── cc: "4111-..."     ← extracted FROM msg (provenance preserved)
  └── amount: 500        ← extracted FROM msg

Nimble/Parquet: msg, msg_cc, msg_amount  ← peer columns, relationship LOST
```

#### 3. Metastore-Level Filtering & Early Termination

```
File metadata:
  dim_str128_service: "auth"        ← File-level constant
  agg_gte_level_error: 52           ← Pre-computed count

Query: "100 recent ERRORs from service=auth"
→ 250,000 files → ~5 files touched (via watermark algorithm)
```

**Unique to CLP:** Hive, Iceberg, Delta Lake, Hudi track total row counts but not per-filter counts. See [Early Termination Design](../design/early-termination.md) for algorithm details.

#### 4. Multi-ERT Architecture

```
CLP-Archive file:
  ERT-1: columns {ts, level, msg}       ← Records with these fields
  ERT-2: columns {ts, host, error_code} ← Different schema, same file
  Shared: MPT, dictionaries

Parquet: ONE schema per file → NULL overhead for variable fields
```

#### 5. Object Storage First Design

```
Parquet: GET footer → GET col A chunk → GET col A chunk → ... = O(columns × row groups)
CLP:     Single GET, sequential stream = O(1) API call

1M queries/day: Parquet ~$2000/day vs CLP ~$20/day in API costs
```

---

## When to Use CLP

| Use Case | CLP? | Why |
|----------|------|-----|
| High-volume logs, events, traces | **Yes** | Semantic compression, early termination |
| Schema varies per record | **Yes** | Schema-less with columnar efficiency |
| Time-ordered "most recent N" queries | **Yes** | Metastore-level early termination |
| Semantic/intelligent search on logs | **Yes** | Embed ~37 log types, expose structure to LLMs |
| Structured analytics (known schema) | No | Use Iceberg/Delta |
| ML feature tables (wide, fixed) | No | Use Nimble |
| Vector similarity (embeddings, RAG) | No | Use LanceDB |
| Ad-hoc full-text search | No | Use Elasticsearch |

---

## Table of Contents

**Core Content**
- [Overview](#overview) — Key design choices
- [When to Use CLP](#when-to-use-clp) — Decision guide
- [Comparison Dimensions](#comparison-dimensions) — File format vs metastore vs full system
- [Schema Models](#schema-models) — Schema-on-write vs schema-less
- [Multi-Layer Filtering](#multi-layer-filtering-architecture) — How CLP prunes 250K files → 5 files
- [Platform Comparison Summary](#platform-comparison-summary) — Key capability tables
- [Query Model](#query-model-comparison) — SQL + schema-less UDFs
- [Visual Trade-offs](#visual-comparison-trade-off-diagrams) — Positioning diagrams
- [Summary](#summary) — When to use what

**Appendices (Deep Dives)**
- [Appendix A: Schema Models Deep Dive](#appendix-a-schema-models-deep-dive) — Type polymorphism, nested data
- [Appendix B: Platform Details](#appendix-b-platform-details) — Per-platform analysis
- [Appendix C: Index-Free Approaches](#appendix-c-index-free-approaches-compared) — LogScale vs Loki vs CLP

---

## Comparison Dimensions

This document compares systems at three levels:

```
Level 3: Full System — MongoDB, Elasticsearch, ClickHouse, Loki
         Document/row-level indexing, integrated storage + query engine

Level 2: Table Format + Metastore — Iceberg, Delta Lake, Hive, Hudi, CLP Metadata
         File-level metadata, schema management, query planning

Level 1: File Format — Parquet, ORC, Nimble, Lance, CLP-IR/Archive
         Compression, column encoding, within-file indexing
```

**Key distinction:** CLP tracks per-filter counts (`agg_*`); others track only total row counts. This enables metastore-level early termination.

---

## Schema Models

| Aspect | Schema-on-Write | Schema-on-Read | Schema-less (CLP) |
|--------|-----------------|----------------|-------------------|
| **Schema defined** | Before write | At query time | Auto-discovered |
| **Write flexibility** | Rigid | Flexible | Flexible |
| **New fields** | Requires ALTER | Just write | Just write |
| **Query performance** | Fast | Slow | Fast (pre-indexed) |

**Systems:** Schema-on-write (Parquet, Iceberg, ClickHouse) | Schema-less (CLP, Elasticsearch, MongoDB)

See [Appendix A](#appendix-a-schema-models-deep-dive) for type polymorphism and nested data handling.

---

## Multi-Layer Filtering Architecture

CLP achieves query performance through a **three-layer filtering cascade**:

```
Layer 1: Table-Level — Query specific table

Layer 2: Metadata-Level (CLP Metastore)
         Time bounds, dimensions (dim_*), aggregations (agg_*)
         Pruning: 250,000 files → ~500 files → ~50 files
         Early termination: Watermark algorithm → ~5 files

Layer 3: Within-File (CLP Internal)
         Schema tree (MPT), ERTs, log types, variable dictionary
         Columnar access to relevant fields only
```

**Query flow example:**

| Layer | Action | Remaining |
|-------|--------|-----------|
| 1. Table | Query target table | 1 table |
| 2. Metadata | `dim_str128_service = 'auth'` | 500 files |
| 2. Metadata | `agg_gte_level_error > 0` | 50 files |
| 2. Metadata | Watermark → early termination | 5 files |
| 3. CLP | Map predicates to relevant ERT | Scan ~10% of file data |

---

## Platform Comparison Summary

### Capability Matrix

| Capability | Hive | Iceberg | Nimble | ES | LogScale | CH | MongoDB | Loki | CLP |
|------------|------|---------|--------|-----|----------|-----|---------|------|-----|
| Schema-less content | - | - | - | Yes | Yes | - | Yes | Yes | **Yes** |
| Type polymorphism | - | - | - | - | Partial | - | Yes | Partial | **Yes** |
| High compression | Partial | Partial | Partial | - | Yes | Yes | - | Partial | **Yes** |
| Pre-computed counts | - | - | - | - | - | - | - | - | **Yes** |
| Early termination | - | - | - | - | - | - | Partial | - | **Yes** |
| Low index overhead | - | Yes | Yes | - | Yes | Partial | Partial | Yes | **Yes** |
| Standard SQL | Yes | Yes | Yes | Partial | - | Yes | - | - | **Yes** |

*ES = Elasticsearch, CH = ClickHouse*

### Compression & Cost Comparison

| System | Compression | Index Overhead | Cost at Scale |
|--------|-------------|----------------|---------------|
| **CLP** | ~32x (semantic) | ~5% (file-level) | $ |
| LogScale | ~15x (LZ4) | Low (Bloom) | $$ |
| ClickHouse | 5-10x | 10-30% | $$ |
| Iceberg/Parquet | 3-5x | Low (stats) | $$ |
| Loki | ~4x (gzip) | Very low (labels) | $$ |
| Elasticsearch | ~0.5x (expansion) | 100-200% | $$$$ |

See [Appendix B](#appendix-b-platform-details) for detailed per-platform analysis.

---

## Query Model Comparison

### CLP: SQL + Schema-less Extensions

```sql
-- Standard SQL on explicit columns
SELECT * FROM logs
WHERE dim_str128_service = 'auth' AND level = 'ERROR'
ORDER BY timestamp DESC LIMIT 100;

-- Schema-less extensions via UDFs
SELECT * FROM logs
WHERE CLP_GET_STRING('kubernetes.pod_name') = 'auth-xyz'
  AND REGEX_LIKE(message, 'connection.*refused');
```

### Available UDFs

| UDF | Purpose |
|-----|---------|
| `CLP_GET_STRING('<path>')` | Access nested string field |
| `CLP_GET_INT('<path>')` | Access nested int field |
| `CLP_GET_JSON_STRING()` | Full record as JSON |
| `CLP_WILDCARD_COLUMN()` | Search all columns |

---

## Visual Comparison: Trade-off Diagrams

### Schema Flexibility vs Optimization Strategy

```
                     Data Organization Strategy
                                     ^
                                     |                              *
          C                          |
          I                          |
          H                          |                          L
   Schema-on-write ------------------+-----------------------------> Schema-less
                                     |
                                     |                E               M
                       Index-based Strategy

Legend: * CLP | I Iceberg | C ClickHouse | H Hive | E Elasticsearch | M MongoDB | L Loki
```

### Compression vs Index Overhead

```
                             High Compression (10-30x+)
                                     ^
                                     |                                    *
              C                      |                               S
   High Index Overhead --------------+-----------------------------> Low Index Overhead
          E                          |                          I   V   L
                             Low Compression (<5x)

Legend: * CLP | S LogScale | I Iceberg | C ClickHouse | E Elasticsearch | L Loki | V LanceDB
```

---

## Summary

### Platform Strengths

| Category | Systems | What They Excel At |
|----------|---------|-------------------|
| **Data Lake** | Iceberg, Delta | ACID, time travel, ecosystem |
| **Wide-Schema** | Nimble | ML feature stores, 1000s of columns |
| **Vector DB** | LanceDB | Similarity search, embeddings |
| **Search** | Elasticsearch | Full-text, relevance scoring |
| **Streaming Logs** | LogScale | Kafka-native, ~15x compression |
| **OLAP** | ClickHouse | Fast aggregations |
| **Lightweight Logs** | Loki | Kubernetes-native, Grafana |
| **CLP** | CLP + Metastore | ~32x compression, early termination |

### When to Use What

| Use Case | Recommended |
|----------|-------------|
| Structured data warehouse | Iceberg/Delta |
| Wide-schema ML tables | Nimble |
| ML embeddings, RAG | LanceDB |
| Ad-hoc full-text search | Elasticsearch |
| Real-time streaming logs | LogScale |
| K8s logs, Grafana | Loki |
| **High-volume logs, streaming queries** | **CLP** |

---

## Appendix A: Schema Models Deep Dive

### Type Polymorphism

**Definition:** The same field can have different types across records.

```json
{"user_id": 12345}           // integer
{"user_id": "user-abc-123"}  // string
{"user_id": null}            // null
```

**How systems handle it:**

| System | Handling | Consequence |
|--------|----------|-------------|
| Parquet/Iceberg | Reject or cast | Data loss or failure |
| Elasticsearch | Dynamic mapping conflict | Field unusable after conflict |
| MongoDB | Accept (BSON) | Works, query complexity increases |
| **CLP** | **Native support** | Each type stored efficiently |

**CLP's approach:**
```sql
WHERE CLP_GET_INT('user_id') = 12345        -- matches int
WHERE CLP_GET_STRING('user_id') = 'user-abc' -- matches string
```

### Nested and Hierarchical Data

Schema-on-write systems require predefined schemas for nested data:

```sql
-- ClickHouse: Must predefine every path
CREATE TABLE logs (
  kubernetes_namespace String,
  kubernetes_pod_name String,
  kubernetes_pod_labels Map(String, String),  -- loses nested structure
  -- ... 50+ columns for one nested object
);
```

**CLP's approach:** Query any path without predefined schema:
```sql
SELECT * FROM logs
WHERE CLP_GET_STRING('kubernetes.pod.labels.app') = 'auth'
  AND CLP_GET_INT('error.details.user.id') = 12345;
```

---

## Appendix B: Platform Details

### Data Lake Systems

#### Hive

**Comparison level:** Level 2 (metastore)

| Aspect | Hive | CLP |
|--------|------|-----|
| Metadata granularity | Partition-level | File-level |
| Statistics | Partition stats only | Per-file `dim_*`, `agg_*` |
| File tracking | Directory listing | Explicit registration with state |
| Lifecycle | Static partitions | Buffering → Consolidation → Done |

#### Iceberg

**Comparison level:** Level 1+2 (format + metastore)

**Metadata gap:**
```
Iceberg: column_stats: {level: min="DEBUG", max="FATAL"}  ← USELESS for filtering

CLP:     agg_gte_level_error: 523  ← EXACT COUNT, pre-computed
```

Iceberg's min/max stats don't help when files contain multiple values.

#### LanceDB

**Comparison level:** Different query model

| Aspect | LanceDB | CLP |
|--------|---------|-----|
| Primary use | Semantic similarity (kNN) | Time-ordered retrieval |
| Query model | Vector similarity | Exact match + time-range |
| Result ordering | By similarity score | By timestamp |

**Complementary:** CLP for operational traces, LanceDB for knowledge retrieval.

#### Nimble

**Comparison level:** Level 1 (file format)

| Aspect | Nimble | CLP |
|--------|--------|-----|
| Target workload | Wide structured tables | Variable-structure data |
| Schema | Required upfront | Auto-discovered |
| Column count | 1000s of fixed columns | Variable fields per record |
| Compression | Extensible encodings | Semantic (log types) |

### Log / Search Systems

#### Elasticsearch

**Comparison level:** Level 3 (full system with per-term indexing)

| Aspect | Elasticsearch | CLP |
|--------|---------------|-----|
| Primary strength | Full-text + relevance | Time-ordered streaming |
| Index granularity | Per-term | Per-file |
| Compression | ~0.5x (expansion) | ~32x |

#### LogScale (Humio)

**Comparison level:** Level 3 (index-free, closest to CLP philosophy)

| Aspect | LogScale | CLP |
|--------|----------|-----|
| Index approach | Bloom filters | Pre-computed counts |
| Compression | ~15x (LZ4) | ~32x (semantic) |
| Early termination | Scan matching segments | Skip files via counts |
| Data model | Opaque segments | Schema tree + log types |

#### ClickHouse

**Comparison level:** Level 3 (OLAP)

| Aspect | ClickHouse | CLP |
|--------|------------|-----|
| Primary strength | Fast aggregations | Early termination + compression |
| Data model | Schema-on-write | Schema-less |
| Compression | 5-10x | ~32x |

#### MongoDB

**Comparison level:** Level 3 (document store)

| Aspect | MongoDB | CLP |
|--------|---------|-----|
| Early termination level | Per-document | Per-file |
| Index strategy | Compound indexes | File-level metadata |

#### Loki

**Comparison level:** Level 2+3 (closest philosophy to CLP)

| Aspect | Loki | CLP |
|--------|------|-----|
| Label/dimension filtering | Yes | Yes |
| Pre-computed counts | No | Yes |
| Compression | ~4x (gzip) | ~32x |

---

## Appendix C: Index-Free Approaches Compared

Three systems share the philosophy of avoiding heavy per-record indexing:

| Aspect | Loki | LogScale | CLP |
|--------|------|----------|-----|
| **Content model** | Opaque bytes (gzip) | Compressed + Bloom | Schema tree + log types |
| **Variable extraction** | Regex at query time | Regex at query time | Single-pass at ingestion |
| **Early termination** | Scan all chunks | Scan matching segments | **Watermark algorithm** |
| **Compression** | ~4x | ~15x | **~32x** |
| **Expose to LLMs** | No (opaque) | No (opaque) | Yes — Schema tree, ERTs |
| **Semantic search** | No (embed raw logs) | No (embed raw logs) | **Embed ~37 log types** |

**The spectrum:**
- **Loki**: Simplest—compress with label index. Cost-effective but limited optimization.
- **LogScale**: Middle—Bloom filters for keyword search. Better than Loki, still brute-force.
- **CLP**: Most sophisticated—semantic compression, pre-computed counts, pattern matching.

**When each makes sense:**
- **Loki**: Kubernetes, Grafana ecosystem, basic query needs
- **LogScale**: Real-time streaming priority, Kafka-native
- **CLP**: Streaming queries, storage cost critical, semantic understanding

---

## Appendix D: Why Not Parquet/Nimble?

**Question:** Can CLP switch to Parquet or Nimble instead of CLP-IR/CLP-Archive?

**Short answer:** No—the value comes from encoding, not just storage.

| What You Might Think | Reality |
|---------------------|---------|
| "Store CLP output in Parquet columns" | Loses ~32x → ~5-10x compression (template deduplication gone) |
| "Use Nimble's extensibility for CLP structures" | Nimble becomes dumb container; tools can't leverage it |
| "Put dictionaries in metadata" | Parquet assumes ONE schema per file; CLP has multiple ERTs |
| "Just use Parquet with better compression" | Compression comes from *semantic understanding*, not byte patterns |

**Bottom line:** CLP's format encodes *meaning* (log types, variable relationships, schema tree). Parquet/Nimble encode *bytes*. You can't get semantic compression by storing semantically-encoded data in a syntactic container.

### IR vs Archive: Different Requirements

| Format | Architecture | Could Use Nimble? |
|--------|--------------|-------------------|
| **CLP-IR** | Row-oriented, appendable, streaming | **No** — Nimble is write-once |
| **CLP-Archive** | Columnar (ERTs), immutable | **Potentially**, but loses value |

### Why the Format Matters

**Semantic vs Syntactic Compression:**
```
Parquet:  "Connection to 10.0.1.5:3306 failed" → compressed bytes (~5-10x)
CLP:      Log type: "Connection to <ip>:<port> failed" stored ONCE
          Variables: [(10.0.1.5, 3306), (10.0.1.8, 3306)] columnar (~32x)
```

Real-world impact: Spark logs have 63,500+ unique messages that reduce to ~37 log types.

**Multi-Schema Per File:** Parquet requires ONE schema per file. CLP-Archive supports multiple ERTs (different schemas) in the same file — a fundamental data model mismatch.

| Capability | Parquet/Nimble | CLP Format |
|------------|----------------|------------|
| Schema model | Schema-on-write | **Schema-less** |
| Type polymorphism | No | **Yes** |
| Compression | ~5-10x | **~32x** |
| Log type extraction | No | **Yes** |
| Multi-schema per file | No | **Yes** (ERTs) |

### When Parquet/Nimble IS the Right Choice

| Data Type | Best Format | Why |
|-----------|-------------|-----|
| ML feature tables (1000s of columns) | Nimble | Optimized for wide schemas |
| Analytics tables (known schema) | Parquet/Iceberg | Ecosystem maturity |
| Data warehouse facts/dimensions | Parquet/Delta | ACID, time travel |
| **Logs, events, traces** | **CLP** | Schema-less, semantic compression |

### CLP-Archive Architecture

```
+----------------------------------------------------------------+
| CLP-Archive File                                                |
+----------------------------------------------------------------+
|  MPT (Merged Parse Tree)     — Compressed schema tree           |
|  Shared Dictionaries         — Log types + string variables     |
|  ERTs (Encoded Record Tables)— Columnar per schema type         |
|    Schema Type A: {ts, host, level, msg_logtype}                |
|    Schema Type B: {ts, host, error_code}                        |
+----------------------------------------------------------------+
```

What you'd lose storing this in Parquet: path queries (no MPT), cross-column deduplication (per-column only in Parquet), multi-schema (single schema only), pattern search (no log type extraction).

### Object Storage Access Patterns

```
Parquet: GET footer → GET col A chunk → GET col A chunk → ... = O(columns x row groups)
CLP:     Single GET, sequential stream = O(1) API call

1M queries/day: Parquet ~$2000/day vs CLP ~$20/day in API costs
```

---

## Appendix E: Why Not Iceberg/Hive/Delta?

**Question:** Why not use Iceberg, Hive, Delta Lake, or another existing metastore?

**Short answer:** CLP's metastore requirements are fundamentally different. Existing solutions don't fit because CLP needs high-throughput per-file updates, not batch table commits.

### Requirements Mismatch

Traditional metastores (Iceberg, Hive, Delta Lake) optimize for **features** (ACID, time travel, schema evolution). CLP's metastore optimizes for **performance** (20,000-22,000 ops/sec/table, frequent per-file lifecycle transitions, metadata-layer early termination).

A single CLP-IR file goes through **4-5+ metadata updates** during its lifecycle. At 5M files/day with 5 updates each = ~290 ops/sec average, with bursts much higher. Traditional metastores cannot sustain this because each "update" requires a full commit cycle with manifest rewrites.

| Requirement | CLP Metastore | Table Format Metastore |
|-------------|---------------|------------------------|
| **Write pattern** | 20,000-22,000 UPSERT/UPDATE per sec per table | Batch commits (hundreds/sec max) |
| **Lifecycle states** | `BUFFERING` → `PENDING` → `ARCHIVE_CLOSED` → `PURGING` | Static file registration |
| **Statistics** | `agg_*` + dimensions for early termination | Column min/max only |
| **Real-time updates** | Direct row UPDATE | Full commit cycle with manifest rewrite |

### Performance Comparison

```
Iceberg commit (per file registration):
  Read manifest list → Create new manifest → Create new manifest list → Atomic commit
  At 21,000 files/sec: 21,000 manifest rewrites/sec = unsustainable

CLP Metastore (per file UPSERT):
  INSERT ... ON DUPLICATE KEY UPDATE → Done
  At 21,000 files/sec: standard database operation
```

### Cost Efficiency

| Solution | Infrastructure | Monthly Cost |
|----------|---------------|--------------|
| **CLP Metastore** | 1 small DB instance, many tables | ~$50-100 |
| **Iceberg + REST Catalog** | Object storage + catalog + compute | Higher |
| **Managed (Glue, Unity)** | Per-request pricing | $$$ at scale |

### Horizontal Scaling

CLP scales via **independent topic/table pairs** within a shared database. Each tenant has its own Kafka topic, database table, and coordinator — logical isolation while sharing infrastructure.

### What About Other Metastores?

| Metastore | Why It Doesn't Fit |
|-----------|-------------------|
| **AWS Glue Catalog** | Per-request pricing unsustainable at 20-22k ops/sec; partition-oriented |
| **Databricks Unity** | Designed for Delta tables; vendor lock-in |
| **Apache Polaris** | Inherits Iceberg's manifest-based limitations |
| **Project Nessie** | Git-like versioning doesn't fit streaming lifecycle |
| **DataHub / Atlas** | Data governance metadata, not operational file tracking |

## See Also

- [Architecture Overview](../concepts/overview.md) — System overview
- [Metadata Tables Reference](metadata-tables.md) — CLP metastore design
- [Early Termination Design](../design/early-termination.md) — Early termination algorithm
- [Performance Tuning](../operations/performance-tuning.md) — Benchmarks and throughput analysis
