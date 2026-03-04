# Data Model

[← Back to docs](../README.md)

What data CLP handles, how files serve as indexes, and the deployment modes that follow from this design.

**Related:** [Architecture Overview](overview.md) · [Metadata Schema](metadata-schema.md) · [Glossary](glossary.md)

---

## What Data CLP Handles

CLP targets **append-only, timestamped, semi-structured records** — not just logs, but any stream of timestamped data: application logs, audit trails, event streams, agent traces, and telemetry. These share key properties:

| Property | Description |
|----------|-------------|
| **Append-only** | Records are immutable once written |
| **Timestamped** | Every record has a time field |
| **Semi-structured** | Known schema patterns with variable content |
| **High volume** | 10K-100K hosts producing data continuously |

CLP is not limited to logs. Any data with these properties can benefit from the same optimizations.

---

## File Formats

Files exist in two formats, both using schema-free semantic compression:

| Format | Description | Use Case |
|--------|-------------|----------|
| **IR** (Intermediate Representation) | Lightweight, streamable, appendable. Semantically compressed at the edge. | Real-time ingestion, tail-like access |
| **Archive** | Columnar format. Higher compression, richer metadata, semantic enrichment. | Analytical queries, long-term storage |

Both formats are directly queryable. The choice between them is a latency vs efficiency trade-off:

- **IR-only**: Fastest to produce and query, but larger storage footprint
- **Archive-only**: Best compression and query performance, but requires consolidation
- **Hybrid (IR → Archive)**: IR for immediate availability, consolidated to Archive in the background

---

## The File IS an Index

Most systems treat indexing and compression as opposing forces — index more for faster queries, compress more for cheaper storage. CLP sidesteps this trade-off entirely. Because CLP understands the *semantics* of the data (log types, variables, schema structure), it compresses data *and* produces queryable metadata in the same pass. The metadata that enables file skipping is a byproduct of compression, not an additional cost.

| Traditional Approach | CLP Approach |
|---------------------|--------------|
| Row-level indexes (B-tree, inverted) | File-level metadata (dimensions, counts, sketches) |
| Indexes maintained on every write | Metadata captured at file creation |
| Index size often exceeds data size | Metadata is ~5% of data size |
| Write amplification from index updates | Zero write amplification |

Each CLP file contains:
- **Time bounds** (`min_timestamp`, `max_timestamp`) — enables overlap-based pruning
- **Dimensions** — file-level constants (e.g., service, host) that apply to every record
- **Counts** — pre-computed subset counts for common filter values
- **Sketches** — probabilistic membership filters (bloom, cuckoo)
- **Semantic model** — auto-discovered structure (log types, field types, relationships)

This metadata enables the query engine to skip files without opening them. See [Query Execution](query-execution.md) for how pruning and early termination work.

---

## Deployment Modes

The entry type in the metadata table determines how files flow through the system:

| Mode | Entry Type | Flow | When to Use |
|------|-----------|------|-------------|
| **IR-only** | `IR_BUFFERING → IR_CLOSED` | Store IR, no consolidation | Real-time access, short retention |
| **Archive-only** | `ARCHIVE_CLOSED` | Archives uploaded directly | Pre-consolidated data, batch pipelines |
| **Hybrid** | `IR_ARCHIVE_BUFFERING → ... → ARCHIVE_CLOSED` | IR ingested, consolidated to Archive | Most production deployments |

See [Architecture Overview: Data Lifecycle](overview.md#data-lifecycle) for the full state diagram.

---

## See Also

- [Architecture Overview](overview.md) — System overview, data lifecycle, components
- [Metadata Schema](metadata-schema.md) — Entry types, storage design, partitioning
- [Query Execution](query-execution.md) — How file-level metadata enables query optimization
- [Semantic Extraction](semantic-extraction.md) — MPT, ERTs, log types
- [Glossary](glossary.md) — CLP terminology and data format definitions
