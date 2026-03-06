# Research Papers

[← Back to docs](../README.md)

Academic research papers behind [CLP](https://github.com/y-scope/clp) (Compressed Log Processor).

---

## Papers

### CLP: A Practical Log Compression and Search System (OSDI 2021)

**Published:** 15th USENIX Symposium on Operating Systems Design and Implementation (OSDI 2021)

CLP compresses text logs losslessly by up to two orders of magnitude more than general-purpose compressors like gzip, and searches the compressed logs without decompression. Designed for unstructured logs (plain text), CLP uses timestamp extraction, dictionary encoding, and variable encoding to achieve high compression ratios.

**Key contributions:**
- Novel compression algorithm for unstructured text logs
- Searchable compression (query without decompression)
- Consistently ~2-5x better compression than gzip on the same workload
- 2.7x faster search than decompressing and grepping

**Relevance to CLP Metastore Service:**
- Foundation for CLP IR (Intermediate Representation) format tracked by the metastore
- Searchable compression enables queries on CLP files without decompression
- Semantic search capabilities leveraged by the metastore's multi-layered query model

**Links:** [USENIX page](https://www.usenix.org/conference/osdi21/presentation/rodrigues) · [GitHub](https://github.com/y-scope/clp)

### CLP-S: Structured Log Compression (OSDI 2024)

**Published:** OSDI 2024

CLP-S extends CLP to structured logs (JSON, key-value pairs). It uses schema inference, column-oriented compression, and semantic-aware encoding to achieve high compression while preserving queryability.

**Key contributions:**
- Schema inference from structured logs
- Column-oriented compression (separate encoding per field)
- 10-100x compression ratio for structured logs
- Semantic queries on compressed data (field filters, aggregations)

**Relevance to CLP Metastore Service:**
- Schema generation for metastore tables
- Dimension extraction heuristics
- Compression strategies for dimension values

**Links:** [USENIX page](https://www.usenix.org/conference/osdi24)

---

## Key Concepts

### Compression Techniques

**From CLP (OSDI 2021):**
- **Timestamp extraction:** Separate timestamp from log message
- **Dictionary encoding:** Common strings stored once, referenced by ID
- **Variable encoding:** Numeric values compressed with variable-length encoding
- **Pattern extraction:** Log templates extracted, variables separated

**From CLP-S (OSDI 2024):**
- **Schema inference:** Automatically detect field structure
- **Column-oriented:** Compress each field separately (better compression)
- **Type-aware encoding:** Different encoding for string, int, float, timestamp

### Search Capabilities

**Semantic search (without decompression):**
- Wildcard search: `*error*` matches any log with "error"
- Regex search: `user-[0-9]+` matches user IDs
- Structured queries: `WHERE level = ERROR AND service = 'api'`
- Aggregations: `COUNT(*) GROUP BY service`

**Performance:**
- 2-10x faster than decompress + grep
- Sub-second search on TB-scale logs
- Parallel search across compressed chunks

### Applications to Metastore

**From CLP research:**
1. Path compression — apply dictionary encoding to common path prefixes
2. Dimension encoding — store common dimension values once
3. Hash-based lookup — similar to CLP's dictionary lookup
4. Compression-aware indexing — index on compressed representation

**From CLP-S research:**
1. Auto-schema generation — infer dimensions from log fields
2. Column-oriented storage — each dimension compressed separately
3. Type inference — detect dimension types (string, int, enum)
4. Semantic queries — filter by dimension values without decompression

---

## Further Reading

**CLP Project:**
- GitHub: https://github.com/y-scope/clp
- Documentation: https://docs.yscope.com/

**OSDI Conference:**
- OSDI 2021: https://www.usenix.org/conference/osdi21
- OSDI 2024: https://www.usenix.org/conference/osdi24

**Related Papers:**
- LogReducer: Identify and reduce log heterogeneity (FAST 2021)
- Drain: Online log parsing (ICWS 2017)
- LogCluster: Log template mining (ICSM 2016)

## See Also

- [Glossary](../concepts/glossary.md) — CLP terminology and compression concepts
- [Consolidation](../concepts/consolidation.md) — How CLP-S schema inference is applied
- [Architecture Overview](../concepts/overview.md) — System overview and data lifecycle
