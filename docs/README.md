# CLP Metastore Service Documentation

[Back to README](../README.md)

## What is the CLP Metastore?

The CLP Metastore Service is a file-level metadata catalog for CLP's compressed log archives. It coordinates ingestion from Kafka and gRPC, tracks file lifecycle states, distributes consolidation tasks to workers, and exposes metadata via gRPC for query engines like Presto. By tracking per-file dimensions, aggregates, and time bounds, it enables queries to skip millions of files and terminate early — without opening a single log file.

## Reading Paths

**New to CLP?**
[Data Model](concepts/data-model.md) → [Architecture Overview](concepts/overview.md) → [Quickstart](getting-started/quickstart.md)

**Deploying to production?**
[Quickstart](getting-started/quickstart.md) → [Deployment](operations/deployment.md) → [Deploy HA](guides/deploy-ha.md) → [Performance Tuning](operations/performance-tuning.md)

**Querying metadata?**
[Query Execution](concepts/query-execution.md) → [gRPC API Reference](reference/grpc-api.md) → [Metadata Tables Reference](reference/metadata-tables.md)

**Extending the codebase?**
[Architecture Overview](concepts/overview.md) → [Task Queue](concepts/task-queue.md) → [Metadata Schema](concepts/metadata-schema.md) → [Design Documents](design/README.md)

**Understanding CLP's semantic capabilities?**
[Semantic Extraction](concepts/semantic-extraction.md) → [gRPC API Reference](reference/grpc-api.md)

---

## Concepts

Understanding-oriented explanations of how the system works.

- [Data Model](concepts/data-model.md) — What data CLP handles, file formats, deployment modes
- [Architecture Overview](concepts/overview.md) — System overview, data lifecycle, goroutine model, components
- [Metadata Schema](concepts/metadata-schema.md) — Entry types, lifecycle, denormalization rationale, partitioning
- [Ingestion Paths](concepts/ingestion.md) — gRPC and Kafka ingestion, BatchingWriter, choosing a path
- [Consolidation](concepts/consolidation.md) — IR→Archive pipeline, policies, worker workflow
- [Task Queue](concepts/task-queue.md) — Database-backed task queue, claim protocol, recovery
- [Query Execution](concepts/query-execution.md) — Pruning pipeline, early termination, query catalog
- [Semantic Extraction](concepts/semantic-extraction.md) — log-surgeon, MPT, ERTs, LLM-powered schema generation
- [Glossary](concepts/glossary.md) — CLP terminology: data formats, schema columns, lifecycle states

## Getting Started

Tutorial-oriented guides to get you up and running.

- [Quickstart](getting-started/quickstart.md) — Prerequisites, build, run, verify
- [Tutorial: End-to-End Ingestion](getting-started/tutorial-ingestion.md) — Walk through the full Kafka ingestion pipeline

## Guides

Task-oriented how-to guides for specific operations.

- [Configure Tables](guides/configure-tables.md) — Table registration, feature flags, registry sub-tables
- [Deploy HA](guides/deploy-ha.md) — Coordinator HA setup, heartbeat vs lease, graceful migration
- [Scale Workers](guides/scale-workers.md) — Worker pool scaling, Docker Compose
- [Evolve Schema](guides/evolve-schema.md) — Online DDL, adding dimensions and aggregates
- [Integrate CLP](guides/integrate-clp.md) — Worker-CLP binary integration
- [Write Transformers](guides/write-transformers.md) — Normalizing Kafka producer schemas

## Reference

Look-up material for specific APIs, schemas, and conventions.

- [gRPC API Reference](reference/grpc-api.md) — gRPC services, proto messages, filter expressions, examples
- [Configuration Reference](reference/configuration.md) — node.yaml, env vars, API server config
- [Metadata Tables Reference](reference/metadata-tables.md) — DDL, column reference, index reference
- [Naming Conventions](reference/naming-conventions.md) — Column, index, file, and state naming patterns
- [Platform Comparison](reference/platform-comparison.md) — CLP vs Iceberg, Elasticsearch, ClickHouse, Loki
- [Research Papers](reference/research.md) — Academic papers behind CLP (OSDI 2021, OSDI 2024)

## Operations

Production operation, monitoring, and troubleshooting.

- [Deployment](operations/deployment.md) — Production deployment patterns
- [Performance Tuning](operations/performance-tuning.md) — Benchmarks, DSN gotchas, index strategy
- [Monitoring](operations/monitoring.md) — Health checks, metrics, alerting
- [Port Configuration](operations/port-configuration.md) — Customizing infrastructure and application ports
- [Troubleshooting](operations/troubleshooting.md) — Common issues and fixes

## Design Documents

ADR-style documents capturing the "why" behind architectural decisions.

- [Design Documents Index](design/README.md)
- [Coordinator HA Design](design/coordinator-ha.md) — Edge cases, walkthroughs, data model, design alternatives
- [Early Termination Design](design/early-termination.md) — Runnable example, Presto integration, streaming cursors
- [Keyset Pagination](design/keyset-pagination.md) — PK-based keyset cursor design
