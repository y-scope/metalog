# Rust deployment and Go migration

The supported Rust server scope is gRPC ingestion/query/metadata/admin, S3-compatible storage,
retention, partition maintenance, and multi-node coordinator HA. This branch is a migration
candidate, not evidence of a completed production rollout. Kafka ingestion, consolidation workers,
aggregation/sketch ingestion, sketch predicates, and telemetry exporters are not implemented end to
end. Requests/configuration for those features fail explicitly. Keep `worker.concurrency: 0` and
`consolidation.enabled: false`.

## Build and run

Install Rust 1.98.0 and `protoc` (proto3 optional support required). The checked-in lockfile fixes
Rust dependency resolution. `make build` produces `target/release/metalog`. The Docker image uses
the same Rust version, includes TLS trust certificates, and runs as UID 10001. `docker/start.sh -d`
starts the local MariaDB/MinIO/Rust stack; `docker compose -f docker/docker-compose.yml up -d
--scale coordinator-node=2` adds a second node. Use `docker compose ... port coordinator-node 9090`
to discover the local gRPC port. Compose is a local test deployment with development credentials.

The local MinIO fixture is built from the pinned upstream source release because the former public
MinIO container images are unavailable. It is a compatibility fixture, not a recommendation to
replace a maintained production object store. Production can use AWS S3 or an existing S3-compatible
service. Configure an endpoint, region, and path-style URLs for MinIO; AWS deployments can use the
SDK credential provider chain (including workload credentials) or explicit access/secret/session
credentials. Endpoints must be set in Metalog's backend configuration; SDK endpoint overrides
from environment/shared profiles are ignored so endpoint behavior and key validation stay aligned.
Avoid embedding real credentials in committed YAML.

`docker/coordinator-node.yaml` is the supported example. Both Go-style camelCase settings/boolean
service toggles and Rust snake_case/map toggles are accepted. Unknown YAML and table-config fields
are rejected. `${VAR}` substitutions and database/port environment overrides are supported:
`DB_PRIMARY_{HOST,PORT,DATABASE,USER,PASSWORD,POOL_SIZE,POOL_MIN_IDLE}`, corresponding `DB_REPLICA_*`,
`GRPC_PORT`, `HEALTH_ENABLED`, `HEALTH_PORT`, and `COORDINATOR_NAME`. Configure a primary database;
query and metadata RPCs use the optional replica. Replica lag affects read-after-write visibility.

Enable `coordinator.enabled` on ingestion nodes. Use a unique, stable node ID per live process via
`coordinator.nodeIdEnvVar` (default `HOSTNAME`); never run two live processes with the same ID. Both
heartbeat and lease strategies maintain node membership. Heartbeat thresholds and lease TTLs must
exceed their renewal intervals. All nodes must share the same configuration and writable primary.
Fair sharing applies to unassigned tables; adding a node does not proactively move existing assignments.
Database availability and failover remain the responsibility of the database deployment. Node HA
does not make a single MariaDB instance highly available.

The server currently exposes plaintext, unauthenticated gRPC. Deploy it on a private network behind
an authenticated TLS ingress/service mesh with authorization for admin and ingestion access. Do not
expose the raw listener to untrusted clients. Filesystem storage requires an absolute administrator-
controlled root, single-component buckets, and canonical relative keys (no leading/repeated slashes,
`.`/`..`, or symlinks); all nodes need the same shared filesystem when using it for HA.

## Data and lifecycle guarantees

An accepted ingestion response means the database committed the UPSERT; stale or protected updates
can be no-ops. A batch may partially
succeed; examine failed indices and retry failures. A transport failure can occur after commit, so
clients must retry idempotently. Lower-level Rust `submit` and `submit_wait` enqueue work; use
`submit_confirmed`/`FlushReceipt::wait` when a durable acknowledgment is required. Shutdown rejects
new work, drains gRPC requests, then drains the writer. SIGINT and SIGTERM follow the same path.
`/health` is liveness; `/ready` also checks database connectivity and becomes false before shutdown.

UPSERTs reject backwards lifecycle movement. Equal-timestamp forward finalization is allowed so a
file can close without new records. Protected consolidation/purging states cannot be overwritten by
ordinary ingestion. Dimension definitions are checked across the whole batch; schema allocation is
serialized across nodes, inactive slots are not reused, and string columns widen as needed.

Ordinary queries use keyset cursors with deterministic ordering. A zero limit streams the full
result in bounded pages. SQL grouping applies before the result limit. Cursors support nullable and
typed sort values even when sort columns are omitted from the projection. Grouped requests do not
accept public pagination cursors. Streams are not database snapshots: concurrent changes to rows,
sort values, or group aggregates can change results while pages are read.

Retention is **opt-in** in Rust. A missing/NULL table configuration disables retention and
consolidation; this differs from the historical Go defaults. Register explicit settings:

```bash
target/release/metalog admin register-table --addr localhost:9090 --table web_logs \
  --config-json '{"consolidation":{"enabled":false},"retention":{"enabled":true,"type":"default","scan_interval_secs":60,"delete_rate":500}}'
```

The admin API merges configuration changes into stored JSON. Running coordinators reload changed
configuration during reconciliation. Unsupported stored configurations prevent that coordinator
from starting and are logged; inspect all existing table configurations before migration.

Expired records become `PURGING`. Their metadata remains until required storage deletion succeeds.
Failures, unknown backends, missing buckets, and cancellation leave retryable work. References in
both file and archive fields are checked across registered tables so a live shared archive is not
deleted. Storage calls occur outside SQL transactions and have bounded timeouts.

Object retirement uses durable `_storage_tombstone` rows and a database-scoped advisory lock shared
with ingestion commits. This closes the race between deleting an object and adding a new reference.
**Object names are immutable and cannot be reused after retirement.** Every persisted path must
include an explicit backend and bucket. Use one canonical backend name for each physical storage
service and identical backend mappings on every node; aliases for the same object evade identity
checks. Do not write metadata directly through SQL or a legacy Go writer while Rust retention is
active: those writers do not participate in this protocol. Do not remove tombstones to reclaim
space unless the object namespace is permanently retired and all potential writers are excluded.

Filesystem and custom S3 endpoints require canonical relative keys: no leading, repeated, or
trailing slashes, dot components, or backslashes. MinIO can resolve `/a` and `a` to the same object,
so accepting both would undermine reference checks. Ingestion rejects ambiguous locations.
Before each retention cycle, all registered tables (including inactive ones) are audited for
ambiguous keys and missing backend/bucket identities. Any violation blocks the cycle before
deletion or state changes. Resolve legacy identities through an explicit migration with retention
disabled; keys are never silently rewritten. Native AWS S3 preserves literal keys, but still
requires explicit backend/bucket identities. The audit may scan metadata across all tables for
each retention cycle; account for that database load when choosing scan intervals.

The lock serializes short ingestion commit/retirement decisions across tables. Measure sustained
throughput and retention backlog on your workload; this correctness choice may become a throughput
bottleneck. Tombstones grow with retired objects. Retention issues individual object deletes, not
S3 version enumeration: versioned buckets need an appropriate bucket lifecycle for noncurrent
versions. Physical metadata partitions are provisioned ahead; old partitions are not compacted or
dropped automatically by this Rust scope.

## Validation and rollout

```bash
make test
make test-integration  # Docker required; builds the pinned MinIO fixture
cargo test --locked -p metalog --test production_smoke
make lint
make fmt              # requires nightly-2026-08-30 rustfmt
```

Integration tests cover real MariaDB writes/queries, rejected writes and durable receipts,
concurrent dimension allocation, cursor/type/null regressions, more than 100,000 streamed rows,
retention failure/restart/cancellation/shared references, object retirement races, S3/MinIO object
operations, and HA membership/lease/start-stop behavior. Process tests exercise gRPC across nodes,
owner death, SIGTERM, and startup bind failure. These are regression checks, not long-running soak or
capacity tests.

Before switching production, restore a backup into an isolated database/object namespace and replay
representative ingestion. Compare Go/Rust query output and lifecycle state, including boundary
timestamps, dimensions, Unicode paths, shared archives, and idempotent retries. Start with retention
disabled, verify configuration and object identities, then enable it only in the isolated namespace.
Run failover/load tests using your actual replica and ingress topology and size pools/thresholds from
those results. Set alerts for ingestion failures, database readiness, coordinator startup errors,
retention failures/PURGING backlog, and tombstone growth (structured logs and database inspection;
no Rust telemetry exporter yet).

For cutover, stop Go writers/coordinators, drain accepted writes, take a restorable backup, and start
Rust nodes against the selected database. Do not run mixed Go/Rust retention or writers. Preserve the
Go deployment as a rollback option, but disable retention and account for Rust tombstones before
rolling back: Go does not enforce retired-object names. Cutover approval should depend on workload
comparisons and a successful soak, not only this test suite.
