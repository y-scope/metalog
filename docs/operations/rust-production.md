# Rust deployment and Go migration

The supported Rust server scope is gRPC ingestion/query/metadata/admin, S3-compatible storage,
consolidation, retention, partition maintenance, and multi-node coordinator HA. Kafka ingestion,
aggregation ingestion and telemetry exporters remain unavailable and fail explicitly. See the
[Rust consolidation guide](rust-consolidation.md) for worker setup, policy parity, recovery, and
operational limits. Integration tests validate the lifecycle; production throughput and a staged
rollout still need validation against your workload.

## Build and run

Install Rust 1.98.0 and `protoc` (proto3 optional support required). The checked-in lockfile fixes
Rust dependency resolution. `make build` produces `target/release/metalog`. The Docker image uses
the same Rust version, includes TLS trust certificates and pinned CLP 0.14.1 `clp-s`, and runs as UID
10001. Compose enables one consolidation worker per node; tables opt in separately. `docker/start.sh -d`
starts the local MariaDB/ProxySQL/MinIO/Rust stack; `docker compose -f docker/docker-compose.yml up -d
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

## ProxySQL and direct metadata readers

The Compose stack routes all Metalog database traffic through pinned ProxySQL 3.0.11. The
`database.primary` pool uses port 6033 and `metalog_writer`; the `database.replica` read pool uses
port 6034 and `metalog_reader`. Both hostgroups point to the same MariaDB instance, preserving
read-after-write visibility. The reader principal has database-enforced SELECT privileges only.
External SQL readers can use the same read endpoint:

```bash
mysql --protocol=TCP -h127.0.0.1 -P6034 -umetalog_reader -p metalog_metastore
# Local development password: metalog_reader_dev
```

`PROXYSQL_WRITER_PORT` and `PROXYSQL_READER_PORT` override the published loopback ports. MariaDB's
direct port remains available for local administration. ProxySQL's admin port 6032 is not published.
The idempotent `database-init` service recreates the three dedicated development accounts on existing
volumes as well as fresh databases, clearing old grants and roles. Restart proxy sessions when
changing credentials or privileges. Use managed secrets, restricted networks and TLS for production;
neither these development passwords nor this local Compose topology constitute a production deployment.

The checked-in [proxy configuration](../../docker/proxysql.cnf) separates writer and reader policies:

- Writers have session affinity (`multiplex=0`) to the single authoritative server in hostgroup 10.
  Ingestion, DDL and retention use advisory locks across multiple autocommit statements, so merely
  pinning SQL transactions is insufficient. Do not add SELECT-based read splitting for this account.
- Readers use hostgroup 20 with multiplexing enabled and eight backend connections per proxy
  instance. Many idle or short, stateless client sessions can share these connections. Transactions,
  session state and some client SQL patterns restrict sharing; this is not a promise of unlimited
  active queries. Keep transactions short and size limits/timeouts using the actual workload.
- Query retries and query caching are disabled. A failed protected write must surface an error,
  rather than continue on another backend without its lock. Backend session state and advisory
  locks are discarded on frontend disconnect before reuse. SQL comment routing overrides are
  disabled, and the real database version is forwarded for Metalog's SQL dialect detection.

The writer backend cap is 200, while total frontend connections are capped at 2048 in this example.
Account for the sum of writer pools across all Metalog nodes, administrative clients and readers
when sizing MariaDB. Each additional ProxySQL instance has its own caps. Inspect
`stats_mysql_connection_pool`, `stats_mysql_processlist` and query digests through the private admin
interface to observe connection use and slow queries. The SQL reader credentials, not a routing
regex, enforce read-only access.

Compose starts ProxySQL with `--initial` and an ephemeral datadir: **the configuration file is the
source of truth**. Runtime admin edits, including `SAVE ... TO DISK`, do not survive recreation or
restart. Update the managed configuration for lasting topology changes. Metalog readiness checks
both endpoints; a reader-only outage makes `/ready` fail even while writer ingestion remains usable.
Readiness verifies connectivity, not replication freshness or every privilege.

For a future database migration, keep a stable proxy endpoint but perform a controlled cutover:

1. Prepare and validate a compatible MySQL/MariaDB target, including grants, schema, all metadata,
   node/assignment state and `_storage_tombstone`. Disable retention during the migration window.
2. Stop/drain all writers and coordinators and fence the old database against further writes.
   Finish replication or a consistent final copy and verify it before opening the target.
3. Update both hostgroups in managed configuration, restart proxy connections, then restart
   Metalog and verify ingestion, queries, membership and retention before resuming normal traffic.

Do not load-balance writers across independent primaries: advisory locks are server-local. Marking
a backend `OFFLINE_SOFT` does not force pinned sessions away. In-flight operations and transactions
cannot be moved transparently; clients must reconnect and retry idempotently after ambiguous
transport failures. ProxySQL does not replicate data, select/fence a new primary, or translate
Metalog's SQL/schema into a different database engine. A production HA topology also needs redundant
proxies and a tested database failover mechanism; the local stack has one of each.

These policies follow ProxySQL's [multiplexing rules](https://proxysql.com/documentation/multiplexing/),
[MySQL variables](https://proxysql.com/documentation/global-variables/mysql-variables/) and
[configuration persistence model](https://proxysql.com/documentation/configuration-file/).

## Network and storage configuration

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

## Per-field Bloom filters and dimension indexes

Rust accepts per-file Bloom snapshots through `MetadataRecord.sketch` in unary and batch ingestion.
Server-streaming queries can use them for pruning. Each entry contains a logical field name and a MessagePack map with
`type: "parquet_sbbf_xxhash64"` and binary `data` (nonempty 32-byte blocks). Metalog stores these maps
under `sketches` in the LZ4-framed `ext` column and allocates durable `_sketch_registry` slots. The
server validates and stores snapshots; producers must build them from **every value in the complete
file**. Values are UTF-8 bytes hashed with xxHash64 seed 0; case and Unicode normalization matter.
A valid but incomplete snapshot cannot be detected by the server and can cause missed matches.

Set `StreamSplitsRequest.sketch_expression`, for example:

```text
trace_id = 'abc-123'
`user.id` IN ('alice', 'bob') AND trace_id = 'abc-123'
```

Equality and `IN` accept string literals, with `AND` and parentheses. Other operators, numeric
literals, and combining sketches with `group_by` are rejected. Metadata predicates still belong in
`filter_expression`. Bloom predicates reduce the candidate files returned; consumers must perform
exact event-level filtering inside those files. `splits_scanned` counts examined candidates and
`splits_matched` counts emitted files. Limits/cursors apply after pruning.

Only `IR_CLOSED` and `ARCHIVE_CLOSED` rows are pruned. Missing, unreadable, oversized, unknown-format,
or malformed filters keep the candidate; unregistered or invalidated fields also pass through.
Each predicate also requires the current registry slot in that row’s `sketches` SET.
Accepted newer updates replace all supplied snapshots;
updates without sketches clear previous snapshots. Equal-timestamp forward closing transitions can
attach the final snapshot; same-state replays and stale/protected updates cannot overwrite it.
Existing files without snapshots remain queryable and receive no pruning benefit.

There are 64 lifetime sketch slots per table. Each ingestion record accepts at most 64 fields and
1 MiB of serialized snapshots. Sketch expressions are bounded to 16 KiB, 64 predicates, and 1024
values. Queued raw snapshots share a 64 MiB budget per writer; decoded blobs have a 16 MiB limit.
The legacy Go Bloom library uses modulo block selection and pair-swapped salts, unlike standard
Parquet, under the same type name. Rust checks both interpretations before pruning, preserving both
formats at the cost of additional false positives. See [sketch compatibility](../concepts/sketches.md#cross-language-compatibility)
for reproducible cross-language fixtures.

For dimensions that are constant within each file, configure ordinary database indexes by logical
field name through the existing admin API:

```bash
target/release/metalog admin register-table --addr localhost:9090 --table web_logs \
  --config-json '{"indexes":["service","region","latency"]}'
```

Up to 32 distinct fields can be configured. Coordinators reconcile indexes in the background at
startup, after config reload, and every 60 seconds; fields not allocated yet wait until a later pass. Numeric/boolean
columns receive full indexes. String indexes cover up to 191 characters, with exact SQL predicates
checking the complete value. Full indexes qualify dimensions for the query API's indexed-sort
check; prefix string indexes still require `allow_unindexed_sort` for full-string sorting.

`{"indexes":[]}` removes only indexes under the reserved `idx_metalog_dim_fNN` names. Operational,
primary, and manually named indexes remain intact. Reconciliation serializes with dimension
allocation and uses online DDL (`ALGORITHM=INPLACE, LOCK=NONE`); unsupported online DDL fails visibly
instead of falling back to a blocking rebuild. DDL may still briefly wait on metadata locks, and
index construction consumes database resources and serializes new dimension allocation/widening.
Existing compatible dimensions continue ingesting. Stored config is the desired state; inspect
`information_schema.STATISTICS` and reconciliation logs to confirm it has been applied.

## Validation and rollout

```bash
make test
make test-integration  # Docker required; builds the pinned MinIO fixture
make test-proxysql     # reruns DB + real-process tests through the deployment proxy policy
cargo test --locked -p metalog --test production_smoke
make lint
make fmt              # requires nightly-2026-08-30 rustfmt
```

Integration tests cover real MariaDB writes/queries, rejected writes and durable receipts,
concurrent dimension/sketch allocation, Bloom compatibility and pruning, configured-index query plans,
cursor/type/null regressions, more than 100,000 streamed rows,
retention failure/restart/cancellation/shared references, object retirement races, S3/MinIO object
operations, and HA membership/lease/start-stop behavior. Process tests exercise gRPC across nodes,
owner death, SIGTERM, and startup bind failure. These are regression checks, not long-running soak or
capacity tests.

ProxySQL regressions additionally exercise prepared advisory locks followed by text-protocol writes,
lock release on cancellation/disconnect, backend-session loss, read-only grants, concurrent reader
multiplexing, dialect detection, and proxy restart recovery. The same database suite runs directly
and through ProxySQL in CI. Real-process tests cover the separate query pool, reader-only outage,
readiness recovery, durable ingestion, and two-node takeover through the proxy.

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
