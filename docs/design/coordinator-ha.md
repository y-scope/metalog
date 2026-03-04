# Coordinator HA Design

[← Back to docs](../README.md)

Design details, edge cases, walkthroughs, and data model for coordinator high availability. For operational setup and configuration, see [Deploy HA](../guides/deploy-ha.md). For the coordinator's goroutine model, see [Architecture Overview](../concepts/overview.md).

---

## How It Works

### Node Startup

When a node starts:

1. Resolve its `node_id` (see [Appendix A.1](#a1-node_id-resolution))
2. Initialize coordination schema (`CREATE TABLE IF NOT EXISTS` for `_table*` and `_node_registry`)
3. Signal liveness — heartbeat mode: register in `_node_registry`; lease mode: renew `lease_expiry` on any previously owned tables. This happens before claiming so other nodes can see this node as alive when computing fair share.
4. Claim unassigned active tables via staggered fight-for-master — each node independently tries to claim tables, and the database ensures only one succeeds per table (tables are expected to be pre-registered; if added later, the reconciliation goroutine picks them up on its next cycle):
   ```
   loop:
     compute fair_share = ceil(currently_assigned_tables / active_nodes)
     if my_load >= fair_share → done
     query one unassigned active table
     if none → done
     attempt atomic claim (UPDATE ... WHERE node_id IS NULL)
     if claimed → signal liveness (lease mode), short random delay (0–1s), repeat
   ```
   Fair share is computed from currently assigned tables (those with a non-NULL `node_id`), not all active tables. Recomputing between each claim gives concurrent nodes time to become visible — especially in lease mode, where a node's first claim creates its first visible lease. This prevents a single fast-starting node from claiming all tables.
5. Start a per-table coordinator for each assigned table
6. Start background goroutines:

   Per-node (one instance regardless of how many tables the node owns):
   - **gRPC server** — accepts metadata from gRPC clients; not HA-related (see [Ingestion Paths](../concepts/ingestion.md))
   - **Reconciliation goroutine** — claims unassigned tables, detects dead owners, reconciles running units with DB assignments
   - **Liveness goroutine** — heartbeat mode writes to `_node_registry`; lease mode renews `lease_expiry`
   - **Watchdog goroutine** — checks per-goroutine progress and restarts stalled coordinators (see [Health Monitoring](#health-monitoring))
   - **Partition maintenance** — runs partition management for all active tables (not just owned ones), coordinated via advisory locks (database-level mutexes that prevent concurrent DDL across nodes; see [Partitioning](../concepts/metadata-schema.md#partitioning))

   Per-table (one instance per claimed table):
   - **Kafka consumer** — if enabled, polls Kafka and submits records to the node-level BatchingWriter (see [Ingestion Paths](../concepts/ingestion.md))
   - **Lifecycle goroutines** — planner, retention, storage deletion

### Liveness Detection

At each liveness interval, the node tells the database it's still alive:

```sql
-- Heartbeat mode (default): one UPSERT per node (registers on first call, refreshes thereafter)
INSERT INTO _node_registry (node_id, last_heartbeat_at)
VALUES (?, UNIX_TIMESTAMP())
ON DUPLICATE KEY UPDATE last_heartbeat_at = UNIX_TIMESTAMP();

-- Lease mode: one UPDATE per owned table
UPDATE _table_assignment SET lease_expiry = UNIX_TIMESTAMP() + ? WHERE node_id = ?;
```

If the database is temporarily unreachable, the node logs an error and retries next cycle. A failed liveness write does not stop processing — the liveness goroutine is independent of per-table coordinators.

Liveness is determined from timestamps — no status flags:

| Mode | Alive | Stale | Dead |
|------|-------|-------|------|
| Heartbeat | Heartbeat < 30s ago | Heartbeat 30s–180s ago | Heartbeat > 180s ago (or no row) |
| Lease | `lease_expiry` > 30s from now | `lease_expiry` < 30s from now | `lease_expiry` in the past |

(Values shown use defaults: 30s liveness interval, 180s dead threshold.)

**Stale is informational only** — no action is triggered until Dead. Stale nodes are still treated as alive for fair-share calculations; the stale window gives transient network glitches time to resolve before triggering recovery.

In lease mode, the dead threshold (TTL) is baked into `lease_expiry` when it's set (`lease_expiry = NOW() + TTL`), so checking `lease_expiry < NOW()` is sufficient — no separate threshold parameter needed for detection.

### Orphan Detection and Recovery

At each reconciliation interval, each node looks for orphaned assignments and tries to take them over. See [Appendix B](#appendix-b-walkthrough-examples) for step-by-step examples with concrete database rows.

**Step 1 — Find orphans.** Find active tables whose owner is dead or missing — these are the tables that need a new owner.

In heartbeat mode, liveness is stored in a separate table (`_node_registry`), so the query joins assignments against the registry to check each table owner's heartbeat. A `LEFT JOIN` is used because a node may have never registered (crashed before its first heartbeat) — in that case, the join produces NULL, which counts as dead.

In lease mode, liveness is stored directly on `_table_assignment.lease_expiry`, so no join is needed — each row carries its own owner's liveness.

Both queries return the same thing: a list of tables that need a new owner. In heartbeat mode, the `node_id` of the dead owner is used as a compare-and-swap guard in step 2: the claim UPDATE includes `WHERE node_id = <dead_owner>`, so if another node already claimed it, the UPDATE matches zero rows. In lease mode, the expired `lease_expiry` itself serves as the guard — the claim checks `WHERE lease_expiry < NOW()`, so if another node already renewed or claimed the lease, the UPDATE matches zero rows.

```sql
-- Heartbeat mode: join _node_registry to check each owner's heartbeat
SELECT a.table_name, a.node_id
FROM _table_assignment a
JOIN _table t ON a.table_name = t.table_name
LEFT JOIN _node_registry n ON a.node_id = n.node_id
WHERE t.active = true
  AND a.node_id IS NOT NULL
  AND (n.node_id IS NULL                                          -- never registered
    OR n.last_heartbeat_at < UNIX_TIMESTAMP() - ?deadThreshold);  -- stale heartbeat

-- Lease mode: check lease_expiry directly (no join needed)
SELECT a.table_name, a.node_id
FROM _table_assignment a
JOIN _table t ON a.table_name = t.table_name
WHERE t.active = true
  AND a.node_id IS NOT NULL
  AND (a.lease_expiry < UNIX_TIMESTAMP()                 -- lease expired (TTL baked in)
    OR a.lease_expiry IS NULL);                           -- leftover from heartbeat→lease switch
```

**Step 2 — Claim orphans** (staggered, same approach as startup):

```
loop:
  compute fair_share = ceil(currently_assigned_tables / active_nodes)
  if my_load >= fair_share → done
  pick one orphan from step 1
  if none → done
  short random delay (0–5s) to reduce collisions
  attempt claim:
    -- Heartbeat mode: guard on dead owner's node_id
    UPDATE _table_assignment
    SET node_id = ?, assignment_updated_at = UNIX_TIMESTAMP()
    WHERE table_name = ? AND node_id = ?;

    -- Lease mode: guard on expired lease_expiry
    UPDATE _table_assignment
    SET node_id = ?, lease_expiry = UNIX_TIMESTAMP() + ?, assignment_updated_at = UNIX_TIMESTAMP()
    WHERE table_name = ? AND (lease_expiry < UNIX_TIMESTAMP() OR lease_expiry IS NULL);

  affected_rows = 1 → claimed, start per-table coordinator, repeat
  affected_rows = 0 → another node already claimed it, move on
```

Recomputing fair share between each claim prevents a single node from over-claiming. Residual imbalances from race conditions self-correct as subsequent orphans go to underloaded nodes.

**Recovery time**: dead threshold + up to one reconciliation interval (worst case). During rolling deployments, the replacement node is typically up before the liveness signal expires, so tables migrate directly old→replacement.

**Ownership verification**: The same reconciliation cycle also verifies existing assignments — a node checks its running per-table coordinators against DB assignments:

- **Assignment lost** — running a coordinator for a table no longer assigned → stop immediately. This handles split-brain: another node claimed it while this node was unreachable.
- **New assignment** — DB shows an assignment not yet running → start per-table coordinator. This handles graceful migration.

### Health Monitoring

The liveness goroutine and watchdog goroutine cover two non-overlapping failure scenarios:

| Failure | Detected by | Response |
|---------|-------------|----------|
| Node death | Remote peers (liveness goroutine stops → signal goes stale) | Claim table |
| Coordinator stall (node alive) | Local watchdog | Restart or release |

A stalled coordinator doesn't stop the liveness goroutine, so remote peers still see the node as alive. Without the watchdog, a stalled coordinator on a live node would never be detected — the liveness signal keeps renewing, no one claims the table, but no work gets done.

Stall detection is entirely local. Each coordinator goroutine updates an in-memory `lastIterationAt` timestamp at the end of each loop iteration. The watchdog checks these in-memory timestamps — it does not read from the database. If all goroutines are healthy, the watchdog writes `last_progress_at` to the database for operator visibility (so cluster health can be queried without SSH-ing into individual nodes). If any goroutine stalls, `last_progress_at` stops advancing.

```sql
UPDATE _table_assignment
SET last_progress_at = UNIX_TIMESTAMP()
WHERE table_name = ? AND node_id = ?;
-- AND node_id = ? prevents overwriting the new owner's progress after ownership loss
```

**Watchdog escalation** (runs at the watchdog interval):

| Step | Trigger | Action |
|------|---------|--------|
| 1 | Goroutine exceeds stall threshold | Log warning |
| 2 | Goroutine exceeds 2x stall threshold | Restart per-table coordinator |
| 3 | Restart fails or same coordinator stalls again shortly after | Release assignment (`node_id = NULL`) for another node |

The stall threshold (hardcoded at 50s) is intentionally generous to avoid false positives during transient slowdowns (temporary database latency). The restart threshold is 2x the stall threshold (100s), and the recurrence window is 5 minutes — if the same coordinator stalls again within 5 minutes of a restart, the assignment is released.

**Goroutine definitions** — a goroutine has made progress when it completes a full loop iteration:

| Goroutine | Iteration completes when... |
|-----------|---------------------------------|
| Kafka Consumer | `Poll()` returns and all records submitted to BatchingWriter |
| tableWriter | batch-UPSERT succeeds |
| Planner | file query, task creation, stale task reclaim finish |
| Storage Deletion | storage deletion finishes (or queue empty) |
| Retention Cleanup | expired row deletion completes |

An idle goroutine still completes iterations — `Poll()` returns 0 records and finishes immediately. A stuck goroutine cannot complete its iteration.

**Visibility queries:**

```sql
-- Stalled coordinators (no recent progress)
SELECT table_name, node_id, last_progress_at
FROM _table_assignment
WHERE node_id IS NOT NULL
  AND last_progress_at < UNIX_TIMESTAMP() - ?stallThreshold;
```

---

## Edge Cases

All edge cases are handled automatically — no manual intervention required.

### Split-Brain (Two Owners)

A network partition (temporary network failure) isolates a node from the database. Another node sees its liveness signal expire and claims its tables. When the partition heals, both nodes think they own the same table.

**Resolution**: The old owner's ownership verification detects that the database no longer shows it as owner. It stops its per-table coordinator immediately. No data corruption — all metadata operations are idempotent (safe to replay), so any overlap during the brief split-brain window causes no harm.

### Missing or Null Liveness Signal

In heartbeat mode, an assignment points to a node with no `_node_registry` entry (crashed before registering, or entry cleaned up). In lease mode, `lease_expiry` is NULL (never set, e.g., after switching from heartbeat mode).

**Resolution**: Heartbeat mode uses `LEFT JOIN`, so a missing registry entry returns NULL — treated as dead. Lease mode treats `lease_expiry IS NULL` (with non-NULL `node_id`) as expired. Either way, the table is claimed normally.

### Two Nodes Claim the Same Orphan

Two nodes detect the same orphan and race to claim it.

**Resolution**: In heartbeat mode, the claim UPDATE includes `WHERE node_id = <dead_owner>` — the first node's UPDATE changes `node_id` to itself, and the second sees zero rows. In lease mode, the claim includes `WHERE lease_expiry < NOW()` — the first node sets a future `lease_expiry`, and the second sees it's no longer expired. Either way, only one succeeds — the database guarantees this atomically.

### Database Goes Down

| Operation | Behavior |
|-----------|----------|
| Liveness write | Fails, logs error, retries next cycle |
| Orphan scan | Fails, skips cycle, retries next interval |
| Processing | Stalls (metadata writes fail), retries when DB recovers |

Everything resumes automatically when the database returns. MariaDB/MySQL is a single point of failure for both coordination and data — an accepted trade-off since coordinators already depend on it for metadata writes (see [Appendix A.2](#a2-mariadbmysql-as-single-dependency)).

### Multiple Nodes Die at Once

Fair-share claiming spreads orphans across survivors. At this scale, even a single surviving node can handle all tables. The query service continues serving existing data throughout.

### Per-Table Coordinator Fails to Start

A node claims a table but the per-table coordinator crashes on startup. The node reverts the claim (`node_id = NULL`), making the table unassigned. On the next reconciliation cycle, any node (including the same one) can claim it via fight-for-master.

> Why revert instead of leaving the assignment? Orphan detection only finds tables with a dead *owner*. If the node is still alive but the coordinator failed to start, the table would appear healthy (liveness signal still active) but no work would get done. Reverting to `node_id = NULL` makes it claimable immediately.

### Clock Skew

All timestamp writes and comparisons use `UNIX_TIMESTAMP()` (database server time). The node's local clock is never used for liveness decisions.

### Source Crashes During Migration

Migration transfers ownership (step 2) only after draining completes (step 1). A crash before the transfer means the assignment still points to the source — normal orphan recovery handles it. No intermediate state to clean up.

### Coordinator Stalls on a Live Node

The local watchdog detects the stall via per-goroutine `lastIterationAt` timestamps and restarts the per-table coordinator. If restart fails, the node releases the assignment for another node to claim. See [Health Monitoring](#health-monitoring).

---

## Data Model

### Overview

The table registry uses sub-tables with 1:1 relationships via `table_name`:

| Table | Purpose | Key question |
|-------|---------|-------------|
| `_table` | Identity | "What tables exist and are active?" |
| `_table_assignment` | Node assignment | "Who owns each table?" |
| `_node_registry` | Liveness (heartbeat mode) | "Which nodes are alive?" |

Everything is derived from timestamps — no status flags:
- **Node alive?** → heartbeat mode: `_node_registry.last_heartbeat_at` is recent; lease mode: `_table_assignment.lease_expiry` is in the future
- **Table owner?** → `_table_assignment.node_id`
- **Table orphaned?** → owner's liveness signal is stale or expired
- **Coordinator healthy?** → `_table_assignment.last_progress_at` is recent (operator visibility only; actual detection is local)

### _table

| Column | Type | Description |
|--------|------|-------------|
| table_id | CHAR(36) PK | Auto-generated UUID |
| table_name | VARCHAR(64) UNIQUE | Metadata table name (e.g., `"spark"`) |
| display_name | VARCHAR(128) | Human-readable name |
| active | BOOLEAN | Whether this table should be processing |

### _table_assignment

| Column | Type | Description |
|--------|------|-------------|
| table_name | VARCHAR(64) PK/FK | References `_table` |
| node_id | VARCHAR(64) NULL | Owner (NULL = unassigned) |
| node_assigned_at | INT UNSIGNED NULL | When first claimed |
| assignment_updated_at | INT UNSIGNED NULL | When last changed |
| last_progress_at | INT UNSIGNED NULL | When coordinator last completed a loop (written for operator visibility; stall detection uses in-memory timestamps) |
| lease_expiry | INT UNSIGNED NULL | Lease mode only; NULL in heartbeat mode |

### _node_registry (heartbeat mode only)

| Column | Type | Description |
|--------|------|-------------|
| node_id | VARCHAR(64) PK | Node identifier |
| last_heartbeat_at | INT UNSIGNED | Last liveness timestamp |

Stale rows from dead nodes accumulate harmlessly. Manual `DELETE` is safe at any time. In lease mode, this table is unused.

### Schema DDL

```sql
CREATE TABLE _table (
    table_id      CHAR(36) NOT NULL DEFAULT (UUID()) PRIMARY KEY,
    table_name    VARCHAR(64) NOT NULL UNIQUE,
    display_name  VARCHAR(128) NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT TRUE
) ENGINE=InnoDB;

CREATE TABLE _table_assignment (
    table_name            VARCHAR(64) NOT NULL PRIMARY KEY,
    node_id               VARCHAR(64) NULL,
    node_assigned_at      INT UNSIGNED NULL,
    assignment_updated_at INT UNSIGNED NULL,
    last_progress_at      INT UNSIGNED NULL,
    lease_expiry          INT UNSIGNED NULL,
    INDEX idx_node_id (node_id),
    FOREIGN KEY (table_name) REFERENCES _table(table_name)
) ENGINE=InnoDB;

CREATE TABLE _node_registry (
    node_id           VARCHAR(64) PRIMARY KEY,
    last_heartbeat_at INT UNSIGNED NOT NULL,
    INDEX idx_heartbeat (last_heartbeat_at)
) ENGINE=InnoDB;
```

See `schema.sql` for the full registry (`_table_kafka`, `_table_config`).

---

## Appendix A: Design Notes

### A.1 `node_id` Resolution

The `node_id` is resolved at startup:

1. Read the environment variable named by `node.nodeIdEnvVar` in `node.yaml` (default: `HOSTNAME`)
2. If empty, fall back to `os.Hostname()`
3. If both fail, startup aborts with a clear error

| Platform | Recommended `nodeIdEnvVar` | Why |
|----------|---------------------------|-----|
| Docker / Kubernetes | `HOSTNAME` (default) | Container ID or pod name |
| Custom platform | Platform-specific env var | Instance identifier |

**Stability**: If a node restarts with the same `node_id`, it picks up old assignments immediately (seconds). With a different `node_id` (typical in stateless platforms), recovery goes through the normal orphan flow (dead threshold + reconciliation interval).

### A.2 MariaDB/MySQL as Single Dependency

MariaDB/MySQL is a single point of failure for both coordination and data storage. This is an accepted trade-off — coordinators already depend on it for metadata writes. If the database is down, coordinators stall regardless of the HA layer.

### A.3 Resumption State

When a new owner takes over, it starts a Kafka consumer with the same consumer group ID as the previous owner (`clp-coordinator-{table_name}-{table_id}`). This ID is deterministic — derived from the table, not the node — so any node that claims the table gets the same ID.

Kafka brokers track how far each consumer group has read (the "committed offset"). Because the new owner reuses the same group ID, Kafka automatically delivers messages starting from where the old owner left off. No offset is stored in the database.

If the old owner crashed before committing its latest offset, some messages may be re-delivered. This is safe because all metadata operations are idempotent (UPSERTs with forward-only state transitions) — re-processing the same message produces the same result.

---

## Appendix B: Walkthrough Examples

Step-by-step walkthroughs showing database rows at each step, using default timings. These use heartbeat mode; lease mode follows the same flow with `lease_expiry` instead of `_node_registry` heartbeats.

**Starting state:**

**_table_assignment**:
| table_name | node_id | assignment_updated_at |
|------------|---------|----------------------|
| spark | node-001 | 10:00:00 |
| flink | node-001 | 10:00:00 |
| ray | node-002 | 10:00:00 |

**_node_registry**:
| node_id | last_heartbeat_at |
|---------|------------------|
| node-001 | 10:05:00 |
| node-002 | 10:05:00 |

---

### B.1 Node Dies, Surviving Node Takes Over

**10:05:30** — node-001 crashes. Stops heartbeating.

**10:08:00** — node-001's heartbeat is 3 min stale (dead). node-002's is current (alive).

**10:08:30** — node-002's orphan scanner finds:
```
spark  — owner node-001 — dead
flink  — owner node-001 — dead
ray    — owner node-002 — alive (skip)
```

node-002 claims `spark`:
```sql
UPDATE _table_assignment SET node_id = 'node-002', assignment_updated_at = UNIX_TIMESTAMP()
WHERE table_name = 'spark' AND node_id = 'node-001';
-- affected_rows = 1 → claimed
```

Then claims `flink` (after random delay). node-002 now handles all 3 tables, each resuming from where the old coordinator left off (see [Appendix A.3](#a3-resumption-state)).

### B.2 Race Condition

Same setup plus **node-003** (0 tables). node-001 dies. Both survivors detect `spark` and `flink` as orphans.

1. node-002 claims `spark` first — `WHERE node_id = 'node-001'` matches, `affected_rows = 1`
2. node-003 tries `spark` — `WHERE node_id = 'node-001'` matches 0 rows (already changed), moves on
3. node-003 claims `flink` instead — succeeds

Result: node-002 has 2 tables (spark + ray), node-003 has 1 (flink). Fair share balanced.

### B.3 Graceful Migration

Admin triggers migration of `spark` to node-002. Source stops ingestion, flushes pending writes, then either reassigns directly (`UPDATE node_id = 'node-002'`) or releases (`UPDATE node_id = NULL`) and lets reconciliation assign it. Target detects the new assignment on its next reconciliation cycle and starts a per-table coordinator.

### B.4 Rolling Deployment

Platform replaces nodes one at a time. Each old node's heartbeat expires after the dead threshold. The replacement is already up with 0 tables, so it claims the orphan. Tables migrate directly old→replacement without bouncing through survivors.

---

## Appendix C: Design Alternatives Analysis

Three designs were evaluated:

- **Design A (DB Heartbeat):** Nodes write heartbeat to `_node_registry`. Peers detect stale heartbeats. Precedent: Kubernetes node heartbeats, Apache Airflow.
- **Design B (DB Lease):** Assignments carry `lease_expiry`. Owner renews periodically; expired leases are claimable. Precedent: DynamoDB leader election.
- **Design C (External Store):** ZooKeeper/etcd ephemeral nodes. Precedent: Kafka (pre-3.3), HBase.

### Design C: Ruled Out

An external consensus cluster adds a dependency to a service that must function during outages. If ZooKeeper is down when failover is needed, the mechanism fails precisely when it matters most. Kafka spent years migrating away from ZooKeeper (KRaft) for similar reasons.

### Where A and B Are Equivalent

Both use MariaDB/MySQL only, require no new infrastructure, and degrade gracefully during a database outage — existing owners keep processing because coordination is based on periodic writes, not active sessions (unlike ZooKeeper/etcd where a session loss triggers immediate ownership revocation). Recovery time is a function of a tunable threshold/TTL parameter.

### Where They Differ

A node can fail two ways: **node death** (the process crashes) or **coordinator stall** (a goroutine deadlocks or hangs — the node is alive, but the coordinator is stuck).

Design A uses two mechanisms with non-overlapping responsibilities: heartbeat detects node death (remote peers claim the table), watchdog detects stalls (local restart or release).

Design B appears simpler (one mechanism — the lease), but a watchdog is still needed. The dilemma: if the lease is renewed independently of processing, stalls are invisible to remote peers (the lease keeps renewing while no work gets done). If the lease is tied to processing progress, idle coordinators (legitimately waiting for data) falsely appear expired. Either way, a separate watchdog is required, and the lease reduces to a per-table heartbeat with additional complexity but no additional capability.

### Recommendation: Design A

Design B's simplicity advantage (one fewer table, one fewer goroutine) is real but bounded. Design A's advantage is that each component has a single responsibility. The system supports both modes — see [Deploy HA](../guides/deploy-ha.md).

### References

- [Apache ZooKeeper — Sessions](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkSessions)
- [etcd — Leases](https://etcd.io/docs/v3.5/learning/api/#lease-api)
- Burrows, M. (2006). [The Chubby lock service](https://research.google/pubs/pub27897/). OSDI '06.
- [Kubernetes — Leases](https://kubernetes.io/docs/concepts/architecture/leases/)
- [AWS Builders' Library — Leader Election](https://aws.amazon.com/builders-library/leader-election-in-distributed-systems/)

---

## See Also

- [Deploy HA](../guides/deploy-ha.md) — Operational HA configuration, graceful migration, shutdown
- [Architecture Overview](../concepts/overview.md) — System overview and deployment options
- [Ingestion Paths](../concepts/ingestion.md) — gRPC and Kafka protocols, BatchingWriter
- [Configuration](../reference/configuration.md) — HA strategy and heartbeat/lease settings
- [Task Queue](../concepts/task-queue.md) — Task recovery on failover
- [Metadata Schema: Partitioning](../concepts/metadata-schema.md#partitioning) — Daily partitions, advisory lock coordination
