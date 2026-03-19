# High Availability & Node Lifecycle

[← Back to docs](../README.md)

How nodes coordinate, detect failures, and recover automatically — without ZooKeeper, etcd, or any external consensus system. MariaDB/MySQL is the single coordination plane.

**Related:** [Coordinator HA Design](../design/coordinator-ha.md) · [Deploy HA](../guides/deploy-ha.md) · [Architecture Overview](overview.md)

---

## Design Principles

| Principle | Description |
|-----------|-------------|
| **DB-centric coordination** | All HA state lives in MariaDB/MySQL — the same database that stores metadata. No additional infrastructure. |
| **No leader election** | Fair-share claiming with atomic compare-and-swap (`UPDATE ... WHERE node_id IS NULL`). Any node can adopt orphaned tables. |
| **Forward-only recovery** | Idempotent UPSERTs and monotonic state transitions mean any component can restart at any time without data loss. |
| **Graceful degradation** | Failed operations log but don't block subsequent reconciliation steps. The system continues making progress under partial failure. |

---

## Coordination Model

Each metadata table is owned by exactly one node at a time. The owner runs per-table lifecycle goroutines (retention strategy, partition maintenance, alias refresh, and optionally Kafka consumer and planner). Ownership is tracked in `_table_assignment` — a database table where `node_id` indicates the current owner.

### Two HA Strategies

Both strategies detect dead nodes and enable automatic failover. The choice affects where liveness state is stored:

| | Heartbeat (default) | Lease |
|---|---|---|
| **Liveness signal** | Per-node: UPSERT to `_node_registry` | Per-table: set `lease_expiry` on `_table_assignment` |
| **Dead detection** | Peers JOIN registry for stale heartbeats | Peers check `WHERE lease_expiry < NOW()` |
| **Write cost** | O(1) per node | O(tables owned) per node |

Heartbeat mode is the default — cleaner separation between node liveness and table ownership, one write per node regardless of table count. See [Deploy HA](../guides/deploy-ha.md) for configuration.

### Table Claiming

On startup and during reconciliation, nodes claim tables via staggered fight-for-master:

1. Compute fair share: `ceil(assigned_tables / active_nodes)`
2. If already at fair share, stop
3. Attempt atomic claim on one unassigned table
4. Short random delay, recompute fair share, repeat

Recomputing between each claim gives concurrent nodes time to become visible, preventing a single fast-starting node from claiming all tables. See [Coordinator HA Design](../design/coordinator-ha.md) for SQL and edge cases.

---

## Multi-Layer Watchdog

Three layers of failure detection, each covering a non-overlapping failure scenario:

| Layer | What It Detects | Detected By | Response |
|-------|----------------|-------------|----------|
| **Node** | Dead node (process crash) | Remote peers (stale heartbeat or expired lease) | Surviving nodes claim orphaned tables |
| **Coordinator** | Stalled goroutine (node alive, goroutine hung) | Local watchdog (in-memory `lastIterationAt` timestamps) | Restart coordinator; release assignment if restart fails |
| **Task** | Abandoned worker task (worker died mid-processing) | Planner (stale `processing` tasks in `_task_queue`) | Reclaim task → new pending row (or dead-letter if max retries exceeded) |

Without the watchdog, a stalled coordinator on a live node would never be detected — the liveness signal keeps renewing, no peer claims the table, but no work gets done.

**Watchdog escalation:**

| Step | Trigger | Action |
|------|---------|--------|
| 1 | Goroutine exceeds stall threshold (50s) | Log warning |
| 2 | Goroutine exceeds 2× stall threshold (100s) | Restart coordinator |
| 3 | Same coordinator stalls again within 5 min | Release assignment for another node |

See [Coordinator HA Design: Health Monitoring](../design/coordinator-ha.md#health-monitoring) for details.

---

## Reconciliation Loop

Every node runs a reconciliation goroutine at a configurable interval (default: 60s). Four independent steps — each tolerates failure without blocking the others:

| Step | Action | Purpose |
|------|--------|---------|
| 1 | **Claim orphans** | Adopt tables from dead nodes |
| 2 | **Claim unassigned** | Pick up newly registered tables |
| 3 | **Watchdog check** | Restart stalled coordinators |
| 4 | **Ownership verification** | Stop coordinators for lost assignments; start newly assigned ones |

Step 4 handles split-brain: if a network partition caused another node to claim a table, the original owner detects the loss and stops its coordinator immediately. No data corruption — all metadata operations are idempotent.

---

## Node Lifecycle

### Startup Sequence

**Node-level (once):**

1. Load configuration (`node.yaml`)
2. Create shared resources (database pool, StorageRegistry)
3. Initialize coordination schema, claim tables
4. Create BatchingWriter
5. Start all coordinator units (each runs per-coordinator startup below)
6. Create `IngestionService`
7. Start gRPC server (if enabled)
8. Start node-level goroutines: Heartbeat/Lease Renewal, Reconciliation, Partition Maintenance, Watchdog
9. Start Health Check Server (if configured)

**Per-coordinator (each claimed table):**

1. Initialize schema and components
2. **[BLOCKING]** Ensure lookahead partitions exist (one-time check)
3. Recover from restart (Kafka consumer group resumes from last committed offset)
4. Start goroutines: Partition Maintenance, Alias Refresh, Column Recycler, and conditionally Kafka Consumer, Planner, Retention Strategy

### Shutdown Sequence

When a node receives SIGTERM:

1. Mark health check as NOT_READY (stop receiving new requests)
2. Cancel node-level context (stops Liveness, Reconciliation goroutines)
3. Stop gRPC server
4. Stop all coordinator units:
   - Cancel coordinator context → stop all per-coordinator goroutines
5. Signal BatchingWriter to stop, wait for per-table goroutines to drain
6. Stop worker units (two-phase: stop Prefetcher → drain workers with 30s timeout → force-cancel)
7. Close shared resources (database pool, StorageRegistry)

On graceful shutdown, the node releases all table assignments (`node_id = NULL` in `_table_assignment`) and deregisters from `_node_registry`. This allows other nodes to claim the tables immediately via the next reconciliation cycle, without waiting for the dead threshold to expire.

### Two-Phase Worker Shutdown

Workers use a dedicated shutdown sequence to minimize task loss:

1. **Phase 1**: Cancel Prefetcher context → close task channel (no new claims)
2. **Phase 2**: Worker goroutines drain remaining tasks from the channel. After 30s timeout, force-cancel any still running.

Incomplete tasks remain in `processing` state and are reclaimed by the Planner as stale after the task timeout (5 min).

### Health Probes

HTTP endpoints for load balancer integration:

| Endpoint | Response | When |
|----------|----------|------|
| `GET /health` or `/health/live` | 200 OK | Always (process is running) |
| `GET /ready` or `/health/ready` | 200 READY / 503 NOT READY | After startup / during shutdown |

The ready flag is atomic — set to true after startup completes, false at the start of shutdown.

---

## Key Timings

| Setting | Default | Description |
|---------|---------|-------------|
| Liveness interval | 30s | How often the node refreshes its heartbeat or lease |
| Dead threshold / Lease TTL | 180s | Time before a silent node is considered dead |
| Reconciliation interval | 60s | How often nodes scan for orphans |
| Watchdog interval | 60s | How often the watchdog checks goroutine progress |
| Stall threshold | 50s | Time before a stalled goroutine triggers a warning |
| Worker drain timeout | 30s | How long to wait for workers to finish during shutdown |
| Task stale timeout | 5 min | How long before abandoned tasks are reclaimed |

**Worst-case failover time**: dead threshold + reconciliation interval = ~4 minutes with defaults.

---

## See Also

- [Coordinator HA Design](../design/coordinator-ha.md) — SQL, edge cases, walkthroughs, data model, design alternatives
- [Deploy HA](../guides/deploy-ha.md) — Configuration, graceful migration, mode switching, monitoring queries
- [Architecture Overview](overview.md) — Goroutine model, data flow diagrams
- [Configuration Reference](../reference/configuration.md) — HA strategy and timing settings
- [Deletion & Lifecycle](deletion-lifecycle.md) — File lifecycle states, retention, storage cleanup
