# Deploy HA

[← Back to docs](../README.md)

How to configure, operate, and troubleshoot coordinator high availability. For design details, edge cases, and walkthroughs, see [Coordinator HA Design](../design/coordinator-ha.md).

## Summary

The CLP Metastore Service can be deployed on stateless platforms where nodes disappear at any time and may not be replaced for hours or days. Nodes claim tables on startup, report health periodically, detect dead peers, and automatically take over from failed nodes — no human intervention required.

**What HA coordinates:** Each table has a single owner node that runs its lifecycle threads (Kafka poller, planner, retention, storage deletion) — exactly one instance per table, with automatic recovery during outages or planned rollouts.

**What's independent of HA:** gRPC ingestion runs on every node and requires no single-owner coordination (see [Ingestion Paths](../concepts/ingestion.md)). Workers and the query service are also independent.

**Scale:** A handful of nodes managing a dozen or so tables.

**Key decisions:**
- Use the existing MariaDB/MySQL database for coordination — no ZooKeeper, etcd, or additional infrastructure
- Each table is processed by exactly one node at a time (single-owner)
- Two liveness detection modes — heartbeat (default) or lease — toggled by a single config key
- Every node participates in failure detection and recovery (no single manager)

---

## Heartbeat vs Lease

For most deployments, the default **heartbeat mode** is the right choice. Both modes have the same recovery time, the same configuration pattern, and the same edge case handling.

**Why heartbeat is the default:**
- Cleaner conceptual separation — node liveness is tracked independently from table ownership
- One DB write per node regardless of how many tables it owns
- Industry precedent (Kubernetes node heartbeats, Apache Airflow)

**The main reason to pick lease:** slightly simpler schema — no `_node_registry` table needed.

| | Heartbeat (default) | Lease |
|---|---|---|
| **Liveness signal** | Per-node: writes `_node_registry` | Per-table: sets `lease_expiry` on `_table_assignment` |
| **Detection** | Peers JOIN `_node_registry` for stale heartbeats | Peers check `WHERE lease_expiry < NOW()` |
| **Extra table** | `_node_registry` required | Not needed |
| **Write cost** | O(1) per node | O(tables owned) per node |

---

## Configuration

```yaml
node:
  coordinatorHaStrategy: heartbeat  # or "lease"
  reconciliationIntervalSeconds: 60 # both modes: orphan scan + claiming

  # Heartbeat mode
  heartbeatIntervalSeconds: 30
  deadNodeThresholdSeconds: 180

  # Lease mode
  leaseTtlSeconds: 180
  leaseRenewalIntervalSeconds: 30
```

### Default Timings

| Setting | Default | Description |
|---------|---------|-------------|
| Liveness interval | 30s | How often the node refreshes its heartbeat or lease |
| Dead threshold / Lease TTL | 180s (3 min) | Time before a silent node is considered dead |
| Reconciliation interval | 60s | How often nodes scan for orphans and reconcile assignments |
| Partition maintenance interval | 1h | How often nodes run partition housekeeping |
| Watchdog interval | 60s | How often the watchdog checks thread progress |
| Stall threshold | 50s | Time before a stalled thread triggers a warning |
| Progress write interval | 60s | How often `last_progress_at` is updated in the database |

**Threshold/interval ratio** defaults to 6:1 — the node must miss 6 consecutive writes before peers declare it dead. Below 3:1 risks spurious failover from scheduling jitter.

**Worst-case failover time**: dead threshold + reconciliation interval. With defaults: ~4 minutes (3 min detection + up to 1 min for next reconciliation scan).

**Startup validation**: In lease mode, `leaseRenewalIntervalSeconds` must be less than `leaseTtlSeconds`.

---

## How Recovery Works

1. **Node goes down** — in heartbeat mode, it stops saying "I'm alive" (node-level); in lease mode, it stops renewing its per-table claims
2. **Dead threshold passes** — heartbeat becomes stale (all tables orphaned at once) or leases expire (each table becomes individually claimable)
3. **Surviving nodes detect orphans** — during their next reconciliation scan
4. **Surviving nodes claim orphans** — each calculates a fair share and attempts to claim accordingly; claiming may take multiple iterations
5. **Processing resumes** — the new owner starts a per-table coordinator. For Kafka ingestion, the new owner resumes from the last committed offset. For gRPC ingestion, clients reconnect and resume sending.

During the recovery window, surviving nodes continue ingesting for their own tables and the query service continues serving previously registered data. No data is lost — gRPC clients receive errors and can retry, and Kafka events remain in the topic.

---

## Operations

### Graceful Migration

A coordinator can be moved between nodes without data loss. If the target node is omitted, the system picks the active node with the fewest assignments.

```
Source Node                          Database                         New Owner
     │                                   │                                  │
     │  1. Stop ingestion, drain work   │                                  │
     │     Flush pending writes          │                                  │
     │     Stop coordinator              │                                  │
     │                                   │                                  │
     │  2. Transfer ownership ─────────►│                                  │
     │                                   │                                  │
     │  ── Option A: Reassign to specific target ──                        │
     │  (UPDATE node_id = 'target'      │                                  │
     │   WHERE node_id = 'source')      │                                  │
     │                                   │◄─ 3a. Target detects & starts ─│
     │                                   │                                  │
     │  ── Option B: Release; any node claims via reconciliation ──        │
     │  (UPDATE node_id = NULL           │                                  │
     │   WHERE node_id = 'source')      │                                  │
     │                                   │◄─ 3b. Any node's next recon. ──│
     │                                   │        cycle claims the table   │
```

The source fully finishes before transferring ownership. Both options can migrate multiple tables in a single database transaction, ensuring all reassignments are atomic.

### Graceful Shutdown

When a node receives SIGTERM (e.g., rolling deployment):

1. For each assigned table: stop ingestion, finish in-progress work, flush pending writes
2. Exit

The node does **not** release its assignments or clean up its liveness signal. In heartbeat mode, the heartbeat goes stale; in lease mode, the lease expires. Either way, the table becomes claimable after the dead threshold passes. During rolling deployments, the replacement node typically starts before the threshold expires and claims the orphans.

### Mode Switching

All nodes must use the same mode. Stop all nodes before switching.

**Heartbeat → Lease**: After the switch, all existing assignments have `lease_expiry = NULL` (never set in heartbeat mode). On startup, each node first renews leases for tables it already owns, then claims tables with `lease_expiry IS NULL` (treated as expired). The `_node_registry` table remains but is no longer read.

**Lease → Heartbeat**: After the switch, no nodes have `_node_registry` entries. On startup, each node registers its heartbeat, then claims tables whose owners have no heartbeat (treated as dead via `LEFT JOIN`). Old `lease_expiry` values remain but are no longer read.

| Risk | Mitigation |
|------|------------|
| Mixed-mode cluster (not all nodes stopped) | Tables may be double-claimed — verify all nodes are down first |
| Node restarts with old config | Use a shared config source (e.g., ConfigMap) |

---

## Monitoring

### Why `lease_expiry` and `last_progress_at` Are Separate

Both involve timestamps, but they detect different failures:

| Field | Detects | Actor | Response |
|-------|---------|-------|----------|
| `last_heartbeat_at` / `lease_expiry` | Dead node | Remote peers | Claim table |
| `last_progress_at` | Stalled coordinator (node alive) | Local watchdog (in-memory) | Restart or release |

### Visibility Queries

```sql
-- All table assignments and health
SELECT a.table_name, a.node_id, a.last_progress_at,
  UNIX_TIMESTAMP() - a.last_progress_at AS seconds_since_progress
FROM _table_assignment a
WHERE a.node_id IS NOT NULL;

-- Stalled coordinators (no recent progress)
SELECT table_name, node_id, last_progress_at
FROM _table_assignment
WHERE node_id IS NOT NULL
  AND last_progress_at < UNIX_TIMESTAMP() - 100;  -- 2x stall threshold

-- Node liveness (heartbeat mode)
SELECT node_id, last_heartbeat_at,
  UNIX_TIMESTAMP() - last_heartbeat_at AS seconds_stale
FROM _node_registry;
```

---

## See Also

- [Coordinator HA Design](../design/coordinator-ha.md) — Edge cases, walkthroughs, data model, design alternatives
- [Architecture Overview](../concepts/overview.md) — System overview and deployment options
- [Configuration](../reference/configuration.md) — Full configuration reference
- [Ingestion Paths](../concepts/ingestion.md) — gRPC and Kafka protocols
