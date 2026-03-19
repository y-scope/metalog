// Package node provides the top-level orchestrator for metalog processes.
//
// A [Node] manages the full lifecycle of coordinator and worker units:
// provisioning tables, claiming ownership via the [CoordinatorRegistry],
// starting per-table [CoordinatorUnit] instances and a shared [WorkerUnit]
// pool, and running background goroutines for liveness (heartbeat or lease
// renewal) and reconciliation (orphan reclaim, stall detection).
//
// [SharedResources] holds database pools, storage backends, and column
// registries shared across all units within a node.
package node
