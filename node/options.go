package node

import (
	"database/sql"

	"github.com/y-scope/metalog/telemetry"
)

// NodeOption configures optional Node behavior.
type NodeOption func(*Node)

// WithTelemetryProvider sets a pre-created telemetry provider. When set,
// NewNode uses this provider instead of creating its own from config.
// Passing nil explicitly suppresses config-based provider creation.
func WithTelemetryProvider(p *telemetry.Provider) NodeOption {
	return func(n *Node) {
		n.externalTelemetry = p
		n.hasTelemetryProvider = true
	}
}

// WithDB injects an externally-managed primary (RW) database pool.
// When set, NewNode skips creating its own pool from config.Database.Primary.
// The caller retains ownership — the pool will NOT be closed when Node stops.
func WithDB(pool *sql.DB) NodeOption {
	return func(n *Node) {
		n.externalDB = pool
		n.hasExternalDB = true
	}
}

// WithReadDB injects an externally-managed read-replica (RO) database pool.
// When set, NewNode skips creating its own pool from config.Database.Replica.
// The caller retains ownership — the pool will NOT be closed when Node stops.
func WithReadDB(pool *sql.DB) NodeOption {
	return func(n *Node) {
		n.externalReadDB = pool
		n.hasExternalReadDB = true
	}
}
