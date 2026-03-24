package node

import (
	"database/sql"

	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/telemetry"
)

// KafkaAdapter is an alias for kafka.Adapter, kept for backward compatibility
// with code that references node.KafkaAdapter.
type KafkaAdapter = kafka.Adapter

// KafkaAdapterFactory is an alias for kafka.AdapterFactory, kept for backward
// compatibility with code that references node.KafkaAdapterFactory.
type KafkaAdapterFactory = kafka.AdapterFactory

// ErrKafkaNotConfigured is an alias for kafka.ErrNotConfigured.
var ErrKafkaNotConfigured = kafka.ErrNotConfigured

// NodeOption configures optional Node behavior.
type NodeOption func(*Node)

// WithKafkaAdapterFactory sets a custom Kafka adapter factory, overriding
// the config-driven driver selection. Use this when you need to pass
// runtime dependencies (e.g. a shared handler registry) to the factory.
func WithKafkaAdapterFactory(f KafkaAdapterFactory) NodeOption {
	return func(n *Node) {
		n.kafkaFactory = f
	}
}

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
