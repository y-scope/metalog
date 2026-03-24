package node

import (
	"context"
	"database/sql"
	"errors"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/telemetry"
)

// ErrKafkaNotConfigured is returned by a KafkaAdapterFactory when the table
// does not have Kafka enabled or configured. The caller skips Kafka setup
// for this table rather than treating it as a failure.
var ErrKafkaNotConfigured = errors.New("kafka not configured for table")

// KafkaAdapter handles message consumption for a single table.
type KafkaAdapter interface {
	// Start begins consuming messages for the table and blocks until ctx is done.
	Start(ctx context.Context)
	// Stop performs any cleanup before the context is cancelled (e.g. deregistering
	// a push-based handler). For pull-based consumers this is a no-op.
	Stop()
}

// KafkaAdapterFactory creates a KafkaAdapter for a table.
// Return [ErrKafkaNotConfigured] if Kafka is not configured for this table —
// the caller will skip Kafka setup rather than treating it as a failure.
type KafkaAdapterFactory func(
	tableName, tableID string,
	tableCfg metastore.TableConfig,
	ingestSvc *ingestion.Service,
	log *zap.Logger,
) (KafkaAdapter, error)

// NodeOption configures optional Node behavior.
type NodeOption func(*Node)

// WithKafkaAdapterFactory sets a custom Kafka adapter factory, replacing the
// default confluent-kafka consumer. Use this to plug in alternative Kafka
// transports (e.g. KCP push delivery).
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
