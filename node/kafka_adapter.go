package node

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
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
// Returns (nil, nil) if Kafka is not configured for this table.
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
