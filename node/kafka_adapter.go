package node

import (
	"context"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
)

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
