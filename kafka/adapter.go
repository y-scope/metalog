package kafka

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node"
)

// kafkaGroupPrefix is the prefix for Kafka consumer group IDs.
// Matches the Java implementation (clp-coordinator-{table_name}-{table_id}).
const kafkaGroupPrefix = "clp-coordinator-"

// NewDefaultAdapterFactory returns a KafkaAdapterFactory that creates
// confluent-kafka consumers (the default upstream transport).
// If meter is non-nil, consumer metrics are registered.
func NewDefaultAdapterFactory(meter ...metric.Meter) node.KafkaAdapterFactory {
	var m metric.Meter
	if len(meter) > 0 {
		m = meter[0]
	}
	return func(
		tableName, tableID string,
		tableCfg metastore.TableConfig,
		ingestSvc *ingestion.Service,
		log *zap.Logger,
	) (node.KafkaAdapter, error) {
		if !tableCfg.Kafka.Enabled || tableCfg.Kafka.Topic == "" || tableCfg.Kafka.BootstrapServers == "" {
			return nil, node.ErrKafkaNotConfigured
		}
		transformer, err := NewTransformer(tableCfg.Kafka.RecordTransformer)
		if err != nil {
			return nil, err
		}
		groupID := kafkaGroupPrefix + tableName + "-" + tableID
		consumer := NewConsumer(
			tableCfg.Kafka.BootstrapServers, groupID, tableCfg.Kafka.Topic, tableName,
			transformer,
			ingestSvc,
			log,
		)
		if m != nil {
			consumer.SetMeter(m)
		}
		return &confluentAdapter{consumer: consumer}, nil
	}
}

// confluentAdapter wraps kafka.Consumer as a node.KafkaAdapter.
type confluentAdapter struct {
	consumer MessageSource
}

func (a *confluentAdapter) Start(ctx context.Context) { a.consumer.Run(ctx) }
func (a *confluentAdapter) Stop()                     {}
