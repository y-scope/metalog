package confluent

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node"
)

// NewDefaultAdapterFactory returns a KafkaAdapterFactory that creates
// confluent-kafka consumers (the default upstream transport).
// If meter is non-nil, consumer metrics are registered.
func NewDefaultAdapterFactory(meter metric.Meter) node.KafkaAdapterFactory {
	return func(
		tableName, tableID string,
		tableCfg metastore.TableConfig,
		ingestSvc *ingestion.Service,
		log *zap.Logger,
	) (node.KafkaAdapter, error) {
		if !tableCfg.Kafka.Enabled || tableCfg.Kafka.Topic == "" || tableCfg.Kafka.BootstrapServers == "" {
			return nil, node.ErrKafkaNotConfigured
		}
		transformer, err := kafka.NewTransformer(tableCfg.Kafka.RecordTransformer)
		if err != nil {
			return nil, err
		}
		groupID := kafka.KafkaGroupPrefix + tableName + "-" + tableID
		consumer := NewConsumer(
			tableCfg.Kafka.BootstrapServers, groupID, tableCfg.Kafka.Topic, tableName,
			transformer,
			ingestSvc,
			log,
		)
		if meter != nil {
			consumer.SetMeter(meter)
		}
		return &adapter{consumer: consumer}, nil
	}
}

// adapter wraps a Consumer as a node.KafkaAdapter.
type adapter struct {
	consumer kafka.MessageSource
}

func (a *adapter) Start(ctx context.Context) { a.consumer.Run(ctx) }
func (a *adapter) Stop()                     {}
