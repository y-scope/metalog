package franzgo

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/metastore"
)

func init() {
	kafka.RegisterDriver("franzgo", NewDefaultAdapterFactory(nil))
}

// NewDefaultAdapterFactory returns an AdapterFactory that creates
// franz-go consumers (pure Go, no CGO).
// If meter is non-nil, consumer metrics are registered.
func NewDefaultAdapterFactory(meter metric.Meter) kafka.AdapterFactory {
	return func(
		tableName, tableID string,
		src *metastore.KafkaSource,
		ingestSvc *ingestion.Service,
		log *zap.Logger,
	) (kafka.Adapter, error) {
		if src.Topic == "" || src.BootstrapServers == "" {
			return nil, kafka.ErrNotConfigured
		}
		if src.ConsumerGroupID == "" {
			return nil, fmt.Errorf("kafka source %s/%s: consumer_group_id is required", tableName, src.SourceName)
		}
		transformer, err := kafka.NewTransformer(src.RecordTransformer)
		if err != nil {
			return nil, err
		}
		consumer := NewConsumer(
			src.BootstrapServers, src.ConsumerGroupID, src.Topic, tableName,
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

// adapter wraps a Consumer as a kafka.Adapter.
type adapter struct {
	consumer kafka.MessageSource
}

func (a *adapter) Start(ctx context.Context) { a.consumer.Run(ctx) }
func (a *adapter) Stop()                     {}
