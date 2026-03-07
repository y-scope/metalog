package kafka

import (
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/coordinator/ingestion"
)

// PollerFactory creates Kafka pollers from table configurations.
type PollerFactory struct {
	service *ingestion.Service
	log     *zap.Logger
}

// NewPollerFactory creates a PollerFactory.
func NewPollerFactory(service *ingestion.Service, log *zap.Logger) *PollerFactory {
	return &PollerFactory{service: service, log: log}
}

// Create builds a Poller for the given table config.
// tableID is the UUID from the _table registry, used to derive a unique Kafka consumer
// group ID across environments sharing the same Kafka cluster.
func (f *PollerFactory) Create(tableConfig config.TableConfig, tableID string) *Poller {
	groupID := "clp-coordinator-" + tableConfig.Name + "-" + tableID

	transformer := NewTransformer(tableConfig.Kafka.RecordTransformer)

	return NewPoller(
		tableConfig.Kafka.BootstrapServers,
		groupID,
		tableConfig.Kafka.Topic,
		tableConfig.Name,
		transformer,
		f.service,
		f.log,
	)
}
