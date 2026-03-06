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
func (f *PollerFactory) Create(tableConfig config.TableConfig) *Poller {
	groupID := "metalog-" + tableConfig.Name

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
