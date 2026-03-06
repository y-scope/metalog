package kafka

import (
	"context"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/coordinator/ingestion"
)

// Poller wraps a Consumer with a polling goroutine for a single table.
type Poller struct {
	consumer  *Consumer
	tableName string
	log       *zap.Logger
}

// NewPoller creates a Kafka poller for a table.
func NewPoller(
	bootstrapServers, groupID, topic, tableName string,
	transformer MessageTransformer,
	service *ingestion.Service,
	log *zap.Logger,
) *Poller {
	consumer := NewConsumer(bootstrapServers, groupID, topic, tableName, transformer, service, log)
	return &Poller{
		consumer:  consumer,
		tableName: tableName,
		log:       log.With(zap.String("poller", tableName)),
	}
}

// Run starts the polling loop until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	p.log.Info("kafka poller starting")
	p.consumer.Run(ctx)
}

// Consumer returns the underlying Consumer for offset inspection.
func (p *Poller) Consumer() *Consumer {
	return p.consumer
}
