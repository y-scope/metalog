package node

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/metastore"
)

// KafkaIngestionUnit manages a single Kafka adapter for a source.
// It is decoupled from the CoordinatorUnit — only handles Kafka consumption
// and ingestion into the BatchingWriter. No planner, retention, or schema
// management responsibilities.
type KafkaIngestionUnit struct {
	tableName string
	sourceName  string
	adapter   kafka.Adapter
	log       *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewKafkaIngestionUnit creates a KafkaIngestionUnit for a Kafka source.
// The adapter is created via the provided factory. Returns nil if the source
// has no topic or bootstrap servers configured.
func NewKafkaIngestionUnit(
	ctx context.Context,
	tableName, tableID string,
	src *metastore.KafkaSource,
	factory kafka.AdapterFactory,
	ingestSvc *ingestion.Service,
	log *zap.Logger,
) (*KafkaIngestionUnit, error) {
	adapter, err := factory(tableName, tableID, src, ingestSvc, log)
	if err != nil {
		return nil, err
	}

	childCtx, cancel := context.WithCancel(ctx)
	return &KafkaIngestionUnit{
		tableName: tableName,
		sourceName:  src.SourceName,
		adapter:   adapter,
		log:       log.With(zap.String("unit", "kafka"), zap.String("table", tableName), zap.String("source", src.SourceName)),
		ctx:       childCtx,
		cancel:    cancel,
	}, nil
}

// Start begins the Kafka adapter goroutine.
func (u *KafkaIngestionUnit) Start() {
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.log.Info("kafka ingestion unit started")
		u.adapter.Start(u.ctx)
		if u.ctx.Err() == nil {
			u.log.Error("kafka adapter exited unexpectedly")
		}
	}()
}

// Stop signals the adapter to stop and waits for the goroutine to exit.
func (u *KafkaIngestionUnit) Stop() {
	u.log.Info("stopping kafka ingestion unit")
	u.adapter.Stop()
	u.cancel()
	u.wg.Wait()
	u.log.Info("kafka ingestion unit stopped")
}
