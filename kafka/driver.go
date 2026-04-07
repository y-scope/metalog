// Package kafka provides shared interfaces and utilities for Kafka-based
// metadata ingestion.
//
// The concrete consumer implementation lives in the kafka/franzgo sub-package,
// which wraps franz-go (pure Go, no CGO).
//
// To add a different Kafka client library (e.g., Sarama, a push-based proxy),
// create a new sub-package under kafka/ that implements [MessageSource] and
// registers itself via [RegisterDriver] in an init() function. Metalog
// selects the driver from config.KafkaDriver (default: "franzgo").
// See kafka/franzgo/ for the reference implementation.
//
// Both implementations use the [MessageTransformer] interface to convert raw
// Kafka message payloads into protobuf MetadataRecord values, and integrate
// with the ingestion pipeline's back-pressure mechanism by waiting for flush
// confirmations before committing offsets.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/coordinator/ingestion"
	"github.com/y-scope/metalog/metastore"
)

// ErrNotConfigured is returned by an AdapterFactory when the table
// does not have Kafka enabled or configured. The caller skips Kafka setup
// for this table rather than treating it as a failure.
var ErrNotConfigured = errors.New("kafka not configured for table")

// Adapter handles message consumption for a single table.
type Adapter interface {
	// Start begins consuming messages for the table and blocks until ctx is done.
	Start(ctx context.Context)
	// Stop performs any cleanup before the context is cancelled (e.g. deregistering
	// a push-based handler). For pull-based consumers this is a no-op.
	Stop()
}

// AdapterFactory creates an Adapter for a Kafka source.
type AdapterFactory func(
	tableName, tableID string,
	src *metastore.KafkaSource,
	ingestSvc *ingestion.Service,
	log *zap.Logger,
) (Adapter, error)

// --- Driver registry ---

var (
	driverMu       sync.RWMutex
	driverRegistry = map[string]AdapterFactory{}
)

// RegisterDriver registers a named AdapterFactory.
// Callers register custom drivers (e.g. "kcp" for push-based delivery)
// via init() and metalog selects the driver from config.KafkaDriver.
func RegisterDriver(name string, factory AdapterFactory) {
	driverMu.Lock()
	defer driverMu.Unlock()
	driverRegistry[name] = factory
}

// GetDriver returns the AdapterFactory registered under the given name.
func GetDriver(name string) (AdapterFactory, error) {
	driverMu.RLock()
	factory, ok := driverRegistry[name]
	driverMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown kafka driver %q", name)
	}
	return factory, nil
}
