// Package kafka provides a Kafka consumer and poller for metadata ingestion.
//
// The [Consumer] wraps confluent-kafka-go and implements a single-threaded
// poll loop that transforms Kafka messages into protobuf MetadataRecord values
// using a pluggable [MessageTransformer]. It integrates with the ingestion
// pipeline's back-pressure mechanism by waiting for Flushed channel signals
// before committing offsets.
//
// The [Poller] and [PollerFactory] manage consumer lifecycle within a
// coordinator unit, handling subscription, polling, and graceful shutdown.
package kafka
