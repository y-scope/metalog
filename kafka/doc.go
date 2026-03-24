// Package kafka provides shared interfaces and utilities for Kafka-based
// metadata ingestion.
//
// The concrete consumer implementation lives in the kafka/franzgo sub-package,
// which wraps franz-go (pure Go, no CGO).
//
// To use a different Kafka client library (e.g., confluent-kafka-go, Sarama),
// create a new sub-package under kafka/ that implements kafka.MessageSource
// and provides a NewDefaultAdapterFactory returning a node.KafkaAdapterFactory.
// Then change the import in cmd/metalog/server.go to point to your package.
// See kafka/franzgo/ for the reference implementation.
//
// Both implementations use the [MessageTransformer] interface to convert raw
// Kafka message payloads into protobuf MetadataRecord values, and integrate
// with the ingestion pipeline's back-pressure mechanism by waiting for flush
// confirmations before committing offsets.
package kafka
