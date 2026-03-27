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
