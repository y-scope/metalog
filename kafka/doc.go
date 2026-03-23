// Package kafka provides shared interfaces and utilities for Kafka-based
// metadata ingestion.
//
// Concrete consumer implementations live in sub-packages:
//   - kafka/confluent — wraps confluent-kafka-go (requires CGO + librdkafka)
//   - kafka/franzgo   — wraps franz-go (pure Go, no CGO)
//
// Both implementations use the [MessageTransformer] interface to convert raw
// Kafka message payloads into protobuf MetadataRecord values, and integrate
// with the ingestion pipeline's back-pressure mechanism by waiting for flush
// confirmations before committing offsets.
package kafka
