// Package kafka provides shared interfaces and utilities for Kafka-based
// metadata ingestion.
//
// Concrete consumer implementations live in sub-packages:
//   - kafka/franzgo   — wraps franz-go (pure Go, no CGO) — default
//   - kafka/confluent — wraps confluent-kafka-go (requires CGO + librdkafka) — available but not used by default
//
// Both implementations use the [MessageTransformer] interface to convert raw
// Kafka message payloads into protobuf MetadataRecord values, and integrate
// with the ingestion pipeline's back-pressure mechanism by waiting for flush
// confirmations before committing offsets.
package kafka
