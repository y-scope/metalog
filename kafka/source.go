package kafka

import "context"

// MessageSource consumes messages from a Kafka topic and submits them
// to the ingestion pipeline. Implementations handle offset management.
type MessageSource interface {
	Run(ctx context.Context)
}

// DeadLetterHandler receives messages that failed transform or convert.
// Implementations can write to a dead-letter Kafka topic, a database table,
// or an object storage bucket. The default (nil) silently drops the message
// after logging via FailureLogger.
type DeadLetterHandler interface {
	// Handle processes a failed message. reason is "transform" or "convert".
	Handle(ctx context.Context, topic string, partition int32, offset int64, payload []byte, reason string, err error)
}
