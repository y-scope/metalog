// Package ingestion implements the metadata ingestion pipeline.
//
// The pipeline has three stages:
//  1. Records arrive via gRPC push or Kafka pull and are transformed into
//     [metastore.FileRecord] values by a [RecordTransformer].
//  2. The [Service] validates and routes records to the [BatchingWriter].
//  3. The [BatchingWriter] maintains a per-table goroutine that accumulates
//     records into batches and flushes them to the database using guarded
//     UPSERTs. Each record's Flushed channel is notified on completion,
//     enabling back-pressure to Kafka offset commits.
package ingestion
