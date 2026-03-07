// Package run provides top-level entry points for metalog server processes.
//
// [Server] runs a full coordinator node with gRPC services, Kafka consumers,
// workers, and health checks. [APIServer] runs a lightweight query-only
// server exposing gRPC query and metadata services without ingestion.
// Both handle signal-based graceful shutdown.
package run
