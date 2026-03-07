// Package grpc provides the gRPC server and service handlers for metalog.
//
// It exposes four services:
//   - Metadata ingestion (push-based record ingestion)
//   - Query splits (paginated metadata queries with filtering)
//   - Metadata service (table listing and schema introspection)
//   - Coordinator service (runtime table registration)
//
// The [Server] wraps a net/grpc server with graceful shutdown support.
// Each handler adapts between protobuf request/response types and the
// corresponding internal service layer.
package grpc
