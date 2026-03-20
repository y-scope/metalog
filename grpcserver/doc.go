// Package grpcserver provides the gRPC server and service handlers for metalog.
//
// It exposes four services:
//   - Metadata ingestion (push-based record ingestion)
//   - Query splits (paginated metadata queries with filtering)
//   - Metadata service (table listing and schema introspection)
//   - Admin service (runtime table registration)
//
// The [Server] wraps a google.golang.org/grpc server with graceful shutdown support.
// Each handler adapts between protobuf request/response types and the
// corresponding internal service layer.
package grpcserver
