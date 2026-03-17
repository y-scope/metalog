package grpc

import (
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Server wraps a gRPC server with lifecycle management.
type Server struct {
	srv    *grpc.Server
	health *health.Server
	port   int
	log    *zap.Logger
}

// NewServer creates a gRPC server on the given port with the standard
// gRPC health checking service registered.
func NewServer(port int, log *zap.Logger) *Server {
	srv := grpc.NewServer()
	reflection.Register(srv)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	// Set overall server status to SERVING.
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	return &Server{srv: srv, health: hs, port: port, log: log}
}

// GRPCServer returns the underlying grpc.Server for service registration.
func (s *Server) GRPCServer() *grpc.Server {
	return s.srv
}

// Start begins listening. Blocks until the server is stopped.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.log.Info("gRPC server starting", zap.Int("port", s.port))
	return s.srv.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	s.log.Info("stopping gRPC server")
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.srv.GracefulStop()
}
