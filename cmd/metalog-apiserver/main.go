package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	metadatapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	splitspb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/db"
	grpcserver "github.com/y-scope/metalog/internal/grpc"
	"github.com/y-scope/metalog/internal/health"
	"github.com/y-scope/metalog/internal/query"
	"github.com/y-scope/metalog/internal/schema"
)

func main() {
	configPath := flag.String("config", "/etc/clp/node.yaml", "path to node.yaml config")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	// Load config
	cfg, err := config.LoadNodeConfig(*configPath)
	if err != nil {
		log.Fatal("failed to load config", zap.String("path", *configPath), zap.Error(err))
	}

	// Create DB pool (read-only for query API)
	pool, err := db.NewPool(cfg.Node.Database)
	if err != nil {
		log.Fatal("failed to create DB pool", zap.Error(err))
	}
	defer pool.Close()

	// Health server
	var healthSrv *health.Server
	if cfg.Node.Health.Enabled {
		healthSrv = health.NewServer(cfg.Node.Health.Port, log)
		go func() {
			if err := healthSrv.Start(); err != nil {
				log.Error("health server error", zap.Error(err))
			}
		}()
	}

	// gRPC server with query + metadata services only
	grpcSrv := grpcserver.NewServer(cfg.Node.GRPC.Port, log)

	queryEngine := query.NewSplitQueryEngine(pool, log)
	// API server has no coordinator, so registry lookup always returns nil.
	queryGrpc := grpcserver.NewQueryHandler(queryEngine, func(string) *schema.ColumnRegistry { return nil }, log)
	splitspb.RegisterQuerySplitsServiceServer(grpcSrv.GRPCServer(), queryGrpc)

	metaGrpc := grpcserver.NewMetadataHandler(pool, log)
	metadatapb.RegisterMetadataServiceServer(grpcSrv.GRPCServer(), metaGrpc)

	if healthSrv != nil {
		healthSrv.SetReady(true)
	}

	go func() {
		if err := grpcSrv.Start(); err != nil {
			log.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	log.Info("API server started", zap.Int("grpcPort", cfg.Node.GRPC.Port))

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", zap.String("signal", sig.String()))

	grpcSrv.Stop()

	if healthSrv != nil {
		healthSrv.SetReady(false)
	}

	log.Info("API server shutdown complete")
}
