package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	coordinatorpb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	ingestionpb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	metadatapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	splitspb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/coordinator"
	grpcserver "github.com/y-scope/metalog/internal/grpc"
	"github.com/y-scope/metalog/internal/node"
	"github.com/y-scope/metalog/internal/query"
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

	// Create and start node
	n, err := node.NewNode(cfg, log)
	if err != nil {
		log.Fatal("failed to create node", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := n.Start(ctx); err != nil {
		log.Fatal("failed to start node", zap.Error(err))
	}

	// Start gRPC server
	var grpcSrv *grpcserver.Server
	if cfg.Node.GRPC.Enabled {
		grpcSrv = grpcserver.NewServer(cfg.Node.GRPC.Port, log)

		// Register ingestion service (shared with Kafka consumer)
		ingestionGrpc := grpcserver.NewIngestionHandler(n.IngestionService(), log)
		ingestionpb.RegisterMetadataIngestionServiceServer(grpcSrv.GRPCServer(), ingestionGrpc)

		// Register coordinator service
		regSvc := coordinator.NewTableRegistration(n.Shared().DB, n.Shared().IsMariaDB, cfg.Node.Storage.TableCompression, log)
		coordGrpc := grpcserver.NewCoordinatorHandler(regSvc, log)
		coordinatorpb.RegisterCoordinatorServiceServer(grpcSrv.GRPCServer(), coordGrpc)

		// Register query service
		queryEngine := query.NewSplitQueryEngine(n.Shared().DB, log)
		queryGrpc := grpcserver.NewQueryHandler(queryEngine, n.Shared().GetColumnRegistry, log)
		splitspb.RegisterQuerySplitsServiceServer(grpcSrv.GRPCServer(), queryGrpc)

		// Register metadata service
		metaGrpc := grpcserver.NewMetadataHandler(n.Shared().DB, log)
		metadatapb.RegisterMetadataServiceServer(grpcSrv.GRPCServer(), metaGrpc)

		go func() {
			if err := grpcSrv.Start(); err != nil {
				log.Fatal("gRPC server failed", zap.Error(err))
			}
		}()
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", zap.String("signal", sig.String()))

	// Stop gRPC server
	if grpcSrv != nil {
		grpcSrv.Stop()
	}

	// Stop node
	n.Stop()

	log.Info("shutdown complete")
}
