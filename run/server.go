package run

import (
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

// Server implements the "metalog serve" subcommand. It parses --config, starts
// the node (coordinator, workers, gRPC — all config-driven), and blocks until
// SIGINT/SIGTERM.
func Server() {
	configPath := flag.String("config", "/etc/clp/node.yaml", "path to node.yaml config")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.LoadNodeConfig(*configPath)
	if err != nil {
		log.Fatal("failed to load config", zap.String("path", *configPath), zap.Error(err))
	}

	n, err := node.NewNode(cfg, log)
	if err != nil {
		log.Fatal("failed to create node", zap.Error(err))
	}

	if err := n.Start(); err != nil {
		log.Fatal("failed to start node", zap.Error(err))
	}

	// Log database pool availability
	if n.Shared().DB == nil {
		log.Warn("no primary database configured — coordinator, worker, ingestion, and admin services are disabled")
	}
	if n.Shared().ReadDB != nil {
		log.Info("replica database pool active — query and metadata services will use replica")
	} else if n.Shared().DB != nil {
		log.Warn("no replica database configured — query and metadata services will use primary")
	}

	var grpcSrv *grpcserver.Server
	if cfg.GRPC.HasAnyService() {
		grpcSrv = grpcserver.NewServer(cfg.GRPC.Port, log)

		if cfg.GRPC.Ingestion {
			ingestionGrpc := grpcserver.NewIngestionHandler(n.IngestionService(), log)
			ingestionpb.RegisterMetadataIngestionServiceServer(grpcSrv.GRPCServer(), ingestionGrpc)
			log.Info("gRPC service registered", zap.String("service", "ingestion"))
		}

		if cfg.GRPC.Admin {
			regSvc := coordinator.NewTableRegistration(n.Shared().DB, n.Shared().IsMariaDB, cfg.Coordinator.TableCompression, log)
			adminGrpc := grpcserver.NewAdminHandler(regSvc, log)
			coordinatorpb.RegisterAdminServiceServer(grpcSrv.GRPCServer(), adminGrpc)
			log.Info("gRPC service registered", zap.String("service", "admin"))
		}

		if cfg.GRPC.Query {
			roDB := n.Shared().ReadOnlyDB()
			queryEngine := query.NewSplitQueryEngine(roDB, log)
			queryGrpc := grpcserver.NewQueryHandler(queryEngine, n.Shared().GetColumnRegistry, log)
			splitspb.RegisterQuerySplitsServiceServer(grpcSrv.GRPCServer(), queryGrpc)
			log.Info("gRPC service registered", zap.String("service", "query"))
		}

		if cfg.GRPC.Metadata {
			roDB := n.Shared().ReadOnlyDB()
			metaGrpc := grpcserver.NewMetadataHandler(roDB, log)
			metadatapb.RegisterMetadataServiceServer(grpcSrv.GRPCServer(), metaGrpc)
			log.Info("gRPC service registered", zap.String("service", "metadata"))
		}

		errCh := make(chan error, 1)
		go func() {
			if err := grpcSrv.Start(); err != nil {
				errCh <- err
			}
		}()

		// Wait for signal or gRPC failure — whichever comes first.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		select {
		case sig := <-sigCh:
			log.Info("received signal, shutting down", zap.String("signal", sig.String()))
		case err := <-errCh:
			log.Error("gRPC server failed, shutting down", zap.Error(err))
		}
	} else {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
	}

	if grpcSrv != nil {
		grpcSrv.Stop()
	}

	n.Stop()

	log.Info("shutdown complete")
}
