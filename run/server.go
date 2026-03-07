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

// Server runs the metalog coordinator server. It parses --config from flags,
// starts the node + gRPC services, and blocks until SIGINT/SIGTERM.
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

	var grpcSrv *grpcserver.Server
	if cfg.Node.GRPC.Enabled {
		grpcSrv = grpcserver.NewServer(cfg.Node.GRPC.Port, log)

		ingestionGrpc := grpcserver.NewIngestionHandler(n.IngestionService(), log)
		ingestionpb.RegisterMetadataIngestionServiceServer(grpcSrv.GRPCServer(), ingestionGrpc)

		regSvc := coordinator.NewTableRegistration(n.Shared().DB, n.Shared().IsMariaDB, cfg.Node.Storage.TableCompression, log)
		coordGrpc := grpcserver.NewCoordinatorHandler(regSvc, log)
		coordinatorpb.RegisterCoordinatorServiceServer(grpcSrv.GRPCServer(), coordGrpc)

		queryEngine := query.NewSplitQueryEngine(n.Shared().DB, log)
		queryGrpc := grpcserver.NewQueryHandler(queryEngine, n.Shared().GetColumnRegistry, log)
		splitspb.RegisterQuerySplitsServiceServer(grpcSrv.GRPCServer(), queryGrpc)

		metaGrpc := grpcserver.NewMetadataHandler(n.Shared().DB, log)
		metadatapb.RegisterMetadataServiceServer(grpcSrv.GRPCServer(), metaGrpc)

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
