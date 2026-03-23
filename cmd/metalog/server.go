package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	coordinatorpb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
	ingestionpb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	metadatapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	splitspb "github.com/y-scope/metalog/gen/proto/splitspb"
	"github.com/y-scope/metalog/config"
	"github.com/y-scope/metalog/coordinator"
	"github.com/y-scope/metalog/grpcserver"
	kafkaconfluent "github.com/y-scope/metalog/kafka/confluent"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/node"
	"github.com/y-scope/metalog/query"
	"github.com/y-scope/metalog/telemetry"
)

// Server implements the "metalog serve" subcommand. It parses --config, starts
// the node (coordinator, workers, gRPC — all config-driven), and blocks until
// SIGINT/SIGTERM.
func runServer() {
	configPath := flag.String("config", "/etc/clp/node.yaml", "path to node.yaml config")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.LoadNodeConfig(*configPath)
	if err != nil {
		log.Fatal("failed to load config", zap.String("path", *configPath), zap.Error(err))
	}

	// Create telemetry provider before the node so the Kafka adapter factory
	// can receive its meter at construction time.
	var telProv *telemetry.Provider
	if cfg.Telemetry.Enabled {
		telProv, err = telemetry.NewProvider(cfg.Telemetry)
		if err != nil {
			log.Fatal("failed to create telemetry provider", zap.Error(err))
		}
	}

	var kafkaMeter metric.Meter
	if telProv != nil {
		kafkaMeter = telProv.Meter("metalog.kafka")
	}

	n, err := node.NewNode(cfg, log,
		node.WithKafkaAdapterFactory(kafkaconfluent.NewDefaultAdapterFactory(kafkaMeter)),
		node.WithTelemetryProvider(telProv),
	)
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
			splitspb.RegisterSplitQueryServiceServer(grpcSrv.GRPCServer(), queryGrpc)
			log.Info("gRPC service registered", zap.String("service", "query"))
		}

		if cfg.GRPC.Metadata {
			roDB := n.Shared().ReadOnlyDB()
			querier := metastore.NewMetadataReader(roDB, log)
			metaGrpc := grpcserver.NewMetadataHandler(querier, log)
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
