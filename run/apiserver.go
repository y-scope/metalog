package run

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

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

// APIServer runs the metalog query API server. It parses --config from flags,
// starts the gRPC query + metadata services, and blocks until SIGINT/SIGTERM.
func APIServer() {
	configPath := flag.String("config", "/etc/clp/node.yaml", "path to node.yaml config")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.LoadNodeConfig(*configPath)
	if err != nil {
		log.Fatal("failed to load config", zap.String("path", *configPath), zap.Error(err))
	}

	pool, err := db.NewPool(cfg.Node.Database)
	if err != nil {
		log.Fatal("failed to create DB pool", zap.Error(err))
	}
	defer pool.Close()

	var healthSrv *health.Server
	if cfg.Node.Health.Enabled {
		healthSrv = health.NewServer(cfg.Node.Health.Port, log)
		go func() {
			if err := healthSrv.Start(); err != nil {
				log.Error("health server error", zap.Error(err))
			}
		}()
	}

	grpcSrv := grpcserver.NewServer(cfg.Node.GRPC.Port, log)

	queryEngine := query.NewSplitQueryEngine(pool, log)
	queryGrpc := grpcserver.NewQueryHandler(queryEngine, func(string) *schema.ColumnRegistry { return nil }, log)
	splitspb.RegisterQuerySplitsServiceServer(grpcSrv.GRPCServer(), queryGrpc)

	metaGrpc := grpcserver.NewMetadataHandler(pool, log)
	metadatapb.RegisterMetadataServiceServer(grpcSrv.GRPCServer(), metaGrpc)

	if healthSrv != nil {
		healthSrv.SetReady(true)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := grpcSrv.Start(); err != nil {
			errCh <- err
		}
	}()

	log.Info("API server started", zap.Int("grpcPort", cfg.Node.GRPC.Port))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		log.Error("gRPC server failed, shutting down", zap.Error(err))
	}

	grpcSrv.Stop()

	if healthSrv != nil {
		healthSrv.SetReady(false)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		healthSrv.Stop(shutdownCtx)
	}

	log.Info("API server shutdown complete")
}
