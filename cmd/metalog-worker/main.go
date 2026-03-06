package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/health"
	"github.com/y-scope/metalog/storage"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/worker"
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

	// Use worker DB config if available, otherwise main DB
	dbCfg := cfg.Node.Database
	if cfg.Worker.Database != nil {
		dbCfg = *cfg.Worker.Database
	}
	pool, err := db.NewPool(dbCfg)
	if err != nil {
		log.Fatal("failed to create DB pool", zap.Error(err))
	}
	defer pool.Close()

	// Create storage registry
	storageReg := storage.NewRegistry()
	for name, backendCfg := range cfg.Node.Storage.Backends {
		s3Client := buildS3Client(backendCfg)
		if s3Client != nil {
			storageReg.Register(name, storage.NewS3Backend(s3Client))
		}
	}

	// Create compressor and archive creator
	var compressor *storage.ClpCompressor
	if cfg.Node.Storage.ClpBinaryPath != "" {
		compressor = storage.NewClpCompressor(
			cfg.Node.Storage.ClpBinaryPath,
			time.Duration(cfg.Node.Storage.ClpProcessTimeoutSeconds)*time.Second,
			log,
		)
	}
	archiveCreator := storage.NewArchiveCreator(storageReg, compressor, log)

	// Create task queue and prefetcher
	tq := taskqueue.NewQueue(pool, log)
	nodeID := cfg.ResolveNodeID()
	numWorkers := cfg.Worker.NumWorkers
	if numWorkers <= 0 {
		numWorkers = 4
	}

	prefetcher := worker.NewPrefetcher(tq, nodeID, config.DefaultTaskClaimBatchSize, log)

	ctx, cancel := context.WithCancel(context.Background())

	// Start health server
	var healthSrv *health.Server
	if cfg.Node.Health.Enabled {
		healthSrv = health.NewServer(cfg.Node.Health.Port, log)
		go func() {
			if err := healthSrv.Start(); err != nil {
				log.Error("health server error", zap.Error(err))
			}
		}()
		healthSrv.SetReady(true)
	}

	// Start prefetcher
	go prefetcher.Run(ctx)

	// Start workers
	done := make(chan struct{})
	go func() {
		defer close(done)
		workerDone := make(chan struct{}, numWorkers)
		for i := 0; i < numWorkers; i++ {
			core := worker.NewCore(
				tq, archiveCreator, prefetcher,
				log.With(zap.Int("workerId", i)),
			)
			go func() {
				core.Run(ctx)
				workerDone <- struct{}{}
			}()
		}
		for i := 0; i < numWorkers; i++ {
			<-workerDone
		}
	}()

	log.Info("standalone worker started", zap.Int("workers", numWorkers), zap.String("nodeId", nodeID))

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", zap.String("signal", sig.String()))

	cancel()
	<-done

	if healthSrv != nil {
		healthSrv.SetReady(false)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		healthSrv.Stop(shutdownCtx)
	}

	log.Info("worker shutdown complete")
}

func buildS3Client(cfg config.StorageBackendConfig) *s3.Client {
	if cfg.Endpoint == "" {
		return nil
	}
	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{URL: cfg.Endpoint}, nil
		})
	awsCfg := aws.Config{
		Region:                      cfg.Region,
		EndpointResolverWithOptions: resolver,
		Credentials: aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     cfg.AccessKey,
					SecretAccessKey: cfg.SecretKey,
				}, nil
			}),
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})
}
