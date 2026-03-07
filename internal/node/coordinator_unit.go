package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/config"
	"github.com/y-scope/metalog/internal/coordinator"
	"github.com/y-scope/metalog/internal/coordinator/consolidation"
	"github.com/y-scope/metalog/internal/coordinator/ingestion"
	kafkaconsumer "github.com/y-scope/metalog/kafka"
	"github.com/y-scope/metalog/internal/schema"
	"github.com/y-scope/metalog/internal/taskqueue"
)

// CoordinatorUnit manages coordinator goroutines for a single table.
type CoordinatorUnit struct {
	tableName     string
	shared        *SharedResources
	writer        *ingestion.BatchingWriter
	planner       *consolidation.Planner
	partition     *schema.PartitionManager
	registry      *schema.ColumnRegistry
	progress      *coordinator.ProgressTracker
	kafkaConsumer *kafkaconsumer.Consumer
	log           *zap.Logger

	parentCtx context.Context // preserved for Restart
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewCoordinatorUnit creates a coordinator unit for a table.
func NewCoordinatorUnit(
	ctx context.Context,
	tableName string,
	kafkaCfg config.TableKafkaConfig,
	shared *SharedResources,
	writer *ingestion.BatchingWriter,
	ingestSvc *ingestion.Service,
	log *zap.Logger,
) (*CoordinatorUnit, error) {
	reg, err := schema.NewColumnRegistry(ctx, shared.DB, tableName, log)
	if err != nil {
		return nil, fmt.Errorf("new coordinator unit: column registry: %w", err)
	}

	writer.SetRegistry(tableName, reg)
	ingestSvc.SetRegistry(tableName, reg)
	shared.SetColumnRegistry(tableName, reg)

	inFlight := consolidation.NewInFlightSet()
	policy := consolidation.NewTimeWindowPolicy(time.Hour, 2, 100)
	taskQueue := taskqueue.NewQueue(shared.DB, log)

	planner, err := consolidation.NewPlanner(
		shared.DB, tableName, policy, inFlight, taskQueue,
		shared.StorageRegistry,
		shared.ArchiveBackend, shared.ArchiveBucket,
		config.DefaultPlannerInterval,
		log,
	)
	if err != nil {
		return nil, fmt.Errorf("new coordinator unit: planner: %w", err)
	}

	partMgr := schema.NewPartitionManager(shared.DB, tableName, 3, 90, 1000, log)

	// Create Kafka consumer if configured — routes through IngestionService
	// for proper dim/agg column resolution.
	var kc *kafkaconsumer.Consumer
	if kafkaCfg.Topic != "" && kafkaCfg.BootstrapServers != "" {
		groupID := "metalog-coordinator-" + tableName
		kc = kafkaconsumer.NewConsumer(
			kafkaCfg.BootstrapServers, groupID, kafkaCfg.Topic, tableName,
			kafkaconsumer.NewTransformer(kafkaCfg.RecordTransformer),
			ingestSvc,
			log,
		)
	}

	progress := coordinator.NewProgressTracker(config.DefaultProgressStallTimeout, log)

	childCtx, cancel := context.WithCancel(ctx)

	return &CoordinatorUnit{
		tableName:     tableName,
		shared:        shared,
		writer:        writer,
		planner:       planner,
		partition:     partMgr,
		registry:      reg,
		progress:      progress,
		kafkaConsumer: kc,
		log:           log.With(zap.String("unit", "coordinator"), zap.String("table", tableName)),
		parentCtx:     ctx,
		ctx:           childCtx,
		cancel:        cancel,
	}, nil
}

// IsStalled returns true if the coordinator has not made progress within the stall timeout.
func (u *CoordinatorUnit) IsStalled() bool {
	return u.progress.IsStalled()
}

// Restart stops and restarts the coordinator goroutines.
func (u *CoordinatorUnit) Restart() {
	u.log.Warn("restarting stalled coordinator")
	u.Stop()
	u.ctx, u.cancel = context.WithCancel(u.parentCtx)
	u.progress.RecordProgress()
	u.Start()
}

// Start begins the coordinator goroutines.
func (u *CoordinatorUnit) Start() {
	u.log.Info("starting coordinator unit")

	// Planner goroutine
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.planner.Run(u.ctx)
	}()

	// Partition maintenance goroutine
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		u.runPartitionMaintenance()
	}()

	// Kafka consumer goroutine
	if u.kafkaConsumer != nil {
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			u.kafkaConsumer.Run(u.ctx)
		}()
	}

	u.log.Info("coordinator unit started")
}

// Stop signals all goroutines to stop and waits for completion.
func (u *CoordinatorUnit) Stop() {
	u.log.Info("stopping coordinator unit")
	u.cancel()
	u.wg.Wait()
	u.log.Info("coordinator unit stopped")
}

func (u *CoordinatorUnit) runPartitionMaintenance() {
	// Run once on startup
	if err := u.partition.RunMaintenance(u.ctx); err != nil {
		u.log.Warn("initial partition maintenance failed", zap.Error(err))
	}

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-u.ctx.Done():
			return
		case <-ticker.C:
			if err := u.partition.RunMaintenance(u.ctx); err != nil {
				if u.ctx.Err() != nil {
					return
				}
				u.log.Warn("partition maintenance failed", zap.Error(err))
			}
		}
	}
}
