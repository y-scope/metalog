package retention

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/logutil"
	"github.com/y-scope/metalog/metastore"
	"github.com/y-scope/metalog/timeutil"
	"github.com/y-scope/metalog/storage"
)

const defaultScanInterval = 60 * time.Second

// defaultDeleteRate is the maximum number of storage object deletions per second.
// Prevents overwhelming object storage during large retention sweeps.
const defaultDeleteRate = 500

func init() {
	RegisterType("default", StrategyMeta{
		Factory: newDefaultStrategy,
	})
}

// defaultStrategy is the built-in retention cleanup implementation.
//
// Two-phase delete:
//  1. TransitionExpiredToPurging — crash-safe marker (DB survives restart)
//  2. DeleteExpiredFiles — collects storage paths, deletes DB rows
//  3. Delete from object storage (best-effort, idempotent)
type defaultStrategy struct {
	fileRecs           *metastore.FileRecords
	storageRegistry    *storage.Registry
	interval           time.Duration
	failureLogInterval time.Duration
	deleteRate         int // max storage deletions per second
	log                *zap.Logger
}

func newDefaultStrategy(deps Deps) (Strategy, error) {
	fr, err := metastore.NewFileRecords(deps.DB, deps.TableName, deps.IsMariaDB, deps.Log)
	if err != nil {
		return nil, fmt.Errorf("default retention strategy: %w", err)
	}

	failureInterval := deps.FailureLogInterval
	if failureInterval <= 0 {
		failureInterval = 60 * time.Second
	}

	return &defaultStrategy{
		fileRecs:           fr,
		storageRegistry:    deps.StorageRegistry,
		interval:           defaultScanInterval,
		failureLogInterval: failureInterval,
		deleteRate:         defaultDeleteRate,
		log:                deps.Log.With(zap.String("table", deps.TableName)),
	}, nil
}

// Run executes the retention cleanup loop until ctx is canceled.
func (s *defaultStrategy) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	fl := logutil.NewFailureLogger(s.log, s.failureLogInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.runOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				fl.Fail("retention scan failed", zap.Error(err))
			} else {
				fl.OK()
			}
		}
	}
}

// runOnce executes one retention cleanup cycle:
// transition → delete rows (collecting paths) → delete storage.
func (s *defaultStrategy) runOnce(ctx context.Context) error {
	now := timeutil.EpochNanos()

	// Phase 1: mark expired files as PURGING.
	transitioned, err := s.fileRecs.TransitionExpiredToPurging(ctx, now)
	if err != nil {
		return fmt.Errorf("transition to purging: %w", err)
	}
	if transitioned > 0 {
		s.log.Info("transitioned expired files to purging", zap.Int64("count", transitioned))
	}

	// Phase 2: delete PURGING rows and collect storage paths.
	result, err := s.fileRecs.DeleteExpiredFiles(ctx, now)
	if err != nil {
		return fmt.Errorf("delete expired files: %w", err)
	}
	if result.DeletedCount == 0 {
		return nil
	}
	s.log.Info("deleted expired metadata rows",
		zap.Int64("count", result.DeletedCount),
		zap.Int("irPaths", len(result.IRPaths)),
		zap.Int("archivePaths", len(result.ArchivePaths)),
	)

	// Phase 3: delete from object storage (best-effort).
	s.deleteStoragePaths(ctx, result.IRPaths)
	s.deleteStoragePaths(ctx, result.ArchivePaths)

	return nil
}

// deleteStoragePaths deletes files from object storage, throttled to deleteRate ops/sec.
// Errors are logged but do not fail the cycle — storage backends are idempotent
// (NotFound = success).
func (s *defaultStrategy) deleteStoragePaths(ctx context.Context, paths []metastore.StoragePath) {
	if s.storageRegistry == nil || len(paths) == 0 {
		return
	}

	interval := time.Second / time.Duration(s.deleteRate)

	for i, p := range paths {
		if p.Backend == "" || p.Path == "" {
			continue
		}

		// Rate-limit after the first deletion.
		if i > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		backend, err := s.storageRegistry.Get(p.Backend)
		if err != nil {
			s.log.Warn("retention: unknown storage backend",
				zap.String("backend", p.Backend), zap.String("path", p.Path))
			continue
		}
		if err := backend.Delete(ctx, p.Bucket, p.Path); err != nil {
			s.log.Warn("retention: failed to delete storage object",
				zap.String("backend", p.Backend), zap.String("bucket", p.Bucket),
				zap.String("path", p.Path), zap.Error(err))
		}
	}
}
