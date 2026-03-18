package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/timeutil"
)

// Structural partition names — these bookend the daily partition range and
// are never dropped or merged away by cleanup.
const (
	partFloor  = "p_floor"  // catch-all for timestamps before the daily range
	partFuture = "p_future" // catch-all for timestamps beyond the last daily partition
)

// PartitionManager manages MySQL RANGE partitions on the metadata table.
// Partitions are daily, keyed on min_timestamp (epoch nanoseconds).
type PartitionManager struct {
	db             *sql.DB
	tableName      string
	lookaheadDays  int
	cleanupAgeDays int
	log            *zap.Logger
}

// NewPartitionManager creates a PartitionManager.
func NewPartitionManager(db *sql.DB, tableName string, lookaheadDays, cleanupAgeDays int, log *zap.Logger) *PartitionManager {
	return &PartitionManager{
		db:             db,
		tableName:      tableName,
		lookaheadDays:  lookaheadDays,
		cleanupAgeDays: cleanupAgeDays,
		log:            log,
	}
}

// RunMaintenance acquires an advisory lock and runs partition maintenance.
// Returns immediately without waiting if another node holds the lock.
func (pm *PartitionManager) RunMaintenance(ctx context.Context) error {
	lockName := "pm_" + pm.tableName
	lock, err := metastore.AcquireAdvisoryLock(ctx, pm.db, lockName, 0)
	if err != nil {
		if errors.Is(err, metastore.ErrLockNotAcquired) {
			pm.log.Debug("partition maintenance lock held by another node, skipping",
				zap.String("table", pm.tableName))
			return nil
		}
		return fmt.Errorf("acquire partition maintenance lock: %w", err)
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			pm.log.Warn("release partition maintenance lock failed", zap.Error(err))
		}
	}()

	pm.log.Debug("starting partition maintenance", zap.String("table", pm.tableName))

	if _, err := pm.createLookaheadPartitions(ctx); err != nil {
		return err
	}

	if pm.cleanupAgeDays > 0 {
		if err := pm.cleanupOldPartitions(ctx); err != nil {
			pm.log.Warn("partition cleanup failed", zap.Error(err))
		}
	}

	pm.log.Debug("partition maintenance completed", zap.String("table", pm.tableName))
	return nil
}

// EnsureLookaheadPartitions creates partitions ahead of today. It attempts to
// acquire an advisory lock and proceeds without it after a timeout.
func (pm *PartitionManager) EnsureLookaheadPartitions(ctx context.Context) (int, error) {
	lockName := "pm_" + pm.tableName
	lock, err := metastore.AcquireAdvisoryLock(ctx, pm.db, lockName, 5)
	if err != nil {
		if !errors.Is(err, metastore.ErrLockNotAcquired) {
			return 0, fmt.Errorf("acquire lookahead lock: %w", err)
		}
		pm.log.Warn("advisory lock held by another node, proceeding without lock",
			zap.String("table", pm.tableName))
	} else {
		defer func() {
			if err := lock.Release(ctx); err != nil {
				pm.log.Warn("release lookahead lock failed", zap.Error(err))
			}
		}()
	}

	return pm.createLookaheadPartitions(ctx)
}

func (pm *PartitionManager) createLookaheadPartitions(ctx context.Context) (int, error) {
	return createLookaheadPartitions(ctx, pm.db, pm.tableName, pm.lookaheadDays, pm.log)
}

// PartitionInfo holds metadata about a single partition.
type PartitionInfo struct {
	Name        string
	Description string
	Rows        int64
	DataLength  int64
}

// cleanupOldPartitions merges or drops old partitions to reduce partition count.
//
// Empty partitions (verified via COUNT(*), not the INFORMATION_SCHEMA estimate)
// are dropped outright — no data loss. Sparse partitions older than
// cleanupAgeDays are merged into p_floor via REORGANIZE PARTITION, expanding
// its boundary. This preserves all rows while reducing the partition count.
//
// REORGANIZE requires consecutive partitions, so the merge builds a contiguous
// run from p_floor through the last old partition. Any partition that couldn't
// be dropped (failed DROP or non-empty) is included in the merge instead.
//
// p_floor and p_future are never dropped or merged away — they are the
// structural bookends of the partition scheme.
func (pm *PartitionManager) cleanupOldPartitions(ctx context.Context) error {
	partitions, err := getExistingPartitions(ctx, pm.db, pm.tableName)
	if err != nil {
		return err
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -pm.cleanupAgeDays).Truncate(24 * time.Hour)
	cutoffName := timeutil.DayPartitionName(cutoff.UnixNano())

	// Check if p_floor exists. Without it, we can't merge.
	hasFloor := false
	for _, p := range partitions {
		if p.Name == partFloor {
			hasFloor = true
			break
		}
	}

	// Collect old daily partitions eligible for cleanup.
	// Skip p_floor (structural) and p_future, plus anything newer than the cutoff.
	var candidates []PartitionInfo
	for _, p := range partitions {
		if p.Name == partFloor || p.Name == partFuture || p.Name >= cutoffName {
			continue
		}
		candidates = append(candidates, p)
	}

	// Process candidates: drop verified-empty partitions, collect the rest for merge.
	// REORGANIZE requires consecutive partitions, so if a DROP fails we must
	// include the partition in the merge group to avoid gaps.
	var mergeGroup []PartitionInfo
	for _, p := range candidates {
		if p.Rows == 0 && pm.isPartitionEmpty(ctx, p.Name) {
			alterSQL := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s",
				db.QuoteIdentifier(pm.tableName), db.QuoteIdentifier(p.Name))
			if _, err := pm.db.ExecContext(ctx, alterSQL); err != nil {
				pm.log.Warn("failed to drop empty partition, will include in merge",
					zap.String("partition", p.Name), zap.Error(err))
				mergeGroup = append(mergeGroup, p)
			} else {
				pm.log.Info("dropped empty partition", zap.String("partition", p.Name))
			}
			continue
		}
		mergeGroup = append(mergeGroup, p)
	}

	if len(mergeGroup) == 0 || !hasFloor {
		return nil
	}

	// Merge old partitions into p_floor via REORGANIZE PARTITION.
	// This expands p_floor's boundary to cover the merged range.
	lastBoundary := mergeGroup[len(mergeGroup)-1].Description

	partNames := db.QuoteIdentifier(partFloor)
	var totalRows int64
	for _, p := range mergeGroup {
		partNames += ", " + db.QuoteIdentifier(p.Name)
		totalRows += p.Rows
	}

	alterSQL := fmt.Sprintf("ALTER TABLE %s REORGANIZE PARTITION %s INTO (PARTITION %s VALUES LESS THAN (%s))",
		db.QuoteIdentifier(pm.tableName), partNames, db.QuoteIdentifier(partFloor), lastBoundary)

	if _, err := pm.db.ExecContext(ctx, alterSQL); err != nil {
		pm.log.Warn("failed to merge partitions into floor",
			zap.Int("count", len(mergeGroup)), zap.Error(err))
		return nil
	}

	pm.log.Info("merged old partitions into floor",
		zap.Int("merged", len(mergeGroup)),
		zap.Int64("totalRows", totalRows),
	)
	return nil
}

// isPartitionEmpty verifies a partition has zero rows via COUNT(*).
// INFORMATION_SCHEMA.TABLE_ROWS is an InnoDB estimate that can be inaccurate;
// this provides an exact check before dropping.
func (pm *PartitionManager) isPartitionEmpty(ctx context.Context, partName string) bool {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s PARTITION (%s)",
		db.QuoteIdentifier(pm.tableName), db.QuoteIdentifier(partName))
	var count int64
	if err := pm.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		pm.log.Warn("failed to count partition rows, assuming non-empty",
			zap.String("partition", partName), zap.Error(err))
		return false
	}
	return count == 0
}

func (pm *PartitionManager) getExistingPartitions(ctx context.Context) ([]PartitionInfo, error) {
	return getExistingPartitions(ctx, pm.db, pm.tableName)
}

// defaultProvisionLookaheadDays is the number of lookahead partitions created
// during table provisioning. The PartitionManager's configured lookaheadDays
// takes over for ongoing maintenance.
const defaultProvisionLookaheadDays = 7

// createLookaheadPartitions creates daily partitions from today through
// today + lookaheadDays by reorganizing p_future.
func createLookaheadPartitions(ctx context.Context, database *sql.DB, tableName string, lookaheadDays int, log *zap.Logger) (int, error) {
	existing, err := getExistingPartitions(ctx, database, tableName)
	if err != nil {
		return 0, err
	}

	existingNames := make(map[string]bool)
	for _, p := range existing {
		existingNames[p.Name] = true
	}

	today := time.Now().UTC().Truncate(24 * time.Hour)
	created := 0

	for i := 0; i <= lookaheadDays; i++ {
		partDate := today.AddDate(0, 0, i)
		partName := timeutil.DayPartitionName(partDate.UnixNano())

		if existingNames[partName] {
			continue
		}

		nextDay := partDate.AddDate(0, 0, 1)
		boundary := nextDay.UnixNano()

		alterSQL := fmt.Sprintf(
			"ALTER TABLE %s REORGANIZE PARTITION %s INTO (PARTITION %s VALUES LESS THAN (%d), PARTITION %s VALUES LESS THAN MAXVALUE)",
			db.QuoteIdentifier(tableName), partFuture, db.QuoteIdentifier(partName), boundary, partFuture,
		)

		_, err := database.ExecContext(ctx, alterSQL)
		if err != nil {
			if db.IsDuplicatePartition(err) {
				log.Debug("partition already exists (concurrent creation)",
					zap.String("partition", partName))
				continue
			}
			return created, fmt.Errorf("create partition %s: %w", partName, err)
		}
		created++
		log.Info("created partition", zap.String("partition", partName))
	}

	return created, nil
}

// getExistingPartitions queries INFORMATION_SCHEMA for all partitions of a table.
func getExistingPartitions(ctx context.Context, database *sql.DB, tableName string) ([]PartitionInfo, error) {
	rows, err := database.QueryContext(ctx,
		"SELECT PARTITION_NAME, PARTITION_DESCRIPTION, TABLE_ROWS, DATA_LENGTH "+
			"FROM INFORMATION_SCHEMA.PARTITIONS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? "+
			"ORDER BY PARTITION_ORDINAL_POSITION",
		tableName,
	)
	if err != nil {
		return nil, fmt.Errorf("query partitions: %w", err)
	}
	defer rows.Close()

	var partitions []PartitionInfo
	for rows.Next() {
		var p PartitionInfo
		var desc sql.NullString
		if err := rows.Scan(&p.Name, &desc, &p.Rows, &p.DataLength); err != nil {
			return nil, err
		}
		if desc.Valid {
			p.Description = desc.String
		}
		partitions = append(partitions, p)
	}
	return partitions, rows.Err()
}
