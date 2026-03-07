package schema

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/timeutil"
)

// PartitionManager manages MySQL RANGE partitions on the metadata table.
// Partitions are daily, keyed on min_timestamp (epoch nanoseconds).
type PartitionManager struct {
	db                 *sql.DB
	tableName          string
	lookaheadDays      int
	cleanupAgeDays     int
	sparseRowThreshold int64
	log                *zap.Logger
}

// NewPartitionManager creates a PartitionManager.
func NewPartitionManager(db *sql.DB, tableName string, lookaheadDays, cleanupAgeDays int, sparseRowThreshold int64, log *zap.Logger) *PartitionManager {
	return &PartitionManager{
		db:                 db,
		tableName:          tableName,
		lookaheadDays:      lookaheadDays,
		cleanupAgeDays:     cleanupAgeDays,
		sparseRowThreshold: sparseRowThreshold,
		log:                log,
	}
}

// RunMaintenance acquires an advisory lock and runs partition maintenance.
// Returns immediately without waiting if another node holds the lock.
func (pm *PartitionManager) RunMaintenance(ctx context.Context) error {
	lockName := "pm_" + pm.tableName
	lock, err := metastore.AcquireAdvisoryLock(ctx, pm.db, lockName, 0)
	if err != nil {
		pm.log.Debug("partition maintenance lock held by another node, skipping",
			zap.String("table", pm.tableName))
		return nil
	}
	defer lock.Release(ctx)

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
		pm.log.Warn("advisory lock held by another node, proceeding without lock",
			zap.String("table", pm.tableName))
	} else {
		defer lock.Release(ctx)
	}

	return pm.createLookaheadPartitions(ctx)
}

func (pm *PartitionManager) createLookaheadPartitions(ctx context.Context) (int, error) {
	existing, err := pm.getExistingPartitions(ctx)
	if err != nil {
		return 0, err
	}

	existingNames := make(map[string]bool)
	for _, p := range existing {
		existingNames[p.Name] = true
	}

	today := time.Now().UTC().Truncate(24 * time.Hour)
	created := 0

	for i := 0; i <= pm.lookaheadDays; i++ {
		partDate := today.AddDate(0, 0, i)
		partName := timeutil.DayPartitionName(partDate.UnixNano())

		if existingNames[partName] {
			continue
		}

		// REORGANIZE p_future to create new partition
		nextDay := partDate.AddDate(0, 0, 1)
		boundary := nextDay.UnixNano()

		alterSQL := fmt.Sprintf(
			"ALTER TABLE %s REORGANIZE PARTITION p_future INTO (PARTITION %s VALUES LESS THAN (%d), PARTITION p_future VALUES LESS THAN MAXVALUE)",
			db.QuoteIdentifier(pm.tableName), db.QuoteIdentifier(partName), boundary,
		)

		_, err := pm.db.ExecContext(ctx, alterSQL)
		if err != nil {
			return created, fmt.Errorf("create partition %s: %w", partName, err)
		}
		created++
		pm.log.Info("created partition", zap.String("partition", partName))
	}

	return created, nil
}

// PartitionInfo holds metadata about a single partition.
type PartitionInfo struct {
	Name        string
	Description string
	Rows        int64
	DataLength  int64
}

// cleanupOldPartitions drops partitions older than cleanupAgeDays that contain few rows.
func (pm *PartitionManager) cleanupOldPartitions(ctx context.Context) error {
	partitions, err := pm.getExistingPartitions(ctx)
	if err != nil {
		return err
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -pm.cleanupAgeDays).Truncate(24 * time.Hour)
	cutoffName := timeutil.DayPartitionName(cutoff.UnixNano())

	for _, p := range partitions {
		// Never drop p_future or partitions that are too recent
		if p.Name == "p_future" || p.Name >= cutoffName {
			continue
		}
		// Only drop partitions that are sparse (few rows)
		if pm.sparseRowThreshold > 0 && p.Rows > pm.sparseRowThreshold {
			pm.log.Debug("skipping non-sparse partition",
				zap.String("partition", p.Name), zap.Int64("rows", p.Rows))
			continue
		}

		alterSQL := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s",
			db.QuoteIdentifier(pm.tableName), db.QuoteIdentifier(p.Name))
		_, err := pm.db.ExecContext(ctx, alterSQL)
		if err != nil {
			pm.log.Warn("failed to drop partition", zap.String("partition", p.Name), zap.Error(err))
			continue
		}
		pm.log.Info("dropped old partition", zap.String("partition", p.Name), zap.Int64("rows", p.Rows))
	}
	return nil
}

func (pm *PartitionManager) getExistingPartitions(ctx context.Context) ([]PartitionInfo, error) {
	rows, err := pm.db.QueryContext(ctx,
		"SELECT PARTITION_NAME, PARTITION_DESCRIPTION, TABLE_ROWS, DATA_LENGTH "+
			"FROM INFORMATION_SCHEMA.PARTITIONS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? "+
			"ORDER BY PARTITION_ORDINAL_POSITION",
		pm.tableName,
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
