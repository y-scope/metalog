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
// Empty partitions (0 rows) are dropped outright — no data loss.
// Consecutive sparse partitions older than cleanupAgeDays are merged into
// a single partition covering the combined range. This preserves all rows
// while reducing the number of partitions the query planner must evaluate.
func (pm *PartitionManager) cleanupOldPartitions(ctx context.Context) error {
	partitions, err := getExistingPartitions(ctx, pm.db, pm.tableName)
	if err != nil {
		return err
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -pm.cleanupAgeDays).Truncate(24 * time.Hour)
	cutoffName := timeutil.DayPartitionName(cutoff.UnixNano())

	// Collect old partitions eligible for merge/drop.
	var candidates []PartitionInfo
	for _, p := range partitions {
		if p.Name == "p_future" || p.Name >= cutoffName {
			continue
		}
		candidates = append(candidates, p)
	}

	// Drop empty partitions (safe — no data loss).
	var mergeGroup []PartitionInfo
	for _, p := range candidates {
		if p.Rows == 0 {
			alterSQL := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s",
				db.QuoteIdentifier(pm.tableName), db.QuoteIdentifier(p.Name))
			if _, err := pm.db.ExecContext(ctx, alterSQL); err != nil {
				pm.log.Warn("failed to drop empty partition",
					zap.String("partition", p.Name), zap.Error(err))
			} else {
				pm.log.Info("dropped empty partition", zap.String("partition", p.Name))
			}
			continue
		}
		mergeGroup = append(mergeGroup, p)
	}

	// Merge consecutive sparse partitions into one.
	// Need at least 2 to merge; the merged partition keeps the last one's boundary.
	if len(mergeGroup) < 2 {
		return nil
	}

	// Build REORGANIZE PARTITION p1, p2, ... INTO (p_merged VALUES LESS THAN (boundary))
	// The merged partition name is the first partition's name (preserves the oldest date
	// for readability) and its boundary is the last partition's boundary.
	mergedName := mergeGroup[0].Name
	lastBoundary := mergeGroup[len(mergeGroup)-1].Description // LESS THAN value

	var partNames string
	var totalRows int64
	for i, p := range mergeGroup {
		if i > 0 {
			partNames += ", "
		}
		partNames += db.QuoteIdentifier(p.Name)
		totalRows += p.Rows
	}

	alterSQL := fmt.Sprintf("ALTER TABLE %s REORGANIZE PARTITION %s INTO (PARTITION %s VALUES LESS THAN (%s))",
		db.QuoteIdentifier(pm.tableName), partNames, db.QuoteIdentifier(mergedName), lastBoundary)

	if _, err := pm.db.ExecContext(ctx, alterSQL); err != nil {
		pm.log.Warn("failed to merge partitions",
			zap.String("into", mergedName), zap.Int("count", len(mergeGroup)), zap.Error(err))
		return nil
	}

	pm.log.Info("merged old partitions",
		zap.String("into", mergedName),
		zap.Int("merged", len(mergeGroup)),
		zap.Int64("totalRows", totalRows),
	)
	return nil
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
			"ALTER TABLE %s REORGANIZE PARTITION p_future INTO (PARTITION %s VALUES LESS THAN (%d), PARTITION p_future VALUES LESS THAN MAXVALUE)",
			db.QuoteIdentifier(tableName), db.QuoteIdentifier(partName), boundary,
		)

		_, err := database.ExecContext(ctx, alterSQL)
		if err != nil {
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
