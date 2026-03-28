package schema

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/metastore"
)

// recyclerScanInterval is how often the background recycler checks for reclaimable slots.
const recyclerScanInterval = time.Hour

// recyclerMinAge is the minimum time a column must be INVALIDATED before recycling.
const recyclerMinAge = 30 * 24 * time.Hour

// recyclerMaxNonNullRows is the maximum non-NULL rows allowed for recycling eligibility.
const recyclerMaxNonNullRows = 10000

// RefreshAliases re-reads alias_column values from the database for all active
// entries and updates the in-memory cache. Call periodically so that alias
// changes made via the admin API (possibly on a different node) propagate.
func (cr *ColumnRegistry) RefreshAliases(ctx context.Context) error {
	// Refresh dim aliases.
	dimQuery, dimArgs, _ := sq.Select("column_name", "COALESCE(alias_column, '')").
		From(metastore.DimRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "state": statusActive}).
		ToSql()
	dimRows, err := cr.db.QueryContext(ctx, dimQuery, dimArgs...)
	if err != nil {
		return fmt.Errorf("refresh dim aliases: %w", err)
	}
	defer dimRows.Close() //nolint:errcheck // cleanup

	dimAliases := make(map[string]string) // column_name -> alias
	for dimRows.Next() {
		var colName, alias string
		err = dimRows.Scan(&colName, &alias)
		if err != nil {
			return err
		}
		dimAliases[colName] = alias
	}
	err = dimRows.Err()
	if err != nil {
		return err
	}

	// Refresh agg aliases.
	aggQuery, aggArgs, _ := sq.Select("column_name", "COALESCE(alias_column, '')").
		From(metastore.AggRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "state": statusActive}).
		ToSql()
	aggRows, err := cr.db.QueryContext(ctx, aggQuery, aggArgs...)
	if err != nil {
		return fmt.Errorf("refresh agg aliases: %w", err)
	}
	defer aggRows.Close() //nolint:errcheck // cleanup

	aggAliases := make(map[string]string)
	for aggRows.Next() {
		var colName, alias string
		if err := aggRows.Scan(&colName, &alias); err != nil {
			return err
		}
		aggAliases[colName] = alias
	}
	if err := aggRows.Err(); err != nil {
		return err
	}

	// Apply changes under write lock. Replace entries whose alias changed
	// with new immutable copies to avoid data races with concurrent readers.
	// Also evict entries that are no longer ACTIVE (e.g., invalidated via admin API).
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Evict dims no longer in ACTIVE set.
	for colName, e := range cr.dimByColumn {
		if _, stillActive := dimAliases[colName]; !stillActive {
			delete(cr.dimByColumn, colName)
			delete(cr.dimByKey, e.DimKey)
			cr.log.Info("evicted invalidated dim from cache",
				zap.String("column", colName), zap.String("dimKey", e.DimKey))
		}
	}
	// Evict aggs no longer in ACTIVE set.
	for colName, e := range cr.aggByColumn {
		if _, stillActive := aggAliases[colName]; !stillActive {
			delete(cr.aggByColumn, colName)
			key := AggCacheKey(e.AggKey, e.AggValue, e.AggregationType)
			delete(cr.aggByKey, key)
			cr.log.Info("evicted invalidated agg from cache",
				zap.String("column", colName), zap.String("aggKey", e.AggKey))
		}
	}

	// Update aliases for remaining ACTIVE entries.
	for colName, newAlias := range dimAliases {
		if e, ok := cr.dimByColumn[colName]; ok && e.AliasCol != newAlias {
			updated := &DimRegistryEntry{
				TableName: e.TableName, ColumnName: e.ColumnName,
				BaseType: e.BaseType, Width: e.Width, DimKey: e.DimKey,
				AliasCol: newAlias, Status: e.Status,
			}
			cr.dimByKey[e.DimKey] = updated
			cr.dimByColumn[colName] = updated
		}
	}
	for colName, newAlias := range aggAliases {
		if e, ok := cr.aggByColumn[colName]; ok && e.AliasCol != newAlias {
			key := AggCacheKey(e.AggKey, e.AggValue, e.AggregationType)
			updated := &AggRegistryEntry{
				TableName: e.TableName, ColumnName: e.ColumnName,
				AggKey: e.AggKey, AggValue: e.AggValue,
				AggregationType: e.AggregationType, ValueType: e.ValueType,
				AliasCol: newAlias, Status: e.Status,
			}
			cr.aggByKey[key] = updated
			cr.aggByColumn[colName] = updated
		}
	}
	return nil
}

// RunRecycler periodically scans for INVALIDATED columns that have aged past
// retention and recycles them to AVAILABLE. The flow:
//  1. Find INVALIDATED entries where invalidated_at + recyclerMinAge < now.
//  2. Check if the physical column has any non-NULL rows remaining.
//  3. If few remain, NULL them out in batches.
//  4. Mark the registry entry as AVAILABLE for reuse.
func (cr *ColumnRegistry) RunRecycler(ctx context.Context) {
	// Run once on startup to reclaim any aged-out INVALIDATED slots immediately.
	if err := cr.recycleOnce(ctx); err != nil && ctx.Err() == nil {
		cr.log.Warn("initial column recycler scan failed", zap.Error(err))
	}

	ticker := time.NewTicker(recyclerScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := cr.recycleOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				cr.log.Warn("column recycler cycle failed", zap.Error(err))
			}
		}
	}
}

// recycleOnce runs one recycler scan cycle for both dim and agg registries.
func (cr *ColumnRegistry) recycleOnce(ctx context.Context) error {
	cutoff := time.Now().Add(-recyclerMinAge).UnixNano()

	for _, cfg := range []struct {
		table  string
		prefix string
		keyCol string
	}{
		{metastore.DimRegistryTable, metastore.DimColumnPrefix, "dim_key"},
		{metastore.AggRegistryTable, metastore.AggColumnPrefix, "agg_key"},
	} {
		if err := cr.recycleRegistry(ctx, cfg.table, cfg.prefix, cfg.keyCol, cutoff); err != nil {
			return err
		}
	}
	return nil
}

// recycleRegistry scans one registry table for recyclable INVALIDATED entries.
func (cr *ColumnRegistry) recycleRegistry(ctx context.Context, registryTable, colPrefix, keyCol string, cutoff int64) error {
	// Find INVALIDATED entries past the minimum age.
	q, args, _ := sq.Select("column_name", keyCol).
		From(registryTable).
		Where(sq.And{
			sq.Eq{"table_name": cr.tableName, "state": statusInvalidated},
			sq.LtOrEq{"invalidated_at": cutoff},
		}).
		ToSql()
	rows, err := cr.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("recycler scan %s: %w", registryTable, err)
	}
	defer rows.Close() //nolint:errcheck // cleanup

	type candidate struct {
		colName string
		key     string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.colName, &c.key); err != nil {
			return err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, c := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := cr.recycleColumn(ctx, registryTable, c.colName, c.key); err != nil {
			cr.log.Warn("failed to recycle column",
				zap.String("column", c.colName), zap.Error(err))
			continue
		}
	}
	return nil
}

// recycleColumn checks if a column has few enough non-NULL rows to recycle,
// NULLs out remaining data, and marks it AVAILABLE.
//
// The slow work (COUNT + batch UPDATE) runs without allocMu to avoid starving
// allocation requests. This is safe because the column is INVALIDATED — no new
// data can be written to it. The final state transition to AVAILABLE is done
// under allocMu with a WHERE state = INVALIDATED guard, ensuring atomicity
// with concurrent claimAvailable*Slot calls.
func (cr *ColumnRegistry) recycleColumn(ctx context.Context, registryTable, colName, key string) error {
	quotedTable := db.QuoteIdentifier(cr.tableName)
	quotedCol := db.QuoteIdentifier(colName)

	// Count remaining non-NULL rows. If above threshold, skip — retention will
	// continue dropping partitions until the count falls below.
	var remaining int64
	err := cr.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(%s) FROM %s", quotedCol, quotedTable)).
		Scan(&remaining)
	if err != nil {
		return fmt.Errorf("count non-null rows: %w", err)
	}
	if remaining > recyclerMaxNonNullRows {
		cr.log.Info("recycler: column still has too many rows, skipping",
			zap.String("column", colName), zap.Int64("remaining", remaining),
			zap.Int("threshold", recyclerMaxNonNullRows))
		return nil
	}

	// NULL out remaining rows in batches.
	if remaining > 0 {
		cr.log.Info("recycler: clearing column data",
			zap.String("column", colName), zap.Int64("rows", remaining))
		var totalCleared int64
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			res, err := cr.db.ExecContext(ctx,
				fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s IS NOT NULL LIMIT 10000",
					quotedTable, quotedCol, quotedCol))
			if err != nil {
				return fmt.Errorf("null out column data: %w", err)
			}
			affected, _ := res.RowsAffected()
			if affected == 0 {
				break
			}
			totalCleared += affected
		}
		cr.log.Info("recycler: column data cleared",
			zap.String("column", colName), zap.Int64("totalRows", totalCleared))
	}

	// Hold allocMu for the state transition only, so claimAvailable*Slot cannot
	// observe the slot as AVAILABLE until the data has been fully cleared.
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Mark as AVAILABLE: clear metadata, reset key to empty string (NOT NULL constraint).
	update := sq.Update(registryTable).
		Set("state", statusAvailable).
		Set("invalidated_at", nil).
		Set("alias_column", nil).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": colName, "state": statusInvalidated})

	// Clear the key column: dim_key or agg_key (NOT NULL, use empty string sentinel).
	if registryTable == metastore.DimRegistryTable {
		update = update.Set("dim_key", "")
	} else {
		update = update.Set("agg_key", "").Set("agg_value", nil)
	}

	updateQ, updateArgs, _ := update.ToSql()
	if _, err := cr.db.ExecContext(ctx, updateQ, updateArgs...); err != nil {
		return fmt.Errorf("mark column available: %w", err)
	}

	cr.log.Info("recycled column to AVAILABLE",
		zap.String("column", colName), zap.String("previousKey", key))
	return nil
}
