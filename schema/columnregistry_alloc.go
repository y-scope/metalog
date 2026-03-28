package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/metastore"
)

// ResolveOrAllocateDim resolves an existing dim mapping or allocates a new slot.
// If a new slot is needed, it inserts into the registry and issues ALTER TABLE ADD COLUMN.
// If the existing slot is narrower than width, the column is widened via ALTER TABLE MODIFY.
//
// Uses a fast-path/slow-path pattern: the fast path (RLock) handles the common
// case where the dim already exists. The slow path (allocMu) serializes DDL
// operations and double-checks under the lock to handle concurrent allocations.
func (cr *ColumnRegistry) ResolveOrAllocateDim(ctx context.Context, dimKey, baseType string, width int) (string, error) {
	if err := ValidateDimBaseType(baseType); err != nil {
		return "", err
	}
	// Fast path: check if already allocated (read-only, concurrent-safe).
	cr.mu.RLock()
	if e, ok := cr.dimByKey[dimKey]; ok {
		cr.mu.RUnlock()
		if width > e.Width && (baseType == "str" || baseType == "str_utf8") {
			return cr.expandDimWidth(ctx, e, width)
		}
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	// Slow path: allocate new slot (serialized via allocMu).
	return cr.allocateNewDimSlot(ctx, dimKey, baseType, width)
}

// expandDimWidth widens a VARCHAR dim column via ALTER TABLE MODIFY COLUMN.
// Widening within the same InnoDB length-prefix tier (≤255 or >255) is an
// in-place metadata change. Crossing the 255→256 boundary changes the length
// prefix from 1 to 2 bytes and requires a full table rebuild, so we cap at 255
// when the current width is ≤255.
func (cr *ColumnRegistry) expandDimWidth(ctx context.Context, entry *DimRegistryEntry, newWidth int) (string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Double-check under lock: another goroutine may have widened concurrently.
	cr.mu.RLock()
	current := cr.dimByKey[entry.DimKey]
	cr.mu.RUnlock()
	if current != nil {
		entry = current // use the latest snapshot for all subsequent checks
	}
	if newWidth <= entry.Width {
		return entry.ColumnName, nil
	}

	sqlType := dimSQLType(entry.BaseType, newWidth)

	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
			db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(entry.ColumnName), sqlType, lockMode(cr.isMariaDB)))
	if err != nil {
		return "", fmt.Errorf("expand dim width: %w", err)
	}

	// Update registry (only if still ACTIVE — a concurrent invalidation should not be overwritten).
	updateQuery, updateArgs, _ := sq.Update(metastore.DimRegistryTable).
		Set("width", newWidth).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": entry.ColumnName, "state": statusActive}).
		ToSql()
	res, err := cr.db.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return "", fmt.Errorf("update dim width registry: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		// The column was concurrently invalidated. The ALTER TABLE already widened
		// the physical column, so update the width on the INVALIDATED row to keep
		// the registry consistent with the physical schema. Without this, a future
		// reclaim would read the stale narrow width and might issue a narrowing ALTER.
		cr.log.Warn("dim column was concurrently invalidated during width expansion",
			zap.String("column", entry.ColumnName), zap.String("dimKey", entry.DimKey))
		fixQ, fixArgs, _ := sq.Update(metastore.DimRegistryTable).
			Set("width", newWidth).
			Where(sq.Eq{"table_name": cr.tableName, "column_name": entry.ColumnName}).
			ToSql()
		if _, fixErr := cr.db.ExecContext(ctx, fixQ, fixArgs...); fixErr != nil {
			cr.log.Error("failed to sync width on invalidated column",
				zap.String("column", entry.ColumnName), zap.Error(fixErr))
		}
		return "", fmt.Errorf("expand dim width: column %s concurrently invalidated", entry.ColumnName)
	}

	// Replace entry with a new immutable copy to avoid data races with readers.
	updated := &DimRegistryEntry{
		TableName:  entry.TableName,
		ColumnName: entry.ColumnName,
		BaseType:   entry.BaseType,
		Width:      newWidth,
		DimKey:     entry.DimKey,
		AliasCol:   entry.AliasCol,
		Status:     entry.Status,
	}
	cr.mu.Lock()
	cr.dimByKey[entry.DimKey] = updated
	cr.dimByColumn[entry.ColumnName] = updated
	cr.mu.Unlock()

	cr.log.Info("expanded dim column width",
		zap.String("column", entry.ColumnName),
		zap.String("dimKey", entry.DimKey),
		zap.Int("newWidth", newWidth),
	)
	return entry.ColumnName, nil
}

// lookupDimFromDB checks the database for an existing ACTIVE dim mapping,
// updating the in-memory cache if found. Used after acquiring the advisory
// lock to detect allocations by other nodes.
func (cr *ColumnRegistry) lookupDimFromDB(ctx context.Context, dimKey string) (string, error) {
	query, args, _ := sq.Select("column_name").
		From(metastore.DimRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "dim_key": dimKey, "state": statusActive}).
		Limit(1).
		ToSql()
	var col string
	err := cr.db.QueryRowContext(ctx, query, args...).Scan(&col)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lookup dim from DB: %w", err)
	}
	return col, nil
}

// lookupAggFromDB checks the database for an existing ACTIVE agg mapping.
func (cr *ColumnRegistry) lookupAggFromDB(ctx context.Context, aggKey, aggValue, aggType string) (string, error) {
	query, args, _ := sq.Select("column_name").
		From(metastore.AggRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "agg_key": aggKey, "aggregation_type": aggType, "state": statusActive}).
		ToSql()
	if aggValue != "" {
		query, args, _ = sq.Select("column_name").
			From(metastore.AggRegistryTable).
			Where(sq.Eq{"table_name": cr.tableName, "agg_key": aggKey, "agg_value": aggValue, "aggregation_type": aggType, "state": statusActive}).
			Limit(1).
			ToSql()
	}
	var col string
	err := cr.db.QueryRowContext(ctx, query, args...).Scan(&col)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lookup agg from DB: %w", err)
	}
	return col, nil
}

func (cr *ColumnRegistry) allocateNewDimSlot(ctx context.Context, dimKey, baseType string, width int) (string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Double-check: another goroutine may have allocated this dim while we
	// were waiting for allocMu.
	cr.mu.RLock()
	if e, ok := cr.dimByKey[dimKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	// Acquire cross-node advisory lock to prevent multi-node slot races.
	// Column allocation is infrequent (~10x on first day, then rarely),
	// so the lock overhead is negligible.
	lock, err := metastore.AcquireAdvisoryLock(ctx, cr.db, "col_alloc_"+cr.tableName, 10)
	if err != nil {
		return "", fmt.Errorf("acquire column allocation lock: %w", err)
	}
	defer func() {
		if releaseErr := lock.Release(ctx); releaseErr != nil {
			cr.log.Warn("release column allocation lock failed", zap.Error(releaseErr))
		}
	}()

	// Re-check from DB — another node may have allocated this dim while we
	// were waiting for the advisory lock.
	var col string
	col, err = cr.lookupDimFromDB(ctx, dimKey)
	if err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	// Try to claim an AVAILABLE (recycled) slot before allocating a fresh one.
	col, err = cr.claimAvailableDimSlot(ctx, dimKey, baseType, width)
	if err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	if cr.nextDimSlot > 99 {
		return "", ErrSlotExhausted
	}
	colName := fmt.Sprintf("%s%02d", metastore.DimColumnPrefix, cr.nextDimSlot)

	// Determine SQL type
	sqlType := dimSQLType(baseType, width)

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	if _, err = cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
			db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), sqlType, lockMode(cr.isMariaDB))); err != nil {
		// Column may already exist from a previous crashed attempt — check.
		if !isDuplicateColumn(err) {
			return "", fmt.Errorf("alter table add dim: %w", err)
		}
	}

	// INSERT into registry (safe now — the physical column exists)
	insertQuery, insertArgs, _ := sq.Insert(metastore.DimRegistryTable).
		Columns("table_name", "column_name", "base_type", "width", "dim_key", "alias_column", "state", "created_at").
		Values(cr.tableName, colName, baseType, width, dimKey, nil, statusActive, time.Now().UnixNano()).
		ToSql()
	if _, err = cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
		return "", fmt.Errorf("insert dim registry: %w", err)
	}

	cr.nextDimSlot++

	// Update cache
	entry := &DimRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		BaseType: baseType, Width: width, DimKey: dimKey, Status: statusActive,
	}
	cr.mu.Lock()
	cr.dimByKey[dimKey] = entry
	cr.dimByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("allocated dim slot", zap.String("dimKey", dimKey), zap.String("column", colName))
	return colName, nil
}

// ResolveOrAllocateAgg resolves an existing agg mapping or allocates a new slot.
// Same fast-path/slow-path pattern as [ResolveOrAllocateDim].
func (cr *ColumnRegistry) ResolveOrAllocateAgg(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, error) {
	cacheKey := AggCacheKey(aggKey, aggValue, aggType)

	// Fast path: read-only check.
	cr.mu.RLock()
	if e, ok := cr.aggByKey[cacheKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	// Slow path: allocate new slot.
	return cr.allocateNewAggSlot(ctx, aggKey, aggValue, aggType, valueType)
}

func (cr *ColumnRegistry) allocateNewAggSlot(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Double-check: another goroutine may have allocated this agg while we
	// were waiting for allocMu.
	cacheKey := AggCacheKey(aggKey, aggValue, aggType)
	cr.mu.RLock()
	if e, ok := cr.aggByKey[cacheKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	// Acquire cross-node advisory lock (same lock as dim allocation —
	// serializes all column changes for this table).
	lock, err := metastore.AcquireAdvisoryLock(ctx, cr.db, "col_alloc_"+cr.tableName, 10)
	if err != nil {
		return "", fmt.Errorf("acquire column allocation lock: %w", err)
	}
	defer func() {
		if releaseErr := lock.Release(ctx); releaseErr != nil {
			cr.log.Warn("release column allocation lock failed", zap.Error(releaseErr))
		}
	}()

	// Re-check from DB — another node may have allocated this agg.
	var col string
	col, err = cr.lookupAggFromDB(ctx, aggKey, aggValue, aggType)
	if err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	// Try to claim an AVAILABLE (recycled) slot before allocating a fresh one.
	col, err = cr.claimAvailableAggSlot(ctx, aggKey, aggValue, aggType, valueType)
	if err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	if cr.nextAggSlot > 99 {
		return "", ErrSlotExhausted
	}
	colName := fmt.Sprintf("%s%02d", metastore.AggColumnPrefix, cr.nextAggSlot)

	sqlType := "BIGINT"
	if valueType == "FLOAT" {
		sqlType = "DOUBLE"
	}

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	if _, err = cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
			db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), sqlType, lockMode(cr.isMariaDB))); err != nil {
		if !isDuplicateColumn(err) {
			return "", fmt.Errorf("alter table add agg: %w", err)
		}
	}

	// INSERT into registry (safe now — the physical column exists)
	insertQuery, insertArgs, _ := sq.Insert(metastore.AggRegistryTable).
		Columns("table_name", "column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column", "state", "created_at").
		Values(cr.tableName, colName, aggKey, nullIfEmpty(aggValue), aggType, valueType, nil, statusActive, time.Now().UnixNano()).
		ToSql()
	if _, err = cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
		return "", fmt.Errorf("insert agg registry: %w", err)
	}

	cr.nextAggSlot++

	entry := &AggRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		AggKey: aggKey, AggValue: aggValue,
		AggregationType: aggType, ValueType: valueType, Status: statusActive,
	}
	cr.mu.Lock()
	cr.aggByKey[cacheKey] = entry
	cr.aggByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("allocated agg slot", zap.String("aggKey", aggKey), zap.String("column", colName))
	return colName, nil
}

// ResolveOrAllocateDims resolves existing dim mappings and batch-allocates new slots.
// Returns a map from dimKey to column name. Width expansion for existing columns
// is handled individually (MODIFY COLUMN), while new columns are added via a
// single multi-column ALTER TABLE.
func (cr *ColumnRegistry) ResolveOrAllocateDims(ctx context.Context, reqs []DimRequest) (map[string]string, error) {
	result := make(map[string]string, len(reqs))

	// Fast path: partition into resolved, needs-widening, and unresolved.
	var unresolved []DimRequest
	type wideningCase struct {
		entry *DimRegistryEntry
		req   DimRequest
	}
	var needsWidening []wideningCase

	cr.mu.RLock()
	for _, r := range reqs {
		if e, ok := cr.dimByKey[r.DimKey]; ok {
			result[r.DimKey] = e.ColumnName
			if r.Width > e.Width && (r.BaseType == "str" || r.BaseType == "str_utf8") {
				needsWidening = append(needsWidening, wideningCase{entry: e, req: r})
			}
		} else {
			unresolved = append(unresolved, r)
		}
	}
	cr.mu.RUnlock()

	// Handle width expansion individually (MODIFY COLUMN, not ADD COLUMN).
	for _, w := range needsWidening {
		col, err := cr.expandDimWidth(ctx, w.entry, w.req.Width)
		if err != nil {
			return nil, err
		}
		result[w.req.DimKey] = col
	}

	if len(unresolved) == 0 {
		return result, nil
	}

	cols, err := cr.batchAllocateDimSlots(ctx, unresolved)
	if err != nil {
		return nil, err
	}
	for k, v := range cols {
		result[k] = v
	}
	return result, nil
}

// batchAllocateDimSlots adds multiple dim columns in a single ALTER TABLE.
// Falls back to individual ALTERs if the batch fails (e.g., duplicate column
// from a previous crashed attempt).
func (cr *ColumnRegistry) batchAllocateDimSlots(ctx context.Context, reqs []DimRequest) (map[string]string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Acquire cross-node advisory lock.
	lock, err := metastore.AcquireAdvisoryLock(ctx, cr.db, "col_alloc_"+cr.tableName, 10)
	if err != nil {
		return nil, fmt.Errorf("acquire column allocation lock: %w", err)
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			cr.log.Warn("release column allocation lock failed", zap.Error(err))
		}
	}()

	// Double-check: filter out any allocated while we waited for locks.
	// Check DB (not just cache) to catch allocations by other nodes.
	var pending []DimRequest
	result := make(map[string]string)

	for _, r := range reqs {
		cr.mu.RLock()
		e, ok := cr.dimByKey[r.DimKey]
		cr.mu.RUnlock()
		if ok {
			result[r.DimKey] = e.ColumnName
			continue
		}
		// Check DB for cross-node allocation
		if col, err := cr.lookupDimFromDB(ctx, r.DimKey); err != nil {
			return nil, err
		} else if col != "" {
			result[r.DimKey] = col
		} else {
			pending = append(pending, r)
		}
	}

	if len(pending) == 0 {
		return result, nil
	}

	// Try to claim AVAILABLE (recycled) slots first, one at a time.
	var stillPending []DimRequest
	for _, r := range pending {
		col, err := cr.claimAvailableDimSlot(ctx, r.DimKey, r.BaseType, r.Width)
		if err != nil {
			return nil, err
		}
		if col != "" {
			result[r.DimKey] = col
		} else {
			stillPending = append(stillPending, r)
		}
	}
	pending = stillPending
	if len(pending) == 0 {
		return result, nil
	}

	remaining := 100 - cr.nextDimSlot
	if remaining <= 0 {
		dropped := make([]string, len(pending))
		for i, r := range pending {
			dropped[i] = r.DimKey
		}
		cr.exhaustionFL.Fail("dim slots exhausted, dropping dimensions",
			zap.Int("dropped", len(pending)), zap.Strings("keys", dropped))
		cr.mSlotsExhausted.Add(context.Background(), int64(len(pending)),
			metric.WithAttributes(cr.tableAttr, attribute.String("kind", "dim")))
		return result, nil
	}
	if len(pending) > remaining {
		dropped := make([]string, 0, len(pending)-remaining)
		for _, r := range pending[remaining:] {
			dropped = append(dropped, r.DimKey)
		}
		cr.exhaustionFL.Fail("dim slots partially exhausted, dropping excess dimensions",
			zap.Int("allocated", remaining), zap.Int("dropped", len(dropped)), zap.Strings("droppedKeys", dropped))
		cr.mSlotsExhausted.Add(context.Background(), int64(len(dropped)),
			metric.WithAttributes(cr.tableAttr, attribute.String("kind", "dim")))
		pending = pending[:remaining]
	}

	// Assign slot names and SQL types for fresh allocations.
	type pendingSlot struct {
		colName string
		sqlType string
		req     DimRequest
	}
	slots := make([]pendingSlot, len(pending))
	alterParts := make([]string, len(pending))
	for i, r := range pending {
		colName := fmt.Sprintf("%s%02d", metastore.DimColumnPrefix, cr.nextDimSlot+i)
		sqlType := dimSQLType(r.BaseType, r.Width)
		slots[i] = pendingSlot{req: r, colName: colName, sqlType: sqlType}
		alterParts[i] = fmt.Sprintf("ADD COLUMN %s %s NULL",
			db.QuoteIdentifier(colName), sqlType)
	}

	// Single ALTER TABLE with all new columns.
	ddl := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=%s",
		db.QuoteIdentifier(cr.tableName), strings.Join(alterParts, ", "), lockMode(cr.isMariaDB))

	if _, err := cr.db.ExecContext(ctx, ddl); err != nil {
		if !isDuplicateColumn(err) {
			return nil, fmt.Errorf("batch alter table add dims: %w", err)
		}
		// Fallback: a previous crash left some columns without registry rows.
		// Add each individually so isDuplicateColumn is handled per-column.
		for _, s := range slots {
			_, fallbackErr := cr.db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
					db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(s.colName),
					s.sqlType, lockMode(cr.isMariaDB)))
			if fallbackErr != nil && !isDuplicateColumn(fallbackErr) {
				return nil, fmt.Errorf("alter table add dim %s: %w", s.colName, fallbackErr)
			}
		}
	}

	// Insert registry rows and update cache individually. The slot counter
	// is advanced per successful INSERT so that on a partial failure, only
	// the successfully registered slots are consumed — orphaned physical
	// columns (from the ALTER) will be reused on the next attempt since
	// isDuplicateColumn handles the already-existing column gracefully.
	now := time.Now().UnixNano()
	for _, s := range slots {
		insertQuery, insertArgs, _ := sq.Insert(metastore.DimRegistryTable).
			Columns("table_name", "column_name", "base_type", "width", "dim_key", "alias_column", "state", "created_at").
			Values(cr.tableName, s.colName, s.req.BaseType, s.req.Width, s.req.DimKey, nullIfEmpty(s.req.AliasCol), statusActive, now).
			ToSql()
		if _, err := cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
			return nil, fmt.Errorf("insert dim registry for %s: %w", s.req.DimKey, err)
		}
		cr.nextDimSlot++

		entry := &DimRegistryEntry{
			TableName: cr.tableName, ColumnName: s.colName,
			BaseType: s.req.BaseType, Width: s.req.Width, DimKey: s.req.DimKey,
			AliasCol: s.req.AliasCol, Status: statusActive,
		}
		cr.mu.Lock()
		cr.dimByKey[s.req.DimKey] = entry
		cr.dimByColumn[s.colName] = entry
		cr.mu.Unlock()

		result[s.req.DimKey] = s.colName
	}

	cr.log.Info("batch allocated dim slots", zap.Int("count", len(slots)))
	return result, nil
}

// ResolveOrAllocateAggs resolves existing agg mappings and batch-allocates new slots.
// Returns a map from composite key (aggKey+aggValue+aggType) to column name.
func (cr *ColumnRegistry) ResolveOrAllocateAggs(ctx context.Context, reqs []AggRequest) (map[string]string, error) {
	result := make(map[string]string, len(reqs))
	var unresolved []AggRequest

	cr.mu.RLock()
	for _, r := range reqs {
		key := AggCacheKey(r.AggKey, r.AggValue, r.AggType)
		if e, ok := cr.aggByKey[key]; ok {
			result[key] = e.ColumnName
		} else {
			unresolved = append(unresolved, r)
		}
	}
	cr.mu.RUnlock()

	if len(unresolved) == 0 {
		return result, nil
	}

	cols, err := cr.batchAllocateAggSlots(ctx, unresolved)
	if err != nil {
		return nil, err
	}
	for k, v := range cols {
		result[k] = v
	}
	return result, nil
}

// batchAllocateAggSlots adds multiple agg columns in a single ALTER TABLE.
func (cr *ColumnRegistry) batchAllocateAggSlots(ctx context.Context, reqs []AggRequest) (map[string]string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Acquire cross-node advisory lock.
	lock, err := metastore.AcquireAdvisoryLock(ctx, cr.db, "col_alloc_"+cr.tableName, 10)
	if err != nil {
		return nil, fmt.Errorf("acquire column allocation lock: %w", err)
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			cr.log.Warn("release column allocation lock failed", zap.Error(err))
		}
	}()

	// Double-check: filter out any allocated (cache + DB) while we waited.
	var pending []AggRequest
	result := make(map[string]string)

	for _, r := range reqs {
		key := AggCacheKey(r.AggKey, r.AggValue, r.AggType)
		cr.mu.RLock()
		e, ok := cr.aggByKey[key]
		cr.mu.RUnlock()
		if ok {
			result[key] = e.ColumnName
			continue
		}
		if col, err := cr.lookupAggFromDB(ctx, r.AggKey, r.AggValue, r.AggType); err != nil {
			return nil, err
		} else if col != "" {
			result[key] = col
		} else {
			pending = append(pending, r)
		}
	}

	if len(pending) == 0 {
		return result, nil
	}

	// Try to claim AVAILABLE (recycled) slots first.
	var stillPending []AggRequest
	for _, r := range pending {
		col, err := cr.claimAvailableAggSlot(ctx, r.AggKey, r.AggValue, r.AggType, r.ValueType)
		if err != nil {
			return nil, err
		}
		if col != "" {
			key := AggCacheKey(r.AggKey, r.AggValue, r.AggType)
			result[key] = col
		} else {
			stillPending = append(stillPending, r)
		}
	}
	pending = stillPending
	if len(pending) == 0 {
		return result, nil
	}

	remaining := 100 - cr.nextAggSlot
	if remaining <= 0 {
		dropped := make([]string, len(pending))
		for i, r := range pending {
			dropped[i] = AggCacheKey(r.AggKey, r.AggValue, r.AggType)
		}
		cr.exhaustionFL.Fail("agg slots exhausted, dropping aggregations",
			zap.Int("dropped", len(pending)), zap.Strings("keys", dropped))
		cr.mSlotsExhausted.Add(context.Background(), int64(len(pending)),
			metric.WithAttributes(cr.tableAttr, attribute.String("kind", "agg")))
		return result, nil
	}
	if len(pending) > remaining {
		dropped := make([]string, 0, len(pending)-remaining)
		for _, r := range pending[remaining:] {
			dropped = append(dropped, AggCacheKey(r.AggKey, r.AggValue, r.AggType))
		}
		cr.exhaustionFL.Fail("agg slots partially exhausted, dropping excess aggregations",
			zap.Int("allocated", remaining), zap.Int("dropped", len(dropped)), zap.Strings("droppedKeys", dropped))
		cr.mSlotsExhausted.Add(context.Background(), int64(len(dropped)),
			metric.WithAttributes(cr.tableAttr, attribute.String("kind", "agg")))
		pending = pending[:remaining]
	}

	type pendingSlot struct {
		req     AggRequest
		colName string
		sqlType string
	}
	slots := make([]pendingSlot, len(pending))
	alterParts := make([]string, len(pending))
	for i, r := range pending {
		colName := fmt.Sprintf("%s%02d", metastore.AggColumnPrefix, cr.nextAggSlot+i)
		sqlType := "BIGINT"
		if r.ValueType == "FLOAT" {
			sqlType = "DOUBLE"
		}
		slots[i] = pendingSlot{req: r, colName: colName, sqlType: sqlType}
		alterParts[i] = fmt.Sprintf("ADD COLUMN %s %s NULL",
			db.QuoteIdentifier(colName), sqlType)
	}

	ddl := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=%s",
		db.QuoteIdentifier(cr.tableName), strings.Join(alterParts, ", "), lockMode(cr.isMariaDB))

	if _, err := cr.db.ExecContext(ctx, ddl); err != nil {
		if !isDuplicateColumn(err) {
			return nil, fmt.Errorf("batch alter table add aggs: %w", err)
		}
		for _, s := range slots {
			_, fallbackErr := cr.db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
					db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(s.colName),
					s.sqlType, lockMode(cr.isMariaDB)))
			if fallbackErr != nil && !isDuplicateColumn(fallbackErr) {
				return nil, fmt.Errorf("alter table add agg %s: %w", s.colName, fallbackErr)
			}
		}
	}

	// Insert registry rows and advance slot counter per successful INSERT
	// (same rationale as dims — avoids orphaning slots on partial failure).
	now := time.Now().UnixNano()
	for _, s := range slots {
		insertQuery, insertArgs, _ := sq.Insert(metastore.AggRegistryTable).
			Columns("table_name", "column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column", "state", "created_at").
			Values(cr.tableName, s.colName, s.req.AggKey, nullIfEmpty(s.req.AggValue), s.req.AggType, s.req.ValueType, nullIfEmpty(s.req.AliasCol), statusActive, now).
			ToSql()
		if _, err := cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
			return nil, fmt.Errorf("insert agg registry for %s: %w", s.req.AggKey, err)
		}
		cr.nextAggSlot++

		key := AggCacheKey(s.req.AggKey, s.req.AggValue, s.req.AggType)
		entry := &AggRegistryEntry{
			TableName: cr.tableName, ColumnName: s.colName,
			AggKey: s.req.AggKey, AggValue: s.req.AggValue,
			AggregationType: s.req.AggType, ValueType: s.req.ValueType,
			AliasCol: s.req.AliasCol, Status: statusActive,
		}
		cr.mu.Lock()
		cr.aggByKey[key] = entry
		cr.aggByColumn[s.colName] = entry
		cr.mu.Unlock()

		result[key] = s.colName
	}

	cr.log.Info("batch allocated agg slots", zap.Int("count", len(slots)))
	return result, nil
}

// claimAvailableDimSlot attempts to reuse an AVAILABLE slot for a new dimension.
// Must be called with allocMu held. Returns ("", nil) if no AVAILABLE slot exists.
//
// DDL (ALTER TABLE) is issued outside the transaction because MariaDB/MySQL
// implicitly commits any open transaction before executing DDL. The flow:
//  1. Transaction: SELECT FOR UPDATE SKIP LOCKED → UPDATE state to ACTIVE → COMMIT
//  2. ALTER TABLE MODIFY COLUMN (outside tx, if type changed)
//  3. On ALTER failure: revert registry row back to AVAILABLE
//
// Deadlock-free by design: the SELECT uses SKIP LOCKED, so competing coordinators
// never block on each other's row locks — they simply skip already-locked rows and
// claim the next available slot. No gap locks are involved because the query targets
// concrete, existing rows (WHERE state = 'AVAILABLE' ... LIMIT 1).
func (cr *ColumnRegistry) claimAvailableDimSlot(ctx context.Context, dimKey, baseType string, width int) (string, error) {
	colName, oldBaseType, oldWidth, err := cr.claimAvailableDimSlotTx(ctx, dimKey, baseType, width)
	if err != nil || colName == "" {
		return colName, err
	}

	// Phase 2: ALTER TABLE outside transaction if type changed.
	newSQLType := dimSQLType(baseType, width)
	oldSQLType := dimSQLType(oldBaseType, oldWidth)
	if newSQLType != oldSQLType {
		_, err := cr.db.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
				db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), newSQLType, lockMode(cr.isMariaDB)))
		if err != nil {
			// Revert: mark slot back to AVAILABLE so it's not permanently lost.
			cr.revertClaimedSlot(ctx, metastore.DimRegistryTable, colName)
			return "", fmt.Errorf("modify recycled dim column type: %w", err)
		}
	}

	// Phase 3: update in-memory cache.
	entry := &DimRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		BaseType: baseType, Width: width, DimKey: dimKey, Status: statusActive,
	}
	cr.mu.Lock()
	cr.dimByKey[dimKey] = entry
	cr.dimByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("claimed recycled dim slot",
		zap.String("dimKey", dimKey), zap.String("column", colName))
	return colName, nil
}

// claimAvailableDimSlotTx performs the transactional part of claiming an AVAILABLE dim slot.
// Returns the claimed column name, old base type, old width, or ("", "", 0, nil) if none available.
func (cr *ColumnRegistry) claimAvailableDimSlotTx(ctx context.Context, dimKey, baseType string, width int) (string, string, int, error) {
	tx, err := cr.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", 0, fmt.Errorf("begin tx for available dim slot: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	var colName, oldBaseType string
	var oldWidth sql.NullInt32
	row := tx.QueryRowContext(ctx,
		"SELECT column_name, base_type, width FROM "+metastore.DimRegistryTable+
			" WHERE table_name = ? AND state = ? ORDER BY column_name LIMIT 1 FOR UPDATE SKIP LOCKED",
		cr.tableName, statusAvailable)
	err = row.Scan(&colName, &oldBaseType, &oldWidth)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", 0, nil
		}
		return "", "", 0, fmt.Errorf("scan available dim slot: %w", err)
	}

	now := time.Now().UnixNano()
	updateQ, updateArgs, _ := sq.Update(metastore.DimRegistryTable).
		Set("state", statusActive).
		Set("dim_key", dimKey).
		Set("base_type", baseType).
		Set("width", width).
		Set("alias_column", nil).
		Set("invalidated_at", nil).
		Set("created_at", now).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": colName, "state": statusAvailable}).
		ToSql()
	res, err := tx.ExecContext(ctx, updateQ, updateArgs...)
	if err != nil {
		return "", "", 0, fmt.Errorf("claim available dim slot: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return "", "", 0, nil
	}

	if err := tx.Commit(); err != nil {
		return "", "", 0, fmt.Errorf("commit available dim slot: %w", err)
	}

	var oldW int
	if oldWidth.Valid {
		oldW = int(oldWidth.Int32)
	}
	return colName, oldBaseType, oldW, nil
}

// claimAvailableAggSlot attempts to reuse an AVAILABLE slot for a new aggregation.
// Must be called with allocMu held. Returns ("", nil) if no AVAILABLE slot exists.
// Same DDL-outside-transaction and deadlock-free SKIP LOCKED pattern as
// claimAvailableDimSlot — see that function's doc comment for details.
func (cr *ColumnRegistry) claimAvailableAggSlot(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, error) {
	colName, oldValueType, err := cr.claimAvailableAggSlotTx(ctx, aggKey, aggValue, aggType, valueType)
	if err != nil || colName == "" {
		return colName, err
	}

	// Phase 2: ALTER TABLE outside transaction (only if physical type changed).
	// Agg columns are BIGINT (INT) or DOUBLE (FLOAT). Skipping when types match
	// avoids an unnecessary DDL operation.
	newSQLType := "BIGINT"
	if valueType == "FLOAT" {
		newSQLType = "DOUBLE"
	}
	oldSQLType := "BIGINT"
	if oldValueType == "FLOAT" {
		oldSQLType = "DOUBLE"
	}
	if newSQLType != oldSQLType {
		_, err = cr.db.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
				db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), newSQLType, lockMode(cr.isMariaDB)))
		if err != nil {
			cr.revertClaimedSlot(ctx, metastore.AggRegistryTable, colName)
			return "", fmt.Errorf("modify recycled agg column type: %w", err)
		}
	}

	// Phase 3: update in-memory cache.
	cacheKey := AggCacheKey(aggKey, aggValue, aggType)
	entry := &AggRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		AggKey: aggKey, AggValue: aggValue,
		AggregationType: aggType, ValueType: valueType, Status: statusActive,
	}
	cr.mu.Lock()
	cr.aggByKey[cacheKey] = entry
	cr.aggByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("claimed recycled agg slot",
		zap.String("aggKey", aggKey), zap.String("column", colName))
	return colName, nil
}

// claimAvailableAggSlotTx performs the transactional part of claiming an AVAILABLE agg slot.
// Returns the claimed column name and the old value_type (for ALTER TABLE comparison).
func (cr *ColumnRegistry) claimAvailableAggSlotTx(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, string, error) {
	tx, err := cr.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", fmt.Errorf("begin tx for available agg slot: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	var colName, oldValueType string
	row := tx.QueryRowContext(ctx,
		"SELECT column_name, value_type FROM "+metastore.AggRegistryTable+
			" WHERE table_name = ? AND state = ? ORDER BY column_name LIMIT 1 FOR UPDATE SKIP LOCKED",
		cr.tableName, statusAvailable)
	err = row.Scan(&colName, &oldValueType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("scan available agg slot: %w", err)
	}

	now := time.Now().UnixNano()
	updateQ, updateArgs, _ := sq.Update(metastore.AggRegistryTable).
		Set("state", statusActive).
		Set("agg_key", aggKey).
		Set("agg_value", nullIfEmpty(aggValue)).
		Set("aggregation_type", aggType).
		Set("value_type", valueType).
		Set("alias_column", nil).
		Set("invalidated_at", nil).
		Set("created_at", now).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": colName, "state": statusAvailable}).
		ToSql()
	res, err := tx.ExecContext(ctx, updateQ, updateArgs...)
	if err != nil {
		return "", "", fmt.Errorf("claim available agg slot: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return "", "", nil
	}

	if err := tx.Commit(); err != nil {
		return "", "", fmt.Errorf("commit available agg slot: %w", err)
	}
	return colName, oldValueType, nil
}

// revertClaimedSlot reverts a claimed slot back to AVAILABLE after a failed ALTER TABLE.
// Only reverts if the column is still ACTIVE — if a concurrent admin InvalidateColumn
// already transitioned it to INVALIDATED, the revert is a no-op and the recycler will
// handle it through the normal INVALIDATED → AVAILABLE path (with data nulling).
func (cr *ColumnRegistry) revertClaimedSlot(ctx context.Context, registryTable, colName string) {
	update := sq.Update(registryTable).
		Set("state", statusAvailable).
		Set("alias_column", nil).
		Set("invalidated_at", nil).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": colName, "state": statusActive})

	if registryTable == metastore.DimRegistryTable {
		update = update.Set("dim_key", "")
	} else {
		update = update.Set("agg_key", "").Set("agg_value", nil)
	}

	revertQ, revertArgs, _ := update.ToSql()
	if _, err := cr.db.ExecContext(ctx, revertQ, revertArgs...); err != nil {
		cr.log.Error("failed to revert claimed slot after ALTER failure",
			zap.String("column", colName), zap.Error(err))
	}
}

// ResolveOrAllocateSketches resolves logical sketch keys to SET member names
// (s01..s64). Unlike dims/aggs, sketch slots are pre-allocated in the table
// schema — no ALTER TABLE is needed. Resolution claims AVAILABLE rows in
// _sketch_registry via UPDATE.
//
// Returns a map from sketch key (e.g. "uuid") to SET member name (e.g. "s03").
func (cr *ColumnRegistry) ResolveOrAllocateSketches(ctx context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	var unresolved []string

	// Fast path: check cache.
	cr.mu.RLock()
	for _, key := range keys {
		if e, ok := cr.sketchByKey[key]; ok {
			result[key] = e.SketchName
		} else {
			unresolved = append(unresolved, key)
		}
	}
	cr.mu.RUnlock()

	if len(unresolved) == 0 {
		return result, nil
	}

	// Slow path: claim AVAILABLE slots.
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	// Double-check after acquiring lock.
	var stillUnresolved []string
	cr.mu.RLock()
	for _, key := range unresolved {
		if e, ok := cr.sketchByKey[key]; ok {
			result[key] = e.SketchName
		} else {
			stillUnresolved = append(stillUnresolved, key)
		}
	}
	cr.mu.RUnlock()

	if len(stillUnresolved) == 0 {
		return result, nil
	}

	now := time.Now().UnixNano()
	for _, key := range stillUnresolved {
		sketchName, err := cr.claimSketchSlot(ctx, key, now)
		if err != nil {
			return nil, err
		}

		entry := &SketchRegistryEntry{
			TableName:  cr.tableName,
			SketchName: sketchName,
			SketchKey:  key,
			Status:     statusActive,
		}
		cr.mu.Lock()
		cr.sketchByKey[key] = entry
		cr.mu.Unlock()

		result[key] = sketchName
		cr.log.Info("claimed sketch slot", zap.String("sketchKey", key), zap.String("slot", sketchName))
	}

	return result, nil
}

// claimSketchSlot claims the first AVAILABLE sketch slot in a transaction,
// sets it to ACTIVE with the given key, and returns the slot name.
func (cr *ColumnRegistry) claimSketchSlot(ctx context.Context, key string, now int64) (string, error) {
	tx, err := cr.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("claim sketch slot: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	// Claim the first AVAILABLE slot.
	claimQuery := fmt.Sprintf(
		"UPDATE %s SET sketch_key = ?, state = ?, created_at = ? WHERE table_name = ? AND state = ? ORDER BY sketch_name LIMIT 1",
		metastore.SketchRegistryTable,
	)
	res, err := tx.ExecContext(ctx, claimQuery, key, statusActive, now, cr.tableName, statusAvailable)
	if err != nil {
		return "", fmt.Errorf("claim sketch slot for %q: %w", key, err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return "", ErrSlotExhausted
	}

	// Read back the claimed slot name within the same transaction.
	var sketchName string
	readQuery, readArgs, _ := sq.Select("sketch_name").
		From(metastore.SketchRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "sketch_key": key, "state": statusActive}).
		Limit(1).
		ToSql()
	if err := tx.QueryRowContext(ctx, readQuery, readArgs...).Scan(&sketchName); err != nil {
		return "", fmt.Errorf("read claimed sketch slot for %q: %w", key, err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("claim sketch slot: commit: %w", err)
	}
	return sketchName, nil
}
