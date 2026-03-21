package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/metastore"
)

// Registry column states.
const (
	statusActive      = "ACTIVE"
	statusInvalidated = "INVALIDATED"
	statusAvailable   = "AVAILABLE"
)

// recyclerScanInterval is how often the background recycler checks for reclaimable slots.
const recyclerScanInterval = time.Hour

// recyclerMinAge is the minimum time a column must be INVALIDATED before recycling.
// This should exceed the table's retention period so most data has been naturally purged.
const recyclerMinAge = 30 * 24 * time.Hour

// recyclerMaxNonNullRows is the maximum number of non-NULL rows allowed for a
// column to be eligible for recycling. If the column has more rows than this,
// the recycler skips it and retries on the next cycle — retention will continue
// dropping partitions until the count falls below the threshold.
const recyclerMaxNonNullRows = 10000

// DimRegistryEntry represents an active dimension column mapping.
type DimRegistryEntry struct {
	TableName  string
	ColumnName string
	BaseType   string // str, str_utf8, bool, int, float
	Width      int
	DimKey     string
	AliasCol   string
	Status     string
}

// AggRegistryEntry represents an active aggregation column mapping.
type AggRegistryEntry struct {
	TableName       string
	ColumnName      string
	AggKey          string
	AggValue        string
	AggregationType string // EQ, GTE, GT, LTE, LT, SUM, AVG, MIN, MAX
	ValueType       string // INT, FLOAT
	AliasCol        string
	Status          string
}

// SketchRegistryEntry represents an active sketch SET member mapping.
type SketchRegistryEntry struct {
	TableName  string
	SketchName string // SET member name (e.g. "s01")
	SketchKey  string // logical field name (e.g. "uuid")
	Status     string
}

// ColumnRegistry maps physical placeholder columns (dim_fNN, agg_fNN) to field metadata.
// Thread-safe: RWMutex guards map reads/writes; a separate Mutex serializes slot allocation.
//
// SQL safety: DDL and DML in this type use fmt.Sprintf with interpolated identifiers.
// This is safe because all table/column names are validated via [db.ValidateSQLIdentifier]
// (restricted to ^[a-z_][a-z0-9_]{0,63}$) and quoted via [db.QuoteIdentifier] before
// interpolation. SQL types come from internal [dimSQLType] / hardcoded strings, never
// from user input.
type ColumnRegistry struct {
	db        *sql.DB
	tableName string
	isMariaDB bool
	log       *zap.Logger

	mu sync.RWMutex

	// dimKey -> DimRegistryEntry (ACTIVE entries only)
	dimByKey map[string]*DimRegistryEntry
	// columnName -> DimRegistryEntry
	dimByColumn map[string]*DimRegistryEntry

	// compositeKey -> AggRegistryEntry
	aggByKey map[string]*AggRegistryEntry
	// columnName -> AggRegistryEntry
	aggByColumn map[string]*AggRegistryEntry

	// sketchKey -> SketchRegistryEntry (ACTIVE entries only)
	sketchByKey map[string]*SketchRegistryEntry

	nextDimSlot int
	nextAggSlot int

	allocMu sync.Mutex // serializes slot allocation
}

// NewColumnRegistry creates a ColumnRegistry and loads all ACTIVE entries from the DB.
func NewColumnRegistry(ctx context.Context, db *sql.DB, tableName string, isMariaDB bool, log *zap.Logger) (*ColumnRegistry, error) {
	cr := &ColumnRegistry{
		db:          db,
		tableName:   tableName,
		isMariaDB:   isMariaDB,
		log:         log,
		dimByKey:    make(map[string]*DimRegistryEntry),
		dimByColumn: make(map[string]*DimRegistryEntry),
		aggByKey:    make(map[string]*AggRegistryEntry),
		aggByColumn: make(map[string]*AggRegistryEntry),
		sketchByKey: make(map[string]*SketchRegistryEntry),
		nextDimSlot: 1,
		nextAggSlot: 1,
	}
	if err := cr.loadActiveEntries(ctx); err != nil {
		return nil, err
	}
	log.Info("column registry loaded",
		zap.String("table", tableName),
		zap.Int("dims", len(cr.dimByKey)),
		zap.Int("aggs", len(cr.aggByKey)),
		zap.Int("sketches", len(cr.sketchByKey)),
	)
	return cr, nil
}

func (cr *ColumnRegistry) loadActiveEntries(ctx context.Context) error {
	// Compute nextDimSlot from ALL registry entries (ACTIVE + INVALIDATED + AVAILABLE)
	// since physical columns exist for all states.
	if err := cr.loadSlotHighWaterMarks(ctx); err != nil {
		return err
	}

	// Load ACTIVE dims into in-memory maps.
	dimQuery, dimArgs, _ := sq.Select("column_name", "base_type", "width", "dim_key", "alias_column").
		From(metastore.DimRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "state": statusActive}).
		ToSql()
	rows, err := cr.db.QueryContext(ctx, dimQuery, dimArgs...)
	if err != nil {
		return fmt.Errorf("load dim registry: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		e := &DimRegistryEntry{TableName: cr.tableName, Status: statusActive}
		var aliasCol sql.NullString
		var width sql.NullInt32
		if err := rows.Scan(&e.ColumnName, &e.BaseType, &width, &e.DimKey, &aliasCol); err != nil {
			return err
		}
		if width.Valid {
			e.Width = int(width.Int32)
		}
		if aliasCol.Valid {
			e.AliasCol = aliasCol.String
		}
		cr.dimByKey[e.DimKey] = e
		cr.dimByColumn[e.ColumnName] = e
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Load ACTIVE aggs into in-memory maps.
	aggQuery, aggArgs, _ := sq.Select("column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column").
		From(metastore.AggRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "state": statusActive}).
		ToSql()
	rows2, err := cr.db.QueryContext(ctx, aggQuery, aggArgs...)
	if err != nil {
		return fmt.Errorf("load agg registry: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		e := &AggRegistryEntry{TableName: cr.tableName, Status: statusActive}
		var aggValue, aliasCol sql.NullString
		if err := rows2.Scan(&e.ColumnName, &e.AggKey, &aggValue, &e.AggregationType, &e.ValueType, &aliasCol); err != nil {
			return err
		}
		if aggValue.Valid {
			e.AggValue = aggValue.String
		}
		if aliasCol.Valid {
			e.AliasCol = aliasCol.String
		}
		key := AggCacheKey(e.AggKey, e.AggValue, e.AggregationType)
		cr.aggByKey[key] = e
		cr.aggByColumn[e.ColumnName] = e
	}
	if err := rows2.Err(); err != nil {
		return err
	}

	// Load ACTIVE sketches into in-memory map.
	sketchQuery, sketchArgs, _ := sq.Select("sketch_name", "sketch_key").
		From(metastore.SketchRegistryTable).
		Where(sq.Eq{"table_name": cr.tableName, "state": statusActive}).
		ToSql()
	rows3, err := cr.db.QueryContext(ctx, sketchQuery, sketchArgs...)
	if err != nil {
		return fmt.Errorf("load sketch registry: %w", err)
	}
	defer rows3.Close()

	for rows3.Next() {
		e := &SketchRegistryEntry{TableName: cr.tableName, Status: statusActive}
		if err := rows3.Scan(&e.SketchName, &e.SketchKey); err != nil {
			return err
		}
		cr.sketchByKey[e.SketchKey] = e
	}
	return rows3.Err()
}

// loadSlotHighWaterMarks queries all registry entries (any state) to find the
// highest allocated slot number. This prevents new allocations from colliding
// with INVALIDATED or AVAILABLE slots that still have physical columns.
func (cr *ColumnRegistry) loadSlotHighWaterMarks(ctx context.Context) error {
	for _, cfg := range []struct {
		table  string
		prefix string
		target *int
	}{
		{metastore.DimRegistryTable, metastore.DimColumnPrefix, &cr.nextDimSlot},
		{metastore.AggRegistryTable, metastore.AggColumnPrefix, &cr.nextAggSlot},
	} {
		q, args, _ := sq.Select("column_name").
			From(cfg.table).
			Where(sq.Eq{"table_name": cr.tableName}).
			ToSql()
		rows, err := cr.db.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("load slot high water marks (%s): %w", cfg.table, err)
		}
		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				rows.Close()
				return err
			}
			slot := parseSlotNumber(colName, cfg.prefix)
			if slot >= *cfg.target {
				*cfg.target = slot + 1
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

// ResolveDim returns the column name for a dimension key, or empty string if not found.
func (cr *ColumnRegistry) ResolveDim(dimKey string) string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	if e, ok := cr.dimByKey[dimKey]; ok {
		return e.ColumnName
	}
	return ""
}

// ResolveAgg returns the column name for an aggregation, or empty string if not found.
func (cr *ColumnRegistry) ResolveAgg(aggKey, aggValue, aggType string) string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	key := AggCacheKey(aggKey, aggValue, aggType)
	if e, ok := cr.aggByKey[key]; ok {
		return e.ColumnName
	}
	return ""
}

// ResolveSketch returns the registry entry for a sketch key, or nil if not found.
func (cr *ColumnRegistry) ResolveSketch(sketchKey string) *SketchRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.sketchByKey[sketchKey]
}

// ResolveOrAllocateDim resolves an existing dim mapping or allocates a new slot.
// If a new slot is needed, it inserts into the registry and issues ALTER TABLE ADD COLUMN.
// If the existing slot is narrower than width, the column is widened via ALTER TABLE MODIFY.
//
// Uses a fast-path/slow-path pattern: the fast path (RLock) handles the common
// case where the dim already exists. The slow path (allocMu) serializes DDL
// operations and double-checks under the lock to handle concurrent allocations.
func (cr *ColumnRegistry) ResolveOrAllocateDim(ctx context.Context, dimKey, baseType string, width int) (string, error) {
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

	// Prevent crossing the 255→256 boundary: InnoDB changes the VARCHAR
	// length prefix from 1 byte to 2 bytes, forcing a full table rebuild.
	if entry.Width <= defaultVarcharWidth && newWidth > defaultVarcharWidth {
		cr.log.Warn("capping dim width at 255 to avoid full table rebuild",
			zap.String("column", entry.ColumnName),
			zap.String("dimKey", entry.DimKey),
			zap.Int("requestedWidth", newWidth),
		)
		newWidth = defaultVarcharWidth
		if newWidth <= entry.Width {
			return entry.ColumnName, nil
		}
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

	// Try to claim an AVAILABLE (recycled) slot before allocating a fresh one.
	if col, err := cr.claimAvailableDimSlot(ctx, dimKey, baseType, width); err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	// Slot numbers above 99 would produce 3-digit names (dim_f100) breaking the
	// %02d zero-padding convention.
	if cr.nextDimSlot > 99 {
		return "", fmt.Errorf("dim slot exhausted: slot %d exceeds maximum 99", cr.nextDimSlot)
	}
	colName := fmt.Sprintf("%s%02d", metastore.DimColumnPrefix, cr.nextDimSlot)

	// Determine SQL type
	sqlType := dimSQLType(baseType, width)

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
			db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), sqlType, lockMode(cr.isMariaDB)))
	if err != nil {
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

	// Try to claim an AVAILABLE (recycled) slot before allocating a fresh one.
	if col, err := cr.claimAvailableAggSlot(ctx, aggKey, aggValue, aggType, valueType); err != nil {
		return "", err
	} else if col != "" {
		return col, nil
	}

	if cr.nextAggSlot > 99 {
		return "", fmt.Errorf("agg slot exhausted: slot %d exceeds maximum 99", cr.nextAggSlot)
	}
	colName := fmt.Sprintf("%s%02d", metastore.AggColumnPrefix, cr.nextAggSlot)

	sqlType := "BIGINT"
	if valueType == "FLOAT" {
		sqlType = "DOUBLE"
	}

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL, ALGORITHM=INPLACE, LOCK=%s",
			db.QuoteIdentifier(cr.tableName), db.QuoteIdentifier(colName), sqlType, lockMode(cr.isMariaDB)))
	if err != nil {
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

// DimRequest describes a dimension column to resolve or allocate.
type DimRequest struct {
	DimKey   string
	BaseType string
	Width    int
	AliasCol string // optional human-readable alias
}

// AggRequest describes an aggregation column to resolve or allocate.
type AggRequest struct {
	AggKey    string
	AggValue  string
	AggType   string
	ValueType string
	AliasCol  string // optional human-readable alias
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
		req   DimRequest
		entry *DimRegistryEntry
	}
	var needsWidening []wideningCase

	cr.mu.RLock()
	for _, r := range reqs {
		if e, ok := cr.dimByKey[r.DimKey]; ok {
			result[r.DimKey] = e.ColumnName
			if r.Width > e.Width && (r.BaseType == "str" || r.BaseType == "str_utf8") {
				needsWidening = append(needsWidening, wideningCase{r, e})
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

	// Double-check: filter out any allocated while we waited for allocMu.
	var pending []DimRequest
	result := make(map[string]string)

	cr.mu.RLock()
	for _, r := range reqs {
		if e, ok := cr.dimByKey[r.DimKey]; ok {
			result[r.DimKey] = e.ColumnName
		} else {
			pending = append(pending, r)
		}
	}
	cr.mu.RUnlock()

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

	if cr.nextDimSlot+len(pending)-1 > 99 {
		return nil, fmt.Errorf("dim slot exhausted: need %d slots, have %d remaining",
			len(pending), 100-cr.nextDimSlot)
	}

	// Assign slot names and SQL types for fresh allocations.
	type pendingSlot struct {
		req     DimRequest
		colName string
		sqlType string
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

	var pending []AggRequest
	result := make(map[string]string)

	cr.mu.RLock()
	for _, r := range reqs {
		key := AggCacheKey(r.AggKey, r.AggValue, r.AggType)
		if e, ok := cr.aggByKey[key]; ok {
			result[key] = e.ColumnName
		} else {
			pending = append(pending, r)
		}
	}
	cr.mu.RUnlock()

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

	if cr.nextAggSlot+len(pending)-1 > 99 {
		return nil, fmt.Errorf("agg slot exhausted: need %d slots, have %d remaining",
			len(pending), 100-cr.nextAggSlot)
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
	defer tx.Rollback()

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
		return "", fmt.Errorf("sketch slots exhausted: no AVAILABLE slots for key %q", key)
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

// ActiveDimColumns returns the column names of all active dimension entries.
// The result is sorted for deterministic SQL generation.
func (cr *ColumnRegistry) ActiveDimColumns() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	cols := make([]string, 0, len(cr.dimByColumn))
	for col := range cr.dimByColumn {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

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
	defer dimRows.Close()

	dimAliases := make(map[string]string) // column_name -> alias
	for dimRows.Next() {
		var colName, alias string
		if err := dimRows.Scan(&colName, &alias); err != nil {
			return err
		}
		dimAliases[colName] = alias
	}
	if err := dimRows.Err(); err != nil {
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
	defer aggRows.Close()

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

// LookupDimByColumn returns the DimRegistryEntry for a physical column name, or nil.
func (cr *ColumnRegistry) LookupDimByColumn(colName string) *DimRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.dimByColumn[colName]
}

// LookupAggByColumn returns the AggRegistryEntry for a physical column name, or nil.
func (cr *ColumnRegistry) LookupAggByColumn(colName string) *AggRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.aggByColumn[colName]
}

// ActiveAggColumns returns the column names of all active aggregation entries.
// The result is sorted for deterministic SQL generation.
func (cr *ColumnRegistry) ActiveAggColumns() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	cols := make([]string, 0, len(cr.aggByColumn))
	for col := range cr.aggByColumn {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

// AllDimEntries returns a snapshot of all active dimension registry entries.
func (cr *ColumnRegistry) AllDimEntries() []*DimRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	entries := make([]*DimRegistryEntry, 0, len(cr.dimByKey))
	for _, e := range cr.dimByKey {
		entries = append(entries, e)
	}
	return entries
}

// AllAggEntries returns a snapshot of all active aggregation registry entries.
func (cr *ColumnRegistry) AllAggEntries() []*AggRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	entries := make([]*AggRegistryEntry, 0, len(cr.aggByKey))
	for _, e := range cr.aggByKey {
		entries = append(entries, e)
	}
	return entries
}

// EntryCount returns the total number of active dim + agg entries.
// Used as a cache-busting version token: when new columns are provisioned,
// the count changes and cached filter rewrites are invalidated.
func (cr *ColumnRegistry) EntryCount() int {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return len(cr.dimByKey) + len(cr.aggByKey)
}

// FloatAggColumns returns a set of agg column names that hold DOUBLE values.
func (cr *ColumnRegistry) FloatAggColumns() map[string]bool {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	result := make(map[string]bool)
	for _, e := range cr.aggByColumn {
		if e.ValueType == "FLOAT" {
			result[e.ColumnName] = true
		}
	}
	return result
}

// RegistrySnapshot is an immutable point-in-time copy of a ColumnRegistry.
// Used by the query path to avoid lock contention with the ingestion path.
type RegistrySnapshot struct {
	dimByKey    map[string]*DimRegistryEntry
	dimByColumn map[string]*DimRegistryEntry
	aggByKey    map[string]*AggRegistryEntry
	aggByColumn map[string]*AggRegistryEntry
}

// Snapshot creates a read-only snapshot of the current registry state.
// The snapshot is safe to use concurrently without locks.
func (cr *ColumnRegistry) Snapshot() *RegistrySnapshot {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	snap := &RegistrySnapshot{
		dimByKey:    make(map[string]*DimRegistryEntry, len(cr.dimByKey)),
		dimByColumn: make(map[string]*DimRegistryEntry, len(cr.dimByColumn)),
		aggByKey:    make(map[string]*AggRegistryEntry, len(cr.aggByKey)),
		aggByColumn: make(map[string]*AggRegistryEntry, len(cr.aggByColumn)),
	}
	for k, v := range cr.dimByKey {
		cp := *v
		snap.dimByKey[k] = &cp
	}
	for k, v := range cr.dimByColumn {
		cp := *v
		snap.dimByColumn[k] = &cp
	}
	for k, v := range cr.aggByKey {
		cp := *v
		snap.aggByKey[k] = &cp
	}
	for k, v := range cr.aggByColumn {
		cp := *v
		snap.aggByColumn[k] = &cp
	}
	return snap
}

// ResolveDim returns the column name for a dimension key, or empty string if not found.
func (r *RegistrySnapshot) ResolveDim(dimKey string) string {
	if e, ok := r.dimByKey[dimKey]; ok {
		return e.ColumnName
	}
	return ""
}

// ResolveAgg returns the column name for an aggregation, or empty string if not found.
func (r *RegistrySnapshot) ResolveAgg(aggKey, aggValue, aggType string) string {
	key := AggCacheKey(aggKey, aggValue, aggType)
	if e, ok := r.aggByKey[key]; ok {
		return e.ColumnName
	}
	return ""
}

// AllDimEntries returns all dim entries in the snapshot.
func (r *RegistrySnapshot) AllDimEntries() []*DimRegistryEntry {
	entries := make([]*DimRegistryEntry, 0, len(r.dimByKey))
	for _, e := range r.dimByKey {
		entries = append(entries, e)
	}
	return entries
}

// AllAggEntries returns all agg entries in the snapshot.
func (r *RegistrySnapshot) AllAggEntries() []*AggRegistryEntry {
	entries := make([]*AggRegistryEntry, 0, len(r.aggByKey))
	for _, e := range r.aggByKey {
		entries = append(entries, e)
	}
	return entries
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
	defer tx.Rollback()

	var colName, oldBaseType string
	var oldWidth sql.NullInt32
	row := tx.QueryRowContext(ctx,
		"SELECT column_name, base_type, width FROM "+metastore.DimRegistryTable+
			" WHERE table_name = ? AND state = ? ORDER BY column_name LIMIT 1 FOR UPDATE SKIP LOCKED",
		cr.tableName, statusAvailable)
	if err := row.Scan(&colName, &oldBaseType, &oldWidth); err != nil {
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
	defer tx.Rollback()

	var colName, oldValueType string
	row := tx.QueryRowContext(ctx,
		"SELECT column_name, value_type FROM "+metastore.AggRegistryTable+
			" WHERE table_name = ? AND state = ? ORDER BY column_name LIMIT 1 FOR UPDATE SKIP LOCKED",
		cr.tableName, statusAvailable)
	if err := row.Scan(&colName, &oldValueType); err != nil {
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
	defer rows.Close()

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
			cr.log.Info("recycler: cleared column data batch",
				zap.String("column", colName), zap.Int64("rows", affected))
		}
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

// AggCacheKey builds the composite key for agg cache lookups.
func AggCacheKey(aggKey, aggValue, aggType string) string {
	return aggType + "\x00" + aggKey + "\x00" + aggValue
}

// defaultVarcharWidth is the default VARCHAR width when not specified.
// 255 is the largest width that fits in a single-byte InnoDB length prefix.
const defaultVarcharWidth = 255

func dimSQLType(baseType string, width int) string {
	if width <= 0 {
		width = defaultVarcharWidth
	}
	switch baseType {
	case "str":
		return fmt.Sprintf("VARCHAR(%d) CHARACTER SET ascii COLLATE ascii_bin", width)
	case "str_utf8":
		return fmt.Sprintf("VARCHAR(%d)", width)
	case "int":
		return "BIGINT"
	case "bool":
		return "BOOLEAN"
	case "float":
		return "DOUBLE"
	default:
		return fmt.Sprintf("VARCHAR(%d)", width)
	}
}

func parseSlotNumber(colName, prefix string) int {
	if len(colName) <= len(prefix) {
		return 0
	}
	n := 0
	for _, c := range colName[len(prefix):] {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}

// isDuplicateColumn checks if an error is a "duplicate column name" DDL error (MySQL error 1060).
func isDuplicateColumn(err error) bool {
	return db.IsDuplicateColumn(err)
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
