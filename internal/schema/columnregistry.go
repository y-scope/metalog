package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
)

// statusActive is the registry status for columns available for use.
const statusActive = "ACTIVE"

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
	)
	return cr, nil
}

func (cr *ColumnRegistry) loadActiveEntries(ctx context.Context) error {
	// Load dims
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
		slot := parseSlotNumber(e.ColumnName, metastore.DimColumnPrefix)
		if slot >= cr.nextDimSlot {
			cr.nextDimSlot = slot + 1
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Load aggs
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
		slot := parseSlotNumber(e.ColumnName, metastore.AggColumnPrefix)
		if slot >= cr.nextAggSlot {
			cr.nextAggSlot = slot + 1
		}
	}
	return rows2.Err()
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

	// Double-check under lock
	cr.mu.RLock()
	current := cr.dimByKey[entry.DimKey]
	cr.mu.RUnlock()
	if current != nil && newWidth <= current.Width {
		return current.ColumnName, nil
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

	// Update registry
	updateQuery, updateArgs, _ := sq.Update(metastore.DimRegistryTable).
		Set("width", newWidth).
		Where(sq.Eq{"table_name": cr.tableName, "column_name": entry.ColumnName}).
		ToSql()
	if _, err = cr.db.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return "", fmt.Errorf("update dim width registry: %w", err)
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

	if cr.nextDimSlot+len(pending)-1 > 99 {
		return nil, fmt.Errorf("dim slot exhausted: need %d slots, have %d remaining",
			len(pending), 100-cr.nextDimSlot)
	}

	// Assign slot names and SQL types.
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

	// Advance slot counter immediately after ALTER succeeds so that any
	// retry (if a registry INSERT below fails) will generate fresh slot names
	// rather than colliding with the physical columns we just added.
	cr.nextDimSlot += len(slots)

	// Insert registry rows and update cache individually.
	now := time.Now().UnixNano()
	for _, s := range slots {
		insertQuery, insertArgs, _ := sq.Insert(metastore.DimRegistryTable).
			Columns("table_name", "column_name", "base_type", "width", "dim_key", "alias_column", "state", "created_at").
			Values(cr.tableName, s.colName, s.req.BaseType, s.req.Width, s.req.DimKey, nullIfEmpty(s.req.AliasCol), statusActive, now).
			ToSql()
		if _, err := cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
			return nil, fmt.Errorf("insert dim registry for %s: %w", s.req.DimKey, err)
		}

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

	// Advance slot counter immediately after ALTER succeeds (same rationale as dims).
	cr.nextAggSlot += len(slots)

	now := time.Now().UnixNano()
	for _, s := range slots {
		insertQuery, insertArgs, _ := sq.Insert(metastore.AggRegistryTable).
			Columns("table_name", "column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column", "state", "created_at").
			Values(cr.tableName, s.colName, s.req.AggKey, nullIfEmpty(s.req.AggValue), s.req.AggType, s.req.ValueType, nullIfEmpty(s.req.AliasCol), statusActive, now).
			ToSql()
		if _, err := cr.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
			return nil, fmt.Errorf("insert agg registry for %s: %w", s.req.AggKey, err)
		}

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
	cr.mu.Lock()
	defer cr.mu.Unlock()

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
