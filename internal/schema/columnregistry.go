package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
)

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

// ColumnRegistry maps opaque placeholder columns (dim_f01, agg_f01) to field metadata.
// Thread-safe via sync.RWMutex for reads and sync.Mutex for allocation.
type ColumnRegistry struct {
	db        *sql.DB
	tableName string
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
func NewColumnRegistry(ctx context.Context, db *sql.DB, tableName string, log *zap.Logger) (*ColumnRegistry, error) {
	cr := &ColumnRegistry{
		db:          db,
		tableName:   tableName,
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
	rows, err := cr.db.QueryContext(ctx,
		"SELECT column_name, base_type, width, dim_key, alias_column FROM "+metastore.DimRegistryTable+
			" WHERE table_name = ? AND state = 'ACTIVE'", cr.tableName)
	if err != nil {
		return fmt.Errorf("load dim registry: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		e := &DimRegistryEntry{TableName: cr.tableName, Status: "ACTIVE"}
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
	rows2, err := cr.db.QueryContext(ctx,
		"SELECT column_name, agg_key, agg_value, aggregation_type, value_type, alias_column FROM "+metastore.AggRegistryTable+
			" WHERE table_name = ? AND state = 'ACTIVE'", cr.tableName)
	if err != nil {
		return fmt.Errorf("load agg registry: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		e := &AggRegistryEntry{TableName: cr.tableName, Status: "ACTIVE"}
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
func (cr *ColumnRegistry) ResolveOrAllocateDim(ctx context.Context, dimKey, baseType string, width int) (string, error) {
	cr.mu.RLock()
	if e, ok := cr.dimByKey[dimKey]; ok {
		cr.mu.RUnlock()
		// Check if width expansion is needed (only for string types)
		if width > e.Width && (baseType == "str" || baseType == "str_utf8") {
			return cr.expandDimWidth(ctx, e, width)
		}
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	return cr.allocateNewDimSlot(ctx, dimKey, baseType, width)
}

// expandDimWidth widens a VARCHAR dim column via ALTER TABLE MODIFY COLUMN.
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

	sqlType := dimSQLType(entry.BaseType, newWidth)

	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s NULL",
			db.QuoteIdentifier(cr.tableName), entry.ColumnName, sqlType))
	if err != nil {
		return "", fmt.Errorf("expand dim width: %w", err)
	}

	// Update registry
	_, err = cr.db.ExecContext(ctx,
		"UPDATE "+metastore.DimRegistryTable+" SET width = ? WHERE table_name = ? AND column_name = ?",
		newWidth, cr.tableName, entry.ColumnName)
	if err != nil {
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

	// Double-check after acquiring lock
	cr.mu.RLock()
	if e, ok := cr.dimByKey[dimKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	colName := fmt.Sprintf("%s%02d", metastore.DimColumnPrefix, cr.nextDimSlot)

	// Determine SQL type
	sqlType := dimSQLType(baseType, width)

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL",
			db.QuoteIdentifier(cr.tableName), colName, sqlType))
	if err != nil {
		// Column may already exist from a previous crashed attempt — check.
		if !isDuplicateColumn(err) {
			return "", fmt.Errorf("alter table add dim: %w", err)
		}
	}

	// INSERT into registry (safe now — the physical column exists)
	_, err = cr.db.ExecContext(ctx,
		"INSERT INTO "+metastore.DimRegistryTable+
			" (table_name, column_name, base_type, width, dim_key, state) VALUES (?, ?, ?, ?, ?, 'ACTIVE')",
		cr.tableName, colName, baseType, width, dimKey,
	)
	if err != nil {
		return "", fmt.Errorf("insert dim registry: %w", err)
	}

	cr.nextDimSlot++

	// Update cache
	entry := &DimRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		BaseType: baseType, Width: width, DimKey: dimKey, Status: "ACTIVE",
	}
	cr.mu.Lock()
	cr.dimByKey[dimKey] = entry
	cr.dimByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("allocated dim slot", zap.String("dimKey", dimKey), zap.String("column", colName))
	return colName, nil
}

// ResolveOrAllocateAgg resolves an existing agg mapping or allocates a new slot.
func (cr *ColumnRegistry) ResolveOrAllocateAgg(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, error) {
	cacheKey := AggCacheKey(aggKey, aggValue, aggType)

	cr.mu.RLock()
	if e, ok := cr.aggByKey[cacheKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	return cr.allocateNewAggSlot(ctx, aggKey, aggValue, aggType, valueType)
}

func (cr *ColumnRegistry) allocateNewAggSlot(ctx context.Context, aggKey, aggValue, aggType, valueType string) (string, error) {
	cr.allocMu.Lock()
	defer cr.allocMu.Unlock()

	cacheKey := AggCacheKey(aggKey, aggValue, aggType)
	cr.mu.RLock()
	if e, ok := cr.aggByKey[cacheKey]; ok {
		cr.mu.RUnlock()
		return e.ColumnName, nil
	}
	cr.mu.RUnlock()

	colName := fmt.Sprintf("%s%02d", metastore.AggColumnPrefix, cr.nextAggSlot)

	sqlType := "BIGINT"
	if valueType == "FLOAT" {
		sqlType = "DOUBLE"
	}

	// ALTER TABLE ADD COLUMN first — if it fails, no orphaned registry row is left.
	_, err := cr.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NOT NULL DEFAULT 0",
			db.QuoteIdentifier(cr.tableName), colName, sqlType))
	if err != nil {
		if !isDuplicateColumn(err) {
			return "", fmt.Errorf("alter table add agg: %w", err)
		}
	}

	// INSERT into registry (safe now — the physical column exists)
	_, err = cr.db.ExecContext(ctx,
		"INSERT INTO "+metastore.AggRegistryTable+
			" (table_name, column_name, agg_key, agg_value, aggregation_type, value_type, state) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')",
		cr.tableName, colName, aggKey, nullIfEmpty(aggValue), aggType, valueType,
	)
	if err != nil {
		return "", fmt.Errorf("insert agg registry: %w", err)
	}

	cr.nextAggSlot++

	entry := &AggRegistryEntry{
		TableName: cr.tableName, ColumnName: colName,
		AggKey: aggKey, AggValue: aggValue,
		AggregationType: aggType, ValueType: valueType, Status: "ACTIVE",
	}
	cr.mu.Lock()
	cr.aggByKey[cacheKey] = entry
	cr.aggByColumn[colName] = entry
	cr.mu.Unlock()

	cr.log.Info("allocated agg slot", zap.String("aggKey", aggKey), zap.String("column", colName))
	return colName, nil
}

// ActiveDimColumns returns the column names of all active dimension entries.
func (cr *ColumnRegistry) ActiveDimColumns() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	cols := make([]string, 0, len(cr.dimByColumn))
	for col := range cr.dimByColumn {
		cols = append(cols, col)
	}
	return cols
}

// ActiveAggColumns returns the column names of all active aggregation entries.
func (cr *ColumnRegistry) ActiveAggColumns() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	cols := make([]string, 0, len(cr.aggByColumn))
	for col := range cr.aggByColumn {
		cols = append(cols, col)
	}
	return cols
}

// AllDimEntries returns all active dim entries.
func (cr *ColumnRegistry) AllDimEntries() []*DimRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	entries := make([]*DimRegistryEntry, 0, len(cr.dimByKey))
	for _, e := range cr.dimByKey {
		entries = append(entries, e)
	}
	return entries
}

// AllAggEntries returns all active agg entries.
func (cr *ColumnRegistry) AllAggEntries() []*AggRegistryEntry {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	entries := make([]*AggRegistryEntry, 0, len(cr.aggByKey))
	for _, e := range cr.aggByKey {
		entries = append(entries, e)
	}
	return entries
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

// ColumnRegistryReader provides read-only access to column registry data.
// Used by the query path to avoid lock contention with the ingestion path.
type ColumnRegistryReader struct {
	dimByKey    map[string]*DimRegistryEntry
	dimByColumn map[string]*DimRegistryEntry
	aggByKey    map[string]*AggRegistryEntry
	aggByColumn map[string]*AggRegistryEntry
}

// Snapshot creates a read-only snapshot of the current registry state.
// The snapshot is safe to use concurrently without locks.
func (cr *ColumnRegistry) Snapshot() *ColumnRegistryReader {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	snap := &ColumnRegistryReader{
		dimByKey:    make(map[string]*DimRegistryEntry, len(cr.dimByKey)),
		dimByColumn: make(map[string]*DimRegistryEntry, len(cr.dimByColumn)),
		aggByKey:    make(map[string]*AggRegistryEntry, len(cr.aggByKey)),
		aggByColumn: make(map[string]*AggRegistryEntry, len(cr.aggByColumn)),
	}
	for k, v := range cr.dimByKey {
		snap.dimByKey[k] = v
	}
	for k, v := range cr.dimByColumn {
		snap.dimByColumn[k] = v
	}
	for k, v := range cr.aggByKey {
		snap.aggByKey[k] = v
	}
	for k, v := range cr.aggByColumn {
		snap.aggByColumn[k] = v
	}
	return snap
}

// ResolveDim returns the column name for a dimension key, or empty string if not found.
func (r *ColumnRegistryReader) ResolveDim(dimKey string) string {
	if e, ok := r.dimByKey[dimKey]; ok {
		return e.ColumnName
	}
	return ""
}

// ResolveAgg returns the column name for an aggregation, or empty string if not found.
func (r *ColumnRegistryReader) ResolveAgg(aggKey, aggValue, aggType string) string {
	key := AggCacheKey(aggKey, aggValue, aggType)
	if e, ok := r.aggByKey[key]; ok {
		return e.ColumnName
	}
	return ""
}

// AllDimEntries returns all dim entries in the snapshot.
func (r *ColumnRegistryReader) AllDimEntries() []*DimRegistryEntry {
	entries := make([]*DimRegistryEntry, 0, len(r.dimByKey))
	for _, e := range r.dimByKey {
		entries = append(entries, e)
	}
	return entries
}

// AllAggEntries returns all agg entries in the snapshot.
func (r *ColumnRegistryReader) AllAggEntries() []*AggRegistryEntry {
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
	return strings.Contains(err.Error(), "Duplicate column name") || strings.Contains(err.Error(), "1060")
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
