// Package schema manages the physical database schema for metadata tables.
//
// It provisions tables from a template ([EnsureTable]), manages RANGE partitions
// by timestamp ([PartitionManager]), maintains secondary indexes ([IndexManager]),
// and tracks dynamic column allocation via the [ColumnRegistry].
//
// The column registry maps user-facing field names to physical dim_fNN/agg_fNN
// columns, supporting thread-safe slot allocation and atomic snapshots for
// concurrent readers.
package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/db"
	"github.com/y-scope/metalog/logutil"
	"github.com/y-scope/metalog/metastore"
)

// Registry column states.
const (
	statusActive      = "ACTIVE"
	statusInvalidated = "INVALIDATED"
	statusAvailable   = "AVAILABLE"
)

// ErrSlotExhausted is returned when all 99 dim or agg slots are consumed.
// Callers should log a warning and skip the dimension/aggregation — ingestion
// continues without the exhausted columns rather than stalling.
var ErrSlotExhausted = errors.New("column slot exhausted (max 99)")

// defaultVarcharWidth is the default VARCHAR width when not specified.
const defaultVarcharWidth = 256

// minVarcharWidth is the minimum VARCHAR width for new dim columns.
// 256 avoids ever needing to widen columns (which requires ALTER TABLE).
// MySQL 8.0+ temptable engine uses variable-length storage, so the
// overhead of a wider declared width is negligible.
const minVarcharWidth = 256

// DimRegistryEntry represents an active dimension column mapping.
type DimRegistryEntry struct {
	TableName  string
	ColumnName string
	BaseType   string
	DimKey     string
	AliasCol   string
	Status     string
	Width      int
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
	tableAttr       attribute.KeyValue
	mSlotsExhausted metric.Int64Counter
	dimByColumn     map[string]*DimRegistryEntry
	log             *zap.Logger
	db              *sql.DB
	dimByKey        map[string]*DimRegistryEntry
	exhaustionFL    *logutil.FailureLogger
	aggByKey        map[string]*AggRegistryEntry
	aggByColumn     map[string]*AggRegistryEntry
	sketchByKey     map[string]*SketchRegistryEntry
	tableName       string
	nextDimSlot     int
	nextAggSlot     int
	mu              sync.RWMutex
	allocMu         sync.Mutex
	isMariaDB       bool
}

// DimRequest describes a dimension column to resolve or allocate.
type DimRequest struct {
	DimKey   string
	BaseType string
	AliasCol string
	Width    int
}

// AggRequest describes an aggregation column to resolve or allocate.
type AggRequest struct {
	AggKey    string
	AggValue  string
	AggType   string
	ValueType string
	AliasCol  string // optional human-readable alias
}

// RegistrySnapshot is an immutable point-in-time copy of a ColumnRegistry.
// Used by the query path to avoid lock contention with the ingestion path.
type RegistrySnapshot struct {
	dimByKey    map[string]*DimRegistryEntry
	dimByColumn map[string]*DimRegistryEntry
	aggByKey    map[string]*AggRegistryEntry
	aggByColumn map[string]*AggRegistryEntry
}

// NewColumnRegistry creates a ColumnRegistry and loads all ACTIVE entries from the DB.
func NewColumnRegistry(ctx context.Context, db *sql.DB, tableName string, isMariaDB bool, log *zap.Logger) (*ColumnRegistry, error) {
	cr := &ColumnRegistry{
		db:           db,
		tableName:    tableName,
		isMariaDB:    isMariaDB,
		log:          log,
		dimByKey:     make(map[string]*DimRegistryEntry),
		dimByColumn:  make(map[string]*DimRegistryEntry),
		aggByKey:     make(map[string]*AggRegistryEntry),
		aggByColumn:  make(map[string]*AggRegistryEntry),
		sketchByKey:  make(map[string]*SketchRegistryEntry),
		nextDimSlot:  1,
		nextAggSlot:  1,
		tableAttr:    attribute.String("table", tableName),
		exhaustionFL: logutil.NewFailureLogger(log, time.Minute),
	}
	cr.initMetrics(noop.Meter{})
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

// SetMeter configures OpenTelemetry metrics. Must be called before serving.
func (cr *ColumnRegistry) SetMeter(m metric.Meter) { cr.initMetrics(m) }

func (cr *ColumnRegistry) initMetrics(m metric.Meter) {
	cr.mSlotsExhausted, _ = m.Int64Counter("metalog.schema.slots_exhausted",
		metric.WithDescription("Dimension/aggregation columns dropped due to slot exhaustion"),
		metric.WithUnit("{column}"))
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
	defer rows.Close() //nolint:errcheck // cleanup

	for rows.Next() {
		e := &DimRegistryEntry{TableName: cr.tableName, Status: statusActive}
		var aliasCol sql.NullString
		var width sql.NullInt32
		err = rows.Scan(&e.ColumnName, &e.BaseType, &width, &e.DimKey, &aliasCol)
		if err != nil {
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
	err = rows.Err()
	if err != nil {
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
	defer rows2.Close() //nolint:errcheck // cleanup

	for rows2.Next() {
		e := &AggRegistryEntry{TableName: cr.tableName, Status: statusActive}
		var aggValue, aliasCol sql.NullString
		err = rows2.Scan(&e.ColumnName, &e.AggKey, &aggValue, &e.AggregationType, &e.ValueType, &aliasCol)
		if err != nil {
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
	err = rows2.Err()
	if err != nil {
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
	defer rows3.Close() //nolint:errcheck // cleanup

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
		target *int
		table  string
		prefix string
	}{
		{target: &cr.nextDimSlot, table: metastore.DimRegistryTable, prefix: metastore.DimColumnPrefix},
		{target: &cr.nextAggSlot, table: metastore.AggRegistryTable, prefix: metastore.AggColumnPrefix},
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
				_ = rows.Close()
				return err
			}
			slot := parseSlotNumber(colName, cfg.prefix)
			if slot >= *cfg.target {
				*cfg.target = slot + 1
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()
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

// validDimBaseTypes is the set of recognized dimension base types.
var validDimBaseTypes = map[string]bool{
	"str": true, "str_utf8": true, "int": true, "bool": true, "float": true,
}

// ValidateDimBaseType returns an error if baseType is not a recognized dimension type.
func ValidateDimBaseType(baseType string) error {
	if !validDimBaseTypes[baseType] {
		return fmt.Errorf("unknown dimension base type: %q", baseType)
	}
	return nil
}

func dimSQLType(baseType string, width int) string {
	if width <= 0 {
		width = defaultVarcharWidth
	}
	switch baseType {
	case "str":
		if width < minVarcharWidth {
			width = minVarcharWidth
		}
		return fmt.Sprintf("VARCHAR(%d) CHARACTER SET ascii COLLATE ascii_bin", width)
	case "str_utf8":
		if width < minVarcharWidth {
			width = minVarcharWidth
		}
		return fmt.Sprintf("VARCHAR(%d)", width)
	case "int":
		return "BIGINT"
	case "bool":
		return "BOOLEAN"
	case "float":
		return "DOUBLE"
	default:
		// Should never reach here if ValidateDimBaseType was called at entry.
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
