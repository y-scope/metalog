package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/schema"
)

// Sentinel errors for admin operations.
var (
	// ErrColumnNotFound is returned when no ACTIVE column matches the request.
	ErrColumnNotFound = errors.New("column not found")
	// ErrInvalidColumnPrefix is returned when column_name doesn't start with dim_f or agg_f.
	ErrInvalidColumnPrefix = errors.New("column name must start with dim_f or agg_f")
	// ErrInvalidAlias is returned when the alias value fails validation.
	ErrInvalidAlias = errors.New("invalid alias format")
)

// maxAliasLength is the maximum length of an alias column value.
const maxAliasLength = 128

// aliasPattern is the allowed pattern for alias values: alphanumeric, underscores, dots, hyphens, slashes.
var aliasPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_./-]*$`)

// TableRegistration handles registering new tables at runtime.
type TableRegistration struct {
	db                  *sql.DB
	isMariaDB           bool
	compressionOverride string
	log                 *zap.Logger
}

// NewTableRegistration creates a TableRegistration.
func NewTableRegistration(db *sql.DB, isMariaDB bool, compressionOverride string, log *zap.Logger) *TableRegistration {
	return &TableRegistration{db: db, isMariaDB: isMariaDB, compressionOverride: compressionOverride, log: log}
}

// RegisterTableOpts holds optional fields for RegisterTable.
// Nil pointers mean "don't update" (keep DB default or existing value).
type RegisterTableOpts struct {
	ConfigJSON *string // JSON blob to merge into the config
}

// RegisterTable provisions a table and applies config overrides.
// Returns (created bool, err error). created is true if the table was newly provisioned.
func (s *TableRegistration) RegisterTable(
	ctx context.Context,
	tableName, displayName string,
	opts RegisterTableOpts,
) (bool, error) {
	if err := db.ValidateSQLIdentifier(tableName); err != nil {
		return false, err
	}

	// Check if table already exists
	existsQuery, existsArgs, _ := sq.Select("COUNT(*) > 0").
		From(metastore.TableRegistry).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	var exists bool
	if err := s.db.QueryRowContext(ctx, existsQuery, existsArgs...).Scan(&exists); err != nil {
		return false, err
	}

	// Provision table
	if err := schema.EnsureTable(ctx, s.db, tableName, s.isMariaDB, s.compressionOverride, s.log); err != nil {
		return false, err
	}

	// Update display name if provided
	if displayName != "" {
		updateQuery, updateArgs, _ := sq.Update(metastore.TableRegistry).
			Set("display_name", displayName).
			Where(sq.Eq{"table_name": tableName}).
			ToSql()
		if _, err := s.db.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
			return false, err
		}
	}

	// Update table config blob if any config fields are explicitly set.
	if err := s.updateTableConfig(ctx, tableName, opts); err != nil {
		return false, fmt.Errorf("update table config: %w", err)
	}

	created := !exists
	s.log.Info("table registered",
		zap.String("table", tableName),
		zap.Bool("created", created),
	)
	return created, nil
}

// updateTableConfig performs a read-modify-write on the config blob.
// Only fields explicitly set in opts are overridden.
func (s *TableRegistration) updateTableConfig(ctx context.Context, tableName string, opts RegisterTableOpts) error {
	if opts.ConfigJSON == nil {
		return nil
	}

	// Read existing config blob.
	query, args, _ := sq.Select("config").
		From(metastore.TableRegistryConfig).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	var blob []byte
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&blob); err != nil && err != sql.ErrNoRows {
		return err
	}

	cfg, err := metastore.DecodeTableConfig(blob)
	if err != nil {
		return err
	}

	// Merge JSON overrides into existing config.
	// DisallowUnknownFields catches typos like "consolidation_enbaled".
	dec := json.NewDecoder(strings.NewReader(*opts.ConfigJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("parse config_json: %w", err)
	}

	// Encode and write back.
	newBlob, err := metastore.EncodeTableConfig(cfg)
	if err != nil {
		return err
	}

	updateQuery, updateArgs, _ := sq.Update(metastore.TableRegistryConfig).
		Set("config", newBlob).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	_, err = s.db.ExecContext(ctx, updateQuery, updateArgs...)
	return err
}

// resolveRegistryTable returns the registry table name and key column for a
// given column name based on its prefix (dim_f or agg_f).
func resolveRegistryTable(colName string) (registryTable, keyColumn string, err error) {
	if strings.HasPrefix(colName, metastore.DimColumnPrefix) {
		return metastore.DimRegistryTable, "dim_key", nil
	}
	if strings.HasPrefix(colName, metastore.AggColumnPrefix) {
		return metastore.AggRegistryTable, "agg_key", nil
	}
	return "", "", fmt.Errorf("%w: %q (must start with %q or %q)",
		ErrInvalidColumnPrefix, colName, metastore.DimColumnPrefix, metastore.AggColumnPrefix)
}

// validateAlias checks alias format. Returns the trimmed alias or an error.
func validateAlias(alias string) (string, error) {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "", nil // empty alias clears the value
	}
	if len(alias) > maxAliasLength {
		return "", fmt.Errorf("%w: exceeds max length of %d characters", ErrInvalidAlias, maxAliasLength)
	}
	if !aliasPattern.MatchString(alias) {
		return "", fmt.Errorf("%w: must match [a-zA-Z_][a-zA-Z0-9_./-]*", ErrInvalidAlias)
	}
	return alias, nil
}

// SetColumnAlias sets or clears the alias for a dimension or aggregation column.
// Returns the validated alias. Returns ErrColumnNotFound if no ACTIVE column matches.
func (s *TableRegistration) SetColumnAlias(ctx context.Context, tableName, colName, alias string) (string, error) {
	alias, err := validateAlias(alias)
	if err != nil {
		return "", err
	}

	registryTable, _, err := resolveRegistryTable(colName)
	if err != nil {
		return "", err
	}

	// Atomic update — only touches ACTIVE rows.
	var aliasVal any
	if alias != "" {
		aliasVal = alias
	}
	updateQuery, updateArgs, _ := sq.Update(registryTable).
		Set("alias_column", aliasVal).
		Where(sq.Eq{"table_name": tableName, "column_name": colName, "state": "ACTIVE"}).
		ToSql()
	res, err := s.db.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return "", fmt.Errorf("update alias: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return "", fmt.Errorf("%w: no ACTIVE column %s in table %s", ErrColumnNotFound, colName, tableName)
	}

	s.log.Info("column alias updated",
		zap.String("table", tableName),
		zap.String("column", colName),
		zap.String("alias", alias),
	)

	return alias, nil
}

// InvalidateColumn marks a dimension or aggregation column as INVALIDATED.
// Returns the previous key of the invalidated column.
func (s *TableRegistration) InvalidateColumn(ctx context.Context, tableName, colName string) (string, error) {
	registryTable, keyColumn, err := resolveRegistryTable(colName)
	if err != nil {
		return "", err
	}

	// Read the current key before invalidating.
	var previousKey string
	selectQuery, selectArgs, _ := sq.Select(keyColumn).
		From(registryTable).
		Where(sq.Eq{"table_name": tableName, "column_name": colName, "state": "ACTIVE"}).
		ToSql()
	if err := s.db.QueryRowContext(ctx, selectQuery, selectArgs...).Scan(&previousKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: no ACTIVE column %s in table %s", ErrColumnNotFound, colName, tableName)
		}
		return "", fmt.Errorf("lookup column: %w", err)
	}

	// Transition ACTIVE → INVALIDATED.
	now := time.Now().UnixNano()
	updateQuery, updateArgs, _ := sq.Update(registryTable).
		Set("state", "INVALIDATED").
		Set("invalidated_at", now).
		Where(sq.Eq{"table_name": tableName, "column_name": colName, "state": "ACTIVE"}).
		ToSql()
	res, err := s.db.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return "", fmt.Errorf("invalidate column: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return "", fmt.Errorf("%w: column %s in table %s was invalidated concurrently",
			ErrColumnNotFound, colName, tableName)
	}

	s.log.Info("column invalidated",
		zap.String("table", tableName),
		zap.String("column", colName),
		zap.String("previousKey", previousKey),
	)

	return previousKey, nil
}
