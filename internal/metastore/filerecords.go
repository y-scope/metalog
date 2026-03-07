package metastore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	dbutil "github.com/y-scope/metalog/internal/db"
)

// MaxMultiRowInsert limits rows per multi-row INSERT to stay within max_allowed_packet.
const MaxMultiRowInsert = 1000

// MaxConsolidationBatch limits the number of pending files returned per query.
const MaxConsolidationBatch = 10000

// FileRecords is the repository for metadata table operations.
type FileRecords struct {
	db        *sql.DB
	tableName string
	log       *zap.Logger
}

// NewFileRecords creates a FileRecords repository for the given table.
func NewFileRecords(db *sql.DB, tableName string, log *zap.Logger) (*FileRecords, error) {
	if err := dbutil.ValidateSQLIdentifier(tableName); err != nil {
		return nil, fmt.Errorf("new file records: %w", err)
	}
	return &FileRecords{db: db, tableName: tableName, log: log}, nil
}

// UpsertBatch inserts or updates multiple file records with guarded UPSERT.
// dimCols and aggCols specify the dynamic columns to include.
// floatAggCols is the subset of aggCols that hold DOUBLE values (rest are BIGINT).
func (fr *FileRecords) UpsertBatch(
	ctx context.Context,
	records []*FileRecord,
	dimCols []string,
	aggCols []string,
	floatAggCols map[string]bool,
) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	var totalAffected int64
	for start := 0; start < len(records); start += MaxMultiRowInsert {
		end := start + MaxMultiRowInsert
		if end > len(records) {
			end = len(records)
		}
		batch := records[start:end]

		query, args, _ := BuildGuardedUpsertSQL(fr.tableName, dimCols, aggCols, floatAggCols, len(batch))

		// Fill args for each row
		allArgs := make([]any, 0, len(args))
		for _, rec := range batch {
			allArgs = append(allArgs, baseColValues(rec)...)
			for _, dc := range dimCols {
				allArgs = append(allArgs, rec.Dims[dc])
			}
			for _, ac := range aggCols {
				allArgs = append(allArgs, rec.Aggs[ac])
			}
		}

		res, err := fr.db.ExecContext(ctx, query, allArgs...)
		if err != nil {
			return totalAffected, fmt.Errorf("upsert batch: %w", err)
		}
		n, _ := res.RowsAffected() // error always nil for MySQL/MariaDB driver
		totalAffected += n
	}

	fr.log.Debug("batch upserted",
		zap.Int("records", len(records)),
		zap.Int("dimCols", len(dimCols)),
		zap.Int("aggCols", len(aggCols)),
		zap.Int64("rowsAffected", totalAffected),
	)
	return totalAffected, nil
}

// FindConsolidationPending returns files in IR_ARCHIVE_CONSOLIDATION_PENDING state,
// ordered by min_timestamp ASC, limited to 10000.
func (fr *FileRecords) FindConsolidationPending(ctx context.Context) ([]*FileRecord, error) {
	query, args, err := sq.Select(baseSelectCols()...).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Eq{ColState: string(StateIRArchiveConsolidationPending)}).
		OrderBy(ColMinTimestamp + " ASC").
		Limit(MaxConsolidationBatch).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("find consolidation pending: build query: %w", err)
	}

	rows, err := fr.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find consolidation pending: %w", err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// GetCurrentStates returns the current state for files identified by IR path hash.
func (fr *FileRecords) GetCurrentStates(ctx context.Context, irPaths []string) (map[string]FileState, error) {
	if len(irPaths) == 0 {
		return map[string]FileState{}, nil
	}

	hashArgs := make([]any, len(irPaths))
	for i, p := range irPaths {
		hashArgs[i] = p
	}

	inClause := ColClpIRPathHash + " IN (" + joinRepeat("UNHEX(MD5(?))", len(irPaths), ",") + ")"
	query := "SELECT " + ColClpIRPath + ", " + ColState +
		" FROM " + dbutil.QuoteIdentifier(fr.tableName) +
		" WHERE " + inClause

	rows, err := fr.db.QueryContext(ctx, query, hashArgs...)
	if err != nil {
		return nil, fmt.Errorf("get current states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]FileState)
	for rows.Next() {
		var path string
		var state string
		if err := rows.Scan(&path, &state); err != nil {
			return nil, err
		}
		states[path] = FileState(state)
	}
	return states, rows.Err()
}

// MarkArchiveClosed transitions files to ARCHIVE_CLOSED after consolidation completes.
func (fr *FileRecords) MarkArchiveClosed(
	ctx context.Context,
	irPaths []string,
	archivePath string,
	archiveBackend string,
	archiveBucket string,
	archiveSizeBytes int64,
	archiveCreatedAt int64,
) error {
	if len(irPaths) == 0 {
		return nil
	}

	tx, err := fr.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	currentStates, err := fr.getCurrentStatesInTx(ctx, tx, irPaths)
	if err != nil {
		return err
	}

	target := StateArchiveClosed
	for _, p := range irPaths {
		cs, ok := currentStates[p]
		if !ok {
			fr.log.Warn("file not found for archive update", zap.String("irPath", p))
			continue
		}
		if !cs.CanTransitionTo(target) {
			return fmt.Errorf("cannot transition %s from %s to %s", p, cs, target)
		}
	}

	for _, p := range irPaths {
		cs, ok := currentStates[p]
		if !ok {
			continue // already warned above
		}
		query := "UPDATE " + dbutil.QuoteIdentifier(fr.tableName) +
			" SET " + ColClpArchivePath + " = ?, " +
			ColClpArchiveStorageBackend + " = ?, " +
			ColClpArchiveBucket + " = ?, " +
			ColClpArchiveSizeBytes + " = ?, " +
			ColClpArchiveCreatedAt + " = ?, " +
			ColState + " = ? " +
			"WHERE " + ColClpIRPathHash + " = UNHEX(MD5(?)) " +
			"AND " + ColClpArchivePath + " IS NULL " +
			"AND " + ColState + " = ?"

		_, err := tx.ExecContext(ctx, query,
			archivePath, archiveBackend, archiveBucket,
			archiveSizeBytes, archiveCreatedAt,
			string(target), p, string(cs),
		)
		if err != nil {
			return fmt.Errorf("mark archive closed: %w", err)
		}
	}

	return tx.Commit()
}

// UpdateState transitions files to a new state with validation.
func (fr *FileRecords) UpdateState(ctx context.Context, irPaths []string, newState FileState) (int64, error) {
	if len(irPaths) == 0 {
		return 0, nil
	}

	tx, err := fr.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("update state: begin tx: %w", err)
	}
	defer tx.Rollback()

	currentStates, err := fr.getCurrentStatesInTx(ctx, tx, irPaths)
	if err != nil {
		return 0, err
	}

	for _, p := range irPaths {
		cs, ok := currentStates[p]
		if !ok {
			continue
		}
		if !cs.CanTransitionTo(newState) {
			return 0, fmt.Errorf("cannot transition %s from %s to %s", p, cs, newState)
		}
	}

	var totalAffected int64
	for _, p := range irPaths {
		cs, ok := currentStates[p]
		if !ok {
			continue
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE "+dbutil.QuoteIdentifier(fr.tableName)+
				" SET "+ColState+" = ? WHERE "+ColClpIRPathHash+" = UNHEX(MD5(?)) AND "+ColState+" = ?",
			string(newState), p, string(cs),
		)
		if err != nil {
			return totalAffected, fmt.Errorf("update state: %w", err)
		}
		n, _ := res.RowsAffected()
		totalAffected += n
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("update state: commit: %w", err)
	}
	return totalAffected, nil
}

// getCurrentStatesInTx is like GetCurrentStates but executes within an existing transaction.
func (fr *FileRecords) getCurrentStatesInTx(ctx context.Context, tx *sql.Tx, irPaths []string) (map[string]FileState, error) {
	inClause := ColClpIRPathHash + " IN (" + joinRepeat("UNHEX(MD5(?))", len(irPaths), ",") + ")"
	query := "SELECT " + ColClpIRPath + ", " + ColState +
		" FROM " + dbutil.QuoteIdentifier(fr.tableName) +
		" WHERE " + inClause + " FOR UPDATE"

	hashArgs := make([]any, len(irPaths))
	for i, p := range irPaths {
		hashArgs[i] = p
	}

	rows, err := tx.QueryContext(ctx, query, hashArgs...)
	if err != nil {
		return nil, fmt.Errorf("get current states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]FileState)
	for rows.Next() {
		var path string
		var state string
		if err := rows.Scan(&path, &state); err != nil {
			return nil, err
		}
		states[path] = FileState(state)
	}
	return states, rows.Err()
}

// DeleteExpiredFiles removes files past their expiration and returns their storage paths.
func (fr *FileRecords) DeleteExpiredFiles(ctx context.Context, currentNanos int64) (*DeletionResult, error) {
	tx, err := fr.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("delete expired files: begin tx: %w", err)
	}
	defer tx.Rollback()

	// Select expired files
	rows, err := tx.QueryContext(ctx,
		"SELECT "+ColClpIRStorageBackend+", "+ColClpIRBucket+", "+ColClpIRPath+", "+
			ColClpArchiveStorageBackend+", "+ColClpArchiveBucket+", "+ColClpArchivePath+", "+
			ColClpIRPathHash+
			" FROM "+dbutil.QuoteIdentifier(fr.tableName)+
			" WHERE "+ColExpiresAt+" > 0 AND "+ColExpiresAt+" < ? LIMIT 1000",
		currentNanos,
	)
	if err != nil {
		return nil, fmt.Errorf("query expired files: %w", err)
	}

	result := &DeletionResult{}
	var hashes [][]byte
	for rows.Next() {
		var irBackend, irBucket, irPath sql.NullString
		var archiveBackend, archiveBucket, archivePath sql.NullString
		var pathHash []byte

		if err := rows.Scan(&irBackend, &irBucket, &irPath,
			&archiveBackend, &archiveBucket, &archivePath, &pathHash); err != nil {
			rows.Close()
			return nil, fmt.Errorf("delete expired files: scan: %w", err)
		}

		if irPath.Valid && irPath.String != "" {
			result.IRPaths = append(result.IRPaths, StoragePath{
				Backend: irBackend.String, Bucket: irBucket.String, Path: irPath.String,
			})
		}
		if archivePath.Valid && archivePath.String != "" {
			result.ArchivePaths = append(result.ArchivePaths, StoragePath{
				Backend: archiveBackend.String, Bucket: archiveBucket.String, Path: archivePath.String,
			})
		}
		hashes = append(hashes, pathHash)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delete expired files: rows: %w", err)
	}

	if len(hashes) == 0 {
		return result, tx.Commit()
	}

	// Delete by hash
	for _, h := range hashes {
		_, err := tx.ExecContext(ctx,
			"DELETE FROM "+dbutil.QuoteIdentifier(fr.tableName)+" WHERE "+ColClpIRPathHash+" = ?", h)
		if err != nil {
			return nil, fmt.Errorf("delete expired: %w", err)
		}
		result.DeletedCount++
	}

	return result, tx.Commit()
}

// FindByID returns a single file record by its primary key ID.
func (fr *FileRecords) FindByID(ctx context.Context, id int64) (*FileRecord, error) {
	query, args, err := sq.Select(baseSelectCols()...).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Eq{ColID: id}).
		Limit(1).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("find by id: build query: %w", err)
	}

	rows, err := fr.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find by id: %w", err)
	}
	defer rows.Close()

	records, err := scanFileRecords(rows)
	if err != nil {
		return nil, fmt.Errorf("find by id: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// FindByIRPath returns a single file record by its IR path (via UNHEX(MD5()) hash).
func (fr *FileRecords) FindByIRPath(ctx context.Context, irPath string) (*FileRecord, error) {
	query := "SELECT " + joinCols(baseSelectCols()) +
		" FROM " + dbutil.QuoteIdentifier(fr.tableName) +
		" WHERE " + ColClpIRPathHash + " = UNHEX(MD5(?)) LIMIT 1"

	rows, err := fr.db.QueryContext(ctx, query, irPath)
	if err != nil {
		return nil, fmt.Errorf("find by ir path: %w", err)
	}
	defer rows.Close()

	records, err := scanFileRecords(rows)
	if err != nil {
		return nil, fmt.Errorf("find by ir path: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// FindByArchivePath returns a single file record by its archive path (via UNHEX(MD5()) hash).
func (fr *FileRecords) FindByArchivePath(ctx context.Context, archivePath string) (*FileRecord, error) {
	query := "SELECT " + joinCols(baseSelectCols()) +
		" FROM " + dbutil.QuoteIdentifier(fr.tableName) +
		" WHERE " + ColClpArchivePathHash + " = UNHEX(MD5(?)) LIMIT 1"

	rows, err := fr.db.QueryContext(ctx, query, archivePath)
	if err != nil {
		return nil, fmt.Errorf("find by archive path: %w", err)
	}
	defer rows.Close()

	records, err := scanFileRecords(rows)
	if err != nil {
		return nil, fmt.Errorf("find by archive path: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

// baseColValues returns the ordered values for BaseCols from a FileRecord.
func baseColValues(rec *FileRecord) []any {
	return []any{
		rec.MinTimestamp,
		rec.MaxTimestamp,
		rec.ClpArchiveCreatedAt,
		rec.ClpIRStorageBackend,
		rec.ClpIRBucket,
		rec.ClpIRPath,
		rec.ClpArchiveStorageBackend,
		rec.ClpArchiveBucket,
		rec.ClpArchivePath,
		string(rec.State),
		rec.RecordCount,
		rec.RawSizeBytes,
		rec.ClpIRSizeBytes,
		rec.ClpArchiveSizeBytes,
		rec.RetentionDays,
		rec.ExpiresAt,
	}
}

func baseSelectCols() []string {
	return []string{
		ColID,
		ColClpIRStorageBackend, ColClpIRBucket, ColClpIRPath,
		ColClpArchiveStorageBackend, ColClpArchiveBucket, ColClpArchivePath,
		ColState, ColMinTimestamp, ColMaxTimestamp, ColClpArchiveCreatedAt,
		ColRecordCount, ColRawSizeBytes, ColClpIRSizeBytes, ColClpArchiveSizeBytes,
		ColRetentionDays, ColExpiresAt,
	}
}

func scanFileRecords(rows *sql.Rows) ([]*FileRecord, error) {
	var records []*FileRecord
	for rows.Next() {
		rec := &FileRecord{}
		var state string
		if err := rows.Scan(
			&rec.ID,
			&rec.ClpIRStorageBackend, &rec.ClpIRBucket, &rec.ClpIRPath,
			&rec.ClpArchiveStorageBackend, &rec.ClpArchiveBucket, &rec.ClpArchivePath,
			&state, &rec.MinTimestamp, &rec.MaxTimestamp, &rec.ClpArchiveCreatedAt,
			&rec.RecordCount, &rec.RawSizeBytes, &rec.ClpIRSizeBytes, &rec.ClpArchiveSizeBytes,
			&rec.RetentionDays, &rec.ExpiresAt,
		); err != nil {
			return nil, err
		}
		rec.State = FileState(state)
		records = append(records, rec)
	}
	return records, rows.Err()
}

func joinCols(cols []string) string {
	return strings.Join(cols, ", ")
}

func joinRepeat(s string, n int, sep string) string {
	if n <= 0 {
		return ""
	}
	result := s
	for i := 1; i < n; i++ {
		result += sep + s
	}
	return result
}
