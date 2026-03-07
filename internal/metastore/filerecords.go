package metastore

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	dbutil "github.com/y-scope/metalog/internal/db"
)

// MaxMultiRowInsert limits rows per multi-row INSERT to stay within max_allowed_packet.
const MaxMultiRowInsert = 1000

// MaxConsolidationBatch limits the number of pending files returned per query.
const MaxConsolidationBatch = 10000

// MaxExpirationBatch limits rows fetched per expiration cycle.
const MaxExpirationBatch = 1000

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

		query, _, paramsPerRow := BuildGuardedUpsertSQL(fr.tableName, dimCols, aggCols, floatAggCols, len(batch), false)

		// Fill args for each row
		allArgs := make([]any, 0, len(batch)*paramsPerRow)
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

	builder := sq.Select(ColClpIRPath, ColState).
		From(dbutil.QuoteIdentifier(fr.tableName))
	builder = whereIRPathHashIn(builder, irPaths)

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("get current states: build query: %w", err)
	}

	rows, err := fr.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get current states: %w", err)
	}
	defer rows.Close()

	return scanStateMap(rows)
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

	// Collect paths that exist in the DB (skip warned-about missing ones).
	validPaths := make([]string, 0, len(irPaths))
	for _, p := range irPaths {
		if _, ok := currentStates[p]; ok {
			validPaths = append(validPaths, p)
		}
	}
	if len(validPaths) == 0 {
		return tx.Commit()
	}

	// Batch update — all validated files are in the same source state.
	or := make(sq.Or, len(validPaths))
	for i, p := range validPaths {
		or[i] = sq.Expr(ColClpIRPathHash+" = UNHEX(MD5(?))", p)
	}
	query, args, _ := sq.Update(dbutil.QuoteIdentifier(fr.tableName)).
		Set(ColClpArchivePath, archivePath).
		Set(ColClpArchiveStorageBackend, archiveBackend).
		Set(ColClpArchiveBucket, archiveBucket).
		Set(ColClpArchiveSizeBytes, archiveSizeBytes).
		Set(ColClpArchiveCreatedAt, archiveCreatedAt).
		Set(ColState, string(target)).
		Where(or).
		Where(ColClpArchivePath + " IS NULL").
		Where(sq.Eq{ColState: string(StateIRArchiveConsolidationPending)}).
		ToSql()

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("mark archive closed: %w", err)
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

	// Collect valid source states for the WHERE clause.
	sourceStates := make(map[FileState]bool)
	validPaths := make([]string, 0, len(irPaths))
	for _, p := range irPaths {
		if cs, ok := currentStates[p]; ok {
			validPaths = append(validPaths, p)
			sourceStates[cs] = true
		}
	}
	if len(validPaths) == 0 {
		_ = tx.Commit()
		return 0, nil
	}

	// Batch update with OR-ed hash conditions.
	or := make(sq.Or, len(validPaths))
	for i, p := range validPaths {
		or[i] = sq.Expr(ColClpIRPathHash+" = UNHEX(MD5(?))", p)
	}
	stateList := make([]string, 0, len(sourceStates))
	for s := range sourceStates {
		stateList = append(stateList, string(s))
	}
	query, args, _ := sq.Update(dbutil.QuoteIdentifier(fr.tableName)).
		Set(ColState, string(newState)).
		Where(or).
		Where(sq.Eq{ColState: stateList}).
		ToSql()
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("update state: %w", err)
	}
	totalAffected, _ := res.RowsAffected()

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("update state: commit: %w", err)
	}
	return totalAffected, nil
}

// getCurrentStatesInTx is like GetCurrentStates but executes within an existing transaction.
func (fr *FileRecords) getCurrentStatesInTx(ctx context.Context, tx *sql.Tx, irPaths []string) (map[string]FileState, error) {
	builder := sq.Select(ColClpIRPath, ColState).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Suffix("FOR UPDATE")
	builder = whereIRPathHashIn(builder, irPaths)

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("get current states: build query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get current states: %w", err)
	}
	defer rows.Close()

	return scanStateMap(rows)
}

// DeleteExpiredFiles removes files past their expiration and returns their storage paths.
func (fr *FileRecords) DeleteExpiredFiles(ctx context.Context, currentNanos int64) (*DeletionResult, error) {
	tx, err := fr.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("delete expired files: begin tx: %w", err)
	}
	defer tx.Rollback()

	// Select expired files
	expQuery, expArgs, _ := sq.Select(
		ColClpIRStorageBackend, ColClpIRBucket, ColClpIRPath,
		ColClpArchiveStorageBackend, ColClpArchiveBucket, ColClpArchivePath,
		ColClpIRPathHash,
	).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Gt{ColExpiresAt: 0}).
		Where(sq.Lt{ColExpiresAt: currentNanos}).
		Where(sq.Eq{ColState: []string{string(StateIRPurging), string(StateArchivePurging)}}).
		Limit(MaxExpirationBatch).
		ToSql()
	rows, err := tx.QueryContext(ctx, expQuery, expArgs...)
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

	// Delete by hash (batched)
	hashArgs := make([]any, len(hashes))
	for i, h := range hashes {
		hashArgs[i] = h
	}
	delQuery, delArgs, _ := sq.Delete(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Eq{ColClpIRPathHash: hashArgs}).
		ToSql()
	res, err := tx.ExecContext(ctx, delQuery, delArgs...)
	if err != nil {
		return nil, fmt.Errorf("delete expired: %w", err)
	}
	result.DeletedCount, _ = res.RowsAffected()

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
	query, args, _ := sq.Select(baseSelectCols()...).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Expr(ColClpIRPathHash+" = UNHEX(MD5(?))", irPath)).
		Limit(1).
		ToSql()

	rows, err := fr.db.QueryContext(ctx, query, args...)
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
	query, args, _ := sq.Select(baseSelectCols()...).
		From(dbutil.QuoteIdentifier(fr.tableName)).
		Where(sq.Expr(ColClpArchivePathHash+" = UNHEX(MD5(?))", archivePath)).
		Limit(1).
		ToSql()

	rows, err := fr.db.QueryContext(ctx, query, args...)
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

// baseColValues returns the ordered values for baseCols from a FileRecord.
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

// whereIRPathHashIn adds a WHERE clause matching IR path hashes via UNHEX(MD5(?)).
// Squirrel doesn't natively support UNHEX(MD5(?)) in Eq, so we build the clause
// with sq.Expr for each path and combine with sq.Or.
func whereIRPathHashIn(builder sq.SelectBuilder, irPaths []string) sq.SelectBuilder {
	if len(irPaths) == 1 {
		return builder.Where(sq.Expr(ColClpIRPathHash+" = UNHEX(MD5(?))", irPaths[0]))
	}
	or := make(sq.Or, len(irPaths))
	for i, p := range irPaths {
		or[i] = sq.Expr(ColClpIRPathHash+" = UNHEX(MD5(?))", p)
	}
	return builder.Where(or)
}

// scanStateMap scans rows of (path, state) into a map.
func scanStateMap(rows *sql.Rows) (map[string]FileState, error) {
	states := make(map[string]FileState)
	for rows.Next() {
		var path, state string
		if err := rows.Scan(&path, &state); err != nil {
			return nil, err
		}
		states[path] = FileState(state)
	}
	return states, rows.Err()
}
