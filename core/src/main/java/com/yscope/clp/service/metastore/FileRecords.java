package com.yscope.clp.service.metastore;

import static com.yscope.clp.service.metastore.JooqFields.*;
import static org.jooq.impl.DSL.val;

import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.common.config.db.DSLContextFactory;
import com.yscope.clp.service.common.config.db.DataSourceFactory;
import com.yscope.clp.service.metastore.model.FileRecord;
import com.yscope.clp.service.metastore.model.FileState;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository for MySQL metadata operations.
 *
 * <p>Handles all database interactions for IR file metadata. Uses jOOQ DSL for type-safe SQL
 * generation with HikariCP for connection pooling.
 */
public class FileRecords implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(FileRecords.class);

  /**
   * SQL fragment: states where ingestion should not overwrite the row. Once a file has progressed
   * past buffering (e.g., picked up by the Planner), late re-deliveries must not regress it.
   */
  private static final List<String> PROTECTED_STATES =
      List.of("IR_ARCHIVE_CONSOLIDATION_PENDING", "ARCHIVE_CLOSED", "ARCHIVE_PURGING");

  /** Max rows per multi-row INSERT to stay within max_allowed_packet. */
  private static final int MAX_MULTI_ROW_INSERT = 1000;

  private final HikariDataSource dataSource;
  private final DSLContext dsl;
  private final Table<?> table;
  private final String tableName;
  private final boolean ownsDataSource;

  /**
   * Create a FileRecords with configuration.
   *
   * <p>Creates and owns a HikariDataSource that will be closed when this repository is closed.
   */
  public FileRecords(ServiceConfig config) {
    this.tableName = SqlIdentifiers.requireValidTableName(config.getDatabaseTable());
    this.table = JooqFields.table(tableName);
    this.dataSource = createDataSource(config);
    this.dsl = DSLContextFactory.create(dataSource);
    this.ownsDataSource = true;
    logger.info("Initialized FileRecords with table={}", tableName);
  }

  /**
   * Create a FileRecords with a shared DataSource.
   *
   * <p>Used by the Node architecture where the DataSource is shared across units. The DataSource
   * will NOT be closed when this repository is closed.
   */
  public FileRecords(HikariDataSource dataSource, String tableName) {
    this.tableName = SqlIdentifiers.requireValidTableName(tableName);
    this.table = JooqFields.table(this.tableName);
    this.dataSource = dataSource;
    this.dsl = DSLContextFactory.create(dataSource);
    this.ownsDataSource = false;
    logger.info("Initialized FileRecords with shared DataSource, table={}", this.tableName);
  }

  /**
   * Create a FileRecords with a pre-configured DSLContext.
   *
   * <p>Used when a DSLContext is already available (e.g., from SharedResources).
   */
  public FileRecords(HikariDataSource dataSource, DSLContext dsl, String tableName) {
    this.tableName = SqlIdentifiers.requireValidTableName(tableName);
    this.table = JooqFields.table(this.tableName);
    this.dataSource = dataSource;
    this.dsl = dsl;
    this.ownsDataSource = false;
    logger.info("Initialized FileRecords with shared DSLContext, table={}", this.tableName);
  }

  private HikariDataSource createDataSource(ServiceConfig config) {
    return DataSourceFactory.create(config);
  }

  // ========================================================================
  // Upsert guard helpers
  // ========================================================================

  /**
   * Guard condition: true when the existing row's state allows ingestion updates AND the incoming
   * max_timestamp is strictly newer.
   *
   * <p><b>Column ordering constraint:</b> Because this guard references max_timestamp, and
   * MySQL/MariaDB evaluates ON DUPLICATE KEY UPDATE assignments left-to-right, max_timestamp must
   * be the <b>last</b> column assigned.
   */
  private static org.jooq.Condition upsertGuard() {
    return STATE
        .notIn(PROTECTED_STATES)
        .and(DSL.field("VALUES({0})", Long.class, MAX_TIMESTAMP).gt(MAX_TIMESTAMP));
  }

  /**
   * Wrap a column value with the upsert guard: {@code IF(guard, VALUES(col), col)}.
   *
   * <p>Do NOT use this for max_timestamp in the middle of the update list.
   */
  @SuppressWarnings("unchecked")
  private static <T> Field<T> guardedValue(Field<T> f) {
    return (Field<T>)
        DSL.when(upsertGuard(), DSL.field("VALUES({0})", f.getDataType(), f)).otherwise(f);
  }

  // ========================================================================
  // Upsert operations
  // ========================================================================

  /**
   * Upsert IR file metadata (idempotent operation).
   *
   * <p>Used by Kafka consumer to insert/update metadata from log-collector messages.
   */
  public void upsertFileRecord(FileRecord file) throws SQLException {
    try {
      // max_timestamp must be the LAST assignment — see upsertGuard() Javadoc.
      dsl.insertInto(table)
          .set(IR_STORAGE_BACKEND, file.getIrStorageBackend())
          .set(IR_BUCKET, nullIfEmpty(file.getIrBucket()))
          .set(IR_PATH, nullIfEmpty(file.getIrPath()))
          .set(ARCHIVE_STORAGE_BACKEND, file.getArchiveStorageBackend())
          .set(ARCHIVE_BUCKET, nullIfEmpty(file.getArchiveBucket()))
          .set(ARCHIVE_PATH, nullIfEmpty(file.getArchivePath()))
          .set(STATE, file.getState().name())
          .set(MIN_TIMESTAMP, file.getMinTimestamp())
          .set(MAX_TIMESTAMP, file.getMaxTimestamp())
          .set(ARCHIVE_CREATED_AT, file.getArchiveCreatedAt())
          .set(RECORD_COUNT, file.getRecordCount())
          .set(RAW_SIZE_BYTES, file.getRawSizeBytes())
          .set(IR_SIZE_BYTES, file.getIrSizeBytes())
          .set(ARCHIVE_SIZE_BYTES, file.getArchiveSizeBytes())
          .set(RETENTION_DAYS, file.getRetentionDays())
          .set(EXPIRES_AT, file.getExpiresAt())
          .onDuplicateKeyUpdate()
          .set(STATE, guardedValue(STATE))
          .set(RECORD_COUNT, guardedValue(RECORD_COUNT))
          .set(RAW_SIZE_BYTES, guardedValue(RAW_SIZE_BYTES))
          .set(IR_SIZE_BYTES, guardedValue(IR_SIZE_BYTES))
          .set(ARCHIVE_PATH, guardedValue(ARCHIVE_PATH))
          .set(ARCHIVE_STORAGE_BACKEND, guardedValue(ARCHIVE_STORAGE_BACKEND))
          .set(ARCHIVE_BUCKET, guardedValue(ARCHIVE_BUCKET))
          .set(ARCHIVE_SIZE_BYTES, guardedValue(ARCHIVE_SIZE_BYTES))
          .set(ARCHIVE_CREATED_AT, guardedValue(ARCHIVE_CREATED_AT))
          .set(MAX_TIMESTAMP, guardedValue(MAX_TIMESTAMP)) // LAST
          .execute();
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Upsert IR file metadata with dynamic dimension columns.
   *
   * @param file The IR file to upsert
   * @param dimensionColumns Set of dimension column names that exist in the schema
   */
  public void upsertWithDynamicDimensions(FileRecord file, Set<String> dimensionColumns)
      throws SQLException {
    try {
      var insert = baseInsertStep(file);

      List<String> includedDimensions = new ArrayList<>();
      for (String dimCol : dimensionColumns) {
        if (file.getDimension(dimCol) != null) {
          insert.set(dimField(dimCol), dimensionAsString(file, dimCol));
          includedDimensions.add(dimCol);
        }
      }

      var update = addBaseGuardedUpdates(insert.onDuplicateKeyUpdate());
      for (String dimCol : includedDimensions) {
        update = update.set(dimField(dimCol), guardedValue(dimField(dimCol)));
      }
      update.set(MAX_TIMESTAMP, guardedValue(MAX_TIMESTAMP)).execute();
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Batch upsert IR file metadata with dynamic dimension columns.
   *
   * <p>Generates a single multi-row INSERT ... VALUES (...), (...), ... ON DUPLICATE KEY UPDATE for
   * maximum throughput (one round trip instead of N).
   *
   * @param files List of IR files to upsert
   * @param dimensionColumns Set of dimension column names that exist in the schema
   * @return Number of rows affected
   */
  public int upsertBatchWithDynamicDimensions(List<FileRecord> files, Set<String> dimensionColumns)
      throws SQLException {
    if (files.isEmpty()) {
      return 0;
    }

    List<String> includedDimensions = new ArrayList<>(dimensionColumns);

    try {
      return dsl.transactionResult(
          ctx -> {
            DSLContext txDsl = ctx.dsl();
            int totalAffected = 0;

            for (int start = 0; start < files.size(); start += MAX_MULTI_ROW_INSERT) {
              int end = Math.min(start + MAX_MULTI_ROW_INSERT, files.size());

              InsertSetMoreStep<?> insertStep =
                  setBaseValues(txDsl.insertInto(table), files.get(start));
              for (String dimCol : includedDimensions) {
                insertStep =
                    insertStep.set(dimField(dimCol), dimensionAsString(files.get(start), dimCol));
              }
              for (int i = start + 1; i < end; i++) {
                insertStep = setBaseValues(insertStep.newRecord(), files.get(i));
                for (String dimCol : includedDimensions) {
                  insertStep =
                      insertStep.set(dimField(dimCol), dimensionAsString(files.get(i), dimCol));
                }
              }

              var update = addBaseGuardedUpdates(insertStep.onDuplicateKeyUpdate());
              for (String dimCol : includedDimensions) {
                update = update.set(dimField(dimCol), guardedValue(dimField(dimCol)));
              }
              totalAffected += update.set(MAX_TIMESTAMP, guardedValue(MAX_TIMESTAMP)).execute();
            }

            logger.debug(
                "Batch upserted {} IR files with {} dimensions ({} rows affected)",
                files.size(),
                includedDimensions.size(),
                totalAffected);
            return totalAffected;
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Batch upsert with column registry translation.
   *
   * <p>Generates a single multi-row INSERT ... VALUES (...), (...), ... ON DUPLICATE KEY UPDATE.
   * Placeholder column names (e.g., {@code dim_f01}, {@code agg_f01}) are used in SQL. Values are
   * read from {@link FileRecord} dimensions, counts, or floatCounts via the translation maps.
   *
   * @param files IR files to upsert
   * @param dimPlaceholders ordered list of dimension placeholder column names
   * @param dimFieldToColumn map from original FileRecord dimension key to placeholder column
   * @param aggPlaceholders ordered list of agg placeholder column names
   * @param aggFieldToColumn map from original FileRecord agg key to placeholder column
   * @param floatAggPlaceholders subset of aggPlaceholders that hold DOUBLE (float) values; the
   *     remainder are treated as BIGINT (int)
   */
  public int upsertBatchWithTranslation(
      List<FileRecord> files,
      List<String> dimPlaceholders,
      Map<String, String> dimFieldToColumn,
      List<String> aggPlaceholders,
      Map<String, String> aggFieldToColumn,
      Set<String> floatAggPlaceholders)
      throws SQLException {
    if (files.isEmpty()) {
      return 0;
    }

    // Build reverse maps: columnName -> fieldKey
    Map<String, String> dimColumnToField = reverseMap(dimFieldToColumn);
    Map<String, String> aggColumnToField = reverseMap(aggFieldToColumn);

    try {
      return dsl.transactionResult(
          ctx -> {
            DSLContext txDsl = ctx.dsl();
            int totalAffected = 0;

            for (int start = 0; start < files.size(); start += MAX_MULTI_ROW_INSERT) {
              int end = Math.min(start + MAX_MULTI_ROW_INSERT, files.size());

              InsertSetMoreStep<?> insertStep =
                  setBaseValues(txDsl.insertInto(table), files.get(start));
              insertStep =
                  setDimAndAggValues(
                      insertStep,
                      files.get(start),
                      dimPlaceholders,
                      dimColumnToField,
                      aggPlaceholders,
                      aggColumnToField,
                      floatAggPlaceholders);

              for (int i = start + 1; i < end; i++) {
                insertStep = setBaseValues(insertStep.newRecord(), files.get(i));
                insertStep =
                    setDimAndAggValues(
                        insertStep,
                        files.get(i),
                        dimPlaceholders,
                        dimColumnToField,
                        aggPlaceholders,
                        aggColumnToField,
                        floatAggPlaceholders);
              }

              var update = addBaseGuardedUpdates(insertStep.onDuplicateKeyUpdate());
              for (String colName : dimPlaceholders) {
                update = update.set(dimField(colName), guardedValue(dimField(colName)));
              }
              for (String colName : aggPlaceholders) {
                if (floatAggPlaceholders.contains(colName)) {
                  update = update.set(aggFloatField(colName), guardedValue(aggFloatField(colName)));
                } else {
                  update = update.set(aggField(colName), guardedValue(aggField(colName)));
                }
              }
              totalAffected += update.set(MAX_TIMESTAMP, guardedValue(MAX_TIMESTAMP)).execute();
            }

            logger.debug(
                "Batch upserted {} IR files with {} dims + {} aggs translated ({} rows affected)",
                files.size(),
                dimPlaceholders.size(),
                aggPlaceholders.size(),
                totalAffected);
            return totalAffected;
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /** Set dimension and aggregate placeholder values on an insert row. */
  private static InsertSetMoreStep<?> setDimAndAggValues(
      InsertSetMoreStep<?> row,
      FileRecord file,
      List<String> dimPlaceholders,
      Map<String, String> dimColumnToField,
      List<String> aggPlaceholders,
      Map<String, String> aggColumnToField,
      Set<String> floatAggPlaceholders) {
    for (String colName : dimPlaceholders) {
      String fieldKey = dimColumnToField.get(colName);
      Object value = fieldKey != null ? file.getDimension(fieldKey) : null;
      row = row.set(dimField(colName), value != null ? value.toString() : null);
    }
    for (String colName : aggPlaceholders) {
      String fieldKey = aggColumnToField.get(colName);
      if (floatAggPlaceholders.contains(colName)) {
        Double value = fieldKey != null ? file.getFloatAgg(fieldKey) : null;
        row = row.set(aggFloatField(colName), value != null ? value : 0.0);
      } else {
        Long value = fieldKey != null ? file.getIntAgg(fieldKey) : null;
        row = row.set(aggField(colName), value != null ? value : 0L);
      }
    }
    return row;
  }

  /**
   * Batch upsert IR file metadata (idempotent operation).
   *
   * <p>Generates a single multi-row INSERT ... VALUES (...), (...), ... ON DUPLICATE KEY UPDATE for
   * maximum throughput (one round trip instead of N).
   *
   * @param files List of IR files to upsert
   * @return Number of rows affected
   */
  public int upsertBatch(List<FileRecord> files) throws SQLException {
    if (files.isEmpty()) {
      return 0;
    }

    try {
      return dsl.transactionResult(
          ctx -> {
            DSLContext txDsl = ctx.dsl();
            int totalAffected = 0;

            for (int start = 0; start < files.size(); start += MAX_MULTI_ROW_INSERT) {
              int end = Math.min(start + MAX_MULTI_ROW_INSERT, files.size());

              InsertSetMoreStep<?> insertStep =
                  setBaseValues(txDsl.insertInto(table), files.get(start));
              for (int i = start + 1; i < end; i++) {
                insertStep = setBaseValues(insertStep.newRecord(), files.get(i));
              }

              totalAffected +=
                  addBaseGuardedUpdates(insertStep.onDuplicateKeyUpdate())
                      .set(MAX_TIMESTAMP, guardedValue(MAX_TIMESTAMP))
                      .execute();
            }

            logger.debug(
                "Batch upserted {} IR files ({} rows affected)", files.size(), totalAffected);
            return totalAffected;
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  // ========================================================================
  // Read operations
  // ========================================================================

  /**
   * Find files in CONSOLIDATION_PENDING state.
   *
   * <p>Used by coordinator to find files ready for aggregation.
   */
  public List<FileRecord> findConsolidationPendingFiles() throws SQLException {
    try {
      return dsl.select(
              IR_STORAGE_BACKEND,
              IR_BUCKET,
              IR_PATH,
              ARCHIVE_STORAGE_BACKEND,
              ARCHIVE_BUCKET,
              ARCHIVE_PATH,
              STATE,
              MIN_TIMESTAMP,
              MAX_TIMESTAMP,
              ARCHIVE_CREATED_AT,
              RECORD_COUNT,
              RAW_SIZE_BYTES,
              IR_SIZE_BYTES,
              ARCHIVE_SIZE_BYTES,
              RETENTION_DAYS,
              EXPIRES_AT)
          .from(table)
          .where(STATE.eq(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.name()))
          .orderBy(MIN_TIMESTAMP.asc())
          .limit(10000)
          .fetch(this::mapRecordToFileRecord);
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Get current states for multiple IR paths.
   *
   * @param irPaths List of IR paths to look up
   * @return Map of IR path to current state (missing paths are not in the map)
   */
  public Map<String, FileState> getCurrentStates(List<String> irPaths) throws SQLException {
    if (irPaths == null || irPaths.isEmpty()) {
      return new HashMap<>();
    }

    try {
      Map<String, FileState> states = new HashMap<>();

      var rows =
          dsl.select(IR_PATH, STATE)
              .from(table)
              .where(IR_PATH_HASH.in(irPaths.stream().map(p -> md5Hash(val(p))).toList()))
              .fetch();

      for (var r : rows) {
        String path = r.get(IR_PATH);
        String stateStr = r.get(STATE);
        try {
          FileState state = FileState.valueOf(stateStr);
          states.put(path, state);
        } catch (IllegalArgumentException e) {
          logger.warn("Unknown state '{}' for irPath={}", stateStr, path);
        }
      }
      return states;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Get the current state of a file by IR path.
   *
   * @param irPath The IR path to look up
   * @return The current state, or null if not found
   */
  public FileState getCurrentState(String irPath) throws SQLException {
    if (irPath == null || irPath.isEmpty()) {
      return null;
    }
    Map<String, FileState> states = getCurrentStates(List.of(irPath));
    return states.get(irPath);
  }

  // ========================================================================
  // State transition operations
  // ========================================================================

  /**
   * Transition files to ARCHIVE_CLOSED after consolidation completes.
   *
   * @param irPaths List of IR paths to update
   * @param archivePath The archive path to set
   * @param archiveSizeBytes Size of the archive in bytes
   * @param archiveCreatedAt Timestamp when archive was created
   */
  public void markArchiveClosed(
      List<String> irPaths, String archivePath, long archiveSizeBytes, long archiveCreatedAt)
      throws SQLException {
    if (irPaths == null || irPaths.isEmpty()) {
      return;
    }

    Map<String, FileState> currentStates = getCurrentStates(irPaths);
    FileState targetState = FileState.ARCHIVE_CLOSED;

    for (String irPath : irPaths) {
      FileState currentState = currentStates.get(irPath);
      if (currentState == null) {
        logger.warn("File not found for archive update: irPath={}", irPath);
        continue;
      }
      if (!currentState.canTransitionTo(targetState)) {
        logger.warn(
            "Invalid state transition attempted: irPath={}, currentState={}, targetState={}",
            irPath,
            currentState,
            targetState);
        throw new IllegalStateException(
            String.format(
                "Cannot transition file from %s to %s: irPath=%s",
                currentState, targetState, irPath));
      }
    }

    try {
      dsl.transaction(
          ctx -> {
            DSLContext txDsl = ctx.dsl();
            for (String irPath : irPaths) {
              FileState currentState = currentStates.get(irPath);
              if (currentState != null) {
                logger.debug(
                    "State transition: irPath={}, {} -> {}", irPath, currentState, targetState);
              }

              txDsl
                  .update(table)
                  .set(ARCHIVE_PATH, archivePath)
                  .set(ARCHIVE_SIZE_BYTES, archiveSizeBytes)
                  .set(ARCHIVE_CREATED_AT, archiveCreatedAt)
                  .set(STATE, targetState.name())
                  .where(IR_PATH_HASH.eq(md5Hash(val(irPath))))
                  .and(ARCHIVE_PATH.isNull())
                  .and(STATE.eq(currentState.name()))
                  .execute();
            }
          });

      logger.info("Updated {} IR files with archive path={}", irPaths.size(), archivePath);
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Update state for a list of IR files with validation.
   *
   * @param irPaths List of IR paths to update
   * @param newState The target state
   * @return Number of rows affected
   */
  public int updateState(List<String> irPaths, FileState newState) throws SQLException {
    if (irPaths == null || irPaths.isEmpty()) {
      return 0;
    }
    if (newState == null) {
      throw new IllegalArgumentException("Target state cannot be null");
    }

    Map<String, FileState> currentStates = getCurrentStates(irPaths);

    List<String> invalidTransitions = new ArrayList<>();
    for (String irPath : irPaths) {
      FileState currentState = currentStates.get(irPath);
      if (currentState == null) {
        logger.warn("File not found for state update: irPath={}", irPath);
        continue;
      }
      if (!currentState.canTransitionTo(newState)) {
        invalidTransitions.add(String.format("%s (%s -> %s)", irPath, currentState, newState));
      }
    }

    if (!invalidTransitions.isEmpty()) {
      String message =
          String.format("Invalid state transitions: %s", String.join(", ", invalidTransitions));
      logger.warn(message);
      throw new IllegalStateException(message);
    }

    try {
      int totalAffected = 0;
      for (String irPath : irPaths) {
        FileState currentState = currentStates.get(irPath);
        if (currentState != null) {
          logger.debug("State transition: irPath={}, {} -> {}", irPath, currentState, newState);
          totalAffected +=
              dsl.update(table)
                  .set(STATE, newState.name())
                  .where(IR_PATH_HASH.eq(md5Hash(val(irPath))))
                  .and(STATE.eq(currentState.name()))
                  .execute();
        }
      }

      logger.info(
          "Updated state to {} for {} IR files ({} rows affected)",
          newState,
          irPaths.size(),
          totalAffected);
      return totalAffected;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /** Alias for {@link #updateState(List, FileState)}. */
  public int updateStateBatch(List<String> irPaths, FileState newState) throws SQLException {
    return updateState(irPaths, newState);
  }

  /** Update state for a single IR file. */
  public boolean updateState(String irPath, FileState newState) throws SQLException {
    if (irPath == null || irPath.isEmpty()) {
      throw new IllegalArgumentException("IR path cannot be null or empty");
    }
    return updateState(List.of(irPath), newState) > 0;
  }

  /** Transition files to IR_ARCHIVE_CONSOLIDATION_PENDING state. */
  public int markConsolidationPending(List<String> irPaths) throws SQLException {
    return updateState(irPaths, FileState.IR_ARCHIVE_CONSOLIDATION_PENDING);
  }

  // ========================================================================
  // Delete operations
  // ========================================================================

  /**
   * Delete expired files (retention cleanup).
   *
   * @param currentTimestamp Current time in UNIX seconds
   * @return Number of deleted records
   */
  public int deleteExpiredFiles(long currentTimestamp) throws SQLException {
    try {
      var delete =
          dsl.deleteFrom(table).where(EXPIRES_AT.gt(0L)).and(EXPIRES_AT.lt(currentTimestamp));
      int deleted = dsl.execute(delete.getSQL() + " LIMIT 1000", delete.getBindValues().toArray());

      if (deleted > 0) {
        logger.info("Deleted {} expired files", deleted);
      }
      return deleted;
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  /**
   * Delete expired files and return their paths for storage cleanup.
   *
   * @param currentTimestamp Current time in UNIX seconds
   * @return Result containing deleted count and file paths for storage cleanup
   */
  public DeletionResult deleteExpiredFilesWithPaths(long currentTimestamp) throws SQLException {
    try {
      return dsl.transactionResult(
          ctx -> {
            DSLContext txDsl = ctx.dsl();

            var expiredFiles =
                txDsl
                    .select(
                        IR_STORAGE_BACKEND,
                        IR_BUCKET,
                        IR_PATH,
                        ARCHIVE_STORAGE_BACKEND,
                        ARCHIVE_BUCKET,
                        ARCHIVE_PATH)
                    .from(table)
                    .where(EXPIRES_AT.gt(0L))
                    .and(EXPIRES_AT.lt(currentTimestamp))
                    .limit(1000)
                    .fetch();

            if (expiredFiles.isEmpty()) {
              return new DeletionResult(0, List.of(), List.of());
            }

            List<String> irPaths = new ArrayList<>();
            List<String> archivePaths = new ArrayList<>();
            int totalDeleted = 0;
            for (var row : expiredFiles) {
              String irPath = row.get(IR_PATH);
              String archivePath = row.get(ARCHIVE_PATH);

              txDsl.deleteFrom(table).where(IR_PATH_HASH.eq(md5Hash(val(irPath)))).execute();
              totalDeleted++;

              if (irPath != null && !irPath.isEmpty()) {
                irPaths.add(
                    formatStoragePath(row.get(IR_STORAGE_BACKEND), row.get(IR_BUCKET), irPath));
              }
              if (archivePath != null && !archivePath.isEmpty()) {
                archivePaths.add(
                    formatStoragePath(
                        row.get(ARCHIVE_STORAGE_BACKEND), row.get(ARCHIVE_BUCKET), archivePath));
              }
            }

            if (totalDeleted > 0) {
              logger.info(
                  "Deleted {} expired files from database (IR paths: {}, archive paths: {})",
                  totalDeleted,
                  irPaths.size(),
                  archivePaths.size());
            }

            return new DeletionResult(totalDeleted, irPaths, archivePaths);
          });
    } catch (DataAccessException e) {
      throw translated(e);
    }
  }

  // ========================================================================
  // Insert step builders (shared between upsert variants)
  // ========================================================================

  /** Build a single-row INSERT with all base columns set. */
  private InsertSetMoreStep<?> baseInsertStep(FileRecord file) {
    return setBaseValues(dsl.insertInto(table), file);
  }

  /**
   * Set all base column values on an insert step.
   *
   * <p>Works with both {@code ctx.insertInto(table)} (first row) and {@code insertStep.newRecord()}
   * (subsequent rows in multi-row INSERT).
   */
  private InsertSetMoreStep<?> setBaseValues(InsertSetStep<?> step, FileRecord file) {
    return step.set(IR_STORAGE_BACKEND, file.getIrStorageBackend())
        .set(IR_BUCKET, nullIfEmpty(file.getIrBucket()))
        .set(IR_PATH, nullIfEmpty(file.getIrPath()))
        .set(ARCHIVE_STORAGE_BACKEND, file.getArchiveStorageBackend())
        .set(ARCHIVE_BUCKET, nullIfEmpty(file.getArchiveBucket()))
        .set(ARCHIVE_PATH, nullIfEmpty(file.getArchivePath()))
        .set(STATE, file.getState().name())
        .set(MIN_TIMESTAMP, file.getMinTimestamp())
        .set(MAX_TIMESTAMP, file.getMaxTimestamp())
        .set(ARCHIVE_CREATED_AT, file.getArchiveCreatedAt())
        .set(RECORD_COUNT, file.getRecordCount())
        .set(RAW_SIZE_BYTES, file.getRawSizeBytes())
        .set(IR_SIZE_BYTES, file.getIrSizeBytes())
        .set(ARCHIVE_SIZE_BYTES, file.getArchiveSizeBytes())
        .set(RETENTION_DAYS, file.getRetentionDays())
        .set(EXPIRES_AT, file.getExpiresAt());
  }

  /** Add standard guarded update columns (without max_timestamp). */
  private InsertOnDuplicateSetMoreStep<?> addBaseGuardedUpdates(InsertOnDuplicateSetStep<?> step) {
    return step.set(STATE, guardedValue(STATE))
        .set(RECORD_COUNT, guardedValue(RECORD_COUNT))
        .set(RAW_SIZE_BYTES, guardedValue(RAW_SIZE_BYTES))
        .set(IR_SIZE_BYTES, guardedValue(IR_SIZE_BYTES))
        .set(ARCHIVE_PATH, guardedValue(ARCHIVE_PATH))
        .set(ARCHIVE_STORAGE_BACKEND, guardedValue(ARCHIVE_STORAGE_BACKEND))
        .set(ARCHIVE_BUCKET, guardedValue(ARCHIVE_BUCKET))
        .set(ARCHIVE_SIZE_BYTES, guardedValue(ARCHIVE_SIZE_BYTES))
        .set(ARCHIVE_CREATED_AT, guardedValue(ARCHIVE_CREATED_AT));
  }

  // ========================================================================
  // Record mapping
  // ========================================================================

  private FileRecord mapRecordToFileRecord(Record r) {
    return mapRecordToFileRecord(r, Set.of());
  }

  private FileRecord mapRecordToFileRecord(Record r, Set<String> dimensionColumns) {
    FileRecord file = new FileRecord();
    file.setIrStorageBackend(r.get(IR_STORAGE_BACKEND));
    file.setIrBucket(r.get(IR_BUCKET));
    file.setIrPath(r.get(IR_PATH));
    file.setArchiveStorageBackend(r.get(ARCHIVE_STORAGE_BACKEND));
    file.setArchiveBucket(r.get(ARCHIVE_BUCKET));
    file.setArchivePath(r.get(ARCHIVE_PATH));
    file.setState(FileState.valueOf(r.get(STATE)));
    file.setMinTimestamp(r.get(MIN_TIMESTAMP));
    file.setMaxTimestamp(r.get(MAX_TIMESTAMP));
    file.setArchiveCreatedAt(r.get(ARCHIVE_CREATED_AT));
    file.setRecordCount(r.get(RECORD_COUNT));
    file.setRawSizeBytes(r.get(RAW_SIZE_BYTES));
    file.setIrSizeBytes(r.get(IR_SIZE_BYTES));
    file.setArchiveSizeBytes(r.get(ARCHIVE_SIZE_BYTES));
    file.setRetentionDays(r.get(RETENTION_DAYS));
    file.setExpiresAt(r.get(EXPIRES_AT));

    for (String dimCol : dimensionColumns) {
      try {
        String value = r.get(dimField(dimCol));
        if (value != null) {
          file.setDimension(dimCol, value);
        }
      } catch (Exception e) {
        logger.trace("Dimension column {} not in result set", dimCol);
      }
    }

    return file;
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private static String orEmpty(String value) {
    return value != null ? value : "";
  }

  private static String nullIfEmpty(String value) {
    return (value != null && !value.isEmpty()) ? value : null;
  }

  private static String dimensionAsString(FileRecord file, String dimCol) {
    Object value = file.getDimension(dimCol);
    return value != null ? value.toString() : null;
  }

  private static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
    Map<V, K> reversed = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      reversed.put(entry.getValue(), entry.getKey());
    }
    return reversed;
  }

  private String formatStoragePath(String backend, String bucket, String path) {
    if (backend == null || backend.isEmpty()) {
      return path;
    }
    String bucketPart = (bucket != null) ? bucket : "";
    return String.format("%s://%s%s", backend, bucketPart, path);
  }

  private static SQLException translated(DataAccessException e) {
    if (e.getCause() instanceof SQLException sqlEx) {
      return sqlEx;
    }
    return new SQLException(e.getMessage(), e);
  }

  /** Get the underlying DataSource for direct access. */
  public HikariDataSource getDataSource() {
    return dataSource;
  }

  /** Get the DSLContext. */
  public DSLContext getDsl() {
    return dsl;
  }

  @Override
  public void close() {
    if (ownsDataSource && dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      logger.info("Closed FileRecords and its DataSource");
    } else {
      logger.info("Closed FileRecords (shared DataSource not closed)");
    }
  }

  /** Result of a deletion operation containing paths for storage cleanup. */
  public static class DeletionResult {
    private final int deletedCount;
    private final List<String> irPaths;
    private final List<String> archivePaths;

    public DeletionResult(int deletedCount, List<String> irPaths, List<String> archivePaths) {
      this.deletedCount = deletedCount;
      this.irPaths = irPaths;
      this.archivePaths = archivePaths;
    }

    public int getDeletedCount() {
      return deletedCount;
    }

    public List<String> getIrPaths() {
      return irPaths;
    }

    public List<String> getArchivePaths() {
      return archivePaths;
    }

    public List<String> getAllPaths() {
      List<String> allPaths = new ArrayList<>(irPaths);
      allPaths.addAll(archivePaths);
      return allPaths;
    }

    public boolean hasDeletedFiles() {
      return deletedCount > 0;
    }
  }
}
