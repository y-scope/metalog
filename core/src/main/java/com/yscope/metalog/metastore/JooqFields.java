package com.yscope.metalog.metastore;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.BLOB;
import static org.jooq.impl.SQLDataType.DOUBLE;
import static org.jooq.impl.SQLDataType.INTEGER;
import static org.jooq.impl.SQLDataType.VARCHAR;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Type-safe jOOQ field references matching {@link Columns} string constants.
 *
 * <p>Provides typed fields for use in jOOQ DSL queries. For dynamic placeholder columns use the
 * factory methods {@link #dimField(String)}, {@link #aggField(String)}, and {@link
 * #aggFloatField(String)}.
 */
public final class JooqFields {

  private JooqFields() {}

  // --- IR Location ---
  public static final Field<String> IR_STORAGE_BACKEND =
      field(name("clp_ir_storage_backend"), VARCHAR);
  public static final Field<String> IR_BUCKET = field(name("clp_ir_bucket"), VARCHAR);
  public static final Field<String> IR_PATH = field(name("clp_ir_path"), VARCHAR);

  // --- Archive Location ---
  public static final Field<String> ARCHIVE_STORAGE_BACKEND =
      field(name("clp_archive_storage_backend"), VARCHAR);
  public static final Field<String> ARCHIVE_BUCKET = field(name("clp_archive_bucket"), VARCHAR);
  public static final Field<String> ARCHIVE_PATH = field(name("clp_archive_path"), VARCHAR);

  // --- Hash Columns ---
  public static final Field<byte[]> IR_PATH_HASH = field(name("clp_ir_path_hash"), BLOB);
  public static final Field<byte[]> ARCHIVE_PATH_HASH = field(name("clp_archive_path_hash"), BLOB);

  // --- State ---
  public static final Field<String> STATE = field(name("state"), VARCHAR);

  // --- Timestamps ---
  public static final Field<Long> MIN_TIMESTAMP = field(name("min_timestamp"), BIGINT);
  public static final Field<Long> MAX_TIMESTAMP = field(name("max_timestamp"), BIGINT);
  public static final Field<Long> ARCHIVE_CREATED_AT =
      field(name("clp_archive_created_at"), BIGINT);

  // --- Record Count ---
  public static final Field<Long> RECORD_COUNT = field(name("record_count"), BIGINT);

  // --- Sizes ---
  public static final Field<Long> RAW_SIZE_BYTES = field(name("raw_size_bytes"), BIGINT);
  public static final Field<Long> IR_SIZE_BYTES = field(name("clp_ir_size_bytes"), BIGINT);
  public static final Field<Long> ARCHIVE_SIZE_BYTES =
      field(name("clp_archive_size_bytes"), BIGINT);

  // --- Retention ---
  public static final Field<Integer> RETENTION_DAYS = field(name("retention_days"), INTEGER);
  public static final Field<Long> EXPIRES_AT = field(name("expires_at"), BIGINT);

  // --- Sketches / Extension ---
  public static final Field<String> SKETCHES = field(name("sketches"), VARCHAR);
  public static final Field<byte[]> EXT = field(name("ext"), BLOB);

  // --- Task Queue Fields ---
  public static final Field<Long> TASK_ID = field(name("task_id"), BIGINT);
  public static final Field<String> TABLE_NAME = field(name("table_name"), VARCHAR);
  public static final Field<byte[]> INPUT = field(name("input"), BLOB);
  public static final Field<byte[]> OUTPUT = field(name("output"), BLOB);
  public static final Field<Integer> RETRY_COUNT = field(name("retry_count"), INTEGER);
  public static final Field<String> WORKER_ID = field(name("worker_id"), VARCHAR);
  public static final Field<Long> CLAIMED_AT = field(name("claimed_at"), BIGINT);
  public static final Field<Long> COMPLETED_AT = field(name("completed_at"), BIGINT);

  // --- Auto-increment ID ---
  public static final Field<Long> ID = field(name("id"), BIGINT);

  // --- Dynamic column factories ---

  /** Create a typed field reference for a dimension placeholder column (e.g., "dim_f01"). */
  public static Field<String> dimField(String columnName) {
    return field(name(columnName), VARCHAR);
  }

  /** Create a typed field reference for an integer agg placeholder column (e.g., "agg_i01"). */
  public static Field<Long> aggField(String columnName) {
    return field(name(columnName), BIGINT);
  }

  /** Create a typed field reference for a float agg placeholder column (e.g., "agg_f01"). */
  public static Field<Double> aggFloatField(String columnName) {
    return field(name(columnName), DOUBLE);
  }

  /** Create a jOOQ table reference from a string name. */
  public static Table<?> table(String tableName) {
    return DSL.table(name(tableName));
  }

  /** The _task_queue table. */
  public static final Table<?> TASK_QUEUE = DSL.table(name("_task_queue"));

  /** The _dim_registry table. */
  public static final Table<?> DIM_REGISTRY = DSL.table(name("_dim_registry"));

  /** The _agg_registry table. */
  public static final Table<?> AGG_REGISTRY = DSL.table(name("_agg_registry"));

  // --- SQL function helpers ---

  /** {@code UNHEX(MD5(field))} — for hash-based lookups via virtual column. */
  public static Field<byte[]> md5Hash(Field<String> f) {
    return DSL.field("UNHEX(MD5({0}))", BLOB, f);
  }

  /** {@code UNIX_TIMESTAMP()} — current database time. */
  public static Field<Long> unixTimestamp() {
    return DSL.field("UNIX_TIMESTAMP()", BIGINT);
  }
}
