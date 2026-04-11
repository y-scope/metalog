use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_schema::ColumnRegistry;
use metalog_types::FileRecord;
use mysql_async::prelude::Queryable;
use sqlx::MySqlPool;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinSet,
};

/// Default batch size (records per flush).
const DEFAULT_BATCH_SIZE: usize = 5000;

/// Default flush interval.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Channel capacity per table.
const CHANNEL_CAPACITY: usize = 5000;

/// Error returned when the per-table channel is full (non-blocking submit).
#[derive(Debug, thiserror::Error)]
#[error("channel full for table {0}")]
pub struct ChannelFullError(pub String);

/// Batches file records per table and flushes to the database.
///
/// Lazily creates one tokio task per active table on first record arrival.
/// Each task batches records and flushes when:
/// - Batch size is reached (default 5000), or
/// - Timer fires (default 1 second)
///
/// Premium processors (aggs, sketches) are injected as `Option<Arc<dyn T>>`
/// and called during the flush pipeline if present.
pub struct BatchingWriter {
    db: MySqlPool,
    /// mysql_async pool for high-throughput UPSERT (text protocol, single RT).
    mysql_pool: Option<mysql_async::Pool>,
    _is_mariadb: bool,
    writers: Arc<RwLock<HashMap<String, mpsc::Sender<FileRecord>>>>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    join_set: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    batch_size: usize,
    flush_interval: Duration,
}

impl BatchingWriter {
    /// Creates a new `BatchingWriter`.
    pub fn new(db: MySqlPool, is_mariadb: bool) -> Self {
        Self {
            db,
            mysql_pool: None,
            _is_mariadb: is_mariadb,
            writers: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            join_set: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    /// Sets a mysql_async pool for high-throughput UPSERT via text protocol.
    pub fn with_mysql_pool(mut self, pool: mysql_async::Pool) -> Self {
        self.mysql_pool = Some(pool);
        self
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Sets the flush interval.
    pub fn with_flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    /// Associates a column registry with a table.
    pub async fn set_registry(&self, table_name: &str, registry: Arc<ColumnRegistry>) {
        let mut regs = self.registries.write().await;
        regs.insert(table_name.to_string(), registry);
    }

    /// Non-blocking submit. Returns `ChannelFullError` if the channel is full.
    pub async fn submit(&self, table_name: &str, rec: FileRecord) -> Result<(), ChannelFullError> {
        let tx = self.get_or_create_writer(table_name).await;
        tx.try_send(rec)
            .map_err(|_| ChannelFullError(table_name.to_string()))
    }

    /// Blocking submit. Waits until channel has space or context is cancelled.
    pub async fn submit_wait(
        &self,
        table_name: &str,
        rec: FileRecord,
    ) -> Result<(), ChannelFullError> {
        let tx = self.get_or_create_writer(table_name).await;
        tx.send(rec)
            .await
            .map_err(|_| ChannelFullError(table_name.to_string()))
    }

    /// Stops all writer tasks and waits for completion.
    pub async fn stop(&self) {
        // Drop all senders to signal tasks to drain and exit.
        {
            let mut writers = self.writers.write().await;
            writers.clear();
        }

        // Wait for all tasks to complete.
        let mut js = self.join_set.lock().await;
        while js.join_next().await.is_some() {}
    }

    async fn get_or_create_writer(&self, table_name: &str) -> mpsc::Sender<FileRecord> {
        // Fast path: read lock.
        {
            let writers = self.writers.read().await;
            if let Some(tx) = writers.get(table_name) {
                return tx.clone();
            }
        }

        // Slow path: write lock + spawn task.
        let mut writers = self.writers.write().await;

        // Double-check under write lock.
        if let Some(tx) = writers.get(table_name) {
            return tx.clone();
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        writers.insert(table_name.to_string(), tx.clone());

        let ctx = WriterContext {
            table_name: table_name.to_string(),
            db: self.db.clone(),
            mysql_pool: self.mysql_pool.clone(),
            registries: self.registries.clone(),
            dim_cache: HashMap::new(),
            dim_keys: Vec::new(),
            sql_prefix: None,
            sql_suffix: None,
            sql_buf: String::new(),
            batch_size: self.batch_size,
            flush_interval: self.flush_interval,
        };

        let mut js = self.join_set.lock().await;
        js.spawn(table_writer_loop(ctx, rx));

        tx
    }
}

struct WriterContext {
    table_name: String,
    db: MySqlPool,
    mysql_pool: Option<mysql_async::Pool>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    /// Cached dim key → physical column mappings (persists across flushes).
    dim_cache: HashMap<String, String>,
    /// Cached ordered dim keys (rebuilt when dim_cache changes).
    dim_keys: Vec<String>,
    /// Cached SQL template: INSERT prefix + ON DUPLICATE KEY UPDATE suffix.
    /// Rebuilt only when dim columns change.
    sql_prefix: Option<String>,
    sql_suffix: Option<String>,
    /// Reusable buffer for building the full SQL statement across flushes.
    sql_buf: String,
    batch_size: usize,
    flush_interval: Duration,
}

/// Per-table writer loop: batches records and flushes to DB.
#[allow(clippy::too_many_arguments)]
async fn table_writer_loop(mut ctx: WriterContext, mut rx: mpsc::Receiver<FileRecord>) {
    let mut batch = Vec::with_capacity(ctx.batch_size);
    let mut tick = tokio::time::interval(ctx.flush_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(table = %ctx.table_name, "table writer started");

    loop {
        tokio::select! {
            n = rx.recv_many(&mut batch, ctx.batch_size) => {
                if n == 0 {
                    // Channel closed — flush remaining and exit.
                    if !batch.is_empty() {
                        flush_batch(&mut ctx, &mut batch).await;
                    }
                    break;
                }
                if batch.len() >= ctx.batch_size {
                    flush_batch(&mut ctx, &mut batch).await;
                }
            }
            _ = tick.tick() => {
                if !batch.is_empty() {
                    flush_batch(&mut ctx, &mut batch).await;
                }
            }
        }
    }

    tracing::info!(table = %ctx.table_name, "table writer stopped");
}

/// Maximum rows per INSERT statement. Matches Go's MaxMultiRowInsert = 1000.
const MAX_ROWS_PER_INSERT: usize = 1000;

/// Flushes a batch of records to the database via multi-row guarded UPSERT.
///
/// 1. Resolves dim keys → physical columns via ColumnRegistry (if available)
/// 2. Builds multi-row INSERT with guarded ON DUPLICATE KEY UPDATE
/// 3. Executes in chunks of 1000 rows
async fn flush_batch(ctx: &mut WriterContext, batch: &mut Vec<FileRecord>) {
    let count = batch.len();
    let t0 = std::time::Instant::now();

    // Resolve NEW dim keys only (check first record for new keys).
    // After warmup, the cache hit rate is 100% and this loop is skipped.
    let mut dims_changed = false;
    if let Some(first) = batch.first() {
        let has_new_keys = first
            .dim_meta
            .iter()
            .any(|m| !ctx.dim_cache.contains_key(&m.key));
        if has_new_keys {
            // Get registry from cache, or lazy-load from DB if not yet set
            // (e.g. before the reconciliation loop assigns this table).
            let registry = {
                let regs = ctx.registries.read().await;
                regs.get(&ctx.table_name).cloned()
            };
            let registry = if registry.is_none() {
                match ColumnRegistry::new(ctx.db.clone(), &ctx.table_name).await {
                    Ok(reg) => {
                        let reg = Arc::new(reg);
                        ctx.registries
                            .write()
                            .await
                            .insert(ctx.table_name.clone(), Arc::clone(&reg));
                        Some(reg)
                    }
                    Err(e) => {
                        tracing::warn!(
                            table = %ctx.table_name, error = %e, "failed to lazy-load registry"
                        );
                        None
                    }
                }
            } else {
                registry
            };
            if let Some(reg) = &registry {
                for meta in &first.dim_meta {
                    if !ctx.dim_cache.contains_key(&meta.key) {
                        match reg
                            .resolve_or_allocate_dim(&meta.key, &meta.base_type, meta.width)
                            .await
                        {
                            Ok(col) => {
                                ctx.dim_cache.insert(meta.key.clone(), col);
                                ctx.dim_keys.push(meta.key.clone());
                                dims_changed = true;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    dim_key = %meta.key, error = %e, "dim alloc failed"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Rebuild SQL template only when dim columns change.
    // dim_keys is maintained incrementally above (pushed on each new dim).
    if dims_changed || ctx.sql_prefix.is_none() {
        let dim_cols: Vec<(&str, &str)> = ctx
            .dim_cache
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (prefix, suffix) = build_sql_template(&ctx.table_name, &dim_cols);
        ctx.sql_prefix = Some(prefix);
        ctx.sql_suffix = Some(suffix);
    }
    let sql_prefix = ctx.sql_prefix.as_deref().unwrap();
    let sql_suffix = ctx.sql_suffix.as_deref().unwrap();
    let t_resolve = t0.elapsed();

    // Build and execute INSERT in chunks, reusing sql_buf across calls.
    for chunk in batch.chunks(MAX_ROWS_PER_INSERT) {
        if let Err(e) = execute_upsert_into(
            &ctx.db,
            &ctx.mysql_pool,
            chunk,
            &ctx.dim_keys,
            sql_prefix,
            sql_suffix,
            &mut ctx.sql_buf,
        )
        .await
        {
            tracing::error!(table = &ctx.table_name, error = %e, "upsert failed");
        }
    }
    let t_total = t0.elapsed();

    tracing::info!(
        table = &ctx.table_name,
        count,
        resolve_us = t_resolve.as_micros(),
        total_ms = t_total.as_millis(),
        "flush complete"
    );

    batch.clear();
}

/// Builds and executes a multi-row INSERT using text protocol (COM_QUERY).
///
/// When a `mysql_async::Pool` is provided, uses it for optimal throughput
/// (native text protocol, single round-trip). Falls back to sqlx otherwise.
/// Builds the SQL template (prefix + suffix) for UPSERT.
/// Only needs to be rebuilt when dim columns change.
fn build_sql_template(table_name: &str, dim_mappings: &[(&str, &str)]) -> (String, String) {
    let mut col_names: Vec<&str> = vec![
        "min_timestamp",
        "max_timestamp",
        "state",
        "file_storage_backend",
        "file_bucket",
        "file_path",
        "file_size_bytes",
        "archive_storage_backend",
        "archive_bucket",
        "archive_path",
        "archive_size_bytes",
        "archive_created_at",
        "raw_size_bytes",
        "record_count",
        "retention_days",
        "expires_at",
    ];

    let dim_col_strings: Vec<&str> = dim_mappings.iter().map(|(_, col)| *col).collect();
    for col in &dim_col_strings {
        col_names.push(*col);
    }

    let cols_sql = col_names
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");

    let prefix = format!("INSERT INTO `{table_name}` ({cols_sql}) VALUES ");

    // Guarded ON DUPLICATE KEY UPDATE.
    let guard_states = metalog_types::FileState::upsert_guard_states()
        .iter()
        .map(|s| format!("'{}'", s.as_db_str()))
        .collect::<Vec<_>>()
        .join(",");

    let update_cols: &[&str] = &[
        "max_timestamp",
        "state",
        "file_storage_backend",
        "file_bucket",
        "file_size_bytes",
        "archive_storage_backend",
        "archive_bucket",
        "archive_path",
        "archive_size_bytes",
        "archive_created_at",
        "raw_size_bytes",
        "record_count",
    ];

    let mut update_parts: Vec<String> = Vec::new();
    for col in update_cols {
        update_parts.push(format!(
            "`{col}` = IF(`state` NOT IN ({guard_states}) AND VALUES(`max_timestamp`) > \
             `max_timestamp`, VALUES(`{col}`), `{col}`)"
        ));
    }
    for col in &dim_col_strings {
        update_parts.push(format!(
            "`{col}` = IF(`state` NOT IN ({guard_states}) AND VALUES(`max_timestamp`) > \
             `max_timestamp`, VALUES(`{col}`), `{col}`)"
        ));
    }

    let suffix = format!(" ON DUPLICATE KEY UPDATE {}", update_parts.join(", "));

    (prefix, suffix)
}

/// Builds and executes a multi-row INSERT, reusing `sql_buf` across calls.
///
/// Builds directly into one buffer: prefix → VALUES rows → suffix.
/// No intermediate allocation. The buffer is `.clear()`ed but retains capacity.
async fn execute_upsert_into(
    db: &MySqlPool,
    mysql_pool: &Option<mysql_async::Pool>,
    records: &[FileRecord],
    dim_keys: &[String],
    sql_prefix: &str,
    sql_suffix: &str,
    sql_buf: &mut String,
) -> Result<(), sqlx::Error> {
    if records.is_empty() {
        return Ok(());
    }

    let build_start = std::time::Instant::now();

    // Clear but keep allocated capacity from previous flush.
    sql_buf.clear();

    // Reserve: prefix + ~200 bytes/row + suffix.
    let estimated = sql_prefix.len() + records.len() * 200 + sql_suffix.len();
    if sql_buf.capacity() < estimated {
        sql_buf.reserve(estimated - sql_buf.capacity());
    }

    // Push prefix directly (no format! allocation).
    sql_buf.push_str(sql_prefix);

    // Build VALUES rows with itoa for integers.
    let mut itoa_buf = itoa::Buffer::new();
    for (i, rec) in records.iter().enumerate() {
        if i > 0 {
            sql_buf.push(',');
        }
        sql_buf.push('(');
        sql_buf.push_str(itoa_buf.format(rec.min_timestamp));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.max_timestamp));
        write_escaped(sql_buf, rec.state.as_db_str());
        write_escaped_opt(sql_buf, rec.file_storage_backend.as_deref());
        write_escaped_opt(sql_buf, rec.file_bucket.as_deref());
        write_escaped_opt(sql_buf, rec.file_path.as_deref());
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.file_size_bytes));
        write_escaped_opt(sql_buf, rec.archive_storage_backend.as_deref());
        write_escaped_opt(sql_buf, rec.archive_bucket.as_deref());
        write_escaped_opt(sql_buf, rec.archive_path.as_deref());
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.archive_size_bytes));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.archive_created_at));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.raw_size_bytes));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.record_count));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.retention_days));
        sql_buf.push(',');
        sql_buf.push_str(itoa_buf.format(rec.expires_at));
        for logical_key in dim_keys {
            match rec.dims.get(logical_key.as_str()) {
                Some(serde_json::Value::String(s)) => write_escaped(sql_buf, s),
                Some(serde_json::Value::Number(n)) => {
                    sql_buf.push(',');
                    // itoa for integers, fallback to Display for floats.
                    if let Some(v) = n.as_i64() {
                        sql_buf.push_str(itoa_buf.format(v));
                    } else {
                        use std::fmt::Write;
                        write!(sql_buf, "{n}").unwrap();
                    }
                }
                Some(serde_json::Value::Bool(b)) => {
                    sql_buf.push(',');
                    sql_buf.push(if *b { '1' } else { '0' });
                }
                _ => sql_buf.push_str(",NULL"),
            }
        }
        sql_buf.push(')');
    }

    // Push suffix directly.
    sql_buf.push_str(sql_suffix);

    let sql_len = sql_buf.len();
    let build_elapsed = build_start.elapsed();
    let exec_start = std::time::Instant::now();

    // Use mysql_async for native text protocol if available, else sqlx.
    if let Some(pool) = mysql_pool {
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        conn.query_drop(sql_buf.as_str())
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    } else {
        sqlx::query(sql_buf.as_str()).execute(db).await?;
    }

    let exec_elapsed = exec_start.elapsed();
    tracing::info!(
        rows = records.len(),
        sql_kb = sql_len / 1024,
        build_us = build_elapsed.as_micros(),
        exec_ms = exec_elapsed.as_millis(),
        "upsert"
    );
    Ok(())
}

/// Writes `,'{escaped_value}'` into the buffer.
/// Escapes all MySQL-special characters per the MySQL C API `mysql_real_escape_string`.
///
/// Fast path: if the string contains no special characters (common for hostnames,
/// service names, paths), copies it in bulk without per-byte branching.
fn write_escaped(buf: &mut String, s: &str) {
    buf.push_str(",'");
    // Fast path: scan for special bytes. Most metadata strings are clean ASCII.
    let needs_escape = s
        .bytes()
        .any(|b| matches!(b, b'\'' | b'\\' | b'\0' | b'\n' | b'\r' | 0x1a));
    if !needs_escape {
        buf.push_str(s);
    } else {
        for c in s.chars() {
            match c {
                '\'' => buf.push_str("\\'"),
                '\\' => buf.push_str("\\\\"),
                '\0' => buf.push_str("\\0"),
                '\n' => buf.push_str("\\n"),
                '\r' => buf.push_str("\\r"),
                '\x1a' => buf.push_str("\\Z"),
                _ => buf.push(c),
            }
        }
    }
    buf.push('\'');
}

/// Writes `,'{escaped}'` or `,NULL` for an optional string.
fn write_escaped_opt(buf: &mut String, opt: Option<&str>) {
    match opt {
        Some(s) if !s.is_empty() => write_escaped(buf, s),
        _ => buf.push_str(",NULL"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_full_error_display() {
        let err = ChannelFullError("test_table".into());
        assert!(err.to_string().contains("test_table"));
    }

    #[test]
    fn default_batch_size() {
        assert_eq!(DEFAULT_BATCH_SIZE, 5000);
    }

    #[test]
    fn default_flush_interval() {
        assert_eq!(DEFAULT_FLUSH_INTERVAL, Duration::from_secs(1));
    }

    #[test]
    fn channel_capacity() {
        assert_eq!(CHANNEL_CAPACITY, 5000);
    }

    #[test]
    fn write_escaped_fast_path() {
        let mut buf = String::new();
        write_escaped(&mut buf, "hello-world_123");
        assert_eq!(buf, ",'hello-world_123'");
    }

    #[test]
    fn write_escaped_special_chars() {
        let mut buf = String::new();
        write_escaped(&mut buf, "it's a \"test\"\nwith\\stuff");
        assert_eq!(buf, ",'it\\'s a \"test\"\\nwith\\\\stuff'");
    }

    #[test]
    fn write_escaped_opt_null() {
        let mut buf = String::new();
        write_escaped_opt(&mut buf, None);
        assert_eq!(buf, ",NULL");

        let mut buf2 = String::new();
        write_escaped_opt(&mut buf2, Some(""));
        assert_eq!(buf2, ",NULL");
    }

    #[test]
    fn sql_buffer_reuse() {
        let mut buf = String::with_capacity(1024);
        buf.push_str("first use");
        assert!(buf.capacity() >= 1024);
        buf.clear();
        // Capacity is retained after clear.
        assert!(buf.capacity() >= 1024);
        buf.push_str("second use");
        assert_eq!(buf, "second use");
    }

    #[test]
    fn build_template_with_dims() {
        let dims = vec![("host", "dim_f01"), ("zone", "dim_f02")];
        let (prefix, suffix) = build_sql_template("test_table", &dims);
        assert!(prefix.contains("INSERT INTO `test_table`"));
        assert!(prefix.contains("`dim_f01`"));
        assert!(prefix.contains("`dim_f02`"));
        assert!(suffix.contains("ON DUPLICATE KEY UPDATE"));
    }
}
