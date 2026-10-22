use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use metalog_metastore::{StorageMutationGuard, StoragePath};
use metalog_schema::ColumnRegistry;
use metalog_types::FileRecord;
use sqlx::{Executor, MySqlPool};
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task::JoinSet,
};

/// Default batch size (records per flush).
const DEFAULT_BATCH_SIZE: usize = 5000;

/// Default flush interval.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Channel capacity per table.
const CHANNEL_CAPACITY: usize = 5000;

/// A rejected submission or failed database commit.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("invalid table name: {0}")]
    InvalidTable(String),
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    #[error("channel full for table {0}")]
    ChannelFull(String),
    #[error("ingestion writer has stopped")]
    Stopped,
    #[error("database flush failed: {0}")]
    Flush(String),
}

/// Completion of a queued record. Success means its database statement committed.
pub struct FlushReceipt(oneshot::Receiver<Result<(), String>>);

impl FlushReceipt {
    pub async fn wait(self) -> Result<(), WriteError> {
        self.0
            .await
            .map_err(|_| WriteError::Stopped)?
            .map_err(WriteError::Flush)
    }
}

struct PendingRecord {
    record: FileRecord,
    flushed: Option<oneshot::Sender<Result<(), String>>>,
}

/// Batches file records per table and flushes to the database.
///
/// Lazily creates one tokio task per active table on first record arrival.
/// Each task batches records and flushes when:
/// - Batch size is reached (default 5000), or
/// - Timer fires (default 1 second)
///
/// Unsupported aggregation/sketch inputs are rejected during validation.
pub struct BatchingWriter {
    db: MySqlPool,
    _is_mariadb: bool,
    storage: Option<Arc<metalog_storage::Registry>>,
    writers: Arc<RwLock<HashMap<String, mpsc::Sender<PendingRecord>>>>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    join_set: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    batch_size: usize,
    flush_interval: Duration,
    stopped: AtomicBool,
}

impl BatchingWriter {
    /// Creates a new `BatchingWriter`.
    pub fn new(db: MySqlPool, is_mariadb: bool) -> Self {
        Self {
            db,
            _is_mariadb: is_mariadb,
            storage: None,
            writers: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            join_set: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            stopped: AtomicBool::new(false),
        }
    }

    /// Compatibility hook for callers that previously supplied a separate pool.
    /// Writes now use SQLx text protocol on the connection holding the storage
    /// mutation lock, so disconnecting cannot release the lock before a commit.
    pub fn with_mysql_pool(self, _pool: mysql_async::Pool) -> Self {
        tracing::warn!("separate mysql_async ingestion pool is unused; guarded writes use SQLx");
        self
    }

    /// Validates object identities against the configured backends before queueing.
    /// Keys are preserved verbatim; backends requiring canonical keys reject aliases.
    pub fn with_storage(mut self, storage: Arc<metalog_storage::Registry>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// Sets the flush interval.
    pub fn with_flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d.max(Duration::from_millis(1));
        self
    }

    /// Associates a column registry with a table.
    pub async fn set_registry(&self, table_name: &str, registry: Arc<ColumnRegistry>) {
        let mut regs = self.registries.write().await;
        regs.insert(table_name.to_string(), registry);
    }

    /// Enqueues without waiting for durability; fails immediately if the queue is full.
    pub async fn submit(&self, table_name: &str, rec: FileRecord) -> Result<(), WriteError> {
        self.enqueue(table_name, rec, false, None).await
    }

    /// Enqueues with backpressure. Use `submit_confirmed` for a commit receipt.
    pub async fn submit_wait(&self, table_name: &str, rec: FileRecord) -> Result<(), WriteError> {
        self.enqueue(table_name, rec, true, None).await
    }

    /// Enqueues a record and returns a receipt that resolves after commit or failure.
    /// Callers can enqueue a whole batch before awaiting its receipts.
    pub async fn submit_confirmed(
        &self,
        table_name: &str,
        rec: FileRecord,
        blocking: bool,
    ) -> Result<FlushReceipt, WriteError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue(table_name, rec, blocking, Some(tx)).await?;
        Ok(FlushReceipt(rx))
    }

    async fn enqueue(
        &self,
        table_name: &str,
        rec: FileRecord,
        blocking: bool,
        flushed: Option<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), WriteError> {
        metalog_db::validate_sql_identifier(table_name)
            .map_err(|e| WriteError::InvalidTable(e.to_string()))?;
        metalog_metastore::validate_file_record(&rec)
            .map_err(|e| WriteError::InvalidRecord(e.to_string()))?;
        if let Some(storage) = &self.storage {
            for (kind, backend, bucket, path) in [
                (
                    "file",
                    &rec.file_storage_backend,
                    &rec.file_bucket,
                    &rec.file_path,
                ),
                (
                    "archive",
                    &rec.archive_storage_backend,
                    &rec.archive_bucket,
                    &rec.archive_path,
                ),
            ] {
                if let Some(path) = path.as_deref().filter(|path| !path.is_empty()) {
                    storage
                        .validate_location(
                            backend.as_deref().unwrap_or_default(),
                            bucket.as_deref().unwrap_or_default(),
                            path,
                        )
                        .map_err(|error| {
                            WriteError::InvalidRecord(format!("{kind} location: {error}"))
                        })?;
                }
            }
        }
        let tx = self.get_or_create_writer(table_name).await?;
        let pending = PendingRecord {
            record: rec,
            flushed,
        };
        if blocking {
            tx.send(pending).await.map_err(|_| WriteError::Stopped)
        } else {
            tx.try_send(pending).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    WriteError::ChannelFull(table_name.to_string())
                }
                mpsc::error::TrySendError::Closed(_) => WriteError::Stopped,
            })
        }
    }

    /// Stops all writer tasks and waits for completion.
    pub async fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        // Drop all senders to signal tasks to drain and exit.
        {
            let mut writers = self.writers.write().await;
            writers.clear();
        }

        // Wait for all tasks to complete.
        let mut js = self.join_set.lock().await;
        while js.join_next().await.is_some() {}
    }

    async fn get_or_create_writer(
        &self,
        table_name: &str,
    ) -> Result<mpsc::Sender<PendingRecord>, WriteError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(WriteError::Stopped);
        }
        // Fast path: read lock.
        {
            let writers = self.writers.read().await;
            if let Some(tx) = writers.get(table_name) {
                return Ok(tx.clone());
            }
        }

        // Slow path: write lock + spawn task.
        let mut writers = self.writers.write().await;
        if self.stopped.load(Ordering::Acquire) {
            return Err(WriteError::Stopped);
        }

        // Double-check under write lock.
        if let Some(tx) = writers.get(table_name) {
            return Ok(tx.clone());
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        writers.insert(table_name.to_string(), tx.clone());

        let ctx = WriterContext {
            table_name: table_name.to_string(),
            db: self.db.clone(),
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

        Ok(tx)
    }
}

struct WriterContext {
    table_name: String,
    db: MySqlPool,
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
async fn table_writer_loop(mut ctx: WriterContext, mut rx: mpsc::Receiver<PendingRecord>) {
    let mut batch = Vec::with_capacity(ctx.batch_size);
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + ctx.flush_interval,
        ctx.flush_interval,
    );
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
async fn flush_batch(ctx: &mut WriterContext, batch: &mut Vec<PendingRecord>) {
    let count = batch.len();
    let t0 = std::time::Instant::now();
    if let Err(error) = resolve_batch_dimensions(ctx, batch).await {
        tracing::error!(table = %ctx.table_name, %error, "dimension resolution failed");
        for pending in batch.drain(..) {
            if let Some(tx) = pending.flushed {
                let _ = tx.send(Err(error.clone()));
            }
        }
        return;
    }

    // Rebuild SQL template only when dim columns change.
    if ctx.sql_prefix.is_none() {
        // Sort keys for deterministic column order. Both the SQL template
        // (INSERT column list) and value ordering (dim_keys) must use the
        // same order — a single sorted iteration guarantees this.
        let mut sorted_keys: Vec<String> = ctx.dim_cache.keys().cloned().collect();
        sorted_keys.sort();
        let dim_cols: Vec<(&str, &str)> = sorted_keys
            .iter()
            .map(|k| (k.as_str(), ctx.dim_cache[k].as_str()))
            .collect();
        let (prefix, suffix) = build_sql_template(&ctx.table_name, &dim_cols);
        ctx.sql_prefix = Some(prefix);
        ctx.sql_suffix = Some(suffix);
        ctx.dim_keys = sorted_keys;
    }
    let sql_prefix = ctx.sql_prefix.as_deref().unwrap();
    let sql_suffix = ctx.sql_suffix.as_deref().unwrap();
    let t_resolve = t0.elapsed();

    // Statements commit independently. A failed chunk rejects its own records;
    // previously committed chunks retain their success receipts.
    for chunk in batch.chunks_mut(MAX_ROWS_PER_INSERT) {
        let paths = chunk
            .iter()
            .flat_map(|pending| {
                let rec = &pending.record;
                [
                    (&rec.file_storage_backend, &rec.file_bucket, &rec.file_path),
                    (
                        &rec.archive_storage_backend,
                        &rec.archive_bucket,
                        &rec.archive_path,
                    ),
                ]
                .into_iter()
                .filter_map(|(backend, bucket, path)| {
                    path.as_ref()
                        .filter(|path| !path.is_empty())
                        .map(|path| StoragePath {
                            backend: backend.clone().unwrap_or_default(),
                            bucket: bucket.clone().unwrap_or_default(),
                            path: path.clone(),
                        })
                })
                .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let result: Result<(), String> = async {
            let mut guard = StorageMutationGuard::acquire(&ctx.db)
                .await
                .map_err(|e| e.to_string())?;
            let result = async {
                guard
                    .ensure_writable(&paths)
                    .await
                    .map_err(|e| e.to_string())?;
                execute_upsert_into(
                    guard.connection(),
                    chunk,
                    &ctx.dim_keys,
                    sql_prefix,
                    sql_suffix,
                    &mut ctx.sql_buf,
                )
                .await
                .map_err(|e| e.to_string())
            }
            .await;
            // A cleanup failure must not turn a committed write into an ambiguous
            // retry. The guard closes its connection if explicit release fails.
            if let Err(error) = guard.release().await {
                tracing::error!(%error, "failed to release storage mutation guard");
            }
            result
        }
        .await;
        if let Err(ref error) = result {
            tracing::error!(table = %ctx.table_name, %error, "upsert failed");
        }
        for pending in chunk {
            if let Some(tx) = pending.flushed.take() {
                let _ = tx.send(result.clone());
            }
        }
    }
    tracing::info!(table = %ctx.table_name, count, resolve_us = t_resolve.as_micros(),
        total_ms = t0.elapsed().as_millis(), "flush complete");
    batch.clear();
}

/// Resolve every dimension used by the batch. Allocation errors reject the batch
/// instead of committing records with silently missing dimensions.
async fn resolve_batch_dimensions(
    ctx: &mut WriterContext,
    batch: &[PendingRecord],
) -> Result<(), String> {
    let mut requests: HashMap<&str, (&str, i32)> = HashMap::new();
    for pending in batch {
        for meta in &pending.record.dim_meta {
            match requests.get_mut(meta.key.as_str()) {
                Some((base_type, width)) => {
                    if *base_type != meta.base_type {
                        return Err(format!("inconsistent types for dimension {}", meta.key));
                    }
                    *width = (*width).max(meta.width);
                }
                None => {
                    requests.insert(&meta.key, (&meta.base_type, meta.width));
                }
            }
        }
    }
    if requests.is_empty() {
        return Ok(());
    }
    let existing = ctx.registries.read().await.get(&ctx.table_name).cloned();
    let registry = match existing {
        Some(registry) => registry,
        None => {
            let registry = Arc::new(
                ColumnRegistry::new(ctx.db.clone(), &ctx.table_name)
                    .await
                    .map_err(|e| e.to_string())?,
            );
            ctx.registries
                .write()
                .await
                .insert(ctx.table_name.clone(), registry.clone());
            registry
        }
    };
    registry
        .refresh_aliases()
        .await
        .map_err(|e| e.to_string())?;
    let mut requests: Vec<_> = requests.into_iter().collect();
    requests.sort_by_key(|(key, _)| *key);
    for (key, (base_type, width)) in requests {
        let column = registry
            .resolve_or_allocate_dim(key, base_type, width)
            .await
            .map_err(|e| format!("dimension {key}: {e}"))?;
        metalog_db::validate_sql_identifier(&column).map_err(|e| e.to_string())?;
        if ctx.dim_cache.get(key) != Some(&column) {
            ctx.dim_cache.insert(key.to_string(), column);
            ctx.sql_prefix = None;
        }
    }
    Ok(())
}

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

    let extra_cols = dim_col_strings
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>();
    // VALUES() is supported by both MariaDB and MySQL (including MySQL 8).
    let suffix = format!(
        " {}",
        metalog_metastore::build_guarded_update(
            metalog_metastore::GUARDED_UPDATE_COLUMNS,
            &extra_cols,
            true,
        )
    );

    (prefix, suffix)
}

/// Builds and executes a multi-row INSERT, reusing `sql_buf` across calls.
///
/// Builds directly into one buffer: prefix → VALUES rows → suffix.
/// No intermediate allocation. The buffer is `.clear()`ed but retains capacity.
async fn execute_upsert_into(
    connection: &mut sqlx::MySqlConnection,
    records: &[PendingRecord],
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
    for (i, pending) in records.iter().enumerate() {
        let rec = &pending.record;
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

    // COM_QUERY keeps the single-round-trip path on the SAME connection that
    // checked tombstones and owns the advisory lock. A separate execution
    // connection could commit after cancellation has released that lock.
    connection.execute(sql_buf.as_str()).await?;

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

/// Writes a UTF-8 hex literal, independent of NO_BACKSLASH_ESCAPES and connection
/// string escaping modes. Metadata remains data even when it contains SQL syntax.
fn write_escaped(buf: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    buf.push_str(",CONVERT(X'");
    for byte in s.bytes() {
        buf.push(HEX[(byte >> 4) as usize] as char);
        buf.push(HEX[(byte & 15) as usize] as char);
    }
    buf.push_str("' USING utf8mb4)");
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
        let err = WriteError::ChannelFull("test_table".into());
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
    fn text_literals_do_not_depend_on_sql_mode() {
        let mut buf = String::new();
        write_escaped(&mut buf, "a'\\é");
        assert_eq!(buf, ",CONVERT(X'61275cc3a9' USING utf8mb4)");
        let mut empty = String::new();
        write_escaped(&mut empty, "");
        assert_eq!(empty, ",CONVERT(X'' USING utf8mb4)");
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

    #[test]
    fn build_template_dim_column_order_matches_sorted_keys() {
        // Dim columns must appear in the same order in the INSERT column list
        // and the VALUES rows. Using a single sorted iteration guarantees this.
        // Pass dims in reverse alphabetical order by logical key;
        // build_sql_template should place physical columns in the order given.
        let dims = vec![("zone", "dim_f02"), ("host", "dim_f01")];
        let (prefix, _suffix) = build_sql_template("test_table", &dims);
        // dim_f02 (zone) should appear before dim_f01 (host) in the column list
        // because that's the order we passed them.
        let f02_pos = prefix.find("dim_f02").unwrap();
        let f01_pos = prefix.find("dim_f01").unwrap();
        assert!(
            f02_pos < f01_pos,
            "dim columns should appear in the order passed to build_sql_template"
        );
    }
}
