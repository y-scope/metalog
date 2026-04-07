use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_schema::ColumnRegistry;
use metalog_types::FileRecord;
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
    is_mariadb: bool,
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
            is_mariadb,
            writers: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            join_set: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
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

        let table = table_name.to_string();
        let db = self.db.clone();
        let batch_size = self.batch_size;
        let flush_interval = self.flush_interval;
        let is_mariadb = self.is_mariadb;
        let registries = self.registries.clone();

        let mut js = self.join_set.lock().await;
        js.spawn(table_writer_loop(
            table,
            db,
            rx,
            registries,
            batch_size,
            flush_interval,
            is_mariadb,
        ));

        tx
    }
}

/// Per-table writer loop: batches records and flushes to DB.
async fn table_writer_loop(
    table_name: String,
    db: MySqlPool,
    mut rx: mpsc::Receiver<FileRecord>,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    batch_size: usize,
    flush_interval: Duration,
    _is_mariadb: bool,
) {
    let mut batch: Vec<FileRecord> = Vec::with_capacity(batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(table = %table_name, "table writer started");

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(rec) => {
                        batch.push(rec);
                        if batch.len() >= batch_size {
                            flush_batch(&table_name, &db, &registries, &mut batch).await;
                        }
                    }
                    None => {
                        // Channel closed — drain remaining records and exit.
                        if !batch.is_empty() {
                            flush_batch(&table_name, &db, &registries, &mut batch).await;
                        }
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&table_name, &db, &registries, &mut batch).await;
                }
            }
        }
    }

    tracing::info!(table = %table_name, "table writer stopped");
}

/// Flushes a batch of records to the database.
///
/// In a full implementation, this would:
/// 1. Resolve dim keys → physical columns via ColumnRegistry
/// 2. Call premium AggProcessor/SketchProcessor if present
/// 3. Build multi-row guarded INSERT
/// 4. Execute and notify each record via its flushed channel
///
/// Currently a placeholder that logs the batch size.
async fn flush_batch(
    table_name: &str,
    _db: &MySqlPool,
    _registries: &Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
    batch: &mut Vec<FileRecord>,
) {
    let count = batch.len();
    tracing::debug!(table = table_name, count, "flushing batch");

    // TODO: implement full UPSERT pipeline (commit 12+ will build this out)

    batch.clear();
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
}
