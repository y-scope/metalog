use std::{collections::HashMap, sync::Arc};

use metalog_metastore::AdvisoryLock;
use metalog_types::{
    agg::agg_cache_key,
    column::{AGG_COLUMN_PREFIX, MAX_AGG_SLOTS},
};
use sqlx::MySqlPool;
use tokio::sync::RwLock;

/// Agg registry entry from `_agg_registry`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AggRegistryEntry {
    pub table_name: String,
    pub column_name: String,
    pub agg_key: String,
    #[sqlx(default)]
    pub agg_value: Option<String>,
    pub aggregation_type: String,
    pub value_type: String,
    #[sqlx(rename = "alias_column")]
    pub alias_col: Option<String>,
    #[sqlx(rename = "state")]
    pub status: String,
}

/// Maps physical `agg_fNN` columns to logical aggregation keys.
///
/// Thread-safe via [`RwLock`]. Mirrors ColumnRegistry's fast-path/slow-path
/// pattern for dim columns.
pub struct AggRegistry {
    db: MySqlPool,
    table_name: String,
    inner: Arc<RwLock<AggInner>>,
}

struct AggInner {
    /// Composite cache key → physical column name.
    cache: HashMap<String, String>,
    entries: Vec<AggRegistryEntry>,
    next_slot: u32,
}

/// Errors from agg registry operations.
#[derive(Debug, thiserror::Error)]
pub enum AggRegistryError {
    #[error("agg slot exhausted: all {MAX_AGG_SLOTS} slots in use")]
    SlotExhausted,

    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("advisory lock: {0}")]
    Lock(#[from] metalog_metastore::AdvisoryLockError),
}

impl AggRegistry {
    /// Creates a new `AggRegistry`, loading all ACTIVE entries.
    pub async fn new(db: MySqlPool, table_name: &str) -> Result<Self, AggRegistryError> {
        let entries = load_agg_entries(&db, table_name).await?;

        let mut cache = HashMap::new();
        let mut max_slot: u32 = 0;

        for entry in &entries {
            let key = agg_cache_key(
                &entry.agg_key,
                entry.agg_value.as_deref().unwrap_or(""),
                &entry.aggregation_type,
            );
            cache.insert(key, entry.column_name.clone());
            if let Some(slot) = parse_agg_slot(&entry.column_name) {
                max_slot = max_slot.max(slot);
            }
        }

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            inner: Arc::new(RwLock::new(AggInner {
                cache,
                entries,
                next_slot: max_slot + 1,
            })),
        })
    }

    /// Fast-path agg lookup.
    pub async fn resolve_agg(
        &self,
        agg_key: &str,
        agg_value: &str,
        agg_type: &str,
    ) -> Option<String> {
        let key = agg_cache_key(agg_key, agg_value, agg_type);
        let inner = self.inner.read().await;
        inner.cache.get(&key).cloned()
    }

    /// Resolves or allocates an aggregation column.
    pub async fn resolve_or_allocate_agg(
        &self,
        agg_key: &str,
        agg_value: &str,
        agg_type: &str,
        value_type: &str,
    ) -> Result<String, AggRegistryError> {
        let cache_key = agg_cache_key(agg_key, agg_value, agg_type);

        // Fast path.
        {
            let inner = self.inner.read().await;
            if let Some(col) = inner.cache.get(&cache_key) {
                return Ok(col.clone());
            }
        }

        // Slow path: advisory lock + allocate.
        let lock_name = format!("metalog_agg_alloc_{}", self.table_name);
        let mut advisory = AdvisoryLock::acquire(&self.db, &lock_name, 10).await?;

        // Re-check after lock.
        let fresh = load_agg_entries(&self.db, &self.table_name).await?;
        let found = fresh
            .iter()
            .find(|entry| {
                let k = agg_cache_key(
                    &entry.agg_key,
                    entry.agg_value.as_deref().unwrap_or(""),
                    &entry.aggregation_type,
                );
                k == cache_key
            })
            .map(|e| e.column_name.clone());

        if let Some(col_name) = found {
            let mut inner = self.inner.write().await;
            inner.cache.insert(cache_key, col_name.clone());
            inner.entries = fresh;
            advisory.release().await?;
            return Ok(col_name);
        }

        // Allocate new slot.
        let mut inner = self.inner.write().await;
        let slot = inner.next_slot;
        if slot > MAX_AGG_SLOTS {
            advisory.release().await?;
            return Err(AggRegistryError::SlotExhausted);
        }

        let col_name = format!("{AGG_COLUMN_PREFIX}{slot:02}");
        let sql_type = if value_type == "FLOAT" {
            "DOUBLE"
        } else {
            "BIGINT"
        };

        // ALTER TABLE ADD COLUMN.
        let alter = format!(
            "ALTER TABLE `{}` ADD COLUMN `{col_name}` {sql_type} NULL",
            self.table_name
        );
        match sqlx::query(&alter).execute(&self.db).await {
            Ok(_) => {}
            Err(e) if metalog_db::is_duplicate_column(&e) => {}
            Err(e) => {
                advisory.release().await?;
                return Err(AggRegistryError::Sql(e));
            }
        }

        // INSERT into _agg_registry.
        let now = metalog_timeutil::epoch_nanos();
        sqlx::query(
            "INSERT INTO _agg_registry (table_name, column_name, agg_key, agg_value, \
             aggregation_type, value_type, state, created_at) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', \
             ?)",
        )
        .bind(&self.table_name)
        .bind(&col_name)
        .bind(agg_key)
        .bind(if agg_value.is_empty() {
            None
        } else {
            Some(agg_value)
        })
        .bind(agg_type)
        .bind(value_type)
        .bind(now)
        .execute(&self.db)
        .await?;

        inner.cache.insert(cache_key, col_name.clone());
        inner.next_slot = slot + 1;
        inner.entries = load_agg_entries(&self.db, &self.table_name).await?;

        advisory.release().await?;

        tracing::info!(
            table = %self.table_name,
            agg_key,
            agg_value,
            agg_type,
            column = %col_name,
            "allocated agg column"
        );

        Ok(col_name)
    }

    /// Returns all active agg entries.
    pub async fn all_entries(&self) -> Vec<AggRegistryEntry> {
        let inner = self.inner.read().await;
        inner.entries.clone()
    }

    /// Returns active agg column names.
    pub async fn active_columns(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        inner
            .entries
            .iter()
            .map(|e| e.column_name.clone())
            .collect()
    }

    /// Returns columns that store FLOAT values (for UPSERT type handling).
    pub async fn float_columns(&self) -> HashMap<String, bool> {
        let inner = self.inner.read().await;
        inner
            .entries
            .iter()
            .filter(|e| e.value_type == "FLOAT")
            .map(|e| (e.column_name.clone(), true))
            .collect()
    }
}

async fn load_agg_entries(
    db: &MySqlPool,
    table_name: &str,
) -> Result<Vec<AggRegistryEntry>, sqlx::Error> {
    sqlx::query_as::<_, AggRegistryEntry>(
        "SELECT table_name, column_name, agg_key, agg_value, aggregation_type, value_type, \
         alias_column, state FROM _agg_registry WHERE table_name = ? AND state = 'ACTIVE' ORDER \
         BY column_name",
    )
    .bind(table_name)
    .fetch_all(db)
    .await
}

fn parse_agg_slot(col_name: &str) -> Option<u32> {
    col_name
        .strip_prefix(AGG_COLUMN_PREFIX)
        .and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_slot() {
        assert_eq!(parse_agg_slot("agg_f01"), Some(1));
        assert_eq!(parse_agg_slot("agg_f99"), Some(99));
        assert_eq!(parse_agg_slot("dim_f01"), None);
    }

    #[test]
    fn cache_key_format() {
        let key = agg_cache_key("level", "error", "EQ");
        assert_eq!(key, "EQ\0level\0error");
    }

    #[test]
    fn slot_exhaustion() {
        let err = AggRegistryError::SlotExhausted;
        assert!(err.to_string().contains("99"));
    }
}
