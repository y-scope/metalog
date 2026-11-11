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
    preallocated: bool,
}

struct AggInner {
    /// Composite cache key → physical column name.
    cache: HashMap<String, String>,
    entries: Vec<AggRegistryEntry>,
}

/// Errors from agg registry operations.
#[derive(Debug, thiserror::Error)]
pub enum AggRegistryError {
    #[error("invalid aggregation definition: {0}")]
    InvalidDefinition(String),

    #[error(
        "no reserved {0} aggregation column available; table capacity requires a planned migration"
    )]
    ReserveExhausted(String),
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
        metalog_db::validate_sql_identifier(table_name)
            .map_err(|e| AggRegistryError::InvalidDefinition(e.to_string()))?;
        let entries = load_agg_entries(&db, table_name).await?;
        let preallocated = metalog_metastore::has_column_reserves(&db, table_name).await?;

        let mut cache = HashMap::new();

        for entry in &entries {
            let key = agg_cache_key(
                &entry.agg_key,
                entry.agg_value.as_deref().unwrap_or(""),
                &entry.aggregation_type,
            );
            cache.insert(key, entry.column_name.clone());
        }

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            inner: Arc::new(RwLock::new(AggInner { cache, entries })),
            preallocated,
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
        if agg_key.is_empty()
            || agg_key.chars().count() > 1024
            || agg_value.chars().count() > 1024
            || !matches!(
                agg_type,
                "EQ" | "GTE" | "GT" | "LTE" | "LT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
            || !matches!(value_type, "INT" | "FLOAT")
        {
            return Err(AggRegistryError::InvalidDefinition(
                "invalid key, aggregation type or value type".into(),
            ));
        }
        let cache_key = agg_cache_key(agg_key, agg_value, agg_type);

        // Fast path.
        {
            let inner = self.inner.read().await;
            if let Some(col) = inner.cache.get(&cache_key) {
                if inner
                    .entries
                    .iter()
                    .any(|e| e.column_name == *col && e.value_type != value_type)
                {
                    return Err(AggRegistryError::InvalidDefinition(
                        "aggregation value type cannot change".into(),
                    ));
                }
                return Ok(col.clone());
            }
        }

        // Slow path: advisory lock + allocate.
        let mut advisory =
            AdvisoryLock::acquire_scoped(&self.db, "agg_alloc", &self.table_name, 10).await?;

        // Re-check after lock.
        let fresh: Vec<AggRegistryEntry> = sqlx::query_as(
            "SELECT table_name, column_name, agg_key, agg_value, aggregation_type, value_type, \
             alias_column, state FROM _agg_registry WHERE table_name = ? ORDER BY column_name",
        )
        .bind(&self.table_name)
        .fetch_all(advisory.connection())
        .await?;
        let found = fresh
            .iter()
            .find(|entry| {
                let k = agg_cache_key(
                    &entry.agg_key,
                    entry.agg_value.as_deref().unwrap_or(""),
                    &entry.aggregation_type,
                );
                entry.status == "ACTIVE" && k == cache_key
            })
            .map(|e| e.column_name.clone());

        if let Some(col_name) = found {
            if fresh
                .iter()
                .any(|e| e.column_name == col_name && e.value_type != value_type)
            {
                return Err(AggRegistryError::InvalidDefinition(
                    "aggregation value type cannot change".into(),
                ));
            }
            self.replace_entries(fresh).await;
            advisory.release().await?;
            return Ok(col_name);
        }

        if self.preallocated {
            let available: Vec<_> = fresh
                .iter()
                .filter(|entry| entry.status == "AVAILABLE" && entry.value_type == value_type)
                .collect();
            let entry = available
                .first()
                .ok_or_else(|| AggRegistryError::ReserveExhausted(value_type.into()))?;
            let col_name = entry.column_name.clone();
            sqlx::query(
                "UPDATE _agg_registry SET agg_key = ?, agg_value = ?, aggregation_type = ?, state \
                 = 'ACTIVE' WHERE table_name = ? AND column_name = ? AND state = 'AVAILABLE'",
            )
            .bind(agg_key)
            .bind((!agg_value.is_empty()).then_some(agg_value))
            .bind(agg_type)
            .bind(&self.table_name)
            .bind(&col_name)
            .execute(advisory.connection())
            .await?;
            if available.len() <= 3 {
                tracing::warn!(
                    table = %self.table_name,
                    value_type,
                    remaining = available.len() - 1,
                    "aggregation column reserve is low"
                );
            }
            self.replace_entries(load_agg_entries(advisory.connection(), &self.table_name).await?)
                .await;
            advisory.release().await?;
            return Ok(col_name);
        }

        // Allocate new slot.
        let physical: Vec<String> = sqlx::query_scalar(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
             AND TABLE_NAME = ?",
        )
        .bind(&self.table_name)
        .fetch_all(advisory.connection())
        .await?;
        let slot = fresh
            .iter()
            .map(|e| e.column_name.as_str())
            .chain(physical.iter().map(String::as_str))
            .filter_map(parse_agg_slot)
            .max()
            .unwrap_or(0)
            + 1;
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
        match sqlx::query(&alter).execute(advisory.connection()).await {
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
        .execute(advisory.connection())
        .await?;

        self.replace_entries(load_agg_entries(advisory.connection(), &self.table_name).await?)
            .await;

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

    async fn replace_entries(&self, entries: Vec<AggRegistryEntry>) {
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|e| e.status == "ACTIVE")
            .collect();
        let cache = entries
            .iter()
            .map(|e| {
                (
                    agg_cache_key(
                        &e.agg_key,
                        e.agg_value.as_deref().unwrap_or(""),
                        &e.aggregation_type,
                    ),
                    e.column_name.clone(),
                )
            })
            .collect();
        *self.inner.write().await = AggInner { cache, entries };
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

async fn load_agg_entries<'e, E: sqlx::Executor<'e, Database = sqlx::MySql>>(
    db: E,
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
