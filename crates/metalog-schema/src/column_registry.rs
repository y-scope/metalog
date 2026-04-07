use std::{collections::HashMap, sync::Arc};

use metalog_metastore::AdvisoryLock;
use metalog_types::column::{
    DEFAULT_VARCHAR_WIDTH,
    DIM_COLUMN_PREFIX,
    MAX_DIM_SLOTS,
    MIN_VARCHAR_WIDTH,
};
use sqlx::MySqlPool;
use tokio::sync::RwLock;

/// Maps physical `dim_fNN` columns to logical dimension keys.
///
/// Thread-safe via [`RwLock`]: reads (resolve) take a read lock, allocation
/// (resolve-or-allocate) takes a write lock + cross-node advisory lock.
pub struct ColumnRegistry {
    db: MySqlPool,
    table_name: String,
    inner: Arc<RwLock<RegistryInner>>,
}

struct RegistryInner {
    /// dim_key → physical column name (e.g., "hostname" → "dim_f01").
    dim_cache: HashMap<String, String>,
    /// All active dim entries loaded from DB.
    dim_entries: Vec<DimRegistryEntry>,
    /// Next slot number to allocate (1-based, max 99).
    next_dim_slot: u32,
}

/// A single row from `_dim_registry`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DimRegistryEntry {
    pub table_name: String,
    pub column_name: String,
    pub base_type: String,
    pub dim_key: String,
    #[sqlx(rename = "alias_column")]
    pub alias_col: Option<String>,
    #[sqlx(rename = "state")]
    pub status: String,
    pub width: Option<i32>,
}

/// Immutable snapshot of the registry for lock-free reads.
#[derive(Debug, Clone)]
pub struct RegistrySnapshot {
    dim_map: HashMap<String, String>,
}

impl RegistrySnapshot {
    /// Resolves a logical dimension key to a physical column name.
    pub fn resolve_dim(&self, dim_key: &str) -> Option<&str> {
        self.dim_map.get(dim_key).map(|s| s.as_str())
    }
}

/// Errors from column registry operations.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("slot exhausted: all {MAX_DIM_SLOTS} dim slots are in use")]
    SlotExhausted,

    #[error("invalid dim base type: {0} (must be str, str_utf8, int, bool, float)")]
    InvalidBaseType(String),

    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("advisory lock: {0}")]
    Lock(#[from] metalog_metastore::AdvisoryLockError),
}

const VALID_BASE_TYPES: &[&str] = &["str", "str_utf8", "int", "bool", "float"];

/// Validates a dimension base type.
pub fn validate_dim_base_type(base_type: &str) -> Result<(), RegistryError> {
    if VALID_BASE_TYPES.contains(&base_type) {
        Ok(())
    } else {
        Err(RegistryError::InvalidBaseType(base_type.to_string()))
    }
}

impl ColumnRegistry {
    /// Creates a new `ColumnRegistry`, loading all ACTIVE entries from the database.
    pub async fn new(db: MySqlPool, table_name: &str) -> Result<Self, RegistryError> {
        let entries = load_dim_entries(&db, table_name).await?;

        let mut dim_cache = HashMap::new();
        let mut max_slot: u32 = 0;

        for entry in &entries {
            dim_cache.insert(entry.dim_key.clone(), entry.column_name.clone());
            if let Some(slot) = parse_slot_number(&entry.column_name) {
                max_slot = max_slot.max(slot);
            }
        }

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            inner: Arc::new(RwLock::new(RegistryInner {
                dim_cache,
                dim_entries: entries,
                next_dim_slot: max_slot + 1,
            })),
        })
    }

    /// Fast-path dimension lookup (read lock only).
    pub async fn resolve_dim(&self, dim_key: &str) -> Option<String> {
        let inner = self.inner.read().await;
        inner.dim_cache.get(dim_key).cloned()
    }

    /// Returns a lock-free snapshot of the current registry state.
    pub async fn snapshot(&self) -> RegistrySnapshot {
        let inner = self.inner.read().await;
        RegistrySnapshot {
            dim_map: inner.dim_cache.clone(),
        }
    }

    /// Returns all active dim entries.
    pub async fn all_dim_entries(&self) -> Vec<DimRegistryEntry> {
        let inner = self.inner.read().await;
        inner.dim_entries.clone()
    }

    /// Returns the number of active entries (used as a cache-busting version token).
    pub async fn entry_count(&self) -> usize {
        let inner = self.inner.read().await;
        inner.dim_entries.len()
    }

    /// Resolves or allocates a dimension column. Fast-path checks the cache;
    /// slow-path acquires advisory lock and allocates a new slot.
    pub async fn resolve_or_allocate_dim(
        &self,
        dim_key: &str,
        base_type: &str,
        width: i32,
    ) -> Result<String, RegistryError> {
        validate_dim_base_type(base_type)?;

        // Fast path: read lock.
        {
            let inner = self.inner.read().await;
            if let Some(col) = inner.dim_cache.get(dim_key) {
                return Ok(col.clone());
            }
        }

        // Slow path: write lock + advisory lock.
        self.allocate_dim_slot(dim_key, base_type, width).await
    }

    async fn allocate_dim_slot(
        &self,
        dim_key: &str,
        base_type: &str,
        width: i32,
    ) -> Result<String, RegistryError> {
        let lock_name = format!("metalog_dim_alloc_{}", self.table_name);
        let mut advisory = AdvisoryLock::acquire(&self.db, &lock_name, 10).await?;

        // Re-check under advisory lock (another node may have allocated).
        let fresh_entries = load_dim_entries(&self.db, &self.table_name).await?;
        let found = fresh_entries
            .iter()
            .find(|e| e.dim_key == dim_key)
            .map(|e| e.column_name.clone());
        if let Some(col_name) = found {
            let mut inner = self.inner.write().await;
            inner
                .dim_cache
                .insert(dim_key.to_string(), col_name.clone());
            inner.dim_entries = fresh_entries;
            advisory.release().await?;
            return Ok(col_name);
        }

        // Allocate new slot.
        let mut inner = self.inner.write().await;
        let slot = inner.next_dim_slot;
        if slot > MAX_DIM_SLOTS {
            advisory.release().await?;
            return Err(RegistryError::SlotExhausted);
        }

        let col_name = format!("{DIM_COLUMN_PREFIX}{slot:02}");
        let effective_width = width.max(MIN_VARCHAR_WIDTH);
        let sql_type = dim_sql_type(base_type, effective_width);

        // ALTER TABLE ADD COLUMN.
        let alter = format!(
            "ALTER TABLE `{}` ADD COLUMN `{col_name}` {sql_type} NULL",
            self.table_name
        );
        match sqlx::query(&alter).execute(&self.db).await {
            Ok(_) => {}
            Err(e) if metalog_db::is_duplicate_column(&e) => {
                // Column already exists (concurrent allocation) — proceed with INSERT.
            }
            Err(e) => {
                advisory.release().await?;
                return Err(RegistryError::Sql(e));
            }
        }

        // INSERT into _dim_registry.
        let now = metalog_timeutil::epoch_nanos();
        sqlx::query(
            "INSERT INTO _dim_registry (table_name, column_name, base_type, dim_key, width, \
             state, created_at) VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?)",
        )
        .bind(&self.table_name)
        .bind(&col_name)
        .bind(base_type)
        .bind(dim_key)
        .bind(effective_width)
        .bind(now)
        .execute(&self.db)
        .await?;

        inner
            .dim_cache
            .insert(dim_key.to_string(), col_name.clone());
        inner.next_dim_slot = slot + 1;

        // Refresh entries.
        inner.dim_entries = load_dim_entries(&self.db, &self.table_name).await?;

        advisory.release().await?;

        tracing::info!(
            table = %self.table_name,
            dim_key,
            column = %col_name,
            "allocated dimension column"
        );

        Ok(col_name)
    }

    /// Re-reads alias columns from the database, evicts invalidated entries.
    pub async fn refresh_aliases(&self) -> Result<(), RegistryError> {
        let fresh = load_dim_entries(&self.db, &self.table_name).await?;
        let mut inner = self.inner.write().await;

        inner.dim_cache.clear();
        for entry in &fresh {
            inner
                .dim_cache
                .insert(entry.dim_key.clone(), entry.column_name.clone());
        }
        inner.dim_entries = fresh;

        Ok(())
    }
}

/// Returns the SQL type for a dimension column.
fn dim_sql_type(base_type: &str, width: i32) -> String {
    match base_type {
        "str" => format!("VARCHAR({width}) CHARACTER SET ascii COLLATE ascii_bin"),
        "str_utf8" => format!("VARCHAR({width}) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"),
        "int" => "BIGINT".to_string(),
        "bool" => "BOOLEAN".to_string(),
        "float" => "DOUBLE".to_string(),
        _ => format!("VARCHAR({DEFAULT_VARCHAR_WIDTH}) CHARACTER SET ascii COLLATE ascii_bin"),
    }
}

/// Loads all ACTIVE dim entries from `_dim_registry` for a table.
async fn load_dim_entries(
    db: &MySqlPool,
    table_name: &str,
) -> Result<Vec<DimRegistryEntry>, sqlx::Error> {
    sqlx::query_as::<_, DimRegistryEntry>(
        "SELECT table_name, column_name, base_type, dim_key, alias_column, state, width FROM \
         _dim_registry WHERE table_name = ? AND state = 'ACTIVE' ORDER BY column_name",
    )
    .bind(table_name)
    .fetch_all(db)
    .await
}

/// Extracts the numeric slot number from a column name like "dim_f01" → 1.
fn parse_slot_number(col_name: &str) -> Option<u32> {
    col_name
        .strip_prefix(DIM_COLUMN_PREFIX)
        .and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_slot() {
        assert_eq!(parse_slot_number("dim_f01"), Some(1));
        assert_eq!(parse_slot_number("dim_f99"), Some(99));
        assert_eq!(parse_slot_number("agg_f01"), None);
        assert_eq!(parse_slot_number("invalid"), None);
    }

    #[test]
    fn dim_sql_types() {
        assert_eq!(
            dim_sql_type("str", 256),
            "VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin"
        );
        assert_eq!(
            dim_sql_type("str_utf8", 512),
            "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );
        assert_eq!(dim_sql_type("int", 0), "BIGINT");
        assert_eq!(dim_sql_type("bool", 0), "BOOLEAN");
        assert_eq!(dim_sql_type("float", 0), "DOUBLE");
    }

    #[test]
    fn validate_base_type() {
        assert!(validate_dim_base_type("str").is_ok());
        assert!(validate_dim_base_type("str_utf8").is_ok());
        assert!(validate_dim_base_type("int").is_ok());
        assert!(validate_dim_base_type("bool").is_ok());
        assert!(validate_dim_base_type("float").is_ok());
        assert!(validate_dim_base_type("invalid").is_err());
        assert!(validate_dim_base_type("").is_err());
    }

    #[test]
    fn snapshot_resolve() {
        let mut dim_map = HashMap::new();
        dim_map.insert("hostname".to_string(), "dim_f01".to_string());
        dim_map.insert("region".to_string(), "dim_f02".to_string());

        let snap = RegistrySnapshot { dim_map };
        assert_eq!(snap.resolve_dim("hostname"), Some("dim_f01"));
        assert_eq!(snap.resolve_dim("region"), Some("dim_f02"));
        assert_eq!(snap.resolve_dim("missing"), None);
    }

    #[test]
    fn slot_exhaustion_error() {
        let err = RegistryError::SlotExhausted;
        assert!(err.to_string().contains("99"));
    }
}
