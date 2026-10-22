use std::{collections::HashMap, sync::Arc};

use metalog_metastore::AdvisoryLock;
use metalog_types::column::{
    DEFAULT_VARCHAR_WIDTH,
    DIM_COLUMN_PREFIX,
    MAX_DIM_SLOTS,
    MIN_VARCHAR_WIDTH,
};
use parking_lot::RwLock;
use sqlx::MySqlPool;

/// Maps physical `dim_fNN` columns to logical dimension keys.
///
/// Uses `std::sync::RwLock` for the cache (sync reads, no await needed) and
/// `tokio::sync::Mutex` for allocation serialization (async DDL operations).
pub struct ColumnRegistry {
    db: MySqlPool,
    table_name: String,
    /// Cache for fast-path sync reads. Never held across .await.
    cache: Arc<RwLock<RegistryCache>>,
    /// Serializes slow-path allocation (advisory lock + ALTER TABLE).
    alloc_lock: tokio::sync::Mutex<()>,
}

struct RegistryCache {
    /// Arc-wrapped for O(1) snapshot cloning (pointer bump, not data copy).
    dim_cache: Arc<HashMap<String, String>>,
    dim_entries: Vec<DimRegistryEntry>,
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
    pub width: Option<u16>,
}

/// Immutable snapshot of the registry for lock-free reads.
/// Cloning is O(1) thanks to Arc-wrapped inner map.
#[derive(Debug, Clone)]
pub struct RegistrySnapshot {
    dim_map: Arc<HashMap<String, String>>,
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
    #[error("invalid dimension definition: {0}")]
    InvalidDefinition(String),

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
        metalog_db::validate_sql_identifier(table_name)
            .map_err(|e| RegistryError::InvalidDefinition(e.to_string()))?;
        let entries = load_dim_entries(&db, table_name).await?;

        let mut dim_cache = HashMap::new();

        for entry in &entries {
            dim_cache.insert(entry.dim_key.clone(), entry.column_name.clone());
            if let Some(alias) = &entry.alias_col {
                dim_cache.insert(alias.clone(), entry.column_name.clone());
            }
        }

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            cache: Arc::new(RwLock::new(RegistryCache {
                dim_cache: Arc::new(dim_cache),
                dim_entries: entries,
            })),
            alloc_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Fast-path dimension lookup. **Sync** — no await, no async overhead.
    pub fn resolve_dim(&self, dim_key: &str) -> Option<String> {
        let cache = self.cache.read();
        cache.dim_cache.get(dim_key).cloned()
    }

    /// Returns a lock-free snapshot of the current registry state.
    /// O(1) — clones the Arc pointer, not the map data.
    pub fn snapshot(&self) -> RegistrySnapshot {
        let cache = self.cache.read();
        RegistrySnapshot {
            dim_map: Arc::clone(&cache.dim_cache),
        }
    }

    /// Returns all active dim entries.
    pub fn all_dim_entries(&self) -> Vec<DimRegistryEntry> {
        let cache = self.cache.read();
        cache.dim_entries.clone()
    }

    /// Returns the number of active entries (cache-busting version token).
    pub fn entry_count(&self) -> usize {
        let cache = self.cache.read();
        cache.dim_entries.len()
    }

    /// Resolves or allocates a dimension column. Fast-path is sync (cache lookup);
    /// slow-path is async (advisory lock + ALTER TABLE).
    pub async fn resolve_or_allocate_dim(
        &self,
        dim_key: &str,
        base_type: &str,
        width: i32,
    ) -> Result<String, RegistryError> {
        validate_dim_base_type(base_type)?;

        if dim_key.is_empty() || dim_key.chars().count() > 1024 || !(0..=16383).contains(&width) {
            return Err(RegistryError::InvalidDefinition(
                "invalid dimension key or width".into(),
            ));
        }
        if let Some(col) = self.cached_compatible_dim(dim_key, base_type, width)? {
            return Ok(col);
        }

        // Slow path: async allocation.
        self.allocate_dim_slot(dim_key, base_type, width).await
    }

    async fn allocate_dim_slot(
        &self,
        dim_key: &str,
        base_type: &str,
        width: i32,
    ) -> Result<String, RegistryError> {
        // Serialize allocation within this process.
        let _alloc_guard = self.alloc_lock.lock().await;

        // Re-check cache (another task may have allocated while we waited).
        if let Some(col) = self.cached_compatible_dim(dim_key, base_type, width)? {
            return Ok(col);
        }

        let mut advisory =
            AdvisoryLock::acquire_scoped(&self.db, "dim_alloc", &self.table_name, 10).await?;

        // Re-check authoritative rows on the lock connection. Include inactive
        // slots so they cannot be reused while old files still reference them.
        let fresh_entries = load_all_dim_entries(advisory.connection(), &self.table_name).await?;
        if let Some(entry) = fresh_entries
            .iter()
            .find(|e| e.dim_key == dim_key && e.status == "ACTIVE")
        {
            if entry.base_type != base_type {
                return Err(RegistryError::InvalidDefinition(format!(
                    "dimension {dim_key} already has type {}",
                    entry.base_type,
                )));
            }
            let col_name = entry.column_name.clone();
            if matches!(base_type, "str" | "str_utf8")
                && width > i32::from(entry.width.unwrap_or(0))
            {
                let effective_width = width.max(MIN_VARCHAR_WIDTH);
                let sql_type = dim_sql_type(base_type, effective_width);
                let alter = format!(
                    "ALTER TABLE `{}` MODIFY COLUMN `{col_name}` {sql_type} NULL",
                    self.table_name
                );
                sqlx::query(&alter).execute(advisory.connection()).await?;
                sqlx::query(
                    "UPDATE _dim_registry SET width = ? WHERE table_name = ? AND column_name = ?",
                )
                .bind(effective_width)
                .bind(&self.table_name)
                .bind(&col_name)
                .execute(advisory.connection())
                .await?;
            }
            let entries = load_dim_entries(advisory.connection(), &self.table_name).await?;
            self.replace_entries(entries);
            advisory.release().await?;
            return Ok(col_name);
        }
        // ALTER TABLE commits independently of registry INSERT. A crash can
        // leave an unregistered physical column; never assign that column to
        // another key/type on retry.
        let physical_columns: Vec<String> = sqlx::query_scalar(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
             AND TABLE_NAME = ?",
        )
        .bind(&self.table_name)
        .fetch_all(advisory.connection())
        .await?;
        let slot = fresh_entries
            .iter()
            .map(|e| e.column_name.as_str())
            .chain(physical_columns.iter().map(String::as_str))
            .filter_map(parse_slot_number)
            .max()
            .unwrap_or(0)
            + 1;
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
        match sqlx::query(&alter).execute(advisory.connection()).await {
            Ok(_) => {}
            Err(e) if metalog_db::is_duplicate_column(&e) => {}
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
        .execute(advisory.connection())
        .await?;

        // Load fresh entries BEFORE acquiring write lock (no await under lock).
        let updated_entries = load_dim_entries(advisory.connection(), &self.table_name).await?;
        self.replace_entries(updated_entries);

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
        self.replace_entries(fresh);
        Ok(())
    }

    fn replace_entries(&self, entries: Vec<DimRegistryEntry>) {
        let mut map = HashMap::new();
        for entry in &entries {
            map.insert(entry.dim_key.clone(), entry.column_name.clone());
            if let Some(alias) = &entry.alias_col {
                map.insert(alias.clone(), entry.column_name.clone());
            }
        }
        let mut cache = self.cache.write();
        cache.dim_cache = Arc::new(map);
        cache.dim_entries = entries;
    }

    fn cached_compatible_dim(
        &self,
        key: &str,
        base_type: &str,
        width: i32,
    ) -> Result<Option<String>, RegistryError> {
        let cache = self.cache.read();
        let Some(entry) = cache.dim_entries.iter().find(|e| e.dim_key == key) else {
            return Ok(None);
        };
        if entry.base_type != base_type {
            return Err(RegistryError::InvalidDefinition(format!(
                "dimension {key} already has type {}",
                entry.base_type
            )));
        }
        if matches!(base_type, "str" | "str_utf8") && width > i32::from(entry.width.unwrap_or(0)) {
            return Ok(None);
        }
        Ok(Some(entry.column_name.clone()))
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
async fn load_dim_entries<'e, E: sqlx::Executor<'e, Database = sqlx::MySql>>(
    db: E,
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

async fn load_all_dim_entries(
    db: &mut sqlx::MySqlConnection,
    table_name: &str,
) -> Result<Vec<DimRegistryEntry>, sqlx::Error> {
    sqlx::query_as::<_, DimRegistryEntry>(
        "SELECT table_name, column_name, base_type, dim_key, alias_column, state, width FROM \
         _dim_registry WHERE table_name = ? ORDER BY column_name",
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

        let snap = RegistrySnapshot {
            dim_map: Arc::new(dim_map),
        };
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
