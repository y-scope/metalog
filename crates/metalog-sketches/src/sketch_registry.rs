use std::{collections::HashMap, sync::Arc};

use sqlx::MySqlPool;
use tokio::sync::RwLock;

/// Sketch registry: maps SET members (s01..s64) to logical field names.
pub struct SketchRegistry {
    db: MySqlPool,
    table_name: String,
    inner: Arc<RwLock<SketchInner>>,
}

struct SketchInner {
    /// sketch_key → SET member name (e.g., "uuid" → "s01").
    cache: HashMap<String, String>,
}

/// A sketch registry entry from `_sketch_registry`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SketchRegistryEntry {
    #[allow(dead_code)]
    pub table_name: String,
    pub sketch_name: String,
    pub sketch_key: Option<String>,
    #[sqlx(rename = "state")]
    #[allow(dead_code)]
    pub status: String,
}

impl SketchRegistry {
    /// Creates a new registry, loading ACTIVE entries.
    pub async fn new(db: MySqlPool, table_name: &str) -> Result<Self, sqlx::Error> {
        let entries = load_entries(&db, table_name).await?;

        let mut cache = HashMap::new();
        for entry in &entries {
            if let Some(ref key) = entry.sketch_key {
                cache.insert(key.clone(), entry.sketch_name.clone());
            }
        }

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            inner: Arc::new(RwLock::new(SketchInner { cache })),
        })
    }

    /// Resolves a sketch key to its SET member name.
    pub async fn resolve(&self, sketch_key: &str) -> Option<String> {
        let inner = self.inner.read().await;
        inner.cache.get(sketch_key).cloned()
    }

    /// Claims an AVAILABLE slot for a new sketch key.
    pub async fn claim_slot(&self, sketch_key: &str) -> Result<Option<String>, sqlx::Error> {
        // Check cache first.
        {
            let inner = self.inner.read().await;
            if let Some(name) = inner.cache.get(sketch_key) {
                return Ok(Some(name.clone()));
            }
        }

        // Find an AVAILABLE slot and claim it.
        let now = metalog_timeutil::epoch_nanos();
        let result = sqlx::query(
            "UPDATE _sketch_registry SET sketch_key = ?, state = 'ACTIVE', created_at = ? WHERE \
             table_name = ? AND state = 'AVAILABLE' AND sketch_key IS NULL LIMIT 1",
        )
        .bind(sketch_key)
        .bind(now)
        .bind(&self.table_name)
        .execute(&self.db)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None); // No slots available.
        }

        // Find which slot was claimed.
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT sketch_name FROM _sketch_registry WHERE table_name = ? AND sketch_key = ? AND \
             state = 'ACTIVE'",
        )
        .bind(&self.table_name)
        .bind(sketch_key)
        .fetch_optional(&self.db)
        .await?;

        if let Some((name,)) = row {
            let mut inner = self.inner.write().await;
            inner.cache.insert(sketch_key.to_string(), name.clone());
            Ok(Some(name))
        } else {
            Ok(None)
        }
    }

    /// Builds the FIND_IN_SET SQL for sketch predicate evaluation.
    pub fn build_find_in_set_condition(&self, set_members: &[String]) -> String {
        set_members
            .iter()
            .map(|m| format!("FIND_IN_SET('{m}', sketches) > 0"))
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

async fn load_entries(
    db: &MySqlPool,
    table_name: &str,
) -> Result<Vec<SketchRegistryEntry>, sqlx::Error> {
    sqlx::query_as::<_, SketchRegistryEntry>(
        "SELECT table_name, sketch_name, sketch_key, state FROM _sketch_registry WHERE table_name \
         = ? AND state = 'ACTIVE' AND sketch_key IS NOT NULL ORDER BY sketch_name",
    )
    .bind(table_name)
    .fetch_all(db)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn find_in_set_single() {
        let reg = SketchRegistry {
            db: MySqlPool::connect_lazy("mysql://root@localhost/test").unwrap(),
            table_name: "test".into(),
            inner: Arc::new(RwLock::new(SketchInner {
                cache: HashMap::new(),
            })),
        };
        let sql = reg.build_find_in_set_condition(&["s01".into()]);
        assert_eq!(sql, "FIND_IN_SET('s01', sketches) > 0");
    }

    #[tokio::test]
    async fn find_in_set_multiple() {
        let reg = SketchRegistry {
            db: MySqlPool::connect_lazy("mysql://root@localhost/test").unwrap(),
            table_name: "test".into(),
            inner: Arc::new(RwLock::new(SketchInner {
                cache: HashMap::new(),
            })),
        };
        let sql = reg.build_find_in_set_condition(&["s01".into(), "s03".into()]);
        assert!(sql.contains("s01"));
        assert!(sql.contains("s03"));
        assert!(sql.contains(" AND "));
    }
}
