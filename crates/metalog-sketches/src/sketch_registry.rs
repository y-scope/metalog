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
        let names = self.resolve_or_allocate(&[sketch_key.to_owned()]).await?;
        Ok(names.get(sketch_key).cloned())
    }

    /// Serialize cross-node allocation and always re-read all slots under the
    /// lock. The schema has no unique logical-key index; a process-local cache
    /// alone cannot prevent two nodes allocating the same key twice.
    pub async fn resolve_or_allocate(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, String>, sqlx::Error> {
        let mut lock = metalog_metastore::AdvisoryLock::acquire_scoped(
            &self.db,
            "sketch_registry",
            &self.table_name,
            10,
        )
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let entries: Vec<SketchRegistryEntry> = sqlx::query_as(
            "SELECT table_name, sketch_name, sketch_key, state FROM _sketch_registry WHERE \
             table_name = ? ORDER BY sketch_name",
        )
        .bind(&self.table_name)
        .fetch_all(lock.connection())
        .await?;
        let mut mapping = HashMap::new();
        let mut available = Vec::new();
        for entry in entries {
            match (entry.status.as_str(), entry.sketch_key) {
                ("ACTIVE", Some(key)) => {
                    if mapping.insert(key, entry.sketch_name).is_some() {
                        return Err(sqlx::Error::Protocol(
                            "duplicate active sketch key; repair registry before ingesting".into(),
                        ));
                    }
                }
                ("AVAILABLE", None) => available.push(entry.sketch_name),
                _ => {} // Never recycle invalidated slots.
            }
        }
        let mut keys = keys.to_vec();
        keys.sort();
        keys.dedup();
        let missing: Vec<_> = keys
            .iter()
            .filter(|key| !mapping.contains_key(*key))
            .collect();
        if missing.len() > available.len() {
            return Err(sqlx::Error::Protocol(
                "all 64 sketch slots are allocated".into(),
            ));
        }
        for (key, slot) in missing.into_iter().zip(available) {
            sqlx::query(
                "UPDATE _sketch_registry SET sketch_key = ?, state = 'ACTIVE', created_at = ? \
                 WHERE table_name = ? AND sketch_name = ? AND state = 'AVAILABLE'",
            )
            .bind(key)
            .bind(metalog_timeutil::epoch_nanos())
            .bind(&self.table_name)
            .bind(&slot)
            .execute(lock.connection())
            .await?;
            mapping.insert(key.clone(), slot);
        }
        lock.release()
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        self.inner.write().await.cache = mapping.clone();
        Ok(mapping)
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
