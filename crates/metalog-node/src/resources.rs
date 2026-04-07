use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_schema::ColumnRegistry;
use metalog_telemetry::TelemetryProvider;
use sqlx::MySqlPool;
use tokio::sync::RwLock;

/// Shared resources accessible by all coordinator/worker units.
pub struct Resources {
    pub db: MySqlPool,
    pub read_db: Option<MySqlPool>,
    pub telemetry: Option<TelemetryProvider>,
    pub is_mariadb: bool,
    pub failure_log_interval: Duration,
    column_registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
}

impl Resources {
    pub fn new(db: MySqlPool, is_mariadb: bool, failure_log_interval: Duration) -> Self {
        Self {
            db,
            read_db: None,
            telemetry: None,
            is_mariadb,
            failure_log_interval,
            column_registries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the read-only DB pool (replica if configured, else primary).
    pub fn read_only_db(&self) -> &MySqlPool {
        self.read_db.as_ref().unwrap_or(&self.db)
    }

    /// Caches a column registry for a table.
    pub async fn set_column_registry(&self, table_name: &str, registry: Arc<ColumnRegistry>) {
        let mut regs = self.column_registries.write().await;
        regs.insert(table_name.to_string(), registry);
    }

    /// Gets a cached column registry for a table.
    pub async fn get_column_registry(&self, table_name: &str) -> Option<Arc<ColumnRegistry>> {
        let regs = self.column_registries.read().await;
        regs.get(table_name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resources_struct_size() {
        // Verify the type compiles (pool requires tokio runtime to construct).
        assert!(std::mem::size_of::<Resources>() > 0);
    }
}
