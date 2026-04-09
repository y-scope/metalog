use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_schema::ColumnRegistry;
use metalog_telemetry::TelemetryProvider;
use parking_lot::RwLock;
use sqlx::MySqlPool;

use crate::CoordinatorUnit;

/// Shared resources accessible by all coordinator/worker units.
pub struct Resources {
    pub db: MySqlPool,
    pub read_db: Option<MySqlPool>,
    pub telemetry: Option<TelemetryProvider>,
    pub is_mariadb: bool,
    pub failure_log_interval: Duration,
    column_registries: RwLock<HashMap<String, Arc<ColumnRegistry>>>,
    coordinator_units: RwLock<HashMap<String, Arc<CoordinatorUnit>>>,
}

impl Resources {
    pub fn new(db: MySqlPool, is_mariadb: bool, failure_log_interval: Duration) -> Self {
        Self {
            db,
            read_db: None,
            telemetry: None,
            is_mariadb,
            failure_log_interval,
            column_registries: RwLock::new(HashMap::new()),
            coordinator_units: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the read-only DB pool (replica if configured, else primary).
    pub fn read_only_db(&self) -> &MySqlPool {
        self.read_db.as_ref().unwrap_or(&self.db)
    }

    /// Caches a column registry for a table.
    pub fn set_column_registry(&self, table_name: &str, registry: Arc<ColumnRegistry>) {
        self.column_registries.write().insert(table_name.to_string(), registry);
    }

    /// Gets a cached column registry for a table.
    pub fn get_column_registry(&self, table_name: &str) -> Option<Arc<ColumnRegistry>> {
        self.column_registries.read().get(table_name).cloned()
    }

    /// Stores the coordinator unit for a table (used by HA watchdog).
    pub fn store_coordinator_unit(&self, table_name: &str, unit: Arc<CoordinatorUnit>) {
        self.coordinator_units.write().insert(table_name.to_string(), unit);
    }

    /// Cancels and removes the coordinator unit for a table.
    pub fn stop_coordinator(&self, table_name: &str) {
        if let Some(unit) = self.coordinator_units.write().remove(table_name) {
            unit.stop();
        }
    }

    /// Returns true if the coordinator for a table has stalled.
    pub fn is_coordinator_stalled(&self, table_name: &str) -> bool {
        self.coordinator_units
            .read()
            .get(table_name)
            .map(|u| u.is_stalled())
            .unwrap_or(false)
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
