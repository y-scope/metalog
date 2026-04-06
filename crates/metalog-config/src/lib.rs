mod database;
mod node;
mod storage;

// Re-export default intervals used by coordinator and worker subsystems.
use std::time::Duration;

pub use database::DatabaseConfig;
pub use node::{
    CoordinatorConfig,
    GRPCConfig,
    HAStrategy,
    HealthConfig,
    LoggingConfig,
    NodeConfig,
    ServiceConfig,
    WorkerConfig,
};
pub use storage::{ObjectStorageConfig, StorageBackendConfig};

/// Default interval between planner cycles.
pub const DEFAULT_PLANNER_INTERVAL: Duration = Duration::from_secs(30);

/// Default stall timeout for coordinator progress tracking.
pub const DEFAULT_PROGRESS_STALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Default interval between worker poll attempts when idle.
pub const DEFAULT_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum backoff between worker poll attempts.
pub const DEFAULT_WORKER_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Default timeout for stale task detection (processing tasks older than this are reclaimed).
pub const DEFAULT_TASK_STALE_TIMEOUT: Duration = Duration::from_secs(300);

/// Default age for cleaning up completed/failed task rows.
pub const DEFAULT_TASK_CLEANUP_AGE: Duration = Duration::from_secs(86400);
