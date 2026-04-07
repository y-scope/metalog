use std::time::Duration;

use metalog_config::DatabaseConfig;
use sqlx::{mysql::MySqlPoolOptions, MySqlPool};

/// Creates a MySQL connection pool from a [`DatabaseConfig`].
///
/// Pool settings:
/// - `max_connections`: from config (default 5)
/// - `min_connections`: from config (default 2)
/// - `idle_timeout`: 5 minutes
/// - `max_lifetime`: 30 minutes
pub async fn new_pool(cfg: &DatabaseConfig) -> Result<MySqlPool, sqlx::Error> {
    MySqlPoolOptions::new()
        .max_connections(cfg.pool_size)
        .min_connections(cfg.pool_min_idle)
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&cfg.dsn())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_options_from_config() {
        let cfg = DatabaseConfig {
            host: "localhost".into(),
            database: "test".into(),
            user: "root".into(),
            password: "".into(),
            port: 3306,
            pool_size: 10,
            pool_min_idle: 3,
        };
        // Just verify DSN is valid — actual connection requires a running DB.
        let dsn = cfg.dsn();
        assert!(dsn.starts_with("mysql://"));
        assert!(dsn.contains("test"));
    }
}
