use metalog_metastore::{
    KAFKA_ASSIGNMENT_TABLE,
    KAFKA_SOURCE_TABLE,
    TABLE_REGISTRY,
    TABLE_REGISTRY_ASSIGNMENT,
    TABLE_REGISTRY_CONFIG,
    TEMPLATE_TABLE,
};
use metalog_timeutil::epoch_nanos;
use sqlx::MySqlPool;

use crate::ddl::{execute_ddl_statements, SCHEMA_SQL};
use crate::partition_manager::create_lookahead_partitions;
use crate::DEFAULT_PROVISION_LOOKAHEAD_DAYS;

/// Provisions a new table by:
/// 1. Ensuring system tables exist (idempotent DDL)
/// 2. Registering the table in `_table` + `_table_config` + `_table_assignment`
/// 3. Cloning `_clp_template` as the table's data table
///
/// Returns `true` if the table was newly created, `false` if it already existed.
pub async fn ensure_table(
    pool: &MySqlPool,
    table_name: &str,
    config_json: Option<&str>,
) -> Result<bool, EnsureTableError> {
    metalog_db::validate_sql_identifier(table_name)
        .map_err(|e| EnsureTableError::Validation(e.to_string()))?;

    // 1. Ensure system tables.
    execute_ddl_statements(pool, SCHEMA_SQL)
        .await
        .map_err(EnsureTableError::Sql)?;

    // 2. Register in _table (INSERT IGNORE for idempotency).
    let result = sqlx::query(&format!(
        "INSERT IGNORE INTO {TABLE_REGISTRY} (table_name, display_name, active) VALUES (?, ?, \
         TRUE)"
    ))
    .bind(table_name)
    .bind(table_name)
    .execute(pool)
    .await
    .map_err(EnsureTableError::Sql)?;

    let created = result.rows_affected() > 0;

    if created {
        // Insert config.
        let config_blob = config_json.unwrap_or("{}");
        sqlx::query(&format!(
            "INSERT IGNORE INTO {TABLE_REGISTRY_CONFIG} (table_name, config) VALUES (?, ?)"
        ))
        .bind(table_name)
        .bind(config_blob)
        .execute(pool)
        .await
        .map_err(EnsureTableError::Sql)?;

        // Insert assignment (unassigned).
        sqlx::query(&format!(
            "INSERT IGNORE INTO {TABLE_REGISTRY_ASSIGNMENT} (table_name) VALUES (?)"
        ))
        .bind(table_name)
        .execute(pool)
        .await
        .map_err(EnsureTableError::Sql)?;

        // 3. Clone template table.
        let create_sql =
            format!("CREATE TABLE IF NOT EXISTS `{table_name}` LIKE `{TEMPLATE_TABLE}`");
        sqlx::query(&create_sql)
            .execute(pool)
            .await
            .map_err(EnsureTableError::Sql)?;

        // Pre-populate daily partitions for the next N days.
        if let Err(e) = create_lookahead_partitions(pool, table_name, DEFAULT_PROVISION_LOOKAHEAD_DAYS).await {
            tracing::warn!(
                table_name,
                error = %e,
                "failed to create initial lookahead partitions during provisioning"
            );
        }

        // Pre-populate sketch slots (s01..s64) as AVAILABLE.
        let now = epoch_nanos();
        for i in 1..=64 {
            let sketch_name = format!("s{i:02}");
            sqlx::query(
                "INSERT IGNORE INTO _sketch_registry (table_name, sketch_name, state, created_at) \
                 VALUES (?, ?, 'AVAILABLE', ?)",
            )
            .bind(table_name)
            .bind(&sketch_name)
            .bind(now)
            .execute(pool)
            .await
            .map_err(EnsureTableError::Sql)?;
        }

        tracing::info!(table_name, "provisioned new table");
    }

    Ok(created)
}

/// Ensures the Kafka source tables exist (premium DDL injection point).
///
/// Called by the `KafkaProvider` premium crate during system table setup.
pub async fn ensure_kafka_tables(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_SOURCE_TABLE} (
            table_name VARCHAR(64) NOT NULL,
            source_name VARCHAR(128) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            bootstrap_servers VARCHAR(1024) NOT NULL,
            record_transformer VARCHAR(64) DEFAULT 'proto',
            consumer_group_id VARCHAR(255) NULL,
            required_env VARCHAR(512) NULL,
            created_at BIGINT NOT NULL,
            PRIMARY KEY (table_name, source_name),
            FOREIGN KEY (table_name) REFERENCES {TABLE_REGISTRY}(table_name) ON DELETE CASCADE
        ) ENGINE=InnoDB"
    );
    execute_ddl_statements(pool, &ddl).await?;

    let ddl2 = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_ASSIGNMENT_TABLE} (
            table_name VARCHAR(64) NOT NULL,
            source_name VARCHAR(128) NOT NULL,
            node_id VARCHAR(64) NULL,
            lease_expiry BIGINT NULL,
            claimed_at BIGINT NULL,
            PRIMARY KEY (table_name, source_name),
            INDEX idx_kafka_node (node_id),
            FOREIGN KEY (table_name, source_name)
                REFERENCES {KAFKA_SOURCE_TABLE}(table_name, source_name) ON DELETE CASCADE
        ) ENGINE=InnoDB"
    );
    execute_ddl_statements(pool, &ddl2).await?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum EnsureTableError {
    #[error("validation: {0}")]
    Validation(String),

    #[error("sql: {0}")]
    Sql(#[source] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_table_name() {
        // Can't run full ensure_table without DB, but validate the name check.
        assert!(metalog_db::validate_sql_identifier("valid_name").is_ok());
        assert!(metalog_db::validate_sql_identifier("DROP TABLE").is_err());
    }

    #[test]
    fn error_display() {
        let err = EnsureTableError::Validation("bad name".into());
        assert!(err.to_string().contains("bad name"));
    }
}
