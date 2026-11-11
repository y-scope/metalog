use metalog_metastore::{
    KAFKA_ASSIGNMENT_TABLE,
    KAFKA_SOURCE_TABLE,
    TABLE_REGISTRY,
    TABLE_REGISTRY_ASSIGNMENT,
    TABLE_REGISTRY_CONFIG,
    TEMPLATE_TABLE,
};
use metalog_timeutil::epoch_nanos;
use sqlx::{Connection, MySqlPool};

use crate::{
    ddl::{execute_ddl_statements, SCHEMA_SQL},
    partition_manager::create_lookahead_partitions_on,
    DEFAULT_PROVISION_LOOKAHEAD_DAYS,
};

/// Provisions a new table by:
/// 1. Ensuring system tables exist (idempotent DDL)
/// 2. Recording its configuration and creating the physical table with its reserves
/// 3. Publishing registry slots and the coordinator assignment atomically
///
/// Returns `true` if the table was newly created, `false` if it already existed.
pub async fn ensure_table(
    pool: &MySqlPool,
    table_name: &str,
    config_json: Option<&str>,
) -> Result<bool, EnsureTableError> {
    metalog_db::validate_sql_identifier(table_name)
        .map_err(|e| EnsureTableError::Validation(e.to_string()))?;
    if table_name.starts_with('_') || table_name.len() > 64 {
        return Err(EnsureTableError::Validation(
            "table names must be at most 64 characters and must not use the reserved '_' prefix"
                .into(),
        ));
    }
    let mut requested = metalog_metastore::decode_table_config(config_json)
        .map_err(|e| EnsureTableError::Validation(format!("invalid table configuration: {e}")))?;
    if let Some(reserves) = &requested.column_reserves {
        reserves.validate().map_err(EnsureTableError::Validation)?;
    }
    let explicit_reserves = config_json
        .filter(|s| !s.is_empty())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .is_some_and(|value| value.get("column_reserves").is_some());

    // 1. Ensure system tables.
    execute_ddl_statements(pool, SCHEMA_SQL)
        .await
        .map_err(EnsureTableError::Sql)?;

    // DDL implicitly commits, so serialize provisioning and make every step
    // independently restartable. A previous crash may have left only _table.
    let mut lock =
        metalog_metastore::AdvisoryLock::acquire_scoped(pool, "provision", table_name, 30).await?;

    let stored: Option<Option<String>> =
        sqlx::query_scalar("SELECT config FROM _table_config WHERE table_name = ?")
            .bind(table_name)
            .fetch_optional(lock.connection())
            .await
            .map_err(EnsureTableError::Sql)?;
    let physical_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND \
         TABLE_NAME = ?",
    )
    .bind(table_name)
    .fetch_one(lock.connection())
    .await
    .map_err(EnsureTableError::Sql)?;
    let effective = if let Some(stored) = &stored {
        let config = metalog_metastore::decode_table_config(stored.as_deref())
            .map_err(|e| EnsureTableError::Validation(e.to_string()))?;
        if explicit_reserves && requested.column_reserves != config.column_reserves {
            return Err(EnsureTableError::Validation(
                "column_reserves is creation-only and cannot be changed on an existing table"
                    .into(),
            ));
        }
        config
    } else {
        if physical_exists != 0 && requested.column_reserves.is_some() {
            return Err(EnsureTableError::Validation(
                "column_reserves can only be set when creating a new physical table".into(),
            ));
        }
        // Apply defaults only when creating a physical table. Legacy configs
        // remain dynamic, and explicit JSON null opts new tables out.
        if physical_exists == 0 && !explicit_reserves {
            requested.column_reserves = Some(Default::default());
        }
        requested
    };
    let reserved = effective
        .column_reserves
        .as_ref()
        .map(crate::reserves::columns)
        .unwrap_or_default();

    // 2. Register in _table (INSERT IGNORE for idempotency).
    let result = sqlx::query(&format!(
        "INSERT IGNORE INTO {TABLE_REGISTRY} (table_name, display_name, active) VALUES (?, ?, \
         FALSE)"
    ))
    .bind(table_name)
    .bind(table_name)
    .execute(lock.connection())
    .await
    .map_err(EnsureTableError::Sql)?;

    let created = result.rows_affected() > 0;

    {
        // Insert config.
        let config_blob = metalog_metastore::encode_table_config(&effective)
            .map_err(|e| EnsureTableError::Validation(e.to_string()))?;
        sqlx::query(&format!(
            "INSERT IGNORE INTO {TABLE_REGISTRY_CONFIG} (table_name, config) VALUES (?, ?)"
        ))
        .bind(table_name)
        .bind(&config_blob)
        .execute(lock.connection())
        .await
        .map_err(EnsureTableError::Sql)?;

        // 3. Clone template table.
        if physical_exists == 0 {
            let create_sql = if reserved.is_empty() {
                format!("CREATE TABLE `{table_name}` LIKE `{TEMPLATE_TABLE}`")
            } else {
                let (_, template): (String, String) =
                    sqlx::query_as(&format!("SHOW CREATE TABLE `{TEMPLATE_TABLE}`"))
                        .fetch_one(lock.connection())
                        .await
                        .map_err(EnsureTableError::Sql)?;
                let prefix = format!("CREATE TABLE `{TEMPLATE_TABLE}` (");
                let tail = template.strip_prefix(&prefix).ok_or_else(|| {
                    EnsureTableError::Validation("unexpected template DDL".into())
                })?;
                let definitions = reserved
                    .iter()
                    .map(|c| format!("`{}` {} NULL", c.name, c.sql_type))
                    .collect::<Vec<_>>()
                    .join(",\n");
                format!("CREATE TABLE `{table_name}` ({definitions},{tail}")
            };
            sqlx::query(&create_sql)
                .execute(lock.connection())
                .await
                .map_err(EnsureTableError::Sql)?;
        }

        // Pre-populate daily partitions for the next N days.
        create_lookahead_partitions_on(
            lock.connection(),
            table_name,
            DEFAULT_PROVISION_LOOKAHEAD_DAYS,
        )
        .await
        .map_err(|e| EnsureTableError::Validation(format!("partition provisioning failed: {e}")))?;

        // Publish registries and assignment atomically, after all DDL succeeds.
        // Coordinators only discover active tables with an assignment row.
        let mut tx = lock
            .connection()
            .begin()
            .await
            .map_err(EnsureTableError::Sql)?;
        let now = epoch_nanos();
        for column in &reserved {
            if column.aggregate {
                sqlx::query(
                    "INSERT IGNORE INTO _agg_registry (table_name, column_name, agg_key, \
                     value_type, state, created_at) VALUES (?, ?, '', ?, 'AVAILABLE', ?)",
                )
                .bind(table_name)
                .bind(&column.name)
                .bind(&column.kind)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(EnsureTableError::Sql)?;
            } else {
                sqlx::query(
                    "INSERT IGNORE INTO _dim_registry (table_name, column_name, base_type, width, \
                     dim_key, state, created_at) VALUES (?, ?, ?, ?, '', 'AVAILABLE', ?)",
                )
                .bind(table_name)
                .bind(&column.name)
                .bind(&column.kind)
                .bind(column.width)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(EnsureTableError::Sql)?;
            }
        }
        // Pre-populate sketch slots (s01..s64) as AVAILABLE.
        for i in 1..=64 {
            let sketch_name = format!("s{i:02}");
            sqlx::query(
                "INSERT IGNORE INTO _sketch_registry (table_name, sketch_name, state, created_at) \
                 VALUES (?, ?, 'AVAILABLE', ?)",
            )
            .bind(table_name)
            .bind(&sketch_name)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(EnsureTableError::Sql)?;
        }

        let assignment = sqlx::query(&format!(
            "INSERT IGNORE INTO {TABLE_REGISTRY_ASSIGNMENT} (table_name) VALUES (?)"
        ))
        .bind(table_name)
        .execute(&mut *tx)
        .await
        .map_err(EnsureTableError::Sql)?;
        if assignment.rows_affected() > 0 {
            sqlx::query("UPDATE _table SET active = TRUE WHERE table_name = ?")
                .bind(table_name)
                .execute(&mut *tx)
                .await
                .map_err(EnsureTableError::Sql)?;
        }
        tx.commit().await.map_err(EnsureTableError::Sql)?;

        if created {
            tracing::info!(table_name, "provisioned new table");
        }
    }
    lock.release().await?;
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
    #[error("provisioning lock: {0}")]
    Lock(#[from] metalog_metastore::AdvisoryLockError),
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
