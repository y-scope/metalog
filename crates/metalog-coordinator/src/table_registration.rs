use metalog_schema::ensure_table;
use sqlx::MySqlPool;

/// Handles runtime table provisioning and configuration.
pub struct TableRegistration {
    db: MySqlPool,
    _compression_override: String,
}

/// Options for registering a table.
#[derive(Debug, Default)]
pub struct RegisterTableOpts {
    /// JSON config to merge into the stored config blob.
    pub config_json: Option<String>,
}

impl TableRegistration {
    pub fn new(db: MySqlPool, compression_override: &str) -> Self {
        Self {
            db,
            _compression_override: compression_override.to_string(),
        }
    }

    /// Registers a new table. Returns true if newly created, false if already exists.
    ///
    /// Idempotent: calling with the same table name multiple times is safe.
    pub async fn register_table(
        &self,
        table_name: &str,
        display_name: &str,
        opts: RegisterTableOpts,
    ) -> Result<bool, TableRegistrationError> {
        if display_name.chars().count() > 255 {
            return Err(TableRegistrationError::InvalidConfig(
                "display_name is too long".into(),
            ));
        }
        if let Some(json) = &opts.config_json {
            validate_supported_config(json)?;
        }
        let created = ensure_table(&self.db, table_name, opts.config_json.as_deref())
            .await
            .map_err(TableRegistrationError::Provision)?;

        // Provisioning is already committed and independently restartable. Retry
        // only this atomic, idempotent display/config patch so `created` retains
        // its original meaning and retries do not repeat DDL.
        metalog_db::with_deadlock_retry(3, || async {
            match self
                .update_registration(table_name, display_name, opts.config_json.as_deref())
                .await
            {
                Err(TableRegistrationError::Sql(error)) => Err(error),
                result => Ok(result),
            }
        })
        .await
        .map_err(TableRegistrationError::Sql)??;

        Ok(created)
    }

    async fn update_registration(
        &self,
        table_name: &str,
        display_name: &str,
        config_patch: Option<&str>,
    ) -> Result<(), TableRegistrationError> {
        let mut tx = self.db.begin().await.map_err(TableRegistrationError::Sql)?;
        // Lock parent before child, matching provisioning's FK lock order.
        // Taking _table_config first deadlocks with another node's INSERT IGNORE:
        // its FK check holds _table S while waiting for our _table_config X lock.
        if !display_name.is_empty() {
            sqlx::query("UPDATE _table SET display_name = ? WHERE table_name = ?")
                .bind(display_name)
                .bind(table_name)
                .execute(&mut *tx)
                .await
                .map_err(TableRegistrationError::Sql)?;
        } else {
            sqlx::query("SELECT table_name FROM _table WHERE table_name = ? FOR UPDATE")
                .bind(table_name)
                .fetch_one(&mut *tx)
                .await
                .map_err(TableRegistrationError::Sql)?;
        }
        if let Some(patch) = config_patch.filter(|s| !s.is_empty()) {
            let stored: Option<String> = sqlx::query_scalar(
                "SELECT CAST(config AS CHAR) FROM _table_config WHERE table_name = ? FOR UPDATE",
            )
            .bind(table_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(TableRegistrationError::Sql)?;
            let mut merged: serde_json::Value =
                serde_json::from_str(stored.as_deref().unwrap_or("{}"))
                    .map_err(|e| TableRegistrationError::InvalidConfig(e.to_string()))?;
            let patch = serde_json::from_str(patch)
                .map_err(|e| TableRegistrationError::InvalidConfig(e.to_string()))?;
            merge_config(&mut merged, patch);
            let encoded = merged.to_string();
            validate_supported_config(&encoded)?;
            sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = ?")
                .bind(encoded)
                .bind(table_name)
                .execute(&mut *tx)
                .await
                .map_err(TableRegistrationError::Sql)?;
        }
        tx.commit().await.map_err(TableRegistrationError::Sql)?;

        Ok(())
    }

    /// Sets or clears the alias for a dimension or aggregation column.
    pub async fn set_column_alias(
        &self,
        table_name: &str,
        column_name: &str,
        alias: &str,
    ) -> Result<String, TableRegistrationError> {
        let alias_val = if alias.is_empty() {
            None
        } else {
            validate_alias(alias)?;
            Some(alias)
        };

        let registry_table = resolve_registry_table(column_name)?;

        let sql = format!(
            "UPDATE {registry_table} SET alias_column = ? WHERE table_name = ? AND column_name = \
             ? AND state = 'ACTIVE'"
        );
        let result = sqlx::query(&sql)
            .bind(alias_val)
            .bind(table_name)
            .bind(column_name)
            .execute(&self.db)
            .await
            .map_err(TableRegistrationError::Sql)?;

        if result.rows_affected() == 0 {
            return Err(TableRegistrationError::ColumnNotFound(
                column_name.to_string(),
            ));
        }

        Ok(alias_val.unwrap_or("").to_string())
    }

    /// Marks a column as INVALIDATED.
    pub async fn invalidate_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<String, TableRegistrationError> {
        let registry_table = resolve_registry_table(column_name)?;
        let now = metalog_timeutil::epoch_nanos();

        // Get the current key before invalidating.
        let key_col = if column_name.starts_with("dim_") {
            "dim_key"
        } else {
            "agg_key"
        };
        let select_sql = format!(
            "SELECT {key_col} FROM {registry_table} WHERE table_name = ? AND column_name = ? AND \
             state = 'ACTIVE'"
        );
        let key: Option<(String,)> = sqlx::query_as(&select_sql)
            .bind(table_name)
            .bind(column_name)
            .fetch_optional(&self.db)
            .await
            .map_err(TableRegistrationError::Sql)?;

        let previous_key = key
            .map(|r| r.0)
            .ok_or_else(|| TableRegistrationError::ColumnNotFound(column_name.to_string()))?;

        let update_sql = format!(
            "UPDATE {registry_table} SET state = 'INVALIDATED', invalidated_at = ? WHERE \
             table_name = ? AND column_name = ? AND state = 'ACTIVE'"
        );
        sqlx::query(&update_sql)
            .bind(now)
            .bind(table_name)
            .bind(column_name)
            .execute(&self.db)
            .await
            .map_err(TableRegistrationError::Sql)?;

        tracing::info!(
            table = table_name,
            column = column_name,
            previous_key = %previous_key,
            "invalidated column"
        );

        Ok(previous_key)
    }
}

fn merge_config(stored: &mut serde_json::Value, patch: serde_json::Value) {
    if let (Some(target), Some(patch)) = (stored.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            merge_config(
                target.entry(key).or_insert(serde_json::Value::Null),
                value.clone(),
            );
        }
    } else {
        *stored = patch;
    }
}

fn validate_supported_config(json: &str) -> Result<(), TableRegistrationError> {
    let cfg = metalog_metastore::decode_table_config(Some(json))
        .map_err(|e| TableRegistrationError::InvalidConfig(e.to_string()))?;
    if cfg.consolidation.enabled {
        return Err(TableRegistrationError::InvalidConfig(
            "consolidation is not implemented in the Rust server".into(),
        ));
    }
    let r = cfg.retention;
    if r.enabled
        && ((!r.type_name.is_empty() && r.type_name != "default")
            || r.scan_interval_secs == 0
            || r.scan_interval_secs > i64::MAX as u64 / 1_000_000_000
            || r.delete_rate == 0
            || r.grace_period_secs > i64::MAX as u64 / 1_000_000_000)
    {
        return Err(TableRegistrationError::InvalidConfig(
            "retention requires default strategy, positive scan_interval_secs/delete_rate and a \
             valid grace period"
                .into(),
        ));
    }
    Ok(())
}

fn resolve_registry_table(column_name: &str) -> Result<&'static str, TableRegistrationError> {
    if column_name.starts_with("dim_") {
        Ok("_dim_registry")
    } else if column_name.starts_with("agg_") {
        Ok("_agg_registry")
    } else {
        Err(TableRegistrationError::InvalidColumnPrefix(
            column_name.to_string(),
        ))
    }
}

fn validate_alias(alias: &str) -> Result<(), TableRegistrationError> {
    if alias.len() > 128 {
        return Err(TableRegistrationError::InvalidAlias(
            "alias too long (max 128 chars)".into(),
        ));
    }
    // Allow alphanumeric, underscore, dot, hyphen, slash.
    if !alias
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-' || c == '/')
    {
        return Err(TableRegistrationError::InvalidAlias(format!(
            "alias contains invalid characters: {alias}"
        )));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TableRegistrationError {
    #[error("invalid table configuration: {0}")]
    InvalidConfig(String),
    #[error("provision: {0}")]
    Provision(#[source] metalog_schema::EnsureTableError),

    #[error("sql: {0}")]
    Sql(#[source] sqlx::Error),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("invalid column prefix: {0} (expected dim_ or agg_)")]
    InvalidColumnPrefix(String),

    #[error("invalid alias: {0}")]
    InvalidAlias(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_dim_registry() {
        assert_eq!(resolve_registry_table("dim_f01").unwrap(), "_dim_registry");
    }

    #[test]
    fn resolve_agg_registry() {
        assert_eq!(resolve_registry_table("agg_f01").unwrap(), "_agg_registry");
    }

    #[test]
    fn resolve_invalid_prefix() {
        assert!(resolve_registry_table("invalid_col").is_err());
    }

    #[test]
    fn validate_alias_valid() {
        assert!(validate_alias("my_alias").is_ok());
        assert!(validate_alias("path/to/field").is_ok());
        assert!(validate_alias("field-name").is_ok());
        assert!(validate_alias("field.name").is_ok());
    }

    #[test]
    fn validate_alias_invalid() {
        assert!(validate_alias("has space").is_err());
        assert!(validate_alias(&"a".repeat(129)).is_err());
    }

    #[test]
    fn error_display() {
        let err = TableRegistrationError::ColumnNotFound("dim_f99".into());
        assert!(err.to_string().contains("dim_f99"));
    }
}
