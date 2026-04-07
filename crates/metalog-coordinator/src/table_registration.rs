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
        _display_name: &str,
        opts: RegisterTableOpts,
    ) -> Result<bool, TableRegistrationError> {
        let created = ensure_table(&self.db, table_name, opts.config_json.as_deref())
            .await
            .map_err(TableRegistrationError::Provision)?;

        Ok(created)
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
