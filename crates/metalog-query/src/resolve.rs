use metalog_schema::ColumnRegistry;

/// Resolves a column reference with virtual namespace prefixes.
///
/// Supported prefixes:
/// - `__FILE.xxx` → base file column (e.g., `__FILE.min_timestamp` → `min_timestamp`)
/// - `__DIM.xxx` → dimension column (e.g., `__DIM.hostname` → `dim_f01`)
/// - Unprefixed → passed through as-is (for base column names)
///
/// Premium prefixes (`__AGG.*`) are handled by the AggProcessor if present.
pub async fn resolve_column_ref(
    raw: &str,
    registry: Option<&ColumnRegistry>,
) -> Result<String, ResolveError> {
    // __FILE.xxx → strip prefix, return base column name.
    if let Some(col) = raw.strip_prefix("__FILE.") {
        metalog_db::validate_sql_identifier(col)
            .map_err(|e| ResolveError::InvalidColumn(e.to_string()))?;
        return Ok(col.to_string());
    }

    // __DIM.xxx → resolve via registry.
    if let Some(dim_key) = raw.strip_prefix("__DIM.") {
        let reg = registry.ok_or_else(|| {
            ResolveError::InvalidColumn("no registry available for __DIM resolution".into())
        })?;
        let col = reg.resolve_dim(dim_key).await.ok_or_else(|| {
            ResolveError::UnknownColumn(format!("dimension {dim_key} not found in registry"))
        })?;
        return Ok(col);
    }

    // __AGG.* → not handled in base (premium AggProcessor provides this).
    if raw.starts_with("__AGG") {
        return Err(ResolveError::PremiumRequired(
            "__AGG.* column resolution requires premium edition".into(),
        ));
    }

    // Unprefixed — validate and pass through.
    metalog_db::validate_sql_identifier(raw)
        .map_err(|e| ResolveError::InvalidColumn(e.to_string()))?;
    Ok(raw.to_string())
}

/// Resolves a list of projection columns, expanding virtual namespace prefixes.
pub async fn resolve_projection_columns(
    columns: &[String],
    registry: Option<&ColumnRegistry>,
) -> Result<Vec<String>, ResolveError> {
    let mut resolved = Vec::with_capacity(columns.len());
    for col in columns {
        resolved.push(resolve_column_ref(col, registry).await?);
    }
    Ok(resolved)
}

/// Rewrites column references in a filter expression.
///
/// Replaces `__FILE.xxx` and `__DIM.xxx` prefixes with physical column names.
/// This is a simple string-based rewrite — the filter has already been validated.
pub async fn rewrite_filter_columns(
    filter: &str,
    registry: Option<&ColumnRegistry>,
) -> Result<String, ResolveError> {
    let mut result = filter.to_string();

    // Replace __FILE.xxx references.
    while let Some(pos) = result.find("__FILE.") {
        let start = pos;
        let rest = &result[pos + 7..];
        let end = rest
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(rest.len());
        let col_name = &rest[..end];
        result = format!("{}{col_name}{}", &result[..start], &rest[end..]);
    }

    // Replace __DIM.xxx references.
    while let Some(pos) = result.find("__DIM.") {
        let start = pos;
        let rest = &result[pos + 6..];
        let end = rest
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(rest.len());
        let dim_key = &rest[..end];

        let physical = if let Some(reg) = registry {
            reg.resolve_dim(dim_key).await.ok_or_else(|| {
                ResolveError::UnknownColumn(format!("dimension {dim_key} not found"))
            })?
        } else {
            return Err(ResolveError::InvalidColumn(
                "no registry for __DIM resolution".into(),
            ));
        };

        result = format!("{}{physical}{}", &result[..start], &rest[end..]);
    }

    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("invalid column: {0}")]
    InvalidColumn(String),

    #[error("unknown column: {0}")]
    UnknownColumn(String),

    #[error("premium required: {0}")]
    PremiumRequired(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_file_column() {
        let col = resolve_column_ref("__FILE.min_timestamp", None)
            .await
            .unwrap();
        assert_eq!(col, "min_timestamp");
    }

    #[tokio::test]
    async fn resolve_unprefixed() {
        let col = resolve_column_ref("state", None).await.unwrap();
        assert_eq!(col, "state");
    }

    #[tokio::test]
    async fn resolve_dim_no_registry() {
        let result = resolve_column_ref("__DIM.hostname", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn resolve_agg_premium() {
        let result = resolve_column_ref("__AGG.level.error", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("premium"));
    }

    #[tokio::test]
    async fn resolve_invalid_identifier() {
        let result = resolve_column_ref("DROP TABLE", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rewrite_file_prefix() {
        let result = rewrite_filter_columns("__FILE.min_timestamp > 1000", None)
            .await
            .unwrap();
        assert_eq!(result, "min_timestamp > 1000");
    }

    #[tokio::test]
    async fn rewrite_multiple_prefixes() {
        let result = rewrite_filter_columns(
            "__FILE.min_timestamp > 1000 AND __FILE.max_timestamp < 2000",
            None,
        )
        .await
        .unwrap();
        assert_eq!(result, "min_timestamp > 1000 AND max_timestamp < 2000");
    }
}
