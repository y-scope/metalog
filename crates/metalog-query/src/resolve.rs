use metalog_schema::ColumnRegistry;
use sqlparser::ast::{Expr, Ident, Value};

use crate::{engine::SplitQueryEngine, filter::parse_filter};

pub const FILE_COLUMNS: &[&str] = &[
    "id",
    "min_timestamp",
    "max_timestamp",
    "state",
    "record_count",
    "raw_size_bytes",
    "file_path",
    "file_storage_backend",
    "file_bucket",
    "file_size_bytes",
    "archive_path",
    "archive_storage_backend",
    "archive_bucket",
    "archive_size_bytes",
    "archive_created_at",
    "retention_days",
    "expires_at",
];

pub fn resolve_column_ref(
    raw: &str,
    registry: Option<&ColumnRegistry>,
) -> Result<String, ResolveError> {
    if let Some(column) = raw.strip_prefix("__FILE.") {
        if !FILE_COLUMNS.contains(&column) {
            return Err(ResolveError::UnknownColumn(raw.into()));
        }
        return Ok(column.into());
    }
    if let Some(key) = raw.strip_prefix("__DIM.") {
        let registry = registry.ok_or_else(|| {
            ResolveError::InvalidColumn("no registry available for __DIM resolution".into())
        })?;
        return registry
            .resolve_dim(key)
            .or_else(|| {
                registry
                    .all_dim_entries()
                    .into_iter()
                    .find(|entry| entry.alias_col.as_deref() == Some(key))
                    .map(|entry| entry.column_name)
            })
            .ok_or_else(|| ResolveError::UnknownColumn(format!("dimension {key} not found")));
    }
    if raw.starts_with("__AGG") {
        return Err(ResolveError::PremiumRequired(
            "__AGG.* column resolution requires premium edition".into(),
        ));
    }
    metalog_db::validate_sql_identifier(raw)
        .map_err(|e| ResolveError::InvalidColumn(e.to_string()))?;
    Ok(raw.into())
}

/// Resolve namespaced columns and the documented FUNC(column) grouped projections.
pub fn resolve_projection_columns(
    columns: &[String],
    registry: Option<&ColumnRegistry>,
) -> Result<Vec<String>, ResolveError> {
    let mut resolved = Vec::new();
    for column in columns {
        let expanded = match column.as_str() {
            "*" if columns.len() == 1 => return Ok(Vec::new()),
            "__FILE.*" => FILE_COLUMNS.iter().map(|s| s.to_string()).collect(),
            "__DIM.*" => registry
                .map(|r| {
                    r.all_dim_entries()
                        .into_iter()
                        .map(|e| e.column_name)
                        .collect()
                })
                .unwrap_or_default(),
            _ => {
                let value = if let Some(func) = SplitQueryEngine::parse_aggregate_func(column) {
                    let inner = SplitQueryEngine::extract_column_name(column);
                    if inner == "*" && func == "COUNT" {
                        "COUNT(*)".into()
                    } else {
                        format!("{func}({})", resolve_column_ref(inner, registry)?)
                    }
                } else {
                    resolve_column_ref(column, registry)?
                };
                vec![value]
            }
        };
        for value in expanded {
            if !resolved.contains(&value) {
                resolved.push(value);
            }
        }
    }
    Ok(resolved)
}

/// Rewrites parsed identifiers, never text inside string literals. Hex literals preserve
/// UTF-8 and backslashes independently of the server's NO_BACKSLASH_ESCAPES setting.
pub fn rewrite_filter_columns(
    filter: &str,
    registry: Option<&ColumnRegistry>,
) -> Result<String, ResolveError> {
    if filter.trim().is_empty() {
        return Ok(String::new());
    }
    let mut expression =
        parse_filter(filter).map_err(|e| ResolveError::InvalidColumn(e.to_string()))?;
    rewrite_expr(&mut expression, registry)?;
    Ok(expression.to_string())
}

fn rewrite_expr(expr: &mut Expr, registry: Option<&ColumnRegistry>) -> Result<(), ResolveError> {
    match expr {
        Expr::Identifier(identifier) => {
            let column = resolve_column_ref(&identifier.value, registry)?;
            *identifier = Ident::new(column);
        }
        Expr::CompoundIdentifier(parts) => {
            let raw = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            *expr = Expr::Identifier(Ident::new(resolve_column_ref(&raw, registry)?));
        }
        Expr::Value(Value::SingleQuotedString(value) | Value::DoubleQuotedString(value)) => {
            *expr = Expr::IntroducedString {
                introducer: "_utf8mb4".into(),
                value: Value::HexStringLiteral(hex_string(value)),
            };
        }
        Expr::IntroducedString { value, .. } => {
            if let Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) = value {
                *value = Value::HexStringLiteral(hex_string(text));
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left, registry)?;
            rewrite_expr(right, registry)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::Nested(expr) => rewrite_expr(expr, registry)?,
        Expr::InList { expr, list, .. } => {
            rewrite_expr(expr, registry)?;
            for value in list {
                rewrite_expr(value, registry)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr(expr, registry)?;
            rewrite_expr(low, registry)?;
            rewrite_expr(high, registry)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::RLike { expr, pattern, .. } => {
            rewrite_expr(expr, registry)?;
            rewrite_expr(pattern, registry)?;
        }
        _ => {} // parse_filter already rejected unsafe expression variants.
    }
    Ok(())
}

pub(crate) fn hex_string(value: &str) -> String {
    use std::fmt::Write;
    let mut hex = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(hex, "{byte:02x}").unwrap();
    }
    hex
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

    #[test]
    fn resolve_file_column() {
        let col = resolve_column_ref("__FILE.min_timestamp", None).unwrap();
        assert_eq!(col, "min_timestamp");
    }

    #[test]
    fn resolve_unprefixed() {
        let col = resolve_column_ref("state", None).unwrap();
        assert_eq!(col, "state");
    }

    #[test]
    fn resolve_dim_no_registry() {
        assert!(resolve_column_ref("__DIM.hostname", None).is_err());
    }

    #[test]
    fn resolve_agg_premium() {
        let result = resolve_column_ref("__AGG.level.error", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("premium"));
    }

    #[test]
    fn resolve_invalid_identifier() {
        assert!(resolve_column_ref("DROP TABLE", None).is_err());
    }

    #[test]
    fn rewrite_file_prefix() {
        let result = rewrite_filter_columns("__FILE.min_timestamp > 1000", None).unwrap();
        assert_eq!(result, "min_timestamp > 1000");
    }

    #[test]
    fn rewrite_multiple_prefixes() {
        let result = rewrite_filter_columns(
            "__FILE.min_timestamp > 1000 AND __FILE.max_timestamp < 2000",
            None,
        )
        .unwrap();
        assert_eq!(result, "min_timestamp > 1000 AND max_timestamp < 2000");
    }
}
