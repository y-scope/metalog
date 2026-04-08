use std::collections::HashMap;

use metalog_db::{quote_identifier, validate_sql_identifier};
use sqlx::{Column, MySqlPool};

/// Indexed sort columns that support efficient keyset pagination.
const INDEXED_SORT_COLUMNS: &[&str] = &["min_timestamp", "max_timestamp"];

/// Sort specification for ORDER BY.
#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub column: String,
    pub desc: bool,
}

/// Parameters for preparing a split query.
#[derive(Debug)]
pub struct QueryParams {
    pub table_name: String,
    pub columns: Vec<String>,
    pub filter_expr: String,
    pub order_by: Vec<OrderBySpec>,
    pub limit: i32,
    pub allow_unindexed_sort: bool,
}

/// A single result row from a split query.
pub type SplitRow = HashMap<String, sqlx::types::JsonValue>;

/// Executes paginated queries on metadata tables using keyset pagination.
///
/// The engine pre-validates queries (column names, filter safety, sort indexes)
/// and caches rewritten filter expressions. Each page is fetched with a keyset
/// WHERE clause built from the previous page's last row.
pub struct SplitQueryEngine {
    db: MySqlPool,
}

impl SplitQueryEngine {
    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Executes a single page of results with keyset pagination.
    ///
    /// When `group_by` is non-empty, the query is rewritten as an aggregate
    /// query: columns in `group_by` are selected as-is, while known numeric
    /// columns get SUM/MIN/MAX wrappers and all others default to MAX.
    pub async fn execute_page(
        &self,
        table_name: &str,
        columns: &[String],
        filter_expr: &str,
        order_clauses: &[String],
        keyset_where: &str,
        limit: i32,
        group_by: &[String],
    ) -> Result<Vec<SplitRow>, EngineError> {
        let cols_sql = if !group_by.is_empty() {
            // GROUP BY mode: projection must be explicit and caller specifies aggregations.
            // Projection expressions like "MIN(min_timestamp)", "archive_path", "SUM(record_count)"
            // are passed through as-is. The group_by columns are validated as identifiers.
            if columns.is_empty() {
                return Err(EngineError::GroupByRequiresProjection);
            }
            for col in group_by {
                validate_sql_identifier(col)
                    .map_err(|_| EngineError::InvalidGroupByColumn(col.clone()))?;
            }
            // Validate projection expressions: each must be either a bare identifier or
            // an aggregate function call (FUNC(identifier) or FUNC(identifier) AS identifier).
            columns
                .iter()
                .map(|c| Self::validate_and_format_projection_expr(c))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        } else if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        use std::fmt::Write;
        let mut sql = format!("SELECT {cols_sql} FROM `{table_name}`");

        // WHERE clauses.
        let mut conditions = Vec::new();
        if !filter_expr.is_empty() {
            conditions.push(filter_expr.to_string());
        }
        if !keyset_where.is_empty() {
            conditions.push(format!("({keyset_where})"));
        }
        if !conditions.is_empty() {
            write!(sql, " WHERE {}", conditions.join(" AND ")).unwrap();
        }

        // GROUP BY.
        if !group_by.is_empty() {
            let gb_cols: Vec<String> = group_by.iter().map(|c| quote_identifier(c)).collect();
            write!(sql, " GROUP BY {}", gb_cols.join(", ")).unwrap();
        }

        // ORDER BY.
        if !order_clauses.is_empty() {
            write!(sql, " ORDER BY {}", order_clauses.join(", ")).unwrap();
        }

        // LIMIT (fetch limit+1 for truncation detection).
        let fetch_limit = if limit > 0 { limit + 1 } else { 10000 };
        write!(sql, " LIMIT {fetch_limit}").unwrap();

        tracing::info!(sql = %sql, "executing split query");

        let rows = sqlx::query(&sql).fetch_all(&self.db).await?;

        let mut results = Vec::with_capacity(rows.len());

        // Detect column types from the first row, then reuse for subsequent rows.
        // Avoids repeated try_get failures (each creates a discarded error).
        let mut col_types: Vec<ColType> = Vec::new();

        for (row_idx, row) in rows.iter().enumerate() {
            use sqlx::Row;
            let mut split_row = SplitRow::new();

            if row_idx == 0 {
                // First row: detect types via try cascade, cache the result.
                col_types.reserve(row.columns().len());
                for col in row.columns() {
                    let name = col.name().to_string();
                    let (val, typ) = detect_column_value(row, col.ordinal());
                    tracing::debug!(column = %name, ?typ, value = %val, "detected column");
                    col_types.push(typ);
                    split_row.insert(name, val);
                }
            } else {
                // Subsequent rows: use cached types directly.
                for (col, typ) in row.columns().iter().zip(col_types.iter()) {
                    let name = col.name().to_string();
                    let val = extract_by_type(row, col.ordinal(), *typ);
                    split_row.insert(name, val);
                }
            }

            results.push(split_row);
        }

        Ok(results)
    }

    /// Allowed aggregate functions in projection expressions.
    const ALLOWED_AGGREGATES: &[&str] = &["MIN", "MAX", "SUM", "COUNT", "AVG", "ANY_VALUE"];

    /// Validates and formats a projection expression for GROUP BY queries.
    ///
    /// Accepts:
    /// - Bare identifiers: `"archive_path"` → `` `archive_path` ``
    /// - Aggregate calls: `"MIN(min_timestamp)"` → `` MIN(`min_timestamp`) AS `min_timestamp` ``
    /// - Allowed aggregates: MIN, MAX, SUM, COUNT, AVG
    fn validate_and_format_projection_expr(expr: &str) -> Result<String, EngineError> {
        // Check for aggregate function pattern: FUNC(column_name)
        if let Some(paren_start) = expr.find('(') {
            if !expr.ends_with(')') {
                return Err(EngineError::InvalidProjection(expr.to_string()));
            }
            let func = &expr[..paren_start].trim().to_uppercase();
            let col_name = &expr[paren_start + 1..expr.len() - 1].trim();

            // Validate the column name inside the function.
            validate_sql_identifier(col_name)
                .map_err(|_| EngineError::InvalidProjection(expr.to_string()))?;
            let quoted_col = quote_identifier(col_name);

            // Validate function name.
            if !Self::ALLOWED_AGGREGATES.contains(&func.as_str()) {
                return Err(EngineError::InvalidProjection(expr.to_string()));
            }

            Ok(format!("{func}({quoted_col}) AS {quoted_col}"))
        } else {
            // Bare identifier.
            validate_sql_identifier(expr)
                .map_err(|_| EngineError::InvalidProjection(expr.to_string()))?;
            Ok(quote_identifier(expr))
        }
    }

    /// Validates that all ORDER BY columns are indexed (unless `allow_unindexed` is set).
    pub fn validate_sort_columns(
        order_by: &[OrderBySpec],
        allow_unindexed: bool,
    ) -> Result<(), EngineError> {
        if !allow_unindexed {
            for ob in order_by {
                if !INDEXED_SORT_COLUMNS.contains(&ob.column.as_str()) {
                    return Err(EngineError::UnindexedSort(ob.column.clone()));
                }
            }
        }
        Ok(())
    }

    /// Builds the ORDER BY clause with id tiebreaker.
    ///
    /// The id tiebreaker direction matches the primary sort direction to allow
    /// the database to use a composite index scan instead of a filesort.
    pub fn build_order_clauses(order_by: &[OrderBySpec]) -> Vec<String> {
        let mut clauses: Vec<String> = order_by
            .iter()
            .map(|ob| {
                let dir = if ob.desc { "DESC" } else { "ASC" };
                format!("{} {dir}", quote_identifier(&ob.column))
            })
            .collect();

        // Id tiebreaker matches primary sort direction.
        let id_dir = if order_by.first().is_some_and(|ob| ob.desc) {
            "DESC"
        } else {
            "ASC"
        };
        clauses.push(format!("`id` {id_dir}"));

        clauses
    }

    /// Builds the keyset WHERE clause for cursor-based pagination.
    ///
    /// For ORDER BY c1 DESC, c2 ASC, id DESC with cursor (v1, v2, vid):
    /// ```sql
    /// (c1 < v1)
    ///   OR (c1 = v1 AND c2 > v2)
    ///   OR (c1 = v1 AND c2 IS NULL)
    ///   OR (c1 = v1 AND c2 = v2 AND id < vid)
    /// ```
    pub fn build_keyset_where(
        order_by: &[OrderBySpec],
        cursor_values: &[String],
        cursor_id: i64,
    ) -> String {
        if order_by.is_empty() || cursor_values.is_empty() {
            return String::new();
        }

        let mut conditions = Vec::new();

        for i in 0..order_by.len() {
            let ob = &order_by[i];
            let col = quote_identifier(&ob.column);
            let op = if ob.desc { "<" } else { ">" };
            let val = escape_sql_value(&cursor_values[i]);

            if i == 0 {
                conditions.push(format!("({col} {op} {val})"));
            } else {
                let mut prefix_parts = Vec::new();
                for j in 0..i {
                    let prev_col = quote_identifier(&order_by[j].column);
                    let prev_val = escape_sql_value(&cursor_values[j]);
                    prefix_parts.push(format!("{prev_col} = {prev_val}"));
                }
                let prefix = prefix_parts.join(" AND ");
                conditions.push(format!("({prefix} AND {col} {op} {val})"));
            }
        }

        // Id tiebreaker.
        let id_op = if order_by.first().is_some_and(|ob| ob.desc) {
            "<"
        } else {
            ">"
        };
        let mut id_prefix_parts = Vec::new();
        for (j, ob) in order_by.iter().enumerate() {
            let col = quote_identifier(&ob.column);
            let val = escape_sql_value(&cursor_values[j]);
            id_prefix_parts.push(format!("{col} = {val}"));
        }
        let id_prefix = id_prefix_parts.join(" AND ");
        conditions.push(format!("({id_prefix} AND `id` {id_op} {cursor_id})"));

        conditions.join(" OR ")
    }
}

/// Column type detected from the first row, cached for subsequent rows.
#[derive(Debug, Clone, Copy)]
enum ColType {
    Int,
    Str,
    Float,
    Decimal,
    Null,
}

/// Detects the column type using the database column type metadata.
///
/// We use the column type name from the database rather than try-cast, because MySQL/MariaDB
/// will silently cast VARCHAR values like "5075fd79-..." to integer 5075, giving wrong results.
fn detect_column_value(
    row: &sqlx::mysql::MySqlRow,
    ordinal: usize,
) -> (serde_json::Value, ColType) {
    use sqlx::{Column, Row, TypeInfo};

    let col = &row.columns()[ordinal];
    let type_name = col.type_info().name();
    match type_name {
        // Integer types
        "BIGINT" | "INT" | "MEDIUMINT" | "SMALLINT" | "TINYINT" | "BIGINT UNSIGNED"
        | "INT UNSIGNED" | "MEDIUMINT UNSIGNED" | "SMALLINT UNSIGNED" | "TINYINT UNSIGNED" => {
            if let Ok(v) = row.try_get::<i64, _>(ordinal) {
                (serde_json::json!(v), ColType::Int)
            } else {
                (serde_json::Value::Null, ColType::Null)
            }
        }
        // DECIMAL is returned by SUM/AVG aggregates in MariaDB.
        // Use rust_decimal to read natively, then convert to i64 or f64.
        "DECIMAL" | "NEWDECIMAL" => {
            if let Ok(v) = row.try_get::<rust_decimal::Decimal, _>(ordinal) {
                use rust_decimal::prelude::ToPrimitive;
                if let Some(n) = v.to_i64() {
                    (serde_json::json!(n), ColType::Decimal)
                } else if let Some(n) = v.to_f64() {
                    (serde_json::json!(n), ColType::Decimal)
                } else {
                    (serde_json::json!(v.to_string()), ColType::Decimal)
                }
            } else {
                (serde_json::Value::Null, ColType::Null)
            }
        }
        "FLOAT" | "DOUBLE" => {
            if let Ok(v) = row.try_get::<f64, _>(ordinal) {
                (serde_json::json!(v), ColType::Float)
            } else {
                (serde_json::Value::Null, ColType::Null)
            }
        }
        // Everything else (VARCHAR, TEXT, ENUM, VARBINARY, etc.) -> String
        _ => {
            // For VARBINARY columns (utf8mb4_bin collation), try_get::<String> may fail.
            // Fall back to reading raw bytes and converting to String.
            if let Ok(v) = row.try_get::<String, _>(ordinal) {
                (serde_json::json!(v), ColType::Str)
            } else if let Ok(v) = row.try_get::<Vec<u8>, _>(ordinal) {
                let s = String::from_utf8_lossy(&v).to_string();
                (serde_json::json!(s), ColType::Str)
            } else {
                (serde_json::Value::Null, ColType::Null)
            }
        }
    }
}

/// Extracts a value using the cached column type, avoiding failed try_get calls.
fn extract_by_type(
    row: &sqlx::mysql::MySqlRow,
    ordinal: usize,
    typ: ColType,
) -> serde_json::Value {
    use sqlx::Row;
    match typ {
        ColType::Int => row
            .try_get::<i64, _>(ordinal)
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        ColType::Str => row
            .try_get::<String, _>(ordinal)
            .or_else(|_| {
                row.try_get::<Vec<u8>, _>(ordinal)
                    .map(|v| String::from_utf8_lossy(&v).to_string())
            })
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        ColType::Float => row
            .try_get::<f64, _>(ordinal)
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        ColType::Decimal => row
            .try_get::<rust_decimal::Decimal, _>(ordinal)
            .map(|v| {
                use rust_decimal::prelude::ToPrimitive;
                if let Some(n) = v.to_i64() {
                    serde_json::json!(n)
                } else if let Some(n) = v.to_f64() {
                    serde_json::json!(n)
                } else {
                    serde_json::json!(v.to_string())
                }
            })
            .unwrap_or(serde_json::Value::Null),
        ColType::Null => serde_json::Value::Null,
    }
}

/// Escapes a value for safe SQL embedding in keyset WHERE clauses.
/// Wraps in single quotes with backslash/quote escaping.
/// Escapes a value for safe SQL embedding. Handles all MySQL-special characters.
fn escape_sql_value(val: &str) -> String {
    let mut escaped = String::with_capacity(val.len() + 2);
    escaped.push('\'');
    for c in val.chars() {
        match c {
            '\'' => escaped.push_str("\\'"),
            '\\' => escaped.push_str("\\\\"),
            '\0' => escaped.push_str("\\0"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\x1a' => escaped.push_str("\\Z"),
            _ => escaped.push(c),
        }
    }
    escaped.push('\'');
    escaped
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "sort column {0:?} is not indexed; use min_timestamp or max_timestamp, or set \
         allow_unindexed_sort=true"
    )]
    UnindexedSort(String),

    #[error("group_by requires an explicit projection (columns must not be empty)")]
    GroupByRequiresProjection,

    #[error("invalid group_by column: {0:?}")]
    InvalidGroupByColumn(String),

    #[error("invalid projection expression: {0:?} (use bare column names or FUNC(column))")]
    InvalidProjection(String),

    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_indexed_sort() {
        let order = vec![OrderBySpec {
            column: "max_timestamp".into(),
            desc: true,
        }];
        assert!(SplitQueryEngine::validate_sort_columns(&order, false).is_ok());
    }

    #[test]
    fn reject_unindexed_sort() {
        let order = vec![OrderBySpec {
            column: "state".into(),
            desc: false,
        }];
        assert!(SplitQueryEngine::validate_sort_columns(&order, false).is_err());
    }

    #[test]
    fn allow_unindexed_sort() {
        let order = vec![OrderBySpec {
            column: "state".into(),
            desc: false,
        }];
        assert!(SplitQueryEngine::validate_sort_columns(&order, true).is_ok());
    }

    #[test]
    fn order_clauses_asc() {
        let order = vec![OrderBySpec {
            column: "min_timestamp".into(),
            desc: false,
        }];
        let clauses = SplitQueryEngine::build_order_clauses(&order);
        assert_eq!(clauses, vec!["`min_timestamp` ASC", "`id` ASC"]);
    }

    #[test]
    fn order_clauses_desc() {
        let order = vec![OrderBySpec {
            column: "max_timestamp".into(),
            desc: true,
        }];
        let clauses = SplitQueryEngine::build_order_clauses(&order);
        assert_eq!(clauses, vec!["`max_timestamp` DESC", "`id` DESC"]);
    }

    #[test]
    fn order_clauses_multi() {
        let order = vec![
            OrderBySpec {
                column: "max_timestamp".into(),
                desc: true,
            },
            OrderBySpec {
                column: "min_timestamp".into(),
                desc: false,
            },
        ];
        let clauses = SplitQueryEngine::build_order_clauses(&order);
        assert_eq!(
            clauses,
            vec!["`max_timestamp` DESC", "`min_timestamp` ASC", "`id` DESC"]
        );
    }

    #[test]
    fn keyset_where_single_desc() {
        let order = vec![OrderBySpec {
            column: "max_timestamp".into(),
            desc: true,
        }];
        let cursor = vec!["5000".to_string()];
        let sql = SplitQueryEngine::build_keyset_where(&order, &cursor, 42);
        assert!(sql.contains("`max_timestamp` < '5000'"));
        assert!(sql.contains("`id` < 42"));
    }

    #[test]
    fn keyset_where_single_asc() {
        let order = vec![OrderBySpec {
            column: "min_timestamp".into(),
            desc: false,
        }];
        let cursor = vec!["1000".to_string()];
        let sql = SplitQueryEngine::build_keyset_where(&order, &cursor, 10);
        assert!(sql.contains("`min_timestamp` > '1000'"));
        assert!(sql.contains("`id` > 10"));
    }

    #[test]
    fn keyset_where_empty() {
        let sql = SplitQueryEngine::build_keyset_where(&[], &[], 0);
        assert!(sql.is_empty());
    }
}
