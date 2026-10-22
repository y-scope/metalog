use std::collections::HashMap;

use metalog_db::{quote_identifier, try_quote_identifier, validate_sql_identifier};
use serde_json::Value;
use sqlx::{Column, MySqlPool, Row, TypeInfo};

use crate::resolve::{hex_string, rewrite_filter_columns};

const INDEXED_SORT_COLUMNS: &[&str] = &["min_timestamp", "max_timestamp"];
/// Maximum number of rows materialized by one database page, not a stream limit.
pub const MAX_QUERY_LIMIT: i32 = 100_000;
/// Bounds memory and allows backpressure/cancellation between database pages.
pub const QUERY_PAGE_SIZE: i32 = 1_000;

#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub column: String,
    pub desc: bool,
}

#[derive(Debug)]
pub struct QueryParams {
    pub table_name: String,
    pub columns: Vec<String>,
    pub filter_expr: String,
    pub order_by: Vec<OrderBySpec>,
    pub limit: i32,
    pub allow_unindexed_sort: bool,
}

pub type SplitRow = HashMap<String, Value>;

pub struct SplitQueryEngine {
    db: MySqlPool,
}

impl SplitQueryEngine {
    pub const ALLOWED_AGGREGATES: &[&str] = &["MIN", "MAX", "SUM", "COUNT", "AVG", "ANY_VALUE"];

    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Returns at most `limit` rows. Streaming callers advance a keyset cursor;
    /// grouped queries aggregate in SQL before applying ordering and the limit.
    #[allow(clippy::too_many_arguments)]
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
        self.execute_page_offset(
            table_name,
            columns,
            filter_expr,
            order_clauses,
            keyset_where,
            limit,
            group_by,
            0,
        )
        .await
    }

    /// Offset is used only for grouped streams, whose public API has no cursor.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_page_offset(
        &self,
        table_name: &str,
        columns: &[String],
        filter_expr: &str,
        order_clauses: &[String],
        keyset_where: &str,
        limit: i32,
        group_by: &[String],
        offset: u64,
    ) -> Result<Vec<SplitRow>, EngineError> {
        let table = try_quote_identifier(table_name)
            .map_err(|e| EngineError::InvalidTableName(table_name.into(), e.to_string()))?;
        let filter_expr = rewrite_filter_columns(filter_expr, None)
            .map_err(|e| EngineError::InvalidFilter(e.to_string()))?;
        let keyset_where = rewrite_filter_columns(keyset_where, None)
            .map_err(|e| EngineError::InvalidCursor(e.to_string()))?;
        if limit < 0 {
            return Err(EngineError::InvalidLimit);
        }
        if !group_by.is_empty() && !keyset_where.is_empty() {
            return Err(EngineError::InvalidCursor(
                "group_by cannot be combined with a cursor".into(),
            ));
        }
        let cols_sql = Self::projection_sql(columns, group_by)?;
        let order_sql = order_clauses
            .iter()
            .map(|clause| {
                let mut parts = clause.split_whitespace();
                let column = parts.next().unwrap_or("").trim_matches('`');
                let direction = parts.next().unwrap_or("ASC").to_ascii_uppercase();
                if parts.next().is_some() || !matches!(direction.as_str(), "ASC" | "DESC") {
                    return Err(EngineError::InvalidSort(clause.clone()));
                }
                let column = try_quote_identifier(column)
                    .map_err(|_| EngineError::InvalidSort(clause.clone()))?;
                Ok(format!("{column} {direction}"))
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        use std::fmt::Write;
        let mut sql = format!("SELECT {cols_sql} FROM {table}");
        let mut conditions = Vec::new();
        if !filter_expr.trim().is_empty() {
            conditions.push(format!("({filter_expr})"));
        }
        if !keyset_where.trim().is_empty() {
            conditions.push(format!("({keyset_where})"));
        }
        if !conditions.is_empty() {
            write!(sql, " WHERE {}", conditions.join(" AND ")).unwrap();
        }
        if !group_by.is_empty() {
            let group = group_by
                .iter()
                .map(|c| {
                    try_quote_identifier(c)
                        .map_err(|_| EngineError::InvalidGroupByColumn(c.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            write!(sql, " GROUP BY {}", group.join(", ")).unwrap();
        }
        if !order_sql.is_empty() {
            write!(sql, " ORDER BY {}", order_sql.join(", ")).unwrap();
        }
        let page_limit = if limit == 0 {
            MAX_QUERY_LIMIT
        } else {
            limit.min(MAX_QUERY_LIMIT)
        };
        write!(sql, " LIMIT {page_limit}").unwrap();
        if offset > 0 {
            write!(sql, " OFFSET {offset}").unwrap();
        }
        tracing::debug!(sql = %sql, "executing split query page");
        let rows = sqlx::query(&sql).fetch_all(&self.db).await?;
        rows.iter()
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|column| {
                        Ok((
                            column.name().to_string(),
                            decode_column(row, column.ordinal())?,
                        ))
                    })
                    .collect::<Result<SplitRow, sqlx::Error>>()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(EngineError::Sql)
    }

    pub fn extract_column_name(expr: &str) -> &str {
        let expr = expr.trim();
        if let Some(start) = expr.find('(') {
            if expr.ends_with(')') {
                return expr[start + 1..expr.len() - 1].trim();
            }
        }
        expr
    }

    pub fn parse_aggregate_func(expr: &str) -> Option<String> {
        let expr = expr.trim();
        let start = expr.find('(')?;
        if !expr.ends_with(')') {
            return None;
        }
        let function = expr[..start].trim().to_ascii_uppercase();
        Self::ALLOWED_AGGREGATES
            .contains(&function.as_str())
            .then_some(function)
    }

    /// Validate grouping before spawning a streaming task, including empty projection.
    pub fn projection_sql(columns: &[String], group_by: &[String]) -> Result<String, EngineError> {
        if columns.is_empty() {
            return if group_by.is_empty() {
                Ok("*".into())
            } else {
                Err(EngineError::GroupByRequiresProjection)
            };
        }
        columns
            .iter()
            .map(|projection| {
                let column = Self::extract_column_name(projection);
                if let Some(function) = Self::parse_aggregate_func(projection) {
                    if group_by.is_empty() {
                        return Err(EngineError::InvalidProjection(projection.clone()));
                    }
                    let (argument, alias) = if column == "*" && function == "COUNT" {
                        ("*".into(), "record_count".into())
                    } else {
                        (
                            try_quote_identifier(column)
                                .map_err(|_| EngineError::InvalidProjection(projection.clone()))?,
                            column.to_string(),
                        )
                    };
                    // MariaDB 10.6 has no ANY_VALUE. MIN selects a valid representative
                    // and has the same result where archive metadata is constant per group.
                    let function = if function == "ANY_VALUE" {
                        "MIN"
                    } else {
                        function.as_str()
                    };
                    Ok(format!(
                        "{function}({argument}) AS {}",
                        quote_identifier(&alias)
                    ))
                } else {
                    let quoted = try_quote_identifier(projection)
                        .map_err(|_| EngineError::InvalidProjection(projection.clone()))?;
                    if !group_by.is_empty() && !group_by.contains(projection) {
                        return Err(EngineError::InvalidProjection(projection.clone()));
                    }
                    Ok(quoted)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| parts.join(", "))
    }

    pub fn validate_sort_columns(
        order_by: &[OrderBySpec],
        allow_unindexed: bool,
    ) -> Result<(), EngineError> {
        for order in order_by {
            validate_sql_identifier(&order.column)
                .map_err(|_| EngineError::InvalidSort(order.column.clone()))?;
            if !allow_unindexed && !INDEXED_SORT_COLUMNS.contains(&order.column.as_str()) {
                return Err(EngineError::UnindexedSort(order.column.clone()));
            }
        }
        Ok(())
    }

    pub fn build_order_clauses(order_by: &[OrderBySpec]) -> Vec<String> {
        let mut clauses: Vec<_> = order_by
            .iter()
            .map(|order| {
                let direction = if order.desc { "DESC" } else { "ASC" };
                format!("{} {direction}", quote_identifier(&order.column))
            })
            .collect();
        if !order_by.iter().any(|order| order.column == "id") {
            let direction = if order_by.first().is_some_and(|order| order.desc) {
                "DESC"
            } else {
                "ASC"
            };
            clauses.push(format!("`id` {direction}"));
        }
        clauses
    }

    /// Compatibility helper for callers that provide string cursor values.
    /// Invalid arity fails closed instead of indexing beyond the supplied slice.
    pub fn build_keyset_where(order_by: &[OrderBySpec], values: &[String], id: i64) -> String {
        if order_by.is_empty() && values.is_empty() {
            return String::new();
        }
        Self::build_typed_keyset_where(
            order_by,
            &values
                .iter()
                .cloned()
                .map(Value::String)
                .collect::<Vec<_>>(),
            id,
        )
        .unwrap_or_else(|_| "0 = 1".into())
    }

    /// Lexicographic keyset predicate matching MySQL's ASC NULL-first and
    /// DESC NULL-last ordering. Equality prefixes use the NULL-safe operator.
    pub fn build_typed_keyset_where(
        order_by: &[OrderBySpec],
        values: &[Value],
        id: i64,
    ) -> Result<String, EngineError> {
        if values.len() != order_by.len() || id < 0 {
            return Err(EngineError::InvalidCursor(
                "cursor must have one value per sort column and a nonnegative id".into(),
            ));
        }
        Self::validate_sort_columns(order_by, true)?;
        let mut columns: Vec<_> = order_by
            .iter()
            .map(|o| (o.column.clone(), o.desc))
            .collect();
        let mut values = values.to_vec();
        if !order_by.iter().any(|o| o.column == "id") {
            columns.push(("id".into(), order_by.first().is_some_and(|o| o.desc)));
            values.push(Value::from(id));
        }
        let mut equal = Vec::new();
        let mut branches = Vec::new();
        for ((column, desc), value) in columns.iter().zip(values.iter()) {
            let column = quote_identifier(column);
            let literal = sql_literal(value)?;
            let after = if value.is_null() {
                if *desc {
                    None
                } else {
                    Some(format!("{column} IS NOT NULL"))
                }
            } else if *desc {
                Some(format!("({column} < {literal} OR {column} IS NULL)"))
            } else {
                Some(format!("{column} > {literal}"))
            };
            if let Some(after) = after {
                let mut parts = equal.clone();
                parts.push(after);
                branches.push(format!("({})", parts.join(" AND ")));
            }
            equal.push(format!("{column} <=> {literal}"));
        }
        Ok(if branches.is_empty() {
            "0 = 1".into()
        } else {
            branches.join(" OR ")
        })
    }
}

fn sql_literal(value: &Value) -> Result<String, EngineError> {
    match value {
        Value::Null => Ok("NULL".into()),
        Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.into()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(format!("_utf8mb4 X'{}'", hex_string(value))),
        _ => Err(EngineError::InvalidCursor(
            "cursor values must be scalar".into(),
        )),
    }
}

/// Decode from schema metadata for every row; an initial NULL does not change
/// the type of later rows, and UNSIGNED columns use sqlx's unsigned decoder.
fn decode_column(row: &sqlx::mysql::MySqlRow, ordinal: usize) -> Result<Value, sqlx::Error> {
    let kind = row.columns()[ordinal].type_info().name();
    match kind {
        "BIGINT UNSIGNED" | "INT UNSIGNED" | "MEDIUMINT UNSIGNED" | "SMALLINT UNSIGNED"
        | "TINYINT UNSIGNED" => row
            .try_get::<Option<u64>, _>(ordinal)
            .map(|v| v.map(Value::from).unwrap_or(Value::Null)),
        "BIGINT" | "INT" | "MEDIUMINT" | "SMALLINT" | "TINYINT" => row
            .try_get::<Option<i64>, _>(ordinal)
            .map(|v| v.map(Value::from).unwrap_or(Value::Null)),
        "FLOAT" => row
            .try_get::<Option<f32>, _>(ordinal)
            .map(|v| v.map(|n| Value::from(f64::from(n))).unwrap_or(Value::Null)),
        "DOUBLE" => row
            .try_get::<Option<f64>, _>(ordinal)
            .map(|v| v.map(Value::from).unwrap_or(Value::Null)),
        "DECIMAL" | "NEWDECIMAL" => row
            .try_get::<Option<rust_decimal::Decimal>, _>(ordinal)
            .map(|v| {
                use rust_decimal::prelude::ToPrimitive;
                v.map(|v| {
                    if v.fract().is_zero() {
                        if let Some(n) = v.to_i64() {
                            return Value::from(n);
                        }
                        if let Some(n) = v.to_u64() {
                            return Value::from(n);
                        }
                    }
                    v.to_f64()
                        .map(Value::from)
                        .unwrap_or_else(|| Value::String(v.to_string()))
                })
                .unwrap_or(Value::Null)
            }),
        "NULL" => Ok(Value::Null),
        _ => row
            .try_get::<Option<String>, _>(ordinal)
            .or_else(|_| {
                row.try_get::<Option<Vec<u8>>, _>(ordinal)
                    .map(|v| v.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
            })
            .map(|v| v.map(Value::String).unwrap_or(Value::Null)),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "sort column {0:?} is not indexed; use min_timestamp or max_timestamp, or set \
         allow_unindexed_sort=true"
    )]
    UnindexedSort(String),
    #[error("invalid table name {0:?}: {1}")]
    InvalidTableName(String, String),
    #[error("group_by requires an explicit projection")]
    GroupByRequiresProjection,
    #[error("invalid group_by column: {0:?}")]
    InvalidGroupByColumn(String),
    #[error("invalid projection {0:?}; grouped columns require a group key or FUNC(column)")]
    InvalidProjection(String),
    #[error("invalid sort column: {0:?}")]
    InvalidSort(String),
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    #[error("limit must be nonnegative")]
    InvalidLimit,
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
        assert!(sql.contains("`max_timestamp` < _utf8mb4 X'35303030'"));
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
        assert!(sql.contains("`min_timestamp` > _utf8mb4 X'31303030'"));
        assert!(sql.contains("`id` > 10"));
    }

    #[test]
    fn reject_invalid_table_name() {
        // Table names with special chars should be rejected.
        assert!(SplitQueryEngine::validate_sort_columns(&[], true).is_ok());
        // We can't call execute_page without a DB, but try_quote_identifier
        // validates the same thing — verify it rejects dangerous names.
        assert!(try_quote_identifier("DROP TABLE").is_err());
        assert!(try_quote_identifier("has space").is_err());
        assert!(try_quote_identifier("has-dash").is_err());
        assert!(try_quote_identifier("").is_err());
        // Valid names should pass.
        assert!(try_quote_identifier("clp_files").is_ok());
        assert!(try_quote_identifier("my_table_1").is_ok());
    }

    #[test]
    fn filter_expr_or_wrapped_in_parens() {
        // A filter with OR must be parenthesized when combined with keyset WHERE,
        // otherwise `a = 1 OR b = 2 AND keyset` is parsed as
        // `a = 1 OR (b = 2 AND keyset)` — wrong results.
        let filter = "state = 1 OR state = 2".to_string();
        let keyset = "`min_timestamp` > _utf8mb4 X'31303030'".to_string();
        // Replicate the condition-joining logic from execute_page.
        let mut conditions = Vec::new();
        if !filter.is_empty() {
            conditions.push(format!("({filter})"));
        }
        if !keyset.is_empty() {
            conditions.push(format!("({keyset})"));
        }
        let where_clause = conditions.join(" AND ");
        assert_eq!(
            where_clause,
            "(state = 1 OR state = 2) AND (`min_timestamp` > _utf8mb4 X'31303030')"
        );
        // Without the fix, this would produce:
        // "state = 1 OR state = 2 AND (`min_timestamp` > _utf8mb4 X'31303030')"
    }

    #[test]
    fn keyset_where_empty() {
        let sql = SplitQueryEngine::build_keyset_where(&[], &[], 0);
        assert!(sql.is_empty());
    }

    #[test]
    fn max_query_limit_constant() {
        // Verify MAX_QUERY_LIMIT is set correctly.
        assert_eq!(MAX_QUERY_LIMIT, 100_000);
    }
}
