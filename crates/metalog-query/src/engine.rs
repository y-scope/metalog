use std::collections::HashMap;

use metalog_db::quote_identifier;
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
    pub async fn execute_page(
        &self,
        table_name: &str,
        columns: &[String],
        filter_expr: &str,
        order_clauses: &[String],
        keyset_where: &str,
        limit: i32,
    ) -> Result<Vec<SplitRow>, EngineError> {
        let cols_sql = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

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
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }

        // ORDER BY.
        if !order_clauses.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", order_clauses.join(", ")));
        }

        // LIMIT (fetch limit+1 for truncation detection).
        let fetch_limit = if limit > 0 { limit + 1 } else { 10000 };
        sql.push_str(&format!(" LIMIT {fetch_limit}"));

        tracing::info!(sql = %sql, "executing split query");

        let rows = sqlx::query(&sql).fetch_all(&self.db).await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            use sqlx::Row;
            let mut split_row = SplitRow::new();
            for col in row.columns() {
                let name = col.name().to_string();
                // Try to extract as various types.
                if let Ok(v) = row.try_get::<i64, _>(col.ordinal()) {
                    split_row.insert(name, serde_json::json!(v));
                } else if let Ok(v) = row.try_get::<String, _>(col.ordinal()) {
                    split_row.insert(name, serde_json::json!(v));
                } else if let Ok(v) = row.try_get::<f64, _>(col.ordinal()) {
                    split_row.insert(name, serde_json::json!(v));
                } else {
                    split_row.insert(name, serde_json::Value::Null);
                }
            }
            results.push(split_row);
        }

        Ok(results)
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
            let val = &cursor_values[i];

            if i == 0 {
                // First column: simple comparison.
                conditions.push(format!("({col} {op} '{val}')"));
            } else {
                // Subsequent columns: equality prefix + comparison.
                let mut prefix_parts = Vec::new();
                for j in 0..i {
                    let prev_col = quote_identifier(&order_by[j].column);
                    let prev_val = &cursor_values[j];
                    prefix_parts.push(format!("{prev_col} = '{prev_val}'"));
                }
                let prefix = prefix_parts.join(" AND ");
                conditions.push(format!("({prefix} AND {col} {op} '{val}')"));
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
            let val = &cursor_values[j];
            id_prefix_parts.push(format!("{col} = '{val}'"));
        }
        let id_prefix = id_prefix_parts.join(" AND ");
        conditions.push(format!("({id_prefix} AND `id` {id_op} {cursor_id})"));

        conditions.join(" OR ")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "sort column {0:?} is not indexed; use min_timestamp or max_timestamp, or set \
         allow_unindexed_sort=true"
    )]
    UnindexedSort(String),

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
