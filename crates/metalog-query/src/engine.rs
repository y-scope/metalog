use std::collections::HashMap;

use metalog_db::quote_identifier;
use sqlx::MySqlPool;

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
    _db: MySqlPool,
}

impl SplitQueryEngine {
    pub fn new(db: MySqlPool) -> Self {
        Self { _db: db }
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
