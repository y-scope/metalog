use sqlx::MySqlPool;

/// The embedded schema DDL for all system tables and the template table.
pub const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Splits a multi-statement SQL string into individual statements.
///
/// Handles:
/// - Semicolon-terminated statements
/// - `--` line comments (stripped)
/// - Multi-line statements
/// - Empty lines (skipped)
///
/// Does NOT handle: stored procedures with DELIMITER changes, string literals
/// containing semicolons (not needed for DDL-only schemas).
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();

    for line in sql.lines() {
        let trimmed = line.trim();

        // Skip empty lines and comments.
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        current.push(' ');
        current.push_str(trimmed);

        if trimmed.ends_with(';') {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() && stmt != ";" {
                statements.push(stmt);
            }
            current.clear();
        }
    }

    // Handle trailing statement without semicolon.
    let remaining = current.trim().to_string();
    if !remaining.is_empty() {
        statements.push(remaining);
    }

    statements
}

/// Executes a multi-statement DDL string, splitting by semicolons.
///
/// Each statement is executed individually. Errors on table/partition already
/// existing are silently ignored (idempotent).
pub async fn execute_ddl_statements(pool: &MySqlPool, sql: &str) -> Result<(), sqlx::Error> {
    for stmt in split_sql_statements(sql) {
        match sqlx::query(&stmt).execute(pool).await {
            Ok(_) => {}
            Err(e) if metalog_db::is_table_exists(&e) => {
                tracing::debug!(stmt = %stmt.chars().take(80).collect::<String>(), "table already exists, skipping");
            }
            Err(e) if metalog_db::is_duplicate_partition(&e) => {
                tracing::debug!("partition already exists, skipping");
            }
            Err(e) if metalog_db::is_duplicate_column(&e) => {
                tracing::debug!("column already exists, skipping");
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_sql_embedded() {
        assert!(!SCHEMA_SQL.is_empty());
        assert!(SCHEMA_SQL.contains("_clp_template"));
        assert!(SCHEMA_SQL.contains("_table"));
        assert!(SCHEMA_SQL.contains("_dim_registry"));
    }

    #[test]
    fn split_simple() {
        let sql = "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("TABLE a"));
        assert!(stmts[1].contains("TABLE b"));
    }

    #[test]
    fn split_with_comments() {
        let sql = "-- This is a comment\nCREATE TABLE a (id INT);\n-- Another comment\n";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn split_multiline() {
        let sql = "CREATE TABLE a (\n  id INT,\n  name VARCHAR(64)\n);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("id INT"));
        assert!(stmts[0].contains("name VARCHAR"));
    }

    #[test]
    fn split_empty() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("-- just comments\n-- more").is_empty());
    }

    #[test]
    fn split_no_trailing_semicolon() {
        let sql = "SELECT 1";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn schema_has_system_tables() {
        let stmts = split_sql_statements(SCHEMA_SQL);
        let joined = stmts.join(" ");
        assert!(joined.contains("_table"));
        assert!(joined.contains("_table_config"));
        assert!(joined.contains("_table_assignment"));
        assert!(joined.contains("_node_registry"));
        assert!(joined.contains("_task_queue"));
        assert!(joined.contains("_dim_registry"));
        assert!(joined.contains("_agg_registry"));
        assert!(joined.contains("_sketch_registry"));
        assert!(joined.contains("_clp_template"));
    }
}
