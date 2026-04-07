use metalog_types::file_state::FileState;

/// Maximum number of rows per multi-row INSERT statement (MySQL parameter limit).
pub const MAX_MULTI_ROW_INSERT: usize = 1000;

/// Base columns always included in the INSERT clause (order matters for VALUES).
pub const BASE_COLUMNS: &[&str] = &[
    "min_timestamp",
    "max_timestamp",
    "state",
    "clp_ir_storage_backend",
    "clp_ir_bucket",
    "clp_ir_path",
    "clp_ir_size_bytes",
    "clp_archive_storage_backend",
    "clp_archive_bucket",
    "clp_archive_path",
    "clp_archive_size_bytes",
    "clp_archive_created_at",
    "raw_size_bytes",
    "record_count",
    "retention_days",
    "expires_at",
];

/// Columns protected by the guarded UPDATE (not overwritten for protected states).
pub const GUARDED_UPDATE_COLUMNS: &[&str] = &[
    "max_timestamp",
    "state",
    "clp_ir_storage_backend",
    "clp_ir_bucket",
    "clp_ir_size_bytes",
    "clp_archive_storage_backend",
    "clp_archive_bucket",
    "clp_archive_path",
    "clp_archive_size_bytes",
    "clp_archive_created_at",
    "raw_size_bytes",
    "record_count",
];

/// Builds the guard expression for the ON DUPLICATE KEY UPDATE clause.
///
/// The guard prevents overwriting rows in protected states and ensures monotonic
/// timestamp progression (newer data only).
///
/// MariaDB: `IF(state NOT IN ('...') AND VALUES(max_timestamp) > max_timestamp, VALUES(col), col)`
/// MySQL 8.0.20+: `IF(state NOT IN ('...') AND new.max_timestamp > max_timestamp, new.col, col)`
pub fn build_guard_expression(col: &str, is_mariadb: bool) -> String {
    let guard_states: Vec<&str> = FileState::upsert_guard_states()
        .iter()
        .map(|s| s.as_db_str())
        .collect();
    let states_csv = guard_states
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(",");

    if is_mariadb {
        format!(
            "IF(`state` NOT IN ({states_csv}) AND VALUES(`max_timestamp`) > `max_timestamp`, \
             VALUES(`{col}`), `{col}`)"
        )
    } else {
        format!(
            "IF(`state` NOT IN ({states_csv}) AND `new`.`max_timestamp` > `max_timestamp`, \
             `new`.`{col}`, `{col}`)"
        )
    }
}

/// Builds the full ON DUPLICATE KEY UPDATE clause with guards.
///
/// Each guarded column uses the guard expression; unguarded columns (dim/agg)
/// are updated unconditionally.
pub fn build_guarded_update(
    guarded_cols: &[&str],
    extra_cols: &[String],
    is_mariadb: bool,
) -> String {
    let mut parts = Vec::new();

    // Guarded base columns.
    for col in guarded_cols {
        let expr = build_guard_expression(col, is_mariadb);
        parts.push(format!("`{col}` = {expr}"));
    }

    // Extra columns (dims, aggs) updated unconditionally with same guard.
    for col in extra_cols {
        let expr = build_guard_expression(col, is_mariadb);
        parts.push(format!("`{col}` = {expr}"));
    }

    if is_mariadb {
        format!("ON DUPLICATE KEY UPDATE {}", parts.join(", "))
    } else {
        format!("AS `new` ON DUPLICATE KEY UPDATE {}", parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_expression_mariadb() {
        let expr = build_guard_expression("max_timestamp", true);
        assert!(expr.contains("VALUES(`max_timestamp`)"));
        assert!(expr.contains("IR_PURGING"));
        assert!(expr.contains("ARCHIVE_CLOSED"));
    }

    #[test]
    fn guard_expression_mysql() {
        let expr = build_guard_expression("max_timestamp", false);
        assert!(expr.contains("`new`.`max_timestamp`"));
        assert!(expr.contains("IR_PURGING"));
    }

    #[test]
    fn guarded_update_mariadb() {
        let sql = build_guarded_update(&["max_timestamp", "state"], &[], true);
        assert!(sql.starts_with("ON DUPLICATE KEY UPDATE"));
        assert!(sql.contains("`max_timestamp` = IF("));
        assert!(sql.contains("`state` = IF("));
    }

    #[test]
    fn guarded_update_mysql_has_alias() {
        let sql = build_guarded_update(&["state"], &[], false);
        assert!(sql.starts_with("AS `new` ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn guarded_update_with_extra_cols() {
        let extras = vec!["dim_f01".to_string(), "dim_f02".to_string()];
        let sql = build_guarded_update(&["state"], &extras, true);
        assert!(sql.contains("`dim_f01` = IF("));
        assert!(sql.contains("`dim_f02` = IF("));
    }

    #[test]
    fn base_columns_count() {
        assert_eq!(BASE_COLUMNS.len(), 16);
    }

    #[test]
    fn guard_states_in_expression() {
        let expr = build_guard_expression("state", true);
        for state in FileState::upsert_guard_states() {
            assert!(expr.contains(state.as_db_str()));
        }
    }
}
