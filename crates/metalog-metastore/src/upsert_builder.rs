use metalog_types::file_state::FileState;

/// Maximum number of rows per multi-row INSERT statement (MySQL parameter limit).
pub const MAX_MULTI_ROW_INSERT: usize = 1000;

/// Base columns always included in the INSERT clause (order matters for VALUES).
pub const BASE_COLUMNS: &[&str] = &[
    "min_timestamp",
    "max_timestamp",
    "state",
    "file_storage_backend",
    "file_bucket",
    "file_path",
    "file_size_bytes",
    "archive_storage_backend",
    "archive_bucket",
    "archive_path",
    "archive_size_bytes",
    "archive_created_at",
    "raw_size_bytes",
    "record_count",
    "retention_days",
    "expires_at",
];

/// Columns protected by the guarded UPDATE (not overwritten for protected states).
pub const GUARDED_UPDATE_COLUMNS: &[&str] = &[
    "max_timestamp",
    "state",
    "file_storage_backend",
    "file_bucket",
    "file_size_bytes",
    "archive_storage_backend",
    "archive_bucket",
    "archive_path",
    "archive_size_bytes",
    "archive_created_at",
    "raw_size_bytes",
    "record_count",
];

fn new_value(col: &str, is_mariadb: bool) -> String {
    if is_mariadb {
        format!("VALUES(`{col}`)")
    } else {
        format!("`new`.`{col}`")
    }
}

/// Lifecycle eligibility is checked before changing either state or timestamp.
/// This also prevents newer replayed BUFFERING records from reopening closed files.
fn lifecycle_guard(is_mariadb: bool) -> String {
    let incoming = new_value("state", is_mariadb);
    let mut transitions = vec![format!("{incoming} = `state`")];
    for from in FileState::all() {
        for to in FileState::all() {
            if from.can_transition_to(*to) {
                transitions.push(format!(
                    "(`state` = '{}' AND {incoming} = '{}')",
                    from.as_db_str(),
                    to.as_db_str()
                ));
            }
        }
    }
    format!("({})", transitions.join(" OR "))
}

fn state_guard(is_mariadb: bool) -> String {
    let states = FileState::upsert_guard_states()
        .iter()
        .map(|s| format!("'{}'", s.as_db_str()))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "`state` NOT IN ({states}) AND {}",
        lifecycle_guard(is_mariadb)
    )
}

/// Guards payload updates using the original row's lifecycle and timestamp.
pub fn build_guard_expression(col: &str, is_mariadb: bool) -> String {
    format!(
        "IF({} AND {} > `max_timestamp`, {}, `{col}`)",
        state_guard(is_mariadb),
        new_value("max_timestamp", is_mariadb),
        new_value(col, is_mariadb)
    )
}

/// Builds an update in dependency order: payload, timestamp, then lifecycle state.
///
/// Payload expressions observe the original state and timestamp. State is assigned
/// last so entering a protected state cannot suppress other assignments. A forward
/// lifecycle transition is allowed at an equal timestamp: finalizing a file need not
/// add a newer event. Replays cannot reopen files or cross lifecycle chains.
pub fn build_guarded_update(
    guarded_cols: &[&str],
    extra_cols: &[String],
    is_mariadb: bool,
) -> String {
    let mut parts = Vec::new();
    for col in guarded_cols
        .iter()
        .copied()
        .chain(extra_cols.iter().map(String::as_str))
    {
        if col == "state" || col == "max_timestamp" {
            continue;
        }
        parts.push(format!(
            "`{col}` = {}",
            build_guard_expression(col, is_mariadb)
        ));
    }
    if guarded_cols.contains(&"max_timestamp") {
        parts.push(format!(
            "`max_timestamp` = {}",
            build_guard_expression("max_timestamp", is_mariadb)
        ));
    }
    if guarded_cols.contains(&"state") {
        parts.push(format!(
            "`state` = IF({} AND {} >= `max_timestamp`, {}, `state`)",
            state_guard(is_mariadb),
            new_value("max_timestamp", is_mariadb),
            new_value("state", is_mariadb)
        ));
    }
    let prefix = if is_mariadb {
        "ON DUPLICATE KEY UPDATE"
    } else {
        "AS `new` ON DUPLICATE KEY UPDATE"
    };
    format!("{prefix} {}", parts.join(", "))
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
