/// Generates `ON DUPLICATE KEY UPDATE col = VALUES(col), ...` (MariaDB / MySQL <8.0.20).
pub fn on_duplicate_key_update_values(cols: &[&str]) -> String {
    let assignments: Vec<String> = cols
        .iter()
        .map(|col| format!("`{col}` = VALUES(`{col}`)"))
        .collect();
    format!("ON DUPLICATE KEY UPDATE {}", assignments.join(", "))
}

/// Generates `AS {alias} ON DUPLICATE KEY UPDATE col = {alias}.col, ...` (MySQL 8.0.20+).
pub fn on_duplicate_key_update_alias(alias: &str, cols: &[&str]) -> String {
    let assignments: Vec<String> = cols
        .iter()
        .map(|col| format!("`{col}` = `{alias}`.`{col}`"))
        .collect();
    format!(
        "AS `{alias}` ON DUPLICATE KEY UPDATE {}",
        assignments.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_syntax() {
        let sql = on_duplicate_key_update_values(&["max_timestamp", "state"]);
        assert_eq!(
            sql,
            "ON DUPLICATE KEY UPDATE `max_timestamp` = VALUES(`max_timestamp`), `state` = \
             VALUES(`state`)"
        );
    }

    #[test]
    fn alias_syntax() {
        let sql = on_duplicate_key_update_alias("new", &["max_timestamp", "state"]);
        assert_eq!(
            sql,
            "AS `new` ON DUPLICATE KEY UPDATE `max_timestamp` = `new`.`max_timestamp`, `state` = \
             `new`.`state`"
        );
    }

    #[test]
    fn single_column() {
        let sql = on_duplicate_key_update_values(&["state"]);
        assert_eq!(sql, "ON DUPLICATE KEY UPDATE `state` = VALUES(`state`)");
    }
}
