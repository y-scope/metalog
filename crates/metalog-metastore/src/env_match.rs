use std::collections::HashMap;

/// Parses a `required_env` string ("KEY=VALUE,KEY=VALUE") into a map.
///
/// AND semantics: all conditions must match for a node to claim a source.
pub fn parse_required_env(required_env: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if required_env.is_empty() {
        return map;
    }
    for pair in required_env.split(',') {
        let pair = pair.trim();
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            if !key.is_empty() {
                map.insert(key.to_string(), value.to_string());
            }
        }
    }
    map
}

/// Returns true if the current process environment satisfies all conditions
/// in `required_env`. Returns true if `required_env` is empty (no restrictions).
pub fn matches_env(required_env: &str) -> bool {
    if required_env.is_empty() {
        return true;
    }
    let conditions = parse_required_env(required_env);
    for (key, expected) in &conditions {
        match std::env::var(key) {
            Ok(actual) if actual == *expected => {}
            _ => return false,
        }
    }
    true
}

/// Validates that a `required_env` string is well-formed.
/// Each entry must be `KEY=VALUE` with non-empty key.
pub fn validate_required_env(required_env: &str) -> Result<(), String> {
    if required_env.is_empty() {
        return Ok(());
    }
    for pair in required_env.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        match pair.split_once('=') {
            Some((key, _)) if !key.trim().is_empty() => {}
            _ => {
                return Err(format!(
                    "invalid required_env entry: {pair:?} (expected KEY=VALUE)"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty() {
        assert!(parse_required_env("").is_empty());
    }

    #[test]
    fn parse_single() {
        let map = parse_required_env("REGION=us-east");
        assert_eq!(map.get("REGION").unwrap(), "us-east");
    }

    #[test]
    fn parse_multiple() {
        let map = parse_required_env("REGION=us-east,CLUSTER=prod");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("REGION").unwrap(), "us-east");
        assert_eq!(map.get("CLUSTER").unwrap(), "prod");
    }

    #[test]
    fn parse_with_spaces() {
        let map = parse_required_env(" REGION = us-east , CLUSTER = prod ");
        assert_eq!(map.get("REGION").unwrap(), "us-east");
        assert_eq!(map.get("CLUSTER").unwrap(), "prod");
    }

    #[test]
    fn matches_env_empty() {
        assert!(matches_env(""));
    }

    #[test]
    fn matches_env_set() {
        std::env::set_var("METALOG_TEST_ENV", "test_value");
        assert!(matches_env("METALOG_TEST_ENV=test_value"));
        assert!(!matches_env("METALOG_TEST_ENV=wrong"));
        std::env::remove_var("METALOG_TEST_ENV");
    }

    #[test]
    fn matches_env_missing() {
        assert!(!matches_env("METALOG_NONEXISTENT_VAR=anything"));
    }

    #[test]
    fn validate_valid() {
        assert!(validate_required_env("").is_ok());
        assert!(validate_required_env("REGION=us-east").is_ok());
        assert!(validate_required_env("A=1,B=2").is_ok());
    }

    #[test]
    fn validate_invalid() {
        assert!(validate_required_env("no_equals").is_err());
        assert!(validate_required_env("=value").is_err());
    }
}
