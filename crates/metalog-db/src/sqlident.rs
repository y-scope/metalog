use once_cell::sync::Lazy;
use regex::Regex;

/// Pattern for valid SQL identifiers: lowercase letter or underscore, followed by
/// up to 63 lowercase alphanumeric or underscore characters.
static IDENT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-z_][a-z0-9_]{0,63}$").unwrap());

/// Validates that `name` is a safe SQL identifier.
///
/// Must match `^[a-z_][a-z0-9_]{0,63}$`.
pub fn validate_sql_identifier(name: &str) -> Result<(), IdentError> {
    if IDENT_RE.is_match(name) {
        Ok(())
    } else {
        Err(IdentError::Invalid(name.to_string()))
    }
}

/// Wraps `name` in backticks for use as a MySQL identifier.
///
/// # Panics
///
/// Panics if `name` is not a valid SQL identifier.
pub fn quote_identifier(name: &str) -> String {
    validate_sql_identifier(name).unwrap_or_else(|_| {
        panic!("invalid SQL identifier: {name:?}");
    });
    format!("`{name}`")
}

#[derive(Debug, thiserror::Error)]
pub enum IdentError {
    #[error("invalid SQL identifier: {0:?} (must match [a-z_][a-z0-9_]{{0,63}})")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers() {
        for name in [
            "id",
            "min_timestamp",
            "dim_f01",
            "_private",
            "a",
            "a_b_c_d_e",
        ] {
            assert!(
                validate_sql_identifier(name).is_ok(),
                "should be valid: {name}"
            );
        }
    }

    #[test]
    fn invalid_identifiers() {
        for name in [
            "",
            "1abc",
            "CamelCase",
            "has space",
            "has-dash",
            "has.dot",
            "DROP TABLE",
            &"a".repeat(65),
        ] {
            assert!(
                validate_sql_identifier(name).is_err(),
                "should be invalid: {name}"
            );
        }
    }

    #[test]
    fn quote() {
        assert_eq!(quote_identifier("dim_f01"), "`dim_f01`");
        assert_eq!(quote_identifier("min_timestamp"), "`min_timestamp`");
    }

    #[test]
    #[should_panic(expected = "invalid SQL identifier")]
    fn quote_invalid_panics() {
        let _ = quote_identifier("DROP TABLE");
    }

    #[test]
    fn max_length_64() {
        let name = &"a".repeat(64);
        assert!(validate_sql_identifier(name).is_ok());

        let too_long = &"a".repeat(65);
        assert!(validate_sql_identifier(too_long).is_err());
    }
}
