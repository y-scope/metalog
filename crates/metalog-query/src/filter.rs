use sqlparser::{
    ast::{BinaryOperator, Expr, UnaryOperator, Value},
    dialect::MySqlDialect,
    parser::Parser,
    tokenizer::Token,
};

/// Parses exactly one predicate; statement suffixes and placeholders are forbidden.
pub(crate) fn parse_filter(expr: &str) -> Result<Expr, FilterError> {
    let dialect = MySqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(expr)
        .map_err(|e| FilterError::Parse(e.to_string()))?;
    let expression = parser
        .parse_expr()
        .map_err(|e| FilterError::Parse(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(FilterError::Unsafe(
            "expected only a filter predicate".into(),
        ));
    }
    validate_expr(&expression)?;
    Ok(expression)
}

pub fn validate_filter_expression(expr: &str) -> Result<(), FilterError> {
    if expr.trim().is_empty() {
        return Ok(());
    }
    parse_filter(expr).map(|_| ())
}

fn validate_expr(expr: &Expr) -> Result<(), FilterError> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(()),
        Expr::Value(
            Value::Number(..)
            | Value::SingleQuotedString(_)
            | Value::DoubleQuotedString(_)
            | Value::Boolean(_)
            | Value::Null,
        ) => Ok(()),
        Expr::Value(Value::HexStringLiteral(value))
            if value.bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            Ok(())
        }
        Expr::IntroducedString { introducer, value }
            if introducer.eq_ignore_ascii_case("_utf8mb4") =>
        {
            validate_expr(&Expr::Value(value.clone()))
        }
        Expr::BinaryOp {
            left,
            op:
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Spaceship
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Xor
                | BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo,
            right,
        } => {
            validate_expr(left)?;
            validate_expr(right)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not | UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::Nested(expr) => validate_expr(expr),
        Expr::InList { expr, list, .. } => {
            validate_expr(expr)?;
            for item in list {
                validate_expr(item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr)?;
            validate_expr(low)?;
            validate_expr(high)
        }
        Expr::RLike { expr, pattern, .. } => {
            validate_expr(expr)?;
            validate_expr(pattern)
        }
        Expr::Like {
            expr,
            pattern,
            escape_char,
            ..
        } => {
            // The AST printer does not escape backslashes in ESCAPE strings.
            if escape_char
                .as_ref()
                .is_some_and(|s| s.contains(['\\', '\'']))
            {
                return Err(FilterError::Unsafe(
                    "unsupported LIKE escape character".into(),
                ));
            }
            validate_expr(expr)?;
            validate_expr(pattern)
        }
        other => Err(FilterError::Unsafe(format!(
            "unsupported expression type: {other}"
        ))),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("filter parse error: {0}")]
    Parse(String),
    #[error("unsafe filter expression: {0}")]
    Unsafe(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_expressions() {
        let cases = [
            "min_timestamp > 1000",
            "min_timestamp >= 1000 AND max_timestamp < 2000",
            "state = 'IR_CLOSED'",
            "min_timestamp BETWEEN 1000 AND 2000",
            "state IN ('IR_CLOSED', 'ARCHIVE_CLOSED')",
            "(min_timestamp > 1000) AND (state = 'IR_CLOSED')",
            "NOT state = 'IR_PURGING'",
            "file_path IS NOT NULL",
            "archive_path IS NULL",
            "state LIKE 'IR_%'",
        ];
        for expr in cases {
            assert!(
                validate_filter_expression(expr).is_ok(),
                "should be valid: {expr}"
            );
        }
    }

    #[test]
    fn empty_expression() {
        assert!(validate_filter_expression("").is_ok());
    }

    #[test]
    fn reject_subquery() {
        let result = validate_filter_expression("id IN (SELECT id FROM other)");
        assert!(result.is_err());
    }

    #[test]
    fn reject_function_call() {
        let result = validate_filter_expression("NOW() > min_timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_sql() {
        let result = validate_filter_expression("this is not sql at all");
        assert!(result.is_err());
    }

    #[test]
    fn rejects_predicate_suffixes_and_placeholders() {
        for expression in [
            "1=1; DROP TABLE t",
            "1=1 ORDER BY id",
            "1=1 LIMIT 5",
            "1=1 UNION SELECT 1",
            "state = ?",
            "id IN (SELECT id FROM t)",
            "file_path = X'ff'' OR 1=1 --'",
        ] {
            assert!(
                validate_filter_expression(expression).is_err(),
                "accepted {expression}"
            );
        }
    }
}
