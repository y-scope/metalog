use sqlparser::{
    ast::{Expr, Statement, UnaryOperator},
    dialect::MySqlDialect,
    parser::Parser,
};

/// Validates a filter expression for safety.
///
/// Parses the expression as `SELECT 1 FROM t WHERE <expr>` and walks the AST,
/// allowing only safe constructs: comparisons, AND/OR/NOT, LIKE, IN, BETWEEN,
/// IS NULL/IS NOT NULL, column references, and literals.
///
/// Rejects: subqueries, function calls, UNION, JOIN, etc.
pub fn validate_filter_expression(expr: &str) -> Result<(), FilterError> {
    if expr.is_empty() {
        return Ok(());
    }

    let sql = format!("SELECT 1 FROM t WHERE {expr}");
    let dialect = MySqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, &sql).map_err(|e| FilterError::Parse(e.to_string()))?;

    let stmt = statements
        .first()
        .ok_or_else(|| FilterError::Parse("empty parse result".into()))?;

    let Statement::Query(query) = stmt else {
        return Err(FilterError::Unsafe("expected SELECT statement".into()));
    };

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(FilterError::Unsafe("expected simple SELECT".into()));
    };

    let Some(ref selection) = select.selection else {
        return Err(FilterError::Unsafe("missing WHERE clause".into()));
    };

    validate_expr(selection)?;
    Ok(())
}

fn validate_expr(expr: &Expr) -> Result<(), FilterError> {
    match expr {
        // Safe leaf nodes.
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(()),
        Expr::Value(_) => Ok(()),

        // Binary operations (comparisons, AND, OR).
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)
        }

        // Unary NOT.
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => validate_expr(inner),

        // IS NULL / IS NOT NULL.
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => validate_expr(inner),

        // IN (value list).
        Expr::InList {
            expr: inner, list, ..
        } => {
            validate_expr(inner)?;
            for item in list {
                validate_expr(item)?;
            }
            Ok(())
        }

        // BETWEEN.
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            validate_expr(inner)?;
            validate_expr(low)?;
            validate_expr(high)
        }

        // LIKE.
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            validate_expr(inner)?;
            validate_expr(pattern)
        }

        // Nested parentheses.
        Expr::Nested(inner) => validate_expr(inner),

        // Everything else is rejected.
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
            "clp_ir_path IS NOT NULL",
            "clp_archive_path IS NULL",
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
}
