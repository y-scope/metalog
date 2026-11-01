use sqlparser::{
    ast::{BinaryOperator, Expr, Value},
    dialect::MySqlDialect,
    parser::Parser,
    tokenizer::Token,
};

use crate::{decode_ext_blob, sbbf_contains, MAX_EXT_BYTES, SBBF_TYPE};

#[derive(Debug, Clone)]
pub struct SketchPredicate {
    pub key: String,
    pub values: Vec<String>,
}

/// Parse only equality/IN on string values, joined with AND. Never interpolate
/// this input into SQL: logical sketch keys are keys in the decoded payload.
pub fn parse_expression(input: &str) -> Result<Vec<SketchPredicate>, String> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    if input.len() > 16 * 1024 {
        return Err("sketch expression exceeds 16 KiB".into());
    }
    let mut parser = Parser::new(&MySqlDialect {})
        .try_with_sql(input)
        .map_err(|e| e.to_string())?;
    let expr = parser.parse_expr().map_err(|e| e.to_string())?;
    if parser.peek_token().token != Token::EOF {
        return Err("expected only a sketch predicate".into());
    }
    let mut out = Vec::new();
    extract(&expr, &mut out)?;
    if out.len() > 64 || out.iter().map(|p| p.values.len()).sum::<usize>() > 1024 {
        return Err("sketch expression exceeds 64 predicates or 1024 values".into());
    }
    Ok(out)
}

fn extract(expr: &Expr, out: &mut Vec<SketchPredicate>) -> Result<(), String> {
    match expr {
        Expr::Nested(expr) => extract(expr, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract(left, out)?;
            extract(right, out)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            out.push(SketchPredicate {
                key: key(left)?,
                values: vec![literal(right)?],
            });
            Ok(())
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if !list.is_empty() => {
            out.push(SketchPredicate {
                key: key(expr)?,
                values: list.iter().map(literal).collect::<Result<_, _>>()?,
            });
            Ok(())
        }
        _ => Err("sketch predicates support string equality, IN, and AND only".into()),
    }
}
fn key(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(id) if !id.value.is_empty() && id.value.chars().count() <= 1024 => {
            Ok(id.value.clone())
        }
        _ => Err(
            "sketch field must be an identifier (quote dotted field names with backticks)".into(),
        ),
    }
}
fn literal(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(value)) => Ok(value.clone()),
        _ => Err("sketch values must be single-quoted strings".into()),
    }
}

/// Only complete, closed file snapshots can prove absence. Unreadable, missing,
/// or future-format entries conservatively keep the file. Exact event filtering
/// remains the responsibility of the consumer that opens the surviving files.
pub fn may_match<'a>(
    state: &str,
    ext: Option<&[u8]>,
    predicates: impl IntoIterator<Item = &'a SketchPredicate>,
) -> bool {
    if !matches!(state, "IR_CLOSED" | "ARCHIVE_CLOSED") {
        return true;
    }
    let Some(ext) = ext.filter(|v| v.len() <= MAX_EXT_BYTES) else {
        return true;
    };
    let Ok(payload) = decode_ext_blob(ext) else {
        return true;
    };
    predicates.into_iter().all(|predicate| {
        let Some(snapshot) = payload.sketches.get(&predicate.key) else {
            return true;
        };
        snapshot.type_name != SBBF_TYPE
            || predicate
                .values
                .iter()
                .any(|value| sbbf_contains(&snapshot.data, value.as_bytes()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restrict_syntax() {
        for s in [
            "x != 'y'",
            "x NOT IN ('y')",
            "x = 'y' OR z = 'a'",
            "x LIKE 'a%'",
            "x = 12",
            "x = 'y'; DROP TABLE x",
            "x IN (SELECT x FROM t)",
        ] {
            assert!(parse_expression(s).is_err(), "{s}");
        }
        let parsed =
            parse_expression("(`trace.id` IN ('abc', 'def')) AND user_id = 'a''b'").unwrap();
        assert_eq!(parsed[0].key, "trace.id");
        assert_eq!(parsed[1].values, ["a'b"]);
    }
    #[test]
    fn absent_and_corrupt_keep_files() {
        let p = parse_expression("x = 'y'").unwrap();
        assert!(may_match("IR_CLOSED", None, &p));
        assert!(may_match("IR_CLOSED", Some(b"corrupt"), &p));
    }
}
