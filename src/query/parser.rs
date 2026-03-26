use crate::core::error::{VtfError, VtfResult};
use crate::query::ast::Expr;

/// Parse a query string into an Expr AST.
///
/// Grammar (simplified):
///   expr     = or_expr
///   or_expr  = and_expr ("OR" and_expr)*
///   and_expr = unary ("AND" unary)*
///   unary    = "NOT" unary | primary
///   primary  = "(" expr ")" | comparison
///   comparison = IDENT OP VALUE
///   OP       = "=" | "!=" | ">" | ">=" | "<" | "<="
///   VALUE    = number | string_literal | "true" | "false" | "null"
pub fn parse(input: &str) -> VtfResult<Expr> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    let expr = parse_or(&tokens, &mut pos)?;
    if pos < tokens.len() {
        return Err(VtfError::query(format!(
            "unexpected token '{}' at position {pos}",
            tokens[pos]
        )));
    }
    Ok(expr)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    Number(String),
    True,
    False,
    Null,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Not,
    LParen,
    RParen,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Ident(s) => write!(f, "{s}"),
            Token::StringLit(s) => write!(f, "'{s}'"),
            Token::Number(s) => write!(f, "{s}"),
            Token::True => write!(f, "true"),
            Token::False => write!(f, "false"),
            Token::Null => write!(f, "null"),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "!="),
            Token::Gt => write!(f, ">"),
            Token::Gte => write!(f, ">="),
            Token::Lt => write!(f, "<"),
            Token::Lte => write!(f, "<="),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
        }
    }
}

fn tokenize(input: &str) -> VtfResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if c.is_whitespace() {
            i += 1;
            continue;
        }

        if c == '(' {
            tokens.push(Token::LParen);
            i += 1;
            continue;
        }
        if c == ')' {
            tokens.push(Token::RParen);
            i += 1;
            continue;
        }

        if c == '!' && i + 1 < chars.len() && chars[i + 1] == '=' {
            tokens.push(Token::Neq);
            i += 2;
            continue;
        }
        if c == '>' && i + 1 < chars.len() && chars[i + 1] == '=' {
            tokens.push(Token::Gte);
            i += 2;
            continue;
        }
        if c == '<' && i + 1 < chars.len() && chars[i + 1] == '=' {
            tokens.push(Token::Lte);
            i += 2;
            continue;
        }
        if c == '=' {
            tokens.push(Token::Eq);
            i += 1;
            continue;
        }
        if c == '>' {
            tokens.push(Token::Gt);
            i += 1;
            continue;
        }
        if c == '<' {
            tokens.push(Token::Lt);
            i += 1;
            continue;
        }

        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != quote {
                i += 1;
            }
            if i >= chars.len() {
                return Err(VtfError::query(format!("unterminated string literal starting at position {start}")));
            }
            let s: String = chars[start..i].iter().collect();
            tokens.push(Token::StringLit(s));
            i += 1;
            continue;
        }

        if c == '-' || c.is_ascii_digit() {
            let start = i;
            if c == '-' {
                i += 1;
            }
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i < chars.len() && chars[i] == '.' {
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let s: String = chars[start..i].iter().collect();
            tokens.push(Token::Number(s));
            continue;
        }

        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            match word.to_uppercase().as_str() {
                "AND" => tokens.push(Token::And),
                "OR" => tokens.push(Token::Or),
                "NOT" => tokens.push(Token::Not),
                "TRUE" => tokens.push(Token::True),
                "FALSE" => tokens.push(Token::False),
                "NULL" => tokens.push(Token::Null),
                _ => tokens.push(Token::Ident(word)),
            }
            continue;
        }

        return Err(VtfError::query(format!("unexpected character '{c}' at position {i}")));
    }

    Ok(tokens)
}

fn parse_or(tokens: &[Token], pos: &mut usize) -> VtfResult<Expr> {
    let mut left = parse_and(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == Token::Or {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(tokens: &[Token], pos: &mut usize) -> VtfResult<Expr> {
    let mut left = parse_unary(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == Token::And {
        *pos += 1;
        let right = parse_unary(tokens, pos)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_unary(tokens: &[Token], pos: &mut usize) -> VtfResult<Expr> {
    if *pos < tokens.len() && tokens[*pos] == Token::Not {
        *pos += 1;
        let inner = parse_unary(tokens, pos)?;
        return Ok(Expr::Not(Box::new(inner)));
    }
    parse_primary(tokens, pos)
}

fn parse_primary(tokens: &[Token], pos: &mut usize) -> VtfResult<Expr> {
    if *pos >= tokens.len() {
        return Err(VtfError::query("unexpected end of query expression"));
    }

    if tokens[*pos] == Token::LParen {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != Token::RParen {
            return Err(VtfError::query("expected closing ')'"));
        }
        *pos += 1;
        return Ok(expr);
    }

    parse_comparison(tokens, pos)
}

fn parse_comparison(tokens: &[Token], pos: &mut usize) -> VtfResult<Expr> {
    if *pos >= tokens.len() {
        return Err(VtfError::query("expected column name"));
    }

    let column = match &tokens[*pos] {
        Token::Ident(s) => s.clone(),
        other => return Err(VtfError::query(format!("expected column name, got '{other}'"))),
    };
    *pos += 1;

    if *pos >= tokens.len() {
        return Err(VtfError::query(format!("expected operator after '{column}'")));
    }

    let op = &tokens[*pos];
    let op_clone = op.clone();
    *pos += 1;

    if *pos >= tokens.len() {
        return Err(VtfError::query(format!("expected value after '{column} {op_clone}'")));
    }

    let value = parse_value(tokens, pos)?;

    Ok(match op_clone {
        Token::Eq => Expr::Eq { column, value },
        Token::Neq => Expr::Neq { column, value },
        Token::Gt => Expr::Gt { column, value },
        Token::Gte => Expr::Gte { column, value },
        Token::Lt => Expr::Lt { column, value },
        Token::Lte => Expr::Lte { column, value },
        other => return Err(VtfError::query(format!("expected operator, got '{other}'"))),
    })
}

fn parse_value(tokens: &[Token], pos: &mut usize) -> VtfResult<serde_json::Value> {
    let tok = &tokens[*pos];
    *pos += 1;
    Ok(match tok {
        Token::StringLit(s) => serde_json::Value::String(s.clone()),
        Token::Number(s) => {
            if let Ok(n) = s.parse::<i64>() {
                serde_json::json!(n)
            } else if let Ok(n) = s.parse::<f64>() {
                serde_json::json!(n)
            } else {
                return Err(VtfError::query(format!("invalid number: '{s}'")));
            }
        }
        Token::True => serde_json::json!(true),
        Token::False => serde_json::json!(false),
        Token::Null => serde_json::Value::Null,
        other => return Err(VtfError::query(format!("expected value, got '{other}'"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_simple_eq() {
        let expr = parse("name = 'Alice'").unwrap();
        assert_eq!(expr, Expr::Eq {
            column: "name".to_string(),
            value: json!("Alice"),
        });
    }

    #[test]
    fn parse_int_comparison() {
        let expr = parse("age > 25").unwrap();
        assert_eq!(expr, Expr::Gt {
            column: "age".to_string(),
            value: json!(25),
        });
    }

    #[test]
    fn parse_gte_lte() {
        let expr = parse("score >= 90").unwrap();
        assert_eq!(expr, Expr::Gte {
            column: "score".to_string(),
            value: json!(90),
        });

        let expr = parse("score <= 50").unwrap();
        assert_eq!(expr, Expr::Lte {
            column: "score".to_string(),
            value: json!(50),
        });
    }

    #[test]
    fn parse_neq() {
        let expr = parse("status != 'active'").unwrap();
        assert_eq!(expr, Expr::Neq {
            column: "status".to_string(),
            value: json!("active"),
        });
    }

    #[test]
    fn parse_and_or() {
        let expr = parse("age > 20 AND name = 'Alice'").unwrap();
        assert_eq!(expr, Expr::And(
            Box::new(Expr::Gt { column: "age".to_string(), value: json!(20) }),
            Box::new(Expr::Eq { column: "name".to_string(), value: json!("Alice") }),
        ));
    }

    #[test]
    fn parse_or_lower_precedence_than_and() {
        // "a = 1 OR b = 2 AND c = 3" should parse as "a = 1 OR (b = 2 AND c = 3)"
        let expr = parse("a = 1 OR b = 2 AND c = 3").unwrap();
        assert_eq!(expr, Expr::Or(
            Box::new(Expr::Eq { column: "a".to_string(), value: json!(1) }),
            Box::new(Expr::And(
                Box::new(Expr::Eq { column: "b".to_string(), value: json!(2) }),
                Box::new(Expr::Eq { column: "c".to_string(), value: json!(3) }),
            )),
        ));
    }

    #[test]
    fn parse_not() {
        let expr = parse("NOT active = true").unwrap();
        assert_eq!(expr, Expr::Not(
            Box::new(Expr::Eq { column: "active".to_string(), value: json!(true) }),
        ));
    }

    #[test]
    fn parse_parenthesized() {
        let expr = parse("(a = 1 OR b = 2) AND c = 3").unwrap();
        assert_eq!(expr, Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::Eq { column: "a".to_string(), value: json!(1) }),
                Box::new(Expr::Eq { column: "b".to_string(), value: json!(2) }),
            )),
            Box::new(Expr::Eq { column: "c".to_string(), value: json!(3) }),
        ));
    }

    #[test]
    fn parse_null_value() {
        let expr = parse("name = null").unwrap();
        assert_eq!(expr, Expr::Eq {
            column: "name".to_string(),
            value: serde_json::Value::Null,
        });
    }

    #[test]
    fn parse_float_value() {
        let expr = parse("score > 3.14").unwrap();
        assert_eq!(expr, Expr::Gt {
            column: "score".to_string(),
            value: json!(3.14),
        });
    }

    #[test]
    fn parse_negative_number() {
        let expr = parse("temp < -10").unwrap();
        assert_eq!(expr, Expr::Lt {
            column: "temp".to_string(),
            value: json!(-10),
        });
    }

    #[test]
    fn parse_double_quoted_string() {
        let expr = parse("name = \"Bob\"").unwrap();
        assert_eq!(expr, Expr::Eq {
            column: "name".to_string(),
            value: json!("Bob"),
        });
    }

    #[test]
    fn parse_error_unclosed_string() {
        assert!(parse("name = 'Alice").is_err());
    }

    #[test]
    fn parse_error_missing_value() {
        assert!(parse("name =").is_err());
    }

    #[test]
    fn parse_error_missing_operator() {
        assert!(parse("name 'Alice'").is_err());
    }

    #[test]
    fn parse_chained_and() {
        let expr = parse("a = 1 AND b = 2 AND c = 3").unwrap();
        assert_eq!(expr, Expr::And(
            Box::new(Expr::And(
                Box::new(Expr::Eq { column: "a".to_string(), value: json!(1) }),
                Box::new(Expr::Eq { column: "b".to_string(), value: json!(2) }),
            )),
            Box::new(Expr::Eq { column: "c".to_string(), value: json!(3) }),
        ));
    }

    #[test]
    fn parse_boolean_value() {
        let expr = parse("active = true").unwrap();
        assert_eq!(expr, Expr::Eq {
            column: "active".to_string(),
            value: json!(true),
        });
        let expr = parse("active = false").unwrap();
        assert_eq!(expr, Expr::Eq {
            column: "active".to_string(),
            value: json!(false),
        });
    }
}
