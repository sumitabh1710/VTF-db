use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Eq { column: String, value: Value },
    Neq { column: String, value: Value },
    Gt { column: String, value: Value },
    Gte { column: String, value: Value },
    Lt { column: String, value: Value },
    Lte { column: String, value: Value },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Eq { column, value } => write!(f, "{column} = {value}"),
            Expr::Neq { column, value } => write!(f, "{column} != {value}"),
            Expr::Gt { column, value } => write!(f, "{column} > {value}"),
            Expr::Gte { column, value } => write!(f, "{column} >= {value}"),
            Expr::Lt { column, value } => write!(f, "{column} < {value}"),
            Expr::Lte { column, value } => write!(f, "{column} <= {value}"),
            Expr::And(l, r) => write!(f, "({l} AND {r})"),
            Expr::Or(l, r) => write!(f, "({l} OR {r})"),
            Expr::Not(e) => write!(f, "NOT ({e})"),
        }
    }
}
