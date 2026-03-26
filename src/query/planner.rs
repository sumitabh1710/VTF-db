use crate::core::error::VtfResult;
use crate::core::model::*;
use crate::query::ast::Expr;

/// A physical execution plan produced by the planner.
#[derive(Debug, Clone)]
pub enum Plan {
    /// Use a hash index for equality lookup
    HashIndexLookup { column: String, value: serde_json::Value },
    /// Use a sorted index for range scan
    SortedIndexRange {
        column: String,
        low: Option<String>,
        high: Option<String>,
        low_inclusive: bool,
        high_inclusive: bool,
    },
    /// Full column scan with a predicate
    ColumnScan { expr: Expr },
    /// Intersection of two plans (AND)
    Intersect(Box<Plan>, Box<Plan>),
    /// Union of two plans (OR)
    Union(Box<Plan>, Box<Plan>),
    /// Complement of a plan (NOT) against total row set
    Complement(Box<Plan>),
}

impl VtfTable {
    /// Given an AST expression, produce an optimized execution plan
    /// that uses indexes when available.
    pub fn plan_query(&self, expr: &Expr) -> Plan {
        match expr {
            Expr::Eq { column, value } => {
                if let Some(idx) = self.indexes.get(column) {
                    match idx.index_type {
                        IndexType::Hash | IndexType::Sorted => {
                            return Plan::HashIndexLookup {
                                column: column.clone(),
                                value: value.clone(),
                            };
                        }
                    }
                }
                Plan::ColumnScan { expr: expr.clone() }
            }

            Expr::Gt { column, value } => self.plan_range(column, value, false, false),
            Expr::Gte { column, value } => self.plan_range(column, value, false, true),
            Expr::Lt { column, value } => self.plan_range(column, value, true, false),
            Expr::Lte { column, value } => self.plan_range(column, value, true, true),

            Expr::Neq { .. } | Expr::Not(_) => {
                match expr {
                    Expr::Not(inner) => Plan::Complement(Box::new(self.plan_query(inner))),
                    _ => Plan::ColumnScan { expr: expr.clone() },
                }
            }

            Expr::And(left, right) => {
                let lp = self.plan_query(left);
                let rp = self.plan_query(right);
                Plan::Intersect(Box::new(lp), Box::new(rp))
            }

            Expr::Or(left, right) => {
                let lp = self.plan_query(left);
                let rp = self.plan_query(right);
                Plan::Union(Box::new(lp), Box::new(rp))
            }
        }
    }

    fn plan_range(&self, column: &str, value: &serde_json::Value, is_less: bool, inclusive: bool) -> Plan {
        if let Some(idx) = self.indexes.get(column) {
            if idx.sorted_keys.is_some() {
                let key = value_to_plan_key(value);
                return if is_less {
                    Plan::SortedIndexRange {
                        column: column.to_string(),
                        low: None,
                        high: Some(key),
                        low_inclusive: true,
                        high_inclusive: inclusive,
                    }
                } else {
                    Plan::SortedIndexRange {
                        column: column.to_string(),
                        low: Some(key),
                        high: None,
                        low_inclusive: inclusive,
                        high_inclusive: true,
                    }
                };
            }
        }
        Plan::ColumnScan {
            expr: if is_less {
                if inclusive {
                    Expr::Lte { column: column.to_string(), value: value.clone() }
                } else {
                    Expr::Lt { column: column.to_string(), value: value.clone() }
                }
            } else if inclusive {
                Expr::Gte { column: column.to_string(), value: value.clone() }
            } else {
                Expr::Gt { column: column.to_string(), value: value.clone() }
            },
        }
    }
}

fn value_to_plan_key(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => val.to_string(),
    }
}

/// Execute a plan against a table, returning matching row indices.
pub fn execute(table: &VtfTable, plan: &Plan) -> VtfResult<Vec<usize>> {
    use std::collections::HashSet;

    match plan {
        Plan::HashIndexLookup { column, value } => {
            table.filter_eq(column, value)
        }

        Plan::SortedIndexRange { column, low, high, low_inclusive, high_inclusive } => {
            if let Some(idx) = table.indexes.get(column) {
                Ok(crate::index::sorted::range_query(
                    idx,
                    low.as_deref(),
                    high.as_deref(),
                    *low_inclusive,
                    *high_inclusive,
                ))
            } else {
                Ok(Vec::new())
            }
        }

        Plan::ColumnScan { expr } => {
            table.eval_expr(expr)
        }

        Plan::Intersect(left, right) => {
            let l: HashSet<usize> = execute(table, left)?.into_iter().collect();
            let r: HashSet<usize> = execute(table, right)?.into_iter().collect();
            let mut result: Vec<usize> = l.intersection(&r).copied().collect();
            result.sort_unstable();
            Ok(result)
        }

        Plan::Union(left, right) => {
            let l: HashSet<usize> = execute(table, left)?.into_iter().collect();
            let r: HashSet<usize> = execute(table, right)?.into_iter().collect();
            let mut result: Vec<usize> = l.union(&r).copied().collect();
            result.sort_unstable();
            Ok(result)
        }

        Plan::Complement(inner) => {
            let matches: HashSet<usize> = execute(table, inner)?.into_iter().collect();
            Ok((0..table.row_count).filter(|i| !matches.contains(i)).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    fn test_table() -> VtfTable {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ],
            "rowCount": 5,
            "data": {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
                "age": [30, 25, 35, 28, 22]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn plan_uses_hash_index_for_eq() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        let expr = Expr::Eq { column: "name".to_string(), value: json!("Alice") };
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::HashIndexLookup { .. }));
        let result = execute(&table, &plan).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn plan_uses_sorted_index_for_range() {
        let mut table = test_table();
        table.create_index("age", IndexType::Sorted).unwrap();
        let expr = Expr::Gt { column: "age".to_string(), value: json!(28) };
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::SortedIndexRange { .. }));
        let result = execute(&table, &plan).unwrap();
        assert_eq!(result, vec![0, 2]); // Alice(30), Charlie(35)
    }

    #[test]
    fn plan_falls_back_to_scan_without_index() {
        let table = test_table();
        let expr = Expr::Gt { column: "age".to_string(), value: json!(28) };
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::ColumnScan { .. }));
        let result = execute(&table, &plan).unwrap();
        assert_eq!(result, vec![0, 2]); // same result via scan
    }

    #[test]
    fn plan_and_uses_intersect() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        let expr = Expr::And(
            Box::new(Expr::Gt { column: "age".to_string(), value: json!(24) }),
            Box::new(Expr::Lt { column: "age".to_string(), value: json!(31) }),
        );
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::Intersect(_, _)));
        let result = execute(&table, &plan).unwrap();
        // age > 24 AND age < 31: Bob(25), Dave(28), Alice(30)
        assert_eq!(result, vec![0, 1, 3]);
    }

    #[test]
    fn plan_or_uses_union() {
        let table = test_table();
        let expr = Expr::Or(
            Box::new(Expr::Eq { column: "name".to_string(), value: json!("Alice") }),
            Box::new(Expr::Eq { column: "name".to_string(), value: json!("Eve") }),
        );
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::Union(_, _)));
        let result = execute(&table, &plan).unwrap();
        assert_eq!(result, vec![0, 4]);
    }

    #[test]
    fn plan_not_uses_complement() {
        let table = test_table();
        let expr = Expr::Not(
            Box::new(Expr::Eq { column: "name".to_string(), value: json!("Alice") }),
        );
        let plan = table.plan_query(&expr);
        assert!(matches!(plan, Plan::Complement(_)));
        let result = execute(&table, &plan).unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn plan_complex_nested() {
        let table = test_table();
        // (age >= 30 OR name = 'Bob') AND NOT name = 'Charlie'
        let expr = Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::Gte { column: "age".to_string(), value: json!(30) }),
                Box::new(Expr::Eq { column: "name".to_string(), value: json!("Bob") }),
            )),
            Box::new(Expr::Not(
                Box::new(Expr::Eq { column: "name".to_string(), value: json!("Charlie") }),
            )),
        );
        let plan = table.plan_query(&expr);
        let result = execute(&table, &plan).unwrap();
        // age >= 30: Alice(0), Charlie(2). name=Bob: Bob(1). Union: {0,1,2}
        // NOT Charlie: {0,1,3,4}. Intersect: {0,1}
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn execute_via_parser_and_planner() {
        let table = test_table();
        let expr = crate::query::parser::parse("age > 25 AND age <= 30").unwrap();
        let plan = table.plan_query(&expr);
        let result = execute(&table, &plan).unwrap();
        // age > 25: Alice(30), Charlie(35), Dave(28) -> {0,2,3}
        // age <= 30: Alice(30), Bob(25), Dave(28), Eve(22) -> {0,1,3,4}
        // Intersect: {0,3}
        assert_eq!(result, vec![0, 3]);
    }
}
