use std::collections::HashSet;

use crate::core::error::VtfResult;
use crate::core::model::*;
use crate::query::ast::Expr;

/// Walk an expression tree and collect all column names it references.
pub fn required_columns(expr: &Expr) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_columns(expr, &mut cols);
    cols
}

fn collect_columns(expr: &Expr, cols: &mut HashSet<String>) {
    match expr {
        Expr::Eq { column, .. }
        | Expr::Neq { column, .. }
        | Expr::Gt { column, .. }
        | Expr::Gte { column, .. }
        | Expr::Lt { column, .. }
        | Expr::Lte { column, .. } => {
            cols.insert(column.clone());
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_columns(l, cols);
            collect_columns(r, cols);
        }
        Expr::Not(inner) => {
            collect_columns(inner, cols);
        }
    }
}

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
    Intersect(Box<PlanNode>, Box<PlanNode>),
    /// Union of two plans (OR)
    Union(Box<PlanNode>, Box<PlanNode>),
    /// Complement of a plan (NOT) against total row set
    Complement(Box<PlanNode>),
}

/// A plan enriched with cost estimates. All plan-related code returns `PlanNode`
/// rather than bare `Plan` so that `EXPLAIN` and the executor have access to
/// estimated cardinalities without a separate pass.
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub plan: Plan,
    /// Estimated number of rows this node will produce.
    pub estimated_rows: usize,
    /// Estimated cost in abstract units (lower is cheaper).
    pub cost: f64,
}

impl PlanNode {
    fn new(plan: Plan, estimated_rows: usize, cost: f64) -> Self {
        PlanNode { plan, estimated_rows, cost }
    }
}

impl VtfTable {
    /// Given an AST expression, produce a cost-estimated execution plan.
    /// Uses column statistics when valid; falls back to heuristic estimates
    /// (30% selectivity rule) when stats are absent or stale.
    pub fn plan_query(&self, expr: &Expr) -> PlanNode {
        let total = self.row_count.max(1);

        match expr {
            Expr::Eq { column, value } => {
                if let Some(idx) = self.indexes.get(column) {
                    let key = value_to_plan_key(value);
                    let hit_count = idx.map.get(&key).map(|v| v.len()).unwrap_or(0);
                    // Cost model: hash lookup = number of matching rows (no traversal overhead).
                    // Scan cost = total rows (must read all).
                    let hash_cost = hit_count as f64 + 1.0; // +1 for the map lookup
                    let scan_cost = total as f64;
                    if hash_cost < scan_cost {
                        return PlanNode::new(
                            Plan::HashIndexLookup { column: column.clone(), value: value.clone() },
                            hit_count,
                            hash_cost,
                        );
                    }
                }
                let est_rows = self.estimate_eq_rows(column, value);
                PlanNode::new(
                    Plan::ColumnScan { expr: expr.clone() },
                    est_rows,
                    total as f64,
                )
            }

            Expr::Gt { column, value } => self.plan_range(column, value, false, false),
            Expr::Gte { column, value } => self.plan_range(column, value, false, true),
            Expr::Lt { column, value } => self.plan_range(column, value, true, false),
            Expr::Lte { column, value } => self.plan_range(column, value, true, true),

            Expr::Neq { .. } => {
                let est = total.saturating_sub(self.estimate_eq_rows_from_neq(expr));
                PlanNode::new(Plan::ColumnScan { expr: expr.clone() }, est, total as f64)
            }

            Expr::Not(inner) => {
                let inner_node = self.plan_query(inner);
                let est = total.saturating_sub(inner_node.estimated_rows);
                let cost = inner_node.cost + total as f64 * 0.01; // small complement pass cost
                PlanNode::new(Plan::Complement(Box::new(inner_node)), est, cost)
            }

            Expr::And(left, right) => {
                let lp = self.plan_query(left);
                let rp = self.plan_query(right);
                // Intersection estimate: min of the two (conservative)
                let est = lp.estimated_rows.min(rp.estimated_rows);
                let cost = lp.cost + rp.cost;
                PlanNode::new(Plan::Intersect(Box::new(lp), Box::new(rp)), est, cost)
            }

            Expr::Or(left, right) => {
                let lp = self.plan_query(left);
                let rp = self.plan_query(right);
                let est = (lp.estimated_rows + rp.estimated_rows).min(total);
                let cost = lp.cost + rp.cost;
                PlanNode::new(Plan::Union(Box::new(lp), Box::new(rp)), est, cost)
            }
        }
    }

    fn plan_range(&self, column: &str, value: &serde_json::Value, is_less: bool, inclusive: bool) -> PlanNode {
        let total = self.row_count.max(1);

        if let Some(idx) = self.indexes.get(column) {
            if idx.sorted_keys.is_some() {
                let key = value_to_plan_key(value);
                let est = self.estimate_range_rows(column, value, is_less);
                let distinct = idx.sorted_keys.as_ref().map(|k| k.len()).unwrap_or(1).max(1);
                // Range cost: matched rows + log2(distinct) for tree traversal
                let range_cost = est as f64 + (distinct as f64).log2();
                let (low, high, low_inclusive, high_inclusive) = if is_less {
                    (None, Some(key), true, inclusive)
                } else {
                    (Some(key), None, inclusive, true)
                };
                return PlanNode::new(
                    Plan::SortedIndexRange { column: column.to_string(), low, high, low_inclusive, high_inclusive },
                    est,
                    range_cost,
                );
            }
        }

        let est = self.estimate_range_rows(column, value, is_less);
        let scan_expr = if is_less {
            if inclusive {
                Expr::Lte { column: column.to_string(), value: value.clone() }
            } else {
                Expr::Lt { column: column.to_string(), value: value.clone() }
            }
        } else if inclusive {
            Expr::Gte { column: column.to_string(), value: value.clone() }
        } else {
            Expr::Gt { column: column.to_string(), value: value.clone() }
        };
        PlanNode::new(Plan::ColumnScan { expr: scan_expr }, est, total as f64)
    }

    /// Estimate how many rows match `column = value`.
    fn estimate_eq_rows(&self, column: &str, value: &serde_json::Value) -> usize {
        let total = self.row_count.max(1);
        // If we have a live index, read the hit count directly.
        if let Some(idx) = self.indexes.get(column) {
            let key = value_to_plan_key(value);
            return idx.map.get(&key).map(|v| v.len()).unwrap_or(0);
        }
        // Use valid stats: rows / distinct_count
        if let Some(s) = self.stats.get(column) {
            if s.valid && s.distinct_count > 0 {
                return total / s.distinct_count;
            }
        }
        // Fallback: 30% selectivity
        (total as f64 * 0.30) as usize
    }

    fn estimate_eq_rows_from_neq(&self, expr: &Expr) -> usize {
        if let Expr::Neq { column, value } = expr {
            self.estimate_eq_rows(column, value)
        } else {
            0
        }
    }

    /// Estimate how many rows satisfy a range bound using column stats.
    fn estimate_range_rows(&self, column: &str, value: &serde_json::Value, is_less: bool) -> usize {
        let total = self.row_count.max(1);
        if let Some(s) = self.stats.get(column) {
            if s.valid {
                if let (Some(min_v), Some(max_v)) = (&s.min, &s.max) {
                    if let Some(sel) = range_selectivity(min_v, max_v, value, is_less) {
                        return ((total as f64) * sel) as usize;
                    }
                }
            }
        }
        // Fallback: assume half the rows
        total / 2
    }
}

/// Estimate fraction of values below (or above) `bound` given [min, max].
fn range_selectivity(
    min: &serde_json::Value,
    max: &serde_json::Value,
    bound: &serde_json::Value,
    is_less: bool,
) -> Option<f64> {
    let mn = min.as_f64()?;
    let mx = max.as_f64()?;
    let b = bound.as_f64()?;
    let span = mx - mn;
    if span <= 0.0 {
        return Some(if is_less { 1.0 } else { 0.0 });
    }
    let frac = ((b - mn) / span).clamp(0.0, 1.0);
    Some(if is_less { frac } else { 1.0 - frac })
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

/// Execute a plan node against a table, returning matching row indices.
pub fn execute(table: &VtfTable, node: &PlanNode) -> VtfResult<Vec<usize>> {
    execute_plan(table, &node.plan)
}

fn execute_plan(table: &VtfTable, plan: &Plan) -> VtfResult<Vec<usize>> {
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
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::HashIndexLookup { .. }));
        let result = execute(&table, &node).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn plan_uses_sorted_index_for_range() {
        let mut table = test_table();
        table.create_index("age", IndexType::Sorted).unwrap();
        let expr = Expr::Gt { column: "age".to_string(), value: json!(28) };
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::SortedIndexRange { .. }));
        let result = execute(&table, &node).unwrap();
        assert_eq!(result, vec![0, 2]); // Alice(30), Charlie(35)
    }

    #[test]
    fn plan_falls_back_to_scan_without_index() {
        let table = test_table();
        let expr = Expr::Gt { column: "age".to_string(), value: json!(28) };
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::ColumnScan { .. }));
        let result = execute(&table, &node).unwrap();
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
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::Intersect(_, _)));
        let result = execute(&table, &node).unwrap();
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
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::Union(_, _)));
        let result = execute(&table, &node).unwrap();
        assert_eq!(result, vec![0, 4]);
    }

    #[test]
    fn plan_not_uses_complement() {
        let table = test_table();
        let expr = Expr::Not(
            Box::new(Expr::Eq { column: "name".to_string(), value: json!("Alice") }),
        );
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::Complement(_)));
        let result = execute(&table, &node).unwrap();
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
        let node = table.plan_query(&expr);
        let result = execute(&table, &node).unwrap();
        // age >= 30: Alice(0), Charlie(2). name=Bob: Bob(1). Union: {0,1,2}
        // NOT Charlie: {0,1,3,4}. Intersect: {0,1}
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn execute_via_parser_and_planner() {
        let table = test_table();
        let expr = crate::query::parser::parse("age > 25 AND age <= 30").unwrap();
        let node = table.plan_query(&expr);
        let result = execute(&table, &node).unwrap();
        // age > 25: Alice(30), Charlie(35), Dave(28) -> {0,2,3}
        // age <= 30: Alice(30), Bob(25), Dave(28), Eve(22) -> {0,1,3,4}
        // Intersect: {0,3}
        assert_eq!(result, vec![0, 3]);
    }

    #[test]
    fn required_columns_extracts_all_referenced() {
        let expr = crate::query::parser::parse("age > 25 AND name = 'Alice' OR active = true").unwrap();
        let cols = required_columns(&expr);
        assert!(cols.contains("age"));
        assert!(cols.contains("name"));
        assert!(cols.contains("active"));
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn required_columns_with_not() {
        let expr = Expr::Not(Box::new(Expr::Eq {
            column: "status".to_string(),
            value: json!("deleted"),
        }));
        let cols = required_columns(&expr);
        assert_eq!(cols.len(), 1);
        assert!(cols.contains("status"));
    }

    #[test]
    fn plan_node_has_cost_and_estimated_rows() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        let expr = Expr::Eq { column: "name".to_string(), value: json!("Alice") };
        let node = table.plan_query(&expr);
        // Cost must be positive
        assert!(node.cost > 0.0);
        // Estimated rows must be <= row_count
        assert!(node.estimated_rows <= table.row_count);
    }

    #[test]
    fn cost_model_prefers_index_over_scan() {
        let mut table = test_table();
        table.create_index("id", IndexType::Hash).unwrap();
        // id=1 is very selective (1 row out of 5)
        let expr = Expr::Eq { column: "id".to_string(), value: json!(1) };
        let node = table.plan_query(&expr);
        assert!(matches!(node.plan, Plan::HashIndexLookup { .. }),
            "expected hash lookup for selective eq, got {:?}", node.plan);
        assert!(node.cost < table.row_count as f64,
            "index cost should be less than full scan cost");
    }
}
