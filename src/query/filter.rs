use std::collections::HashSet;

use indexmap::IndexMap;
use serde_json::Value;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;
use crate::query::ast::Expr;

impl VtfTable {
    /// Return a reference to the column data for a given column name.
    pub fn scan_column(&self, column: &str) -> VtfResult<&ColumnData> {
        self.data
            .get(column)
            .ok_or_else(|| VtfError::query(format!("column '{column}' not found")))
    }

    /// Filter rows where `column = value`. Returns matching row indices.
    /// Uses hash index if available, otherwise falls back to linear scan.
    pub fn filter_eq(&self, column: &str, value: &Value) -> VtfResult<Vec<usize>> {
        if !self.data.contains_key(column) {
            return Err(VtfError::query(format!("column '{column}' not found")));
        }

        if let Some(idx) = self.indexes.get(column) {
            let key = value_to_search_key(value);
            return Ok(idx.map.get(&key).cloned().unwrap_or_default());
        }

        let col_data = &self.data[column];
        let mut matches = Vec::new();
        for i in 0..self.row_count {
            let cell = col_data.get_json_value(i).unwrap_or(Value::Null);
            if values_match(&cell, value) {
                matches.push(i);
            }
        }
        Ok(matches)
    }

    /// Reconstruct rows at the given indices, projecting only the specified columns.
    /// If `columns` is empty, all columns are included.
    pub fn select_rows(
        &self,
        indices: &[usize],
        columns: &[&str],
    ) -> VtfResult<Vec<IndexMap<String, Value>>> {
        let selected_cols: Vec<&str> = if columns.is_empty() {
            self.columns.iter().map(|c| c.name.as_str()).collect()
        } else {
            for &c in columns {
                if !self.data.contains_key(c) {
                    return Err(VtfError::query(format!("column '{c}' not found")));
                }
            }
            columns.to_vec()
        };

        let mut rows = Vec::with_capacity(indices.len());
        for &idx in indices {
            if idx >= self.row_count {
                return Err(VtfError::query(format!(
                    "row index {idx} out of bounds (rowCount: {})",
                    self.row_count
                )));
            }
            let mut row = IndexMap::new();
            for &col_name in &selected_cols {
                let col_data = &self.data[col_name];
                let val = col_data.get_json_value(idx).unwrap_or(Value::Null);
                row.insert(col_name.to_string(), val);
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

impl VtfTable {
    /// Evaluate a query expression and return matching row indices.
    pub fn eval_expr(&self, expr: &Expr) -> VtfResult<Vec<usize>> {
        match expr {
            Expr::Eq { column, value } => self.filter_eq(column, value),

            Expr::Neq { column, value } => {
                self.validate_column(column)?;
                let eq_matches: HashSet<usize> = self.filter_eq(column, value)?.into_iter().collect();
                Ok((0..self.row_count).filter(|i| !eq_matches.contains(i)).collect())
            }

            Expr::Gt { column, value } => self.filter_cmp(column, value, false, false),
            Expr::Gte { column, value } => self.filter_cmp(column, value, false, true),
            Expr::Lt { column, value } => self.filter_cmp(column, value, true, false),
            Expr::Lte { column, value } => self.filter_cmp(column, value, true, true),

            Expr::And(left, right) => {
                let l: HashSet<usize> = self.eval_expr(left)?.into_iter().collect();
                let r: HashSet<usize> = self.eval_expr(right)?.into_iter().collect();
                let mut result: Vec<usize> = l.intersection(&r).copied().collect();
                result.sort_unstable();
                Ok(result)
            }

            Expr::Or(left, right) => {
                let l: HashSet<usize> = self.eval_expr(left)?.into_iter().collect();
                let r: HashSet<usize> = self.eval_expr(right)?.into_iter().collect();
                let mut result: Vec<usize> = l.union(&r).copied().collect();
                result.sort_unstable();
                Ok(result)
            }

            Expr::Not(inner) => {
                let matches: HashSet<usize> = self.eval_expr(inner)?.into_iter().collect();
                Ok((0..self.row_count).filter(|i| !matches.contains(i)).collect())
            }
        }
    }

    fn validate_column(&self, column: &str) -> VtfResult<()> {
        if !self.data.contains_key(column) {
            return Err(VtfError::query(format!("column '{column}' not found")));
        }
        Ok(())
    }

    /// Compare rows against a value. `is_less` = true means we want rows < value (or <=).
    /// `inclusive` determines strict vs non-strict.
    fn filter_cmp(&self, column: &str, value: &Value, is_less: bool, inclusive: bool) -> VtfResult<Vec<usize>> {
        self.validate_column(column)?;
        let col_data = &self.data[column];

        // Try sorted index if available
        if let Some(idx) = self.indexes.get(column) {
            if idx.sorted_keys.is_some() {
                let search_key = value_to_search_key(value);
                let rows = if is_less {
                    crate::index::sorted::range_query(idx, None, Some(&search_key), true, inclusive)
                } else {
                    crate::index::sorted::range_query(idx, Some(&search_key), None, inclusive, true)
                };
                return Ok(rows);
            }
        }

        // Linear scan
        let mut matches = Vec::new();
        for i in 0..self.row_count {
            let cell = col_data.get_json_value(i).unwrap_or(Value::Null);
            if cell.is_null() {
                continue;
            }
            let ord = compare_values(&cell, value);
            let pass = match ord {
                Some(std::cmp::Ordering::Less) => is_less,
                Some(std::cmp::Ordering::Equal) => inclusive,
                Some(std::cmp::Ordering::Greater) => !is_less,
                None => false,
            };
            if pass {
                matches.push(i);
            }
        }
        Ok(matches)
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            let af = an.as_f64()?;
            let bf = bn.as_f64()?;
            af.partial_cmp(&bf)
        }
        (Value::String(a_s), Value::String(b_s)) => Some(a_s.cmp(b_s)),
        (Value::Bool(a_b), Value::Bool(b_b)) => Some(a_b.cmp(b_b)),
        _ => None,
    }
}

fn value_to_search_key(val: &Value) -> String {
    match val {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        _ => val.to_string(),
    }
}

fn values_match(cell: &Value, search: &Value) -> bool {
    match (cell, search) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                ai == bi
            } else {
                a.as_f64() == b.as_f64()
            }
        }
        (Value::Null, Value::Null) => true,
        _ => cell == search,
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::validation;

    fn test_table() -> crate::VtfTable {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Alice"],
                "age": [30, 25, 30]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn test_scan_column() {
        let table = test_table();
        let col = table.scan_column("name").unwrap();
        assert_eq!(col.len(), 3);
    }

    #[test]
    fn test_scan_missing_column() {
        let table = test_table();
        assert!(table.scan_column("nonexistent").is_err());
    }

    #[test]
    fn test_filter_eq_string() {
        let table = test_table();
        let matches = table
            .filter_eq("name", &serde_json::json!("Alice"))
            .unwrap();
        assert_eq!(matches, vec![0, 2]);
    }

    #[test]
    fn test_filter_eq_int() {
        let table = test_table();
        let matches = table.filter_eq("age", &serde_json::json!(25)).unwrap();
        assert_eq!(matches, vec![1]);
    }

    #[test]
    fn test_filter_eq_no_match() {
        let table = test_table();
        let matches = table
            .filter_eq("name", &serde_json::json!("Charlie"))
            .unwrap();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_select_rows() {
        let table = test_table();
        let rows = table.select_rows(&[0, 2], &["name", "age"]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], serde_json::json!("Alice"));
        assert_eq!(rows[0]["age"], serde_json::json!(30));
        assert_eq!(rows[1]["name"], serde_json::json!("Alice"));
    }

    #[test]
    fn test_select_rows_all_columns() {
        let table = test_table();
        let rows = table.select_rows(&[1], &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn test_select_rows_out_of_bounds() {
        let table = test_table();
        assert!(table.select_rows(&[10], &[]).is_err());
    }
}
