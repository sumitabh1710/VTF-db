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
    /// Uses hash index if available, otherwise falls back to a typed linear scan.
    pub fn filter_eq(&self, column: &str, value: &Value) -> VtfResult<Vec<usize>> {
        if !self.data.contains_key(column) {
            return Err(VtfError::query(format!("column '{column}' not found")));
        }

        if let Some(idx) = self.indexes.get(column) {
            let key = value_to_search_key(value);
            return Ok(idx.map.get(&key).cloned().unwrap_or_default());
        }

        let col_data = &self.data[column];
        Ok(typed_eq_scan(col_data, value))
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

    /// Compare rows against a value using typed comparisons (no per-row JSON allocation).
    /// `is_less` = true means we want rows < value (or <=). `inclusive` is strict vs non-strict.
    pub fn filter_cmp(&self, column: &str, value: &Value, is_less: bool, inclusive: bool) -> VtfResult<Vec<usize>> {
        self.validate_column(column)?;
        let col_data = &self.data[column];

        // Use sorted index if available
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

        // Typed linear scan — no per-row serde_json::Value allocation
        Ok(typed_cmp_scan(col_data, value, is_less, inclusive))
    }
}

// ---------------------------------------------------------------------------
// Typed scan helpers — operate directly on the native Vec<Option<T>> storage
// to avoid allocating serde_json::Value per row.
// ---------------------------------------------------------------------------

/// Equality scan without index: compare directly against the native typed storage.
fn typed_eq_scan(col_data: &ColumnData, value: &Value) -> Vec<usize> {
    match col_data {
        ColumnData::Int(v) => {
            // Accept both integer and float literals that are whole numbers.
            if let Some(target) = value.as_i64().or_else(|| value.as_f64().and_then(|f| {
                if f.fract() == 0.0 { Some(f as i64) } else { None }
            })) {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if *cell == Some(target) { Some(i) } else { None })
                    .collect()
            } else if value.is_null() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if cell.is_none() { Some(i) } else { None })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Float(v) => {
            if let Some(target) = value.as_f64() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if *cell == Some(target) { Some(i) } else { None })
                    .collect()
            } else if value.is_null() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if cell.is_none() { Some(i) } else { None })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            if let Some(target) = value.as_str() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| {
                        if cell.as_deref() == Some(target) { Some(i) } else { None }
                    })
                    .collect()
            } else if value.is_null() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if cell.is_none() { Some(i) } else { None })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Bool(v) => {
            if let Some(target) = value.as_bool() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if *cell == Some(target) { Some(i) } else { None })
                    .collect()
            } else if value.is_null() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| if cell.is_none() { Some(i) } else { None })
                    .collect()
            } else {
                vec![]
            }
        }
        // Array columns: not equality-scannable in the scalar sense; fall back to JSON comparison.
        _ => {
            (0..col_data.len())
                .filter(|&i| {
                    col_data.get_json_value(i).unwrap_or(Value::Null) == *value
                })
                .collect()
        }
    }
}

/// Range comparison scan without index: compare directly against native typed storage.
/// `is_less` = want rows < value; `inclusive` = include equality.
fn typed_cmp_scan(col_data: &ColumnData, value: &Value, is_less: bool, inclusive: bool) -> Vec<usize> {
    fn ord_passes(ord: std::cmp::Ordering, is_less: bool, inclusive: bool) -> bool {
        match ord {
            std::cmp::Ordering::Less => is_less,
            std::cmp::Ordering::Equal => inclusive,
            std::cmp::Ordering::Greater => !is_less,
        }
    }

    match col_data {
        ColumnData::Int(v) => {
            if let Some(target) = value.as_i64().or_else(|| value.as_f64().map(|f| f as i64)) {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| {
                        let n = (*cell)?;
                        if ord_passes(n.cmp(&target), is_less, inclusive) { Some(i) } else { None }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Float(v) => {
            if let Some(target) = value.as_f64() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| {
                        let n = (*cell)?;
                        let ord = n.partial_cmp(&target).unwrap_or(std::cmp::Ordering::Less);
                        if ord_passes(ord, is_less, inclusive) { Some(i) } else { None }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            if let Some(target) = value.as_str() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| {
                        let s = cell.as_deref()?;
                        if ord_passes(s.cmp(target), is_less, inclusive) { Some(i) } else { None }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        ColumnData::Bool(v) => {
            if let Some(target) = value.as_bool() {
                v.iter().enumerate()
                    .filter_map(|(i, cell)| {
                        let b = (*cell)?;
                        if ord_passes(b.cmp(&target), is_less, inclusive) { Some(i) } else { None }
                    })
                    .collect()
            } else {
                vec![]
            }
        }
        _ => vec![],
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

    #[test]
    fn typed_cmp_scan_int_range() {
        let table = test_table();
        let matches = table.filter_cmp("age", &serde_json::json!(25), false, false).unwrap();
        // age > 25: rows 0 and 2 (age=30)
        assert_eq!(matches, vec![0, 2]);
    }

    #[test]
    fn typed_eq_scan_null() {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [{"name": "x", "type": "int"}],
            "rowCount": 3,
            "data": {"x": [1, null, 3]}
        });
        let table = validation::validate_and_build(j).unwrap();
        let matches = table.filter_eq("x", &serde_json::Value::Null).unwrap();
        assert_eq!(matches, vec![1]);
    }
}
