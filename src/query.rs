use indexmap::IndexMap;
use serde_json::Value;

use crate::error::{VtfError, VtfResult};
use crate::model::*;

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

        // Try index-accelerated path
        if let Some(idx) = self.indexes.get(column) {
            let key = value_to_search_key(value);
            return Ok(idx.map.get(&key).cloned().unwrap_or_default());
        }

        // Linear scan
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
    use crate::validation;

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
