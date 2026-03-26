use std::collections::HashSet;

use indexmap::IndexMap;
use serde_json::Value;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;
use crate::core::types;

impl VtfTable {
    /// Update rows at the given indices with the provided values.
    /// `values` can be a subset of columns (partial update).
    /// Returns the number of rows updated.
    pub fn update(
        &mut self,
        indices: &[usize],
        values: IndexMap<String, Value>,
    ) -> VtfResult<usize> {
        if indices.is_empty() || values.is_empty() {
            return Ok(0);
        }

        for &idx in indices {
            if idx >= self.row_count {
                return Err(VtfError::query(format!(
                    "update: row index {idx} out of bounds (rowCount: {})",
                    self.row_count
                )));
            }
        }

        for key in values.keys() {
            if self.find_column(key).is_none() {
                return Err(VtfError::insert(format!(
                    "update: unknown column '{key}'"
                )));
            }
        }

        for (col_name, val) in &values {
            let col = self.find_column(col_name).unwrap();
            types::validate_value(val, &col.col_type, col_name, 0)?;
        }

        if let Some(ref pk) = self.meta.primary_key {
            if let Some(new_pk_val) = values.get(pk) {
                if new_pk_val.is_null() {
                    return Err(VtfError::insert(format!(
                        "update: primary key column '{pk}' cannot be set to null"
                    )));
                }
                let updating_set: HashSet<usize> = indices.iter().copied().collect();
                self.check_pk_update_uniqueness(pk, new_pk_val, &updating_set)?;
            }
        }

        let deduped: Vec<usize> = {
            let mut s: Vec<usize> = indices.to_vec();
            s.sort_unstable();
            s.dedup();
            s
        };
        let count = deduped.len();

        for &idx in &deduped {
            for (col_name, val) in &values {
                let col_data = self.data.get_mut(col_name).unwrap();
                col_data.set(idx, val);
            }
        }

        if !self.indexes.is_empty() {
            self.rebuild_indexes()?;
        }

        Ok(count)
    }

    fn check_pk_update_uniqueness(
        &self,
        pk: &str,
        new_val: &Value,
        updating_indices: &HashSet<usize>,
    ) -> VtfResult<()> {
        let col_data = &self.data[pk];

        for i in 0..self.row_count {
            if updating_indices.contains(&i) {
                continue;
            }
            let existing = col_data.get_json_value(i).unwrap_or(Value::Null);
            if !existing.is_null() && values_match(&existing, new_val) {
                return Err(VtfError::PrimaryKeyViolation {
                    column: pk.to_string(),
                    value: format!("{new_val}"),
                });
            }
        }

        if updating_indices.len() > 1 {
            return Err(VtfError::PrimaryKeyViolation {
                column: pk.to_string(),
                value: format!("{new_val}"),
            });
        }

        Ok(())
    }
}

fn values_match(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            if let (Some(ai), Some(bi)) = (an.as_i64(), bn.as_i64()) {
                ai == bi
            } else {
                an.as_f64() == bn.as_f64()
            }
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::storage::validation;

    fn test_table() -> VtfTable {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn update_single_column() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("name".to_string(), json!("Alicia"));
        let updated = table.update(&[0], vals).unwrap();
        assert_eq!(updated, 1);
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alicia"));
        assert_eq!(rows[0]["age"], json!(30));
    }

    #[test]
    fn update_multiple_columns() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("name".to_string(), json!("Robert"));
        vals.insert("age".to_string(), json!(26));
        let updated = table.update(&[1], vals).unwrap();
        assert_eq!(updated, 1);
        let rows = table.select_rows(&[1], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Robert"));
        assert_eq!(rows[0]["age"], json!(26));
    }

    #[test]
    fn update_multiple_rows() {
        let mut table = test_table();
        table.meta.primary_key = None;
        let mut vals = IndexMap::new();
        vals.insert("age".to_string(), json!(99));
        let updated = table.update(&[0, 2], vals).unwrap();
        assert_eq!(updated, 2);
        let rows = table.select_rows(&[0, 2], &[]).unwrap();
        assert_eq!(rows[0]["age"], json!(99));
        assert_eq!(rows[1]["age"], json!(99));
    }

    #[test]
    fn update_to_null() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("name".to_string(), Value::Null);
        table.update(&[0], vals).unwrap();
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert!(rows[0]["name"].is_null());
    }

    #[test]
    fn update_pk_succeeds_when_unique() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("id".to_string(), json!(99));
        table.update(&[0], vals).unwrap();
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["id"], json!(99));
    }

    #[test]
    fn update_pk_rejects_duplicate() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("id".to_string(), json!(2));
        assert!(table.update(&[0], vals).is_err());
    }

    #[test]
    fn update_pk_rejects_null() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("id".to_string(), Value::Null);
        assert!(table.update(&[0], vals).is_err());
    }

    #[test]
    fn update_rejects_unknown_column() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("nonexistent".to_string(), json!(1));
        assert!(table.update(&[0], vals).is_err());
    }

    #[test]
    fn update_rejects_type_mismatch() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("age".to_string(), json!("not_a_number"));
        assert!(table.update(&[0], vals).is_err());
    }

    #[test]
    fn update_out_of_bounds() {
        let mut table = test_table();
        let mut vals = IndexMap::new();
        vals.insert("age".to_string(), json!(50));
        assert!(table.update(&[99], vals).is_err());
    }

    #[test]
    fn update_empty_noop() {
        let mut table = test_table();
        assert_eq!(table.update(&[], IndexMap::new()).unwrap(), 0);
    }

    #[test]
    fn update_rebuilds_indexes() {
        let mut table = test_table();
        table.create_index("name", crate::IndexType::Hash).unwrap();

        let mut vals = IndexMap::new();
        vals.insert("name".to_string(), json!("Alicia"));
        table.update(&[0], vals).unwrap();

        let matches = table.filter_eq("name", &json!("Alice")).unwrap();
        assert!(matches.is_empty());
        let matches = table.filter_eq("name", &json!("Alicia")).unwrap();
        assert_eq!(matches, vec![0]);
    }
}
