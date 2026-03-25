use indexmap::IndexMap;
use serde_json::Value;

use crate::error::{VtfError, VtfResult};
use crate::model::*;
use crate::types;

impl VtfTable {
    /// Insert a row atomically. Either all columns are updated, or none.
    pub fn insert(&mut self, row: IndexMap<String, Value>) -> VtfResult<()> {
        // Step 1: Validate keys — must match columns exactly
        self.validate_insert_keys(&row)?;

        // Step 2: Validate types for each value
        for col in &self.columns {
            let val = &row[&col.name];
            types::validate_value(val, &col.col_type, &col.name, self.row_count)?;
        }

        // Step 3: Check primary key uniqueness
        if let Some(ref pk) = self.meta.primary_key {
            let pk_val = &row[pk];
            if pk_val.is_null() {
                return Err(VtfError::insert(format!(
                    "primary key column '{pk}' cannot be null"
                )));
            }
            self.check_pk_uniqueness(pk, pk_val)?;
        }

        // Step 4: Build new values in temporaries (copy-on-write for atomicity)
        let new_row_idx = self.row_count;
        let mut appended_values: Vec<(String, AppendValue)> = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let val = &row[&col.name];
            let append_val = parse_value_for_append(val, &col.col_type)?;
            appended_values.push((col.name.clone(), append_val));
        }

        // Step 5: Commit — append all values (this cannot fail if validation passed)
        for (col_name, append_val) in &appended_values {
            let col_data = self.data.get_mut(col_name).unwrap();
            append_to_column(col_data, append_val);
        }

        self.row_count += 1;

        // Step 6: Update indexes
        for (col_name, idx) in self.indexes.iter_mut() {
            let col_data = &self.data[col_name];
            if let Some(key) = col_data.value_as_key(new_row_idx) {
                idx.map
                    .entry(key.clone())
                    .or_insert_with(Vec::new)
                    .push(new_row_idx);

                if let Some(ref mut sorted_keys) = idx.sorted_keys {
                    if let Err(pos) = sorted_keys.binary_search(&key) {
                        sorted_keys.insert(pos, key);
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_insert_keys(&self, row: &IndexMap<String, Value>) -> VtfResult<()> {
        let col_names: std::collections::HashSet<&str> =
            self.columns.iter().map(|c| c.name.as_str()).collect();
        let row_keys: std::collections::HashSet<&str> = row.keys().map(|k| k.as_str()).collect();

        for name in &col_names {
            if !row_keys.contains(name) {
                return Err(VtfError::insert(format!("missing column '{name}'")));
            }
        }
        for key in &row_keys {
            if !col_names.contains(key) {
                return Err(VtfError::insert(format!("extra column '{key}'")));
            }
        }
        Ok(())
    }

    fn check_pk_uniqueness(&self, pk: &str, new_val: &Value) -> VtfResult<()> {
        let col_data = &self.data[pk];

        // Use index if available for O(1) check
        if let Some(idx) = self.indexes.get(pk) {
            let key = value_to_key(new_val);
            if idx.map.contains_key(&key) {
                return Err(VtfError::PrimaryKeyViolation {
                    column: pk.to_string(),
                    value: key,
                });
            }
            return Ok(());
        }

        // Fall back to linear scan
        for i in 0..self.row_count {
            let existing = col_data.get_json_value(i).unwrap_or(Value::Null);
            if !existing.is_null() && values_equal(&existing, new_val) {
                return Err(VtfError::PrimaryKeyViolation {
                    column: pk.to_string(),
                    value: format!("{new_val}"),
                });
            }
        }
        Ok(())
    }
}

enum AppendValue {
    Int(Option<i64>),
    Float(Option<f64>),
    Str(Option<String>),
    Bool(Option<bool>),
    Date(Option<String>),
    ArrayInt(Option<Vec<Option<i64>>>),
    ArrayFloat(Option<Vec<Option<f64>>>),
    ArrayStr(Option<Vec<Option<String>>>),
}

fn parse_value_for_append(val: &Value, col_type: &ColumnType) -> VtfResult<AppendValue> {
    if val.is_null() {
        return Ok(match col_type {
            ColumnType::Int => AppendValue::Int(None),
            ColumnType::Float => AppendValue::Float(None),
            ColumnType::String => AppendValue::Str(None),
            ColumnType::Boolean => AppendValue::Bool(None),
            ColumnType::Date => AppendValue::Date(None),
            ColumnType::ArrayInt => AppendValue::ArrayInt(None),
            ColumnType::ArrayFloat => AppendValue::ArrayFloat(None),
            ColumnType::ArrayString => AppendValue::ArrayStr(None),
        });
    }

    Ok(match col_type {
        ColumnType::Int => {
            let n = val.as_i64().or_else(|| val.as_u64().map(|u| u as i64)).unwrap();
            AppendValue::Int(Some(n))
        }
        ColumnType::Float => {
            let n = val.as_f64().unwrap();
            AppendValue::Float(Some(n))
        }
        ColumnType::String => AppendValue::Str(Some(val.as_str().unwrap().to_string())),
        ColumnType::Boolean => AppendValue::Bool(Some(val.as_bool().unwrap())),
        ColumnType::Date => AppendValue::Date(Some(val.as_str().unwrap().to_string())),
        ColumnType::ArrayInt => {
            let arr = val.as_array().unwrap();
            AppendValue::ArrayInt(Some(
                arr.iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)).unwrap())
                        }
                    })
                    .collect(),
            ))
        }
        ColumnType::ArrayFloat => {
            let arr = val.as_array().unwrap();
            AppendValue::ArrayFloat(Some(
                arr.iter()
                    .map(|v| if v.is_null() { None } else { v.as_f64() })
                    .collect(),
            ))
        }
        ColumnType::ArrayString => {
            let arr = val.as_array().unwrap();
            AppendValue::ArrayStr(Some(
                arr.iter()
                    .map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            ))
        }
    })
}

fn append_to_column(col_data: &mut ColumnData, val: &AppendValue) {
    match (col_data, val) {
        (ColumnData::Int(v), AppendValue::Int(n)) => v.push(*n),
        (ColumnData::Float(v), AppendValue::Float(n)) => v.push(*n),
        (ColumnData::Str(v), AppendValue::Str(s)) => v.push(s.clone()),
        (ColumnData::Bool(v), AppendValue::Bool(b)) => v.push(*b),
        (ColumnData::Date(v), AppendValue::Date(s)) => v.push(s.clone()),
        (ColumnData::ArrayInt(v), AppendValue::ArrayInt(a)) => v.push(a.clone()),
        (ColumnData::ArrayFloat(v), AppendValue::ArrayFloat(a)) => v.push(a.clone()),
        (ColumnData::ArrayStr(v), AppendValue::ArrayStr(a)) => v.push(a.clone()),
        _ => unreachable!("type mismatch should have been caught during validation"),
    }
}

fn value_to_key(val: &Value) -> String {
    match val {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        _ => val.to_string(),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            // Compare as i64 if both are integers, otherwise as f64
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
    use crate::model::{Column, ColumnType};

    fn test_table() -> VtfTable {
        let columns = vec![
            Column {
                name: "id".to_string(),
                col_type: ColumnType::Int,
            },
            Column {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
        ];
        let mut table = VtfTable::new(columns);
        table.meta.primary_key = Some("id".to_string());
        table
    }

    #[test]
    fn test_insert_success() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        row.insert("name".to_string(), serde_json::json!("Alice"));
        assert!(table.insert(row).is_ok());
        assert_eq!(table.row_count, 1);
    }

    #[test]
    fn test_insert_missing_column() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        assert!(table.insert(row).is_err());
    }

    #[test]
    fn test_insert_extra_column() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        row.insert("name".to_string(), serde_json::json!("Alice"));
        row.insert("extra".to_string(), serde_json::json!(true));
        assert!(table.insert(row).is_err());
    }

    #[test]
    fn test_insert_type_mismatch() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!("not_an_int"));
        row.insert("name".to_string(), serde_json::json!("Alice"));
        assert!(table.insert(row).is_err());
    }

    #[test]
    fn test_insert_null_pk() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), Value::Null);
        row.insert("name".to_string(), serde_json::json!("Alice"));
        assert!(table.insert(row).is_err());
    }

    #[test]
    fn test_insert_duplicate_pk() {
        let mut table = test_table();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        row.insert("name".to_string(), serde_json::json!("Alice"));
        table.insert(row).unwrap();

        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), serde_json::json!(1));
        row2.insert("name".to_string(), serde_json::json!("Bob"));
        assert!(table.insert(row2).is_err());
    }

    #[test]
    fn test_insert_with_nulls() {
        let mut table = test_table();
        table.meta.primary_key = None;
        let mut row = IndexMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        row.insert("name".to_string(), Value::Null);
        assert!(table.insert(row).is_ok());
        assert_eq!(table.row_count, 1);
    }
}
