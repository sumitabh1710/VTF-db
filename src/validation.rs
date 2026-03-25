use std::collections::HashSet;

use indexmap::IndexMap;
use serde_json::Value;

use crate::error::{VtfError, VtfResult};
use crate::model::*;
use crate::types;

/// Parse and validate a raw JSON value into a fully validated VtfTable.
/// Validation runs in strict order — stops immediately on first failure.
///
/// Steps: version -> columns -> data keys -> column lengths -> types -> primary key -> indexes
pub fn validate_and_build(raw: Value) -> VtfResult<VtfTable> {
    let obj = raw
        .as_object()
        .ok_or_else(|| VtfError::validation("top-level value must be a JSON object"))?;

    // Step 1: Version
    let version = validate_version(obj)?;

    // Step 2: Columns
    let columns = validate_columns(obj)?;

    // Step 3: RowCount
    let row_count = validate_row_count(obj)?;

    // Step 4: Data keys match columns
    let raw_data = validate_data_keys(obj, &columns)?;

    // Step 5: Column lengths must equal rowCount
    validate_column_lengths(&raw_data, &columns, row_count)?;

    // Step 6: Type checking — every value matches declared type
    validate_types(&raw_data, &columns)?;

    // Build typed column data
    let data = build_column_data(&raw_data, &columns)?;

    // Step 7: Meta / Primary key
    let meta = validate_meta(obj, &columns, &data, row_count)?;

    // Step 8: Indexes
    let indexes = validate_indexes(obj, &columns, &data, row_count)?;

    // Extensions (optional, pass through)
    let extensions = obj
        .get("extensions")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

    Ok(VtfTable {
        version,
        columns,
        row_count,
        data,
        meta,
        indexes,
        extensions,
    })
}

fn validate_version(obj: &serde_json::Map<String, Value>) -> VtfResult<String> {
    let v = obj
        .get("version")
        .ok_or_else(|| VtfError::validation("missing required field 'version'"))?;
    let s = v
        .as_str()
        .ok_or_else(|| VtfError::validation("'version' must be a string"))?;
    if s != "1.0" {
        return Err(VtfError::validation(format!(
            "'version' must be \"1.0\", got \"{s}\""
        )));
    }
    Ok(s.to_string())
}

fn validate_columns(obj: &serde_json::Map<String, Value>) -> VtfResult<Vec<Column>> {
    let cols_val = obj
        .get("columns")
        .ok_or_else(|| VtfError::validation("missing required field 'columns'"))?;
    let cols_arr = cols_val
        .as_array()
        .ok_or_else(|| VtfError::validation("'columns' must be an array"))?;
    if cols_arr.is_empty() {
        return Err(VtfError::validation("'columns' must be non-empty"));
    }

    let mut names = HashSet::new();
    let mut columns = Vec::with_capacity(cols_arr.len());

    for (i, col_val) in cols_arr.iter().enumerate() {
        let col_obj = col_val.as_object().ok_or_else(|| {
            VtfError::validation(format!("columns[{i}] must be an object"))
        })?;

        let name = col_obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                VtfError::validation(format!("columns[{i}] must have a string 'name'"))
            })?;

        if !names.insert(name.to_string()) {
            return Err(VtfError::validation(format!(
                "duplicate column name: '{name}'"
            )));
        }

        let type_str = col_obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                VtfError::validation(format!(
                    "columns[{i}] ('{name}') must have a string 'type'"
                ))
            })?;

        let col_type = ColumnType::from_str(type_str).map_err(|_| {
            VtfError::validation(format!(
                "columns[{i}] ('{name}'): invalid type '{type_str}'"
            ))
        })?;

        columns.push(Column {
            name: name.to_string(),
            col_type,
        });
    }

    Ok(columns)
}

fn validate_row_count(obj: &serde_json::Map<String, Value>) -> VtfResult<usize> {
    let rc = obj
        .get("rowCount")
        .ok_or_else(|| VtfError::validation("missing required field 'rowCount'"))?;
    let n = rc
        .as_u64()
        .ok_or_else(|| VtfError::validation("'rowCount' must be a non-negative integer"))?;
    Ok(n as usize)
}

fn validate_data_keys<'a>(
    obj: &'a serde_json::Map<String, Value>,
    columns: &[Column],
) -> VtfResult<&'a serde_json::Map<String, Value>> {
    let data_val = obj
        .get("data")
        .ok_or_else(|| VtfError::validation("missing required field 'data'"))?;
    let data_obj = data_val
        .as_object()
        .ok_or_else(|| VtfError::validation("'data' must be an object"))?;

    let col_names: HashSet<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    let data_keys: HashSet<&str> = data_obj.keys().map(|k| k.as_str()).collect();

    for name in &col_names {
        if !data_keys.contains(name) {
            return Err(VtfError::validation(format!(
                "column '{name}' declared in schema but missing from data"
            )));
        }
    }

    for key in &data_keys {
        if !col_names.contains(key) {
            return Err(VtfError::validation(format!(
                "extra key '{key}' in data not declared in columns"
            )));
        }
    }

    Ok(data_obj)
}

fn validate_column_lengths(
    data: &serde_json::Map<String, Value>,
    columns: &[Column],
    row_count: usize,
) -> VtfResult<()> {
    for col in columns {
        let arr = data
            .get(&col.name)
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                VtfError::validation(format!("data['{}'] must be an array", col.name))
            })?;

        if arr.len() != row_count {
            return Err(VtfError::validation(format!(
                "column '{}' has length {} but rowCount is {}",
                col.name,
                arr.len(),
                row_count
            )));
        }
    }
    Ok(())
}

fn validate_types(
    data: &serde_json::Map<String, Value>,
    columns: &[Column],
) -> VtfResult<()> {
    for col in columns {
        let arr = data[&col.name].as_array().unwrap();
        for (row, val) in arr.iter().enumerate() {
            types::validate_value(val, &col.col_type, &col.name, row)?;
        }
    }
    Ok(())
}

fn build_column_data(
    raw_data: &serde_json::Map<String, Value>,
    columns: &[Column],
) -> VtfResult<IndexMap<String, ColumnData>> {
    let mut data = IndexMap::new();

    for col in columns {
        let arr = raw_data[&col.name].as_array().unwrap();
        let column_data = match &col.col_type {
            ColumnType::Int => {
                ColumnData::Int(arr.iter().map(|v| {
                    if v.is_null() { None }
                    else if let Some(n) = v.as_i64() { Some(n) }
                    else { Some(v.as_u64().unwrap() as i64) }
                }).collect())
            }
            ColumnType::Float => {
                ColumnData::Float(arr.iter().map(|v| {
                    if v.is_null() { None }
                    else if let Some(n) = v.as_f64() { Some(n) }
                    else { None }
                }).collect())
            }
            ColumnType::String => {
                ColumnData::Str(arr.iter().map(|v| {
                    v.as_str().map(|s| s.to_string())
                }).collect())
            }
            ColumnType::Boolean => {
                ColumnData::Bool(arr.iter().map(|v| v.as_bool()).collect())
            }
            ColumnType::Date => {
                ColumnData::Date(arr.iter().map(|v| {
                    v.as_str().map(|s| s.to_string())
                }).collect())
            }
            ColumnType::ArrayInt => {
                ColumnData::ArrayInt(arr.iter().map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_array().unwrap().iter().map(|elem| {
                            if elem.is_null() { None }
                            else if let Some(n) = elem.as_i64() { Some(n) }
                            else { Some(elem.as_u64().unwrap() as i64) }
                        }).collect())
                    }
                }).collect())
            }
            ColumnType::ArrayFloat => {
                ColumnData::ArrayFloat(arr.iter().map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_array().unwrap().iter().map(|elem| {
                            if elem.is_null() { None }
                            else { elem.as_f64() }
                        }).collect())
                    }
                }).collect())
            }
            ColumnType::ArrayString => {
                ColumnData::ArrayStr(arr.iter().map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_array().unwrap().iter().map(|elem| {
                            elem.as_str().map(|s| s.to_string())
                        }).collect())
                    }
                }).collect())
            }
        };
        data.insert(col.name.clone(), column_data);
    }

    Ok(data)
}

fn validate_meta(
    obj: &serde_json::Map<String, Value>,
    columns: &[Column],
    data: &IndexMap<String, ColumnData>,
    _row_count: usize,
) -> VtfResult<Meta> {
    let meta_val = match obj.get("meta") {
        Some(v) => v,
        None => return Ok(Meta { primary_key: None }),
    };

    let meta_obj = meta_val
        .as_object()
        .ok_or_else(|| VtfError::validation("'meta' must be an object"))?;

    let primary_key = match meta_obj.get("primaryKey") {
        Some(pk_val) => {
            let pk = pk_val
                .as_str()
                .ok_or_else(|| VtfError::validation("'meta.primaryKey' must be a string"))?;

            if !columns.iter().any(|c| c.name == pk) {
                return Err(VtfError::validation(format!(
                    "primary key column '{pk}' does not exist"
                )));
            }

            let col_data = &data[pk];
            validate_primary_key_values(pk, col_data)?;

            Some(pk.to_string())
        }
        None => None,
    };

    Ok(Meta { primary_key })
}

fn validate_primary_key_values(pk: &str, data: &ColumnData) -> VtfResult<()> {
    let mut seen = HashSet::new();

    for i in 0..data.len() {
        let key = data.value_as_key(i);
        match key {
            Some(ref k) if k == "null" => {
                // Check if the actual value is null
                let json_val = data.get_json_value(i).unwrap();
                if json_val.is_null() {
                    return Err(VtfError::validation(format!(
                        "primary key column '{pk}' contains null at row {i}"
                    )));
                }
                if !seen.insert(k.clone()) {
                    return Err(VtfError::PrimaryKeyViolation {
                        column: pk.to_string(),
                        value: k.clone(),
                    });
                }
            }
            Some(k) => {
                if !seen.insert(k.clone()) {
                    return Err(VtfError::PrimaryKeyViolation {
                        column: pk.to_string(),
                        value: k,
                    });
                }
            }
            None => {}
        }
    }

    Ok(())
}

fn validate_indexes(
    obj: &serde_json::Map<String, Value>,
    columns: &[Column],
    _data: &IndexMap<String, ColumnData>,
    row_count: usize,
) -> VtfResult<IndexMap<String, IndexDef>> {
    let indexes_val = match obj.get("indexes") {
        Some(v) => v,
        None => return Ok(IndexMap::new()),
    };

    let indexes_obj = indexes_val
        .as_object()
        .ok_or_else(|| VtfError::validation("'indexes' must be an object"))?;

    let mut result = IndexMap::new();

    for (col_name, idx_val) in indexes_obj {
        let col = columns
            .iter()
            .find(|c| c.name == *col_name)
            .ok_or_else(|| {
                VtfError::validation(format!(
                    "index references non-existent column '{col_name}'"
                ))
            })?;

        if col.col_type.is_array() {
            return Err(VtfError::validation(format!(
                "array column '{col_name}' cannot be indexed"
            )));
        }

        let idx_obj = idx_val.as_object().ok_or_else(|| {
            VtfError::validation(format!("index for '{col_name}' must be an object"))
        })?;

        let idx_type_str = idx_obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                VtfError::validation(format!(
                    "index for '{col_name}' must have a string 'type'"
                ))
            })?;

        let index_type = match idx_type_str {
            "hash" => IndexType::Hash,
            "sorted" => IndexType::Sorted,
            other => {
                return Err(VtfError::validation(format!(
                    "unknown index type '{other}' for column '{col_name}'"
                )))
            }
        };

        let map_val = idx_obj.get("map").or_else(|| idx_obj.get("rowMap"));
        let map = if let Some(map_val) = map_val {
            let map_obj = map_val.as_object().ok_or_else(|| {
                VtfError::validation(format!(
                    "index map for '{col_name}' must be an object"
                ))
            })?;

            let mut hm = std::collections::HashMap::new();
            for (key, rows_val) in map_obj {
                let rows_arr = rows_val.as_array().ok_or_else(|| {
                    VtfError::validation(format!(
                        "index map value for key '{key}' in '{col_name}' must be an array"
                    ))
                })?;

                let mut rows = Vec::new();
                for rv in rows_arr {
                    let row_idx = rv.as_u64().ok_or_else(|| {
                        VtfError::validation(format!(
                            "index row value must be a non-negative integer in '{col_name}'"
                        ))
                    })? as usize;

                    if row_idx >= row_count {
                        return Err(VtfError::validation(format!(
                            "index for '{col_name}' references row {row_idx} but rowCount is {row_count}"
                        )));
                    }
                    rows.push(row_idx);
                }
                hm.insert(key.clone(), rows);
            }
            hm
        } else {
            std::collections::HashMap::new()
        };

        let sorted_keys = if let IndexType::Sorted = &index_type {
            if let Some(vals) = idx_obj.get("values") {
                let vals_arr = vals.as_array().ok_or_else(|| {
                    VtfError::validation(format!(
                        "sorted index 'values' for '{col_name}' must be an array"
                    ))
                })?;
                Some(
                    vals_arr
                        .iter()
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "null".to_string(),
                            _ => v.to_string(),
                        })
                        .collect(),
                )
            } else {
                None
            }
        } else {
            None
        };

        result.insert(
            col_name.clone(),
            IndexDef {
                column: col_name.clone(),
                index_type,
                map,
                sorted_keys,
            },
        );
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_json() -> Value {
        serde_json::json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 2,
            "data": {
                "id": [1, 2],
                "name": ["Alice", "Bob"]
            },
            "meta": {
                "primaryKey": "id"
            },
            "indexes": {},
            "extensions": {}
        })
    }

    #[test]
    fn test_valid_table() {
        let table = validate_and_build(valid_json()).unwrap();
        assert_eq!(table.version, "1.0");
        assert_eq!(table.row_count, 2);
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.meta.primary_key, Some("id".to_string()));
    }

    #[test]
    fn test_missing_version() {
        let mut j = valid_json();
        j.as_object_mut().unwrap().remove("version");
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_wrong_version() {
        let mut j = valid_json();
        j["version"] = serde_json::json!("2.0");
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_empty_columns() {
        let mut j = valid_json();
        j["columns"] = serde_json::json!([]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_duplicate_column_names() {
        let mut j = valid_json();
        j["columns"] = serde_json::json!([
            {"name": "id", "type": "int"},
            {"name": "id", "type": "string"}
        ]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_extra_data_key() {
        let mut j = valid_json();
        j["data"]["extra"] = serde_json::json!([1, 2]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_missing_data_key() {
        let mut j = valid_json();
        j["data"].as_object_mut().unwrap().remove("name");
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_unequal_column_length() {
        let mut j = valid_json();
        j["data"]["id"] = serde_json::json!([1]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_type_mismatch() {
        let mut j = valid_json();
        j["data"]["id"] = serde_json::json!(["one", "two"]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_null_primary_key() {
        let mut j = valid_json();
        j["data"]["id"] = serde_json::json!([1, null]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_duplicate_primary_key() {
        let mut j = valid_json();
        j["data"]["id"] = serde_json::json!([1, 1]);
        assert!(validate_and_build(j).is_err());
    }

    #[test]
    fn test_empty_table() {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [{"name": "id", "type": "int"}],
            "rowCount": 0,
            "data": {"id": []}
        });
        let table = validate_and_build(j).unwrap();
        assert_eq!(table.row_count, 0);
    }

    #[test]
    fn test_null_values_allowed() {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"}
            ],
            "rowCount": 2,
            "data": {
                "id": [1, 2],
                "value": [null, "hello"]
            }
        });
        let table = validate_and_build(j).unwrap();
        assert_eq!(table.row_count, 2);
    }

    #[test]
    fn test_array_column_not_indexable() {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [{"name": "tags", "type": "array<string>"}],
            "rowCount": 1,
            "data": {"tags": [["a", "b"]]},
            "indexes": {
                "tags": {"type": "hash", "map": {}}
            }
        });
        assert!(validate_and_build(j).is_err());
    }
}
