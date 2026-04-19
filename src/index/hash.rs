use std::collections::HashMap;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

pub fn build_hash_index(column: &str, data: &ColumnData) -> VtfResult<IndexDef> {
    if data.col_type().is_array() {
        return Err(VtfError::validation(format!(
            "array column '{column}' cannot be indexed"
        )));
    }

    let mut map: HashMap<String, Vec<usize>> = HashMap::new();
    for i in 0..data.len() {
        if let Some(key) = data.value_as_key(i) {
            map.entry(key).or_insert_with(Vec::new).push(i);
        }
    }

    Ok(IndexDef {
        column: column.to_string(),
        index_type: IndexType::Hash,
        column_type: data.col_type(),
        map,
        sorted_keys: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;

    fn test_table() -> VtfTable {
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "tags", "type": "array<string>"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Alice"],
                "tags": [["a"], ["b"], ["c"]]
            }
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn test_build_hash_index() {
        let table = test_table();
        let idx = build_hash_index("name", &table.data["name"]).unwrap();
        assert_eq!(idx.map["Alice"], vec![0, 2]);
        assert_eq!(idx.map["Bob"], vec![1]);
    }

    #[test]
    fn test_cannot_index_array_column() {
        let table = test_table();
        assert!(build_hash_index("tags", &table.data["tags"]).is_err());
    }
}
