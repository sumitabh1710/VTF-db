use std::collections::{BTreeMap, HashMap};

use crate::error::{VtfError, VtfResult};
use crate::model::*;

/// Build a hash index for a column.
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
        map,
        sorted_keys: None,
    })
}

/// Build a sorted index for a column.
pub fn build_sorted_index(column: &str, data: &ColumnData) -> VtfResult<IndexDef> {
    if data.col_type().is_array() {
        return Err(VtfError::validation(format!(
            "array column '{column}' cannot be indexed"
        )));
    }

    let mut btree: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for i in 0..data.len() {
        if let Some(key) = data.value_as_key(i) {
            btree.entry(key).or_insert_with(Vec::new).push(i);
        }
    }

    let sorted_keys: Vec<String> = btree.keys().cloned().collect();
    let map: HashMap<String, Vec<usize>> = btree.into_iter().collect();

    Ok(IndexDef {
        column: column.to_string(),
        index_type: IndexType::Sorted,
        map,
        sorted_keys: Some(sorted_keys),
    })
}

impl VtfTable {
    /// Create an index on the given column.
    pub fn create_index(&mut self, column: &str, index_type: IndexType) -> VtfResult<()> {
        let col = self
            .find_column(column)
            .ok_or_else(|| VtfError::validation(format!("column '{column}' not found")))?;

        if col.col_type.is_array() {
            return Err(VtfError::validation(format!(
                "array column '{column}' cannot be indexed"
            )));
        }

        let col_data = &self.data[column];
        let idx = match index_type {
            IndexType::Hash => build_hash_index(column, col_data)?,
            IndexType::Sorted => build_sorted_index(column, col_data)?,
        };

        self.indexes.insert(column.to_string(), idx);
        Ok(())
    }

    /// Drop an index on the given column.
    pub fn drop_index(&mut self, column: &str) -> VtfResult<()> {
        if self.indexes.shift_remove(column).is_none() {
            return Err(VtfError::validation(format!(
                "no index exists on column '{column}'"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation;

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
    fn test_build_sorted_index() {
        let table = test_table();
        let idx = build_sorted_index("name", &table.data["name"]).unwrap();
        let keys = idx.sorted_keys.unwrap();
        assert_eq!(keys, vec!["Alice", "Bob"]);
    }

    #[test]
    fn test_cannot_index_array_column() {
        let table = test_table();
        assert!(build_hash_index("tags", &table.data["tags"]).is_err());
    }

    #[test]
    fn test_create_index_on_table() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        assert!(table.indexes.contains_key("name"));

        // Query should now use the index
        let matches = table.filter_eq("name", &serde_json::json!("Bob")).unwrap();
        assert_eq!(matches, vec![1]);
    }

    #[test]
    fn test_drop_index() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        assert!(table.drop_index("name").is_ok());
        assert!(!table.indexes.contains_key("name"));
    }

    #[test]
    fn test_create_index_array_rejected() {
        let mut table = test_table();
        assert!(table.create_index("tags", IndexType::Hash).is_err());
    }
}
