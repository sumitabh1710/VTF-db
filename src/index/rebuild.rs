use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;
use crate::index::{build_hash_index, build_sorted_index};

impl VtfTable {
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

    pub fn rebuild_indexes(&mut self) -> VtfResult<()> {
        let index_specs: Vec<(String, IndexType)> = self
            .indexes
            .iter()
            .map(|(name, idx)| {
                let it = match idx.index_type {
                    IndexType::Hash => IndexType::Hash,
                    IndexType::Sorted => IndexType::Sorted,
                };
                (name.clone(), it)
            })
            .collect();

        for (col_name, idx_type) in index_specs {
            let col_data = &self.data[&col_name];
            let new_idx = match idx_type {
                IndexType::Hash => build_hash_index(&col_name, col_data)?,
                IndexType::Sorted => build_sorted_index(&col_name, col_data)?,
            };
            self.indexes.insert(col_name, new_idx);
        }
        Ok(())
    }

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
    use crate::core::model::*;
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
    fn test_create_index_on_table() {
        let mut table = test_table();
        table.create_index("name", IndexType::Hash).unwrap();
        assert!(table.indexes.contains_key("name"));

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
