use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

impl VtfTable {
    /// Delete rows at the given indices. Returns the number of rows deleted.
    /// Indexes are rebuilt after deletion since row indices shift.
    pub fn delete(&mut self, indices: &[usize]) -> VtfResult<usize> {
        if indices.is_empty() {
            return Ok(0);
        }

        for &idx in indices {
            if idx >= self.row_count {
                return Err(VtfError::query(format!(
                    "delete: row index {idx} out of bounds (rowCount: {})",
                    self.row_count
                )));
            }
        }

        let mut sorted: Vec<usize> = indices.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let count = sorted.len();

        for &idx in sorted.iter().rev() {
            for col_data in self.data.values_mut() {
                col_data.remove(idx);
            }
        }

        self.row_count -= count;

        if !self.indexes.is_empty() {
            self.rebuild_indexes()?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::storage::validation;

    fn test_table() -> crate::VtfTable {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 4,
            "data": {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn delete_single_row() {
        let mut table = test_table();
        let deleted = table.delete(&[1]).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(table.row_count, 3);
        let rows = table.select_rows(&[0, 1, 2], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alice"));
        assert_eq!(rows[1]["name"], json!("Charlie"));
        assert_eq!(rows[2]["name"], json!("Dave"));
    }

    #[test]
    fn delete_multiple_rows() {
        let mut table = test_table();
        let deleted = table.delete(&[0, 2]).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(table.row_count, 2);
        let rows = table.select_rows(&[0, 1], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Bob"));
        assert_eq!(rows[1]["name"], json!("Dave"));
    }

    #[test]
    fn delete_all_rows() {
        let mut table = test_table();
        let deleted = table.delete(&[0, 1, 2, 3]).unwrap();
        assert_eq!(deleted, 4);
        assert_eq!(table.row_count, 0);
    }

    #[test]
    fn delete_empty_indices() {
        let mut table = test_table();
        let deleted = table.delete(&[]).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(table.row_count, 4);
    }

    #[test]
    fn delete_out_of_bounds() {
        let mut table = test_table();
        assert!(table.delete(&[10]).is_err());
        assert_eq!(table.row_count, 4);
    }

    #[test]
    fn delete_duplicate_indices_deduped() {
        let mut table = test_table();
        let deleted = table.delete(&[1, 1, 1]).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(table.row_count, 3);
    }

    #[test]
    fn delete_rebuilds_indexes() {
        let mut table = test_table();
        table.create_index("name", crate::IndexType::Hash).unwrap();
        table.delete(&[0]).unwrap();

        let matches = table.filter_eq("name", &json!("Alice")).unwrap();
        assert!(matches.is_empty());

        let matches = table.filter_eq("name", &json!("Bob")).unwrap();
        assert_eq!(matches, vec![0]);
    }

    #[test]
    fn delete_then_insert() {
        let mut table = test_table();
        table.delete(&[1, 2]).unwrap();
        assert_eq!(table.row_count, 2);

        let mut row = indexmap::IndexMap::new();
        row.insert("id".to_string(), json!(5));
        row.insert("name".to_string(), json!("Eve"));
        table.insert(row).unwrap();
        assert_eq!(table.row_count, 3);
    }
}
