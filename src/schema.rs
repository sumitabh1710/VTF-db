use crate::error::{VtfError, VtfResult};
use crate::model::*;

impl VtfTable {
    /// Add a new column to the table. Existing rows are backfilled with null.
    pub fn add_column(&mut self, name: &str, col_type: ColumnType) -> VtfResult<()> {
        if self.columns.iter().any(|c| c.name == name) {
            return Err(VtfError::schema(format!(
                "column '{name}' already exists"
            )));
        }

        let mut col_data = ColumnData::empty_for_type(&col_type);
        for _ in 0..self.row_count {
            col_data.push_null();
        }

        self.columns.push(Column {
            name: name.to_string(),
            col_type,
        });
        self.data.insert(name.to_string(), col_data);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::*;

    #[test]
    fn test_add_column_empty_table() {
        let columns = vec![Column {
            name: "id".to_string(),
            col_type: ColumnType::Int,
        }];
        let mut table = VtfTable::new(columns);
        table.add_column("name", ColumnType::String).unwrap();
        assert_eq!(table.columns.len(), 2);
        assert!(table.data.contains_key("name"));
    }

    #[test]
    fn test_add_column_with_rows() {
        let columns = vec![Column {
            name: "id".to_string(),
            col_type: ColumnType::Int,
        }];
        let mut table = VtfTable::new(columns);
        // Manually add rows
        if let Some(ColumnData::Int(ref mut v)) = table.data.get_mut("id") {
            v.push(Some(1));
            v.push(Some(2));
        }
        table.row_count = 2;

        table.add_column("name", ColumnType::String).unwrap();
        let name_data = &table.data["name"];
        assert_eq!(name_data.len(), 2);
        // Both should be null
        assert!(matches!(
            name_data.get_json_value(0),
            Some(serde_json::Value::Null)
        ));
    }

    #[test]
    fn test_add_duplicate_column() {
        let columns = vec![Column {
            name: "id".to_string(),
            col_type: ColumnType::Int,
        }];
        let mut table = VtfTable::new(columns);
        assert!(table.add_column("id", ColumnType::String).is_err());
    }
}
