use serde_json::Value;

use crate::core::model::*;
use crate::core::error::VtfResult;

impl VtfTable {
    pub fn to_json(&self) -> VtfResult<String> {
        let val = self.to_json_value();
        Ok(serde_json::to_string(&val)?)
    }

    pub fn to_pretty_json(&self) -> VtfResult<String> {
        let val = self.to_json_value();
        Ok(serde_json::to_string_pretty(&val)?)
    }

    fn to_json_value(&self) -> Value {
        let mut obj = serde_json::Map::new();

        obj.insert("version".to_string(), Value::String(self.version.clone()));

        let columns: Vec<Value> = self
            .columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "type": c.col_type.as_str()
                })
            })
            .collect();
        obj.insert("columns".to_string(), Value::Array(columns));

        obj.insert(
            "rowCount".to_string(),
            Value::Number(serde_json::Number::from(self.row_count)),
        );

        let mut data_obj = serde_json::Map::new();
        for col in &self.columns {
            let col_data = &self.data[&col.name];
            let arr: Vec<Value> = (0..self.row_count)
                .map(|i| col_data.get_json_value(i).unwrap_or(Value::Null))
                .collect();
            data_obj.insert(col.name.clone(), Value::Array(arr));
        }
        obj.insert("data".to_string(), Value::Object(data_obj));

        let mut meta_obj = serde_json::Map::new();
        if let Some(ref pk) = self.meta.primary_key {
            meta_obj.insert("primaryKey".to_string(), Value::String(pk.clone()));
        }
        obj.insert("meta".to_string(), Value::Object(meta_obj));

        let mut indexes_obj = serde_json::Map::new();
        for (col_name, idx) in &self.indexes {
            let mut idx_obj = serde_json::Map::new();
            match &idx.index_type {
                IndexType::Hash => {
                    idx_obj.insert("type".to_string(), Value::String("hash".to_string()));
                    let mut map_obj = serde_json::Map::new();
                    for (key, rows) in &idx.map {
                        map_obj.insert(
                            key.clone(),
                            Value::Array(rows.iter().map(|&r| Value::from(r as u64)).collect()),
                        );
                    }
                    idx_obj.insert("map".to_string(), Value::Object(map_obj));
                }
                IndexType::Sorted => {
                    idx_obj.insert("type".to_string(), Value::String("sorted".to_string()));
                    if let Some(ref keys) = idx.sorted_keys {
                        idx_obj.insert(
                            "values".to_string(),
                            Value::Array(
                                keys.iter().map(|k| Value::String(k.clone())).collect(),
                            ),
                        );
                    }
                    let mut map_obj = serde_json::Map::new();
                    for (key, rows) in &idx.map {
                        map_obj.insert(
                            key.clone(),
                            Value::Array(rows.iter().map(|&r| Value::from(r as u64)).collect()),
                        );
                    }
                    idx_obj.insert("rowMap".to_string(), Value::Object(map_obj));
                }
            }
            indexes_obj.insert(col_name.clone(), Value::Object(idx_obj));
        }
        obj.insert("indexes".to_string(), Value::Object(indexes_obj));

        obj.insert("extensions".to_string(), self.extensions.clone());

        Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::model::*;
    use crate::storage::validation;

    fn sample_table() -> VtfTable {
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

        if let Some(ColumnData::Int(ref mut v)) = table.data.get_mut("id") {
            v.push(Some(1));
            v.push(Some(2));
        }
        if let Some(ColumnData::Str(ref mut v)) = table.data.get_mut("name") {
            v.push(Some("Alice".to_string()));
            v.push(Some("Bob".to_string()));
        }
        table.row_count = 2;
        table
    }

    #[test]
    fn test_roundtrip() {
        let table = sample_table();
        let json = table.to_json().unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        let loaded = validation::validate_and_build(raw).unwrap();
        assert_eq!(loaded.row_count, 2);
        assert_eq!(loaded.columns.len(), 2);
        assert_eq!(loaded.meta.primary_key, Some("id".to_string()));
    }

    #[test]
    fn test_pretty_json() {
        let table = sample_table();
        let pretty = table.to_pretty_json().unwrap();
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("  "));
    }
}
