use std::collections::{BTreeMap, HashMap};

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

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

/// Range query on a sorted index. Returns row indices where the key satisfies the bound.
/// `low` is inclusive lower bound, `high` is inclusive upper bound. Either can be None.
pub fn range_query(idx: &IndexDef, low: Option<&str>, high: Option<&str>, low_inclusive: bool, high_inclusive: bool) -> Vec<usize> {
    let sorted_keys = match &idx.sorted_keys {
        Some(k) => k,
        None => return Vec::new(),
    };

    let mut result = Vec::new();
    for key in sorted_keys {
        let above_low = match low {
            None => true,
            Some(lo) => {
                if low_inclusive { key.as_str() >= lo } else { key.as_str() > lo }
            }
        };
        let below_high = match high {
            None => true,
            Some(hi) => {
                if high_inclusive { key.as_str() <= hi } else { key.as_str() < hi }
            }
        };
        if above_low && below_high {
            if let Some(rows) = idx.map.get(key) {
                result.extend(rows);
            }
        }
    }
    result.sort_unstable();
    result
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
    fn test_build_sorted_index() {
        let table = test_table();
        let idx = build_sorted_index("name", &table.data["name"]).unwrap();
        let keys = idx.sorted_keys.unwrap();
        assert_eq!(keys, vec!["Alice", "Bob"]);
    }
}
