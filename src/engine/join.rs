use std::collections::HashMap;

use indexmap::IndexMap;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

/// Join two tables on equal values of a column from each table using a hash join.
///
/// The smaller table (by row count) is used as the build side.  All output
/// columns are prefixed: `left_<col>` for columns from `left` and
/// `right_<col>` for columns from `right`.  This avoids naming collisions when
/// both tables share column names (e.g. both have `id`).
///
/// The join algorithm:
/// 1. Build a `HashMap<key, Vec<build_row_idx>>` from the build side.
/// 2. Probe each row of the probe side; emit a combined row for every match.
pub fn hash_join(
    left: &VtfTable,
    left_col: &str,
    right: &VtfTable,
    right_col: &str,
) -> VtfResult<VtfTable> {
    // Validate join columns exist
    if left.find_column(left_col).is_none() {
        return Err(VtfError::query(format!(
            "join: column '{left_col}' not found in left table"
        )));
    }
    if right.find_column(right_col).is_none() {
        return Err(VtfError::query(format!(
            "join: column '{right_col}' not found in right table"
        )));
    }

    // Build output schema: all left columns prefixed "left_", all right columns prefixed "right_"
    let mut out_columns: Vec<Column> = Vec::new();
    for col in &left.columns {
        out_columns.push(Column {
            name: format!("left_{}", col.name),
            col_type: col.col_type.clone(),
        });
    }
    for col in &right.columns {
        out_columns.push(Column {
            name: format!("right_{}", col.name),
            col_type: col.col_type.clone(),
        });
    }

    let mut out_table = VtfTable::new(out_columns);

    // Decide build vs probe side — smaller table is the build side
    let (build_table, build_col, probe_table, probe_col, left_is_build) =
        if left.row_count <= right.row_count {
            (left, left_col, right, right_col, true)
        } else {
            (right, right_col, left, left_col, false)
        };

    // Build phase: key → list of build row indices
    let build_data = &build_table.data[build_col];
    let mut build_map: HashMap<String, Vec<usize>> = HashMap::new();
    for i in 0..build_table.row_count {
        if let Some(key) = build_data.value_as_key(i) {
            build_map.entry(key).or_default().push(i);
        }
    }

    // Probe phase: for each probe row find matching build rows and emit combined rows
    let probe_data = &probe_table.data[probe_col];
    for probe_i in 0..probe_table.row_count {
        let key = match probe_data.value_as_key(probe_i) {
            Some(k) => k,
            None => continue,
        };
        let build_rows = match build_map.get(&key) {
            Some(rows) => rows,
            None => continue,
        };

        for &build_i in build_rows {
            // Reconstruct the combined row in schema order (left_* then right_*)
            let (actual_left_i, actual_right_i) = if left_is_build {
                (build_i, probe_i)
            } else {
                (probe_i, build_i)
            };

            let mut row: IndexMap<String, serde_json::Value> = IndexMap::new();
            for col in &left.columns {
                let val = left.data[&col.name].get_json_value(actual_left_i)
                    .unwrap_or(serde_json::Value::Null);
                row.insert(format!("left_{}", col.name), val);
            }
            for col in &right.columns {
                let val = right.data[&col.name].get_json_value(actual_right_i)
                    .unwrap_or(serde_json::Value::Null);
                row.insert(format!("right_{}", col.name), val);
            }

            out_table.insert(row)?;
        }
    }

    Ok(out_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    fn users_table() -> VtfTable {
        validation::validate_and_build(json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"]
            },
            "meta": {"primaryKey": "id"}
        })).unwrap()
    }

    fn orders_table() -> VtfTable {
        validation::validate_and_build(json!({
            "version": "1.0",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "user_id", "type": "int"},
                {"name": "amount", "type": "float"}
            ],
            "rowCount": 4,
            "data": {
                "order_id": [101, 102, 103, 104],
                "user_id":  [1, 2, 1, 4],
                "amount":   [9.99, 19.99, 4.99, 14.99]
            },
            "meta": {"primaryKey": "order_id"}
        })).unwrap()
    }

    #[test]
    fn hash_join_basic() {
        let users = users_table();
        let orders = orders_table();
        let result = hash_join(&users, "id", &orders, "user_id").unwrap();
        // user_id=1 (Alice) has 2 orders, user_id=2 (Bob) has 1 order, user_id=4 has no match
        assert_eq!(result.row_count, 3);
    }

    #[test]
    fn hash_join_prefixes_columns() {
        let users = users_table();
        let orders = orders_table();
        let result = hash_join(&users, "id", &orders, "user_id").unwrap();
        let col_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"left_id"));
        assert!(col_names.contains(&"left_name"));
        assert!(col_names.contains(&"right_order_id"));
        assert!(col_names.contains(&"right_user_id"));
        assert!(col_names.contains(&"right_amount"));
    }

    #[test]
    fn hash_join_invalid_column() {
        let users = users_table();
        let orders = orders_table();
        assert!(hash_join(&users, "nonexistent", &orders, "user_id").is_err());
    }

    #[test]
    fn hash_join_no_matches() {
        let users = users_table();
        // Orders with user_ids that don't exist in users
        let orders = validation::validate_and_build(json!({
            "version": "1.0",
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "amount", "type": "float"}
            ],
            "rowCount": 2,
            "data": {"user_id": [99, 100], "amount": [1.0, 2.0]}
        })).unwrap();
        let result = hash_join(&users, "id", &orders, "user_id").unwrap();
        assert_eq!(result.row_count, 0);
    }
}
