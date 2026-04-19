use std::collections::HashMap;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

/// Compare two index keys in a type-aware manner so that numeric columns sort
/// by value rather than lexicographically.
///
/// For `Int` and `Float` columns the keys are parsed back to numbers before
/// comparison.  All other column types fall back to lexicographic order, which
/// is correct for strings, booleans, and dates (ISO-8601 sorts lexicographically).
pub fn key_cmp(a: &str, b: &str, col_type: &ColumnType) -> std::cmp::Ordering {
    match col_type {
        ColumnType::Int => {
            let ai: i64 = a.parse().unwrap_or(i64::MIN);
            let bi: i64 = b.parse().unwrap_or(i64::MIN);
            ai.cmp(&bi)
        }
        ColumnType::Float => {
            let af: f64 = a.parse().unwrap_or(f64::NEG_INFINITY);
            let bf: f64 = b.parse().unwrap_or(f64::NEG_INFINITY);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => a.cmp(b),
    }
}

/// Check whether `key` satisfies a single bound.
fn satisfies_low(key: &str, lo: &str, inclusive: bool, col_type: &ColumnType) -> bool {
    let ord = key_cmp(key, lo, col_type);
    if inclusive { ord != std::cmp::Ordering::Less } else { ord == std::cmp::Ordering::Greater }
}

fn satisfies_high(key: &str, hi: &str, inclusive: bool, col_type: &ColumnType) -> bool {
    let ord = key_cmp(key, hi, col_type);
    if inclusive { ord != std::cmp::Ordering::Greater } else { ord == std::cmp::Ordering::Less }
}

pub fn build_sorted_index(column: &str, data: &ColumnData) -> VtfResult<IndexDef> {
    if data.col_type().is_array() {
        return Err(VtfError::validation(format!(
            "array column '{column}' cannot be indexed"
        )));
    }

    let col_type = data.col_type();

    // Collect key → row-ids
    let mut raw: HashMap<String, Vec<usize>> = HashMap::new();
    for i in 0..data.len() {
        if let Some(key) = data.value_as_key(i) {
            raw.entry(key).or_insert_with(Vec::new).push(i);
        }
    }

    // Sort keys in type-aware order
    let mut sorted_keys: Vec<String> = raw.keys().cloned().collect();
    sorted_keys.sort_by(|a, b| key_cmp(a, b, &col_type));

    Ok(IndexDef {
        column: column.to_string(),
        index_type: IndexType::Sorted,
        column_type: col_type,
        map: raw,
        sorted_keys: Some(sorted_keys),
    })
}

/// Range query on a sorted index. Returns row indices where the key satisfies the bound.
/// `low` is the lower bound, `high` is the upper bound. Either can be None.
/// Comparisons are type-aware: numeric columns compare by value.
pub fn range_query(
    idx: &IndexDef,
    low: Option<&str>,
    high: Option<&str>,
    low_inclusive: bool,
    high_inclusive: bool,
) -> Vec<usize> {
    let sorted_keys = match &idx.sorted_keys {
        Some(k) => k,
        None => return Vec::new(),
    };

    let col_type = &idx.column_type;
    let mut result = Vec::new();

    for key in sorted_keys {
        let above_low = match low {
            None => true,
            Some(lo) => satisfies_low(key, lo, low_inclusive, col_type),
        };
        let below_high = match high {
            None => true,
            Some(hi) => satisfies_high(key, hi, high_inclusive, col_type),
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

    #[test]
    fn sorted_index_numeric_order() {
        // Ensure integers sort by value, not lexicographically.
        // "10" < "9" lexicographically but 10 > 9 numerically.
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [{"name": "n", "type": "int"}],
            "rowCount": 5,
            "data": {"n": [9, 10, 2, 11, 1]}
        });
        let table = validation::validate_and_build(j).unwrap();
        let idx = build_sorted_index("n", &table.data["n"]).unwrap();
        let keys = idx.sorted_keys.as_ref().unwrap();
        // Must be numerically sorted: 1, 2, 9, 10, 11
        assert_eq!(keys, &["1", "2", "9", "10", "11"]);
    }

    #[test]
    fn range_query_numeric_correctness() {
        // age > 8: must include 9, 10, 11 — previously "10" and "11" were missed.
        let j = serde_json::json!({
            "version": "1.0",
            "columns": [{"name": "age", "type": "int"}],
            "rowCount": 5,
            "data": {"age": [9, 10, 2, 11, 1]}
        });
        let table = validation::validate_and_build(j).unwrap();
        let idx = build_sorted_index("age", &table.data["age"]).unwrap();
        let rows = range_query(&idx, Some("8"), None, false, true);
        // row indices for values 9, 10, 11 are 0, 1, 3
        assert_eq!(rows, vec![0, 1, 3]);
    }
}
