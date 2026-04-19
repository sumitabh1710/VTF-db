use std::collections::HashSet;

use serde_json::Value;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

/// Count non-null values. If `indices` is provided, only count those rows.
pub fn count(col: &ColumnData, indices: Option<&[usize]>) -> usize {
    match indices {
        Some(idx) => idx.iter().filter(|&&i| !is_null(col, i)).count(),
        None => (0..col.len()).filter(|&i| !is_null(col, i)).count(),
    }
}

/// Sum of numeric (int/float) column values.
pub fn sum(col: &ColumnData, indices: Option<&[usize]>) -> VtfResult<f64> {
    let iter: Box<dyn Iterator<Item = usize>> = match indices {
        Some(idx) => Box::new(idx.iter().copied()),
        None => Box::new(0..col.len()),
    };
    match col {
        ColumnData::Int(v) => Ok(iter.filter_map(|i| v.get(i).and_then(|x| *x)).map(|n| n as f64).sum()),
        ColumnData::Float(v) => Ok(iter.filter_map(|i| v.get(i).and_then(|x| *x)).sum()),
        _ => Err(VtfError::query("sum() requires int or float column")),
    }
}

/// Average of numeric (int/float) column values.
pub fn avg(col: &ColumnData, indices: Option<&[usize]>) -> VtfResult<f64> {
    let c = count(col, indices);
    if c == 0 {
        return Ok(0.0);
    }
    let s = sum(col, indices)?;
    Ok(s / c as f64)
}

/// Minimum value. Returns JSON representation.
pub fn min_val(col: &ColumnData, indices: Option<&[usize]>) -> VtfResult<Value> {
    let iter: Box<dyn Iterator<Item = usize>> = match indices {
        Some(idx) => Box::new(idx.iter().copied()),
        None => Box::new(0..col.len()),
    };

    match col {
        ColumnData::Int(v) => {
            let result = iter.filter_map(|i| v.get(i).and_then(|x| *x)).min();
            Ok(result.map_or(Value::Null, |n| Value::from(n)))
        }
        ColumnData::Float(v) => {
            let result = iter
                .filter_map(|i| v.get(i).and_then(|x| *x))
                .fold(None, |acc: Option<f64>, x| {
                    Some(match acc {
                        Some(a) if a < x => a,
                        _ => x,
                    })
                });
            Ok(result.map_or(Value::Null, |n| serde_json::json!(n)))
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            let result = iter
                .filter_map(|i| v.get(i).and_then(|x| x.as_ref()))
                .min();
            Ok(result.map_or(Value::Null, |s| Value::from(s.as_str())))
        }
        _ => Err(VtfError::query("min() requires int, float, string, or date column")),
    }
}

/// Maximum value. Returns JSON representation.
pub fn max_val(col: &ColumnData, indices: Option<&[usize]>) -> VtfResult<Value> {
    let iter: Box<dyn Iterator<Item = usize>> = match indices {
        Some(idx) => Box::new(idx.iter().copied()),
        None => Box::new(0..col.len()),
    };

    match col {
        ColumnData::Int(v) => {
            let result = iter.filter_map(|i| v.get(i).and_then(|x| *x)).max();
            Ok(result.map_or(Value::Null, |n| Value::from(n)))
        }
        ColumnData::Float(v) => {
            let result = iter
                .filter_map(|i| v.get(i).and_then(|x| *x))
                .fold(None, |acc: Option<f64>, x| {
                    Some(match acc {
                        Some(a) if a > x => a,
                        _ => x,
                    })
                });
            Ok(result.map_or(Value::Null, |n| serde_json::json!(n)))
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            let result = iter
                .filter_map(|i| v.get(i).and_then(|x| x.as_ref()))
                .max();
            Ok(result.map_or(Value::Null, |s| Value::from(s.as_str())))
        }
        _ => Err(VtfError::query("max() requires int, float, string, or date column")),
    }
}

/// Compute statistics for a single column.
/// Returns a valid `ColumnStats` reflecting the current state of `col`.
pub fn compute_stats(col: &ColumnData) -> VtfResult<ColumnStats> {
    let total = col.len();
    let null_count = (0..total).filter(|&i| is_null(col, i)).count();
    let row_count = total;

    // Distinct count via key set (exact, acceptable for now)
    let distinct_count = match col {
        ColumnData::Int(v) => {
            let seen: HashSet<_> = v.iter().filter_map(|x| *x).collect();
            seen.len()
        }
        ColumnData::Float(v) => {
            // Use bit pattern for f64 distinctness (NaN treated as a single value)
            let seen: HashSet<u64> = v.iter().filter_map(|x| x.map(|f| f.to_bits())).collect();
            seen.len()
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            let seen: HashSet<_> = v.iter().filter_map(|x| x.as_deref()).collect();
            seen.len()
        }
        ColumnData::Bool(v) => {
            let seen: HashSet<_> = v.iter().filter_map(|x| *x).collect();
            seen.len()
        }
        _ => 0, // array columns have no meaningful distinct count
    };

    let min = min_val(col, None).ok();
    let max = max_val(col, None).ok();

    Ok(ColumnStats {
        row_count,
        null_count,
        distinct_count,
        min: min.filter(|v| !v.is_null()),
        max: max.filter(|v| !v.is_null()),
        valid: true,
    })
}

fn is_null(col: &ColumnData, idx: usize) -> bool {
    match col {
        ColumnData::Int(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::Float(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::Str(v) | ColumnData::Date(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::Bool(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::ArrayInt(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::ArrayFloat(v) => v.get(idx).map_or(true, |x| x.is_none()),
        ColumnData::ArrayStr(v) => v.get(idx).map_or(true, |x| x.is_none()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    fn test_table() -> VtfTable {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "score", "type": "float"}
            ],
            "rowCount": 5,
            "data": {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
                "score": [90.5, 85.0, null, 92.3, 78.1]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn count_all_non_null() {
        let table = test_table();
        assert_eq!(count(&table.data["id"], None), 5);
        assert_eq!(count(&table.data["score"], None), 4); // one null
    }

    #[test]
    fn count_with_indices() {
        let table = test_table();
        let idx = vec![0, 2, 4];
        assert_eq!(count(&table.data["score"], Some(&idx)), 2); // row 2 is null
    }

    #[test]
    fn sum_int() {
        let table = test_table();
        let s = sum(&table.data["id"], None).unwrap();
        assert!((s - 15.0).abs() < 1e-10);
    }

    #[test]
    fn sum_float_skips_nulls() {
        let table = test_table();
        let s = sum(&table.data["score"], None).unwrap();
        assert!((s - (90.5 + 85.0 + 92.3 + 78.1)).abs() < 1e-10);
    }

    #[test]
    fn sum_string_errors() {
        let table = test_table();
        assert!(sum(&table.data["name"], None).is_err());
    }

    #[test]
    fn avg_float() {
        let table = test_table();
        let a = avg(&table.data["score"], None).unwrap();
        let expected = (90.5 + 85.0 + 92.3 + 78.1) / 4.0;
        assert!((a - expected).abs() < 1e-10);
    }

    #[test]
    fn avg_with_filter() {
        let table = test_table();
        let idx = vec![0, 1]; // Alice=90.5, Bob=85.0
        let a = avg(&table.data["score"], Some(&idx)).unwrap();
        assert!((a - 87.75).abs() < 1e-10);
    }

    #[test]
    fn min_int() {
        let table = test_table();
        let v = min_val(&table.data["id"], None).unwrap();
        assert_eq!(v, json!(1));
    }

    #[test]
    fn max_int() {
        let table = test_table();
        let v = max_val(&table.data["id"], None).unwrap();
        assert_eq!(v, json!(5));
    }

    #[test]
    fn min_float_skips_null() {
        let table = test_table();
        let v = min_val(&table.data["score"], None).unwrap();
        assert_eq!(v, json!(78.1));
    }

    #[test]
    fn max_float_skips_null() {
        let table = test_table();
        let v = max_val(&table.data["score"], None).unwrap();
        assert_eq!(v, json!(92.3));
    }

    #[test]
    fn min_string() {
        let table = test_table();
        let v = min_val(&table.data["name"], None).unwrap();
        assert_eq!(v, json!("Alice"));
    }

    #[test]
    fn max_string() {
        let table = test_table();
        let v = max_val(&table.data["name"], None).unwrap();
        assert_eq!(v, json!("Eve"));
    }

    #[test]
    fn min_of_empty_is_null() {
        let j = json!({
            "version": "1.0",
            "columns": [{"name": "x", "type": "int"}],
            "rowCount": 0,
            "data": {"x": []}
        });
        let table = validation::validate_and_build(j).unwrap();
        let v = min_val(&table.data["x"], None).unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn avg_of_empty_is_zero() {
        let j = json!({
            "version": "1.0",
            "columns": [{"name": "x", "type": "int"}],
            "rowCount": 0,
            "data": {"x": []}
        });
        let table = validation::validate_and_build(j).unwrap();
        let a = avg(&table.data["x"], None).unwrap();
        assert_eq!(a, 0.0);
    }
}
