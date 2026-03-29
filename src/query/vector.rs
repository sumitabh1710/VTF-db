use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Metric {
    Cosine,
    Euclidean,
}

pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

pub fn euclidean_distance(a: &[f64], b: &[f64]) -> f64 {
    let mut sum = 0.0;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

/// Brute-force top-K search over an `array<float>` column.
/// Returns `(row_index, score)` pairs sorted best-first:
///   - Cosine: highest similarity first
///   - Euclidean: lowest distance first
/// Null rows and dimension-mismatched rows are skipped.
pub fn top_k(
    table: &VtfTable,
    column: &str,
    query_vec: &[f64],
    k: usize,
    metric: Metric,
) -> VtfResult<Vec<(usize, f64)>> {
    let col_data = table.data.get(column).ok_or_else(|| {
        VtfError::query(format!("column '{column}' not found"))
    })?;

    let vectors = match col_data {
        ColumnData::ArrayFloat(v) => v,
        _ => {
            return Err(VtfError::query(format!(
                "column '{column}' is not array<float>"
            )));
        }
    };

    if query_vec.is_empty() {
        return Err(VtfError::query("query vector must not be empty"));
    }

    let mut scored: Vec<(usize, f64)> = Vec::new();
    for (i, row_vec) in vectors.iter().enumerate() {
        let Some(arr) = row_vec else { continue };
        let flat: Vec<f64> = arr.iter().map(|v| v.unwrap_or(0.0)).collect();
        if flat.len() != query_vec.len() {
            continue;
        }
        let score = match metric {
            Metric::Cosine => cosine_similarity(&flat, query_vec),
            Metric::Euclidean => euclidean_distance(&flat, query_vec),
        };
        scored.push((i, score));
    }

    match metric {
        Metric::Cosine => scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)),
        Metric::Euclidean => scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)),
    }

    scored.truncate(k);
    Ok(scored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    #[test]
    fn cosine_identical_vectors() {
        let a = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &a);
        assert!((sim - 1.0).abs() < 1e-10);
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-10);
    }

    #[test]
    fn cosine_opposite_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - (-1.0)).abs() < 1e-10);
    }

    #[test]
    fn euclidean_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let d = euclidean_distance(&a, &a);
        assert!(d.abs() < 1e-10);
    }

    #[test]
    fn euclidean_known_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let d = euclidean_distance(&a, &b);
        assert!((d - 5.0).abs() < 1e-10);
    }

    #[test]
    fn cosine_zero_vector() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert_eq!(sim, 0.0);
    }

    fn vector_table() -> VtfTable {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "embedding", "type": "array<float>"}
            ],
            "rowCount": 4,
            "data": {
                "id": [1, 2, 3, 4],
                "embedding": [
                    [1.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                    [0.707, 0.707, 0.0],
                    null
                ]
            },
            "meta": {"primaryKey": "id"}
        });
        validation::validate_and_build(j).unwrap()
    }

    #[test]
    fn top_k_cosine_returns_best_match() {
        let table = vector_table();
        let query = vec![1.0, 0.0, 0.0];
        let results = top_k(&table, "embedding", &query, 2, Metric::Cosine).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // exact match
        assert!((results[0].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn top_k_euclidean_returns_nearest() {
        let table = vector_table();
        let query = vec![1.0, 0.0, 0.0];
        let results = top_k(&table, "embedding", &query, 1, Metric::Euclidean).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        assert!(results[0].1.abs() < 1e-10);
    }

    #[test]
    fn top_k_skips_nulls() {
        let table = vector_table();
        let query = vec![1.0, 0.0, 0.0];
        let results = top_k(&table, "embedding", &query, 10, Metric::Cosine).unwrap();
        assert_eq!(results.len(), 3); // row 4 is null, skipped
    }

    #[test]
    fn top_k_wrong_column_type_errors() {
        let table = vector_table();
        let query = vec![1.0];
        assert!(top_k(&table, "id", &query, 5, Metric::Cosine).is_err());
    }

    #[test]
    fn top_k_nonexistent_column_errors() {
        let table = vector_table();
        let query = vec![1.0];
        assert!(top_k(&table, "nope", &query, 5, Metric::Cosine).is_err());
    }

    #[test]
    fn top_k_empty_query_vector_errors() {
        let table = vector_table();
        assert!(top_k(&table, "embedding", &[], 5, Metric::Cosine).is_err());
    }

    #[test]
    fn top_k_dimension_mismatch_skipped() {
        // All embeddings are 3D, query is 2D — should skip all rows
        let table = vector_table();
        let query = vec![1.0, 0.0];
        let results = top_k(&table, "embedding", &query, 5, Metric::Cosine).unwrap();
        assert!(results.is_empty());
    }
}
