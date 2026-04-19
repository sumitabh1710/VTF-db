use serde::{Deserialize, Serialize};

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

// ---------------------------------------------------------------------------
// Point wrapper — implements the instant-distance `Point` trait.
// Uses f32 as required by instant-distance, converting from VTF's f64.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VtfPoint {
    pub values: Vec<f32>,
}

impl instant_distance::Point for VtfPoint {
    fn distance(&self, other: &Self) -> f32 {
        cosine_distance(&self.values, &other.values)
    }
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len().min(b.len());
    let dot: f32 = a[..len].iter().zip(&b[..len]).map(|(x, y)| x * y).sum();
    let na: f32 = a[..len].iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b[..len].iter().map(|y| y * y).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        1.0
    } else {
        1.0 - dot / (na * nb)
    }
}

// ---------------------------------------------------------------------------
// HnswGraph — persisted representation of an HNSW index for one column.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub struct HnswGraph {
    /// HNSW graph; values are the original row indices in the VTF table.
    pub graph: instant_distance::HnswMap<VtfPoint, usize>,
    /// Column name this index was built for.
    pub column: String,
}

impl std::fmt::Debug for HnswGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HnswGraph {{ column: {:?} }}", self.column)
    }
}

impl Clone for HnswGraph {
    fn clone(&self) -> Self {
        // Round-trip through serde — acceptable for an index that is rarely cloned.
        let bytes = serde_json::to_vec(&self.graph).expect("HnswGraph clone: serialize failed");
        let graph = serde_json::from_slice(&bytes).expect("HnswGraph clone: deserialize failed");
        HnswGraph { graph, column: self.column.clone() }
    }
}

impl HnswGraph {
    /// Build the index from an `array<float>` column.
    /// VTF stores float arrays as `Vec<Option<Vec<Option<f64>>>>`.
    pub fn build(column: &str, data: &ColumnData) -> VtfResult<Self> {
        let vecs: &Vec<Option<Vec<Option<f64>>>> = match data {
            ColumnData::ArrayFloat(v) => v,
            _ => {
                return Err(VtfError::validation(format!(
                    "column '{column}' is not array<float>, cannot build HNSW index"
                )))
            }
        };

        let mut points: Vec<VtfPoint> = Vec::new();
        let mut values: Vec<usize> = Vec::new();

        for (i, entry) in vecs.iter().enumerate() {
            if let Some(vec) = entry {
                let floats: Vec<f32> = vec.iter().map(|x| x.unwrap_or(0.0) as f32).collect();
                points.push(VtfPoint { values: floats });
                values.push(i);
            }
        }

        if points.is_empty() {
            return Err(VtfError::validation(format!(
                "column '{column}' has no non-null vectors to index"
            )));
        }

        let graph = instant_distance::Builder::default().build(points, values);
        Ok(HnswGraph { graph, column: column.to_string() })
    }

    /// Search for the top-k nearest row indices to the query vector (f32 input).
    /// Returns `(row_idx, distance)` pairs sorted by ascending distance.
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(usize, f32)> {
        let query_point = VtfPoint { values: query.to_vec() };
        let mut search = instant_distance::Search::default();
        let mut result: Vec<(usize, f32)> = self.graph
            .search(&query_point, &mut search)
            .take(top_k)
            .map(|item| (*item.value, item.distance))
            .collect();
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        result.truncate(top_k);
        result
    }

    /// Serialize the graph to a compact JSON blob (base64-encoded JSON bytes).
    pub fn to_json_blob(&self) -> VtfResult<String> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| VtfError::validation(format!("HNSW serialize error: {e}")))?;
        Ok(base64_encode(&bytes))
    }

    /// Deserialize from a JSON blob.
    pub fn from_json_blob(blob: &str) -> VtfResult<Self> {
        let bytes = base64_decode(blob)?;
        serde_json::from_slice(&bytes)
            .map_err(|e| VtfError::validation(format!("HNSW deserialize error: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Inline base64 helpers (no external dependency required)
// ---------------------------------------------------------------------------

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let v = (b0 << 16) | (b1 << 8) | b2;
        s.push(TABLE[((v >> 18) & 0x3F) as usize] as char);
        s.push(TABLE[((v >> 12) & 0x3F) as usize] as char);
        s.push(if chunk.len() > 1 { TABLE[((v >> 6) & 0x3F) as usize] as char } else { '=' });
        s.push(if chunk.len() > 2 { TABLE[(v & 0x3F) as usize] as char } else { '=' });
    }
    s
}

fn base64_decode(s: &str) -> VtfResult<Vec<u8>> {
    fn decode_char(c: u8) -> VtfResult<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(VtfError::validation(format!("invalid base64 char: {}", c as char))),
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i + 3 < bytes.len() {
        let b0 = decode_char(bytes[i])?;
        let b1 = decode_char(bytes[i + 1])?;
        out.push(((b0 << 2) | (b1 >> 4)) as u8);
        if bytes[i + 2] != b'=' {
            let b2 = decode_char(bytes[i + 2])?;
            out.push(((b1 << 4) | (b2 >> 2)) as u8);
        }
        if bytes[i + 3] != b'=' {
            let b2 = decode_char(bytes[i + 2])?;
            let b3 = decode_char(bytes[i + 3])?;
            out.push(((b2 << 6) | b3) as u8);
        }
        i += 4;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Top-level entry points used by the CLI and vector search
// ---------------------------------------------------------------------------

/// Build an HNSW index on an `array<float>` column and store it in the table.
pub fn build_hnsw_index(table: &mut VtfTable, column: &str) -> VtfResult<()> {
    let col_data = table
        .data
        .get(column)
        .ok_or_else(|| VtfError::validation(format!("column '{column}' not found")))?;
    let graph = HnswGraph::build(column, col_data)?;
    table.vector_indexes.insert(column.to_string(), graph);
    Ok(())
}

/// Search using HNSW if available, otherwise fall back to brute-force scan.
/// Returns `(row_idx, similarity)` pairs where similarity = 1 - cosine_distance.
pub fn search_with_hnsw_or_brute(
    table: &VtfTable,
    column: &str,
    query: &[f32],
    top_k: usize,
) -> VtfResult<Vec<(usize, f32)>> {
    if let Some(graph) = table.vector_indexes.get(column) {
        let results = graph.search(query, top_k)
            .into_iter()
            .map(|(row, dist)| (row, 1.0 - dist))
            .collect();
        return Ok(results);
    }
    // Brute-force fallback
    crate::query::vector::top_k_cosine_rows(table, column, query, top_k)
}
