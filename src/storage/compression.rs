use crate::core::error::{VtfError, VtfResult};
use crate::core::model::VtfTable;
use crate::storage::binary;

/// Magic bytes for compressed VTF: "VTFz"
pub const COMPRESSED_MAGIC: &[u8; 4] = b"VTFz";

/// Check if data is in compressed VTF format.
pub fn is_compressed_format(data: &[u8]) -> bool {
    data.len() >= 4 && &data[..4] == COMPRESSED_MAGIC
}

/// Encode a VtfTable to compressed binary format.
/// Prepends the compressed magic, then zstd-compressed binary data.
pub fn encode_compressed(table: &VtfTable) -> VtfResult<Vec<u8>> {
    let binary_data = binary::encode(table)?;
    let compressed = zstd::encode_all(&binary_data[..], 3).map_err(|e| {
        VtfError::Storage(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("zstd compression failed: {e}"),
        ))
    })?;

    let mut result = Vec::with_capacity(4 + compressed.len());
    result.extend_from_slice(COMPRESSED_MAGIC);
    result.extend(compressed);
    Ok(result)
}

/// Decode compressed binary data into a VtfTable.
pub fn decode_compressed(data: &[u8]) -> VtfResult<VtfTable> {
    if data.len() < 4 || &data[..4] != COMPRESSED_MAGIC {
        return Err(VtfError::validation("invalid compressed magic bytes"));
    }

    let decompressed = zstd::decode_all(&data[4..]).map_err(|e| {
        VtfError::Storage(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("zstd decompression failed: {e}"),
        ))
    })?;

    binary::decode(&decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    #[test]
    fn compressed_magic_detection() {
        assert!(is_compressed_format(b"VTFz\x00"));
        assert!(!is_compressed_format(b"VTFb\x01"));
        assert!(!is_compressed_format(b"{\"v"));
    }

    #[test]
    fn roundtrip_compressed() {
        let j = json!({
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
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode_compressed(&table).unwrap();
        assert!(is_compressed_format(&bytes));

        let decoded = decode_compressed(&bytes).unwrap();
        assert_eq!(decoded.row_count, 3);
        assert_eq!(decoded.meta.primary_key, Some("id".to_string()));

        let rows = decoded.select_rows(&[0, 1, 2], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alice"));
        assert_eq!(rows[2]["name"], json!("Charlie"));
    }

    #[test]
    fn compressed_smaller_than_binary_for_repetitive_data() {
        let names: Vec<serde_json::Value> = (0..500)
            .map(|_| json!("same_repeated_value_here"))
            .collect();
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 500,
            "data": {
                "id": (0..500).collect::<Vec<_>>(),
                "name": names
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let binary_size = crate::storage::binary::encode(&table).unwrap().len();
        let compressed_size = encode_compressed(&table).unwrap().len();
        assert!(
            compressed_size < binary_size,
            "compressed ({compressed_size}) should be smaller than binary ({binary_size})"
        );
    }

    #[test]
    fn decode_rejects_bad_compressed_magic() {
        assert!(decode_compressed(b"BADz\x00").is_err());
    }

    #[test]
    fn roundtrip_with_nulls_compressed() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, null, 3],
                "name": [null, "Bob", null]
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode_compressed(&table).unwrap();
        let decoded = decode_compressed(&bytes).unwrap();
        assert_eq!(decoded.row_count, 3);
        let rows = decoded.select_rows(&[0, 1, 2], &[]).unwrap();
        assert!(rows[0]["name"].is_null());
        assert_eq!(rows[1]["name"], json!("Bob"));
    }
}
