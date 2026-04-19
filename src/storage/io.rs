use std::fs;
use std::io::Write;
use std::path::Path;

use fs2::FileExt;
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;
use crate::storage::validation;

/// Load a VTF file from disk with a shared (read) lock.
/// Automatically detects JSON vs binary (v1 or v2) format.
pub fn load(path: &Path) -> VtfResult<VtfTable> {
    let lock_file = fs::File::open(path)?;
    lock_file.lock_shared().map_err(|e| {
        VtfError::Storage(std::io::Error::new(e.kind(), format!("failed to acquire shared lock: {e}")))
    })?;
    let contents = fs::read(path)?;
    lock_file.unlock().ok();

    if crate::storage::binary::is_binary_format(&contents) {
        crate::storage::binary::decode(&contents)
    } else {
        let text = String::from_utf8(contents).map_err(|e| {
            VtfError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
        })?;
        let raw: Value = serde_json::from_str(&text)?;
        validation::validate_and_build(raw)
    }
}

/// Save a VtfTable to disk in binary v2 format with an exclusive (write) lock.
/// Binary format is more compact and preserves all metadata (indexes, constraints, LSN).
pub fn save(table: &VtfTable, path: &Path) -> VtfResult<()> {
    save_binary(table, path)
}

/// Save a VtfTable in JSON format atomically with an exclusive lock.
/// Use this when you explicitly need a human-readable file (e.g. `vtf export --format json`).
pub fn save_json(table: &VtfTable, path: &Path) -> VtfResult<()> {
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?;
    lock_file.lock_exclusive().map_err(|e| {
        VtfError::Storage(std::io::Error::new(e.kind(), format!("failed to acquire exclusive lock: {e}")))
    })?;
    let json = table.to_json()?;
    atomic_write(path, json.as_bytes())?;
    lock_file.unlock().ok();
    Ok(())
}

/// Save a VtfTable in binary format atomically with an exclusive lock.
pub fn save_binary(table: &VtfTable, path: &Path) -> VtfResult<()> {
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?;
    lock_file.lock_exclusive().map_err(|e| {
        VtfError::Storage(std::io::Error::new(e.kind(), format!("failed to acquire exclusive lock: {e}")))
    })?;
    let bytes = crate::storage::binary::encode(table)?;
    atomic_write(path, &bytes)?;
    lock_file.unlock().ok();
    Ok(())
}

/// Load a VTF file, auto-detecting JSON vs binary format.
pub fn load_auto(path: &Path) -> VtfResult<VtfTable> {
    let lock_file = fs::File::open(path)?;
    lock_file.lock_shared().map_err(|e| {
        VtfError::Storage(std::io::Error::new(e.kind(), format!("failed to acquire shared lock: {e}")))
    })?;
    let contents = fs::read(path)?;
    lock_file.unlock().ok();

    if crate::storage::binary::is_binary_format(&contents) {
        crate::storage::binary::decode(&contents)
    } else {
        let text = String::from_utf8(contents).map_err(|e| {
            VtfError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
        })?;
        let raw: Value = serde_json::from_str(&text)?;
        validation::validate_and_build(raw)
    }
}

/// Atomic write available for external callers (e.g. compressed export).
pub fn atomic_write_public(path: &Path, data: &[u8]) -> VtfResult<()> {
    atomic_write(path, data)
}

fn atomic_write(path: &Path, data: &[u8]) -> VtfResult<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(data)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| {
        VtfError::Storage(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("failed to persist temp file: {e}"),
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_save_load_file() {
        let table = sample_table();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        save(&table, &path).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.row_count, 2);
        assert_eq!(loaded.meta.primary_key, Some("id".to_string()));
    }

    #[test]
    fn test_concurrent_reads_dont_block() {
        let table = sample_table();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.vtf");
        save(&table, &path).unwrap();

        let t1 = load(&path).unwrap();
        let t2 = load(&path).unwrap();
        assert_eq!(t1.row_count, t2.row_count);
    }
}
