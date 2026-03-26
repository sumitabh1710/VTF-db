use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

const DEFAULT_COMPACTION_THRESHOLD: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum WalEntry {
    #[serde(rename = "insert")]
    Insert { row: IndexMap<String, Value> },
    #[serde(rename = "insert_batch")]
    InsertBatch { rows: Vec<IndexMap<String, Value>> },
    #[serde(rename = "delete")]
    Delete { indices: Vec<usize> },
    #[serde(rename = "update")]
    Update {
        indices: Vec<usize>,
        values: IndexMap<String, Value>,
    },
    #[serde(rename = "add_column")]
    AddColumn {
        name: String,
        col_type: String,
    },
}

/// Get the WAL file path for a given VTF file.
pub fn wal_path(vtf_path: &Path) -> PathBuf {
    let mut p = vtf_path.to_path_buf();
    let name = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{name}.wal"));
    p
}

/// Append a WAL entry to the log file.
pub fn append(vtf_path: &Path, entry: &WalEntry) -> VtfResult<()> {
    let path = wal_path(vtf_path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let line = serde_json::to_string(entry)?;
    writeln!(file, "{}", line)?;
    file.sync_all()?;
    Ok(())
}

/// Read all WAL entries from the log file.
pub fn read_entries(vtf_path: &Path) -> VtfResult<Vec<WalEntry>> {
    let path = wal_path(vtf_path);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(&path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: WalEntry = serde_json::from_str(&line).map_err(|e| {
            VtfError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("corrupt WAL entry: {e}"),
            ))
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

/// Replay WAL entries onto a table to reconstruct current state.
pub fn replay(table: &mut VtfTable, entries: &[WalEntry]) -> VtfResult<()> {
    for entry in entries {
        match entry {
            WalEntry::Insert { row } => {
                table.insert(row.clone())?;
            }
            WalEntry::InsertBatch { rows } => {
                table.insert_batch(rows.clone())?;
            }
            WalEntry::Delete { indices } => {
                table.delete(indices)?;
            }
            WalEntry::Update { indices, values } => {
                table.update(indices, values.clone())?;
            }
            WalEntry::AddColumn { name, col_type } => {
                let ct = ColumnType::from_str(col_type)?;
                table.add_column(name, ct)?;
            }
        }
    }
    Ok(())
}

/// Count entries in the WAL file.
pub fn entry_count(vtf_path: &Path) -> VtfResult<usize> {
    Ok(read_entries(vtf_path)?.len())
}

/// Check if the WAL should be compacted (exceeds threshold).
pub fn needs_compaction(vtf_path: &Path) -> VtfResult<bool> {
    Ok(entry_count(vtf_path)? >= DEFAULT_COMPACTION_THRESHOLD)
}

/// Delete the WAL file.
pub fn clear(vtf_path: &Path) -> VtfResult<()> {
    let path = wal_path(vtf_path);
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wal_path_construction() {
        let p = wal_path(Path::new("/tmp/test.vtf"));
        assert_eq!(p, PathBuf::from("/tmp/test.vtf.wal"));
    }

    #[test]
    fn roundtrip_entries() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("test.vtf");

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));

        append(&vtf, &WalEntry::Insert { row: row.clone() }).unwrap();
        append(&vtf, &WalEntry::Delete { indices: vec![0] }).unwrap();

        let entries = read_entries(&vtf).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn replay_insert_and_delete() {
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
            Column { name: "name".to_string(), col_type: ColumnType::String },
        ];
        let mut table = VtfTable::new(columns);
        table.meta.primary_key = Some("id".to_string());

        let entries = vec![
            WalEntry::Insert {
                row: {
                    let mut r = IndexMap::new();
                    r.insert("id".to_string(), json!(1));
                    r.insert("name".to_string(), json!("Alice"));
                    r
                },
            },
            WalEntry::Insert {
                row: {
                    let mut r = IndexMap::new();
                    r.insert("id".to_string(), json!(2));
                    r.insert("name".to_string(), json!("Bob"));
                    r
                },
            },
            WalEntry::Delete { indices: vec![0] },
        ];

        replay(&mut table, &entries).unwrap();
        assert_eq!(table.row_count, 1);
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Bob"));
    }

    #[test]
    fn replay_update() {
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
            Column { name: "name".to_string(), col_type: ColumnType::String },
        ];
        let mut table = VtfTable::new(columns);

        let entries = vec![
            WalEntry::Insert {
                row: {
                    let mut r = IndexMap::new();
                    r.insert("id".to_string(), json!(1));
                    r.insert("name".to_string(), json!("Alice"));
                    r
                },
            },
            WalEntry::Update {
                indices: vec![0],
                values: {
                    let mut v = IndexMap::new();
                    v.insert("name".to_string(), json!("Alicia"));
                    v
                },
            },
        ];

        replay(&mut table, &entries).unwrap();
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alicia"));
    }

    #[test]
    fn replay_batch_insert() {
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
            Column { name: "name".to_string(), col_type: ColumnType::String },
        ];
        let mut table = VtfTable::new(columns);
        table.meta.primary_key = Some("id".to_string());

        let entries = vec![
            WalEntry::InsertBatch {
                rows: vec![
                    {
                        let mut r = IndexMap::new();
                        r.insert("id".to_string(), json!(1));
                        r.insert("name".to_string(), json!("A"));
                        r
                    },
                    {
                        let mut r = IndexMap::new();
                        r.insert("id".to_string(), json!(2));
                        r.insert("name".to_string(), json!("B"));
                        r
                    },
                ],
            },
        ];

        replay(&mut table, &entries).unwrap();
        assert_eq!(table.row_count, 2);
    }

    #[test]
    fn replay_add_column() {
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
        ];
        let mut table = VtfTable::new(columns);

        let entries = vec![
            WalEntry::Insert {
                row: {
                    let mut r = IndexMap::new();
                    r.insert("id".to_string(), json!(1));
                    r
                },
            },
            WalEntry::AddColumn {
                name: "name".to_string(),
                col_type: "string".to_string(),
            },
        ];

        replay(&mut table, &entries).unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.data["name"].len(), 1);
    }

    #[test]
    fn empty_wal_returns_empty_entries() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("nonexistent.vtf");
        let entries = read_entries(&vtf).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn entry_count_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("test.vtf");

        for i in 0..5 {
            let mut row = IndexMap::new();
            row.insert("id".to_string(), json!(i));
            append(&vtf, &WalEntry::Insert { row }).unwrap();
        }

        assert_eq!(entry_count(&vtf).unwrap(), 5);
        clear(&vtf).unwrap();
        assert_eq!(entry_count(&vtf).unwrap(), 0);
    }
}
