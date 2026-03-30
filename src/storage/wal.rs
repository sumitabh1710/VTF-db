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
    #[serde(rename = "create_index")]
    CreateIndex {
        column: String,
        index_type: String,
    },
}

/// Get the WAL file path for a given VTF file.
pub fn wal_path(vtf_path: &Path) -> PathBuf {
    let mut p = vtf_path.to_path_buf();
    let name = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{name}.wal"));
    p
}

/// Append a WAL entry to the log file with a CRC32 checksum.
/// Format: `<entry_json>\t<crc32_hex>`
pub fn append(vtf_path: &Path, entry: &WalEntry) -> VtfResult<()> {
    let path = wal_path(vtf_path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let json = serde_json::to_string(entry)?;
    let checksum = crc32fast::hash(json.as_bytes());
    writeln!(file, "{}\t{:08x}", json, checksum)?;
    file.sync_all()?;
    Ok(())
}

/// Read all WAL entries from the log file, validating CRC32 checksums.
/// Corrupt or unparseable entries are skipped with a warning to stderr.
pub fn read_entries(vtf_path: &Path) -> VtfResult<Vec<WalEntry>> {
    let path = wal_path(vtf_path);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(&path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        if let Some((json_part, checksum_hex)) = line.rsplit_once('\t') {
            if let Ok(expected) = u32::from_str_radix(checksum_hex, 16) {
                let actual = crc32fast::hash(json_part.as_bytes());
                if actual != expected {
                    eprintln!(
                        "[WAL] Warning: corrupt entry at line {} (checksum mismatch), skipping",
                        line_num + 1
                    );
                    continue;
                }
                match serde_json::from_str::<WalEntry>(json_part) {
                    Ok(entry) => entries.push(entry),
                    Err(e) => {
                        eprintln!(
                            "[WAL] Warning: corrupt entry at line {} ({e}), skipping",
                            line_num + 1
                        );
                    }
                }
            } else {
                // Tab found but checksum not valid hex — try whole line as legacy
                match serde_json::from_str::<WalEntry>(&line) {
                    Ok(entry) => entries.push(entry),
                    Err(e) => {
                        eprintln!(
                            "[WAL] Warning: corrupt entry at line {} ({e}), skipping",
                            line_num + 1
                        );
                    }
                }
            }
        } else {
            // No tab — legacy format without checksum
            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    eprintln!(
                        "[WAL] Warning: corrupt entry at line {} ({e}), skipping",
                        line_num + 1
                    );
                }
            }
        }
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
            WalEntry::CreateIndex { column, index_type } => {
                let idx_type = match index_type.as_str() {
                    "hash" => IndexType::Hash,
                    "sorted" => IndexType::Sorted,
                    other => {
                        return Err(VtfError::validation(format!(
                            "unknown index type in WAL: '{other}'"
                        )));
                    }
                };
                table.create_index(column, idx_type)?;
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
    fn corrupt_entry_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("test.vtf");

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        append(&vtf, &WalEntry::Insert { row }).unwrap();

        // Append a corrupt line (bad checksum)
        let wal = wal_path(&vtf);
        let mut f = OpenOptions::new().append(true).open(&wal).unwrap();
        writeln!(f, "{{\"op\":\"insert\",\"row\":{{\"id\":2}}}}\tdeadbeef").unwrap();

        // Append another valid entry
        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), json!(3));
        append(&vtf, &WalEntry::Insert { row: row2 }).unwrap();

        let entries = read_entries(&vtf).unwrap();
        assert_eq!(entries.len(), 2); // corrupt middle entry skipped
    }

    #[test]
    fn totally_garbage_line_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("test.vtf");

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        append(&vtf, &WalEntry::Insert { row }).unwrap();

        let wal = wal_path(&vtf);
        let mut f = OpenOptions::new().append(true).open(&wal).unwrap();
        writeln!(f, "this is total garbage").unwrap();

        let entries = read_entries(&vtf).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn legacy_format_without_checksum_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let vtf = dir.path().join("test.vtf");
        let wal = wal_path(&vtf);

        // Write legacy format (no tab/checksum)
        let mut f = OpenOptions::new().create(true).write(true).open(&wal).unwrap();
        writeln!(f, "{{\"op\":\"insert\",\"row\":{{\"id\":1}}}}").unwrap();

        let entries = read_entries(&vtf).unwrap();
        assert_eq!(entries.len(), 1);
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
    fn replay_create_index() {
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
            WalEntry::CreateIndex {
                column: "name".to_string(),
                index_type: "hash".to_string(),
            },
        ];

        replay(&mut table, &entries).unwrap();
        assert!(table.indexes.contains_key("name"));
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
