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
    /// Predicate-based delete: stores the original filter expression for
    /// human readability, plus the primary key values resolved at write time.
    /// Replay uses pk_values to re-locate rows by their stable identity rather
    /// than by physical row index (which shifts after other deletes).
    #[serde(rename = "delete")]
    Delete {
        filter: String,
        pk_values: Vec<Value>,
    },
    /// Legacy index-based delete written by older VTF versions. Kept for
    /// backward compatibility during replay; new writes always use Delete.
    #[serde(rename = "delete_legacy")]
    DeleteLegacy { indices: Vec<usize> },
    /// Predicate-based update: filter + pk_values identify the rows,
    /// values holds the columns to change.
    #[serde(rename = "update")]
    Update {
        filter: String,
        pk_values: Vec<Value>,
        values: IndexMap<String, Value>,
    },
    /// Legacy index-based update written by older VTF versions.
    #[serde(rename = "update_legacy")]
    UpdateLegacy {
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
    /// Marks the beginning of a multi-operation transaction.
    #[serde(rename = "txn_begin")]
    TxnBegin { txn_id: String },
    /// Marks a successful commit of a transaction. Only entries between a
    /// matching TxnBegin and TxnCommit are replayed.
    #[serde(rename = "txn_commit")]
    TxnCommit { txn_id: String },
    /// Marks an explicit rollback. The transaction's buffered ops are discarded.
    #[serde(rename = "txn_rollback")]
    TxnRollback { txn_id: String },
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
    append_many(vtf_path, std::slice::from_ref(entry))
}

/// Append multiple WAL entries atomically in a single write + fsync.
/// Used by transaction commit to write TxnBegin + ops + TxnCommit together.
pub fn append_many(vtf_path: &Path, entries: &[WalEntry]) -> VtfResult<()> {
    let path = wal_path(vtf_path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    for entry in entries {
        let json = serde_json::to_string(entry)?;
        let checksum = crc32fast::hash(json.as_bytes());
        writeln!(file, "{}\t{:08x}", json, checksum)?;
    }
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
///
/// Transaction semantics: entries wrapped in TxnBegin/TxnCommit are applied
/// as a group only if the matching TxnCommit is present. Entries without any
/// transaction markers (auto-committed single operations) are always applied.
pub fn replay(table: &mut VtfTable, entries: &[WalEntry]) -> VtfResult<()> {
    // Identify all committed txn_ids so we know which groups to apply.
    let committed_txns: std::collections::HashSet<String> = entries
        .iter()
        .filter_map(|e| match e {
            WalEntry::TxnCommit { txn_id } => Some(txn_id.clone()),
            _ => None,
        })
        .collect();

    let mut active_txn: Option<String> = None;

    for entry in entries {
        match entry {
            WalEntry::TxnBegin { txn_id } => {
                active_txn = Some(txn_id.clone());
            }
            WalEntry::TxnCommit { .. } | WalEntry::TxnRollback { .. } => {
                active_txn = None;
            }
            _ => {
                // Skip entries belonging to an uncommitted or rolled-back transaction.
                if let Some(ref txn_id) = active_txn {
                    if !committed_txns.contains(txn_id) {
                        continue;
                    }
                }
                apply_entry(table, entry)?;
            }
        }
    }
    Ok(())
}

/// Public alias so that `Transaction::commit` can apply individual ops
/// to the in-memory table using the same logic as WAL replay.
pub fn apply_entry_public(table: &mut VtfTable, entry: &WalEntry) -> VtfResult<()> {
    apply_entry(table, entry)
}

fn apply_entry(table: &mut VtfTable, entry: &WalEntry) -> VtfResult<()> {
    match entry {
        WalEntry::Insert { row } => {
            table.insert(row.clone())?;
            table.lsn += 1;
        }
        WalEntry::InsertBatch { rows } => {
            table.insert_batch(rows.clone())?;
            table.lsn += 1;
        }
        WalEntry::Delete { pk_values, .. } => {
            let indices = resolve_pk_indices(table, pk_values);
            if !indices.is_empty() {
                table.delete(&indices)?;
                table.lsn += 1;
            }
        }
        WalEntry::DeleteLegacy { indices } => {
            eprintln!("[WAL] Warning: replaying legacy index-based delete — upgrade recommended");
            if !indices.is_empty() && indices.iter().all(|&i| i < table.row_count) {
                table.delete(indices)?;
                table.lsn += 1;
            } else {
                eprintln!("[WAL] Warning: legacy delete indices out of range, skipping");
            }
        }
        WalEntry::Update { pk_values, values, .. } => {
            let indices = resolve_pk_indices(table, pk_values);
            if !indices.is_empty() {
                table.update(&indices, values.clone())?;
                table.lsn += 1;
            }
        }
        WalEntry::UpdateLegacy { indices, values } => {
            eprintln!("[WAL] Warning: replaying legacy index-based update — upgrade recommended");
            let valid: Vec<usize> = indices.iter().copied().filter(|&i| i < table.row_count).collect();
            if !valid.is_empty() {
                table.update(&valid, values.clone())?;
                table.lsn += 1;
            } else {
                eprintln!("[WAL] Warning: legacy update indices out of range, skipping");
            }
        }
        WalEntry::AddColumn { name, col_type } => {
            let ct = ColumnType::from_str(col_type)?;
            table.add_column(name, ct)?;
            table.lsn += 1;
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
            table.lsn += 1;
        }
        // Marker variants handled in replay() above, never reach apply_entry.
        WalEntry::TxnBegin { .. } | WalEntry::TxnCommit { .. } | WalEntry::TxnRollback { .. } => {}
    }
    Ok(())
}

/// Resolve a list of primary key values to their current row indices.
/// Returns only the indices for PKs that still exist (deleted PKs are skipped).
fn resolve_pk_indices(table: &VtfTable, pk_values: &[Value]) -> Vec<usize> {
    let Some(ref pk_col) = table.meta.primary_key else {
        return Vec::new();
    };
    let col_data = match table.data.get(pk_col) {
        Some(d) => d,
        None => return Vec::new(),
    };

    pk_values.iter().filter_map(|pk_val| {
        for i in 0..table.row_count {
            if let Some(existing) = col_data.get_json_value(i) {
                if values_equal_json(&existing, pk_val) {
                    return Some(i);
                }
            }
        }
        None
    }).collect()
}

fn values_equal_json(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            if let (Some(ai), Some(bi)) = (an.as_i64(), bn.as_i64()) {
                ai == bi
            } else {
                an.as_f64() == bn.as_f64()
            }
        }
        _ => a == b,
    }
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
        append(&vtf, &WalEntry::Delete { filter: "id = 1".to_string(), pk_values: vec![json!(1)] }).unwrap();

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
            WalEntry::Delete { filter: "id = 1".to_string(), pk_values: vec![json!(1)] },
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
            WalEntry::Update {
                filter: "id = 1".to_string(),
                pk_values: vec![json!(1)],
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
