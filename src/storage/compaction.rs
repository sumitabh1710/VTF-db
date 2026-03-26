use std::path::Path;

use crate::core::error::VtfResult;
use crate::core::model::VtfTable;
use crate::storage::{wal, io};

/// Compact the WAL by replaying all entries onto the base table,
/// writing a new base file, and deleting the WAL.
pub fn compact(vtf_path: &Path) -> VtfResult<VtfTable> {
    let mut table = io::load(vtf_path)?;
    let entries = wal::read_entries(vtf_path)?;

    if entries.is_empty() {
        return Ok(table);
    }

    wal::replay(&mut table, &entries)?;
    io::save(&table, vtf_path)?;
    wal::clear(vtf_path)?;

    Ok(table)
}

/// Load a table with WAL replay. If the WAL has entries, replay them
/// onto the base table. Optionally trigger compaction if threshold exceeded.
pub fn load_with_wal(vtf_path: &Path) -> VtfResult<VtfTable> {
    let mut table = io::load(vtf_path)?;
    let entries = wal::read_entries(vtf_path)?;

    if !entries.is_empty() {
        wal::replay(&mut table, &entries)?;
    }

    Ok(table)
}

/// Save a mutation to the WAL. If the WAL exceeds the compaction
/// threshold, automatically compact.
pub fn save_with_wal(
    vtf_path: &Path,
    entry: &wal::WalEntry,
) -> VtfResult<()> {
    wal::append(vtf_path, entry)?;

    if wal::needs_compaction(vtf_path)? {
        compact(vtf_path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::*;
    use crate::storage::wal::WalEntry;
    use indexmap::IndexMap;
    use serde_json::json;

    fn create_test_file(dir: &Path) -> std::path::PathBuf {
        let vtf_path = dir.join("test.vtf");
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
            Column { name: "name".to_string(), col_type: ColumnType::String },
        ];
        let mut table = VtfTable::new(columns);
        table.meta.primary_key = Some("id".to_string());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        table.insert(row).unwrap();

        io::save(&table, &vtf_path).unwrap();
        vtf_path
    }

    #[test]
    fn load_with_wal_replays_entries() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(2));
        row.insert("name".to_string(), json!("Bob"));
        wal::append(&vtf_path, &WalEntry::Insert { row }).unwrap();

        let table = load_with_wal(&vtf_path).unwrap();
        assert_eq!(table.row_count, 2);
    }

    #[test]
    fn compact_merges_wal_into_base() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(2));
        row.insert("name".to_string(), json!("Bob"));
        wal::append(&vtf_path, &WalEntry::Insert { row }).unwrap();

        assert!(wal::wal_path(&vtf_path).exists());

        let table = compact(&vtf_path).unwrap();
        assert_eq!(table.row_count, 2);

        assert!(!wal::wal_path(&vtf_path).exists());

        let reloaded = io::load(&vtf_path).unwrap();
        assert_eq!(reloaded.row_count, 2);
    }

    #[test]
    fn compact_noop_without_wal() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());

        let table = compact(&vtf_path).unwrap();
        assert_eq!(table.row_count, 1);
    }

    #[test]
    fn save_with_wal_appends() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(2));
        row.insert("name".to_string(), json!("Bob"));
        save_with_wal(&vtf_path, &WalEntry::Insert { row }).unwrap();

        let entries = wal::read_entries(&vtf_path).unwrap();
        assert_eq!(entries.len(), 1);

        let table = load_with_wal(&vtf_path).unwrap();
        assert_eq!(table.row_count, 2);
    }

    #[test]
    fn load_without_wal_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());
        let table = load_with_wal(&vtf_path).unwrap();
        assert_eq!(table.row_count, 1);
    }

    #[test]
    fn multiple_wal_operations() {
        let dir = tempfile::tempdir().unwrap();
        let vtf_path = create_test_file(dir.path());

        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(2));
        row.insert("name".to_string(), json!("Bob"));
        save_with_wal(&vtf_path, &WalEntry::Insert { row }).unwrap();

        let mut vals = IndexMap::new();
        vals.insert("name".to_string(), json!("Robert"));
        save_with_wal(&vtf_path, &WalEntry::Update {
            indices: vec![1],
            values: vals,
        }).unwrap();

        save_with_wal(&vtf_path, &WalEntry::Delete {
            indices: vec![0],
        }).unwrap();

        let table = load_with_wal(&vtf_path).unwrap();
        assert_eq!(table.row_count, 1);
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Robert"));
    }
}
