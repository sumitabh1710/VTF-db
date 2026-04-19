use std::path::Path;

use indexmap::IndexMap;
use serde_json::Value;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::VtfTable;
use crate::storage::wal::{self, WalEntry};

/// A multi-operation transaction that buffers writes in memory and applies
/// them atomically to the WAL on commit.
///
/// # Usage
/// ```rust,ignore
/// let mut txn = Transaction::new(&table);
/// txn.insert(row1);
/// txn.delete("id = 5", vec![json!(5)]);
/// txn.commit(&path, &mut table)?;
/// ```
///
/// On `commit`, the sequence written to the WAL is:
///   TxnBegin { txn_id }
///   <all buffered ops>
///   TxnCommit { txn_id }
///
/// A crash between TxnBegin and TxnCommit leaves no committed txn_id in the
/// WAL, so replay ignores the entire group.
///
/// ## Optimistic Concurrency Control (OCC)
///
/// `read_lsn` captures the table's LSN at transaction start. On commit,
/// if the table's LSN has advanced (another writer modified the table),
/// `VtfError::OccConflict` is returned and the transaction is not applied.
/// The caller must reload the table and retry.
///
/// This provides snapshot-isolation semantics for the embedded API:
/// ```rust,ignore
/// loop {
///     let mut table = storage::load_with_wal(&path)?;
///     let mut txn = Transaction::new(&table);
///     txn.insert(my_row);
///     match txn.commit(&path, &mut table) {
///         Ok(_) => break,
///         Err(VtfError::OccConflict { .. }) => continue, // retry
///         Err(e) => return Err(e),
///     }
/// }
/// ```
pub struct Transaction {
    pub txn_id: String,
    /// LSN at the time this transaction was created. Used for OCC conflict detection.
    pub read_lsn: u64,
    ops: Vec<WalEntry>,
}

impl Transaction {
    /// Create a new transaction, capturing the current LSN of `table` as the read snapshot.
    pub fn new(table: &VtfTable) -> Self {
        Transaction {
            txn_id: generate_txn_id(),
            read_lsn: table.lsn,
            ops: Vec::new(),
        }
    }

    /// Create a transaction without OCC (read_lsn = 0 always passes).
    /// Useful for migrations, tests, and CLI operations that load fresh from disk.
    pub fn new_unchecked() -> Self {
        Transaction {
            txn_id: generate_txn_id(),
            read_lsn: u64::MAX, // sentinel: skip OCC check
            ops: Vec::new(),
        }
    }

    /// Buffer an insert operation.
    pub fn insert(&mut self, row: IndexMap<String, Value>) {
        self.ops.push(WalEntry::Insert { row });
    }

    /// Buffer a batch insert operation.
    pub fn insert_batch(&mut self, rows: Vec<IndexMap<String, Value>>) {
        self.ops.push(WalEntry::InsertBatch { rows });
    }

    /// Buffer a predicate-based delete operation.
    pub fn delete(&mut self, filter: impl Into<String>, pk_values: Vec<Value>) {
        self.ops.push(WalEntry::Delete {
            filter: filter.into(),
            pk_values,
        });
    }

    /// Buffer a predicate-based update operation.
    pub fn update(
        &mut self,
        filter: impl Into<String>,
        pk_values: Vec<Value>,
        values: IndexMap<String, Value>,
    ) {
        self.ops.push(WalEntry::Update {
            filter: filter.into(),
            pk_values,
            values,
        });
    }

    /// Returns the number of operations buffered so far.
    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    /// Commit the transaction: write TxnBegin + all ops + TxnCommit to the WAL
    /// as a single atomic batch, then apply each op to the in-memory table.
    ///
    /// On success, the table reflects all buffered changes.
    /// On error, any changes already applied to the in-memory table may be
    /// partially applied — the caller should reload from disk in this case.
    pub fn commit(self, vtf_path: &Path, table: &mut VtfTable) -> VtfResult<()> {
        if self.ops.is_empty() {
            return Ok(());
        }

        // OCC conflict check: if another writer has committed since we started,
        // abort this transaction so the caller can reload and retry.
        // The sentinel value u64::MAX bypasses the check for unchecked transactions.
        if self.read_lsn != u64::MAX && table.lsn != self.read_lsn {
            return Err(VtfError::OccConflict {
                read_lsn: self.read_lsn,
                current_lsn: table.lsn,
            });
        }

        // Build the full sequence: begin + ops + commit
        let mut wal_entries = Vec::with_capacity(self.ops.len() + 2);
        wal_entries.push(WalEntry::TxnBegin { txn_id: self.txn_id.clone() });
        wal_entries.extend(self.ops.iter().cloned());
        wal_entries.push(WalEntry::TxnCommit { txn_id: self.txn_id.clone() });

        // Write atomically (single write + fsync)
        wal::append_many(vtf_path, &wal_entries)?;

        // Apply ops to the in-memory table
        for op in &self.ops {
            wal::apply_entry_public(table, op)?;
        }

        // Auto-compact if threshold exceeded
        if wal::needs_compaction(vtf_path)? {
            crate::storage::compaction::compact(vtf_path)?;
        }

        Ok(())
    }

    /// Discard the transaction without writing anything to disk or the table.
    pub fn rollback(self) {
        // Write a TxnRollback marker so the WAL reflects the intent, but since
        // nothing was written yet this is a pure no-op for correctness.
        // Intentionally does nothing — the ops Vec is dropped.
    }
}

impl Default for Transaction {
    /// Creates an unchecked transaction (no OCC). Use `Transaction::new(&table)` when
    /// OCC conflict detection is desired.
    fn default() -> Self {
        Self::new_unchecked()
    }
}

/// Generate a simple transaction ID using timestamp + counter.
fn generate_txn_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("txn-{ts}-{n:04}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::{Column, ColumnType};
    use crate::storage::{io, wal};
    use serde_json::json;

    fn test_table() -> VtfTable {
        let columns = vec![
            Column { name: "id".to_string(), col_type: ColumnType::Int },
            Column { name: "name".to_string(), col_type: ColumnType::String },
        ];
        let mut table = VtfTable::new(columns);
        table.meta.primary_key = Some("id".to_string());
        table
    }

    #[test]
    fn transaction_commit_applies_all_ops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        let mut txn = Transaction::new(&table);
        let mut row1 = IndexMap::new();
        row1.insert("id".to_string(), json!(1));
        row1.insert("name".to_string(), json!("Alice"));
        txn.insert(row1);

        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), json!(2));
        row2.insert("name".to_string(), json!("Bob"));
        txn.insert(row2);

        txn.commit(&path, &mut table).unwrap();

        assert_eq!(table.row_count, 2);
        let rows = table.select_rows(&[0, 1], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alice"));
        assert_eq!(rows[1]["name"], json!("Bob"));
    }

    #[test]
    fn transaction_rollback_leaves_table_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        let mut txn = Transaction::new(&table);
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        txn.insert(row);

        txn.rollback(); // discard

        assert_eq!(table.row_count, 0);
        // WAL should be empty (rollback wrote nothing)
        let entries = wal::read_entries(&path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn incomplete_transaction_not_replayed_after_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        // Simulate crash: write TxnBegin + ops but NO TxnCommit
        let txn_id = "txn-crash-test".to_string();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(99));
        row.insert("name".to_string(), json!("Zombie"));
        let incomplete = vec![
            WalEntry::TxnBegin { txn_id: txn_id.clone() },
            WalEntry::Insert { row },
            // TxnCommit intentionally missing — simulates crash
        ];
        wal::append_many(&path, &incomplete).unwrap();

        // On restart, load with WAL replay
        let reloaded = crate::storage::compaction::load_with_wal(&path).unwrap();
        // The incomplete transaction must NOT appear in the loaded data
        assert_eq!(reloaded.row_count, 0);
    }

    #[test]
    fn committed_transaction_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        let mut txn = Transaction::new(&table);
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        txn.insert(row);
        txn.commit(&path, &mut table).unwrap();

        // Reload from disk — must see the committed row
        let reloaded = crate::storage::compaction::load_with_wal(&path).unwrap();
        assert_eq!(reloaded.row_count, 1);
        let rows = reloaded.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Alice"));
    }

    #[test]
    fn transaction_with_delete_and_insert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();

        // Pre-populate
        let mut row1 = IndexMap::new();
        row1.insert("id".to_string(), json!(1));
        row1.insert("name".to_string(), json!("Alice"));
        table.insert(row1).unwrap();
        io::save(&table, &path).unwrap();

        let mut txn = Transaction::new(&table);
        // Delete Alice
        txn.delete("id = 1", vec![json!(1)]);
        // Insert Bob
        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), json!(2));
        row2.insert("name".to_string(), json!("Bob"));
        txn.insert(row2);
        txn.commit(&path, &mut table).unwrap();

        assert_eq!(table.row_count, 1);
        let rows = table.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["name"], json!("Bob"));
    }

    #[test]
    fn occ_conflict_detected_when_lsn_advances() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        // Start transaction at LSN 0
        let mut txn = Transaction::new(&table);
        assert_eq!(txn.read_lsn, 0);

        // Simulate concurrent write: another writer inserts a row and advances the LSN
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(99));
        row.insert("name".to_string(), json!("Concurrent"));
        table.insert(row).unwrap();
        table.lsn += 1; // advance LSN as a WAL commit would

        // Our transaction's row
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        txn.insert(row);

        // Commit must fail with OccConflict
        let result = txn.commit(&path, &mut table);
        assert!(
            matches!(result, Err(VtfError::OccConflict { read_lsn: 0, current_lsn: 1 })),
            "expected OccConflict, got {:?}",
            result
        );
    }

    #[test]
    fn unchecked_transaction_bypasses_occ() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vtf");
        let mut table = test_table();
        io::save(&table, &path).unwrap();

        // Advance LSN so a checked txn would fail
        table.lsn = 42;

        let mut txn = Transaction::new_unchecked();
        let mut row = IndexMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        txn.insert(row);

        // Should succeed regardless of LSN
        txn.commit(&path, &mut table).unwrap();
        assert_eq!(table.row_count, 1);
    }
}
