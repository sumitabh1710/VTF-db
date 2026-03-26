use indexmap::IndexMap;
use serde_json::json;
use vtf::storage::validation::validate_and_build;
use vtf::*;

fn empty_table() -> VtfTable {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ],
        "rowCount": 0,
        "data": {"id": [], "name": []},
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

fn row(id: i64, name: &str) -> IndexMap<String, serde_json::Value> {
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(id));
    r.insert("name".to_string(), json!(name));
    r
}

#[test]
fn batch_insert_multiple_rows() {
    let mut table = empty_table();
    let rows = vec![row(1, "Alice"), row(2, "Bob"), row(3, "Charlie")];
    let count = table.insert_batch(rows).unwrap();
    assert_eq!(count, 3);
    assert_eq!(table.row_count, 3);
}

#[test]
fn batch_insert_empty() {
    let mut table = empty_table();
    let count = table.insert_batch(vec![]).unwrap();
    assert_eq!(count, 0);
    assert_eq!(table.row_count, 0);
}

#[test]
fn batch_insert_atomic_on_type_error() {
    let mut table = empty_table();
    let rows = vec![
        row(1, "Alice"),
        {
            let mut r = IndexMap::new();
            r.insert("id".to_string(), json!("not_int")); // bad type
            r.insert("name".to_string(), json!("Bad"));
            r
        },
    ];
    assert!(table.insert_batch(rows).is_err());
    assert_eq!(table.row_count, 0); // nothing inserted
}

#[test]
fn batch_insert_atomic_on_pk_duplicate_within_batch() {
    let mut table = empty_table();
    let rows = vec![row(1, "Alice"), row(1, "Duplicate")]; // same PK
    assert!(table.insert_batch(rows).is_err());
    assert_eq!(table.row_count, 0);
}

#[test]
fn batch_insert_atomic_on_pk_duplicate_against_existing() {
    let mut table = empty_table();
    table.insert(row(1, "Alice")).unwrap();
    let rows = vec![row(2, "Bob"), row(1, "Duplicate")]; // id=1 exists
    assert!(table.insert_batch(rows).is_err());
    assert_eq!(table.row_count, 1); // only original row
}

#[test]
fn batch_insert_updates_indexes() {
    let mut table = empty_table();
    table.create_index("name", IndexType::Hash).unwrap();

    let rows = vec![row(1, "Alice"), row(2, "Bob"), row(3, "Alice")];
    table.insert_batch(rows).unwrap();

    let matches = table.filter_eq("name", &json!("Alice")).unwrap();
    assert_eq!(matches, vec![0, 2]);
}

#[test]
fn batch_insert_large() {
    let mut table = empty_table();
    let rows: Vec<IndexMap<String, serde_json::Value>> = (0..500)
        .map(|i| row(i, &format!("User{i}")))
        .collect();
    let count = table.insert_batch(rows).unwrap();
    assert_eq!(count, 500);
    assert_eq!(table.row_count, 500);
}

#[test]
fn batch_insert_then_save_load() {
    let mut table = empty_table();
    let rows = vec![row(1, "Alice"), row(2, "Bob")];
    table.insert_batch(rows).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batch.vtf");
    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.row_count, 2);
}

#[test]
fn batch_insert_rejects_missing_column() {
    let mut table = empty_table();
    let rows = vec![{
        let mut r = IndexMap::new();
        r.insert("id".to_string(), json!(1));
        // missing "name"
        r
    }];
    assert!(table.insert_batch(rows).is_err());
    assert_eq!(table.row_count, 0);
}

#[test]
fn batch_insert_rejects_extra_column() {
    let mut table = empty_table();
    let rows = vec![{
        let mut r = IndexMap::new();
        r.insert("id".to_string(), json!(1));
        r.insert("name".to_string(), json!("Alice"));
        r.insert("extra".to_string(), json!(true));
        r
    }];
    assert!(table.insert_batch(rows).is_err());
    assert_eq!(table.row_count, 0);
}

#[test]
fn batch_insert_with_nulls() {
    let mut table = empty_table();
    let rows = vec![{
        let mut r = IndexMap::new();
        r.insert("id".to_string(), json!(1));
        r.insert("name".to_string(), serde_json::Value::Null);
        r
    }];
    let count = table.insert_batch(rows).unwrap();
    assert_eq!(count, 1);
    let result = table.select_rows(&[0], &[]).unwrap();
    assert!(result[0]["name"].is_null());
}
