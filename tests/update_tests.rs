use indexmap::IndexMap;
use serde_json::json;
use vtf::storage::validation::validate_and_build;
use vtf::*;

fn make_table() -> VtfTable {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "active", "type": "boolean"}
        ],
        "rowCount": 4,
        "data": {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Dave"],
            "age": [30, 25, 35, 28],
            "active": [true, false, true, false]
        },
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

#[test]
fn update_by_filter() {
    let mut table = make_table();
    let indices = table.filter_eq("name", &json!("Bob")).unwrap();
    let mut vals = IndexMap::new();
    vals.insert("age".to_string(), json!(26));
    let count = table.update(&indices, vals).unwrap();
    assert_eq!(count, 1);
    let rows = table.select_rows(&[1], &[]).unwrap();
    assert_eq!(rows[0]["age"], json!(26));
    assert_eq!(rows[0]["name"], json!("Bob")); // unchanged
}

#[test]
fn update_multiple_rows_by_filter() {
    let mut table = make_table();
    table.meta.primary_key = None;
    let indices = table.filter_eq("active", &json!(false)).unwrap();
    let mut vals = IndexMap::new();
    vals.insert("active".to_string(), json!(true));
    let count = table.update(&indices, vals).unwrap();
    assert_eq!(count, 2); // Bob and Dave

    let matches = table.filter_eq("active", &json!(false)).unwrap();
    assert!(matches.is_empty());
}

#[test]
fn update_then_save_load_roundtrip() {
    let mut table = make_table();
    let mut vals = IndexMap::new();
    vals.insert("name".to_string(), json!("Alicia"));
    table.update(&[0], vals).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("updated.vtf");
    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    let rows = loaded.select_rows(&[0], &[]).unwrap();
    assert_eq!(rows[0]["name"], json!("Alicia"));
}

#[test]
fn update_partial_columns() {
    let mut table = make_table();
    let mut vals = IndexMap::new();
    vals.insert("age".to_string(), json!(99));
    vals.insert("active".to_string(), json!(false));
    table.update(&[0], vals).unwrap();
    let rows = table.select_rows(&[0], &[]).unwrap();
    assert_eq!(rows[0]["age"], json!(99));
    assert_eq!(rows[0]["active"], json!(false));
    assert_eq!(rows[0]["name"], json!("Alice")); // untouched
    assert_eq!(rows[0]["id"], json!(1)); // untouched
}

#[test]
fn update_pk_to_new_unique_value() {
    let mut table = make_table();
    let mut vals = IndexMap::new();
    vals.insert("id".to_string(), json!(100));
    table.update(&[0], vals).unwrap();
    let rows = table.select_rows(&[0], &[]).unwrap();
    assert_eq!(rows[0]["id"], json!(100));
}

#[test]
fn update_pk_rejects_conflict() {
    let mut table = make_table();
    let mut vals = IndexMap::new();
    vals.insert("id".to_string(), json!(2)); // already taken by Bob
    assert!(table.update(&[0], vals).is_err());
}

#[test]
fn update_does_not_corrupt_on_failure() {
    let mut table = make_table();
    let original_name = table
        .select_rows(&[0], &["name"])
        .unwrap()[0]["name"]
        .clone();

    let mut vals = IndexMap::new();
    vals.insert("age".to_string(), json!("not_a_number")); // type mismatch
    assert!(table.update(&[0], vals).is_err());

    let rows = table.select_rows(&[0], &["name"]).unwrap();
    assert_eq!(rows[0]["name"], original_name);
}

#[test]
fn update_with_index_stays_consistent() {
    let mut table = make_table();
    table.create_index("name", IndexType::Hash).unwrap();

    let mut vals = IndexMap::new();
    vals.insert("name".to_string(), json!("Alicia"));
    table.update(&[0], vals).unwrap();

    let matches = table.filter_eq("name", &json!("Alice")).unwrap();
    assert!(matches.is_empty());
    let matches = table.filter_eq("name", &json!("Alicia")).unwrap();
    assert_eq!(matches, vec![0]);
}
