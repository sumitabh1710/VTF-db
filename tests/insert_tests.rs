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
            {"name": "active", "type": "boolean"}
        ],
        "rowCount": 1,
        "data": {
            "id": [1],
            "name": ["Alice"],
            "active": [true]
        },
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

fn row(id: i64, name: &str, active: bool) -> IndexMap<String, serde_json::Value> {
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(id));
    r.insert("name".to_string(), json!(name));
    r.insert("active".to_string(), json!(active));
    r
}

#[test]
fn insert_increments_row_count() {
    let mut table = make_table();
    assert_eq!(table.row_count, 1);
    table.insert(row(2, "Bob", false)).unwrap();
    assert_eq!(table.row_count, 2);
    table.insert(row(3, "Charlie", true)).unwrap();
    assert_eq!(table.row_count, 3);
}

#[test]
fn insert_values_are_queryable() {
    let mut table = make_table();
    table.insert(row(2, "Bob", false)).unwrap();
    let matches = table.filter_eq("name", &json!("Bob")).unwrap();
    assert_eq!(matches, vec![1]);
    let rows = table.select_rows(&matches, &[]).unwrap();
    assert_eq!(rows[0]["id"], json!(2));
}

#[test]
fn insert_rejects_missing_column() {
    let mut table = make_table();
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(2));
    r.insert("name".to_string(), json!("Bob"));
    // missing "active"
    assert!(table.insert(r).is_err());
    assert_eq!(table.row_count, 1); // unchanged
}

#[test]
fn insert_rejects_extra_column() {
    let mut table = make_table();
    let mut r = row(2, "Bob", true);
    r.insert("extra".to_string(), json!("oops"));
    assert!(table.insert(r).is_err());
    assert_eq!(table.row_count, 1);
}

#[test]
fn insert_rejects_type_mismatch() {
    let mut table = make_table();
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!("not_an_int"));
    r.insert("name".to_string(), json!("Bob"));
    r.insert("active".to_string(), json!(true));
    assert!(table.insert(r).is_err());
    assert_eq!(table.row_count, 1);
}

#[test]
fn insert_rejects_null_primary_key() {
    let mut table = make_table();
    let mut r = IndexMap::new();
    r.insert("id".to_string(), serde_json::Value::Null);
    r.insert("name".to_string(), json!("Bob"));
    r.insert("active".to_string(), json!(true));
    assert!(table.insert(r).is_err());
    assert_eq!(table.row_count, 1);
}

#[test]
fn insert_rejects_duplicate_primary_key() {
    let mut table = make_table();
    let r = row(1, "Duplicate", true); // id=1 already exists
    assert!(table.insert(r).is_err());
    assert_eq!(table.row_count, 1);
}

#[test]
fn insert_allows_null_in_non_pk_column() {
    let mut table = make_table();
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(2));
    r.insert("name".to_string(), serde_json::Value::Null);
    r.insert("active".to_string(), json!(false));
    assert!(table.insert(r).is_ok());
    assert_eq!(table.row_count, 2);
}

#[test]
fn insert_atomicity_on_failure() {
    let mut table = make_table();
    let original_row_count = table.row_count;

    // This should fail (type mismatch)
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(2));
    r.insert("name".to_string(), json!(123)); // wrong type
    r.insert("active".to_string(), json!(true));
    assert!(table.insert(r).is_err());

    // Table should be completely unchanged
    assert_eq!(table.row_count, original_row_count);
    let col = table.scan_column("id").unwrap();
    assert_eq!(col.len(), original_row_count);
}

#[test]
fn insert_with_index_updates_index() {
    let mut table = make_table();
    table.create_index("name", IndexType::Hash).unwrap();

    table.insert(row(2, "Bob", false)).unwrap();

    // The index should have the new entry
    let idx = &table.indexes["name"];
    assert!(idx.map.contains_key("Bob"));
    assert_eq!(idx.map["Bob"], vec![1]);
}

#[test]
fn insert_multiple_rows_sequentially() {
    let mut table = make_table();
    for i in 2..=100 {
        table.insert(row(i, &format!("User{i}"), i % 2 == 0)).unwrap();
    }
    assert_eq!(table.row_count, 100);
}

#[test]
fn insert_with_array_column() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "tags", "type": "array<string>"}
        ],
        "rowCount": 0,
        "data": {"id": [], "tags": []}
    });
    let mut table = validate_and_build(j).unwrap();

    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(1));
    r.insert("tags".to_string(), json!(["rust", "db"]));
    assert!(table.insert(r).is_ok());
    assert_eq!(table.row_count, 1);
}

#[test]
fn insert_with_null_array_cell() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "scores", "type": "array<int>"}
        ],
        "rowCount": 0,
        "data": {"id": [], "scores": []}
    });
    let mut table = validate_and_build(j).unwrap();

    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(1));
    r.insert("scores".to_string(), serde_json::Value::Null);
    assert!(table.insert(r).is_ok());
    assert_eq!(table.row_count, 1);
}
