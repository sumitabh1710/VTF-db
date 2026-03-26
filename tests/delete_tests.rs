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
            {"name": "age", "type": "int"}
        ],
        "rowCount": 5,
        "data": {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 22]
        },
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

#[test]
fn delete_by_filter() {
    let mut table = make_table();
    let indices = table.filter_eq("name", &json!("Bob")).unwrap();
    let count = table.delete(&indices).unwrap();
    assert_eq!(count, 1);
    assert_eq!(table.row_count, 4);
    let matches = table.filter_eq("name", &json!("Bob")).unwrap();
    assert!(matches.is_empty());
}

#[test]
fn delete_multiple_by_filter() {
    let mut table = make_table();
    let indices = table.filter_eq("age", &json!(30)).unwrap();
    let count = table.delete(&indices).unwrap();
    assert_eq!(count, 1);
    assert_eq!(table.row_count, 4);
}

#[test]
fn delete_preserves_pk_integrity() {
    let mut table = make_table();
    table.delete(&[0]).unwrap(); // remove id=1
    // Should be able to insert id=1 again
    let mut row = IndexMap::new();
    row.insert("id".to_string(), json!(1));
    row.insert("name".to_string(), json!("NewAlice"));
    row.insert("age".to_string(), json!(31));
    assert!(table.insert(row).is_ok());
}

#[test]
fn delete_then_save_load_roundtrip() {
    let mut table = make_table();
    table.delete(&[1, 3]).unwrap(); // remove Bob and Dave
    assert_eq!(table.row_count, 3);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deleted.vtf");
    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.row_count, 3);

    let rows = loaded.select_rows(&[0, 1, 2], &["name"]).unwrap();
    assert_eq!(rows[0]["name"], json!("Alice"));
    assert_eq!(rows[1]["name"], json!("Charlie"));
    assert_eq!(rows[2]["name"], json!("Eve"));
}

#[test]
fn delete_all_then_insert() {
    let mut table = make_table();
    table.delete(&[0, 1, 2, 3, 4]).unwrap();
    assert_eq!(table.row_count, 0);

    let mut row = IndexMap::new();
    row.insert("id".to_string(), json!(10));
    row.insert("name".to_string(), json!("Frank"));
    row.insert("age".to_string(), json!(40));
    table.insert(row).unwrap();
    assert_eq!(table.row_count, 1);
}

#[test]
fn delete_with_index_stays_consistent() {
    let mut table = make_table();
    table.create_index("age", IndexType::Sorted).unwrap();
    table.delete(&[0, 4]).unwrap(); // remove Alice(30) and Eve(22)

    let matches = table.filter_eq("age", &json!(30)).unwrap();
    assert!(matches.is_empty());

    let matches = table.filter_eq("age", &json!(25)).unwrap();
    assert_eq!(matches.len(), 1);
}
