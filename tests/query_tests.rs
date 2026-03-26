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
        "rowCount": 5,
        "data": {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Alice", "Eve"],
            "age": [30, 25, 35, 30, 28],
            "active": [true, false, true, true, false]
        },
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

#[test]
fn scan_column_returns_all_values() {
    let table = make_table();
    let col = table.scan_column("name").unwrap();
    assert_eq!(col.len(), 5);
}

#[test]
fn scan_nonexistent_column_fails() {
    let table = make_table();
    assert!(table.scan_column("nonexistent").is_err());
}

#[test]
fn filter_eq_string_returns_correct_indices() {
    let table = make_table();
    let matches = table.filter_eq("name", &json!("Alice")).unwrap();
    assert_eq!(matches, vec![0, 3]);
}

#[test]
fn filter_eq_int() {
    let table = make_table();
    let matches = table.filter_eq("age", &json!(30)).unwrap();
    assert_eq!(matches, vec![0, 3]);
}

#[test]
fn filter_eq_bool() {
    let table = make_table();
    let matches = table.filter_eq("active", &json!(false)).unwrap();
    assert_eq!(matches, vec![1, 4]);
}

#[test]
fn filter_eq_no_matches() {
    let table = make_table();
    let matches = table.filter_eq("name", &json!("Nobody")).unwrap();
    assert!(matches.is_empty());
}

#[test]
fn filter_eq_nonexistent_column_fails() {
    let table = make_table();
    assert!(table.filter_eq("nonexistent", &json!(1)).is_err());
}

#[test]
fn select_rows_with_projection() {
    let table = make_table();
    let rows = table.select_rows(&[0, 2], &["name", "age"]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0]["name"], json!("Alice"));
    assert_eq!(rows[0]["age"], json!(30));
    assert_eq!(rows[1]["name"], json!("Charlie"));
    assert_eq!(rows[1]["age"], json!(35));
}

#[test]
fn select_rows_all_columns() {
    let table = make_table();
    let rows = table.select_rows(&[1], &[]).unwrap();
    assert_eq!(rows[0].len(), 4);
    assert_eq!(rows[0]["id"], json!(2));
    assert_eq!(rows[0]["name"], json!("Bob"));
}

#[test]
fn select_rows_out_of_bounds_fails() {
    let table = make_table();
    assert!(table.select_rows(&[99], &[]).is_err());
}

#[test]
fn select_rows_nonexistent_column_fails() {
    let table = make_table();
    assert!(table.select_rows(&[0], &["nonexistent"]).is_err());
}

#[test]
fn filter_eq_with_hash_index() {
    let mut table = make_table();
    table.create_index("name", IndexType::Hash).unwrap();

    let matches = table.filter_eq("name", &json!("Alice")).unwrap();
    assert_eq!(matches, vec![0, 3]);
}

#[test]
fn filter_eq_with_hash_index_no_match() {
    let mut table = make_table();
    table.create_index("name", IndexType::Hash).unwrap();

    let matches = table.filter_eq("name", &json!("Nobody")).unwrap();
    assert!(matches.is_empty());
}

#[test]
fn query_pipeline_filter_then_select() {
    let table = make_table();
    let indices = table.filter_eq("age", &json!(30)).unwrap();
    let rows = table.select_rows(&indices, &["name"]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], json!("Alice"));
    assert_eq!(rows[1]["name"], json!("Alice"));
}

#[test]
fn query_with_null_values() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "value", "type": "string"}
        ],
        "rowCount": 3,
        "data": {
            "id": [1, 2, 3],
            "value": ["a", null, "c"]
        }
    });
    let table = validate_and_build(j).unwrap();

    let matches = table.filter_eq("value", &serde_json::Value::Null).unwrap();
    assert_eq!(matches, vec![1]);

    let rows = table.select_rows(&[1], &[]).unwrap();
    assert_eq!(rows[0]["value"], serde_json::Value::Null);
}
