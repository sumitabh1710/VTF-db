use indexmap::IndexMap;
use serde_json::json;
use vtf::validation::validate_and_build;
use vtf::*;

#[test]
fn empty_table_operations() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ],
        "rowCount": 0,
        "data": {"id": [], "name": []}
    });
    let table = validate_and_build(j).unwrap();

    assert_eq!(table.row_count, 0);
    assert_eq!(table.scan_column("id").unwrap().len(), 0);

    let matches = table.filter_eq("id", &json!(1)).unwrap();
    assert!(matches.is_empty());

    let rows = table.select_rows(&[], &[]).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn empty_table_insert_then_query() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []},
        "meta": {"primaryKey": "id"}
    });
    let mut table = validate_and_build(j).unwrap();

    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(1));
    table.insert(r).unwrap();

    assert_eq!(table.row_count, 1);
    let matches = table.filter_eq("id", &json!(1)).unwrap();
    assert_eq!(matches, vec![0]);
}

#[test]
fn schema_evolution_add_column_then_insert() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 2,
        "data": {"id": [1, 2]}
    });
    let mut table = validate_and_build(j).unwrap();

    table.add_column("email", ColumnType::String).unwrap();
    assert_eq!(table.columns.len(), 2);

    // Existing rows have null for new column
    let rows = table.select_rows(&[0, 1], &["email"]).unwrap();
    assert_eq!(rows[0]["email"], serde_json::Value::Null);
    assert_eq!(rows[1]["email"], serde_json::Value::Null);

    // Can insert with new column
    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(3));
    r.insert("email".to_string(), json!("test@example.com"));
    table.insert(r).unwrap();
    assert_eq!(table.row_count, 3);
}

#[test]
fn schema_evolution_roundtrip() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 1,
        "data": {"id": [1]}
    });
    let mut table = validate_and_build(j).unwrap();
    table.add_column("tags", ColumnType::ArrayString).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("evolved.vtf");
    storage::save(&table, &path).unwrap();

    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.columns.len(), 2);
    assert_eq!(loaded.columns[1].name, "tags");
}

#[test]
fn index_survives_roundtrip_with_inserts() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "city", "type": "string"}
        ],
        "rowCount": 0,
        "data": {"id": [], "city": []}
    });
    let mut table = validate_and_build(j).unwrap();
    table.create_index("city", IndexType::Hash).unwrap();

    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(1));
    r.insert("city".to_string(), json!("NYC"));
    table.insert(r).unwrap();

    let mut r2 = IndexMap::new();
    r2.insert("id".to_string(), json!(2));
    r2.insert("city".to_string(), json!("LA"));
    table.insert(r2).unwrap();

    // Index-accelerated query
    let matches = table.filter_eq("city", &json!("NYC")).unwrap();
    assert_eq!(matches, vec![0]);

    // Save and reload
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed.vtf");
    storage::save(&table, &path).unwrap();

    let loaded = storage::load(&path).unwrap();
    assert!(loaded.indexes.contains_key("city"));
}

#[test]
fn large_insert_stress() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "val", "type": "float"}
        ],
        "rowCount": 0,
        "data": {"id": [], "val": []},
        "meta": {"primaryKey": "id"}
    });
    let mut table = validate_and_build(j).unwrap();

    for i in 0..1000 {
        let mut r = IndexMap::new();
        r.insert("id".to_string(), json!(i));
        r.insert("val".to_string(), json!(i as f64 * 1.5));
        table.insert(r).unwrap();
    }

    assert_eq!(table.row_count, 1000);

    let matches = table.filter_eq("id", &json!(500)).unwrap();
    assert_eq!(matches, vec![500]);
}

#[test]
fn null_in_every_type_roundtrip() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "i", "type": "int"},
            {"name": "f", "type": "float"},
            {"name": "s", "type": "string"},
            {"name": "b", "type": "boolean"},
            {"name": "d", "type": "date"},
            {"name": "ai", "type": "array<int>"},
            {"name": "af", "type": "array<float>"},
            {"name": "as_", "type": "array<string>"}
        ],
        "rowCount": 1,
        "data": {
            "i": [null], "f": [null], "s": [null],
            "b": [null], "d": [null],
            "ai": [null], "af": [null], "as_": [null]
        }
    });
    let table = validate_and_build(j).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("allnulls.vtf");
    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();

    let rows = loaded.select_rows(&[0], &[]).unwrap();
    for (_, v) in &rows[0] {
        assert!(v.is_null(), "expected null but got {v}");
    }
}

#[test]
fn float_as_int_in_float_column() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "val", "type": "float"}],
        "rowCount": 2,
        "data": {"val": [1, 2.5]}
    });
    let table = validate_and_build(j).unwrap();
    let rows = table.select_rows(&[0, 1], &[]).unwrap();
    // Integer 1 should be accepted as float
    assert!(rows[0]["val"].is_number());
    assert!(rows[1]["val"].is_number());
}

#[test]
fn create_sorted_index_and_verify_order() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "name", "type": "string"}],
        "rowCount": 4,
        "data": {"name": ["Charlie", "Alice", "Bob", "Alice"]}
    });
    let mut table = validate_and_build(j).unwrap();
    table.create_index("name", IndexType::Sorted).unwrap();

    let idx = &table.indexes["name"];
    let keys = idx.sorted_keys.as_ref().unwrap();
    assert_eq!(keys, &["Alice", "Bob", "Charlie"]);
}

#[test]
fn drop_and_recreate_index() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "x", "type": "int"}],
        "rowCount": 2,
        "data": {"x": [10, 20]}
    });
    let mut table = validate_and_build(j).unwrap();

    table.create_index("x", IndexType::Hash).unwrap();
    assert!(table.indexes.contains_key("x"));

    table.drop_index("x").unwrap();
    assert!(!table.indexes.contains_key("x"));

    table.create_index("x", IndexType::Sorted).unwrap();
    assert!(table.indexes.contains_key("x"));
}

#[test]
fn extensions_preserved() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []},
        "extensions": {"custom": "data", "nested": {"a": 1}}
    });
    let table = validate_and_build(j).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ext.vtf");
    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.extensions["custom"], json!("data"));
    assert_eq!(loaded.extensions["nested"]["a"], json!(1));
}
