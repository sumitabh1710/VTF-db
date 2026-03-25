use indexmap::IndexMap;
use serde_json::json;
use vtf::storage;
use vtf::validation::validate_and_build;
use vtf::*;

fn make_table() -> VtfTable {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "score", "type": "float"},
            {"name": "active", "type": "boolean"},
            {"name": "tags", "type": "array<string>"}
        ],
        "rowCount": 2,
        "data": {
            "id": [1, 2],
            "name": ["Alice", null],
            "score": [99.5, 88.0],
            "active": [true, false],
            "tags": [["a", "b"], null]
        },
        "meta": {"primaryKey": "id"}
    });
    validate_and_build(j).unwrap()
}

#[test]
fn save_load_roundtrip() {
    let table = make_table();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();

    assert_eq!(loaded.version, "1.0");
    assert_eq!(loaded.row_count, 2);
    assert_eq!(loaded.columns.len(), 5);
    assert_eq!(loaded.meta.primary_key, Some("id".to_string()));
}

#[test]
fn roundtrip_preserves_data() {
    let table = make_table();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();

    let rows = loaded.select_rows(&[0, 1], &[]).unwrap();
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["name"], json!("Alice"));
    assert_eq!(rows[0]["score"], json!(99.5));
    assert_eq!(rows[0]["active"], json!(true));
    assert_eq!(rows[0]["tags"], json!(["a", "b"]));
    assert_eq!(rows[1]["name"], serde_json::Value::Null);
    assert_eq!(rows[1]["tags"], serde_json::Value::Null);
}

#[test]
fn roundtrip_preserves_indexes() {
    let mut table = make_table();
    table.create_index("name", IndexType::Hash).unwrap();
    table.create_index("id", IndexType::Sorted).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();

    assert!(loaded.indexes.contains_key("name"));
    assert!(loaded.indexes.contains_key("id"));
}

#[test]
fn roundtrip_empty_table() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []}
    });
    let table = validate_and_build(j).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.row_count, 0);
}

#[test]
fn save_then_insert_then_reload() {
    let table = make_table();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("insert.vtf");

    storage::save(&table, &path).unwrap();
    let mut loaded = storage::load(&path).unwrap();

    let mut r = IndexMap::new();
    r.insert("id".to_string(), json!(3));
    r.insert("name".to_string(), json!("Charlie"));
    r.insert("score".to_string(), json!(77.0));
    r.insert("active".to_string(), json!(true));
    r.insert("tags".to_string(), json!(["c"]));
    loaded.insert(r).unwrap();

    storage::save(&loaded, &path).unwrap();
    let reloaded = storage::load(&path).unwrap();
    assert_eq!(reloaded.row_count, 3);
}

#[test]
fn compact_json_has_no_extra_whitespace() {
    let table = make_table();
    let json = table.to_json().unwrap();
    assert!(!json.contains("  ")); // no indentation
}

#[test]
fn pretty_json_is_human_readable() {
    let table = make_table();
    let pretty = table.to_pretty_json().unwrap();
    assert!(pretty.contains('\n'));
    assert!(pretty.contains("  "));
    // Verify it's valid JSON that round-trips
    let raw: serde_json::Value = serde_json::from_str(&pretty).unwrap();
    let reloaded = validate_and_build(raw).unwrap();
    assert_eq!(reloaded.row_count, 2);
}

#[test]
fn load_nonexistent_file_fails() {
    let result = storage::load(std::path::Path::new("/tmp/nonexistent_vtf_file.vtf"));
    assert!(result.is_err());
}

#[test]
fn load_invalid_json_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.vtf");
    std::fs::write(&path, "not json at all").unwrap();
    assert!(storage::load(&path).is_err());
}

#[test]
fn load_invalid_vtf_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.vtf");
    std::fs::write(&path, r#"{"version":"2.0"}"#).unwrap();
    assert!(storage::load(&path).is_err());
}

#[test]
fn roundtrip_with_date_column() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "created", "type": "date"}
        ],
        "rowCount": 1,
        "data": {
            "id": [1],
            "created": ["2024-06-15T12:00:00Z"]
        }
    });
    let table = validate_and_build(j).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dates.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    let rows = loaded.select_rows(&[0], &[]).unwrap();
    assert_eq!(rows[0]["created"], json!("2024-06-15T12:00:00Z"));
}

#[test]
fn roundtrip_with_all_types() {
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
            "i": [42],
            "f": [3.14],
            "s": ["hello"],
            "b": [true],
            "d": ["2024-01-01T00:00:00Z"],
            "ai": [[1, null, 3]],
            "af": [[1.5, null]],
            "as_": [["a", null]]
        }
    });
    let table = validate_and_build(j).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alltypes.vtf");

    storage::save(&table, &path).unwrap();
    let loaded = storage::load(&path).unwrap();
    assert_eq!(loaded.row_count, 1);

    let rows = loaded.select_rows(&[0], &[]).unwrap();
    assert_eq!(rows[0]["i"], json!(42));
    assert_eq!(rows[0]["f"], json!(3.14));
    assert_eq!(rows[0]["s"], json!("hello"));
    assert_eq!(rows[0]["b"], json!(true));
    assert_eq!(rows[0]["d"], json!("2024-01-01T00:00:00Z"));
    assert_eq!(rows[0]["ai"], json!([1, null, 3]));
    assert_eq!(rows[0]["af"], json!([1.5, null]));
    assert_eq!(rows[0]["as_"], json!(["a", null]));
}
