use serde_json::json;
use vtf::storage::validation::validate_and_build;

#[test]
fn valid_minimal_table() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []}
    });
    let table = validate_and_build(j).unwrap();
    assert_eq!(table.row_count, 0);
    assert_eq!(table.columns.len(), 1);
}

#[test]
fn valid_all_types() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "a_int", "type": "int"},
            {"name": "b_float", "type": "float"},
            {"name": "c_string", "type": "string"},
            {"name": "d_bool", "type": "boolean"},
            {"name": "e_date", "type": "date"},
            {"name": "f_aint", "type": "array<int>"},
            {"name": "g_afloat", "type": "array<float>"},
            {"name": "h_astr", "type": "array<string>"}
        ],
        "rowCount": 1,
        "data": {
            "a_int": [42],
            "b_float": [3.14],
            "c_string": ["hello"],
            "d_bool": [true],
            "e_date": ["2024-01-15T10:30:00Z"],
            "f_aint": [[1, null, 3]],
            "g_afloat": [[1.0, null]],
            "h_astr": [["a", null, "c"]]
        }
    });
    assert!(validate_and_build(j).is_ok());
}

#[test]
fn reject_missing_version() {
    let j = json!({
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []}
    });
    let err = validate_and_build(j).unwrap_err();
    assert!(err.to_string().contains("version"));
}

#[test]
fn reject_numeric_version() {
    let j = json!({
        "version": 1.0,
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_wrong_version_string() {
    let j = json!({
        "version": "2.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_empty_columns_array() {
    let j = json!({
        "version": "1.0",
        "columns": [],
        "rowCount": 0,
        "data": {}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_columns_not_array() {
    let j = json!({
        "version": "1.0",
        "columns": "not an array",
        "rowCount": 0,
        "data": {}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_duplicate_column_names() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "id", "type": "string"}
        ],
        "rowCount": 0,
        "data": {"id": []}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_unknown_type() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "data", "type": "object"}],
        "rowCount": 0,
        "data": {"data": []}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_missing_data_column() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ],
        "rowCount": 1,
        "data": {"id": [1]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_extra_data_key() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 1,
        "data": {"id": [1], "extra": ["x"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_row_count_mismatch() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 3,
        "data": {"id": [1, 2]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_unequal_column_lengths() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ],
        "rowCount": 2,
        "data": {"id": [1, 2], "name": ["Alice"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_int_type_mismatch() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 1,
        "data": {"id": ["not_int"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_bool_type_mismatch() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "flag", "type": "boolean"}],
        "rowCount": 1,
        "data": {"flag": [1]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_mixed_array() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "arr", "type": "array<int>"}],
        "rowCount": 1,
        "data": {"arr": [[1, "two", 3]]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_invalid_date_no_z() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "d", "type": "date"}],
        "rowCount": 1,
        "data": {"d": ["2024-01-15T10:30:00+05:00"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_invalid_date_partial() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "d", "type": "date"}],
        "rowCount": 1,
        "data": {"d": ["2024-01-15"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_invalid_date_feb_30() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "d", "type": "date"}],
        "rowCount": 1,
        "data": {"d": ["2024-02-30T10:30:00Z"]}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_null_primary_key() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 2,
        "data": {"id": [1, null]},
        "meta": {"primaryKey": "id"}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_duplicate_primary_key() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 2,
        "data": {"id": [1, 1]},
        "meta": {"primaryKey": "id"}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_pk_column_not_exist() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0,
        "data": {"id": []},
        "meta": {"primaryKey": "nonexistent"}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_index_on_array_column() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "tags", "type": "array<string>"}],
        "rowCount": 1,
        "data": {"tags": [["a"]]},
        "indexes": {"tags": {"type": "hash", "map": {}}}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_index_invalid_row_ref() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 1,
        "data": {"id": [1]},
        "indexes": {"id": {"type": "hash", "map": {"1": [0, 5]}}}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn nulls_allowed_in_every_column_type() {
    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "float"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "boolean"},
            {"name": "e", "type": "date"},
            {"name": "f", "type": "array<int>"},
            {"name": "g", "type": "array<float>"},
            {"name": "h", "type": "array<string>"}
        ],
        "rowCount": 1,
        "data": {
            "a": [null],
            "b": [null],
            "c": [null],
            "d": [null],
            "e": [null],
            "f": [null],
            "g": [null],
            "h": [null]
        }
    });
    assert!(validate_and_build(j).is_ok());
}

#[test]
fn valid_hash_index() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "age", "type": "int"}],
        "rowCount": 2,
        "data": {"age": [25, 30]},
        "indexes": {
            "age": {
                "type": "hash",
                "map": {"25": [0], "30": [1]}
            }
        }
    });
    let table = validate_and_build(j).unwrap();
    assert!(table.indexes.contains_key("age"));
}

#[test]
fn valid_sorted_index() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "age", "type": "int"}],
        "rowCount": 2,
        "data": {"age": [22, 25]},
        "indexes": {
            "age": {
                "type": "sorted",
                "values": [22, 25],
                "rowMap": {"22": [0], "25": [1]}
            }
        }
    });
    let table = validate_and_build(j).unwrap();
    assert!(table.indexes.contains_key("age"));
}

#[test]
fn reject_not_json_object() {
    let j = json!([1, 2, 3]);
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_missing_row_count() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "data": {"id": []}
    });
    assert!(validate_and_build(j).is_err());
}

#[test]
fn reject_missing_data() {
    let j = json!({
        "version": "1.0",
        "columns": [{"name": "id", "type": "int"}],
        "rowCount": 0
    });
    assert!(validate_and_build(j).is_err());
}
