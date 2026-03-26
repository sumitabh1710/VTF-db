use crate::core::error::{VtfError, VtfResult};
use crate::core::model::ColumnType;
use chrono::DateTime;

impl ColumnType {
    pub fn from_str(s: &str) -> VtfResult<Self> {
        match s {
            "int" => Ok(ColumnType::Int),
            "float" => Ok(ColumnType::Float),
            "string" => Ok(ColumnType::String),
            "boolean" => Ok(ColumnType::Boolean),
            "date" => Ok(ColumnType::Date),
            "array<int>" => Ok(ColumnType::ArrayInt),
            "array<float>" => Ok(ColumnType::ArrayFloat),
            "array<string>" => Ok(ColumnType::ArrayString),
            other => Err(VtfError::validation(format!("unknown type: '{other}'"))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnType::Int => "int",
            ColumnType::Float => "float",
            ColumnType::String => "string",
            ColumnType::Boolean => "boolean",
            ColumnType::Date => "date",
            ColumnType::ArrayInt => "array<int>",
            ColumnType::ArrayFloat => "array<float>",
            ColumnType::ArrayString => "array<string>",
        }
    }

    pub fn is_array(&self) -> bool {
        matches!(
            self,
            ColumnType::ArrayInt | ColumnType::ArrayFloat | ColumnType::ArrayString
        )
    }
}

pub fn validate_date(s: &str) -> VtfResult<()> {
    if !s.ends_with('Z') {
        return Err(VtfError::validation(format!(
            "date must end with 'Z' (UTC), got: '{s}'"
        )));
    }
    DateTime::parse_from_rfc3339(s).map_err(|e| {
        VtfError::validation(format!("invalid date '{s}': {e}"))
    })?;
    Ok(())
}

pub fn validate_value(
    value: &serde_json::Value,
    col_type: &ColumnType,
    column: &str,
    row: usize,
) -> VtfResult<()> {
    if value.is_null() {
        return Ok(());
    }

    match col_type {
        ColumnType::Int => {
            if !value.is_i64() && !value.is_u64() {
                return Err(VtfError::type_error(
                    column, row, "int",
                    json_type_name(value),
                ));
            }
            if value.is_u64() {
                let v = value.as_u64().unwrap();
                if v > i64::MAX as u64 {
                    return Err(VtfError::type_error(
                        column, row, "int (i64 range)",
                        "integer out of i64 range",
                    ));
                }
            }
        }
        ColumnType::Float => {
            if !value.is_f64() && !value.is_i64() && !value.is_u64() {
                return Err(VtfError::type_error(
                    column, row, "float",
                    json_type_name(value),
                ));
            }
        }
        ColumnType::String => {
            if !value.is_string() {
                return Err(VtfError::type_error(
                    column, row, "string",
                    json_type_name(value),
                ));
            }
        }
        ColumnType::Boolean => {
            if !value.is_boolean() {
                return Err(VtfError::type_error(
                    column, row, "boolean",
                    json_type_name(value),
                ));
            }
        }
        ColumnType::Date => {
            let s = value.as_str().ok_or_else(|| {
                VtfError::type_error(column, row, "date (string)", json_type_name(value))
            })?;
            validate_date(s).map_err(|_| {
                VtfError::type_error(column, row, "date (YYYY-MM-DDTHH:mm:ssZ)", s)
            })?;
        }
        ColumnType::ArrayInt => validate_typed_array(value, column, row, "int", |v| {
            v.is_null() || v.is_i64() || (v.is_u64() && v.as_u64().unwrap() <= i64::MAX as u64)
        })?,
        ColumnType::ArrayFloat => validate_typed_array(value, column, row, "float", |v| {
            v.is_null() || v.is_f64() || v.is_i64() || v.is_u64()
        })?,
        ColumnType::ArrayString => validate_typed_array(value, column, row, "string", |v| {
            v.is_null() || v.is_string()
        })?,
    }
    Ok(())
}

fn validate_typed_array(
    value: &serde_json::Value,
    column: &str,
    row: usize,
    inner_type: &str,
    check: impl Fn(&serde_json::Value) -> bool,
) -> VtfResult<()> {
    let arr = value.as_array().ok_or_else(|| {
        VtfError::type_error(column, row, format!("array<{inner_type}>"), json_type_name(value))
    })?;
    for (i, elem) in arr.iter().enumerate() {
        if !check(elem) {
            return Err(VtfError::type_error(
                column, row,
                format!("array<{inner_type}>[{i}] to be {inner_type} or null"),
                json_type_name(elem),
            ));
        }
    }
    Ok(())
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "float"
            }
        }
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_column_types() {
        assert!(matches!(ColumnType::from_str("int"), Ok(ColumnType::Int)));
        assert!(matches!(ColumnType::from_str("float"), Ok(ColumnType::Float)));
        assert!(matches!(ColumnType::from_str("string"), Ok(ColumnType::String)));
        assert!(matches!(ColumnType::from_str("boolean"), Ok(ColumnType::Boolean)));
        assert!(matches!(ColumnType::from_str("date"), Ok(ColumnType::Date)));
        assert!(matches!(ColumnType::from_str("array<int>"), Ok(ColumnType::ArrayInt)));
        assert!(matches!(ColumnType::from_str("array<float>"), Ok(ColumnType::ArrayFloat)));
        assert!(matches!(ColumnType::from_str("array<string>"), Ok(ColumnType::ArrayString)));
        assert!(ColumnType::from_str("object").is_err());
        assert!(ColumnType::from_str("array<object>").is_err());
    }

    #[test]
    fn test_validate_date() {
        assert!(validate_date("2024-01-15T10:30:00Z").is_ok());
        assert!(validate_date("2024-01-15T10:30:00+05:00").is_err());
        assert!(validate_date("2024-01-15").is_err());
        assert!(validate_date("not-a-date").is_err());
        assert!(validate_date("2024-02-30T10:30:00Z").is_err());
    }

    #[test]
    fn test_validate_value_int() {
        let v = serde_json::json!(42);
        assert!(validate_value(&v, &ColumnType::Int, "col", 0).is_ok());
        let v = serde_json::json!("hello");
        assert!(validate_value(&v, &ColumnType::Int, "col", 0).is_err());
        let v = serde_json::Value::Null;
        assert!(validate_value(&v, &ColumnType::Int, "col", 0).is_ok());
    }

    #[test]
    fn test_validate_value_array() {
        let v = serde_json::json!([1, null, 3]);
        assert!(validate_value(&v, &ColumnType::ArrayInt, "col", 0).is_ok());
        let v = serde_json::json!([1, "two"]);
        assert!(validate_value(&v, &ColumnType::ArrayInt, "col", 0).is_err());
    }
}
