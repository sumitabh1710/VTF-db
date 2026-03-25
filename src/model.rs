use indexmap::IndexMap;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    String,
    Boolean,
    Date,
    ArrayInt,
    ArrayFloat,
    ArrayString,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: std::string::String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Str(Vec<Option<std::string::String>>),
    Bool(Vec<Option<bool>>),
    Date(Vec<Option<std::string::String>>),
    ArrayInt(Vec<Option<Vec<Option<i64>>>>),
    ArrayFloat(Vec<Option<Vec<Option<f64>>>>),
    ArrayStr(Vec<Option<Vec<Option<std::string::String>>>>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int(v) => v.len(),
            ColumnData::Float(v) => v.len(),
            ColumnData::Str(v) => v.len(),
            ColumnData::Bool(v) => v.len(),
            ColumnData::Date(v) => v.len(),
            ColumnData::ArrayInt(v) => v.len(),
            ColumnData::ArrayFloat(v) => v.len(),
            ColumnData::ArrayStr(v) => v.len(),
        }
    }

    pub fn empty_for_type(col_type: &ColumnType) -> Self {
        match col_type {
            ColumnType::Int => ColumnData::Int(Vec::new()),
            ColumnType::Float => ColumnData::Float(Vec::new()),
            ColumnType::String => ColumnData::Str(Vec::new()),
            ColumnType::Boolean => ColumnData::Bool(Vec::new()),
            ColumnType::Date => ColumnData::Date(Vec::new()),
            ColumnType::ArrayInt => ColumnData::ArrayInt(Vec::new()),
            ColumnType::ArrayFloat => ColumnData::ArrayFloat(Vec::new()),
            ColumnType::ArrayString => ColumnData::ArrayStr(Vec::new()),
        }
    }

    pub fn push_null(&mut self) {
        match self {
            ColumnData::Int(v) => v.push(None),
            ColumnData::Float(v) => v.push(None),
            ColumnData::Str(v) => v.push(None),
            ColumnData::Bool(v) => v.push(None),
            ColumnData::Date(v) => v.push(None),
            ColumnData::ArrayInt(v) => v.push(None),
            ColumnData::ArrayFloat(v) => v.push(None),
            ColumnData::ArrayStr(v) => v.push(None),
        }
    }

    pub fn col_type(&self) -> ColumnType {
        match self {
            ColumnData::Int(_) => ColumnType::Int,
            ColumnData::Float(_) => ColumnType::Float,
            ColumnData::Str(_) => ColumnType::String,
            ColumnData::Bool(_) => ColumnType::Boolean,
            ColumnData::Date(_) => ColumnType::Date,
            ColumnData::ArrayInt(_) => ColumnType::ArrayInt,
            ColumnData::ArrayFloat(_) => ColumnType::ArrayFloat,
            ColumnData::ArrayStr(_) => ColumnType::ArrayString,
        }
    }

    /// Get the value at a given row index as a serde_json::Value.
    pub fn get_json_value(&self, idx: usize) -> Option<serde_json::Value> {
        if idx >= self.len() {
            return None;
        }
        Some(match self {
            ColumnData::Int(v) => match &v[idx] {
                Some(n) => serde_json::Value::from(*n),
                None => serde_json::Value::Null,
            },
            ColumnData::Float(v) => match &v[idx] {
                Some(n) => serde_json::json!(*n),
                None => serde_json::Value::Null,
            },
            ColumnData::Str(v) => match &v[idx] {
                Some(s) => serde_json::Value::from(s.as_str()),
                None => serde_json::Value::Null,
            },
            ColumnData::Bool(v) => match &v[idx] {
                Some(b) => serde_json::Value::from(*b),
                None => serde_json::Value::Null,
            },
            ColumnData::Date(v) => match &v[idx] {
                Some(s) => serde_json::Value::from(s.as_str()),
                None => serde_json::Value::Null,
            },
            ColumnData::ArrayInt(v) => match &v[idx] {
                Some(arr) => serde_json::Value::Array(
                    arr.iter()
                        .map(|x| match x {
                            Some(n) => serde_json::Value::from(*n),
                            None => serde_json::Value::Null,
                        })
                        .collect(),
                ),
                None => serde_json::Value::Null,
            },
            ColumnData::ArrayFloat(v) => match &v[idx] {
                Some(arr) => serde_json::Value::Array(
                    arr.iter()
                        .map(|x| match x {
                            Some(n) => serde_json::json!(*n),
                            None => serde_json::Value::Null,
                        })
                        .collect(),
                ),
                None => serde_json::Value::Null,
            },
            ColumnData::ArrayStr(v) => match &v[idx] {
                Some(arr) => serde_json::Value::Array(
                    arr.iter()
                        .map(|x| match x {
                            Some(s) => serde_json::Value::from(s.as_str()),
                            None => serde_json::Value::Null,
                        })
                        .collect(),
                ),
                None => serde_json::Value::Null,
            },
        })
    }

    /// Get the string representation of a value at a row index (for index keys).
    pub fn value_as_key(&self, idx: usize) -> Option<std::string::String> {
        if idx >= self.len() {
            return None;
        }
        Some(match self {
            ColumnData::Int(v) => match &v[idx] {
                Some(n) => n.to_string(),
                None => "null".to_string(),
            },
            ColumnData::Float(v) => match &v[idx] {
                Some(n) => n.to_string(),
                None => "null".to_string(),
            },
            ColumnData::Str(v) => match &v[idx] {
                Some(s) => s.clone(),
                None => "null".to_string(),
            },
            ColumnData::Bool(v) => match &v[idx] {
                Some(b) => b.to_string(),
                None => "null".to_string(),
            },
            ColumnData::Date(v) => match &v[idx] {
                Some(s) => s.clone(),
                None => "null".to_string(),
            },
            _ => return None, // arrays are not indexable
        })
    }
}

#[derive(Debug, Clone)]
pub struct Meta {
    pub primary_key: Option<std::string::String>,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    Hash,
    Sorted,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub column: std::string::String,
    pub index_type: IndexType,
    pub map: HashMap<std::string::String, Vec<usize>>,
    /// Only used for sorted indexes: sorted key order
    pub sorted_keys: Option<Vec<std::string::String>>,
}

#[derive(Debug, Clone)]
pub struct VtfTable {
    pub version: std::string::String,
    pub columns: Vec<Column>,
    pub row_count: usize,
    pub data: IndexMap<std::string::String, ColumnData>,
    pub meta: Meta,
    pub indexes: IndexMap<std::string::String, IndexDef>,
    pub extensions: serde_json::Value,
}

impl VtfTable {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut data = IndexMap::new();
        for col in &columns {
            data.insert(col.name.clone(), ColumnData::empty_for_type(&col.col_type));
        }
        VtfTable {
            version: "1.0".to_string(),
            columns,
            row_count: 0,
            data,
            meta: Meta { primary_key: None },
            indexes: IndexMap::new(),
            extensions: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}
