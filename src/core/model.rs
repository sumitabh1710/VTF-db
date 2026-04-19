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

    pub fn remove(&mut self, idx: usize) {
        match self {
            ColumnData::Int(v) => { v.remove(idx); }
            ColumnData::Float(v) => { v.remove(idx); }
            ColumnData::Str(v) => { v.remove(idx); }
            ColumnData::Bool(v) => { v.remove(idx); }
            ColumnData::Date(v) => { v.remove(idx); }
            ColumnData::ArrayInt(v) => { v.remove(idx); }
            ColumnData::ArrayFloat(v) => { v.remove(idx); }
            ColumnData::ArrayStr(v) => { v.remove(idx); }
        }
    }

    pub fn set(&mut self, idx: usize, val: &serde_json::Value) {
        match self {
            ColumnData::Int(v) => {
                v[idx] = if val.is_null() { None }
                else { Some(val.as_i64().or_else(|| val.as_u64().map(|u| u as i64)).unwrap()) };
            }
            ColumnData::Float(v) => {
                v[idx] = if val.is_null() { None } else { val.as_f64() };
            }
            ColumnData::Str(v) => {
                v[idx] = val.as_str().map(|s| s.to_string());
            }
            ColumnData::Bool(v) => {
                v[idx] = if val.is_null() { None } else { val.as_bool() };
            }
            ColumnData::Date(v) => {
                v[idx] = val.as_str().map(|s| s.to_string());
            }
            ColumnData::ArrayInt(v) => {
                v[idx] = if val.is_null() {
                    None
                } else {
                    Some(val.as_array().unwrap().iter().map(|elem| {
                        if elem.is_null() { None }
                        else { Some(elem.as_i64().or_else(|| elem.as_u64().map(|u| u as i64)).unwrap()) }
                    }).collect())
                };
            }
            ColumnData::ArrayFloat(v) => {
                v[idx] = if val.is_null() {
                    None
                } else {
                    Some(val.as_array().unwrap().iter().map(|elem| {
                        if elem.is_null() { None } else { elem.as_f64() }
                    }).collect())
                };
            }
            ColumnData::ArrayStr(v) => {
                v[idx] = if val.is_null() {
                    None
                } else {
                    Some(val.as_array().unwrap().iter().map(|elem| {
                        elem.as_str().map(|s| s.to_string())
                    }).collect())
                };
            }
        }
    }

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
    /// Columns that must have unique values (beyond the primary key).
    pub unique_columns: Vec<std::string::String>,
    /// Columns that reject null values on insert/update.
    pub not_null_columns: Vec<std::string::String>,
    /// Default values applied when a column is omitted from an insert.
    pub defaults: IndexMap<std::string::String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    Hash,
    Sorted,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub column: std::string::String,
    pub index_type: IndexType,
    /// The type of the indexed column, used for type-aware sort-key comparison.
    pub column_type: ColumnType,
    pub map: HashMap<std::string::String, Vec<usize>>,
    /// Only used for sorted indexes: sorted key order (always in type-aware ascending order)
    pub sorted_keys: Option<Vec<std::string::String>>,
}

impl PartialEq for IndexDef {
    fn eq(&self, other: &Self) -> bool {
        self.column == other.column
            && self.index_type == other.index_type
            && self.column_type == other.column_type
            && self.map == other.map
            && self.sorted_keys == other.sorted_keys
    }
}

/// Per-column statistics used by the cost-based query planner.
/// Stats are considered valid only after an explicit `ANALYZE` operation.
/// Any write (insert/update/delete) marks stats as stale (`valid = false`).
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub row_count: usize,
    pub null_count: usize,
    /// Approximate number of distinct non-null values.
    pub distinct_count: usize,
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    /// `false` means stats are stale and should not be trusted by the planner.
    pub valid: bool,
}

impl ColumnStats {
    pub fn invalid() -> Self {
        ColumnStats {
            row_count: 0,
            null_count: 0,
            distinct_count: 0,
            min: None,
            max: None,
            valid: false,
        }
    }
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
    /// Log Sequence Number — incremented on every committed write.
    /// Monotonically increasing; used as the foundation for optimistic
    /// concurrency control in future server mode.
    pub lsn: u64,
    /// Per-column statistics populated by `ANALYZE`. Empty map means no stats collected yet.
    pub stats: IndexMap<std::string::String, ColumnStats>,
    /// HNSW approximate nearest-neighbor indexes for `array<float>` columns.
    /// These are rebuilt offline via `vtf build-vector-index`.
    pub vector_indexes: IndexMap<std::string::String, crate::index::hnsw::HnswGraph>,
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
            meta: Meta {
                primary_key: None,
                unique_columns: Vec::new(),
                not_null_columns: Vec::new(),
                defaults: IndexMap::new(),
            },
            indexes: IndexMap::new(),
            extensions: serde_json::Value::Object(serde_json::Map::new()),
            lsn: 0,
            stats: IndexMap::new(),
            vector_indexes: IndexMap::new(),
        }
    }

    /// Mark all column stats as stale. Called after any write operation.
    pub fn invalidate_stats(&mut self) {
        for s in self.stats.values_mut() {
            s.valid = false;
        }
    }

    /// Recompute statistics for all columns and mark them valid.
    pub fn analyze(&mut self) -> crate::core::error::VtfResult<()> {
        for col in &self.columns {
            let col_data = &self.data[&col.name];
            let s = crate::query::aggregate::compute_stats(col_data)?;
            self.stats.insert(col.name.clone(), s);
        }
        Ok(())
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}
