use std::io::{Cursor, Read, Write};

use indexmap::IndexMap;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

/// Magic bytes to identify a VTF binary file: "VTFb" (0x56 0x54 0x46 0x62)
pub const MAGIC: &[u8; 4] = b"VTFb";
const FORMAT_VERSION: u8 = 1;

/// Check if raw bytes start with the VTF binary magic bytes.
pub fn is_binary_format(data: &[u8]) -> bool {
    data.len() >= 4 && &data[..4] == MAGIC
}

/// Encode a VtfTable into compact binary format.
///
/// Layout:
///   [4 bytes]  magic "VTFb"
///   [1 byte]   format version
///   [4 bytes]  column count (u32 LE)
///   [4 bytes]  row count (u32 LE)
///   [N bytes]  column directory (for each column: name_len(u16) + name + type(u8))
///   [1 byte]   has_primary_key (0 or 1)
///   [N bytes]  if has_pk: pk_name_len(u16) + pk_name
///   For each column:
///     [row_count bits, rounded up to bytes] null bitmap
///     [N bytes] column data (type-specific)
pub fn encode(table: &VtfTable) -> VtfResult<Vec<u8>> {
    let mut buf = Vec::new();

    buf.write_all(MAGIC)?;
    buf.write_all(&[FORMAT_VERSION])?;

    let col_count = table.columns.len() as u32;
    let row_count = table.row_count as u32;
    buf.write_all(&col_count.to_le_bytes())?;
    buf.write_all(&row_count.to_le_bytes())?;

    for col in &table.columns {
        write_string(&mut buf, &col.name)?;
        buf.write_all(&[column_type_to_byte(&col.col_type)])?;
    }

    if let Some(ref pk) = table.meta.primary_key {
        buf.write_all(&[1u8])?;
        write_string(&mut buf, pk)?;
    } else {
        buf.write_all(&[0u8])?;
    }

    for col in &table.columns {
        let col_data = &table.data[&col.name];
        encode_column(&mut buf, col_data, table.row_count)?;
    }

    Ok(buf)
}

/// Decode only the specified columns from binary data, skipping the rest.
/// Columns not in `needed` are represented as empty vectors in the returned table.
/// This avoids allocating/parsing data for columns the query doesn't need.
pub fn decode_partial(data: &[u8], needed: &std::collections::HashSet<String>) -> VtfResult<VtfTable> {
    let mut cursor = Cursor::new(data);

    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(VtfError::validation("invalid binary magic bytes"));
    }

    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != FORMAT_VERSION {
        return Err(VtfError::validation(format!(
            "unsupported binary format version: {}",
            version[0]
        )));
    }

    let col_count = read_u32(&mut cursor)? as usize;
    let row_count = read_u32(&mut cursor)? as usize;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let name = read_string(&mut cursor)?;
        let mut type_byte = [0u8; 1];
        cursor.read_exact(&mut type_byte)?;
        let col_type = byte_to_column_type(type_byte[0])?;
        columns.push(Column { name, col_type });
    }

    let mut has_pk = [0u8; 1];
    cursor.read_exact(&mut has_pk)?;
    let primary_key = if has_pk[0] == 1 {
        Some(read_string(&mut cursor)?)
    } else {
        None
    };

    let mut col_data_map = IndexMap::new();
    for col in &columns {
        if needed.contains(&col.name) {
            let col_data = decode_column(&mut cursor, &col.col_type, row_count)?;
            col_data_map.insert(col.name.clone(), col_data);
        } else {
            skip_column(&mut cursor, &col.col_type, row_count)?;
            col_data_map.insert(col.name.clone(), ColumnData::empty_for_type(&col.col_type));
        }
    }

    Ok(VtfTable {
        version: "1.0".to_string(),
        columns,
        row_count,
        data: col_data_map,
        meta: Meta {
            primary_key,
            unique_columns: Vec::new(),
            not_null_columns: Vec::new(),
            defaults: IndexMap::new(),
        },
        indexes: IndexMap::new(),
        extensions: serde_json::Value::Object(serde_json::Map::new()),
        lsn: 0,
    })
}

/// Decode binary bytes into a VtfTable.
pub fn decode(data: &[u8]) -> VtfResult<VtfTable> {
    let mut cursor = Cursor::new(data);

    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(VtfError::validation("invalid binary magic bytes"));
    }

    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != FORMAT_VERSION {
        return Err(VtfError::validation(format!(
            "unsupported binary format version: {}",
            version[0]
        )));
    }

    let col_count = read_u32(&mut cursor)? as usize;
    let row_count = read_u32(&mut cursor)? as usize;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let name = read_string(&mut cursor)?;
        let mut type_byte = [0u8; 1];
        cursor.read_exact(&mut type_byte)?;
        let col_type = byte_to_column_type(type_byte[0])?;
        columns.push(Column { name, col_type });
    }

    let mut has_pk = [0u8; 1];
    cursor.read_exact(&mut has_pk)?;
    let primary_key = if has_pk[0] == 1 {
        Some(read_string(&mut cursor)?)
    } else {
        None
    };

    let mut col_data_map = IndexMap::new();
    for col in &columns {
        let col_data = decode_column(&mut cursor, &col.col_type, row_count)?;
        col_data_map.insert(col.name.clone(), col_data);
    }

    Ok(VtfTable {
        version: "1.0".to_string(),
        columns,
        row_count,
        data: col_data_map,
        meta: Meta {
            primary_key,
            unique_columns: Vec::new(),
            not_null_columns: Vec::new(),
            defaults: IndexMap::new(),
        },
        indexes: IndexMap::new(),
        extensions: serde_json::Value::Object(serde_json::Map::new()),
        lsn: 0,
    })
}

fn column_type_to_byte(ct: &ColumnType) -> u8 {
    match ct {
        ColumnType::Int => 0,
        ColumnType::Float => 1,
        ColumnType::String => 2,
        ColumnType::Boolean => 3,
        ColumnType::Date => 4,
        ColumnType::ArrayInt => 5,
        ColumnType::ArrayFloat => 6,
        ColumnType::ArrayString => 7,
    }
}

fn byte_to_column_type(b: u8) -> VtfResult<ColumnType> {
    match b {
        0 => Ok(ColumnType::Int),
        1 => Ok(ColumnType::Float),
        2 => Ok(ColumnType::String),
        3 => Ok(ColumnType::Boolean),
        4 => Ok(ColumnType::Date),
        5 => Ok(ColumnType::ArrayInt),
        6 => Ok(ColumnType::ArrayFloat),
        7 => Ok(ColumnType::ArrayString),
        _ => Err(VtfError::validation(format!("unknown column type byte: {b}"))),
    }
}

fn write_string(buf: &mut Vec<u8>, s: &str) -> VtfResult<()> {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    buf.write_all(&len.to_le_bytes())?;
    buf.write_all(bytes)?;
    Ok(())
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> VtfResult<String> {
    let len = read_u16(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| {
        VtfError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    })
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> VtfResult<u16> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> VtfResult<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn encode_null_bitmap(buf: &mut Vec<u8>, nulls: &[bool]) -> VtfResult<()> {
    let byte_count = (nulls.len() + 7) / 8;
    let mut bitmap = vec![0u8; byte_count];
    for (i, &is_null) in nulls.iter().enumerate() {
        if is_null {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.write_all(&bitmap)?;
    Ok(())
}

fn decode_null_bitmap(cursor: &mut Cursor<&[u8]>, count: usize) -> VtfResult<Vec<bool>> {
    let byte_count = (count + 7) / 8;
    let mut bitmap = vec![0u8; byte_count];
    cursor.read_exact(&mut bitmap)?;
    let mut nulls = Vec::with_capacity(count);
    for i in 0..count {
        nulls.push((bitmap[i / 8] >> (i % 8)) & 1 == 1);
    }
    Ok(nulls)
}

fn encode_column(buf: &mut Vec<u8>, data: &ColumnData, row_count: usize) -> VtfResult<()> {
    match data {
        ColumnData::Int(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                let n = val.unwrap_or(0);
                buf.write_all(&n.to_le_bytes())?;
            }
        }
        ColumnData::Float(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                let n = val.unwrap_or(0.0);
                buf.write_all(&n.to_le_bytes())?;
            }
        }
        ColumnData::Str(v) | ColumnData::Date(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                let s = val.as_deref().unwrap_or("");
                write_string(buf, s)?;
            }
        }
        ColumnData::Bool(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            let byte_count = (row_count + 7) / 8;
            let mut bits = vec![0u8; byte_count];
            for (i, val) in v.iter().enumerate() {
                if val.unwrap_or(false) {
                    bits[i / 8] |= 1 << (i % 8);
                }
            }
            buf.write_all(&bits)?;
        }
        ColumnData::ArrayInt(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                match val {
                    Some(arr) => {
                        let len = arr.len() as u32;
                        buf.write_all(&len.to_le_bytes())?;
                        let inner_nulls: Vec<bool> = arr.iter().map(|x| x.is_none()).collect();
                        encode_null_bitmap(buf, &inner_nulls)?;
                        for elem in arr {
                            buf.write_all(&elem.unwrap_or(0).to_le_bytes())?;
                        }
                    }
                    None => {
                        buf.write_all(&0u32.to_le_bytes())?;
                    }
                }
            }
        }
        ColumnData::ArrayFloat(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                match val {
                    Some(arr) => {
                        let len = arr.len() as u32;
                        buf.write_all(&len.to_le_bytes())?;
                        let inner_nulls: Vec<bool> = arr.iter().map(|x| x.is_none()).collect();
                        encode_null_bitmap(buf, &inner_nulls)?;
                        for elem in arr {
                            buf.write_all(&elem.unwrap_or(0.0).to_le_bytes())?;
                        }
                    }
                    None => {
                        buf.write_all(&0u32.to_le_bytes())?;
                    }
                }
            }
        }
        ColumnData::ArrayStr(v) => {
            let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
            encode_null_bitmap(buf, &nulls)?;
            for val in v {
                match val {
                    Some(arr) => {
                        let len = arr.len() as u32;
                        buf.write_all(&len.to_le_bytes())?;
                        let inner_nulls: Vec<bool> = arr.iter().map(|x| x.is_none()).collect();
                        encode_null_bitmap(buf, &inner_nulls)?;
                        for elem in arr {
                            write_string(buf, elem.as_deref().unwrap_or(""))?;
                        }
                    }
                    None => {
                        buf.write_all(&0u32.to_le_bytes())?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Advance the cursor past one column's data without allocating structures.
fn skip_column(cursor: &mut Cursor<&[u8]>, col_type: &ColumnType, row_count: usize) -> VtfResult<()> {
    let nulls = decode_null_bitmap(cursor, row_count)?;

    match col_type {
        ColumnType::Int | ColumnType::Float => {
            let mut skip = vec![0u8; row_count * 8];
            cursor.read_exact(&mut skip)?;
        }
        ColumnType::String | ColumnType::Date => {
            for _ in 0..row_count {
                let len = read_u16(cursor)? as usize;
                let mut skip = vec![0u8; len];
                cursor.read_exact(&mut skip)?;
            }
        }
        ColumnType::Boolean => {
            let mut skip = vec![0u8; (row_count + 7) / 8];
            cursor.read_exact(&mut skip)?;
        }
        ColumnType::ArrayInt | ColumnType::ArrayFloat => {
            for i in 0..row_count {
                let len = read_u32(cursor)? as usize;
                if !nulls[i] && len > 0 {
                    let inner_bmp = (len + 7) / 8;
                    let mut skip = vec![0u8; inner_bmp + len * 8];
                    cursor.read_exact(&mut skip)?;
                }
            }
        }
        ColumnType::ArrayString => {
            for i in 0..row_count {
                let len = read_u32(cursor)? as usize;
                if !nulls[i] && len > 0 {
                    let inner_bmp = (len + 7) / 8;
                    let mut skip = vec![0u8; inner_bmp];
                    cursor.read_exact(&mut skip)?;
                    for _ in 0..len {
                        let slen = read_u16(cursor)? as usize;
                        let mut skip = vec![0u8; slen];
                        cursor.read_exact(&mut skip)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn decode_column(cursor: &mut Cursor<&[u8]>, col_type: &ColumnType, row_count: usize) -> VtfResult<ColumnData> {
    match col_type {
        ColumnType::Int => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                values.push(if nulls[i] { None } else { Some(i64::from_le_bytes(buf)) });
            }
            Ok(ColumnData::Int(values))
        }
        ColumnType::Float => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                values.push(if nulls[i] { None } else { Some(f64::from_le_bytes(buf)) });
            }
            Ok(ColumnData::Float(values))
        }
        ColumnType::String | ColumnType::Date => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let s = read_string(cursor)?;
                values.push(if nulls[i] { None } else { Some(s) });
            }
            if *col_type == ColumnType::Date {
                Ok(ColumnData::Date(values))
            } else {
                Ok(ColumnData::Str(values))
            }
        }
        ColumnType::Boolean => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let byte_count = (row_count + 7) / 8;
            let mut bits = vec![0u8; byte_count];
            cursor.read_exact(&mut bits)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if nulls[i] {
                    values.push(None);
                } else {
                    values.push(Some((bits[i / 8] >> (i % 8)) & 1 == 1));
                }
            }
            Ok(ColumnData::Bool(values))
        }
        ColumnType::ArrayInt => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let len = read_u32(cursor)? as usize;
                if nulls[i] {
                    values.push(None);
                } else {
                    let inner_nulls = decode_null_bitmap(cursor, len)?;
                    let mut arr = Vec::with_capacity(len);
                    for j in 0..len {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        arr.push(if inner_nulls[j] { None } else { Some(i64::from_le_bytes(buf)) });
                    }
                    values.push(Some(arr));
                }
            }
            Ok(ColumnData::ArrayInt(values))
        }
        ColumnType::ArrayFloat => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let len = read_u32(cursor)? as usize;
                if nulls[i] {
                    values.push(None);
                } else {
                    let inner_nulls = decode_null_bitmap(cursor, len)?;
                    let mut arr = Vec::with_capacity(len);
                    for j in 0..len {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        arr.push(if inner_nulls[j] { None } else { Some(f64::from_le_bytes(buf)) });
                    }
                    values.push(Some(arr));
                }
            }
            Ok(ColumnData::ArrayFloat(values))
        }
        ColumnType::ArrayString => {
            let nulls = decode_null_bitmap(cursor, row_count)?;
            let mut values = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let len = read_u32(cursor)? as usize;
                if nulls[i] {
                    values.push(None);
                } else {
                    let inner_nulls = decode_null_bitmap(cursor, len)?;
                    let mut arr = Vec::with_capacity(len);
                    for j in 0..len {
                        let s = read_string(cursor)?;
                        arr.push(if inner_nulls[j] { None } else { Some(s) });
                    }
                    values.push(Some(arr));
                }
            }
            Ok(ColumnData::ArrayStr(values))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::validation;
    use serde_json::json;

    #[test]
    fn binary_magic_detection() {
        assert!(is_binary_format(b"VTFb\x01"));
        assert!(!is_binary_format(b"{\"version\":\"1.0\"}"));
        assert!(!is_binary_format(b"VTF"));
    }

    #[test]
    fn roundtrip_simple_table() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ],
            "rowCount": 2,
            "data": {
                "id": [1, 2],
                "name": ["Alice", "Bob"]
            },
            "meta": {"primaryKey": "id"}
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();
        assert!(is_binary_format(&bytes));

        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.row_count, 2);
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.meta.primary_key, Some("id".to_string()));

        let rows = decoded.select_rows(&[0, 1], &[]).unwrap();
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["name"], json!("Alice"));
        assert_eq!(rows[1]["id"], json!(2));
        assert_eq!(rows[1]["name"], json!("Bob"));
    }

    #[test]
    fn roundtrip_with_nulls() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, null, 3],
                "name": [null, "Bob", null],
                "active": [true, null, false]
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.row_count, 3);

        let rows = decoded.select_rows(&[0, 1, 2], &[]).unwrap();
        assert_eq!(rows[0]["id"], json!(1));
        assert!(rows[0]["name"].is_null());
        assert_eq!(rows[0]["active"], json!(true));
        assert!(rows[1]["id"].is_null());
        assert_eq!(rows[1]["name"], json!("Bob"));
        assert!(rows[1]["active"].is_null());
    }

    #[test]
    fn roundtrip_all_types() {
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
                {"name": "as", "type": "array<string>"}
            ],
            "rowCount": 1,
            "data": {
                "i": [42],
                "f": [3.14],
                "s": ["hello"],
                "b": [true],
                "d": ["2024-01-15T10:30:00Z"],
                "ai": [[1, 2, null]],
                "af": [[1.5, null]],
                "as": [["a", null, "c"]]
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();

        let rows = decoded.select_rows(&[0], &[]).unwrap();
        assert_eq!(rows[0]["i"], json!(42));
        assert_eq!(rows[0]["f"], json!(3.14));
        assert_eq!(rows[0]["s"], json!("hello"));
        assert_eq!(rows[0]["b"], json!(true));
        assert_eq!(rows[0]["d"], json!("2024-01-15T10:30:00Z"));
        assert_eq!(rows[0]["ai"], json!([1, 2, null]));
        assert_eq!(rows[0]["af"], json!([1.5, null]));
        assert_eq!(rows[0]["as"], json!(["a", null, "c"]));
    }

    #[test]
    fn roundtrip_empty_table() {
        let j = json!({
            "version": "1.0",
            "columns": [{"name": "id", "type": "int"}],
            "rowCount": 0,
            "data": {"id": []}
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.row_count, 0);
    }

    #[test]
    fn binary_is_smaller_than_json_for_string_heavy_data() {
        let names: Vec<serde_json::Value> = (0..100)
            .map(|i| json!(format!("user_{:06}_with_a_longer_name", i)))
            .collect();
        let emails: Vec<serde_json::Value> = (0..100)
            .map(|i| json!(format!("user{}@example.com", i)))
            .collect();
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"}
            ],
            "rowCount": 100,
            "data": {
                "id": (1000000..1000100).collect::<Vec<_>>(),
                "name": names,
                "email": emails
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let json_bytes = table.to_json().unwrap().len();
        let binary_bytes = encode(&table).unwrap().len();
        assert!(binary_bytes < json_bytes, "binary ({binary_bytes}) should be smaller than JSON ({json_bytes})");
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let bad = b"BADM\x01";
        assert!(decode(bad).is_err());
    }

    #[test]
    fn decode_partial_only_loads_requested_columns() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35]
            },
            "meta": {"primaryKey": "id"}
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();

        let mut needed = std::collections::HashSet::new();
        needed.insert("age".to_string());

        let partial = decode_partial(&bytes, &needed).unwrap();
        assert_eq!(partial.row_count, 3);
        assert_eq!(partial.columns.len(), 3);

        // Requested column has data
        assert_eq!(partial.data["age"].len(), 3);

        // Skipped columns have empty vectors
        assert_eq!(partial.data["id"].len(), 0);
        assert_eq!(partial.data["name"].len(), 0);
    }

    #[test]
    fn roundtrip_null_arrays() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "ai", "type": "array<int>"},
                {"name": "as", "type": "array<string>"}
            ],
            "rowCount": 2,
            "data": {
                "ai": [null, [1, 2]],
                "as": [["x"], null]
            }
        });
        let table = validation::validate_and_build(j).unwrap();
        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();
        let rows = decoded.select_rows(&[0, 1], &[]).unwrap();
        assert!(rows[0]["ai"].is_null());
        assert_eq!(rows[1]["ai"], json!([1, 2]));
        assert_eq!(rows[0]["as"], json!(["x"]));
        assert!(rows[1]["as"].is_null());
    }
}
