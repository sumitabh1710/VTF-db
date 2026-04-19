use std::io::{Cursor, Read, Write};

use indexmap::IndexMap;

use crate::core::error::{VtfError, VtfResult};
use crate::core::model::*;

/// Magic bytes to identify a VTF binary file: "VTFb" (0x56 0x54 0x46 0x62)
pub const MAGIC: &[u8; 4] = b"VTFb";
const FORMAT_VERSION_V1: u8 = 1;
/// V2 adds: unique/not-null constraints, defaults, indexes (type + map + sorted_keys), LSN, extensions JSON.
const FORMAT_VERSION_V2: u8 = 2;
/// The version written by the encoder.
const FORMAT_VERSION: u8 = FORMAT_VERSION_V2;

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

    // --- V2 extension section ---
    // Unique columns
    buf.write_all(&(table.meta.unique_columns.len() as u32).to_le_bytes())?;
    for col in &table.meta.unique_columns {
        write_string(&mut buf, col)?;
    }

    // Not-null columns
    buf.write_all(&(table.meta.not_null_columns.len() as u32).to_le_bytes())?;
    for col in &table.meta.not_null_columns {
        write_string(&mut buf, col)?;
    }

    // Defaults (JSON object encoded as a length-prefixed JSON string)
    let defaults_json = serde_json::to_string(&table.meta.defaults)
        .unwrap_or_else(|_| "{}".to_string());
    write_string_u32(&mut buf, &defaults_json)?;

    // Indexes
    buf.write_all(&(table.indexes.len() as u32).to_le_bytes())?;
    for (col_name, idx) in &table.indexes {
        write_string(&mut buf, col_name)?;
        // Index type byte: 0 = hash, 1 = sorted
        let type_byte: u8 = match idx.index_type { IndexType::Hash => 0, IndexType::Sorted => 1 };
        buf.write_all(&[type_byte])?;
        // Column type byte
        buf.write_all(&[column_type_to_byte(&idx.column_type)])?;
        // Map: entry count + each (key, row_ids)
        buf.write_all(&(idx.map.len() as u32).to_le_bytes())?;
        for (key, rows) in &idx.map {
            write_string(&mut buf, key)?;
            buf.write_all(&(rows.len() as u32).to_le_bytes())?;
            for &row_id in rows {
                buf.write_all(&(row_id as u32).to_le_bytes())?;
            }
        }
        // Sorted keys
        let sk_count = idx.sorted_keys.as_ref().map(|v| v.len()).unwrap_or(0);
        buf.write_all(&(sk_count as u32).to_le_bytes())?;
        if let Some(ref keys) = idx.sorted_keys {
            for k in keys {
                write_string(&mut buf, k)?;
            }
        }
    }

    // LSN (8 bytes)
    buf.write_all(&table.lsn.to_le_bytes())?;

    // Extensions (length-prefixed JSON)
    let ext_json = serde_json::to_string(&table.extensions)
        .unwrap_or_else(|_| "{}".to_string());
    write_string_u32(&mut buf, &ext_json)?;

    Ok(buf)
}

/// Write a string with a u32 length prefix (for large blobs like JSON).
fn write_string_u32(buf: &mut Vec<u8>, s: &str) -> VtfResult<()> {
    let bytes = s.as_bytes();
    buf.write_all(&(bytes.len() as u32).to_le_bytes())?;
    buf.write_all(bytes)?;
    Ok(())
}

fn read_string_u32(cursor: &mut Cursor<&[u8]>) -> VtfResult<String> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| {
        VtfError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    })
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

    let mut version_buf = [0u8; 1];
    cursor.read_exact(&mut version_buf)?;
    let file_version = version_buf[0];
    if file_version != FORMAT_VERSION_V1 && file_version != FORMAT_VERSION_V2 {
        return Err(VtfError::validation(format!(
            "unsupported binary format version: {file_version}"
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

    let (meta, indexes, lsn, extensions) = if file_version >= FORMAT_VERSION_V2 {
        decode_v2_extensions(&mut cursor, primary_key, &columns)?
    } else {
        (Meta { primary_key, unique_columns: vec![], not_null_columns: vec![], defaults: IndexMap::new() },
         IndexMap::new(), 0, serde_json::Value::Object(serde_json::Map::new()))
    };

    let vector_indexes = crate::storage::validation::load_vector_indexes_from_extensions_pub(&extensions);

    Ok(VtfTable {
        version: "1.0".to_string(),
        columns,
        row_count,
        data: col_data_map,
        meta,
        indexes,
        extensions,
        lsn,
        stats: IndexMap::new(),
        vector_indexes,
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

    let mut version_buf = [0u8; 1];
    cursor.read_exact(&mut version_buf)?;
    let file_version = version_buf[0];
    if file_version != FORMAT_VERSION_V1 && file_version != FORMAT_VERSION_V2 {
        return Err(VtfError::validation(format!(
            "unsupported binary format version: {file_version}"
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

    let (meta, indexes, lsn, extensions) = if file_version >= FORMAT_VERSION_V2 {
        decode_v2_extensions(&mut cursor, primary_key, &columns)?
    } else {
        (Meta { primary_key, unique_columns: vec![], not_null_columns: vec![], defaults: IndexMap::new() },
         IndexMap::new(), 0, serde_json::Value::Object(serde_json::Map::new()))
    };

    let vector_indexes = crate::storage::validation::load_vector_indexes_from_extensions_pub(&extensions);

    Ok(VtfTable {
        version: "1.0".to_string(),
        columns,
        row_count,
        data: col_data_map,
        meta,
        indexes,
        extensions,
        lsn,
        stats: IndexMap::new(),
        vector_indexes,
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

/// Decode the V2 metadata section that follows the column data.
fn decode_v2_extensions(
    cursor: &mut Cursor<&[u8]>,
    primary_key: Option<String>,
    columns: &[Column],
) -> VtfResult<(Meta, IndexMap<String, IndexDef>, u64, serde_json::Value)> {
    // Unique columns
    let uniq_count = read_u32(cursor)? as usize;
    let mut unique_columns = Vec::with_capacity(uniq_count);
    for _ in 0..uniq_count {
        unique_columns.push(read_string(cursor)?);
    }

    // Not-null columns
    let nn_count = read_u32(cursor)? as usize;
    let mut not_null_columns = Vec::with_capacity(nn_count);
    for _ in 0..nn_count {
        not_null_columns.push(read_string(cursor)?);
    }

    // Defaults
    let defaults_json = read_string_u32(cursor)?;
    let defaults: IndexMap<String, serde_json::Value> =
        serde_json::from_str(&defaults_json).unwrap_or_default();

    // Indexes
    let idx_count = read_u32(cursor)? as usize;
    let mut indexes: IndexMap<String, IndexDef> = IndexMap::new();
    for _ in 0..idx_count {
        let col_name = read_string(cursor)?;
        let mut type_buf = [0u8; 1];
        cursor.read_exact(&mut type_buf)?;
        let index_type = match type_buf[0] {
            0 => IndexType::Hash,
            1 => IndexType::Sorted,
            other => return Err(VtfError::validation(format!("unknown index type byte: {other}"))),
        };
        let mut ct_buf = [0u8; 1];
        cursor.read_exact(&mut ct_buf)?;
        let column_type = byte_to_column_type(ct_buf[0])
            .unwrap_or_else(|_| {
                // Fall back to the column's declared type if encoding is unknown
                columns.iter().find(|c| c.name == col_name)
                    .map(|c| c.col_type.clone())
                    .unwrap_or(ColumnType::String)
            });

        let map_entry_count = read_u32(cursor)? as usize;
        let mut map = std::collections::HashMap::new();
        for _ in 0..map_entry_count {
            let key = read_string(cursor)?;
            let row_count = read_u32(cursor)? as usize;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                rows.push(read_u32(cursor)? as usize);
            }
            map.insert(key, rows);
        }

        let sk_count = read_u32(cursor)? as usize;
        let sorted_keys = if sk_count > 0 {
            let mut keys = Vec::with_capacity(sk_count);
            for _ in 0..sk_count {
                keys.push(read_string(cursor)?);
            }
            Some(keys)
        } else {
            None
        };

        indexes.insert(col_name.clone(), IndexDef { column: col_name, index_type, column_type, map, sorted_keys });
    }

    // LSN
    let mut lsn_buf = [0u8; 8];
    cursor.read_exact(&mut lsn_buf)?;
    let lsn = u64::from_le_bytes(lsn_buf);

    // Extensions
    let ext_json = read_string_u32(cursor)?;
    let extensions: serde_json::Value = serde_json::from_str(&ext_json)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

    let meta = Meta { primary_key, unique_columns, not_null_columns, defaults };
    Ok((meta, indexes, lsn, extensions))
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

    #[test]
    fn v2_roundtrip_preserves_full_metadata() {
        use crate::core::model::IndexType;

        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": "string"},
                {"name": "score", "type": "float"}
            ],
            "rowCount": 3,
            "data": {
                "id": [1, 2, 3],
                "email": ["a@b.com", "c@d.com", "e@f.com"],
                "score": [9.5, 7.2, 8.0]
            },
            "meta": {
                "primaryKey": "id",
                "uniqueColumns": ["email"],
                "notNullColumns": ["email"]
            }
        });
        let mut table = validation::validate_and_build(j).unwrap();
        table.create_index("id", IndexType::Hash).unwrap();
        table.create_index("score", IndexType::Sorted).unwrap();
        table.lsn = 42;

        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();

        assert_eq!(decoded.meta.primary_key, Some("id".to_string()));
        assert_eq!(decoded.meta.unique_columns, vec!["email"]);
        assert_eq!(decoded.meta.not_null_columns, vec!["email"]);
        assert_eq!(decoded.lsn, 42);
        assert!(decoded.indexes.contains_key("id"), "id index should survive roundtrip");
        assert!(decoded.indexes.contains_key("score"), "score index should survive roundtrip");
        assert_eq!(decoded.row_count, 3);
    }

    #[test]
    fn v2_index_roundtrip_correctness() {
        let j = json!({
            "version": "1.0",
            "columns": [
                {"name": "n", "type": "int"}
            ],
            "rowCount": 5,
            "data": { "n": [9, 10, 2, 11, 1] }
        });
        let mut table = validation::validate_and_build(j).unwrap();
        table.create_index("n", crate::core::model::IndexType::Sorted).unwrap();

        let bytes = encode(&table).unwrap();
        let decoded = decode(&bytes).unwrap();

        let idx = decoded.indexes.get("n").expect("index must survive roundtrip");
        let keys = idx.sorted_keys.as_ref().expect("sorted_keys must survive");
        // Must be numerically sorted: 1, 2, 9, 10, 11
        assert_eq!(keys, &["1", "2", "9", "10", "11"]);
    }
}
