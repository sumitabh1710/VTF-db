use std::path::PathBuf;
use std::time::Instant;

use indexmap::IndexMap;

use crate::core::error::VtfError;
use crate::core::model::*;
use crate::storage;
use crate::storage::wal::WalEntry;
use crate::query::parser as query_parser;
use crate::cli::commands::Commands;

pub fn run(command: Commands) -> Result<(), VtfError> {
    let start = Instant::now();
    let label = command_label(&command);
    let result = run_inner(command);
    let elapsed = start.elapsed();
    eprintln!("[{label}] {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    result
}

fn command_label(cmd: &Commands) -> &'static str {
    match cmd {
        Commands::Create { .. } => "CREATE",
        Commands::Validate { .. } => "VALIDATE",
        Commands::Insert { .. } => "INSERT",
        Commands::Delete { .. } => "DELETE",
        Commands::Update { .. } => "UPDATE",
        Commands::Query { .. } => "QUERY",
        Commands::Info { .. } => "INFO",
        Commands::Export { .. } => "EXPORT",
        Commands::Search { .. } => "SEARCH",
        Commands::Aggregate { .. } => "AGGREGATE",
        Commands::DropIndex { .. } => "DROP-INDEX",
        Commands::AddColumn { .. } => "ADD-COLUMN",
        Commands::CreateIndex { .. } => "CREATE-INDEX",
    }
}

fn run_inner(command: Commands) -> Result<(), VtfError> {
    match command {
        Commands::Create {
            file,
            columns,
            primary_key,
        } => cmd_create(&file, &columns, primary_key.as_deref()),

        Commands::Validate { file } => cmd_validate(&file),

        Commands::Insert { file, row, rows } => cmd_insert(&file, row.as_deref(), rows.as_deref()),

        Commands::Delete { file, filter } => cmd_delete(&file, &filter),

        Commands::Update { file, filter, set } => cmd_update(&file, &filter, &set),

        Commands::Query {
            file,
            filter,
            select,
            limit,
        } => cmd_query(&file, filter.as_deref(), select.as_deref(), limit),

        Commands::Info { file } => cmd_info(&file),

        Commands::Export { file, pretty, format, output } => {
            cmd_export(&file, pretty, &format, output.as_deref())
        }

        Commands::Aggregate {
            file,
            column,
            function,
            filter,
        } => cmd_aggregate(&file, &column, &function, filter.as_deref()),

        Commands::DropIndex { file, column } => cmd_drop_index(&file, &column),

        Commands::Search {
            file,
            column,
            vector,
            top_k,
            metric,
            select,
        } => cmd_search(&file, &column, &vector, top_k, &metric, select.as_deref()),

        Commands::AddColumn {
            file,
            name,
            col_type,
        } => cmd_add_column(&file, &name, &col_type),

        Commands::CreateIndex {
            file,
            column,
            index_type,
        } => cmd_create_index(&file, &column, &index_type),
    }
}

fn cmd_create(
    file: &PathBuf,
    columns_str: &str,
    primary_key: Option<&str>,
) -> Result<(), VtfError> {
    let columns = parse_columns(columns_str)?;
    let mut table = VtfTable::new(columns);

    if let Some(pk) = primary_key {
        if table.find_column(pk).is_none() {
            return Err(VtfError::validation(format!(
                "primary key column '{pk}' not found in columns"
            )));
        }
        table.meta.primary_key = Some(pk.to_string());
    }

    // Create writes the initial base file directly (no WAL for new files)
    storage::save(&table, file)?;
    println!("Created {}", file.display());
    Ok(())
}

fn cmd_validate(file: &PathBuf) -> Result<(), VtfError> {
    storage::load_with_wal(file)?;
    println!("Valid VTF file: {}", file.display());
    Ok(())
}

fn cmd_insert(
    file: &PathBuf,
    row_json: Option<&str>,
    rows_json: Option<&str>,
) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;

    if let Some(rows_str) = rows_json {
        let val: serde_json::Value = serde_json::from_str(rows_str)
            .map_err(|e| VtfError::insert(format!("invalid JSON: {e}")))?;
        let arr = val
            .as_array()
            .ok_or_else(|| VtfError::insert("--rows must be a JSON array"))?;

        let rows: Vec<IndexMap<String, serde_json::Value>> = arr
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| VtfError::insert(format!("rows[{i}] must be a JSON object")))?;
                Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            })
            .collect::<Result<Vec<_>, VtfError>>()?;

        let wal_entry = WalEntry::InsertBatch { rows: rows.clone() };
        let count = table.insert_batch(rows)?;
        storage::save_with_wal(file, &wal_entry)?;
        println!("Inserted {count} rows (rowCount: {})", table.row_count);
    } else if let Some(row_str) = row_json {
        let val: serde_json::Value = serde_json::from_str(row_str)
            .map_err(|e| VtfError::insert(format!("invalid JSON: {e}")))?;
        let obj = val
            .as_object()
            .ok_or_else(|| VtfError::insert("row must be a JSON object"))?;

        let row: IndexMap<String, serde_json::Value> =
            obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let wal_entry = WalEntry::Insert { row: row.clone() };
        table.insert(row)?;
        storage::save_with_wal(file, &wal_entry)?;
        println!("Inserted row (rowCount: {})", table.row_count);
    } else {
        return Err(VtfError::insert("either --row or --rows must be provided"));
    }

    Ok(())
}

fn cmd_delete(file: &PathBuf, filter_str: &str) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;
    let (col, val) = parse_filter(filter_str)?;
    let indices = table.filter_eq(&col, &val)?;

    if indices.is_empty() {
        println!("No matching rows to delete.");
        return Ok(());
    }

    let wal_entry = WalEntry::Delete { indices: indices.clone() };
    let count = table.delete(&indices)?;
    storage::save_with_wal(file, &wal_entry)?;
    println!("Deleted {count} rows (rowCount: {})", table.row_count);
    Ok(())
}

fn cmd_update(file: &PathBuf, filter_str: &str, set_json: &str) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;
    let (col, val) = parse_filter(filter_str)?;
    let indices = table.filter_eq(&col, &val)?;

    if indices.is_empty() {
        println!("No matching rows to update.");
        return Ok(());
    }

    let set_val: serde_json::Value = serde_json::from_str(set_json)
        .map_err(|e| VtfError::insert(format!("invalid --set JSON: {e}")))?;
    let set_obj = set_val
        .as_object()
        .ok_or_else(|| VtfError::insert("--set must be a JSON object"))?;

    let values: IndexMap<String, serde_json::Value> = set_obj
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let wal_entry = WalEntry::Update {
        indices: indices.clone(),
        values: values.clone(),
    };
    let count = table.update(&indices, values)?;
    storage::save_with_wal(file, &wal_entry)?;
    println!("Updated {count} rows");
    Ok(())
}

fn cmd_query(
    file: &PathBuf,
    filter: Option<&str>,
    select: Option<&str>,
    limit: Option<usize>,
) -> Result<(), VtfError> {
    let table = storage::load_with_wal(file)?;

    let mut indices = if let Some(filter_str) = filter {
        if let Ok(expr) = query_parser::parse(filter_str) {
            let plan = table.plan_query(&expr);
            crate::query::planner::execute(&table, &plan)?
        } else {
            let (col, val) = parse_filter(filter_str)?;
            table.filter_eq(&col, &val)?
        }
    } else {
        (0..table.row_count).collect()
    };

    if let Some(n) = limit {
        indices.truncate(n);
    }

    let cols: Vec<&str> = match select {
        Some(s) => s.split(',').map(|c| c.trim()).collect(),
        None => Vec::new(),
    };

    let rows = table.select_rows(&indices, &cols)?;

    if rows.is_empty() {
        println!("No matching rows.");
        return Ok(());
    }

    print_rows(&rows, &table, &cols);
    Ok(())
}

fn cmd_info(file: &PathBuf) -> Result<(), VtfError> {
    let table = storage::load_with_wal(file)?;

    println!("VTF v{}", table.version);
    println!("Rows: {}", table.row_count);
    println!();
    println!("Columns:");
    for col in &table.columns {
        let pk_marker = if table.meta.primary_key.as_deref() == Some(&col.name) {
            " [PK]"
        } else {
            ""
        };
        println!("  {} : {}{}", col.name, col.col_type.as_str(), pk_marker);
    }

    if !table.indexes.is_empty() {
        println!();
        println!("Indexes:");
        for (col, idx) in &table.indexes {
            let type_str = match &idx.index_type {
                IndexType::Hash => "hash",
                IndexType::Sorted => "sorted",
            };
            println!("  {} : {} ({} keys)", col, type_str, idx.map.len());
        }
    }

    Ok(())
}

fn cmd_export(
    file: &PathBuf,
    pretty: bool,
    format: &str,
    output: Option<&std::path::Path>,
) -> Result<(), VtfError> {
    let table = storage::load_with_wal(file)?;

    match format {
        "json" => {
            let json = if pretty {
                table.to_pretty_json()?
            } else {
                table.to_json()?
            };
            if let Some(out_path) = output {
                std::fs::write(out_path, &json)?;
                println!("Exported JSON to {}", out_path.display());
            } else {
                println!("{json}");
            }
        }
        "binary" => {
            let out = output.ok_or_else(|| {
                VtfError::validation("--output is required for binary format")
            })?;
            storage::io::save_binary(&table, out)?;
            println!("Exported binary to {}", out.display());
        }
        "compressed" => {
            let out = output.ok_or_else(|| {
                VtfError::validation("--output is required for compressed format")
            })?;
            let bytes = crate::storage::compression::encode_compressed(&table)?;
            storage::io::atomic_write_public(out, &bytes)?;
            println!("Exported compressed to {}", out.display());
        }
        other => {
            return Err(VtfError::validation(format!(
                "unknown format: '{other}' (expected 'json', 'binary', or 'compressed')"
            )));
        }
    }
    Ok(())
}

fn cmd_aggregate(
    file: &PathBuf,
    column: &str,
    functions_str: &str,
    filter: Option<&str>,
) -> Result<(), VtfError> {
    use crate::query::aggregate;

    let table = storage::load_with_wal(file)?;

    let col_data = table.data.get(column).ok_or_else(|| {
        VtfError::query(format!("column '{column}' not found"))
    })?;

    let indices: Option<Vec<usize>> = if let Some(filter_str) = filter {
        let idx = if let Ok(expr) = query_parser::parse(filter_str) {
            let plan = table.plan_query(&expr);
            crate::query::planner::execute(&table, &plan)?
        } else {
            let (col, val) = parse_filter(filter_str)?;
            table.filter_eq(&col, &val)?
        };
        Some(idx)
    } else {
        None
    };

    let idx_slice = indices.as_deref();

    for func_name in functions_str.split(',').map(|s| s.trim()) {
        match func_name {
            "count" => {
                let v = aggregate::count(col_data, idx_slice);
                println!("count({column}) = {v}");
            }
            "sum" => {
                let v = aggregate::sum(col_data, idx_slice)?;
                println!("sum({column}) = {v}");
            }
            "avg" => {
                let v = aggregate::avg(col_data, idx_slice)?;
                println!("avg({column}) = {v}");
            }
            "min" => {
                let v = aggregate::min_val(col_data, idx_slice)?;
                println!("min({column}) = {}", format_value(&v));
            }
            "max" => {
                let v = aggregate::max_val(col_data, idx_slice)?;
                println!("max({column}) = {}", format_value(&v));
            }
            other => {
                return Err(VtfError::query(format!(
                    "unknown function: '{other}' (expected count, sum, avg, min, max)"
                )));
            }
        }
    }
    Ok(())
}

fn cmd_drop_index(file: &PathBuf, column: &str) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;
    table.drop_index(column)?;
    storage::save(&table, file)?;
    println!("Dropped index on column '{column}'");
    Ok(())
}

fn cmd_search(
    file: &PathBuf,
    column: &str,
    vector_json: &str,
    k: usize,
    metric_str: &str,
    select: Option<&str>,
) -> Result<(), VtfError> {
    use crate::query::vector::{self, Metric};

    let table = storage::load_with_wal(file)?;

    let vec_val: serde_json::Value = serde_json::from_str(vector_json)
        .map_err(|e| VtfError::query(format!("invalid vector JSON: {e}")))?;
    let arr = vec_val
        .as_array()
        .ok_or_else(|| VtfError::query("--vector must be a JSON array"))?;
    let query_vec: Vec<f64> = arr
        .iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_f64()
                .ok_or_else(|| VtfError::query(format!("vector[{i}] is not a number")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let metric = match metric_str {
        "cosine" => Metric::Cosine,
        "euclidean" => Metric::Euclidean,
        other => {
            return Err(VtfError::query(format!(
                "unknown metric: '{other}' (expected 'cosine' or 'euclidean')"
            )));
        }
    };

    let results = vector::top_k(&table, column, &query_vec, k, metric)?;

    if results.is_empty() {
        println!("No matching vectors.");
        return Ok(());
    }

    let cols: Vec<&str> = match select {
        Some(s) => s.split(',').map(|c| c.trim()).collect(),
        None => Vec::new(),
    };

    let score_label = match metric {
        Metric::Cosine => "similarity",
        Metric::Euclidean => "distance",
    };

    let indices: Vec<usize> = results.iter().map(|(idx, _)| *idx).collect();
    let rows = table.select_rows(&indices, &cols)?;

    let col_names: Vec<&str> = if cols.is_empty() {
        table.columns.iter().map(|c| c.name.as_str()).collect()
    } else {
        cols.clone()
    };

    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    widths.push(score_label.len().max(10));

    for row in &rows {
        for (i, col) in col_names.iter().enumerate() {
            let val_str = format_value(row.get(*col).unwrap_or(&serde_json::Value::Null));
            widths[i] = widths[i].max(val_str.len());
        }
    }

    let mut header: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{:width$}", n, width = widths[i]))
        .collect();
    header.push(format!("{:width$}", score_label, width = *widths.last().unwrap()));
    println!("{}", header.join(" | "));

    let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", separator.join("-+-"));

    for (row_idx, (_, score)) in rows.iter().zip(results.iter()) {
        let mut vals: Vec<String> = col_names
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let val = row_idx.get(*col).unwrap_or(&serde_json::Value::Null);
                format!("{:width$}", format_value(val), width = widths[i])
            })
            .collect();
        vals.push(format!("{:>width$.6}", score, width = *widths.last().unwrap()));
        println!("{}", vals.join(" | "));
    }

    println!("\n({} results)", results.len());
    Ok(())
}

fn cmd_add_column(file: &PathBuf, name: &str, col_type_str: &str) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;
    let col_type = ColumnType::from_str(col_type_str)?;
    let wal_entry = WalEntry::AddColumn {
        name: name.to_string(),
        col_type: col_type_str.to_string(),
    };
    table.add_column(name, col_type)?;
    storage::save_with_wal(file, &wal_entry)?;
    println!("Added column '{name}' ({col_type_str})");
    Ok(())
}

fn cmd_create_index(file: &PathBuf, column: &str, index_type_str: &str) -> Result<(), VtfError> {
    let mut table = storage::load_with_wal(file)?;
    let idx_type = match index_type_str {
        "hash" => IndexType::Hash,
        "sorted" => IndexType::Sorted,
        other => {
            return Err(VtfError::validation(format!(
                "unknown index type: '{other}' (expected 'hash' or 'sorted')"
            )))
        }
    };
    let wal_entry = WalEntry::CreateIndex {
        column: column.to_string(),
        index_type: index_type_str.to_string(),
    };
    table.create_index(column, idx_type)?;
    storage::save_with_wal(file, &wal_entry)?;
    println!("Created {index_type_str} index on column '{column}'");
    Ok(())
}

fn parse_columns(s: &str) -> Result<Vec<Column>, VtfError> {
    let mut columns = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, type_str) = part.split_once(':').ok_or_else(|| {
            VtfError::validation(format!(
                "invalid column spec '{part}', expected 'name:type'"
            ))
        })?;
        let col_type = ColumnType::from_str(type_str.trim())?;
        columns.push(Column {
            name: name.trim().to_string(),
            col_type,
        });
    }
    if columns.is_empty() {
        return Err(VtfError::validation("no columns specified"));
    }
    Ok(columns)
}

fn parse_filter(s: &str) -> Result<(String, serde_json::Value), VtfError> {
    let (col, val_str) = s.split_once('=').ok_or_else(|| {
        VtfError::query("filter must be in format 'column=value'")
    })?;

    let val: serde_json::Value = if val_str == "null" {
        serde_json::Value::Null
    } else if val_str == "true" {
        serde_json::json!(true)
    } else if val_str == "false" {
        serde_json::json!(false)
    } else if let Ok(n) = val_str.parse::<i64>() {
        serde_json::json!(n)
    } else if let Ok(n) = val_str.parse::<f64>() {
        serde_json::json!(n)
    } else {
        serde_json::Value::String(val_str.to_string())
    };

    Ok((col.to_string(), val))
}

fn print_rows(
    rows: &[IndexMap<String, serde_json::Value>],
    table: &VtfTable,
    selected_cols: &[&str],
) {
    let col_names: Vec<&str> = if selected_cols.is_empty() {
        table.columns.iter().map(|c| c.name.as_str()).collect()
    } else {
        selected_cols.to_vec()
    };

    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for row in rows {
        for (i, col) in col_names.iter().enumerate() {
            let val_str = format_value(row.get(*col).unwrap_or(&serde_json::Value::Null));
            widths[i] = widths[i].max(val_str.len());
        }
    }

    let header: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{:width$}", n, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", separator.join("-+-"));

    for row in rows {
        let vals: Vec<String> = col_names
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let val = row.get(*col).unwrap_or(&serde_json::Value::Null);
                format!("{:width$}", format_value(val), width = widths[i])
            })
            .collect();
        println!("{}", vals.join(" | "));
    }

    println!("\n({} rows)", rows.len());
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => {
            let elems: Vec<String> = arr.iter().map(|e| format_value(e)).collect();
            format!("[{}]", elems.join(", "))
        }
        serde_json::Value::Object(_) => "{...}".to_string(),
    }
}
