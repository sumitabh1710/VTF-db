# VTF — Vector Table Format

A **columnar, typed, JSON-based database engine** written in Rust.

VTF is designed for predictable validation, efficient columnar access, and simple indexing. It stores data in a strict, well-defined JSON format and provides a complete engine for creating, querying, inserting, and indexing structured data.

## Features

- **Strict schema validation** — 7-step pipeline that rejects malformed data immediately
- **Columnar storage** — data organized by column for efficient scans and filtering
- **Strong typing** — `int`, `float`, `string`, `boolean`, `date`, `array<T>` with no silent coercion
- **Atomic operations** — inserts either fully succeed or leave the table untouched
- **Hash and sorted indexes** — accelerate equality lookups with `O(1)` hash indexes or ordered sorted indexes
- **Atomic file writes** — write to temp file, fsync, then rename (no partial writes on crash)
- **Schema evolution** — add columns with automatic null backfill
- **Pretty-print export** — `--pretty` flag for human-readable JSON output
- **126 tests** — unit tests in every module plus integration test suites

## Supported Types

| Type             | Rust Mapping                       | Nullable |
|------------------|------------------------------------|----------|
| `int`            | `i64`                              | Yes      |
| `float`          | `f64`                              | Yes      |
| `string`         | `String`                           | Yes      |
| `boolean`        | `bool`                             | Yes      |
| `date`           | `String` (UTC, `YYYY-MM-DDTHH:mm:ssZ`) | Yes |
| `array<int>`     | `Vec<Option<i64>>`                 | Yes      |
| `array<float>`   | `Vec<Option<f64>>`                 | Yes      |
| `array<string>`  | `Vec<Option<String>>`              | Yes      |

## Quick Start

### Build

```bash
cargo build --release
```

### Create a table

```bash
vtf create users.vtf --columns "id:int,name:string,age:int,active:boolean" --primary-key id
```

### Insert rows

```bash
vtf insert users.vtf --row '{"id": 1, "name": "Alice", "age": 30, "active": true}'
vtf insert users.vtf --row '{"id": 2, "name": "Bob", "age": 25, "active": false}'
vtf insert users.vtf --row '{"id": 3, "name": "Charlie", "age": 35, "active": true}'
```

### Query

```bash
# All rows
vtf query users.vtf

# Filter by equality
vtf query users.vtf --where "name=Alice"

# Select specific columns
vtf query users.vtf --where "active=true" --select "name,age"
```

### Table info

```bash
vtf info users.vtf
```

```
VTF v1.0
Rows: 3

Columns:
  id     : int [PK]
  name   : string
  age    : int
  active : boolean
```

### Create an index

```bash
vtf create-index users.vtf --column name --type hash
vtf create-index users.vtf --column age --type sorted
```

### Export

```bash
# Compact JSON (default)
vtf export users.vtf

# Pretty-printed JSON
vtf export users.vtf --pretty
```

### Validate a file

```bash
vtf validate users.vtf
```

### Add a column

```bash
vtf add-column users.vtf --name email --type string
```

Existing rows are backfilled with `null`.

## File Format

A `.vtf` file is a JSON document with this structure:

```json
{
  "version": "1.0",
  "columns": [
    { "name": "id", "type": "int" },
    { "name": "name", "type": "string" }
  ],
  "rowCount": 2,
  "data": {
    "id": [1, 2],
    "name": ["Alice", "Bob"]
  },
  "meta": {
    "primaryKey": "id"
  },
  "indexes": {},
  "extensions": {}
}
```

### Validation Rules

1. `version` must be `"1.0"`
2. `columns` must be a non-empty array with unique names and valid types
3. `data` keys must exactly match column names (no extra, no missing)
4. All data arrays must have equal length matching `rowCount`
5. Every value must match its column's declared type (null is always allowed)
6. If a primary key is declared, values must be non-null and unique
7. Index row references must be valid

Validation stops immediately on the first error.

## Library Usage

VTF is also a Rust library (`use vtf::*`):

```rust
use vtf::*;
use vtf::validation::validate_and_build;
use vtf::storage;
use indexmap::IndexMap;
use serde_json::json;

// Create from JSON
let raw = json!({
    "version": "1.0",
    "columns": [{"name": "id", "type": "int"}],
    "rowCount": 0,
    "data": {"id": []},
    "meta": {"primaryKey": "id"}
});
let mut table = validate_and_build(raw).unwrap();

// Or create programmatically
let mut table = VtfTable::new(vec![
    Column { name: "id".into(), col_type: ColumnType::Int },
    Column { name: "name".into(), col_type: ColumnType::String },
]);
table.meta.primary_key = Some("id".into());

// Insert
let mut row = IndexMap::new();
row.insert("id".into(), json!(1));
row.insert("name".into(), json!("Alice"));
table.insert(row).unwrap();

// Query
let matches = table.filter_eq("name", &json!("Alice")).unwrap();
let rows = table.select_rows(&matches, &["id", "name"]).unwrap();

// Index
table.create_index("name", IndexType::Hash).unwrap();

// Schema evolution
table.add_column("email", ColumnType::String).unwrap();

// Save / Load
storage::save(&table, std::path::Path::new("data.vtf")).unwrap();
let loaded = storage::load(std::path::Path::new("data.vtf")).unwrap();

// Export
println!("{}", table.to_pretty_json().unwrap());
```

## Project Structure

```
src/
  lib.rs          Public API re-exports
  main.rs         CLI binary (clap)
  model.rs        Core types: VtfTable, Column, ColumnData, IndexDef
  error.rs        VtfError enum (thiserror)
  types.rs        Type parsing, date validation, value type checking
  validation.rs   7-step strict validation pipeline
  storage.rs      Atomic file I/O + JSON serialization
  insert.rs       Atomic row insert engine
  query.rs        Equality filter + column scan + row reconstruction
  index.rs        Hash index + sorted index
  schema.rs       Schema evolution (add column)

tests/
  validation_tests.rs   30 validation edge cases
  insert_tests.rs       13 insert scenarios
  query_tests.rs        15 query tests
  storage_tests.rs      12 storage round-trip tests
  edge_cases.rs         11 end-to-end edge cases
```

## Running Tests

```bash
cargo test
```

## Design Decisions

- **IndexMap over HashMap** for `data` — preserves column insertion order for deterministic JSON output
- **Nullable array columns** — `Vec<Option<Vec<Option<T>>>>` supports both null cells and null inner elements
- **chrono for date validation** — catches invalid dates like Feb 30 that regex alone would miss
- **tempfile for atomic writes** — creates temp files on the same filesystem to guarantee atomic rename
- **BTreeMap for sorted indexes** — natural sorted key ordering with efficient range scan potential
- **Copy-on-write insert atomicity** — new values are built in temporaries, only committed if all columns succeed

## License

MIT
