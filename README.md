# VTF — Vector Table Format

A **columnar, typed database engine** written in Rust with JSON, binary, and compressed storage formats.

VTF is designed for predictable validation, efficient columnar access, advanced querying, and robust persistence. It supports a strict schema, atomic CRUD operations, a write-ahead log, and multiple storage backends.

## Features

### Core Engine
- **Strict schema validation** — 7-step pipeline that rejects malformed data immediately
- **Columnar storage** — data organized by column for efficient scans and filtering
- **Strong typing** — `int`, `float`, `string`, `boolean`, `date`, `array<T>` with no silent coercion
- **Atomic operations** — inserts, deletes, and updates either fully succeed or leave the table untouched
- **Batch insert** — insert multiple rows atomically with all-or-nothing validation
- **Schema evolution** — add columns with automatic null backfill

### Query Engine
- **Query AST** — expression tree supporting `=`, `!=`, `>`, `>=`, `<`, `<=` operators
- **AND / OR / NOT** — compound boolean expressions with correct precedence
- **Query parser** — parse human-readable expressions like `age > 25 AND name = 'Alice'`
- **Query planner** — automatically selects hash index, sorted index range scan, or full column scan
- **Query executor** — walks the plan, intersects/unions row sets for compound queries

### Indexing
- **Hash indexes** — `O(1)` equality lookups
- **Sorted indexes** — ordered keys with range query support (`>`, `<`, `>=`, `<=`)
- **Automatic index use** — the planner detects available indexes and chooses the best strategy

### Storage
- **JSON format** — human-readable, with compact and pretty-print modes
- **Binary format** — column-wise encoding with null bitmaps, 2-5x smaller for string-heavy data
- **Zstd compression** — compressed binary format for maximum space efficiency
- **Auto-detection** — `load_auto()` detects format by magic bytes on load
- **Write-ahead log (WAL)** — append-only `.vtf.wal` file eliminates full file rewrites on every mutation
- **Auto-compaction** — WAL entries are merged into the base file when threshold is exceeded
- **Atomic file writes** — write to temp file, fsync, then rename (no partial writes on crash)
- **File locking** — advisory shared/exclusive locks prevent concurrent write corruption

### Testing & Benchmarks
- **224+ tests** — unit tests in every module plus comprehensive integration test suites
- **Criterion benchmarks** — insert, query (scan/index/AST), JSON/binary/compressed encode/decode

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
# Single row
vtf insert users.vtf --row '{"id": 1, "name": "Alice", "age": 30, "active": true}'

# Batch insert (atomic — all or nothing)
vtf insert users.vtf --rows '[
  {"id": 2, "name": "Bob", "age": 25, "active": false},
  {"id": 3, "name": "Charlie", "age": 35, "active": true}
]'
```

### Query

```bash
# All rows
vtf query users.vtf

# Simple equality filter
vtf query users.vtf --where "name = 'Alice'"

# Comparison operators
vtf query users.vtf --where "age > 25"

# Compound expressions with AND/OR/NOT
vtf query users.vtf --where "age >= 25 AND active = true"
vtf query users.vtf --where "(age > 30 OR name = 'Bob') AND active = true"
vtf query users.vtf --where "NOT active = false"

# Select specific columns
vtf query users.vtf --where "age > 25" --select "name,age"
```

### Update rows

```bash
vtf update users.vtf --where "name=Bob" --set '{"age": 26, "active": true}'
```

### Delete rows

```bash
vtf delete users.vtf --where "active=false"
```

### Table info

```bash
vtf info users.vtf
```

### Create an index

```bash
vtf create-index users.vtf --column name --type hash
vtf create-index users.vtf --column age --type sorted
```

### Export

```bash
vtf export users.vtf            # Compact JSON
vtf export users.vtf --pretty   # Pretty-printed JSON
```

### Add a column

```bash
vtf add-column users.vtf --name email --type string
```

## Library Usage

```rust
use vtf::*;
use vtf::storage::validation::validate_and_build;
use vtf::storage;
use vtf::query::{parser, planner};
use indexmap::IndexMap;
use serde_json::json;

// Create a table
let mut table = VtfTable::new(vec![
    Column { name: "id".into(), col_type: ColumnType::Int },
    Column { name: "name".into(), col_type: ColumnType::String },
    Column { name: "age".into(), col_type: ColumnType::Int },
]);
table.meta.primary_key = Some("id".into());

// Insert
let mut row = IndexMap::new();
row.insert("id".into(), json!(1));
row.insert("name".into(), json!("Alice"));
row.insert("age".into(), json!(30));
table.insert(row).unwrap();

// Query with the expression engine
let expr = parser::parse("age > 25 AND name = 'Alice'").unwrap();
let plan = table.plan_query(&expr);
let matches = planner::execute(&table, &plan).unwrap();
let rows = table.select_rows(&matches, &["id", "name"]).unwrap();

// Create indexes for faster queries
table.create_index("name", IndexType::Hash).unwrap();
table.create_index("age", IndexType::Sorted).unwrap();

// Binary format
let bytes = vtf::storage::binary::encode(&table).unwrap();
let decoded = vtf::storage::binary::decode(&bytes).unwrap();

// Compressed format
let compressed = vtf::storage::compression::encode_compressed(&table).unwrap();
let decoded = vtf::storage::compression::decode_compressed(&compressed).unwrap();

// Save / Load (with advisory file locking)
storage::save(&table, std::path::Path::new("data.vtf")).unwrap();
let loaded = storage::load(std::path::Path::new("data.vtf")).unwrap();
```

## Project Structure

```
src/
  lib.rs                Top-level module declarations + re-exports
  main.rs               Thin CLI entry point

  core/
    error.rs            VtfError enum (thiserror)
    model.rs            VtfTable, Column, ColumnData, IndexDef, Meta
    types.rs            Type parsing, date validation, value type checking

  storage/
    validation.rs       7-step strict validation pipeline
    json.rs             JSON serialization (compact + pretty)
    io.rs               Atomic file I/O with advisory locking
    binary.rs           Column-wise binary encoding with null bitmaps
    compression.rs      Zstd-compressed binary format
    wal.rs              Write-ahead log (append-only JSON-lines)
    compaction.rs       WAL-to-base-file merge with auto-trigger

  engine/
    insert.rs           Atomic single-row and batch insert
    delete.rs           Row deletion with index rebuild
    update.rs           Partial row update with PK safety
    schema.rs           Schema evolution (add column)

  query/
    ast.rs              Expression AST (Eq, Neq, Gt, Gte, Lt, Lte, And, Or, Not)
    parser.rs           Recursive-descent query parser
    filter.rs           Equality filter, column scan, expression evaluation
    planner.rs          Query planner (index selection) + executor

  index/
    hash.rs             Hash index construction
    sorted.rs           Sorted index construction + range queries
    rebuild.rs          Index create / rebuild / drop on VtfTable

  cli/
    commands.rs         Clap struct/enum definitions
    handlers.rs         CLI command handler functions

benches/
  vtf_bench.rs          Criterion benchmarks (insert, query, encode, decode)

tests/
  validation_tests.rs   30 validation edge cases
  insert_tests.rs       13 insert scenarios
  batch_insert_tests.rs 11 batch insert tests
  delete_tests.rs        6 delete integration tests
  update_tests.rs        8 update integration tests
  query_tests.rs        15 query tests
  storage_tests.rs      12 storage round-trip tests
  edge_cases.rs         11 end-to-end edge cases
```

## Running Tests

```bash
cargo test
```

## Benchmarks

```bash
cargo bench
```

## Architecture

### Module Dependency DAG

```
core/ ──────┬──> index/ ──┬──> engine/
            │             ├──> query/
            ├─────────────┼──> storage/
            └─────────────┴──> cli/
```

- **core/** — zero dependencies on other VTF modules (error types, data model, type system)
- **index/** — depends only on core (hash/sorted index building)
- **engine/** — depends on core + index (insert, delete, update, schema)
- **query/** — depends on core + index (filter, AST, parser, planner/executor)
- **storage/** — depends on core (validation, JSON, binary, WAL, compaction)
- **cli/** — depends on everything (thin command routing)

### Storage Formats

| Format     | Magic Bytes | Extension | Use Case |
|------------|-------------|-----------|----------|
| JSON       | `{`         | `.vtf`    | Human-readable, debugging, interop |
| Binary     | `VTFb`      | `.vtf`    | Compact storage, fast decode |
| Compressed | `VTFz`      | `.vtf`    | Maximum space efficiency |

### Write-Ahead Log

Mutations are first appended to a `.vtf.wal` file (JSON-lines format), avoiding full file rewrites. On load, the base file is read and WAL entries are replayed. When the WAL exceeds 100 entries, automatic compaction merges everything into a new base file and deletes the WAL.

## Design Decisions

- **IndexMap over HashMap** for `data` — preserves column insertion order for deterministic output
- **Layered module architecture** — enforces a strict dependency DAG, prevents circular dependencies
- **Query AST + planner** — separates parsing, planning, and execution for testability and extensibility
- **WAL before binary format** — eliminates the O(n) rewrite bottleneck before optimizing file size
- **JSON-lines WAL** — simple, debuggable; binary WAL can be added later
- **Null bitmaps in binary format** — 1 bit per row per column, compact representation of nullable data
- **Advisory file locking** — prevents concurrent write corruption without requiring a daemon process
- **Copy-on-write insert atomicity** — new values built in temporaries, only committed if all columns succeed
- **Index rebuild after delete/update** — simplest correct approach since row indices shift

## License

MIT
