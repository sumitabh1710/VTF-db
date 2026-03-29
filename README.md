# VTF — Vector Table Format

A **columnar, typed database engine** written in Rust. No server, no daemon — just a single file and a strict schema.

## What is VTF?

Most databases store data **row by row** (PostgreSQL, MySQL) or as **free-form documents** (MongoDB). VTF takes a different approach: it stores data **column by column** as typed vectors — hence the name *Vector Table Format*.

Every column in a VTF table is a contiguous, homogeneously-typed array. A column of integers is literally a `Vec<Option<i64>>` in memory. A column of strings is a `Vec<Option<String>>`. There is no row object, no BSON document, no tuple — just parallel vectors that share the same length.

The file *is* the database. There is no server process to start, no connection string to configure, no authentication layer to set up. You create a `.vtf` file, read and write to it directly, and that's it. Think SQLite in spirit, but columnar by design.

### The key ideas

- **Columnar layout** — scanning an entire column (e.g. "give me every `age` value") reads one contiguous array, not every row in the table
- **Strict schema** — every write is validated against a 7-step pipeline. No field can be the wrong type, no row can be missing a column, no primary key can be duplicated. The database rejects bad data instead of silently storing it
- **Crash-safe writes** — all mutations go through a Write-Ahead Log with CRC32 checksums. If the process crashes mid-write, the data is recovered on next load. Corrupt WAL entries are detected and skipped automatically
- **Vector similarity search** — store embeddings as `array<float>` columns and run brute-force cosine/euclidean top-K searches for AI and semantic retrieval use cases
- **Multiple storage formats** — the same table can be stored as human-readable JSON (for debugging), compact binary (for size), or zstd-compressed binary (for maximum efficiency)
- **Embedded, zero-dependency runtime** — a single Rust binary with no background processes, no config files, no network ports

## How Data is Stored

Consider a simple `users` table with three rows. Here is how different systems represent it:

**SQL databases (row-oriented)** — data is stored one row at a time:

```
Row 1: { id: 1, name: "Alice",   age: 30 }
Row 2: { id: 2, name: "Bob",     age: 25 }
Row 3: { id: 3, name: "Charlie", age: 35 }
```

To compute the average age, the engine must visit every row and skip past `id` and `name` to reach `age`.

**MongoDB (document-oriented)** — data is stored as independent documents:

```
{ _id: ObjectId(...), id: 1, name: "Alice",   age: 30 }
{ _id: ObjectId(...), id: 2, name: "Bob",     age: 25 }
{ _id: ObjectId(...), id: 3, name: "Charlie", age: 35 }
```

Each document can have different fields. Flexible, but there is no guarantee that every document has an `age` field or that `age` is always an integer.

**VTF (column-oriented)** — data is stored as typed vectors, one per column:

```
id:   [1,       2,     3        ]   ← Vec<Option<i64>>
name: ["Alice", "Bob", "Charlie"]   ← Vec<Option<String>>
age:  [30,      25,    35       ]   ← Vec<Option<i64>>
```

To compute the average age, VTF reads the `age` vector directly — no row scanning, no field skipping. The schema guarantees every value is an `i64` or null. Nothing else can exist there.

## Why VTF?

VTF is not trying to replace PostgreSQL or MongoDB for production web applications. It is a different tool for a different set of problems.

|                    | SQL (Postgres, MySQL)          | MongoDB                         | VTF                                     |
|--------------------|--------------------------------|---------------------------------|-----------------------------------------|
| **Architecture**   | Client-server (daemon required)| Client-server (mongod required) | Embedded, single file, no server        |
| **Data model**     | Row-oriented tables            | Schemaless BSON documents       | Columnar typed vectors                  |
| **Schema**         | Enforced via DDL               | Optional (schemaless by default)| Strictly enforced on every write        |
| **Column scans**   | Requires full table scan or index | Requires collection scan or index | Native — data is already columnar    |
| **Crash safety**   | WAL + fsync (mature)           | Journaling (WiredTiger)         | WAL with CRC32 checksums + auto-compaction |
| **Vector search**  | pgvector extension             | Atlas Vector Search (cloud)     | Built-in brute-force cosine/euclidean   |
| **Aggregations**   | Full SQL (GROUP BY, HAVING)    | Aggregation pipeline            | COUNT, SUM, AVG, MIN, MAX with optional filter |
| **File format**    | Opaque binary (WAL + pages)    | Opaque binary (WiredTiger)      | Inspectable JSON, compact binary, or compressed |
| **Setup**          | Install, configure, start daemon | Install, configure, start mongod | `cargo build` — done                  |
| **Best for**       | General-purpose OLTP           | Flexible document storage       | Typed datasets, column analytics, embedded AI |

### Where VTF shines

- **Typed dataset storage** where schema correctness matters more than flexibility
- **Analytics-style queries** that scan or filter entire columns (e.g. "all users where age > 25")
- **AI / semantic search** — store embeddings alongside structured data and run similarity queries without a separate vector database
- **Aggregations on columnar data** — COUNT, SUM, AVG, MIN, MAX operate directly on column vectors without row reconstruction
- **Embedded use in Rust applications** — link it as a library, no IPC or network overhead
- **Inspectable persistence** — open the `.vtf` file in any text editor (JSON mode) and see exactly what is stored
- **Learning how databases work** — VTF implements a real query planner, WAL with checksums, binary encoding, vector search, and compression in a small, readable codebase

### How it works under the hood

**Write path:** A mutation (insert, update, delete) is validated in memory, then appended to a Write-Ahead Log (`.vtf.wal` file) as a single JSON line with a CRC32 checksum. The base file is not touched. When the WAL exceeds 100 entries, automatic compaction replays all entries into a fresh base file and deletes the WAL.

**Read path:** On load, VTF reads the base file (auto-detecting JSON, binary, or compressed format by magic bytes), then replays any pending WAL entries to reconstruct the current state. Corrupt WAL entries (checksum mismatch) are skipped with a warning — only clean entries are replayed.

**Query path:** A query string like `age > 25 AND active = true` is parsed into an expression tree (AST), fed to a planner that checks which indexes are available, and executed — using hash index lookups for equality, sorted index range scans for comparisons, or full column scans as a fallback. Results can be limited with `--limit`.

**Vector search path:** A similarity query takes an `array<float>` column and a query vector, computes cosine similarity or euclidean distance for every row (brute-force), and returns the top-K closest matches with scores.

**Aggregation path:** Functions like AVG, SUM, COUNT operate directly on the raw column vector — the columnar layout means no row reconstruction is needed. An optional `--where` filter narrows the aggregation to matching rows.

**Observability:** Every CLI command logs its execution time to stderr (e.g. `[QUERY] 1.2ms`, `[INSERT] 0.4ms`). WAL replay logs entry count and duration on startup.

---

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
- **LIMIT** — cap result count with `--limit`

### Vector Search
- **Cosine similarity** — `dot(a,b) / (||a|| * ||b||)` for semantic similarity
- **Euclidean distance** — `sqrt(sum((a_i - b_i)^2))` for spatial proximity
- **Brute-force top-K** — scans `array<float>` column, returns top-K results with scores
- **Null-safe** — skips null rows and dimension-mismatched vectors
- **CLI `search` command** — specify column, query vector, k, metric, and output columns

### Aggregation Functions
- **COUNT** — non-null count
- **SUM / AVG** — numeric columns (int/float)
- **MIN / MAX** — numeric, string, and date columns
- **Filtered aggregation** — combine with `--where` to aggregate only matching rows
- **True columnar advantage** — operates directly on `ColumnData` vectors, no row reconstruction

### Indexing
- **Hash indexes** — `O(1)` equality lookups
- **Sorted indexes** — ordered keys with range query support (`>`, `<`, `>=`, `<=`)
- **Automatic index use** — the planner detects available indexes and chooses the best strategy
- **Drop index** — remove an index when no longer needed

### Storage
- **JSON format** — human-readable, with compact and pretty-print modes
- **Binary format** — column-wise encoding with null bitmaps, 2-5x smaller for string-heavy data
- **Zstd compression** — compressed binary format for maximum space efficiency
- **Projection pushdown** — binary format supports selective column decoding (skip unneeded columns)
- **Auto-detection** — detects format by magic bytes on load
- **Write-ahead log (WAL)** — all mutations go through WAL with CRC32 checksums for crash safety
- **Corrupt entry recovery** — bad WAL entries are detected by checksum and skipped with a warning
- **Auto-compaction** — WAL entries are merged into the base file when threshold is exceeded
- **Atomic file writes** — write to temp file, fsync, then rename (no partial writes on crash)
- **File locking** — advisory shared/exclusive locks prevent concurrent write corruption
- **Multi-format export** — export to JSON, binary, or compressed format via `--format` flag

### Observability
- **Timing on every command** — `[QUERY] 1.2ms`, `[INSERT] 0.4ms`, `[SEARCH] 2.1ms` logged to stderr
- **WAL replay logging** — `[WAL] Replayed 42 entries in 8ms` on startup when entries exist

### Testing & Benchmarks
- **259+ tests** — unit tests in every module plus comprehensive integration test suites
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

# Limit results
vtf query users.vtf --where "age > 20" --limit 10
```

### Vector Similarity Search

```bash
# Create a table with embeddings
vtf create docs.vtf --columns "id:int,text:string,embedding:array<float>" --primary-key id

# Insert documents with embedding vectors
vtf insert docs.vtf --row '{"id": 1, "text": "hello world", "embedding": [0.12, -0.98, 0.44]}'

# Search for nearest vectors (cosine similarity, top 5)
vtf search docs.vtf --column embedding --vector "[0.1, -0.9, 0.5]" --top-k 5 --metric cosine

# Search with euclidean distance and select specific columns
vtf search docs.vtf --column embedding --vector "[0.1, -0.9, 0.5]" --top-k 3 --metric euclidean --select "id,text"
```

### Aggregations

```bash
# Single function
vtf aggregate users.vtf --column age --function avg

# Multiple functions at once
vtf aggregate users.vtf --column age --function "count,sum,avg,min,max"

# Filtered aggregation
vtf aggregate users.vtf --column age --function avg --where "active = true"
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

### Indexes

```bash
# Create indexes
vtf create-index users.vtf --column name --type hash
vtf create-index users.vtf --column age --type sorted

# Drop an index
vtf drop-index users.vtf --column name
```

### Export

```bash
# JSON to stdout
vtf export users.vtf
vtf export users.vtf --pretty

# Binary format
vtf export users.vtf --format binary --output users.vtfb

# Compressed format
vtf export users.vtf --format compressed --output users.vtfz

# JSON to file
vtf export users.vtf --format json --output users.json
```

### Add a column

```bash
vtf add-column users.vtf --name email --type string
```

## Library Usage

```rust
use vtf::*;
use vtf::storage;
use vtf::query::{parser, planner, vector, aggregate};
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

// Aggregation — operates directly on column vectors
let avg_age = aggregate::avg(&table.data["age"], None).unwrap();

// Create indexes for faster queries
table.create_index("name", IndexType::Hash).unwrap();
table.create_index("age", IndexType::Sorted).unwrap();

// Vector similarity search
// (requires an array<float> column — see vector::top_k)
let results = vector::top_k(
    &table, "embedding", &[0.1, -0.9, 0.5], 5, vector::Metric::Cosine
);

// Save / Load with WAL for crash safety
storage::save(&table, std::path::Path::new("data.vtf")).unwrap();
let loaded = storage::load_with_wal(std::path::Path::new("data.vtf")).unwrap();

// Binary and compressed formats
let bytes = storage::binary::encode(&table).unwrap();
let decoded = storage::binary::decode(&bytes).unwrap();
let compressed = storage::compression::encode_compressed(&table).unwrap();
let decoded = storage::compression::decode_compressed(&compressed).unwrap();
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
    binary.rs           Column-wise binary encoding with null bitmaps + partial decode
    compression.rs      Zstd-compressed binary format
    wal.rs              Write-ahead log with CRC32 checksums
    compaction.rs       WAL-to-base-file merge with auto-trigger + replay timing

  engine/
    insert.rs           Atomic single-row and batch insert
    delete.rs           Row deletion with index rebuild
    update.rs           Partial row update with PK safety
    schema.rs           Schema evolution (add column)

  query/
    ast.rs              Expression AST (Eq, Neq, Gt, Gte, Lt, Lte, And, Or, Not)
    parser.rs           Recursive-descent query parser
    filter.rs           Equality filter, column scan, expression evaluation
    planner.rs          Query planner (index selection) + executor + required_columns
    vector.rs           Cosine similarity, euclidean distance, brute-force top-K
    aggregate.rs        COUNT, SUM, AVG, MIN, MAX on ColumnData vectors

  index/
    hash.rs             Hash index construction
    sorted.rs           Sorted index construction + range queries
    rebuild.rs          Index create / rebuild / drop on VtfTable

  cli/
    commands.rs         Clap struct/enum definitions (13 subcommands)
    handlers.rs         CLI command handlers with timing instrumentation

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
            │             ├──> query/  (filter, AST, parser, planner, vector, aggregate)
            ├─────────────┼──> storage/ (validation, json, binary, compression, wal, compaction)
            └─────────────┴──> cli/
```

- **core/** — zero dependencies on other VTF modules (error types, data model, type system)
- **index/** — depends only on core (hash/sorted index building)
- **engine/** — depends on core + index (insert, delete, update, schema)
- **query/** — depends on core + index (filter, AST, parser, planner/executor, vector search, aggregations)
- **storage/** — depends on core (validation, JSON, binary, WAL with CRC32, compaction)
- **cli/** — depends on everything (thin command routing with timing instrumentation)

### Storage Formats

| Format     | Magic Bytes | Extension | Use Case |
|------------|-------------|-----------|----------|
| JSON       | `{`         | `.vtf`    | Human-readable, debugging, interop |
| Binary     | `VTFb`      | `.vtf`    | Compact storage, fast decode, projection pushdown |
| Compressed | `VTFz`      | `.vtf`    | Maximum space efficiency |

### Write-Ahead Log

All mutations go through the WAL by default. Each entry is written as a single JSON line followed by a CRC32 checksum (tab-separated). On load, the base file is read and WAL entries are replayed. Corrupt entries (checksum mismatch or invalid JSON) are skipped with a warning to stderr — only valid entries are applied. When the WAL exceeds 100 entries, automatic compaction merges everything into a new base file and deletes the WAL.

## Design Decisions

- **IndexMap over HashMap** for `data` — preserves column insertion order for deterministic output
- **Layered module architecture** — enforces a strict dependency DAG, prevents circular dependencies
- **WAL-first write path** — all mutations are logged to WAL before compaction; no direct save after mutation
- **CRC32 per WAL line** — lightweight corruption detection without the overhead of cryptographic hashes
- **Graceful corruption recovery** — bad WAL entries are skipped rather than failing the entire load
- **Query AST + planner** — separates parsing, planning, and execution for testability and extensibility
- **Projection pushdown in binary format** — `decode_partial` skips unneeded columns at the byte level
- **Brute-force vector search** — correct and simple; ANN indexes (HNSW, IVF) are out of scope for now
- **Aggregations on raw ColumnData** — the columnar layout means no row reconstruction for COUNT/SUM/AVG/MIN/MAX
- **WAL before binary format** — eliminates the O(n) rewrite bottleneck before optimizing file size
- **JSON-lines WAL** — simple, debuggable; binary WAL can be added later
- **Null bitmaps in binary format** — 1 bit per row per column, compact representation of nullable data
- **Advisory file locking** — prevents concurrent write corruption without requiring a daemon process
- **Copy-on-write insert atomicity** — new values built in temporaries, only committed if all columns succeed
- **Index rebuild after delete/update** — simplest correct approach since row indices shift
- **Timing via stderr** — observability without polluting stdout output that may be piped

## License

MIT
