use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use indexmap::IndexMap;
use serde_json::json;

use vtf::*;
use vtf::storage::{binary, validation};
use vtf::storage::compression;
use vtf::query::parser;
use vtf::query::planner;

fn make_table(n: usize) -> VtfTable {
    let ids: Vec<serde_json::Value> = (0..n).map(|i| json!(i as i64)).collect();
    let names: Vec<serde_json::Value> = (0..n).map(|i| json!(format!("user_{i:06}"))).collect();
    let ages: Vec<serde_json::Value> = (0..n).map(|i| json!((i % 80) as i64 + 18)).collect();

    let j = json!({
        "version": "1.0",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ],
        "rowCount": n,
        "data": {
            "id": ids,
            "name": names,
            "age": ages
        },
        "meta": {"primaryKey": "id"}
    });
    validation::validate_and_build(j).unwrap()
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    for size in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                let mut table = make_table(0);
                let rows: Vec<IndexMap<String, serde_json::Value>> = (0..n)
                    .map(|i| {
                        let mut row = IndexMap::new();
                        row.insert("id".to_string(), json!(i as i64));
                        row.insert("name".to_string(), json!(format!("u_{i}")));
                        row.insert("age".to_string(), json!(25));
                        row
                    })
                    .collect();
                table.insert_batch(rows).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_query_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_scan");
    for size in [1000, 10000, 100000] {
        let table = make_table(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                table.filter_eq("age", &json!(25)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_query_with_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_index");
    for size in [1000, 10000, 100000] {
        let mut table = make_table(size);
        table.create_index("age", IndexType::Hash).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                table.filter_eq("age", &json!(25)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_query_ast(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_ast");
    for size in [1000, 10000] {
        let table = make_table(size);
        let expr = parser::parse("age > 30 AND age <= 50").unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                let plan = table.plan_query(&expr);
                planner::execute(table, &plan).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_json_save(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_save");
    for size in [1000, 10000] {
        let table = make_table(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                table.to_json().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_binary_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_encode");
    for size in [1000, 10000] {
        let table = make_table(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                binary::encode(table).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_binary_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_decode");
    for size in [1000, 10000] {
        let table = make_table(size);
        let bytes = binary::encode(&table).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            b.iter(|| {
                binary::decode(bytes).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_compressed_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("compressed_encode");
    for size in [1000, 10000] {
        let table = make_table(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                compression::encode_compressed(table).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_query_scan,
    bench_query_with_index,
    bench_query_ast,
    bench_json_save,
    bench_binary_encode,
    bench_binary_decode,
    bench_compressed_encode,
);
criterion_main!(benches);
