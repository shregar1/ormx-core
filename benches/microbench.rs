use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ormx_core::logic::{
    rows_to_json_string, route_geo, route_hash, route_list, route_range_sorted,
    route_range_unsorted,
};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

fn make_shards(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("shard-{}", i)).collect()
}

fn make_range_map(n: usize) -> HashMap<i64, String> {
    let mut m = HashMap::new();
    for i in 0..n {
        m.insert(((i + 1) * 100) as i64, format!("shard-{}", i));
    }
    m
}

fn make_geo_map(n: usize) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for i in 0..n {
        m.insert(format!("region-{}", i), format!("shard-{}", i));
    }
    m
}

fn make_rows(num_rows: usize, num_cols: usize) -> Vec<Vec<(String, JsonValue)>> {
    (0..num_rows)
        .map(|row_i| {
            (0..num_cols)
                .map(|col_i| {
                    let name = format!("col_{}", col_i);
                    let val = match col_i % 4 {
                        0 => json!(row_i as i64),
                        1 => json!(format!("text-value-{}-{}", row_i, col_i)),
                        2 => json!((row_i as f64) * 1.5),
                        _ => json!(row_i % 2 == 0),
                    };
                    (name, val)
                })
                .collect()
        })
        .collect()
}

fn bench_route_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_hash");
    for &n in &[4usize, 16, 64] {
        let shards = make_shards(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &shards, |b, shards| {
            b.iter(|| route_hash(black_box("user-12345"), black_box(shards)));
        });
    }
    group.finish();
}

fn bench_route_geo(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_geo");
    for &n in &[4usize, 64, 1024] {
        let map = make_geo_map(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &map, |b, map| {
            b.iter(|| route_geo(black_box("region-3"), black_box(map)));
        });
    }
    group.finish();
}

fn bench_route_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_list");
    let mut map = HashMap::new();
    for i in 0..64 {
        map.insert(format!("key-{}", i), format!("shard-{}", i));
    }
    group.bench_function("64_entries", |b| {
        b.iter(|| route_list(black_box("key-30"), black_box(&map)));
    });
    group.finish();
}

fn bench_route_range_unsorted_vs_sorted(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_range");
    for &n in &[10usize, 100, 1000] {
        let map = make_range_map(n);
        let mut sorted_keys: Vec<i64> = map.keys().copied().collect();
        sorted_keys.sort();
        let probe = format!("{}", (n as i64 / 2) * 100 + 50);

        group.bench_with_input(
            BenchmarkId::new("unsorted_current", n),
            &(probe.clone(), map.clone()),
            |b, (v, m)| b.iter(|| route_range_unsorted(black_box(v), black_box(m))),
        );

        group.bench_with_input(
            BenchmarkId::new("sorted_optimized", n),
            &(probe, sorted_keys, map),
            |b, (v, keys, m)| b.iter(|| route_range_sorted(black_box(v), black_box(keys), black_box(m))),
        );
    }
    group.finish();
}

fn bench_rows_to_json_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("rows_to_json_string");
    let num_cols = 10;
    for &num_rows in &[10usize, 100, 1000, 10_000] {
        let rows = make_rows(num_rows, num_cols);
        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &rows,
            |b, rows| b.iter(|| rows_to_json_string(black_box(rows.clone()))),
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_route_hash,
    bench_route_geo,
    bench_route_list,
    bench_route_range_unsorted_vs_sorted,
    bench_rows_to_json_string,
);
criterion_main!(benches);
