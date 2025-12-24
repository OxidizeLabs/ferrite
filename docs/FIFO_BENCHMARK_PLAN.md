## Goal

Define a rigorous, reproducible benchmarking plan for `src/storage/disk/async_disk/cache/fifo.rs` using Criterion. The plan measures time and space complexity across operations, input sizes, and workload characteristics, and produces clear, actionable reports.

## Scope

- In-module microbenchmarks for `FIFOCache<K, V>` core operations
- Complexity studies by varying capacity, workload size, and hit/miss patterns
- Stale-entry scenarios and their impact on performance
- Space usage estimation and growth behavior
- Reuse and extend existing benches for consistency and comparability

References for style and ideas:
- `benches/fifo_cache_benchmarking.rs`
- `benches/fifo_complexity_benchmarks.rs`
- `benches/cache_benchmarks.rs`
- `benches/time_complexity_demo.rs`

## Environment & Tooling

- Use Criterion with HTML reports
  - Cargo dependency (verify or add): `criterion = { version = "0.5", features = ["html_reports"] }`
- Build profile: `--release`
- Suggested environment for reproducibility
  - macOS: close background apps; keep power settings stable
  - Pin CPU performance as much as possible (no heavy background tasks)
  - Run multiple samples; prefer longer measurement time for stability
- Version stamp the run (Rust toolchain, commit SHA, OS)

### Criterion configuration
- Warmup: 3–5 s (per group)
- Measurement time: 10–20 s (microbenches) and 20–60 s (complexity scaling)
- Sample size: 50–100 (adjust based on variance)
- Configure throughput where applicable (ops/sec)
- Enable HTML reports

## Metrics

- Time per operation and throughput (ops/sec)
- Scaling trends vs:
  - N = number of operations performed in a benchmark iteration
  - C = cache capacity
  - H = hit ratio (0–100%)
  - Distribution: sequential, uniform random, skewed (Zipf-like)
- Space usage:
  - `cache.len()` vs `capacity`
  - `insertion_order_len()` vs `len()` (stale ratio)
  - Estimated memory bytes: `sizeof(Arc<K>) + sizeof(Arc<V>)` times element counts plus HashMap/VecDeque overhead (reported as estimate)

## Benchmark Matrix

### Core operations (microbenchmarks)
- insert(key, value)
- get(&key)
- contains(&key)
- peek_oldest()
- pop_oldest()
- pop_oldest_batch(batch_size ∈ {1, 2, 4, 8, 16, 32})
- clear()
- age_rank(&key)

Parameters per operation:
- C ∈ {1, 16, 64, 256, 1024, 4096}
- Key distribution: sequential, uniform random; optionally skewed
- H ∈ {0, 25, 50, 75, 100} for read operations

### Workload mixes
- Read-heavy: 90% get/contains, 10% insert
- Write-heavy: 90% insert, 10% reads
- Mixed: 60% reads, 40% writes
- Batch-eviction stress: periodic `pop_oldest_batch(k)` with k growing

### Stale entries scenarios
- Use `remove_from_cache_only` (test-only helper) to synthesize stale entries
  - Patterns: isolated stale, runs of 2–8 stale at front, widespread stale
  - Measure impact on `insert` (eviction path), `pop_oldest`, `peek_oldest`, and `age_rank`
  - Report stale ratio = `insertion_order_len / len` and its correlation with slowdown

### Edge cases
- C = 0: all inserts drop; verify behavior cost
- C = 1: FIFO degeneracy
- Near-capacity churn: alternating insert/pop to keep at C

## Complexity Experiments

- Time complexity checks via scaling curves
  - Expected: insert/get/contains/eviction ≈ O(1) average
  - `age_rank` and worst-case stale-scan paths trend toward O(n)
  - Method: vary N and C; collect mean time/op; plot log-log and check slopes
- Space complexity
  - Vary C and fill with unique keys; record `len`, `insertion_order_len`, and estimated bytes
  - Stale growth: create stale entries; track inflation of `insertion_order_len`

## Implementation Plan

### Files
- Keep and extend existing:
  - `benches/fifo_cache_benchmarking.rs`
  - `benches/fifo_complexity_benchmarks.rs`
- Optional new benches (if separation helps):
  - `benches/fifo_micro.rs` (focused per-op microbenches)
  - `benches/fifo_space.rs` (space usage instrumentation)

### Benchmark structure (suggested patterns)
- Parameterized groups with `BenchmarkId` over C, H, distribution, batch_size
- Use `b.iter_batched` for setup/teardown where needed
- Pre-generate key/value datasets to avoid measuring RNG/alloc noise in inner loops
- Use `Throughput::Elements(n_ops)` to annotate throughput
- For space: compute estimates after filling cache; optionally export CSV row

Example skeleton:
```rust
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput, BatchSize};
use ferrite::storage::disk::async_disk::cache::fifo::FIFOCache; // adjust path if needed

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_insert");
    for &cap in &[16usize, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            let keys: Vec<String> = (0..10_000).map(|i| format!("k{}", i)).collect();
            let vals: Vec<String> = (0..10_000).map(|i| format!("v{}", i)).collect();
            b.iter_batched(
                || (FIFOCache::new(cap), 0usize),
                |(mut cache, mut idx)| {
                    let k = &keys[idx % keys.len()];
                    let v = &vals[idx % vals.len()];
                    cache.insert(k.clone(), v.clone());
                    idx += 1;
                    black_box(cache);
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(fifo, bench_insert);
criterion_main!(fifo);
```

### Data generation and distributions
- Sequential: i = 0..N
- Uniform: pre-generate with `rand` over key space
- Skewed: implement simple Zipf sampler or reuse distribution from existing benches
- Ensure datasets are reused across iterations to reduce noise

### Space estimation helper
- Compute: `estimated_bytes ≈ len * (size_of::<Arc<K>>() + size_of::<Arc<V>>()) + overhead(HashMap, VecDeque)`
- Record: `(cap, len, insertion_order_len, estimated_bytes)`
- Export optional CSV (see Reporting)

## Reporting

- Criterion HTML reports (default at `target/criterion/report/index.html`)
- Optional CSV/JSON export per group (append-only) for trend analysis:
  - Path: `test_results/fifo_benchmarks_YYYYMMDD_HHMMSS.csv`
  - Columns: group, cap, n_ops, hit_ratio, distribution, batch_size, mean_ns, stddev_ns, throughput_ops_s, len, insertion_order_len, estimated_bytes
- Summarize complexity by fitting slopes (manual inspection or simple log-log delta)

## How to Run

- All benches:
  - `cargo bench | cat`
- Specific benches:
  - `cargo bench --bench fifo_cache_benchmarking | cat`
  - `cargo bench --bench fifo_complexity_benchmarks | cat`
  - If new files are added: `cargo bench --bench fifo_micro` etc.
- Open HTML report: `open target/criterion/report/index.html`

## Acceptance Criteria

- Reproducible runs with low variance (Coefficient of Variation < ~5% for core ops)
- Reports demonstrate expected trends:
  - O(1)-like behavior for insert/get under normal conditions
  - Degradation with high stale ratios for eviction/age_rank consistent with linear scan
- Space analysis shows linear growth with capacity and quantifies stale overhead
- Bench sources are clean, parameterized, and documented

## Next Steps

1. Verify Criterion dependency and HTML feature in `Cargo.toml`
2. Extract/centralize common dataset generators in benches to reduce duplication
3. Implement microbench groups for each operation
4. Add scaling studies (vary C, N, H, distributions)
5. Add stale-entry test modes
6. Add optional CSV export for post-processing
7. Document findings in `docs/BENCHMARKING.md` with links to Criterion HTML output

## Notes

- `FIFOCache` is single-threaded by design; concurrency throughput is out of scope here (covered by tests). Keep benches single-threaded to isolate algorithmic costs.
- Avoid measuring allocation/random generation inside tight loops—move them to setup.
- For large runs, ensure sufficient wall-clock measurement time to reduce noise.
