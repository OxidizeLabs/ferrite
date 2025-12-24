#![allow(clippy::all, warnings)]

fn main() {}

// use criterion::{black_box, criterion_group, criterion_main, AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, SamplingMode, Throughput};
// use ferrite::storage::disk::async_disk::cache::fifo::FIFOCache;
// use ferrite::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};
//
// /// Comprehensive FIFO Cache Complexity Benchmarks
// ///
// /// These benchmarks measure the time and space complexity characteristics
// /// that were previously embedded as "tests" in the main codebase.
//
//
// fn generate_cache_sizes() -> Vec<usize> {
//     let mut sizes = vec![];
//
//     // Small range: every 500
//     for i in (100..=2000).step_by(500) {
//         sizes.push(i);
//     }
//
//     // Medium range: every 5000
//     for i in (5000..=50000).step_by(5000) {
//         sizes.push(i);
//     }
//
//     // Large range: exponential
//     for exp in [100000, 250000, 500000, 1000000] {
//         sizes.push(exp);
//     }
//
//     sizes
// }
//
//
//
//
// // =================================================================================
// // TIME COMPLEXITY BENCHMARKS
// // =================================================================================
//
// fn benchmark_insert_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_insert_complexity");
//     group.measurement_time(std::time::Duration::from_secs(17));
//     group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
//
//     let test_cache_sizes = generate_cache_sizes();
//
//     // Test O(1) insert complexity across different cache sizes
//     for cache_size in test_cache_sizes {
//         group.bench_with_input(
//             BenchmarkId::new("insert_operations", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 b.iter_batched(
//                     || {
//                         let mut cache = FIFOCache::new(cache_size);
//                         // Pre-fill to capacity-1 to avoid eviction effects
//                         for i in 0..(cache_size - 1) {
//                             cache.insert(i, i);
//                         }
//                         cache
//                     },
//                     |mut cache| {
//                         // Benchmark the insert operation - should be O(1)
//                         let key = fastrand::u64(0..1000000) as usize;
//                         let value = fastrand::u64(0..1000000) as usize;
//                         cache.insert(key, value);
//                         black_box(cache)
//                     },
//                     BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_get_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_get_complexity");
//     group.measurement_time(std::time::Duration::from_secs(10));
//
//     let test_cache_sizes = generate_cache_sizes();
//
//     // Test O(1) get complexity - HashMap lookups should be constant time
//     for cache_size in test_cache_sizes {
//         group.bench_with_input(
//             BenchmarkId::new("get_operations", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 // Setup: Create cache filled with data
//                 let mut cache = FIFOCache::new(cache_size);
//                 for i in 0..cache_size {
//                     cache.insert(format!("key_{}", i), format!("value_{}", i));
//                 }
//
//                 b.iter(|| {
//                     // Random key lookup - should be O(1)
//                     let key_idx = fastrand::usize(0..cache_size);
//                     let key = format!("key_{}", key_idx);
//                     // Just measure the operation time, not return the reference
//                     let result = cache.get(&key).is_some();
//                     black_box(result)
//                 });
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_contains_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_contains_complexity");
//     group.measurement_time(std::time::Duration::from_secs(8));
//
//     let test_cache_sizes = generate_cache_sizes();
//
//     // Test O(1) contains complexity - HashMap contains_key should be constant time
//     for cache_size in test_cache_sizes {
//         group.bench_with_input(
//             BenchmarkId::new("contains_operations", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 let mut cache = FIFOCache::new(cache_size);
//                 for i in 0..cache_size {
//                     cache.insert(format!("key_{}", i), format!("value_{}", i));
//                 }
//
//                 b.iter(|| {
//                     let key_idx = fastrand::usize(0..cache_size);
//                     let key = format!("key_{}", key_idx);
//                     black_box(cache.contains(&key))
//                 });
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_eviction_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_eviction_complexity");
//     group.measurement_time(std::time::Duration::from_secs(15));
//     group.sample_size(30); // Fewer samples for slower operations
//
//     // Test O(n) worst-case eviction when stale entries are present
//     for cache_size in [100, 500, 1000] { // Smaller sizes due to O(n) nature
//         group.bench_with_input(
//             BenchmarkId::new("eviction_worst_case", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 b.iter_batched(
//                     || {
//                         let mut cache = FIFOCache::new(cache_size);
//                         // Fill to capacity
//                         for i in 0..cache_size {
//                             cache.insert(format!("key_{}", i), format!("value_{}", i));
//                         }
//
//                         // Create stale entries (worst case scenario)
//                         // Note: We simulate stale entries by inserting then removing keys
//                         // This creates a scenario where the insertion order has more entries
//                         // than the actual cache HashMap, which is similar to stale entries
//                         let stale_count = cache_size / 2;
//                         for i in 0..stale_count {
//                             cache.insert(format!("stale_key_{}", i), format!("stale_value_{}", i));
//                         }
//                         // Now evict some items naturally to create the stale entry effect
//                         for i in 0..stale_count {
//                             cache.insert(format!("replace_key_{}", i), format!("replace_value_{}", i));
//                         }
//
//                         // Refill to capacity to ensure eviction will be triggered
//                         let fill_needed = cache_size - cache.len();
//                         for i in 0..fill_needed {
//                             cache.insert(format!("fill_{}", i), format!("fill_val_{}", i));
//                         }
//
//                         cache
//                     },
//                     |mut cache| {
//                         // This should trigger eviction that must skip stale entries - O(n) worst case
//                         let key = format!("evict_trigger_{}", fastrand::u64(0..1000000));
//                         let value = format!("evict_value_{}", fastrand::u64(0..1000000));
//                         cache.insert(key, value);
//                         black_box(cache)
//                     },
//                     criterion::BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_age_rank_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_age_rank_complexity");
//     group.measurement_time(std::time::Duration::from_secs(12));
//
//     // Test O(n) age_rank complexity - needs to traverse insertion order
//     for cache_size in [100, 500, 1000, 2000] {
//         group.bench_with_input(
//             BenchmarkId::new("age_rank_operations", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 let mut cache = FIFOCache::new(cache_size);
//                 for i in 0..cache_size {
//                     cache.insert(format!("key_{}", i), format!("value_{}", i));
//                 }
//
//                 b.iter(|| {
//                     // Test age_rank for keys at different positions
//                     let key_idx = fastrand::usize(0..cache_size);
//                     let key = format!("key_{}", key_idx);
//                     black_box(cache.age_rank(&key))
//                 });
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_clear_time_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_clear_complexity");
//     group.measurement_time(std::time::Duration::from_secs(8));
//
//     // Test clear complexity across different cache sizes
//     for cache_size in [100, 1000, 10000, 50000] {
//         group.bench_with_input(
//             BenchmarkId::new("clear_operations", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 b.iter_batched(
//                     || {
//                         let mut cache = FIFOCache::new(cache_size);
//                         // Fill cache with data
//                         for i in 0..cache_size {
//                             cache.insert(format!("key_{}", i), format!("large_value_{}", "x".repeat(100)));
//                         }
//                         cache
//                     },
//                     |mut cache| {
//                         cache.clear();
//                         black_box(cache)
//                     },
//                     criterion::BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// // =================================================================================
// // SPACE COMPLEXITY BENCHMARKS
// // =================================================================================
//
// fn benchmark_memory_usage_patterns(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_memory_usage");
//     group.measurement_time(std::time::Duration::from_secs(9));
//
//     // Benchmark memory allocation patterns across different cache sizes
//     for cache_size in [100, 500, 1000, 2000, 5000] {
//         group.bench_with_input(
//             BenchmarkId::new("memory_allocation", cache_size),
//             &cache_size,
//             |b, &cache_size| {
//                 b.iter_batched(
//                     || {
//                         // Create empty cache
//                         FIFOCache::<String, String>::new(cache_size)
//                     },
//                     |mut cache| {
//                         // Fill cache to capacity and measure allocation behavior
//                         for i in 0..cache_size {
//                             cache.insert(
//                                 format!("memory_key_{:06}", i),
//                                 format!("memory_value_{:06}_{}", i, "x".repeat(50))
//                             );
//                         }
//                         black_box(cache)
//                     },
//                     criterion::BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_memory_pressure_scenarios(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_memory_pressure");
//     group.measurement_time(std::time::Duration::from_secs(10));
//     group.sample_size(20);
//
//     // Test behavior under memory pressure with large values
//     for value_size in [100, 1000, 5000] { // bytes per value
//         group.bench_with_input(
//             BenchmarkId::new("large_values", value_size),
//             &value_size,
//             |b, &value_size| {
//                 b.iter_batched(
//                     || {
//                         FIFOCache::<String, String>::new(1000)
//                     },
//                     |mut cache| {
//                         let large_value = "x".repeat(value_size);
//                         // Fill cache with large values to create memory pressure
//                         for i in 0..1000 {
//                             cache.insert(
//                                 format!("large_key_{}", i),
//                                 format!("{}_{}", large_value, i)
//                             );
//                         }
//                         black_box(cache)
//                     },
//                     criterion::BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// // =================================================================================
// // PERFORMANCE BENCHMARKS
// // =================================================================================
//
// fn benchmark_realistic_workloads(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_realistic_workloads");
//     group.measurement_time(std::time::Duration::from_secs(28));
//
//     // Small cache performance
//     group.bench_function("small_cache_mixed_workload", |b| {
//         b.iter_batched(
//             || FIFOCache::new(100),
//             |mut cache| {
//                 let mut hits = 0;
//                 // Mixed workload: 33% inserts, 67% gets
//                 for i in 0..10000 {
//                     if i % 3 == 0 {
//                         cache.insert(
//                             format!("key_{}", i % 200),
//                             format!("value_{}", i)
//                         );
//                     } else {
//                         let key = format!("key_{}", i % 200);
//                         if cache.get(&key).is_some() {
//                             hits += 1;
//                         }
//                     }
//                 }
//                 black_box((cache, hits))
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     // Medium cache performance
//     group.bench_function("medium_cache_mixed_workload", |b| {
//         b.iter_batched(
//             || FIFOCache::new(1000),
//             |mut cache| {
//                 let mut hits = 0;
//                 // Complex workload: 40% inserts, 60% gets
//                 for i in 0..50000 {
//                     if i % 5 < 2 {
//                         cache.insert(
//                             format!("key_{}", i % 3000),
//                             format!("value_{}", i)
//                         );
//                     } else {
//                         let key = format!("key_{}", i % 3000);
//                         if cache.get(&key).is_some() {
//                             hits += 1;
//                         }
//                     }
//                 }
//                 black_box((cache, hits))
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     // Large cache performance
//     group.bench_function("large_cache_batch_workload", |b| {
//         b.iter_batched(
//             || FIFOCache::new(10000),
//             |mut cache| {
//                 let mut hits = 0;
//                 // Batch workload
//                 for batch in 0..100 {
//                     // Batch insert
//                     for i in 0..500 {
//                         let key = format!("batch_{}_{}", batch, i);
//                         cache.insert(key, format!("data_{}", batch * 500 + i));
//                     }
//
//                     // Batch lookup
//                     for i in 0..500 {
//                         let key = format!("batch_{}_{}", batch, i % 200);
//                         if cache.get(&key).is_some() {
//                             hits += 1;
//                         }
//                     }
//                 }
//                 black_box((cache, hits))
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     group.finish();
// }
//
// fn benchmark_access_patterns(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_access_patterns");
//
//     // Stabilize measurements
//     group.warm_up_time(std::time::Duration::from_secs(3));
//     group.measurement_time(std::time::Duration::from_secs(12));
//     group.sample_size(80);
//     group.sampling_mode(SamplingMode::Flat);
//
//     // Parameters
//     let cache_sizes = [512usize, 1_000, 5_000, 10_000];
//     let working_sets = [1_000usize, 5_000, 20_000];
//     let total_ops = 20_000usize;
//
//     // Ratios (controls how often we insert)
//     // insert_every = 3 -> ~33% inserts; = 4 -> ~25% inserts
//     let seq_insert_every = 3usize;
//     let rnd_insert_every = 4usize;
//
//     // Pre-generate value pool to avoid fmt in hot path
//     // Values are short to reduce allocation noise
//     fn value_for(i: usize) -> String {
//         // Keep it tiny and cheap to clone
//         format!("v{i}")
//     }
//
//     // Sequential access benchmarks
//     for &cache_size in &cache_sizes {
//         for &working_set in &working_sets {
//             // Same op count for comparability
//             group.throughput(Throughput::Elements(total_ops as u64));
//
//             let bench_id = BenchmarkId::new(
//                 "sequential",
//                 format!("c{}_ws{}_ins1/{}", cache_size, working_set, seq_insert_every),
//             );
//
//             group.bench_with_input(bench_id, &(cache_size, working_set), |b, &(cache_size, working_set)| {
//                 // Precompute workload outside timing
//                 let keys = pregen_keys("seq_", working_set);
//                 let ops = build_sequential_ops(total_ops, working_set, seq_insert_every);
//
//                 b.iter_batched(
//                     || {
//                         // Fresh cache per measurement
//                         let mut cache = FIFOCache::new(cache_size);
//                         // Prefill to ~80% to simulate realistic hit/miss behavior
//                         let prefill = (cache_size as f64 * 0.8) as usize;
//                         for i in 0..prefill.min(working_set) {
//                             cache.insert(keys[i].clone(), value_for(i));
//                         }
//                         (cache, keys.clone(), ops.clone())
//                     },
//                     |(mut cache, keys, ops)| {
//                         let mut hits = 0usize;
//                         for (i, op) in ops.iter().enumerate() {
//                             match *op {
//                                 Op::Insert(kidx) => {
//                                     // Cheap values; cloning Strings is still measured but uniform
//                                     cache.insert(keys[kidx].clone(), value_for(i));
//                                 }
//                                 Op::Get(kidx) => {
//                                     if cache.get(&keys[kidx]).is_some() {
//                                         hits += 1;
//                                     }
//                                 }
//                             }
//                         }
//                         black_box((hits, cache))
//                     },
//                     BatchSize::SmallInput,
//                 );
//             });
//         }
//     }
//
//     // Random access benchmarks
//     for &cache_size in &cache_sizes {
//         for &working_set in &working_sets {
//             group.throughput(Throughput::Elements(total_ops as u64));
//
//             let bench_id = BenchmarkId::new(
//                 "random",
//                 format!("c{}_ws{}_ins1/{}", cache_size, working_set, rnd_insert_every),
//             );
//
//             group.bench_with_input(bench_id, &(cache_size, working_set), |b, &(cache_size, working_set)| {
//                 // Precompute workload outside timing
//                 let keys = pregen_keys("rnd_", working_set);
//                 let ops = build_random_ops(total_ops, working_set, rnd_insert_every);
//
//                 b.iter_batched(
//                     || {
//                         // Fresh cache per measurement
//                         let mut cache = FIFOCache::new(cache_size);
//                         // Prefill to ~60% for random to bias toward more misses
//                         let prefill = (cache_size as f64 * 0.6) as usize;
//                         for i in 0..prefill.min(working_set) {
//                             cache.insert(keys[i].clone(), value_for(i));
//                         }
//                         (cache, keys.clone(), ops.clone())
//                     },
//                     |(mut cache, keys, ops)| {
//                         let mut hits = 0usize;
//                         for (i, op) in ops.iter().enumerate() {
//                             match *op {
//                                 Op::Insert(kidx) => {
//                                     cache.insert(keys[kidx].clone(), value_for(i));
//                                 }
//                                 Op::Get(kidx) => {
//                                     if cache.get(&keys[kidx]).is_some() {
//                                         hits += 1;
//                                     }
//                                 }
//                             }
//                         }
//                         black_box((hits, cache))
//                     },
//                     BatchSize::SmallInput,
//                 );
//             });
//         }
//     }
//
//     group.finish();
// }
//
// fn benchmark_eviction_scenarios(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_eviction_scenarios");
//     group.measurement_time(std::time::Duration::from_secs(17));
//
//     // Heavy eviction - small cache, large key space
//     group.bench_function("heavy_eviction", |b| {
//         b.iter_batched(
//             || FIFOCache::new(500),
//             |mut cache| {
//                 // 10x more keys than capacity - forces constant eviction
//                 for i in 0..15000 {
//                     if i % 5 == 0 {
//                         // Lookup (mostly misses due to evictions)
//                         let key = format!("evict_{}", i % 5000);
//                         let result = cache.get(&key).is_some();
//                         black_box(result);
//                     } else {
//                         // Insert (causes evictions)
//                         cache.insert(
//                             format!("evict_{}", i % 5000),
//                             format!("data_{}", i)
//                         );
//                     }
//                 }
//                 black_box(cache)
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     // Light eviction - good locality
//     group.bench_function("light_eviction", |b| {
//         b.iter_batched(
//             || {
//                 let mut cache = FIFOCache::new(2000);
//                 // Pre-populate with working set
//                 for i in 0..1500 {
//                     cache.insert(format!("stable_{}", i), format!("data_{}", i));
//                 }
//                 cache
//             },
//             |mut cache| {
//                 // Working set fits mostly in cache
//                 for i in 0..25000 {
//                     let working_set_idx = i % 1500;
//                     match i % 16 {
//                         0..=9 => {
//                             // 62.5% reads (high hit rate)
//                             let key = format!("stable_{}", working_set_idx);
//                             let result = cache.get(&key).is_some();
//                             black_box(result);
//                         }
//                         10..=13 => {
//                             // 25% updates
//                             let key = format!("stable_{}", working_set_idx);
//                             cache.insert(key, format!("updated_{}", i));
//                         }
//                         _ => {
//                             // 12.5% new inserts
//                             cache.insert(format!("new_{}", i), format!("new_data_{}", i));
//                         }
//                     }
//                 }
//                 black_box(cache)
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     group.finish();
// }
//
// // =================================================================================
// // FIFO-SPECIFIC OPERATIONS BENCHMARKS
// // =================================================================================
//
// fn benchmark_fifo_operations(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_specific_operations");
//     group.measurement_time(std::time::Duration::from_secs(8));
//
//     // peek_oldest performance
//     group.bench_function("peek_oldest", |b| {
//         let mut cache = FIFOCache::new(1000);
//         for i in 0..1000 {
//             cache.insert(format!("key_{}", i), format!("value_{}", i));
//         }
//
//         b.iter(|| {
//             black_box(cache.peek_oldest())
//         });
//     });
//
//     // pop_oldest performance
//     group.bench_function("pop_oldest", |b| {
//         b.iter_batched(
//             || {
//                 let mut cache = FIFOCache::new(1000);
//                 for i in 0..1000 {
//                     cache.insert(format!("key_{}", i), format!("value_{}", i));
//                 }
//                 cache
//             },
//             |mut cache| {
//                 black_box(cache.pop_oldest())
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     // pop_oldest_batch performance
//     for batch_size in [1, 5, 10, 25, 50] {
//         group.bench_with_input(
//             BenchmarkId::new("pop_oldest_batch", batch_size),
//             &batch_size,
//             |b, &batch_size| {
//                 b.iter_batched(
//                     || {
//                         let mut cache = FIFOCache::new(1000);
//                         for i in 0..1000 {
//                             cache.insert(format!("key_{}", i), format!("value_{}", i));
//                         }
//                         cache
//                     },
//                     |mut cache| {
//                         black_box(cache.pop_oldest_batch(batch_size))
//                     },
//                     criterion::BatchSize::SmallInput
//                 );
//             }
//         );
//     }
//
//     group.finish();
// }
//
// // =================================================================================
// // UTILITY FUNCTIONS
// // =================================================================================
//
// /// Simple deterministic random for benchmarks (thread-local state)
// mod fastrand {
//     use std::cell::Cell;
//
//     thread_local! {
//         static RNG: Cell<u64> = Cell::new(0x4d595df4d0f33173);
//     }
//
//     pub fn u64(range: std::ops::Range<u64>) -> u64 {
//         RNG.with(|rng| {
//             let mut state = rng.get();
//             state ^= state >> 12;
//             state ^= state << 25;
//             state ^= state >> 27;
//             rng.set(state);
//
//             let len = range.end - range.start;
//             range.start + (state % len)
//         })
//     }
//
//     pub fn usize(range: std::ops::Range<usize>) -> usize {
//         u64(range.start as u64..range.end as u64) as usize
//     }
// }
//
// #[derive(Clone, Copy)]
// enum Op {
//     Insert(usize),
//     Get(usize),
// }
//
// fn pregen_keys(prefix: &str, count: usize) -> Vec<String> {
//     // Short, cache-friendly keys
//     (0..count).map(|i| format!("{prefix}{i}")).collect()
// }
//
// fn build_sequential_ops(total_ops: usize, working_set: usize, insert_every: usize) -> Vec<Op> {
//     let mut ops = Vec::with_capacity(total_ops);
//     for i in 0..total_ops {
//         let idx = i % working_set;
//         if insert_every != 0 && i % insert_every == 0 {
//             ops.push(Op::Insert(idx));
//         } else {
//             ops.push(Op::Get(idx));
//         }
//     }
//     ops
// }
//
// fn build_random_ops(total_ops: usize, working_set: usize, insert_every: usize) -> Vec<Op> {
//     let mut ops = Vec::with_capacity(total_ops);
//     for i in 0..total_ops {
//         let idx = fastrand::usize(0..working_set);
//         if insert_every != 0 && i % insert_every == 0 {
//             ops.push(Op::Insert(idx));
//         } else {
//             ops.push(Op::Get(idx));
//         }
//     }
//     ops
// }
//
//
// // =================================================================================
// // CRITERION GROUPS
// // =================================================================================
//
// criterion_group!(
//     time_complexity_benches,
//     benchmark_insert_time_complexity,
//     // benchmark_get_time_complexity,
//     // benchmark_contains_time_complexity,
//     // benchmark_eviction_time_complexity,
//     // benchmark_age_rank_time_complexity,
//     // benchmark_clear_time_complexity
// );
//
// // criterion_group!(
// //     space_complexity_benches,
// //     benchmark_memory_usage_patterns,
// //     benchmark_memory_pressure_scenarios
// // );
// //
// criterion_group!(
//     performance_benches,
//     // benchmark_realistic_workloads,
//     benchmark_access_patterns,
//     // benchmark_eviction_scenarios
// );
// //
// // criterion_group!(
// //     fifo_operations_benches,
// //     benchmark_fifo_operations
// // );
// //
// criterion_main!(
//     // time_complexity_benches,
//     // space_complexity_benches,
//     performance_benches,
//     // fifo_operations_benches
// );
