#![allow(clippy::all, warnings)]

fn main() {}

// use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
// use tkdb::storage::disk::async_disk::cache::fifo::FIFOCache;
// use tkdb::storage::disk::async_disk::cache::cache_traits::CoreCache;
//
// fn benchmark_fifo_cache_operations(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_cache_operations");
//
//     // Set measurement time for more stable results
//     group.measurement_time(std::time::Duration::from_secs(10));
//     group.sample_size(100);
//
//     // Test O(1) get operations across different cache sizes
//     for size in [100, 500, 1000, 5000, 10000] {
//         group.bench_with_input(
//             BenchmarkId::new("get_operation", size),
//             &size,
//             |b, &size| {
//                 // Setup
//                 let mut cache = FIFOCache::new(size);
//
//                 // Fill cache to 80% capacity
//                 let fill_count = (size as f64 * 0.8) as usize;
//                 for i in 0..fill_count {
//                     cache.insert(format!("key_{}", i), format!("value_{}", i));
//                 }
//
//                 // Pre-create the key outside the benchmark loop
//                 let test_key = format!("key_{}", 42);
//
//                 // Benchmark get operation - should be O(1)
//                 b.iter(|| {
//                     // Just check if the operation happens without returning reference
//                     let result = cache.get(&test_key);
//                     black_box(result.is_some())
//                 });
//             }
//         );
//     }
//
//     group.finish();
// }
//
// fn benchmark_fifo_eviction_complexity(c: &mut Criterion) {
//     let mut group = c.benchmark_group("fifo_eviction_worst_case");
//
//     // Longer measurement time for eviction benchmarks (more variability)
//     group.measurement_time(std::time::Duration::from_secs(15));
//     group.sample_size(50);
//
//     // Test O(n) worst-case eviction with stale entries
//     for size in [100, 500, 1000] {  // Smaller range due to O(n) nature
//         group.bench_with_input(
//             BenchmarkId::new("eviction_with_stale_entries", size),
//             &size,
//             |b, &size| {
//                 b.iter_batched(
//                     // Setup for each iteration
//                     || {
//                         let mut cache = FIFOCache::new(size);
//
//                         // Fill cache to capacity
//                         for i in 0..size {
//                             cache.insert(format!("key_{}", i), format!("value_{}", i));
//                         }
//
//                         // Note: For now we'll test normal eviction since remove_from_cache_only
//                         // is a test-only method. In a real benchmark, you'd want to expose
//                         // methods to create worst-case scenarios.
//
//                         // Fill to full capacity for eviction testing
//                         for i in size..size*2 {
//                             cache.insert(format!("extra_{}", i), format!("extra_value_{}", i));
//                         }
//
//                         cache
//                     },
//                     // Benchmark the eviction operation
//                     |mut cache| {
//                         // This should trigger eviction through stale entries - O(n) worst case
//                         cache.insert(
//                             format!("evict_{}", fastrand::u64(0..1000000)),
//                             format!("evict_value_{}", fastrand::u64(0..1000000))
//                         );
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
// fn benchmark_cache_comparison(c: &mut Criterion) {
//     let mut group = c.benchmark_group("cache_comparison");
//     group.measurement_time(std::time::Duration::from_secs(5));
//
//     let size = 1000;
//
//     // Compare different cache operations
//     group.bench_function("fifo_insert", |b| {
//         b.iter_batched(
//             || FIFOCache::new(size),
//             |mut cache| {
//                 for i in 0..100 {
//                     cache.insert(black_box(format!("key_{}", i)), black_box(format!("value_{}", i)));
//                 }
//                 black_box(cache)
//             },
//             criterion::BatchSize::SmallInput
//         );
//     });
//
//     group.bench_function("fifo_get_hit", |b| {
//         let mut cache = FIFOCache::new(size);
//         for i in 0..100 {
//             cache.insert(format!("key_{}", i), format!("value_{}", i));
//         }
//
//         let test_key = "key_50".to_string();
//         b.iter(|| {
//             let result = cache.get(&test_key);
//             black_box(result.is_some())
//         });
//     });
//
//     group.bench_function("fifo_get_miss", |b| {
//         let mut cache = FIFOCache::new(size);
//         for i in 0..100 {
//             cache.insert(format!("key_{}", i), format!("value_{}", i));
//         }
//
//         let test_key = "nonexistent_key".to_string();
//         b.iter(|| {
//             let result = cache.get(&test_key);
//             black_box(result.is_some())
//         });
//     });
//
//     group.finish();
// }
//
// // Simple deterministic random for benchmarks
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
// }
//
// criterion_group!(
//     cache_benches,
//     benchmark_fifo_cache_operations,
//     benchmark_fifo_eviction_complexity,
//     benchmark_cache_comparison
// );
//
// criterion_main!(cache_benches);
//
