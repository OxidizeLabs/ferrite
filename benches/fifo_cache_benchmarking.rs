#![allow(clippy::all)]

use criterion::{
    AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, SamplingMode, Throughput,
    criterion_group, criterion_main,
};
use ferrite::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};
use ferrite::storage::disk::async_disk::cache::fifo::FIFOCache;
use std::fs::{OpenOptions, create_dir_all};
use std::hash::Hash;
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

// =================================================================================
// TIME COMPLEXITY BENCHMARKS
// =================================================================================

fn generate_cache_sizes() -> Vec<usize> {
    let mut sizes = vec![];

    // Small range: every 500
    for i in (100..=2000).step_by(500) {
        sizes.push(i);
    }

    // Medium range: every 5000
    for i in (5000..=50000).step_by(5000) {
        sizes.push(i);
    }

    // Large range: exponential
    for exp in [100000, 250000, 500000, 1000000] {
        sizes.push(exp);
    }

    sizes
}

fn benchmark_insert_time_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_insert_complexity");
    group.measurement_time(Duration::from_secs(17));
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    let test_cache_sizes = generate_cache_sizes();

    // Test O(1) insert complexity across different cache sizes
    for cache_size in test_cache_sizes {
        group.bench_with_input(
            BenchmarkId::new("insert_operations", cache_size),
            &cache_size,
            |b, &cache_size| {
                b.iter_batched(
                    || {
                        let mut cache = FIFOCache::new(cache_size);
                        // Pre-fill to capacity-1 to avoid eviction effects
                        for i in 0..(cache_size - 1) {
                            cache.insert(i, i);
                        }
                        cache
                    },
                    |mut cache| {
                        // Benchmark the insert operation - should be O(1)
                        let key = fastrand::u64(0..1_000_000) as usize;
                        let value = fastrand::u64(0..1_000_000) as usize;
                        cache.insert(key, value);
                        black_box(cache)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// =================================================================================
// PERFORMANCE BENCHMARKS (REFINED WITH CALIBRATED PER-SAMPLE DURATION)
// =================================================================================

fn benchmark_access_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_access_patterns");

    // Stabilize measurements and use flat sampling
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(70);
    group.sampling_mode(SamplingMode::Flat);

    // Parameters
    let cache_sizes = [512usize, 1_000, 5_000, 10_000];
    let working_sets = [1_000usize, 5_000, 20_000];
    let total_ops = 20_000usize;

    // Insert ratios (insert_every = 3 -> ~33% inserts; = 4 -> ~25% inserts)
    let seq_insert_every = 3usize;
    let rnd_insert_every = 4usize;

    // Target per-sample duration (τ). Aim to average over several cycles of typical OS jitter.
    let target_sample = Duration::from_millis(8);

    // Sequential access benchmarks (calibrated)
    for &cache_size in &cache_sizes {
        for &working_set in &working_sets {
            let bench_id = BenchmarkId::new(
                "sequential",
                format!(
                    "c{}_ws{}_ins1/{}",
                    cache_size, working_set, seq_insert_every
                ),
            );

            // Precompute workload outside timing: keys, values, and op sequence
            let keys = pregen_keys("seq_", working_set);
            let values = pregen_values(total_ops.max(working_set));
            let ops = build_sequential_ops(total_ops, working_set, seq_insert_every);

            // Calibrate repeat count using a temporary cache so each measured sample aggregates
            // enough work to reach ~target_sample duration.
            let mut tmp_cache = FIFOCache::new(cache_size);
            prefill_cache(
                &mut tmp_cache,
                &keys,
                &values,
                (cache_size as f64 * 0.8) as usize,
            );
            let mut one_pass_tmp = || {
                run_ops_once(&mut tmp_cache, &keys, &ops, &values);
            };
            let repeat = calibrate_repeat(&mut one_pass_tmp, target_sample, 1_000_000);
            let repeat = repeat.max(1);

            // Report per-element throughput: ops per iteration = total_ops * repeat
            group.throughput(Throughput::Elements((total_ops as u64) * repeat));

            group.bench_with_input(
                bench_id,
                &(cache_size, working_set),
                |b, &(cache_size, _)| {
                    b.iter_batched(
                        || {
                            // Fresh cache per sample, setup not timed
                            let mut cache = FIFOCache::new(cache_size);
                            prefill_cache(
                                &mut cache,
                                &keys,
                                &values,
                                (cache_size as f64 * 0.8) as usize,
                            );
                            cache
                        },
                        |mut cache| {
                            // Jitter repeat slightly to avoid coherent sampling against periodic noise
                            let mut hits_total = 0usize;
                            for _k in 0..repeat_jitter_count(repeat, 5) {
                                hits_total += run_ops_once(&mut cache, &keys, &ops, &values);
                                // Optional: if state drift is a concern, you can occasionally refresh
                                // a small portion of the cache here, but this keeps it simple.
                            }
                            black_box((hits_total, cache));
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    // Random access benchmarks (calibrated)
    for &cache_size in &cache_sizes {
        for &working_set in &working_sets {
            let bench_id = BenchmarkId::new(
                "random",
                format!(
                    "c{}_ws{}_ins1/{}",
                    cache_size, working_set, rnd_insert_every
                ),
            );

            let keys = pregen_keys("rnd_", working_set);
            let values = pregen_values(total_ops.max(working_set));
            let ops = build_random_ops(total_ops, working_set, rnd_insert_every);

            let mut tmp_cache = FIFOCache::new(cache_size);
            prefill_cache(
                &mut tmp_cache,
                &keys,
                &values,
                (cache_size as f64 * 0.6) as usize,
            );
            let mut one_pass_tmp = || {
                run_ops_once(&mut tmp_cache, &keys, &ops, &values);
            };
            let repeat = calibrate_repeat(&mut one_pass_tmp, target_sample, 1_000_000);
            let repeat = repeat.max(1);

            group.throughput(Throughput::Elements((total_ops as u64) * repeat));

            group.bench_with_input(
                bench_id,
                &(cache_size, working_set),
                |b, &(cache_size, _)| {
                    b.iter_batched(
                        || {
                            let mut cache = FIFOCache::new(cache_size);
                            prefill_cache(
                                &mut cache,
                                &keys,
                                &values,
                                (cache_size as f64 * 0.6) as usize,
                            );
                            cache
                        },
                        |mut cache| {
                            let mut hits_total = 0usize;
                            for _ in 0..repeat_jitter_count(repeat, 5) {
                                hits_total += run_ops_once(&mut cache, &keys, &ops, &values);
                            }
                            black_box((hits_total, cache));
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

// =================================================================================
// OTHER BENCHMARKS (unchanged)
// =================================================================================

fn benchmark_eviction_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_eviction_scenarios");
    group.measurement_time(Duration::from_secs(17));

    // Heavy eviction - small cache, large key space
    group.bench_function("heavy_eviction", |b| {
        b.iter_batched(
            || FIFOCache::new(500),
            |mut cache| {
                // 10x more keys than capacity - forces constant eviction
                for i in 0..15000 {
                    if i % 5 == 0 {
                        let key = format!("evict_{}", i % 5000);
                        let result = cache.get(&key).is_some();
                        black_box(result);
                    } else {
                        cache.insert(format!("evict_{}", i % 5000), format!("data_{}", i));
                    }
                }
                black_box(cache)
            },
            BatchSize::SmallInput,
        );
    });

    // Light eviction - good locality
    group.bench_function("light_eviction", |b| {
        b.iter_batched(
            || {
                let mut cache = FIFOCache::new(2000);
                // Pre-populate with working set
                for i in 0..1500 {
                    cache.insert(format!("stable_{}", i), format!("data_{}", i));
                }
                cache
            },
            |mut cache| {
                // Working set fits mostly in cache
                for i in 0..25000 {
                    let working_set_idx = i % 1500;
                    match i % 16 {
                        0..=9 => {
                            // 62.5% reads (high hit rate)
                            let key = format!("stable_{}", working_set_idx);
                            let result = cache.get(&key).is_some();
                            black_box(result);
                        },
                        10..=13 => {
                            // 25% updates
                            let key = format!("stable_{}", working_set_idx);
                            cache.insert(key, format!("updated_{}", i));
                        },
                        _ => {
                            // 12.5% new inserts
                            cache.insert(format!("new_{}", i), format!("new_data_{}", i));
                        },
                    }
                }
                black_box(cache)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// =================================================================================
// FIFO-SPECIFIC OPERATIONS (unchanged)
// =================================================================================

fn benchmark_fifo_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_specific_operations");
    group.measurement_time(Duration::from_secs(8));

    // peek_oldest performance
    group.bench_function("peek_oldest", |b| {
        let mut cache = FIFOCache::new(1000);
        for i in 0..1000 {
            cache.insert(format!("key_{}", i), format!("value_{}", i));
        }

        b.iter(|| black_box(cache.peek_oldest()));
    });

    // pop_oldest performance
    group.bench_function("pop_oldest", |b| {
        b.iter_batched(
            || {
                let mut cache = FIFOCache::new(1000);
                for i in 0..1000 {
                    cache.insert(format!("key_{}", i), format!("value_{}", i));
                }
                cache
            },
            |mut cache| black_box(cache.pop_oldest()),
            BatchSize::SmallInput,
        );
    });

    // pop_oldest_batch performance
    for batch_size in [1, 5, 10, 25, 50] {
        group.bench_with_input(
            BenchmarkId::new("pop_oldest_batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let mut cache = FIFOCache::new(1000);
                        for i in 0..1000 {
                            cache.insert(format!("key_{}", i), format!("value_{}", i));
                        }
                        cache
                    },
                    |mut cache| black_box(cache.pop_oldest_batch(batch_size)),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// =================================================================================
// MICROBENCHES: contains, clear, age_rank
// =================================================================================

fn benchmark_fifo_micro_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_micro_ops");
    group.measurement_time(Duration::from_secs(8));

    // contains
    for &cap in &[128usize, 1024, 4096] {
        group.bench_with_input(BenchmarkId::new("contains", cap), &cap, |b, &cap| {
            let mut cache = FIFOCache::new(cap);
            for i in 0..cap {
                cache.insert(format!("k{i}"), format!("v{i}"));
            }
            b.iter(|| {
                let k = format!("k{}", (fastrand::usize(0..cap)));
                black_box(cache.contains(&k))
            });
        });
    }

    // clear
    for &cap in &[128usize, 1024, 4096] {
        group.bench_with_input(BenchmarkId::new("clear", cap), &cap, |b, &cap| {
            b.iter_batched(
                || {
                    let mut cache = FIFOCache::new(cap);
                    for i in 0..cap {
                        cache.insert(format!("k{i}"), format!("v{i}"));
                    }
                    cache
                },
                |mut cache| {
                    cache.clear();
                    black_box(cache)
                },
                BatchSize::SmallInput,
            );
        });
    }

    // age_rank
    for &cap in &[256usize, 1024, 4096] {
        group.bench_with_input(BenchmarkId::new("age_rank", cap), &cap, |b, &cap| {
            let mut cache = FIFOCache::new(cap);
            for i in 0..cap {
                cache.insert(format!("k{i}"), format!("v{i}"));
            }
            b.iter(|| {
                let idx = fastrand::usize(0..cap);
                let k = format!("k{idx}");
                black_box(cache.age_rank(&k))
            });
        });
    }

    group.finish();
}

// =================================================================================
// STALE-ENTRY IMPACT BENCHMARKS
// =================================================================================

fn benchmark_stale_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_stale_impact");
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
    group.sample_size(60);

    // Create various stale ratios by inserting multiples of capacity
    // stale_factor = how many extra unique inserts beyond capacity
    let caps = [256usize, 1024, 4096];
    let stale_factors = [1usize, 2, 4, 8];

    for &cap in &caps {
        for &factor in &stale_factors {
            let id = BenchmarkId::new("pop_oldest", format!("c{}_f{}", cap, factor));
            group.bench_with_input(id, &(cap, factor), |b, &(cap, factor)| {
                b.iter_batched(
                    || {
                        let mut cache = FIFOCache::new(cap);
                        // Fill to capacity
                        for i in 0..cap {
                            cache.insert(format!("k{i}"), format!("v{i}"));
                        }
                        // Create stale entries by evicting many keys
                        let extra = cap * factor;
                        for i in 0..extra {
                            cache.insert(format!("x{i}"), format!("vx{i}"));
                        }
                        cache
                    },
                    |mut cache| black_box(cache.pop_oldest()),
                    BatchSize::SmallInput,
                );
            });

            let id = BenchmarkId::new("peek_oldest", format!("c{}_f{}", cap, factor));
            group.bench_with_input(id, &(cap, factor), |b, &(cap, factor)| {
                b.iter_batched(
                    || {
                        let mut cache = FIFOCache::new(cap);
                        for i in 0..cap {
                            cache.insert(format!("k{i}"), format!("v{i}"));
                        }
                        let extra = cap * factor;
                        for i in 0..extra {
                            cache.insert(format!("x{i}"), format!("vx{i}"));
                        }
                        cache
                    },
                    |cache| {
                        let result = cache.peek_oldest();
                        black_box(result);
                    },
                    BatchSize::SmallInput,
                );
            });

            let id = BenchmarkId::new("age_rank", format!("c{}_f{}", cap, factor));
            group.bench_with_input(id, &(cap, factor), |b, &(cap, factor)| {
                b.iter_batched(
                    || {
                        let mut cache = FIFOCache::new(cap);
                        for i in 0..cap {
                            cache.insert(format!("k{i}"), format!("v{i}"));
                        }
                        let extra = cap * factor;
                        for i in 0..extra {
                            cache.insert(format!("x{i}"), format!("vx{i}"));
                        }
                        cache
                    },
                    |cache| {
                        // Check rank of a recent key (exists) to force scan over stale block
                        let key = format!("x{}", cap / 2);
                        black_box(cache.age_rank(&key))
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

// =================================================================================
// SPACE USAGE BENCHMARK (also emits CSV outside timed region)
// =================================================================================

fn benchmark_space_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("fifo_space_usage");
    group.measurement_time(Duration::from_secs(5));

    for &cap in &[128usize, 512, 2048, 8192] {
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_batched(
                || {
                    let mut cache = FIFOCache::new(cap);
                    // Fill beyond capacity to create stale entries as well
                    for i in 0..(cap * 3) {
                        cache.insert(format!("s{i}"), format!("vs{i}"));
                    }
                    cache
                },
                |cache| {
                    // Compute space stats (estimates). Not timing file I/O here.
                    let len = cache.len();
                    let ins_len = cache.insertion_order_len();
                    let est_bytes = (len * std::mem::size_of::<Arc<String>>())
                        + (len * std::mem::size_of::<Arc<String>>());

                    // Emit a CSV row (best-effort, ignore errors)
                    emit_space_csv_row("fifo_space_usage", cap, len, ins_len, est_bytes).ok();

                    black_box((len, ins_len))
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn emit_space_csv_row(
    group: &str,
    cap: usize,
    len: usize,
    insertion_order_len: usize,
    est_bytes: usize,
) -> std::io::Result<()> {
    let dir = Path::new("test_results");
    if !dir.exists() {
        create_dir_all(dir)?;
    }
    let path = dir.join("fifo_benchmarks_space.csv");
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(
        file,
        "group,cap,len,insertion_order_len,estimated_bytes\n{group},{cap},{len},{insertion_order_len},{est_bytes}"
    )?;
    Ok(())
}

// =================================================================================
// UTILITY FUNCTIONS (CALIBRATION + WORKLOAD HELPERS)
// =================================================================================

/// Simple deterministic random for benchmarks (thread-local state)
mod fastrand {
    use std::cell::Cell;

    thread_local! {
        static RNG: Cell<u64> = Cell::new(0x4d595df4d0f33173);
    }

    pub fn u64(range: std::ops::Range<u64>) -> u64 {
        RNG.with(|rng| {
            let mut state = rng.get();
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            rng.set(state);

            let len = range.end - range.start;
            range.start + (state % len)
        })
    }

    pub fn usize(range: std::ops::Range<usize>) -> usize {
        u64(range.start as u64..range.end as u64) as usize
    }
}

#[derive(Clone, Copy)]
enum Op {
    Insert(usize),
    Get(usize),
}

fn pregen_keys(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{prefix}{i}")).collect()
}

fn pregen_values(count: usize) -> Vec<String> {
    // Very small values to reduce allocation noise; built once outside timing
    (0..count).map(|i| format!("v{i}")).collect()
}

fn build_sequential_ops(total_ops: usize, working_set: usize, insert_every: usize) -> Vec<Op> {
    let mut ops = Vec::with_capacity(total_ops);
    for i in 0..total_ops {
        let idx = i % working_set;
        if insert_every != 0 && i % insert_every == 0 {
            ops.push(Op::Insert(idx));
        } else {
            ops.push(Op::Get(idx));
        }
    }
    ops
}

fn build_random_ops(total_ops: usize, working_set: usize, insert_every: usize) -> Vec<Op> {
    let mut ops = Vec::with_capacity(total_ops);
    for i in 0..total_ops {
        let idx = fastrand::usize(0..working_set);
        if insert_every != 0 && i % insert_every == 0 {
            ops.push(Op::Insert(idx));
        } else {
            ops.push(Op::Get(idx));
        }
    }
    ops
}

fn prefill_cache<K: Clone + Eq + Hash, V: Clone>(
    cache: &mut FIFOCache<K, V>,
    keys: &[K],
    values: &[V],
    prefill: usize,
) {
    let n = prefill.min(keys.len()).min(values.len());
    for i in 0..n {
        cache.insert(keys[i].clone(), values[i].clone());
    }
}

fn run_ops_once<K: Clone + Eq + Hash, V: Clone>(
    cache: &mut FIFOCache<K, V>,
    keys: &[K],
    ops: &[Op],
    values: &[V],
) -> usize {
    let mut hits = 0usize;
    for (i, op) in ops.iter().enumerate() {
        let v = &values[i % values.len()];
        match *op {
            Op::Insert(kidx) => {
                cache.insert(keys[kidx].clone(), v.clone());
            },
            Op::Get(kidx) => {
                if cache.get(&keys[kidx]).is_some() {
                    hits += 1;
                }
            },
        }
    }
    hits
}

/// Calibrate a repeat count so one "sample" (aggregate of many ops) lasts at least `target`.
fn calibrate_repeat<F>(one_pass: &mut F, target: Duration, max_repeat: u64) -> u64
where
    F: FnMut(),
{
    // Light warmup to prime caches and branch predictors
    for _ in 0..128 {
        one_pass();
    }

    let mut repeat = 1u64;
    loop {
        let start = Instant::now();
        for _ in 0..repeat {
            one_pass();
        }
        let dt = start.elapsed();
        if dt >= target || repeat >= max_repeat {
            return repeat.max(1);
        }
        repeat = repeat.saturating_mul(2);
    }
}

/// Add a small deterministic jitter (±spread%) to avoid coherent sampling vs periodic noise.
fn repeat_jitter_count(base: u64, spread_percent: i64) -> u64 {
    // Use a simple sequence for determinism
    // Cycle through -spread..+spread adjustments
    static OFFSETS: [i64; 11] = [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5];
    let adj = OFFSETS[((base % OFFSETS.len() as u64) as usize) % OFFSETS.len()];
    let adj = adj.clamp(-spread_percent, spread_percent);
    let scaled = ((base as i64) * (100 + adj)) / 100;
    scaled.max(1) as u64
}

// =================================================================================
// CRITERION GROUPS
// =================================================================================

criterion_group!(time_complexity_benches, benchmark_insert_time_complexity);

criterion_group!(
    performance_benches,
    benchmark_access_patterns,
    benchmark_eviction_scenarios,
    benchmark_fifo_operations,
    benchmark_fifo_micro_ops,
    benchmark_stale_impact,
    benchmark_space_usage
);

criterion_main!(performance_benches, time_complexity_benches);
