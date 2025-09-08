use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::hint::black_box;
// =================================================================================
// SIMPLE FUNCTIONS WITH DIFFERENT TIME COMPLEXITIES
// =================================================================================

/// O(1) - Constant time: Direct array access
fn constant_time_access(data: &[i32], index: usize) -> i32 {
    data[index]
}

/// O(1) - Hash table lookup
fn hash_lookup(map: &HashMap<i32, i32>, key: i32) -> Option<&i32> {
    map.get(&key)
}

/// O(n) - Linear time: Linear search through array
fn linear_search(data: &[i32], target: i32) -> Option<usize> {
    for (i, &value) in data.iter().enumerate() {
        if value == target {
            return Some(i);
        }
    }
    None
}

/// O(n) - Linear time: Sum all elements
fn sum_array(data: &[i32]) -> i64 {
    data.iter().map(|&x| x as i64).sum()
}

/// O(n log n) - Efficient sorting
fn efficient_sort(mut data: Vec<i32>) -> Vec<i32> {
    data.sort();
    data
}

/// O(n²) - Quadratic time: Bubble sort
fn bubble_sort(mut data: Vec<i32>) -> Vec<i32> {
    let n = data.len();
    for i in 0..n {
        for j in 0..(n - i - 1) {
            if data[j] > data[j + 1] {
                data.swap(j, j + 1);
            }
        }
    }
    data
}

/// O(n²) - Quadratic time: Find all pairs that sum to target
fn find_pairs_naive(data: &[i32], target: i32) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    for i in 0..data.len() {
        for j in (i + 1)..data.len() {
            if data[i] + data[j] == target {
                pairs.push((i, j));
            }
        }
    }
    pairs
}

// =================================================================================
// BENCHMARKS
// =================================================================================

fn benchmark_constant_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("O(1) - Constant Time");

    // Test different data sizes - should show constant time
    for size in [100, 1_000, 10_000, 100_000] {
        let data: Vec<i32> = (0..size).collect();
        let mut map = HashMap::new();
        for i in 0..size {
            map.insert(i, i);
        }

        group.bench_with_input(BenchmarkId::new("array_access", size), &size, |b, _| {
            b.iter(|| {
                let index = size / 2; // Access middle element
                black_box(constant_time_access(&data, index as usize))
            })
        });

        group.bench_with_input(BenchmarkId::new("hash_lookup", size), &size, |b, _| {
            b.iter(|| {
                let key = (size / 2);
                black_box(hash_lookup(&map, key))
            })
        });
    }

    group.finish();
}

fn benchmark_linear_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("O(n) - Linear Time");

    // Test different data sizes - should show linear growth
    for size in [100, 500, 1_000, 5_000, 10_000] {
        let data: Vec<i32> = (0..size).map(|i| i).collect();

        group.bench_with_input(BenchmarkId::new("linear_search", size), &size, |b, _| {
            b.iter(|| {
                // Search for element that doesn't exist (worst case)
                let target = size + 1;
                black_box(linear_search(&data, target))
            })
        });

        group.bench_with_input(BenchmarkId::new("sum_array", size), &size, |b, _| {
            b.iter(|| black_box(sum_array(&data)))
        });
    }

    group.finish();
}

fn benchmark_quadratic_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("O(n²) - Quadratic Time");

    // Use smaller sizes for quadratic algorithms
    for size in [50, 100, 200, 400, 800] {
        let data: Vec<i32> = (0..size).map(|i| i).rev().collect(); // Worst case for bubble sort

        group.bench_with_input(BenchmarkId::new("bubble_sort", size), &size, |b, _| {
            b.iter(|| black_box(bubble_sort(data.clone())))
        });

        group.bench_with_input(BenchmarkId::new("find_pairs", size), &size, |b, _| {
            b.iter(|| {
                let target = data[0] + data[1]; // Sum that exists
                black_box(find_pairs_naive(&data, target))
            })
        });
    }

    group.finish();
}

fn benchmark_linearithmic_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("O(n log n) - Linearithmic Time");

    // Test different data sizes for sorting
    for size in [100, 500, 1_000, 5_000, 10_000, 50_000] {
        let data: Vec<i32> = (0..size).map(|i| (size - i)).collect(); // Reverse sorted (challenging)

        group.bench_with_input(BenchmarkId::new("efficient_sort", size), &size, |b, _| {
            b.iter(|| black_box(efficient_sort(data.clone())))
        });
    }

    group.finish();
}

fn benchmark_complexity_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("Time Complexity Comparison");

    // Compare all complexities at the same input size for relative performance
    let size = 1_000;
    let data: Vec<i32> = (0..size).map(|i| i as i32).collect();
    let mut map = HashMap::new();
    for i in 0..size {
        map.insert(i as i32, i as i32);
    }

    group.bench_function("O(1)_array_access", |b| {
        b.iter(|| black_box(constant_time_access(&data, size / 2)))
    });

    group.bench_function("O(1)_hash_lookup", |b| {
        b.iter(|| black_box(hash_lookup(&map, (size / 2) as i32)))
    });

    group.bench_function("O(n)_linear_search", |b| {
        b.iter(|| {
            black_box(linear_search(&data, -1)) // Not found - worst case
        })
    });

    group.bench_function("O(n)_sum_array", |b| b.iter(|| black_box(sum_array(&data))));

    group.bench_function("O(n_log_n)_sort", |b| {
        b.iter(|| black_box(efficient_sort(data.clone())))
    });

    // Use smaller size for quadratic to avoid timeout
    let small_data: Vec<i32> = (0..100).map(|i| (100 - i)).collect();
    group.bench_function("O(n²)_bubble_sort", |b| {
        b.iter(|| black_box(bubble_sort(small_data.clone())))
    });

    group.finish();
}

// =================================================================================
// CRITERION CONFIGURATION
// =================================================================================

criterion_group!(
    complexity_demos,
    benchmark_constant_time,
    benchmark_linear_time,
    benchmark_quadratic_time,
    benchmark_linearithmic_time,
    benchmark_complexity_comparison
);

criterion_main!(complexity_demos);
