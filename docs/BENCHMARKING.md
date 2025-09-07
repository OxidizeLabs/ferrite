# TKDB Benchmarking Guide

This guide shows how to use **Criterion.rs** for performance benchmarking in TKDB. Criterion provides sophisticated statistical analysis, automatic trend detection, and beautiful HTML reports.

## Quick Start

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark group  
cargo bench cache_benchmarks

# Run with HTML reports (opens browser)
cargo bench -- --output-format html

# Run and compare with baseline
cargo bench -- --save-baseline main
# ... make changes ...
cargo bench -- --baseline main
```

### Viewing Reports

After running benchmarks, open `target/criterion/report/index.html` in your browser for detailed analysis including:
- Performance trends across input sizes
- Statistical confidence intervals  
- Regression detection
- Violin plots showing distribution

## Writing Benchmarks

### 1. Simple Operation Benchmark

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_simple_operation(c: &mut Criterion) {
    c.bench_function("vector_creation", |b| {
        b.iter(|| {
            let v: Vec<i32> = (0..1000).map(|i| black_box(i)).collect();
            black_box(v)
        })
    });
}

criterion_group!(benches, benchmark_simple_operation);
criterion_main!(benches);
```

### 2. Complexity Analysis

```rust
use criterion::{BenchmarkId, Criterion};

fn benchmark_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm_complexity");
    
    // Test different input sizes - Criterion will detect O(n), O(n²), etc.
    for size in [100, 500, 1000, 5000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("linear_search", size),
            &size,
            |b, &size| {
                let data: Vec<i32> = (0..size).collect();
                b.iter(|| {
                    // This will show O(n) complexity in reports
                    data.iter().find(|&&x| x == black_box(size / 2))
                });
            }
        );
    }
    
    group.finish();
}
```

### 3. Setup/Teardown with `iter_batched`

```rust
fn benchmark_with_setup(c: &mut Criterion) {
    c.bench_function("cache_operation", |b| {
        b.iter_batched(
            // Setup (not measured)
            || {
                let mut cache = LRUCache::new(1000);
                // Fill cache...
                cache
            },
            // Operation to benchmark
            |mut cache| {
                cache.get(&black_box("key"))
            },
            criterion::BatchSize::SmallInput
        );
    });
}
```

### 4. Comparing Multiple Implementations

```rust
fn benchmark_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_comparison");
    
    group.bench_function("fifo_cache", |b| {
        // FIFO implementation
        b.iter(|| fifo_operation(black_box(input)))
    });
    
    group.bench_function("lru_cache", |b| {
        // LRU implementation  
        b.iter(|| lru_operation(black_box(input)))
    });
    
    group.bench_function("lfu_cache", |b| {
        // LFU implementation
        b.iter(|| lfu_operation(black_box(input)))
    });
    
    group.finish();
}
```

## Best Practices

### 1. Use `black_box` to Prevent Optimization

```rust
// WRONG - compiler may optimize away
let result = expensive_computation();

// CORRECT - prevents optimization
let result = expensive_computation();
black_box(result);
```

### 2. Configure Measurement Parameters

```rust
fn benchmark_configured(c: &mut Criterion) {
    let mut group = c.benchmark_group("configured_benchmark");
    
    // Longer measurements for more stable results
    group.measurement_time(Duration::from_secs(10));
    
    // More samples for better statistics
    group.sample_size(200);
    
    // Longer warmup for consistent results
    group.warm_up_time(Duration::from_secs(3));
    
    group.bench_function("operation", |b| {
        b.iter(|| operation())
    });
    
    group.finish();
}
```

### 3. Use Appropriate Batch Sizes

```rust
// For expensive setup
criterion::BatchSize::LargeInput

// For cheap setup  
criterion::BatchSize::SmallInput

// Let Criterion decide
criterion::BatchSize::PerIteration
```

## Database-Specific Benchmarks

### Buffer Pool Performance

```rust
fn benchmark_buffer_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool");
    
    for pool_size in [100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("fetch_page", pool_size),
            &pool_size,
            |b, &pool_size| {
                let buffer_pool = BufferPoolManager::new(pool_size);
                b.iter(|| {
                    buffer_pool.fetch_page(black_box(PageId::new(42)))
                });
            }
        );
    }
}
```

### SQL Query Performance

```rust
fn benchmark_sql_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_execution");
    
    group.bench_function("select_query", |b| {
        b.iter_batched(
            || setup_test_database(),
            |db| {
                db.execute(black_box("SELECT * FROM users WHERE age > 25"))
            },
            BatchSize::LargeInput
        );
    });
}
```

### Index Performance

```rust
fn benchmark_index_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_operations");
    
    for size in [1000, 10000, 100000] {
        group.bench_with_input(
            BenchmarkId::new("btree_search", size),
            &size,
            |b, &size| {
                let index = setup_btree_index(size);
                b.iter(|| {
                    index.search(black_box(&42))
                });
            }
        );
    }
}
```

## Understanding Results

### Complexity Detection
Criterion automatically detects algorithmic complexity:
- **O(1)**: Flat line across input sizes
- **O(log n)**: Slowly increasing curve  
- **O(n)**: Linear increase
- **O(n log n)**: Slightly curved upward
- **O(n²)**: Steep upward curve

### Statistical Measures
- **Mean**: Average time (can be skewed by outliers)
- **Median**: Middle value (more robust)
- **MAD**: Median Absolute Deviation (spread measure)
- **Std Dev**: Standard deviation (variability)

### Interpreting Graphs
- **Violin plots**: Show distribution shape
- **Trend lines**: Indicate algorithmic complexity
- **Confidence intervals**: Statistical uncertainty
- **Outliers**: Highlighted separately

## CI/CD Integration

### Automated Performance Testing

```yaml
# .github/workflows/benchmarks.yml
name: Performance Benchmarks

on: [push, pull_request]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          
      - name: Run Benchmarks
        run: |
          cargo bench -- --output-format json | tee output.json
          
      - name: Compare Performance
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: output.json
          alert-threshold: '200%'  # Alert if 2x slower
          fail-on-alert: true
```

### Performance Regression Detection

```bash
# Establish baseline
cargo bench -- --save-baseline main

# After changes, compare
cargo bench -- --baseline main
# Will show % change from baseline and warn about regressions
```

## File Organization

```
tkdb/
├── benches/
│   ├── cache_benchmarks.rs      # Cache performance tests
│   ├── sql_benchmarks.rs        # SQL execution benchmarks  
│   ├── buffer_pool_benchmarks.rs # Buffer management benchmarks
│   └── index_benchmarks.rs      # Index operation benchmarks
├── Cargo.toml                   # Criterion dependencies + [[bench]] config
└── target/
    └── criterion/               # Generated reports and data
        └── report/
            └── index.html       # Main report (open in browser)
```

## Performance Goals

Based on complexity analysis, TKDB aims for:

| Operation | Target Complexity | Acceptable Range |
|-----------|------------------|------------------|
| Hash table get/put | O(1) | < 100ns |
| B+ tree search | O(log n) | < 1μs per level |
| Buffer pool fetch | O(1) | < 500ns |
| SQL parse | O(n) in query length | < 10ms |
| Index scan | O(log n + k) | < 1ms + 50ns/record |

Criterion will automatically detect when these targets are not met and provide detailed analysis of the performance characteristics.
