# AsyncDiskManager Performance Tests

This document describes the comprehensive performance test suite for TKDB's AsyncDiskManager.

## Overview

The performance test suite consists of 8 specialized tests that measure different aspects of disk I/O performance, caching, and concurrency. All tests capture detailed metrics and provide both logging output and stdout summaries.

## Test Suite

### 1. Sequential Read/Write Performance (`test_sequential_read_write_performance`)
**Purpose**: Measures performance for sequential access patterns (typical for table scans)

**Test Details**:
- 100 pages × 4KB = 400KB total data
- Sequential writes followed by sequential reads
- Measures optimal case performance

**Key Metrics**:
- I/O throughput (MB/s)
- Average read/write latency
- Cache performance

### 2. Random Access Performance (`test_random_access_performance`)
**Purpose**: Tests performance with random access patterns (typical for index lookups)

**Test Details**:
- 200 random page accesses within 1000-page range
- Uses deterministic PRNG for reproducible results
- Tests worst-case access patterns

**Key Metrics**:
- Random I/O throughput
- Latency under fragmented access
- Cache miss behavior

### 3. High Concurrency Performance (`test_high_concurrency_performance`)
**Purpose**: Evaluates performance under concurrent load (multi-user scenarios)

**Test Details**:
- 50 concurrent writer tasks + 50 concurrent reader tasks
- 10 operations per task = 1000 total operations
- Tests thread safety and lock contention

**Key Metrics**:
- Concurrent operations per second
- Queue depth management
- Thread synchronization overhead

### 4. Large Data Throughput (`test_large_data_throughput`)
**Purpose**: Measures peak throughput with large transfers (bulk operations)

**Test Details**:
- 50 pages × 64KB = 3.2MB total transfer
- Tests memory allocation and large buffer handling
- Simulates data warehouse workloads

**Key Metrics**:
- Peak throughput (MB/s)
- Memory efficiency
- Large transfer latency

### 5. Cache Performance (`test_cache_performance`)
**Purpose**: Tests caching effectiveness and hit/miss ratios

**Test Details**:
- Initial writes + two read passes
- 32MB cache with 100 × 4KB pages (cache pressure)
- Measures cache warming and hit ratio improvement

**Key Metrics**:
- Cache hit ratio progression
- Cache memory utilization
- Performance improvement from caching

### 6. Batch Operation Performance (`test_batch_operation_performance`)
**Purpose**: Compares batch vs individual operations

**Test Details**:
- 20-page batches for both reads and writes
- Tests batching optimization effectiveness
- Measures API efficiency

**Key Metrics**:
- Batch vs individual operation latency
- Batch processing throughput
- Resource utilization efficiency

### 7. Mixed Workload Performance (`test_mixed_workload_performance`)
**Purpose**: Simulates realistic mixed read/write workloads

**Test Details**:
- 200 operations with varying page sizes (1KB, 4KB, 16KB)
- 60% read / 40% write ratio
- Tests real-world usage patterns

**Key Metrics**:
- Mixed workload throughput
- Variable size handling
- Realistic performance characteristics

### 8. Stress Test Performance (`test_stress_performance`)
**Purpose**: Tests sustained performance under continuous high load

**Test Details**:
- 8 worker threads running for 5 seconds
- Continuous read/write operations
- Tests stability and resource management

**Key Metrics**:
- Sustained operations per second
- Error rates under stress
- Resource exhaustion behavior

## Metrics Captured

Each test captures comprehensive metrics:

### I/O Performance
- **Read Latency Avg**: Average microseconds for read operations
- **Write Latency Avg**: Average microseconds for write operations  
- **I/O Throughput**: Megabytes per second transfer rate
- **I/O Queue Depth**: Current pending operations

### Cache Performance
- **Cache Hit Ratio**: Percentage of reads served from cache
- **Prefetch Accuracy**: Effectiveness of predictive loading
- **Cache Memory Usage**: Current cache memory consumption (MB)

### Write Performance
- **Write Buffer Utilization**: Buffer pool usage efficiency
- **Compression Ratio**: Data compression effectiveness
- **Flush Frequency**: Disk sync operations per second

### Error Tracking
- **Error Rate**: Errors per second during test
- **Retry Count**: Number of operation retries needed

## Running the Tests

### Option 1: Run All Tests (Recommended)
```bash
./run_performance_tests_simple.sh
```

### Option 2: Individual Test Details
```bash
./run_performance_tests.sh
```
This creates detailed logs in `test_results/` directory with timestamps.

### Option 3: Manual Test Execution
```bash
# Set environment for detailed logging
export RUST_LOG=info

# Run specific test
cargo test test_sequential_read_write_performance --lib -- --nocapture

# Run all performance tests
cargo test performance --lib -- --nocapture --test-threads=1
```

## Interpreting Results

### Good Performance Indicators
- **Throughput**: >10 MB/s for sequential, >1 MB/s for random
- **Latency**: <100μs for cached reads, <1000μs for disk reads
- **Cache Hit Ratio**: >80% for repeated access patterns
- **Error Rate**: 0.0 errors/sec for normal operations
- **Concurrency**: Linear scaling with thread count

### Performance Optimization
Based on test results, you can:

1. **Tune Cache Size**: Increase if hit ratio is low
2. **Adjust I/O Threads**: Match to CPU core count
3. **Optimize Batch Size**: Find sweet spot for your workload
4. **Enable Compression**: If data is compressible
5. **Tune Buffer Sizes**: Based on access patterns

## Test Configuration

Performance tests use optimized configuration:
```rust
DiskManagerConfig {
    io_threads: 4,
    max_concurrent_ops: 200,
    batch_size: 32,
    cache_size_mb: 128,
    write_buffer_size_mb: 32,
    metrics_enabled: true,
    detailed_metrics: true,
    prefetch_enabled: true,
    prefetch_distance: 8,
    compression_enabled: true,
    numa_aware: false, // Simplified for testing
}
```

## Adding Custom Tests

To add your own performance test:

1. Follow the naming pattern: `test_*_performance`
2. Use `create_performance_test_config()` for consistent setup
3. Call `log_metrics(test_name, &metrics, elapsed_ms)` for output
4. Include assertions for expected behavior
5. Document the test purpose and expected results

## Troubleshooting

### Common Issues
- **Low Throughput**: Check disk speed, increase I/O threads
- **High Latency**: Reduce concurrency, check disk health
- **Cache Misses**: Increase cache size, improve access patterns
- **Test Failures**: Check disk space, permissions, resource limits

### Environment Factors
- **Disk Type**: SSD vs HDD significantly affects results
- **File System**: ext4, APFS, NTFS have different characteristics
- **Available RAM**: Affects caching effectiveness
- **CPU Cores**: Limits concurrent I/O processing
- **Background Load**: Other processes affect results

## Benchmarking Guidelines

For consistent benchmarking:
1. Run tests multiple times and average results
2. Ensure consistent system load during testing
3. Use same hardware configuration for comparisons
4. Clear OS file system cache between runs if needed
5. Monitor system resources during tests

The performance test suite provides comprehensive coverage of AsyncDiskManager capabilities and helps ensure optimal database performance across various workload patterns.