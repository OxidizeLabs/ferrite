#!/bin/bash

# TKDB AsyncDiskManager Performance Test Runner
# This script runs all performance tests for the AsyncDiskManager and captures output

echo "🚀 TKDB AsyncDiskManager Performance Test Suite"
echo "=============================================="
echo ""

# Set test environment
export RUST_LOG=info
export TOKIO_CONSOLE=1

# Create output directory
mkdir -p test_results
timestamp=$(date +"%Y%m%d_%H%M%S")
output_file="test_results/performance_test_${timestamp}.log"

echo "📊 Running performance tests..."
echo "📝 Output will be saved to: ${output_file}"
echo ""

# Run specific performance tests
echo "Running AsyncDiskManager performance tests..." | tee "${output_file}"

# Run each test individually to get detailed output
echo -e "\n=== Sequential Read/Write Performance ===" | tee -a "${output_file}"
cargo test test_sequential_read_write_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Random Access Performance ===" | tee -a "${output_file}"
cargo test test_random_access_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== High Concurrency Performance ===" | tee -a "${output_file}"
cargo test test_high_concurrency_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Large Data Throughput ===" | tee -a "${output_file}"
cargo test test_large_data_throughput --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Cache Performance ===" | tee -a "${output_file}"
cargo test test_cache_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Batch Operation Performance ===" | tee -a "${output_file}"
cargo test test_batch_operation_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Mixed Workload Performance ===" | tee -a "${output_file}"
cargo test test_mixed_workload_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo -e "\n=== Stress Test Performance ===" | tee -a "${output_file}"
cargo test test_stress_performance --package tkdb --test "" -- --exact --nocapture 2>&1 | tee -a "${output_file}"

echo ""
echo "✅ Performance test suite completed!"
echo "📈 Results saved to: ${output_file}"
echo ""
echo "📋 Summary:"
echo "   - Sequential Read/Write Performance"
echo "   - Random Access Performance"
echo "   - High Concurrency Performance"
echo "   - Large Data Throughput"
echo "   - Cache Performance"
echo "   - Batch Operation Performance"
echo "   - Mixed Workload Performance"
echo "   - Stress Test Performance"
echo ""
echo "💡 Use 'tail -f ${output_file}' to follow test progress in real-time"
