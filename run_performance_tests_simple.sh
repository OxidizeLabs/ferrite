#!/bin/bash

# Simple TKDB AsyncDiskManager Performance Test Runner
# Runs all performance tests in one go

echo "🚀 TKDB AsyncDiskManager Performance Tests"
echo "=========================================="

# Set environment for better logging
export RUST_LOG=info

# Run all performance tests with pattern matching
echo "📊 Running all performance tests..."
cargo test performance --lib -- --nocapture --test-threads=1

echo ""
echo "✅ Performance tests completed!"
echo "💡 For individual test runs with detailed logging, use ./run_performance_tests.sh"
