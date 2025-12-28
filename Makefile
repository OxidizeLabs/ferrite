# ============================================================================
# Ferrite DBMS - Makefile
# ============================================================================
#
# Usage:
#   make              - Build debug binary
#   make release      - Build optimized release binary
#   make test         - Run all tests
#   make bench        - Run benchmarks
#   make server       - Start database server
#   make client       - Connect to database server
#   make cli          - Run embedded CLI
#   make clean        - Remove build artifacts
#   make help         - Show this help
#
# ============================================================================

# Configuration
CARGO := cargo
BINARY := ferrite
PORT ?= 5432
HOST ?= 127.0.0.1

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

.PHONY: all build release test test-unit test-integration bench \
        server client cli run clean fmt lint check doc \
        install-tools setup-hooks evidence-crash-recovery help

# ============================================================================
# Default Target
# ============================================================================

all: build

# ============================================================================
# Build Targets
# ============================================================================

## Build debug binary
build:
	@echo "$(CYAN)Building Ferrite (debug)...$(RESET)"
	$(CARGO) build

## Build release binary (optimized)
release:
	@echo "$(CYAN)Building Ferrite (release)...$(RESET)"
	$(CARGO) build --release

## Build with all features
build-full:
	@echo "$(CYAN)Building Ferrite (all features)...$(RESET)"
	$(CARGO) build --features full

## Build release with all features
release-full:
	@echo "$(CYAN)Building Ferrite (release, all features)...$(RESET)"
	$(CARGO) build --release --features full

# ============================================================================
# Test Targets
# ============================================================================

## Run all tests
test:
	@echo "$(CYAN)Running all tests...$(RESET)"
	$(CARGO) test

## Run tests with output
test-verbose:
	@echo "$(CYAN)Running tests (verbose)...$(RESET)"
	$(CARGO) test -- --nocapture

## Run unit tests only
test-unit:
	@echo "$(CYAN)Running unit tests...$(RESET)"
	$(CARGO) test --lib

## Run integration tests only
test-integration:
	@echo "$(CYAN)Running integration tests...$(RESET)"
	$(CARGO) test --test '*'

## Run SQLLogic tests
test-sqllogic:
	@echo "$(CYAN)Running SQLLogic tests...$(RESET)"
	$(CARGO) test --test sqllogic

## Run a specific test (usage: make test-one TEST=test_name)
test-one:
	@echo "$(CYAN)Running test: $(TEST)$(RESET)"
	$(CARGO) test $(TEST) -- --nocapture

## Run tests with mocking feature
test-mock:
	@echo "$(CYAN)Running tests with mocking...$(RESET)"
	$(CARGO) test --features mocking

# ============================================================================
# Benchmark Targets
# ============================================================================

## Run all benchmarks
bench:
	@echo "$(CYAN)Running benchmarks...$(RESET)"
	$(CARGO) bench

## Run cache benchmarks
bench-cache:
	@echo "$(CYAN)Running cache benchmarks...$(RESET)"
	$(CARGO) bench --bench cache_benchmarks

## Run FIFO cache benchmarks
bench-fifo:
	@echo "$(CYAN)Running FIFO cache benchmarks...$(RESET)"
	$(CARGO) bench --bench fifo_cache_benchmarking

## Run time complexity benchmarks
bench-complexity:
	@echo "$(CYAN)Running complexity benchmarks...$(RESET)"
	$(CARGO) bench --bench time_complexity_demo

# ============================================================================
# Run Targets
# ============================================================================

## Start the database server
server: build
	@echo "$(GREEN)Starting Ferrite server on port $(PORT)...$(RESET)"
	$(CARGO) run -- server -P $(PORT)

## Start server in release mode
server-release: release
	@echo "$(GREEN)Starting Ferrite server (release) on port $(PORT)...$(RESET)"
	$(CARGO) run --release -- server -P $(PORT)

## Connect to database server
client:
	@echo "$(GREEN)Connecting to Ferrite at $(HOST):$(PORT)...$(RESET)"
	$(CARGO) run -- client -H $(HOST) -P $(PORT)

## Run embedded CLI
cli: build
	@echo "$(GREEN)Starting Ferrite CLI...$(RESET)"
	$(CARGO) run -- cli

## Run a SQL file (usage: make run-sql FILE=test.sql)
run-sql: build
	@echo "$(GREEN)Running SQL file: $(FILE)$(RESET)"
	$(CARGO) run --bin ferrite --features cli -- $(FILE)

# ============================================================================
# Code Quality Targets
# ============================================================================

## Format code
fmt:
	@echo "$(CYAN)Formatting code...$(RESET)"
	$(CARGO) fmt

## Check formatting without changes
fmt-check:
	@echo "$(CYAN)Checking code format...$(RESET)"
	$(CARGO) fmt -- --check

## Run clippy lints
lint:
	@echo "$(CYAN)Running clippy...$(RESET)"
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Run clippy with fixes
lint-fix:
	@echo "$(CYAN)Running clippy with fixes...$(RESET)"
	$(CARGO) clippy --fix --allow-dirty --allow-staged

## Type check without building
check:
	@echo "$(CYAN)Type checking...$(RESET)"
	$(CARGO) check --all-features

## Run all quality checks (format, lint, test)
quality: fmt-check lint test
	@echo "$(GREEN)All quality checks passed!$(RESET)"

# ============================================================================
# Documentation Targets
# ============================================================================

## Generate documentation
doc:
	@echo "$(CYAN)Generating documentation...$(RESET)"
	$(CARGO) doc --no-deps

## Generate and open documentation
doc-open:
	@echo "$(CYAN)Generating and opening documentation...$(RESET)"
	$(CARGO) doc --no-deps --open

## Generate documentation with private items
doc-private:
	@echo "$(CYAN)Generating documentation (with private items)...$(RESET)"
	$(CARGO) doc --no-deps --document-private-items

# ============================================================================
# Evidence Targets (Reproducible Demos)
# ============================================================================

## Run crash-recovery evidence demo (simulates crash, restarts, verifies)
evidence-crash-recovery:
	@echo "$(CYAN)Running crash-recovery evidence demo...$(RESET)"
	bash ./scripts/crash_recovery_evidence.sh

# ============================================================================
# Cleanup Targets
# ============================================================================

## Remove build artifacts
clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(RESET)"
	$(CARGO) clean

## Remove build artifacts and database files
clean-all: clean
	@echo "$(YELLOW)Removing database files...$(RESET)"
	rm -f *.db *.log *.wal *.checkpoint
	rm -rf test_results/

## Remove only database files (keep build)
clean-db:
	@echo "$(YELLOW)Removing database files...$(RESET)"
	rm -f *.db *.log *.wal *.checkpoint

# ============================================================================
# Development Setup
# ============================================================================

## Install development tools
install-tools:
	@echo "$(CYAN)Installing development tools...$(RESET)"
	rustup component add rustfmt clippy
	cargo install cargo-watch cargo-audit cargo-outdated

## Setup git hooks
setup-hooks:
	@echo "$(CYAN)Setting up git hooks...$(RESET)"
	./setup-hooks.sh

## Watch for changes and rebuild
watch:
	@echo "$(CYAN)Watching for changes...$(RESET)"
	cargo watch -x build

## Watch for changes and run tests
watch-test:
	@echo "$(CYAN)Watching for changes (running tests)...$(RESET)"
	cargo watch -x test

# ============================================================================
# CI/CD Targets
# ============================================================================

## Run CI pipeline locally
ci: fmt-check lint test
	@echo "$(GREEN)CI pipeline passed!$(RESET)"

## Check for security vulnerabilities
audit:
	@echo "$(CYAN)Auditing dependencies...$(RESET)"
	cargo audit

## Check for outdated dependencies
outdated:
	@echo "$(CYAN)Checking for outdated dependencies...$(RESET)"
	cargo outdated

## Update dependencies
update:
	@echo "$(CYAN)Updating dependencies...$(RESET)"
	$(CARGO) update

# ============================================================================
# Profiling Targets
# ============================================================================

## Generate flamegraph (requires cargo-flamegraph)
flamegraph:
	@echo "$(CYAN)Generating flamegraph...$(RESET)"
	cargo flamegraph --bin ferrite -- cli

## Profile with perf (Linux only)
perf:
	@echo "$(CYAN)Running with perf...$(RESET)"
	perf record --call-graph dwarf cargo run --release -- cli
	perf report

# ============================================================================
# Help Target
# ============================================================================

## Show this help message
help:
	@echo ""
	@echo "$(CYAN)Ferrite DBMS - Available Commands$(RESET)"
	@echo ""
	@echo "$(GREEN)Build:$(RESET)"
	@echo "  make build          Build debug binary"
	@echo "  make release        Build optimized release binary"
	@echo "  make build-full     Build with all features"
	@echo "  make release-full   Build release with all features"
	@echo ""
	@echo "$(GREEN)Test:$(RESET)"
	@echo "  make test           Run all tests"
	@echo "  make test-verbose   Run tests with output"
	@echo "  make test-unit      Run unit tests only"
	@echo "  make test-integration Run integration tests"
	@echo "  make test-sqllogic  Run SQLLogic tests"
	@echo "  make test-one TEST=name  Run specific test"
	@echo ""
	@echo "$(GREEN)Benchmark:$(RESET)"
	@echo "  make bench          Run all benchmarks"
	@echo "  make bench-cache    Run cache benchmarks"
	@echo "  make bench-fifo     Run FIFO cache benchmarks"
	@echo ""
	@echo "$(GREEN)Run:$(RESET)"
	@echo "  make server         Start database server (PORT=5432)"
	@echo "  make server-release Start server in release mode"
	@echo "  make client         Connect to server (HOST=127.0.0.1 PORT=5432)"
	@echo "  make cli            Run embedded CLI"
	@echo "  make run-sql FILE=x Run a SQL file"
	@echo ""
	@echo "$(GREEN)Code Quality:$(RESET)"
	@echo "  make fmt            Format code"
	@echo "  make fmt-check      Check formatting"
	@echo "  make lint           Run clippy lints"
	@echo "  make check          Type check without building"
	@echo "  make quality        Run all quality checks"
	@echo ""
	@echo "$(GREEN)Documentation:$(RESET)"
	@echo "  make doc            Generate documentation"
	@echo "  make doc-open       Generate and open docs"
	@echo ""
	@echo "$(GREEN)Cleanup:$(RESET)"
	@echo "  make clean          Remove build artifacts"
	@echo "  make clean-all      Remove build + database files"
	@echo "  make clean-db       Remove database files only"
	@echo ""
	@echo "$(GREEN)Development:$(RESET)"
	@echo "  make install-tools  Install dev tools"
	@echo "  make setup-hooks    Setup git hooks"
	@echo "  make watch          Watch and rebuild"
	@echo "  make watch-test     Watch and run tests"
	@echo ""
	@echo "$(GREEN)CI/CD:$(RESET)"
	@echo "  make ci             Run CI pipeline locally"
	@echo "  make audit          Check for vulnerabilities"
	@echo "  make outdated       Check for outdated deps"
	@echo ""
	@echo "$(GREEN)Evidence:$(RESET)"
	@echo "  make evidence-crash-recovery  Simulate crash + verify WAL recovery"
	@echo ""
