TKDB Testing Structure

Overview
TKDB uses a layered testing strategy:
- Unit tests: Inline next to module code using #[cfg(test)] in src/* (preferred per project rule)
- Integration tests: Under tests/ organized by domain and suites
- SQLLogic tests: Under tests/sqllogic using sqllogictest runner
- Benchmarks: Under benches/ using criterion

Conventions
- Keep unit tests inline within their respective module files
- Use tests/common for reusable integration test utilities (temp DB configs, logging setup, helpers)
- Prefer deterministic tests (no wall-clock sleeps); use tokio::test for async
- Write both success and error-path tests

Directory Layout
tests/
  common/                 # shared helpers for integration tests
  concurrency/            # concurrency-focused integration tests
  storage/                # storage and buffer-related tests
  serialization/          # encoding/decoding tests
  txn/                    # transaction workflow tests
  sqllogic/               # sqllogictest suites and runner
    ddl/
    dml/
    queries/
    indexes/
    constraints/
    functions/
    transactions/
    test_runner.rs

Common Utilities
- tests/common/logger.rs: initializes test logger via tkdb::common::logger
- tests/common/tempdb.rs: helpers to construct temporary DBInstance configs
- tests/common/macros.rs: optional convenience macros for assertions

Running Tests
- cargo test                   # all unit + integration tests
- cargo test --tests           # integration tests only
- cargo test -p tkdb -- --nocapture RUST_LOG=info

SQLLogic Tests
The runner at tests/sqllogic/test_runner.rs executes .slt files in subdirectories. Each .slt is self-contained; the runner instantiates a fresh database per connection and cleans up files under tests/temp.

Adding New Tests
1. For module-specific logic, add inline #[cfg(test)] tests next to code
2. For cross-module flows, create a new file under tests/<domain>/
3. For SQL behavior, add .slt under tests/sqllogic/<suite>/ and optionally extend the runner

Notes
- Ensure tests leave no persistent files; use tests/temp and clean it up
- Keep tests fast; prefer small buffer pools and minimal data sizes


