Ferrite Testing Structure

Overview
Ferrite uses a layered testing strategy:
- Unit tests: Inline next to module code using #[cfg(test)] in src/* (preferred per project rule)
- Integration tests: In tests/* for cross-module and end-to-end testing

Directory Layout
tests/
├── buffer/           # Buffer pool integration tests
├── catalog/          # Catalog system tests
├── common/           # Shared test utilities (logger, tempdb)
├── concurrency/      # Transaction & lock manager tests
├── execution/        # SQL execution engine tests
├── network/          # Client-server protocol tests
├── planner/          # Query planner tests
├── recovery/         # WAL & crash recovery tests
├── server/           # Server lifecycle tests
├── sqllogic/         # SQL logic test runner (DDL/DML/queries)
├── storage/          # Disk & index layer tests
└── integration_tests.rs  # Top-level imports

SQLLogic Tests
Located in tests/sqllogic/, these follow the sqllogictest format:
- tests/sqllogic/ddl/   — CREATE/DROP statements
- tests/sqllogic/dml/   — INSERT/UPDATE/DELETE
- tests/sqllogic/queries/ — SELECT queries
Runner: tests/sqllogic/test_runner.rs

Common Utilities
- tests/common/logger.rs: initializes test logger via ferrite::common::logger
- tests/common/tempdb.rs: helpers to construct temporary DBInstance configs

Running Tests
- cargo test                   # all tests (unit + integration)
- cargo test --tests           # integration tests only
- cargo test -p ferrite -- --nocapture RUST_LOG=info
