Inline tests to migrate (integration-like tests currently inside src/*)

Criteria
- Uses async runtime (#[tokio::test]) and spins up disk/buffer/catalog contexts
- Crosses module boundaries (planner + catalog, execution + storage, recovery + WAL)
- Performs I/O (temp dirs, db/log files) or multi-component wiring

Why migrate
- Keep unit tests inline and focused; move integration/system tests under tests/
- Reuse shared utilities in tests/common
- Enable clearer suite grouping (execution, planner, storage, recovery, buffer)

Candidates by module

1) src/common/db_instance.rs
- test_recovery_integration
  - Rationale: starts DBInstance, uses filesystem (tests/data/*.db/.log), WAL/recovery
  - Suggested location: tests/recovery/db_instance_recovery.rs

2) src/sql/execution/execution_context.rs (#[cfg(test)] mod tests)
- Tests (examples):
  - test_executor_type_create_table_construction
  - test_executor_type_from_constructors
  - test_executor_type_init_and_operations
  - test_execution_context_add_check_option
  - test_executor_type_pattern_matching
  - test_all_executor_type_names
  - test_execution_context_init_check_options
  - test_execution_context_getters_and_setters
  - test_multiple_executor_types_in_context
- Rationale: builds temp disk manager, buffer pool, catalog, transaction context
- Suggested location: tests/execution/execution_context.rs

3) src/sql/planner/plan_builder.rs (#[cfg(test)] mods basic_behaviour, complex_behaviour)
- Basic behaviour (examples):
  - test_simple_selects
  - test_basic_filters
  - test_basic_aggregations
  - test_error_handling
  - test_schema_validation
- Complex behaviour (examples):
  - test_filters, test_joins, test_aggregations
  - test_sorting_and_limits
  - test_complex_queries
  - test_error_handling
- Rationale: constructs disk/buffer/catalog + QueryPlanner; integration of planner with catalog
- Suggested location: tests/planner/plan_builder_*.rs

4) src/sql/execution/execution_engine.rs (#[cfg(test)] with many suites)
- Groups (non-exhaustive by prefix):
  - Create table: test_create_table_*
  - Create index: test_create_index_*
  - Insert: test_insert_*
  - Delete: test_delete_*
  - Update: test_update_*
  - Select/Where/Order by: test_select_* / test_where_* / test_order_by_*
  - Aggregations/Group by: test_count_* test_sum_* test_avg_* test_group_by_*
  - Joins: test_join_* (includes nested/full/self)
- Rationale: end-to-end engine tests wiring catalog, bpm, txn, wal; many use temp disk
- Suggested location: tests/execution/engine_*.rs or sqllogic suites when expressible

5) src/storage/table/table_heap.rs (#[cfg(test)] mod tests)
- Examples:
  - test_basic_storage_operations
  - test_page_management
  - test_page_overflow
  - test_tuple_update
  - test_tuple_meta_update
  - test_page_iteration
  - test_concurrent_access
  - test_invalid_operations
  - test_page_boundary_conditions
  - test_insert_tuple_from_values*
  - test_tuple_too_large_for_page
- Rationale: uses real BufferPoolManager + AsyncDiskManager; exercises multi-page linking
- Suggested location: tests/storage/table_heap.rs

6) src/buffer/buffer_pool_manager_async.rs (#[cfg(test)] async tests)
- Examples:
  - test_async_basic_page_operations
  - test_async_disk_integration
  - test_async_batch_flush
  - test_run_async_operation_* (success, error_handling, timeout, complex, concurrent)
- Rationale: async integration with disk I/O and background ops
- Suggested location: tests/buffer/async_bpm.rs

7) src/recovery/* (log_manager.rs, log_recovery.rs, log_iterator.rs)
- Contains #[cfg(test)] with async tests (not exhaustively listed here)
- Rationale: exercises WAL/recovery flows over real disk manager
- Suggested location: tests/recovery/*.rs

Notes on migration
- Replace custom inline TestContext with tests/common/tempdb.rs helpers
- Initialize logging via tests/common/logger.rs
- Prefer sqllogic (.slt) for SQL-behavior-only cases; keep engine API tests in Rust
- Keep unit-level pure logic tests inline; migrate only those wiring multiple subsystems

Proposed mapping (high level)
- Execution engine suites → tests/execution/engine_*.rs
- Execution context enum/tests → tests/execution/execution_context.rs
- Planner explain/validation → tests/planner/plan_builder_*.rs
- Buffer pool async ops → tests/buffer/async_bpm.rs
- TableHeap multi-page/storage → tests/storage/table_heap.rs
- Recovery/WAL → tests/recovery/*.rs


