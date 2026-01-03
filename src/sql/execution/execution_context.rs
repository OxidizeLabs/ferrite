//! # Execution Context
//!
//! This module provides the [`ExecutionContext`] and [`ExecutorType`], which together form
//! the runtime environment for query execution. The context holds shared state (catalog,
//! buffer pool, transaction) while the executor enum provides type-safe, zero-overhead
//! dispatch to concrete executor implementations.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────┐
//! │                              Execution Context                                      │
//! ├─────────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                     │
//! │  ┌───────────────────────────────────────────────────────────────────────────────┐  │
//! │  │                         ExecutionContext                                      │  │
//! │  ├───────────────────────────────────────────────────────────────────────────────┤  │
//! │  │  buffer_pool_manager ──────▶ Arc<BufferPoolManager>                           │  │
//! │  │  catalog ──────────────────▶ Arc<RwLock<Catalog>>                             │  │
//! │  │  transaction_context ──────▶ Arc<TransactionContext>                          │  │
//! │  │  check_options ────────────▶ Arc<CheckOptions>                                │  │
//! │  │  nlj_check_exec_set ───────▶ VecDeque<(ExecutorPair)>                         │  │
//! │  └───────────────────────────────────────────────────────────────────────────────┘  │
//! │                             │                                                       │
//! │                             ▼                                                       │
//! │  ┌───────────────────────────────────────────────────────────────────────────────┐  │
//! │  │                         ExecutorType (enum)                                   │  │
//! │  ├───────────────────────────────────────────────────────────────────────────────┤  │
//! │  │  SeqScan │ IndexScan │ Filter │ Projection │ Sort │ Limit │ ...               │  │
//! │  │          │           │        │            │      │       │                   │  │
//! │  │  All variants implement: init(), next(), get_output_schema()                  │  │
//! │  └───────────────────────────────────────────────────────────────────────────────┘  │
//! │                                                                                     │
//! └─────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`ExecutionContext`] | Shared state container for query execution |
//! | [`ExecutorType`] | Type-safe enum wrapping all executor implementations |
//! | `NLJCheckExecSet` | Queue of executor pairs for nested-loop join optimization |
//! | `CheckOptions` | Runtime optimization flags |
//!
//! ## ExecutionContext Fields
//!
//! | Field | Type | Purpose |
//! |-------|------|---------|
//! | `buffer_pool_manager` | `Arc<BufferPoolManager>` | Page I/O for data access |
//! | `catalog` | `Arc<RwLock<Catalog>>` | Schema and table metadata |
//! | `transaction_context` | `Arc<TransactionContext>` | Current transaction state |
//! | `check_options` | `Arc<CheckOptions>` | Optimization control flags |
//! | `is_delete` | `bool` | Marks DELETE operations for special handling |
//! | `chain_after_transaction` | `bool` | COMMIT AND CHAIN support |
//!
//! ## Supported Executor Types
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────┐
//! │                              Executor Categories                                    │
//! ├─────────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                     │
//! │   Scan Executors          Join Executors           DML Executors                   │
//! │   ──────────────          ──────────────           ─────────────                   │
//! │   SeqScan                 NestedLoopJoin           Insert                          │
//! │   IndexScan               NestedIndexJoin          Update                          │
//! │   TableScan               HashJoin                 Delete                          │
//! │                                                                                     │
//! │   Transform Executors     DDL Executors            Transaction Executors           │
//! │   ───────────────────     ─────────────            ──────────────────────          │
//! │   Filter                  CreateTable              StartTransaction                │
//! │   Projection              CreateIndex              CommitTransaction               │
//! │   Sort                                             RollbackTransaction             │
//! │   Limit                                                                            │
//! │   TopN                                                                             │
//! │   TopNPerGroup                                                                     │
//! │                                                                                     │
//! │   Aggregate Executors     Utility Executors                                        │
//! │   ───────────────────     ─────────────────                                        │
//! │   Aggregation             Command                                                  │
//! │   Window                  Values                                                   │
//! │                           Mock / MockScan                                          │
//! │                                                                                     │
//! └─────────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ### Creating an Execution Context
//!
//! ```rust,ignore
//! let ctx = ExecutionContext::new(
//!     buffer_pool_manager.clone(),
//!     catalog.clone(),
//!     transaction_context.clone(),
//! );
//! ```
//!
//! ### Working with Executors
//!
//! ```rust,ignore
//! // Create executor via enum
//! let mut executor = ExecutorType::from_seq_scan(seq_scan_executor);
//!
//! // Initialize and iterate
//! executor.init();
//! while let Ok(Some((tuple, rid))) = executor.next() {
//!     // Process tuple
//! }
//!
//! // Access schema
//! let schema = executor.get_output_schema();
//! ```
//!
//! ### Adding NLJ Check Options
//!
//! ```rust,ignore
//! context.add_check_option_from_executor_type(left_exec, right_exec);
//! ```
//!
//! ## Design: Enum vs Dynamic Dispatch
//!
//! This module uses an enum (`ExecutorType`) instead of `Box<dyn AbstractExecutor>`:
//!
//! | Aspect | Enum Approach | Dynamic Dispatch |
//! |--------|---------------|------------------|
//! | **Dispatch Cost** | Zero (direct calls) | vtable lookup |
//! | **Memory** | Stack-allocated | Heap-allocated |
//! | **Type Safety** | Compile-time | Runtime |
//! | **Pattern Matching** | Native support | Requires downcasting |
//! | **Binary Size** | Larger (monomorphization) | Smaller |
//! | **Extensibility** | Requires enum modification | Just implement trait |
//!
//! ## Thread Safety
//!
//! - `ExecutorType` implements `Send + Sync` (all variants are thread-safe)
//! - `ExecutionContext` uses `Arc`-wrapped shared state
//! - `Mutex<ExecutorType>` used in `nlj_check_exec_set` for safe concurrent access
//!
//! ## Performance Characteristics
//!
//! - **Dispatch**: Zero overhead (direct function calls via match)
//! - **Memory**: Minimal overhead (enum tag + largest variant size)
//! - **Thread Safety**: Lock-free for most operations
//! - **Compile Time**: Slightly increased due to monomorphization

use std::collections::VecDeque;
use std::sync::Arc;

use log::debug;
use parking_lot::{Mutex, RwLock};

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::Catalog;
use crate::sql::execution::check_option::{CheckOption, CheckOptions};
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::aggregation_executor::AggregationExecutor;
use crate::sql::execution::executors::command_executor::CommandExecutor;
use crate::sql::execution::executors::commit_transaction_executor::CommitTransactionExecutor;
use crate::sql::execution::executors::create_index_executor::CreateIndexExecutor;
use crate::sql::execution::executors::create_table_executor::CreateTableExecutor;
use crate::sql::execution::executors::delete_executor::DeleteExecutor;
use crate::sql::execution::executors::filter_executor::FilterExecutor;
use crate::sql::execution::executors::hash_join_executor::HashJoinExecutor;
use crate::sql::execution::executors::index_scan_executor::IndexScanExecutor;
use crate::sql::execution::executors::insert_executor::InsertExecutor;
use crate::sql::execution::executors::limit_executor::LimitExecutor;
use crate::sql::execution::executors::mock_executor::MockExecutor;
use crate::sql::execution::executors::mock_scan_executor::MockScanExecutor;
use crate::sql::execution::executors::nested_index_join_executor::NestedIndexJoinExecutor;
use crate::sql::execution::executors::nested_loop_join_executor::NestedLoopJoinExecutor;
use crate::sql::execution::executors::projection_executor::ProjectionExecutor;
use crate::sql::execution::executors::rollback_transaction_executor::RollbackTransactionExecutor;
use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::sql::execution::executors::sort_executor::SortExecutor;
use crate::sql::execution::executors::start_transaction_executor::StartTransactionExecutor;
use crate::sql::execution::executors::table_scan_executor::TableScanExecutor;
use crate::sql::execution::executors::topn_executor::TopNExecutor;
use crate::sql::execution::executors::topn_per_group_executor::TopNPerGroupExecutor;
use crate::sql::execution::executors::update_executor::UpdateExecutor;
use crate::sql::execution::executors::values_executor::ValuesExecutor;
use crate::sql::execution::executors::window_executor::WindowExecutor;
use crate::sql::execution::transaction_context::TransactionContext;

/// A pair of executors used for nested-loop join optimization checks.
///
/// Each pair represents the left and right sides of a potential join operation.
/// Both executors are wrapped in `Arc<Mutex<>>` for thread-safe shared access.
type ExecutorPair = (Arc<Mutex<ExecutorType>>, Arc<Mutex<ExecutorType>>);

/// Queue of executor pairs for nested-loop join (NLJ) optimization validation.
///
/// During query optimization, potential NLJ operations are queued here for
/// runtime verification that the chosen join strategy is efficient.
type NLJCheckExecSet = VecDeque<ExecutorPair>;

/// Type-safe executor enum that provides zero-overhead dispatch to concrete executor implementations.
///
/// This enum wraps all executor types in the system, enabling direct function calls
/// via pattern matching instead of dynamic dispatch through vtables. This approach
/// provides better performance at the cost of slightly larger binary size.
///
/// # Design Rationale
///
/// Using an enum instead of `Box<dyn AbstractExecutor>` provides:
/// - **Zero dispatch cost**: Direct function calls via match
/// - **Stack allocation**: No heap allocation for the enum itself
/// - **Compile-time safety**: All variants known at compile time
/// - **Native pattern matching**: Easy type inspection and handling
///
/// # Thread Safety
///
/// All variants are thread-safe, and the enum itself implements `Send + Sync`.
#[allow(clippy::large_enum_variant)] // Executors are not frequently moved; boxing would add indirection
pub enum ExecutorType {
    /// Executor for aggregate operations (COUNT, SUM, AVG, etc.).
    Aggregation(AggregationExecutor),
    /// Executor for utility commands (SET, SHOW, EXPLAIN).
    Command(CommandExecutor),
    /// Executor for COMMIT TRANSACTION statements.
    CommitTransaction(CommitTransactionExecutor),
    /// Executor for CREATE INDEX statements.
    CreateIndex(CreateIndexExecutor),
    /// Executor for CREATE TABLE statements.
    CreateTable(CreateTableExecutor),
    /// Executor for DELETE statements.
    Delete(DeleteExecutor),
    /// Executor for WHERE clause filtering.
    Filter(FilterExecutor),
    /// Executor for hash-based join operations.
    HashJoin(HashJoinExecutor),
    /// Executor for index-based scans.
    IndexScan(IndexScanExecutor),
    /// Executor for INSERT statements.
    Insert(InsertExecutor),
    /// Executor for LIMIT clause processing.
    Limit(LimitExecutor),
    /// Mock executor for testing purposes.
    Mock(MockExecutor),
    /// Mock scan executor for testing purposes.
    MockScan(MockScanExecutor),
    /// Executor for index-nested-loop joins.
    NestedIndexJoin(NestedIndexJoinExecutor),
    /// Executor for nested-loop joins without index.
    NestedLoopJoin(NestedLoopJoinExecutor),
    /// Executor for SELECT column projections.
    Projection(ProjectionExecutor),
    /// Executor for ROLLBACK TRANSACTION statements.
    RollbackTransaction(RollbackTransactionExecutor),
    /// Executor for sequential full-table scans.
    SeqScan(SeqScanExecutor),
    /// Executor for ORDER BY sorting.
    Sort(SortExecutor),
    /// Executor for BEGIN TRANSACTION statements.
    StartTransaction(StartTransactionExecutor),
    /// Executor for table scans with predicate pushdown.
    TableScan(TableScanExecutor),
    /// Executor for ORDER BY ... LIMIT N (top-N) queries.
    TopN(TopNExecutor),
    /// Executor for top-N within groups (window-like behavior).
    TopNPerGroup(TopNPerGroupExecutor),
    /// Executor for UPDATE statements.
    Update(UpdateExecutor),
    /// Executor for VALUES clauses in INSERT statements.
    Values(ValuesExecutor),
    /// Executor for window functions (ROW_NUMBER, RANK, etc.).
    Window(WindowExecutor),
}

impl ExecutorType {
    // ==================== Constructor Methods ====================
    //
    // Factory methods for wrapping concrete executor instances into the enum.
    // Each method takes ownership of the executor and returns an `ExecutorType`.

    /// Wraps an `AggregationExecutor` into the enum.
    pub fn from_aggregation(executor: AggregationExecutor) -> Self {
        ExecutorType::Aggregation(executor)
    }

    /// Wraps a `CommandExecutor` into the enum.
    pub fn from_command(executor: CommandExecutor) -> Self {
        ExecutorType::Command(executor)
    }

    /// Wraps a `CommitTransactionExecutor` into the enum.
    pub fn from_commit_transaction(executor: CommitTransactionExecutor) -> Self {
        ExecutorType::CommitTransaction(executor)
    }

    /// Wraps a `CreateIndexExecutor` into the enum.
    pub fn from_create_index(executor: CreateIndexExecutor) -> Self {
        ExecutorType::CreateIndex(executor)
    }

    /// Wraps a `CreateTableExecutor` into the enum.
    pub fn from_create_table(executor: CreateTableExecutor) -> Self {
        ExecutorType::CreateTable(executor)
    }

    /// Wraps a `DeleteExecutor` into the enum.
    pub fn from_delete(executor: DeleteExecutor) -> Self {
        ExecutorType::Delete(executor)
    }

    /// Wraps a `FilterExecutor` into the enum.
    pub fn from_filter(executor: FilterExecutor) -> Self {
        ExecutorType::Filter(executor)
    }

    /// Wraps a `HashJoinExecutor` into the enum.
    pub fn from_hash_join(executor: HashJoinExecutor) -> Self {
        ExecutorType::HashJoin(executor)
    }

    /// Wraps an `IndexScanExecutor` into the enum.
    pub fn from_index_scan(executor: IndexScanExecutor) -> Self {
        ExecutorType::IndexScan(executor)
    }

    /// Wraps an `InsertExecutor` into the enum.
    pub fn from_insert(executor: InsertExecutor) -> Self {
        ExecutorType::Insert(executor)
    }

    /// Wraps a `LimitExecutor` into the enum.
    pub fn from_limit(executor: LimitExecutor) -> Self {
        ExecutorType::Limit(executor)
    }

    /// Wraps a `MockExecutor` into the enum (for testing).
    pub fn from_mock(executor: MockExecutor) -> Self {
        ExecutorType::Mock(executor)
    }

    /// Wraps a `MockScanExecutor` into the enum (for testing).
    pub fn from_mock_scan(executor: MockScanExecutor) -> Self {
        ExecutorType::MockScan(executor)
    }

    /// Wraps a `NestedIndexJoinExecutor` into the enum.
    pub fn from_nested_index_join(executor: NestedIndexJoinExecutor) -> Self {
        ExecutorType::NestedIndexJoin(executor)
    }

    /// Wraps a `NestedLoopJoinExecutor` into the enum.
    pub fn from_nested_loop_join(executor: NestedLoopJoinExecutor) -> Self {
        ExecutorType::NestedLoopJoin(executor)
    }

    /// Wraps a `ProjectionExecutor` into the enum.
    pub fn from_projection(executor: ProjectionExecutor) -> Self {
        ExecutorType::Projection(executor)
    }

    /// Wraps a `RollbackTransactionExecutor` into the enum.
    pub fn from_rollback_transaction(executor: RollbackTransactionExecutor) -> Self {
        ExecutorType::RollbackTransaction(executor)
    }

    /// Wraps a `SeqScanExecutor` into the enum.
    pub fn from_seq_scan(executor: SeqScanExecutor) -> Self {
        ExecutorType::SeqScan(executor)
    }

    /// Wraps a `SortExecutor` into the enum.
    pub fn from_sort(executor: SortExecutor) -> Self {
        ExecutorType::Sort(executor)
    }

    /// Wraps a `StartTransactionExecutor` into the enum.
    pub fn from_start_transaction(executor: StartTransactionExecutor) -> Self {
        ExecutorType::StartTransaction(executor)
    }

    /// Wraps a `TableScanExecutor` into the enum.
    pub fn from_table_scan(executor: TableScanExecutor) -> Self {
        ExecutorType::TableScan(executor)
    }

    /// Wraps a `TopNExecutor` into the enum.
    pub fn from_topn(executor: TopNExecutor) -> Self {
        ExecutorType::TopN(executor)
    }

    /// Wraps a `TopNPerGroupExecutor` into the enum.
    pub fn from_topn_per_group(executor: TopNPerGroupExecutor) -> Self {
        ExecutorType::TopNPerGroup(executor)
    }

    /// Wraps an `UpdateExecutor` into the enum.
    pub fn from_update(executor: UpdateExecutor) -> Self {
        ExecutorType::Update(executor)
    }

    /// Wraps a `ValuesExecutor` into the enum.
    pub fn from_values(executor: ValuesExecutor) -> Self {
        ExecutorType::Values(executor)
    }

    /// Wraps a `WindowExecutor` into the enum.
    pub fn from_window(executor: WindowExecutor) -> Self {
        ExecutorType::Window(executor)
    }

    // ==================== Executor Interface Methods ====================
    //
    // These methods delegate to the corresponding methods on the wrapped executor,
    // implementing the Volcano-style iterator model (init/next pattern).

    /// Initializes the executor, preparing it for tuple iteration.
    ///
    /// This method must be called before the first call to [`next()`](Self::next).
    /// It performs any necessary setup such as opening child executors, building
    /// hash tables, or sorting data.
    pub fn init(&mut self) {
        match self {
            ExecutorType::Aggregation(executor) => executor.init(),
            ExecutorType::Command(executor) => executor.init(),
            ExecutorType::CommitTransaction(executor) => executor.init(),
            ExecutorType::CreateIndex(executor) => executor.init(),
            ExecutorType::CreateTable(executor) => executor.init(),
            ExecutorType::Delete(executor) => executor.init(),
            ExecutorType::Filter(executor) => executor.init(),
            ExecutorType::HashJoin(executor) => executor.init(),
            ExecutorType::IndexScan(executor) => executor.init(),
            ExecutorType::Insert(executor) => executor.init(),
            ExecutorType::Limit(executor) => executor.init(),
            ExecutorType::Mock(executor) => executor.init(),
            ExecutorType::MockScan(executor) => executor.init(),
            ExecutorType::NestedIndexJoin(executor) => executor.init(),
            ExecutorType::NestedLoopJoin(executor) => executor.init(),
            ExecutorType::Projection(executor) => executor.init(),
            ExecutorType::RollbackTransaction(executor) => executor.init(),
            ExecutorType::SeqScan(executor) => executor.init(),
            ExecutorType::Sort(executor) => executor.init(),
            ExecutorType::StartTransaction(executor) => executor.init(),
            ExecutorType::TableScan(executor) => executor.init(),
            ExecutorType::TopN(executor) => executor.init(),
            ExecutorType::TopNPerGroup(executor) => executor.init(),
            ExecutorType::Update(executor) => executor.init(),
            ExecutorType::Values(executor) => executor.init(),
            ExecutorType::Window(executor) => executor.init(),
        }
    }

    /// Returns the next tuple from the executor, or `None` if exhausted.
    ///
    /// This is the core iteration method in the Volcano model. Each call pulls
    /// one tuple from the executor tree. The executor may in turn pull from
    /// child executors to produce the result.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - The next tuple and its record ID
    /// * `Ok(None)` - No more tuples available (executor exhausted)
    /// * `Err(DBError)` - An error occurred during execution
    #[allow(clippy::should_implement_trait)] // Volcano-model `next()` differs from Iterator
    pub fn next(
        &mut self,
    ) -> Result<
        Option<(
            Arc<crate::storage::table::tuple::Tuple>,
            crate::common::rid::RID,
        )>,
        crate::common::exception::DBError,
    > {
        match self {
            ExecutorType::Aggregation(executor) => executor.next(),
            ExecutorType::Command(executor) => executor.next(),
            ExecutorType::CommitTransaction(executor) => executor.next(),
            ExecutorType::CreateIndex(executor) => executor.next(),
            ExecutorType::CreateTable(executor) => executor.next(),
            ExecutorType::Delete(executor) => executor.next(),
            ExecutorType::Filter(executor) => executor.next(),
            ExecutorType::HashJoin(executor) => executor.next(),
            ExecutorType::IndexScan(executor) => executor.next(),
            ExecutorType::Insert(executor) => executor.next(),
            ExecutorType::Limit(executor) => executor.next(),
            ExecutorType::Mock(executor) => executor.next(),
            ExecutorType::MockScan(executor) => executor.next(),
            ExecutorType::NestedIndexJoin(executor) => executor.next(),
            ExecutorType::NestedLoopJoin(executor) => executor.next(),
            ExecutorType::Projection(executor) => executor.next(),
            ExecutorType::RollbackTransaction(executor) => executor.next(),
            ExecutorType::SeqScan(executor) => executor.next(),
            ExecutorType::Sort(executor) => executor.next(),
            ExecutorType::StartTransaction(executor) => executor.next(),
            ExecutorType::TableScan(executor) => executor.next(),
            ExecutorType::TopN(executor) => executor.next(),
            ExecutorType::TopNPerGroup(executor) => executor.next(),
            ExecutorType::Update(executor) => executor.next(),
            ExecutorType::Values(executor) => executor.next(),
            ExecutorType::Window(executor) => executor.next(),
        }
    }

    /// Returns the output schema describing the tuples produced by this executor.
    ///
    /// The schema defines the column names, types, and order of values in each
    /// tuple returned by [`next()`](Self::next). This is used for result formatting
    /// and for child executors to understand their input schema.
    pub fn get_output_schema(&self) -> &crate::catalog::schema::Schema {
        match self {
            ExecutorType::Aggregation(executor) => executor.get_output_schema(),
            ExecutorType::Command(executor) => executor.get_output_schema(),
            ExecutorType::CommitTransaction(executor) => executor.get_output_schema(),
            ExecutorType::CreateIndex(executor) => executor.get_output_schema(),
            ExecutorType::CreateTable(executor) => executor.get_output_schema(),
            ExecutorType::Delete(executor) => executor.get_output_schema(),
            ExecutorType::Filter(executor) => executor.get_output_schema(),
            ExecutorType::HashJoin(executor) => executor.get_output_schema(),
            ExecutorType::IndexScan(executor) => executor.get_output_schema(),
            ExecutorType::Insert(executor) => executor.get_output_schema(),
            ExecutorType::Limit(executor) => executor.get_output_schema(),
            ExecutorType::Mock(executor) => executor.get_output_schema(),
            ExecutorType::MockScan(executor) => executor.get_output_schema(),
            ExecutorType::NestedIndexJoin(executor) => executor.get_output_schema(),
            ExecutorType::NestedLoopJoin(executor) => executor.get_output_schema(),
            ExecutorType::Projection(executor) => executor.get_output_schema(),
            ExecutorType::RollbackTransaction(executor) => executor.get_output_schema(),
            ExecutorType::SeqScan(executor) => executor.get_output_schema(),
            ExecutorType::Sort(executor) => executor.get_output_schema(),
            ExecutorType::StartTransaction(executor) => executor.get_output_schema(),
            ExecutorType::TableScan(executor) => executor.get_output_schema(),
            ExecutorType::TopN(executor) => executor.get_output_schema(),
            ExecutorType::TopNPerGroup(executor) => executor.get_output_schema(),
            ExecutorType::Update(executor) => executor.get_output_schema(),
            ExecutorType::Values(executor) => executor.get_output_schema(),
            ExecutorType::Window(executor) => executor.get_output_schema(),
        }
    }

    /// Returns the execution context shared by all executors in the tree.
    ///
    /// The context provides access to shared resources (buffer pool, catalog,
    /// transaction) needed during query execution. All executors in a query
    /// tree share the same context.
    pub fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        match self {
            ExecutorType::Aggregation(executor) => executor.get_executor_context(),
            ExecutorType::Command(executor) => executor.get_executor_context(),
            ExecutorType::CommitTransaction(executor) => executor.get_executor_context(),
            ExecutorType::CreateIndex(executor) => executor.get_executor_context(),
            ExecutorType::CreateTable(executor) => executor.get_executor_context(),
            ExecutorType::Delete(executor) => executor.get_executor_context(),
            ExecutorType::Filter(executor) => executor.get_executor_context(),
            ExecutorType::HashJoin(executor) => executor.get_executor_context(),
            ExecutorType::IndexScan(executor) => executor.get_executor_context(),
            ExecutorType::Insert(executor) => executor.get_executor_context(),
            ExecutorType::Limit(executor) => executor.get_executor_context(),
            ExecutorType::Mock(executor) => executor.get_executor_context(),
            ExecutorType::MockScan(executor) => executor.get_executor_context(),
            ExecutorType::NestedIndexJoin(executor) => executor.get_executor_context(),
            ExecutorType::NestedLoopJoin(executor) => executor.get_executor_context(),
            ExecutorType::Projection(executor) => executor.get_executor_context(),
            ExecutorType::RollbackTransaction(executor) => executor.get_executor_context(),
            ExecutorType::SeqScan(executor) => executor.get_executor_context(),
            ExecutorType::Sort(executor) => executor.get_executor_context(),
            ExecutorType::StartTransaction(executor) => executor.get_executor_context(),
            ExecutorType::TableScan(executor) => executor.get_executor_context(),
            ExecutorType::TopN(executor) => executor.get_executor_context(),
            ExecutorType::TopNPerGroup(executor) => executor.get_executor_context(),
            ExecutorType::Update(executor) => executor.get_executor_context(),
            ExecutorType::Values(executor) => executor.get_executor_context(),
            ExecutorType::Window(executor) => executor.get_executor_context(),
        }
    }
}

// SAFETY: All executor variants contain only `Send + Sync` types:
// - `Arc<T>` where T: Send + Sync
// - `parking_lot::RwLock<T>` (Send + Sync when T is)
// - Primitive types and standard library thread-safe containers
//
// This enables `ExecutorType` to be shared across threads when wrapped
// in appropriate synchronization primitives (e.g., `Arc<Mutex<ExecutorType>>`).
unsafe impl Send for ExecutorType {}
unsafe impl Sync for ExecutorType {}

/// Shared execution state container for query processing.
///
/// The `ExecutionContext` holds all runtime state needed during query execution,
/// including access to the buffer pool, catalog, and current transaction. It is
/// shared (via `Arc<RwLock<>>`) among all executors in a query tree.
///
/// # Responsibilities
///
/// - **Resource Access**: Provides access to buffer pool and catalog
/// - **Transaction State**: Maintains current transaction context
/// - **Optimization Hints**: Stores runtime optimization options
/// - **Operation Flags**: Tracks special operation modes (DELETE, chaining)
///
/// # Thread Safety
///
/// The context is designed to be shared across threads using `Arc<RwLock<ExecutionContext>>`.
/// Internal fields use appropriate synchronization primitives.
///
/// # Example
///
/// ```rust,ignore
/// let ctx = ExecutionContext::new(
///     buffer_pool_manager.clone(),
///     catalog.clone(),
///     transaction_context.clone(),
/// );
///
/// // Access resources
/// let bpm = ctx.get_buffer_pool_manager();
/// let catalog = ctx.get_catalog();
/// ```
pub struct ExecutionContext {
    /// Buffer pool manager for page-level I/O operations.
    buffer_pool_manager: Arc<BufferPoolManager>,

    /// Catalog containing schema metadata for all tables and indexes.
    catalog: Arc<RwLock<Catalog>>,

    /// Current transaction context with lock manager and transaction state.
    transaction_context: Arc<TransactionContext>,

    /// Queue of executor pairs for nested-loop join optimization checks.
    nlj_check_exec_set: NLJCheckExecSet,

    /// Runtime optimization flags controlling query execution behavior.
    check_options: Arc<CheckOptions>,

    /// Flag indicating this is a DELETE operation (affects tuple visibility).
    is_delete: bool,

    /// Flag for `COMMIT AND CHAIN` / `ROLLBACK AND CHAIN` support.
    chain_after_transaction: bool,
}

impl ExecutionContext {
    /// Creates a new execution context with the provided components.
    ///
    /// Initializes the context with default optimization options including
    /// predicate pushdown and top-N optimizations enabled.
    ///
    /// # Arguments
    ///
    /// * `buffer_pool_manager` - Buffer pool for page I/O operations
    /// * `catalog` - Catalog for schema metadata access
    /// * `transaction_context` - Current transaction state and lock manager
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let ctx = ExecutionContext::new(
    ///     buffer_pool_manager.clone(),
    ///     catalog.clone(),
    ///     transaction_context.clone(),
    /// );
    /// ```
    pub fn new(
        buffer_pool_manager: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_context: Arc<TransactionContext>,
    ) -> Self {
        debug!(
            "Creating ExecutorContext for transaction {}",
            transaction_context.get_transaction_id()
        );

        // Create check options and enable necessary optimizations
        let mut options = CheckOptions::new();
        options.add_check(CheckOption::EnablePushdownCheck);
        options.add_check(CheckOption::EnableTopnCheck);

        Self {
            buffer_pool_manager,
            catalog,
            transaction_context,
            nlj_check_exec_set: VecDeque::new(),
            check_options: Arc::new(options),
            is_delete: false,
            chain_after_transaction: false,
        }
    }

    /// Returns the current transaction context.
    ///
    /// The transaction context provides access to the active transaction,
    /// lock manager, and transaction manager for concurrency control.
    pub fn get_transaction_context(&self) -> Arc<TransactionContext> {
        self.transaction_context.clone()
    }

    /// Returns a reference to the catalog.
    ///
    /// The catalog contains metadata about all tables, indexes, and schemas
    /// in the database.
    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
    }

    /// Returns the buffer pool manager for page I/O operations.
    pub fn get_buffer_pool_manager(&self) -> Arc<BufferPoolManager> {
        self.buffer_pool_manager.clone()
    }

    /// Returns the queue of executor pairs for NLJ optimization checks.
    pub fn get_nlj_check_exec_set(&self) -> &NLJCheckExecSet {
        &self.nlj_check_exec_set
    }

    /// Returns the current runtime optimization options.
    pub fn get_check_options(&self) -> Arc<CheckOptions> {
        Arc::clone(&self.check_options)
    }

    /// Replaces the current optimization options with new ones.
    ///
    /// # Arguments
    ///
    /// * `options` - New set of optimization options to apply
    pub fn set_check_options(&mut self, options: CheckOptions) {
        debug!("Setting check options");
        self.check_options = Arc::new(options);
    }

    /// Adds a pair of executors to the NLJ optimization check queue.
    ///
    /// # Arguments
    ///
    /// * `left_exec` - Left side executor of the potential join
    /// * `right_exec` - Right side executor of the potential join
    pub fn add_check_option(
        &mut self,
        left_exec: Arc<Mutex<ExecutorType>>,
        right_exec: Arc<Mutex<ExecutorType>>,
    ) {
        self.nlj_check_exec_set.push_back((left_exec, right_exec));
    }

    /// Convenience method for adding NLJ check options from owned executor types.
    ///
    /// Wraps both executors in `Arc<Mutex<>>` and adds them to the check queue.
    ///
    /// # Arguments
    ///
    /// * `left_exec` - Left side executor (takes ownership)
    /// * `right_exec` - Right side executor (takes ownership)
    pub fn add_check_option_from_executor_type(
        &mut self,
        left_exec: ExecutorType,
        right_exec: ExecutorType,
    ) {
        let left_arc = Arc::new(Mutex::new(left_exec));
        let right_arc = Arc::new(Mutex::new(right_exec));
        self.add_check_option(left_arc, right_arc);
    }

    /// Initializes optimization options based on current executor state.
    ///
    /// Enables NLJ check if there are executor pairs queued, and always
    /// enables predicate pushdown and top-N optimizations.
    pub fn init_check_options(&mut self) {
        let mut options = CheckOptions::new();

        if !self.nlj_check_exec_set.is_empty() {
            options.add_check(CheckOption::EnableNljCheck);
        }

        options.add_check(CheckOption::EnablePushdownCheck);
        options.add_check(CheckOption::EnableTopnCheck);

        self.check_options = Arc::new(options);
    }

    /// Returns whether this context is for a DELETE operation.
    ///
    /// DELETE operations may require special handling for tuple visibility
    /// and write set tracking.
    pub fn is_delete(&self) -> bool {
        self.is_delete
    }

    /// Marks this context as a DELETE operation.
    ///
    /// # Arguments
    ///
    /// * `is_delete` - `true` if this is a DELETE operation
    pub fn set_delete(&mut self, is_delete: bool) {
        self.is_delete = is_delete;
    }

    /// Replaces the current transaction context with a new one.
    ///
    /// Used during transaction chaining to install a new transaction
    /// after commit/rollback.
    ///
    /// # Arguments
    ///
    /// * `txn_ctx` - The new transaction context to use
    pub fn set_transaction_context(&mut self, txn_ctx: Arc<TransactionContext>) {
        self.transaction_context = txn_ctx;
    }

    /// Returns whether a new transaction should be started after commit/rollback.
    ///
    /// This supports the SQL `COMMIT AND CHAIN` and `ROLLBACK AND CHAIN` syntax.
    pub fn should_chain_after_transaction(&self) -> bool {
        self.chain_after_transaction
    }

    /// Sets whether to chain a new transaction after commit/rollback.
    ///
    /// # Arguments
    ///
    /// * `chain` - `true` to start a new transaction after the current one ends
    pub fn set_chain_after_transaction(&mut self, chain: bool) {
        self.chain_after_transaction = chain;
    }

    /// Returns the type name of an executor as a static string.
    ///
    /// Useful for logging and debugging to identify executor types without
    /// pattern matching in calling code.
    ///
    /// # Arguments
    ///
    /// * `executor` - The executor to get the type name for
    ///
    /// # Returns
    ///
    /// A static string like `"SeqScanExecutor"`, `"HashJoinExecutor"`, etc.
    pub fn get_executor_type_name(executor: &ExecutorType) -> &'static str {
        match executor {
            ExecutorType::Aggregation(_) => "AggregationExecutor",
            ExecutorType::Command(_) => "CommandExecutor",
            ExecutorType::CommitTransaction(_) => "CommitTransactionExecutor",
            ExecutorType::CreateIndex(_) => "CreateIndexExecutor",
            ExecutorType::CreateTable(_) => "CreateTableExecutor",
            ExecutorType::Delete(_) => "DeleteExecutor",
            ExecutorType::Filter(_) => "FilterExecutor",
            ExecutorType::HashJoin(_) => "HashJoinExecutor",
            ExecutorType::IndexScan(_) => "IndexScanExecutor",
            ExecutorType::Insert(_) => "InsertExecutor",
            ExecutorType::Limit(_) => "LimitExecutor",
            ExecutorType::Mock(_) => "MockExecutor",
            ExecutorType::MockScan(_) => "MockScanExecutor",
            ExecutorType::NestedIndexJoin(_) => "NestedIndexJoinExecutor",
            ExecutorType::NestedLoopJoin(_) => "NestedLoopJoinExecutor",
            ExecutorType::Projection(_) => "ProjectionExecutor",
            ExecutorType::RollbackTransaction(_) => "RollbackTransactionExecutor",
            ExecutorType::SeqScan(_) => "SeqScanExecutor",
            ExecutorType::Sort(_) => "SortExecutor",
            ExecutorType::StartTransaction(_) => "StartTransactionExecutor",
            ExecutorType::TableScan(_) => "TableScanExecutor",
            ExecutorType::TopN(_) => "TopNExecutor",
            ExecutorType::TopNPerGroup(_) => "TopNPerGroupExecutor",
            ExecutorType::Update(_) => "UpdateExecutor",
            ExecutorType::Values(_) => "ValuesExecutor",
            ExecutorType::Window(_) => "WindowExecutor",
        }
    }
}

// Inline tests moved to tests/execution/execution_context.rs
