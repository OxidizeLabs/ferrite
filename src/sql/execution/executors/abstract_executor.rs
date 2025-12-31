//! # Abstract Executor Module
//!
//! This module defines the core abstractions for query execution in Ferrite's
//! SQL execution engine. It implements the **Volcano iterator model** (also known
//! as the tuple-at-a-time iterator model), a classic approach to query processing
//! in database systems.
//!
//! ## Volcano Iterator Model
//!
//! The Volcano model represents query plans as trees of iterator operators. Each
//! operator implements a simple interface:
//!
//! - **`init()`**: Initializes the operator and its children
//! - **`next()`**: Returns the next tuple, or `None` when exhausted
//!
//! Tuples flow from leaf operators (e.g., table scans) up through the tree to the
//! root, with each operator processing one tuple at a time. This approach offers:
//!
//! - **Memory efficiency**: Only one tuple per operator is in memory at a time
//! - **Composability**: Operators can be freely combined into complex query plans
//! - **Simplicity**: Each operator has a clear, minimal interface
//!
//! ## Module Contents
//!
//! - [`AbstractExecutor`]: The core trait that all executors must implement
//! - [`BaseExecutor`]: A helper struct providing common executor functionality
//!
//! ## Example
//!
//! Executors are typically composed into a tree structure. For example, a simple
//! `SELECT * FROM users WHERE age > 21` might produce:
//!
//! ```text
//!     FilterExecutor (age > 21)
//!            │
//!     SeqScanExecutor (users)
//! ```
//!
//! The execution engine calls `next()` on the root executor, which recursively
//! pulls tuples from its children until the query is complete.

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::storage::table::tuple::Tuple;
use parking_lot::RwLock;
use std::sync::Arc;

/// The core trait for all query executors in the Volcano iterator model.
///
/// `AbstractExecutor` defines the minimal interface that all executors must
/// implement. Executors form a tree structure where parent executors pull
/// tuples from child executors through the `next()` method.
///
/// # Thread Safety
///
/// All executors must be `Send + Sync` to support concurrent query execution
/// and parallel query plans.
///
/// # Lifecycle
///
/// 1. Create the executor with its plan node and child executors
/// 2. Call `init()` to initialize the executor and all children
/// 3. Repeatedly call `next()` until it returns `Ok(None)`
///
/// # Implementors
///
/// Concrete executor implementations include:
/// - Sequential scan executors for full table scans
/// - Index scan executors for index-based lookups
/// - Filter executors for predicate evaluation
/// - Join executors (nested loop, hash join, sort-merge)
/// - Aggregation executors for GROUP BY operations
/// - Sort executors for ORDER BY clauses
/// - Projection executors for column selection
pub trait AbstractExecutor: Send + Sync {
    /// Initializes the executor and prepares it for tuple production.
    ///
    /// This method must be called exactly once before any calls to `next()`.
    /// Implementations should:
    ///
    /// - Initialize any internal state (cursors, iterators, etc.)
    /// - Call `init()` on all child executors
    /// - Prepare any resources needed for execution
    ///
    /// # Panics
    ///
    /// Behavior is undefined if `next()` is called before `init()`. Most
    /// implementations will panic or produce incorrect results.
    fn init(&mut self);

    /// Returns the next tuple from this executor.
    ///
    /// This is the core method of the Volcano iterator model. Each call
    /// returns at most one tuple, allowing for memory-efficient pipelined
    /// execution.
    ///
    /// # Returns
    ///
    /// - `Ok(Some((tuple, rid)))` - A tuple was successfully produced along
    ///   with its Record ID (RID) indicating its location in storage
    /// - `Ok(None)` - No more tuples are available; the executor is exhausted
    /// - `Err(DBError)` - An error occurred during execution (e.g., I/O error,
    ///   constraint violation, transaction abort)
    ///
    /// # Errors
    ///
    /// Returns a [`DBError`] if:
    /// - A storage layer error occurs (disk I/O failure, page corruption)
    /// - A transaction conflict is detected (for MVCC or 2PL)
    /// - A constraint violation is encountered
    /// - Resource limits are exceeded (memory, open files)
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError>;

    /// Returns the output schema describing tuples produced by this executor.
    ///
    /// The schema defines the structure of tuples returned by `next()`,
    /// including column names, types, and ordering. This information is
    /// essential for:
    ///
    /// - Parent executors to correctly interpret tuple data
    /// - Query result formatting and type checking
    /// - Expression evaluation on output tuples
    ///
    /// # Returns
    ///
    /// A reference to the [`Schema`] describing this executor's output tuples.
    fn get_output_schema(&self) -> &Schema;

    /// Returns the execution context for this executor.
    ///
    /// The execution context provides access to shared resources needed
    /// during query execution, including:
    ///
    /// - Buffer pool manager for page access
    /// - Transaction context for ACID guarantees
    /// - Catalog for schema lookups
    /// - Lock manager for concurrency control
    ///
    /// # Returns
    ///
    /// An `Arc<RwLock<ExecutionContext>>` providing thread-safe access to
    /// the shared execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>>;
}

/// A base struct providing common functionality for concrete executor implementations.
///
/// `BaseExecutor` holds the execution context and provides accessor methods
/// that concrete executors can use. While Rust doesn't support traditional
/// inheritance, concrete executors typically embed a `BaseExecutor` and
/// delegate context access to it.
///
/// # Example
///
/// ```ignore
/// pub struct SeqScanExecutor {
///     base: BaseExecutor,
///     table_oid: TableOid,
///     iterator: Option<TableIterator>,
/// }
///
/// impl SeqScanExecutor {
///     pub fn new(exec_ctx: Arc<ExecutionContext>, table_oid: TableOid) -> Self {
///         Self {
///             base: BaseExecutor::new(exec_ctx),
///             table_oid,
///             iterator: None,
///         }
///     }
/// }
/// ```
pub struct BaseExecutor {
    /// The execution context providing access to shared database resources.
    exec_ctx: Arc<ExecutionContext>,
}

impl BaseExecutor {
    /// Creates a new `BaseExecutor` with the given execution context.
    ///
    /// # Arguments
    ///
    /// * `exec_ctx` - The shared execution context providing access to the
    ///   buffer pool, transaction manager, catalog, and other database
    ///   subsystems required for query execution.
    ///
    /// # Returns
    ///
    /// A new `BaseExecutor` instance holding the execution context.
    pub fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self { exec_ctx }
    }

    /// Returns a reference to the execution context.
    ///
    /// This provides read-only access to the execution context for operations
    /// like fetching pages, looking up schemas, or checking transaction state.
    ///
    /// # Returns
    ///
    /// A reference to the [`ExecutionContext`] associated with this executor.
    pub fn get_executor_context(&self) -> &ExecutionContext {
        &self.exec_ctx
    }
}
