//! # Mock Executor
//!
//! Provides a test double executor for unit testing other executor components.
//!
//! ## Overview
//!
//! The `MockExecutor` is a testing utility that simulates a data source by
//! returning pre-defined tuples. It allows testing of executor pipelines
//! without requiring actual table scans or database infrastructure.
//!
//! ## Purpose
//!
//! This executor is **not intended for production use**. It serves to:
//!
//! - Test executors in isolation without database dependencies
//! - Verify correct tuple propagation through executor chains
//! - Provide deterministic test data for reproducible tests
//! - Enable unit testing of filter, join, aggregation, and other executors
//!
//! ## Usage in Tests
//!
//! ```ignore
//! // Create mock tuples for testing
//! let mock_tuples = vec![
//!     (vec![Value::new(1), Value::new("Alice")], RID::new(0, 0)),
//!     (vec![Value::new(2), Value::new("Bob")], RID::new(0, 1)),
//! ];
//!
//! // Create mock executor as child for testing a filter
//! let mock_executor = MockExecutor::new(
//!     exec_ctx.clone(),
//!     Arc::new(mock_scan_plan),
//!     0,
//!     mock_tuples,
//!     schema,
//! );
//!
//! // Use as child of executor under test
//! let filter_executor = FilterExecutor::new(
//!     Box::new(mock_executor),
//!     context,
//!     filter_plan,
//! );
//! ```
//!
//! ## Execution Model
//!
//! ```text
//! ┌────────────────────────────────────────┐
//! │            MockExecutor                │
//! │  ┌──────────────────────────────────┐  │
//! │  │ Pre-loaded tuples:               │  │
//! │  │   [0] (values, RID) ← current    │  │
//! │  │   [1] (values, RID)              │  │
//! │  │   [2] (values, RID)              │  │
//! │  │   ...                            │  │
//! │  └──────────────────────────────────┘  │
//! │                                        │
//! │  next() → Return tuple[idx], idx++     │
//! │  next() → Return tuple[idx], idx++     │
//! │  next() → None (exhausted)             │
//! └────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;

/// Test double executor that returns pre-defined tuples.
///
/// Used exclusively for unit testing executor pipelines. Provides deterministic
/// tuple output without requiring actual database infrastructure.
///
/// # Fields
///
/// * `context` - Shared execution context (required by trait but minimally used)
/// * `plan` - Mock scan plan containing schema information
/// * `current_index` - Legacy index field (kept for backward compatibility)
/// * `tuples` - Pre-loaded vector of (values, RID) pairs to return
/// * `initialized` - Whether `init()` has been called
/// * `current_tuple_idx` - Current position in the tuples vector
///
/// # Example
///
/// ```ignore
/// let tuples = vec![
///     (vec![Value::new(1)], RID::new(0, 0)),
///     (vec![Value::new(2)], RID::new(0, 1)),
/// ];
///
/// let mut executor = MockExecutor::new(ctx, plan, 0, tuples, schema);
/// executor.init();
///
/// assert_eq!(executor.next()?.unwrap().0.get_value(0), Value::new(1));
/// assert_eq!(executor.next()?.unwrap().0.get_value(0), Value::new(2));
/// assert!(executor.next()?.is_none());
/// ```
///
/// # Note
///
/// This is a **testing-only** component. Do not use in production code.
pub struct MockExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<MockScanNode>,
    current_index: usize,
    tuples: Vec<(Vec<Value>, RID)>,
    initialized: bool,
    current_tuple_idx: usize,
}

impl MockExecutor {
    /// Creates a new `MockExecutor` with pre-loaded test tuples.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context (required by trait interface)
    /// * `plan` - Mock scan plan containing schema and table name
    /// * `current_index` - Initial index value (legacy, typically 0)
    /// * `tuples` - Vector of (values, RID) pairs to return during iteration
    /// * `_schema` - Schema parameter (kept for backward compatibility, not stored)
    ///
    /// # Returns
    ///
    /// A new uninitialized `MockExecutor`. Call `init()` before `next()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let test_data = vec![
    ///     (vec![Value::new(100), Value::new("test")], RID::new(1, 0)),
    /// ];
    ///
    /// let executor = MockExecutor::new(
    ///     exec_context,
    ///     Arc::new(mock_plan),
    ///     0,
    ///     test_data,
    ///     schema,
    /// );
    /// ```
    ///
    /// # Note
    ///
    /// The `_schema` parameter is ignored; the schema from `plan` is used instead.
    /// This parameter exists for backward compatibility with existing test code.
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<MockScanNode>,
        current_index: usize,
        tuples: Vec<(Vec<Value>, RID)>,
        _schema: Schema, // Keep for backward compatibility but don't store
    ) -> Self {
        Self {
            context,
            plan,
            current_index,
            tuples,
            initialized: false,
            current_tuple_idx: 0,
        }
    }
}

impl AbstractExecutor for MockExecutor {
    /// Initializes the mock executor for iteration.
    ///
    /// Resets the `current_index` to 0 and sets the `initialized` flag.
    /// Note that `current_tuple_idx` is not reset here, which may be
    /// intentional for certain test scenarios.
    ///
    /// # Behavior
    ///
    /// After `init()`, the executor is ready to return tuples starting
    /// from the beginning of the pre-loaded tuple vector.
    fn init(&mut self) {
        self.current_index = 0;
        self.initialized = true;
    }

    /// Returns the next pre-loaded tuple.
    ///
    /// Iterates through the pre-loaded tuples vector sequentially,
    /// returning each tuple exactly once in order.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next tuple from the pre-loaded vector
    /// * `Ok(None)` - All tuples have been returned
    ///
    /// # Tuple Construction
    ///
    /// Each tuple is constructed fresh from the stored values using the
    /// schema from the mock scan plan. The RID is preserved as provided
    /// during executor construction.
    ///
    /// # Auto-Initialization
    ///
    /// If `init()` was not called, it will be called automatically on
    /// the first `next()` invocation.
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        if self.current_tuple_idx >= self.tuples.len() {
            return Ok(None);
        }

        let (values, rid) = &self.tuples[self.current_tuple_idx];
        self.current_tuple_idx += 1;

        let tuple = Arc::new(Tuple::new(values, self.plan.get_output_schema(), *rid));

        Ok(Some((tuple, *rid)))
    }

    /// Returns the output schema from the mock scan plan.
    ///
    /// The schema defines the structure of tuples returned by this executor
    /// and is used by parent executors for tuple interpretation.
    ///
    /// # Returns
    ///
    /// Reference to the schema defined in the mock scan plan.
    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// While the mock executor doesn't actively use the context,
    /// it's required by the `AbstractExecutor` trait interface.
    ///
    /// # Returns
    ///
    /// Arc-wrapped RwLock-protected execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}
