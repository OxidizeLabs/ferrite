//! # Nested Index Join Executor
//!
//! Implements an index-optimized nested loop join algorithm for efficient
//! equi-joins when an index exists on the inner (right) relation.
//!
//! ## Overview
//!
//! The `NestedIndexJoinExecutor` performs a nested loop join where the inner
//! relation lookup is accelerated using an index. For each tuple from the
//! outer (left) relation, it uses an index scan on the inner relation to
//! find matching tuples efficiently.
//!
//! ## SQL Syntax
//!
//! ```sql
//! SELECT *
//! FROM left_table
//! INNER JOIN right_table ON left_table.id = right_table.id;
//! ```
//!
//! ## Algorithm
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  Nested Index Join Algorithm                     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                  │
//! │  for each tuple L in outer_relation:                            │
//! │      key = extract_join_key(L)                                  │
//! │                                                                  │
//! │      # Use index to find matching tuples                        │
//! │      for each tuple R in index_lookup(key):                     │
//! │          if evaluate_predicate(L, R):                           │
//! │              emit(L ⋈ R)                                        │
//! │                                                                  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Execution Flow
//!
//! ```text
//!                    ┌──────────────────────┐
//!                    │  NestedIndexJoin     │
//!                    │     Executor         │
//!                    └──────────┬───────────┘
//!                               │
//!          ┌────────────────────┼────────────────────┐
//!          │                    │                    │
//!          ▼                    │                    ▼
//! ┌─────────────────┐           │         ┌──────────────────┐
//! │  Left (Outer)   │           │         │  Right (Inner)   │
//! │  Sequential     │           │         │  Index Scan or   │
//! │  Scan           │           │         │  Sequential Scan │
//! └─────────────────┘           │         └──────────────────┘
//!                               │
//!                    ┌──────────▼───────────┐
//!                    │  Output: Combined    │
//!                    │  Left + Right tuples │
//!                    └──────────────────────┘
//! ```
//!
//! ## Index Optimization
//!
//! The executor attempts to use an index on the right relation:
//!
//! | Scenario | Strategy | Performance |
//! |----------|----------|-------------|
//! | Index exists | Index scan on right | O(N × log M) |
//! | No suitable index | Sequential scan fallback | O(N × M) |
//!
//! Where N = left table size, M = right table size.
//!
//! ## Deduplication
//!
//! The executor tracks processed RIDs from the right relation to avoid
//! returning duplicate join results when the same right tuple matches
//! multiple times within a single left tuple's index lookup.
//!
//! ## Output Schema
//!
//! The output schema is the concatenation of left and right schemas:
//! `[left_col1, left_col2, ..., right_col1, right_col2, ...]`

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::index_scan_executor::IndexScanExecutor;
use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val;
use log::{debug, trace};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for index-optimized nested loop joins.
///
/// Performs an inner join by iterating through the outer (left) relation
/// and using an index to efficiently find matching tuples in the inner
/// (right) relation.
///
/// # Fields
///
/// * `children_executors` - Vector containing the left (outer) executor
/// * `context` - Shared execution context for catalog and transaction access
/// * `plan` - The nested index join plan node
/// * `initialized` - Whether `init()` has been called
/// * `current_left_tuple` - Current tuple from the outer relation being processed
/// * `right_exhausted` - Whether all matches for current left tuple have been found
/// * `current_right_executor` - Index or sequential scan executor for right relation
/// * `processed_right_rids` - RIDs already processed for current left tuple
///
/// # Join Types
///
/// Currently supports `INNER JOIN` only. Outer join support would require
/// additional logic to emit unmatched tuples.
///
/// # Example
///
/// ```ignore
/// // Join employees with departments on department_id
/// let join_plan = NestedIndexJoinNode::new(
///     emp_schema,
///     dept_schema,
///     predicate,        // emp.dept_id = dept.id
///     JoinOperator::Inner(JoinConstraint::None),
///     left_key_exprs,   // emp.dept_id
///     right_key_exprs,  // dept.id
///     children,
/// );
///
/// let mut executor = NestedIndexJoinExecutor::new(context, Arc::new(join_plan));
/// executor.init();
///
/// while let Some((joined_tuple, _)) = executor.next()? {
///     // Each joined_tuple contains: [emp columns..., dept columns...]
/// }
/// ```
pub struct NestedIndexJoinExecutor {
    children_executors: Option<Vec<Box<dyn AbstractExecutor>>>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<NestedIndexJoinNode>,
    initialized: bool,
    current_left_tuple: Option<(Arc<Tuple>, RID)>, // Current tuple from outer (left) relation
    right_exhausted: bool, // Whether we've exhausted the inner (right) relation for current left tuple
    current_right_executor: Option<Box<dyn AbstractExecutor>>, // Track current right relation executor (either index scan or seq scan)
    processed_right_rids: Vec<RID>, // Keep track of right RIDs we've already processed for current left tuple
}

impl NestedIndexJoinExecutor {
    /// Creates a new `NestedIndexJoinExecutor` for the given join plan.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context for catalog and transaction access
    /// * `plan` - The nested index join plan containing:
    ///   - Left and right schemas
    ///   - Join predicate
    ///   - Key expressions for index lookup
    ///   - Child plan nodes
    ///
    /// # Returns
    ///
    /// A new uninitialized executor. Call `init()` before `next()`.
    ///
    /// # Note
    ///
    /// The right (inner) executor is created dynamically during execution
    /// based on the current left tuple's key values, enabling index-based
    /// lookups.
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedIndexJoinNode>) -> Self {
        debug!("Creating NestedIndexJoinExecutor");
        Self {
            children_executors: None,
            context,
            plan,
            initialized: false,
            current_left_tuple: None,
            right_exhausted: false,
            current_right_executor: None,
            processed_right_rids: Vec::new(),
        }
    }

    /// Evaluates the join predicate between left and right tuples.
    ///
    /// # Arguments
    ///
    /// * `left_tuple` - Tuple from the outer (left) relation
    /// * `right_tuple` - Tuple from the inner (right) relation
    ///
    /// # Returns
    ///
    /// `true` if the tuples satisfy the join predicate, `false` otherwise.
    ///
    /// # Predicate Evaluation
    ///
    /// Uses `evaluate_join` which handles column references from both
    /// relations using tuple indices (0 for left, 1 for right).
    fn evaluate_predicate(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> bool {
        let predicate = self.plan.get_predicate();
        let left_schema = self.plan.get_left_schema();
        let right_schema = self.plan.get_right_schema();

        trace!(
            "Evaluating predicate between left tuple: {:?} and right tuple: {:?}",
            left_tuple.get_values(),
            right_tuple.get_values()
        );
        trace!("Left schema: {:?}", left_schema);
        trace!("Right schema: {:?}", right_schema);
        trace!("Predicate: {:?}", predicate);

        match predicate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema) {
            Ok(v) => match v.get_val() {
                Val::Boolean(b) => {
                    trace!("Predicate evaluation returned boolean: {}", b);
                    *b
                },
                other => {
                    trace!("Predicate evaluation returned non-boolean: {:?}", other);
                    false
                },
            },
            Err(e) => {
                trace!("Predicate evaluation error: {:?}", e);
                false
            },
        }
    }

    /// Constructs the output tuple by concatenating left and right tuple values.
    ///
    /// # Arguments
    ///
    /// * `left_tuple` - Tuple from the outer (left) relation
    /// * `right_tuple` - Tuple from the inner (right) relation
    ///
    /// # Returns
    ///
    /// A new tuple containing all values from both input tuples:
    /// `[left_val1, left_val2, ..., right_val1, right_val2, ...]`
    ///
    /// # RID Handling
    ///
    /// The output tuple uses a placeholder RID `(0, 0)` since joined tuples
    /// don't have a single physical location.
    fn construct_output_tuple(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Arc<Tuple> {
        let mut joined_values = left_tuple.get_values().clone();
        joined_values.extend(right_tuple.get_values().clone());

        // Create tuple with combined values and a placeholder RID
        Arc::new(Tuple::new(
            &joined_values,
            self.get_output_schema(),
            RID::new(0, 0), // Use a placeholder RID for joined tuples
        ))
    }

    /// Retrieves the next tuple from the inner (right) relation.
    ///
    /// Uses the current right executor (index scan or sequential scan) to
    /// find the next matching tuple. Automatically skips tuples that have
    /// already been processed for the current left tuple.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next unprocessed matching tuple
    /// * `Ok(None)` - No more matching tuples for current left tuple
    /// * `Err(DBError)` - Failed to create right executor
    ///
    /// # Deduplication
    ///
    /// Maintains a list of processed RIDs to prevent duplicate results
    /// when the same right tuple matches multiple times.
    ///
    /// # Executor Creation
    ///
    /// If no current right executor exists, creates one via
    /// `create_right_executor()` before attempting to fetch tuples.
    fn get_next_inner_tuple(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        // Updated to handle Result type from child executors
        // If we have a current right executor, try to get next tuple from it
        if let Some(executor) = &mut self.current_right_executor {
            while let Ok(Some(tuple_and_rid)) = executor.next() {
                let right_rid = tuple_and_rid.1;
                // Skip if we've already processed this right tuple for the current left tuple
                if self.processed_right_rids.contains(&right_rid) {
                    debug!("Skipping already processed RID: {:?}", right_rid);
                    continue; // Skip to next tuple
                }

                // Add the RID to processed list
                debug!("Adding RID to processed list: {:?}", right_rid);
                self.processed_right_rids.push(right_rid);

                return Ok(Some(tuple_and_rid));
            }
            // If we get here, exhausted the executor
            self.current_right_executor = None;
            return Ok(None);
        }

        // Create executor for the current left tuple
        if self.create_right_executor().is_none() {
            return Err(DBError::Execution(
                "Failed to create right executor".to_string(),
            ));
        }

        // Now try to get the first result with the newly created executor
        if let Some(executor) = &mut self.current_right_executor
            && let Ok(Some(tuple_and_rid)) = executor.next()
        {
            let right_rid = tuple_and_rid.1;

            // Check if we've already processed this RID
            if self.processed_right_rids.contains(&right_rid) {
                debug!(
                    "Skipping already processed RID (first result): {:?}",
                    right_rid
                );
                return self.get_next_inner_tuple(); // Recursively try next
            }

            // Add to processed list
            debug!(
                "Adding RID to processed list (first result): {:?}",
                right_rid
            );
            self.processed_right_rids.push(right_rid);

            return Ok(Some(tuple_and_rid));
        }

        // No results found
        Ok(None)
    }

    /// Creates an executor for the inner (right) relation based on current left tuple.
    ///
    /// Dynamically creates either an index scan or sequential scan executor
    /// for the right relation, parameterized by the current left tuple's
    /// join key values.
    ///
    /// # Algorithm
    ///
    /// 1. Extract key values from current left tuple
    /// 2. Build comparison predicates for index lookup
    /// 3. Search catalog for suitable index on right table
    /// 4. If index found: create `IndexScanExecutor`
    /// 5. If no index: fall back to `SeqScanExecutor`
    ///
    /// # Returns
    ///
    /// * `Some(())` - Executor created successfully
    /// * `None` - Failed to create executor (no current left tuple or error)
    ///
    /// # Index Selection
    ///
    /// Selects an index whose key schema matches the number of join key
    /// expressions. For composite keys, all key columns must be covered.
    ///
    /// # Fallback Behavior
    ///
    /// When no suitable index exists, creates a sequential scan with
    /// predicate pushdown for the join condition. This is less efficient
    /// but ensures correctness.
    fn create_right_executor(&mut self) -> Option<()> {
        // Get the current left tuple
        let (left_tuple, _) = self.current_left_tuple.as_ref()?;

        // Create new index scan for the current left tuple
        let right_schema = self.plan.get_right_schema().clone();
        let right_key_exprs = self.plan.get_right_key_expressions();
        let left_key_exprs = self.plan.get_left_key_expressions();

        // Evaluate left key expressions to get values for index lookup
        let mut predicate_keys = Vec::new();
        for (left_expr, right_expr) in left_key_exprs.iter().zip(right_key_exprs.iter()) {
            if let Ok(value) = left_expr.evaluate(left_tuple, self.plan.get_left_schema()) {
                debug!("Evaluated left key expression to: {:?}", value);

                // Create comparison expression for index scan
                let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
                    right_expr.clone(),
                    Arc::new(Expression::Constant(ConstantExpression::new(
                        value.clone(),
                        Column::new("const", TypeId::Integer),
                        vec![],
                    ))),
                    ComparisonType::Equal,
                    vec![],
                )));
                predicate_keys.push(predicate);
            } else {
                debug!("Failed to evaluate left key expression");
            }
        }

        // Get catalog info for right table
        let binding = self.context.read();
        let catalog = binding.get_catalog();
        let catalog_guard = catalog.read();

        // Extract table name from right child plan node
        let table_name = match self.plan.get_right_child() {
            PlanNode::SeqScan(scan) => scan.get_table_name(),
            PlanNode::IndexScan(scan) => scan.get_table_name(),
            // Add other cases as needed
            _ => return None, // Return None if we can't get table name
        };

        let table_info = catalog_guard.get_table(table_name).unwrap();
        let table_indexes = catalog_guard.get_table_indexes(table_name);

        // Find suitable index
        let index_info = match table_indexes.iter().find(|idx| {
            let key_schema = idx.get_key_schema();
            key_schema.get_column_count() as usize == right_key_exprs.len()
        }) {
            Some(idx) => idx,
            None => {
                // No suitable index found - create a sequential scan instead
                debug!("No suitable index found, falling back to sequential scan");
                let seq_scan_plan = Arc::new(SeqScanPlanNode::new(
                    right_schema.clone(),
                    table_info.get_table_oidt(),
                    table_name.to_string(),
                ));

                let mut seq_scan_executor =
                    SeqScanExecutor::new(self.context.clone(), seq_scan_plan);
                seq_scan_executor.init();

                // Store the executor for retrieving right tuples
                self.current_right_executor = Some(Box::new(seq_scan_executor));
                return Some(());
            },
        };

        // Create and execute index scan
        let index_scan_plan = Arc::new(IndexScanNode::new(
            right_schema,
            table_name.to_string(),
            table_info.get_table_oidt(),
            index_info.get_index_name().to_string(),
            index_info.get_index_oid(),
            predicate_keys,
        ));

        // Create new executor
        let mut index_scan_executor = IndexScanExecutor::new(self.context.clone(), index_scan_plan);

        index_scan_executor.init();

        // Store for future use
        self.current_right_executor = Some(Box::new(index_scan_executor));
        Some(())
    }
}

impl AbstractExecutor for NestedIndexJoinExecutor {
    /// Initializes the nested index join executor.
    ///
    /// Creates and initializes the left (outer) relation executor. The right
    /// (inner) relation executor is created dynamically during `next()` calls
    /// based on the current left tuple's key values.
    ///
    /// # Initialization
    ///
    /// Only the left executor is created here because:
    /// - It provides the outer loop of the nested join
    /// - The right executor depends on the current left tuple's key values
    /// - Creating the right executor per-left-tuple enables index optimization
    ///
    /// # Idempotency
    ///
    /// Multiple calls to `init()` are safe; reinitialization is skipped.
    fn init(&mut self) {
        if self.initialized {
            debug!("NestedIndexJoinExecutor already initialized");
            return;
        }

        debug!("Initializing NestedIndexJoinExecutor");

        // Initialize child executors vector
        let mut children = Vec::with_capacity(1); // Only need left executor

        // Initialize left executor
        let left_plan = self.plan.get_left_child();
        debug!("Creating left executor with plan: {:?}", left_plan);
        if let Ok(mut left_executor) = left_plan.create_executor(self.context.clone()) {
            left_executor.init();
            debug!(
                "Left executor initialized with schema: {:?}",
                left_executor.get_output_schema()
            );
            children.push(left_executor);
        } else {
            debug!("Failed to create left executor");
            return;
        }

        self.children_executors = Some(children);
        self.initialized = true;
    }

    /// Returns the next joined tuple from the nested index join.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// loop:
    ///   if current_left_tuple exists and right not exhausted:
    ///     get next matching right tuple via index
    ///     if found and predicate satisfied:
    ///       return combined tuple
    ///     else:
    ///       mark right exhausted
    ///
    ///   get next left tuple
    ///   if found:
    ///     reset right relation state
    ///     clear processed RIDs
    ///     continue loop
    ///   else:
    ///     return None (join complete)
    /// ```
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next joined tuple
    /// * `Ok(None)` - No more join results
    /// * `Err(DBError)` - Execution error
    ///
    /// # State Management
    ///
    /// For each new left tuple:
    /// - Clears the processed RIDs list
    /// - Resets the right exhausted flag
    /// - Creates a new right executor for index lookup
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            debug!("NestedIndexJoinExecutor not initialized");
            return Ok(None);
        }

        debug!("NestedIndexJoinExecutor::next() called");

        loop {
            // If we have a current left tuple and haven't exhausted right relation
            if !self.right_exhausted {
                if let Some((left_tuple, left_rid)) = &self.current_left_tuple {
                    debug!(
                        "Processing current left tuple: {:?}",
                        left_tuple.get_values()
                    );
                    let left_tuple_clone = left_tuple.clone();
                    let left_rid_clone = *left_rid;

                    // Try to get next matching tuple from right relation using index
                    debug!("Attempting to get next inner tuple");
                    if let Ok(Some((right_tuple, _))) = self.get_next_inner_tuple() {
                        debug!("Got right tuple: {:?}", right_tuple.get_values());

                        // Double-check the predicate since get_next_inner_tuple already filters by key equality
                        if self.evaluate_predicate(&left_tuple_clone, &right_tuple) {
                            debug!("Predicate evaluation successful, constructing join result");
                            let result =
                                self.construct_output_tuple(&left_tuple_clone, &right_tuple);
                            debug!("Constructed tuple: {:?}", result.get_values());
                            return Ok(Some((result, left_rid_clone)));
                        } else {
                            debug!("Predicate evaluation failed, continuing to next right tuple");
                        }
                        continue;
                    } else {
                        // No more matching tuples for this left tuple
                        debug!("No more right tuples for current left tuple");
                        self.right_exhausted = true;
                        self.current_right_executor = None;
                    }
                } else {
                    debug!("No current left tuple set");
                }
            }

            // Get next tuple from left relation
            debug!("Attempting to get next left tuple");
            if let Some(children) = &mut self.children_executors {
                if let Some(left_tuple) = children[0].next()? {
                    debug!(
                        "Got new left tuple: {:?}, clearing processed RIDs list",
                        left_tuple.0.get_values()
                    );
                    self.current_left_tuple = Some(left_tuple);
                    self.right_exhausted = false;
                    // Clear the list of processed right RIDs for the new left tuple
                    self.processed_right_rids.clear();
                    continue;
                } else {
                    debug!("No more left tuples available from left executor");
                }
            } else {
                debug!("Children executors not available");
            }

            // No more tuples from left relation
            debug!("No more left tuples, ending join");
            return Ok(None);
        }
    }

    /// Returns the output schema for the joined result.
    ///
    /// The output schema is the concatenation of left and right schemas:
    /// `[left_columns..., right_columns...]`
    ///
    /// # Returns
    ///
    /// Reference to the combined output schema from the join plan.
    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to:
    /// - Catalog for table and index metadata lookup
    /// - Buffer pool for page access
    /// - Transaction context for MVCC operations
    ///
    /// # Returns
    ///
    /// Arc-wrapped RwLock-protected execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::executors::create_index_executor::CreateIndexExecutor;
    use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(),
            )));

            Self {
                bpm,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }

        pub fn transaction_context(&self) -> Arc<TransactionContext> {
            self.transaction_context.clone()
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
        }
    }

    #[tokio::test]
    async fn test_nested_index_join_executor() {
        let ctx = TestContext::new("test_nested_index_join_executor").await;

        // Create schemas for both tables
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create tables with different OIDs
        let left_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("left_table".to_string(), left_schema.clone())
                .unwrap()
        };
        let right_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("right_table".to_string(), right_schema.clone())
                .unwrap()
        };

        // Verify table OIDs are different
        assert_ne!(
            left_table_info.get_table_oidt(),
            right_table_info.get_table_oidt(),
            "Left and right tables must have different OIDs"
        );

        let left_table = left_table_info.get_table_heap();
        let right_table = right_table_info.get_table_heap();

        debug!("Left table OID: {}", left_table_info.get_table_oidt());
        debug!("Right table OID: {}", right_table_info.get_table_oidt());

        // Insert data into left table
        let left_tuple_meta = Arc::new(TupleMeta::new(0));

        // Insert each tuple separately with proper Value objects
        for (id, value) in [(1, "A"), (2, "B"), (3, "C")] {
            let values = vec![Value::new(id), Value::new(value)];
            left_table
                .insert_tuple_from_values(values, &left_schema, left_tuple_meta.clone())
                .expect("Failed to insert into left table");
        }

        // Verify left table's first page ID
        debug!(
            "Left table first page ID: {}",
            left_table.get_first_page_id()
        );

        // Insert data into right table
        let right_tuple_meta = Arc::new(TupleMeta::new(0));

        // Insert each tuple separately with proper Value objects
        for (id, data) in [(1, "X"), (2, "Y"), (4, "Z")] {
            let values = vec![Value::new(id), Value::new(data)];
            right_table
                .insert_tuple_from_values(values, &right_schema, right_tuple_meta.clone())
                .expect("Failed to insert into right table");
        }

        // Verify right table's first page ID
        debug!(
            "Right table first page ID: {}",
            right_table.get_first_page_id()
        );

        // Verify each table's contents separately
        {
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx.bpm(),
                ctx.catalog(),
                ctx.transaction_context(),
            )));

            // Verify left table
            let left_scan = SeqScanPlanNode::new(
                left_schema.clone(),
                left_table_info.get_table_oidt(),
                "left_table".to_string(),
            );
            let mut left_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(left_scan));
            left_executor.init();

            debug!("Starting left table scan");
            let mut left_tuples = Vec::new();
            while let Ok(Some((tuple, rid))) = left_executor.next() {
                debug!(
                    "Left table scan tuple at RID {:?}: {:?}",
                    rid,
                    tuple.get_values()
                );
                left_tuples.push((tuple, rid));
            }
            assert_eq!(left_tuples.len(), 3, "Left table should have 3 tuples");

            // Verify the values match what we inserted
            for (tuple, _) in left_tuples {
                let values = tuple.get_values();
                let id = values[0].get_val();
                let value = values[1].get_val();
                match id {
                    Val::Integer(1) => assert_eq!(*value, Val::VarLen("A".to_string())),
                    Val::Integer(2) => assert_eq!(*value, Val::VarLen("B".to_string())),
                    Val::Integer(3) => assert_eq!(*value, Val::VarLen("C".to_string())),
                    _ => panic!("Unexpected ID in left table: {:?}", id),
                }
            }

            // Verify right table
            let right_scan = SeqScanPlanNode::new(
                right_schema.clone(),
                right_table_info.get_table_oidt(),
                "right_table".to_string(),
            );
            let mut right_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(right_scan));
            right_executor.init();

            debug!("Starting right table scan");
            let mut right_tuples = Vec::new();
            while let Ok(Some((tuple, rid))) = right_executor.next() {
                debug!(
                    "Right table scan tuple at RID {:?}: {:?}",
                    rid,
                    tuple.get_values()
                );
                right_tuples.push((tuple, rid));
            }
            assert_eq!(right_tuples.len(), 3, "Right table should have 3 tuples");

            // Verify the values match what we inserted
            for (tuple, _) in right_tuples {
                let values = tuple.get_values();
                let id = values[0].get_val();
                let data = values[1].get_val();
                match id {
                    Val::Integer(1) => assert_eq!(*data, Val::VarLen("X".to_string())),
                    Val::Integer(2) => assert_eq!(*data, Val::VarLen("Y".to_string())),
                    Val::Integer(4) => assert_eq!(*data, Val::VarLen("Z".to_string())),
                    _ => panic!("Unexpected ID in right table: {:?}", id),
                }
            }
        }

        // Create Index for right table
        let index_columns = vec![Column::new("id", TypeId::Integer)];
        let index_schema = Schema::new(index_columns);

        let index_plan = Arc::new(CreateIndexPlanNode::new(
            index_schema,
            "right_table".to_string(),
            "right_table_index".to_string(),
            vec![0],
            false, // not unique
        ));

        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog(),
            ctx.transaction_context(),
        )));

        let mut index_executor = CreateIndexExecutor::new(exec_ctx.clone(), index_plan, false);
        index_executor.init();
        // Actually execute the create index executor to create the index
        assert!(index_executor.next().unwrap().is_none());

        // Populate the index with data from right table
        {
            let catalog = ctx.catalog.read();
            let index_info = catalog.get_table_indexes("right_table")[0].clone();

            // Create a sequential scan to iterate through all tuples
            let right_scan = SeqScanPlanNode::new(
                right_schema.clone(),
                right_table_info.get_table_oidt(),
                "right_table".to_string(),
            );
            let mut right_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(right_scan));
            right_executor.init();

            // Manually insert entries into the index using the B+ tree directly
            let index_oid = index_info.get_index_oid();
            if let Some((_, btree)) = catalog.get_index_by_index_oid(index_oid) {
                // Insert each tuple's keys into the index
                while let Ok(Some((tuple, rid))) = right_executor.next() {
                    // Extract the key value (id) from the tuple
                    let key_columns = vec![0]; // column index for "id"
                    let key_values = tuple.keys_from_tuple_checked(&key_columns).unwrap();

                    // Extract the key value for the B+ tree
                    let key_value = key_values[0].clone();

                    // Insert directly into B+ tree
                    let mut btree_write = btree.write();
                    btree_write.insert(key_value, rid);

                    debug!(
                        "Inserted index entry for key: {:?}, RID: {:?}",
                        key_values, rid
                    );
                }
            }
        }

        // Verify index was created
        {
            let catalog = ctx.catalog.read();
            let indexes = catalog.get_table_indexes("right_table");
            assert_eq!(indexes.len(), 1, "Index should be created");
            assert_eq!(indexes[0].get_index_name(), "right_table_index");
        }

        // Create sequential scan plans with correct table OIDs
        let left_scan = SeqScanPlanNode::new(
            left_schema.clone(),
            left_table_info.get_table_oidt(),
            "left_table".to_string(),
        );
        debug!("Created left scan plan: {:?}", left_scan);

        let right_scan = SeqScanPlanNode::new(
            right_schema.clone(),
            right_table_info.get_table_oidt(),
            "right_table".to_string(),
        );
        debug!("Created right scan plan: {:?}", right_scan);

        // Create join predicate (left.id = right.id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index = 0 for left table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple_index = 1 for right table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![], // Don't need to add children here since they're already in left_col and right_col
        )));

        // Create nested loop join plan
        let join_plan = Arc::new(NestedIndexJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinOperator::Inner(JoinConstraint::None),
            vec![left_col],
            vec![right_col],
            vec![PlanNode::SeqScan(left_scan), PlanNode::SeqScan(right_scan)], // Convert directly to PlanNode
        ));

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog(),
            ctx.transaction_context(),
        )));

        // Create and initialize join executor
        let mut join_executor = NestedIndexJoinExecutor::new(exec_ctx, join_plan);
        join_executor.init();

        // Collect and verify results
        let mut results = Vec::new();
        debug!("Starting to collect join results");
        let mut count = 0;
        while let Ok(Some((tuple, _))) = join_executor.next() {
            count += 1;
            debug!("Join result {}: {:?}", count, tuple.get_values());
            results.push(tuple);
        }
        debug!("Finished collecting join results. Total: {}", results.len());

        // Should have 2 matching rows (id=1 and id=2)
        debug!("Total join results: {}", results.len());

        if results.is_empty() {
            debug!(
                "ERROR: No join results produced! This suggests an issue with the join execution."
            );
            // Let's debug what went wrong
            debug!(
                "Left table data and Right table data seem to be inserted correctly based on earlier verification."
            );
            debug!(
                "The issue might be in the index creation, index population, or the join execution itself."
            );
        }

        assert_eq!(
            results.len(),
            2,
            "Expected 2 join results but got {}",
            results.len()
        );
    }

    #[test]
    fn test_join_predicate() {
        initialize_logger();
        // Create simple schemas
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create join predicate (left.id = right.id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index = 0 for left table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple_index = 1 for right table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        // Verify column references are set up correctly
        if let Expression::ColumnRef(left_expr) = left_col.as_ref() {
            debug!(
                "Left column reference - tuple_index: {}, column_index: {}",
                left_expr.get_tuple_index(),
                left_expr.get_column_index()
            );
        }
        if let Expression::ColumnRef(right_expr) = right_col.as_ref() {
            debug!(
                "Right column reference - tuple_index: {}, column_index: {}",
                right_expr.get_tuple_index(),
                right_expr.get_column_index()
            );
        }

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![],
        )));

        // Verify the predicate's column references
        if let Expression::Comparison(comp) = predicate.as_ref() {
            if let Expression::ColumnRef(left_expr) = comp.get_left().as_ref() {
                debug!(
                    "Predicate left column reference - tuple_index: {}, column_index: {}",
                    left_expr.get_tuple_index(),
                    left_expr.get_column_index()
                );
            }
            if let Expression::ColumnRef(right_expr) = comp.get_right().as_ref() {
                debug!(
                    "Predicate right column reference - tuple_index: {}, column_index: {}",
                    right_expr.get_tuple_index(),
                    right_expr.get_column_index()
                );
            }
        }

        // Create test tuples
        let left_tuple = Tuple::new(
            &[Value::new(1), Value::new("A")],
            &left_schema,
            RID::new(0, 0),
        );
        let right_tuple = Tuple::new(
            &[Value::new(1), Value::new("X")],
            &right_schema,
            RID::new(0, 0),
        );

        debug!("Left tuple values: {:?}", left_tuple.get_values());
        debug!("Right tuple values: {:?}", right_tuple.get_values());
        debug!("Left schema: {:?}", left_schema);
        debug!("Right schema: {:?}", right_schema);
        debug!("Predicate: {:?}", predicate);

        // Test predicate evaluation
        let result = predicate
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();

        debug!("Predicate evaluation result: {:?}", result);

        match result.get_val() {
            Val::Boolean(b) => {
                debug!("Boolean result: {}", b);
                assert!(b, "Predicate should evaluate to true for matching IDs");
            },
            other => {
                panic!(
                    "Expected boolean result from predicate evaluation, got: {:?}",
                    other
                );
            },
        }
    }

    #[tokio::test]
    async fn test_simple_table_data_debug() {
        let ctx = TestContext::new("test_simple_table_data_debug").await;

        // Create schemas for both tables
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create tables with different OIDs
        let left_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("left_table".to_string(), left_schema.clone())
                .unwrap()
        };
        let right_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("right_table".to_string(), right_schema.clone())
                .unwrap()
        };

        debug!("Left table OID: {}", left_table_info.get_table_oidt());
        debug!("Right table OID: {}", right_table_info.get_table_oidt());

        let left_table = left_table_info.get_table_heap();
        let right_table = right_table_info.get_table_heap();

        // Insert data into left table
        let left_tuple_meta = Arc::new(TupleMeta::new(0));
        for (id, value) in [(1, "A"), (2, "B"), (3, "C")] {
            let values = vec![Value::new(id), Value::new(value)];
            let result =
                left_table.insert_tuple_from_values(values, &left_schema, left_tuple_meta.clone());
            debug!("Left table insert for ({}, {}): {:?}", id, value, result);
        }

        // Insert data into right table
        let right_tuple_meta = Arc::new(TupleMeta::new(0));
        for (id, data) in [(1, "X"), (2, "Y"), (4, "Z")] {
            let values = vec![Value::new(id), Value::new(data)];
            let result = right_table.insert_tuple_from_values(
                values,
                &right_schema,
                right_tuple_meta.clone(),
            );
            debug!("Right table insert for ({}, {}): {:?}", id, data, result);
        }

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog(),
            ctx.transaction_context(),
        )));

        // Verify left table contents
        debug!("=== VERIFYING LEFT TABLE ===");
        let left_scan = SeqScanPlanNode::new(
            left_schema.clone(),
            left_table_info.get_table_oidt(),
            "left_table".to_string(),
        );
        let mut left_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(left_scan));
        left_executor.init();

        let mut left_tuples = Vec::new();
        while let Ok(Some((tuple, rid))) = left_executor.next() {
            debug!(
                "Left table tuple at RID {:?}: {:?}",
                rid,
                tuple.get_values()
            );
            left_tuples.push((tuple, rid));
        }
        debug!("Left table total tuples: {}", left_tuples.len());

        // Verify right table contents
        debug!("=== VERIFYING RIGHT TABLE ===");
        let right_scan = SeqScanPlanNode::new(
            right_schema.clone(),
            right_table_info.get_table_oidt(),
            "right_table".to_string(),
        );
        let mut right_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(right_scan));
        right_executor.init();

        let mut right_tuples = Vec::new();
        while let Ok(Some((tuple, rid))) = right_executor.next() {
            debug!(
                "Right table tuple at RID {:?}: {:?}",
                rid,
                tuple.get_values()
            );

            // Debug each value separately
            let values = tuple.get_values();
            for (i, value) in values.iter().enumerate() {
                debug!("  Right table value[{}]: {:?}", i, value.get_val());
            }

            right_tuples.push((tuple, rid));
        }
        debug!("Right table total tuples: {}", right_tuples.len());

        // Now let's verify the individual values to see where the issue is
        for (tuple, _) in &right_tuples {
            let values = tuple.get_values();
            let id = values[0].get_val();
            let data = values[1].get_val();
            debug!("Right table verification: ID={:?}, DATA={:?}", id, data);

            match id {
                Val::Integer(1) => {
                    debug!("Checking ID=1, expected data='X', actual data={:?}", data);
                    if *data != Val::VarLen("X".to_string()) {
                        debug!(
                            "ERROR: Data mismatch for ID=1! Expected VarLen(\"X\"), got {:?}",
                            data
                        );
                    }
                },
                Val::Integer(2) => {
                    debug!("Checking ID=2, expected data='Y', actual data={:?}", data);
                    if *data != Val::VarLen("Y".to_string()) {
                        debug!(
                            "ERROR: Data mismatch for ID=2! Expected VarLen(\"Y\"), got {:?}",
                            data
                        );
                    }
                },
                Val::Integer(4) => {
                    debug!("Checking ID=4, expected data='Z', actual data={:?}", data);
                    if *data != Val::VarLen("Z".to_string()) {
                        debug!(
                            "ERROR: Data mismatch for ID=4! Expected VarLen(\"Z\"), got {:?}",
                            data
                        );
                    }
                },
                _ => debug!("Unexpected ID in right table: {:?}", id),
            }
        }
    }
}
