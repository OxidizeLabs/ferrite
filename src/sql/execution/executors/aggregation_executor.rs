//! # Aggregation Executor Module
//!
//! This module implements the aggregation executor for SQL `GROUP BY` and aggregate
//! function processing. It supports standard SQL aggregate functions and handles
//! both grouped and ungrouped aggregations.
//!
//! ## Supported Aggregate Functions
//!
//! | Function     | Description                              | NULL Handling           |
//! |--------------|------------------------------------------|-------------------------|
//! | `COUNT(*)`   | Counts all rows                          | Includes NULL rows      |
//! | `COUNT(col)` | Counts non-NULL values in column         | Excludes NULL values    |
//! | `SUM(col)`   | Computes sum of column values            | Ignores NULL values     |
//! | `AVG(col)`   | Computes arithmetic mean                 | Ignores NULL values     |
//! | `MIN(col)`   | Finds minimum value                      | Ignores NULL values     |
//! | `MAX(col)`   | Finds maximum value                      | Ignores NULL values     |
//!
//! ## Execution Strategy
//!
//! The aggregation executor uses a **hash-based aggregation** strategy:
//!
//! 1. **Initialization Phase** (`init`):
//!    - Consumes all tuples from the child executor
//!    - Computes group keys from `GROUP BY` expressions
//!    - Maintains running aggregates in a hash map keyed by group
//!
//! 2. **Output Phase** (`next`):
//!    - Iterates over computed groups
//!    - Finalizes aggregate values (e.g., computes AVG = SUM / COUNT)
//!    - Returns one tuple per group
//!
//! ## Example Query Flow
//!
//! For a query like:
//! ```sql
//! SELECT department, SUM(salary), COUNT(*)
//! FROM employees
//! GROUP BY department
//! ```
//!
//! The executor:
//! 1. Scans all employee tuples from child executor
//! 2. Groups by department, accumulating salary sums and counts
//! 3. Returns one result tuple per department with final aggregates
//!
//! ## Empty Input Handling
//!
//! - **With `GROUP BY`**: Returns no rows for empty input
//! - **Without `GROUP BY`**: Returns one row with identity values
//!   - `COUNT`: 0
//!   - `SUM`: 0 (or NULL depending on semantics)
//!   - `MIN`/`MAX`: NULL

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::CmpBool;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use log::{debug, error};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// A composite key representing a unique group in aggregation.
///
/// `GroupKey` holds the evaluated values of all `GROUP BY` expressions for a
/// single group. Two tuples belong to the same group if and only if their
/// `GroupKey` values are equal.
///
/// # Example
///
/// For `GROUP BY department, year`, a `GroupKey` might contain:
/// ```text
/// GroupKey { values: [Value("Engineering"), Value(2024)] }
/// ```
///
/// # Hashing
///
/// Implements `Hash` and `Eq` to serve as a key in the aggregation hash map.
/// The hash is computed over all values in order.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct GroupKey {
    /// The evaluated values for each `GROUP BY` expression, in order.
    values: Vec<Value>,
}

/// Running aggregate values for a single group.
///
/// `AggregateValues` maintains the intermediate state of all aggregate
/// computations for one group. Each position corresponds to an aggregate
/// expression in the query.
///
/// # Aggregate State
///
/// - **SUM**: Running total
/// - **COUNT**: Running count (as `i64`)
/// - **MIN/MAX**: Current extreme value
/// - **AVG**: Running sum (divided by count during finalization)
///
/// # Example
///
/// For `SELECT SUM(salary), COUNT(*), MAX(age) FROM ...`:
/// ```text
/// AggregateValues { values: [Value(150000), Value(3), Value(45)] }
/// ```
#[derive(Clone)]
struct AggregateValues {
    /// One intermediate aggregate value per aggregate expression.
    values: Vec<Value>,
}

impl GroupKey {
    /// Creates a new `GroupKey` from the given values.
    ///
    /// # Arguments
    ///
    /// * `values` - Evaluated values for each `GROUP BY` expression.
    fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl AggregateValues {
    /// Creates a new `AggregateValues` with NULL initial values.
    ///
    /// All aggregates start as NULL and are updated as tuples are processed.
    /// This correctly handles the case where a group has no non-NULL input
    /// values for a particular aggregate.
    ///
    /// # Arguments
    ///
    /// * `num_aggregates` - The number of aggregate expressions in the query.
    fn new(num_aggregates: usize) -> Self {
        Self {
            values: vec![Value::new(Val::Null); num_aggregates],
        }
    }
}

/// Executor for SQL aggregation operations with optional grouping.
///
/// `AggregationExecutor` implements hash-based aggregation, consuming all input
/// tuples during initialization and producing grouped aggregate results. It
/// supports all standard SQL aggregate functions and handles `GROUP BY` clauses.
///
/// # Execution Model
///
/// Unlike most Volcano-style executors that process one tuple at a time,
/// `AggregationExecutor` is a **pipeline breaker** that must consume all input
/// before producing any output. This is because aggregate results cannot be
/// computed until all group members are seen.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    AggregationExecutor                      │
/// │  ┌──────────────┐    ┌──────────────────────────────────┐  │
/// │  │  GROUP BY    │    │  Aggregate Hash Map               │  │
/// │  │  Expressions │    │  ┌────────────────────────────┐  │  │
/// │  └──────────────┘    │  │ Key("Eng") → [150k, 3, 45] │  │  │
/// │                      │  │ Key("Sales") → [80k, 2, 38]│  │  │
/// │  ┌──────────────┐    │  └────────────────────────────┘  │  │
/// │  │  Aggregate   │    └──────────────────────────────────┘  │
/// │  │  Expressions │                                          │
/// │  └──────────────┘                                          │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Thread Safety
///
/// The executor is `Send + Sync` compliant. The execution context is wrapped
/// in `Arc<RwLock<_>>` for safe concurrent access.
///
/// # Example
///
/// ```ignore
/// // Query: SELECT dept, SUM(salary) FROM employees GROUP BY dept
/// let executor = AggregationExecutor::new(
///     context,
///     aggregation_plan,
///     seq_scan_executor,
/// );
/// ```
pub struct AggregationExecutor {
    /// Child executor providing input tuples.
    child: Box<dyn AbstractExecutor>,
    /// Expressions defining the `GROUP BY` columns.
    group_by_exprs: Vec<Arc<Expression>>,
    /// Aggregate function expressions (SUM, COUNT, etc.).
    aggregate_exprs: Vec<Arc<Expression>>,
    /// Hash map from group key to running aggregate values.
    groups: HashMap<GroupKey, AggregateValues>,
    /// Tracks counts for AVG aggregates: `(group_key, agg_index) → count`.
    /// Required because AVG = SUM / COUNT, computed during finalization.
    avg_counts: HashMap<(GroupKey, usize), i64>,
    /// Schema of output tuples (group columns + aggregate columns).
    output_schema: Schema,
    /// Shared execution context for buffer pool, catalog, and transaction access.
    exec_ctx: Arc<RwLock<ExecutionContext>>,
    /// Flag indicating whether `init()` has been called.
    initialized: bool,
    /// Finalized groups ready for output, consumed by `next()`.
    groups_to_return: Vec<(GroupKey, AggregateValues)>,
}

impl AggregationExecutor {
    /// Creates a new `AggregationExecutor` from a plan and child executor.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context for database resource access.
    /// * `plan` - The aggregation plan node containing GROUP BY and aggregate
    ///   expressions, along with the output schema.
    /// * `child_executor` - The child executor providing input tuples to aggregate.
    ///
    /// # Returns
    ///
    /// A new uninitialized `AggregationExecutor`. Call `init()` before `next()`.
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<AggregationPlanNode>,
        child_executor: Box<dyn AbstractExecutor>,
    ) -> Self {
        Self {
            exec_ctx: context,
            child: child_executor,
            group_by_exprs: plan.get_group_bys().to_vec(),
            aggregate_exprs: plan.get_aggregates().to_vec(),
            groups: HashMap::new(),
            avg_counts: HashMap::new(),
            groups_to_return: Vec::new(),
            output_schema: plan.get_output_schema().clone(),
            initialized: false,
        }
    }

    /// Updates aggregate values for a group with a new input tuple.
    ///
    /// This is the core aggregation logic that processes each input tuple and
    /// updates the running aggregate state for the appropriate group.
    ///
    /// # Arguments
    ///
    /// * `agg_map` - Mutable reference to the group → aggregate values hash map.
    /// * `avg_counts` - Mutable reference to the AVG count tracking map.
    /// * `aggregates` - Slice of aggregate expressions to evaluate.
    /// * `key` - The group key for this tuple.
    /// * `tuple` - The input tuple to process.
    /// * `schema` - Schema of the input tuple for expression evaluation.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Aggregate was successfully updated.
    /// * `Err(String)` - An error occurred (e.g., unsupported aggregate type).
    ///
    /// # Aggregate Update Logic
    ///
    /// | Type         | Update Rule                                    |
    /// |--------------|------------------------------------------------|
    /// | SUM          | Add value to running sum                       |
    /// | COUNT/COUNT* | Increment counter                              |
    /// | MIN          | Keep smaller of current and new value          |
    /// | MAX          | Keep larger of current and new value           |
    /// | AVG          | Add to sum, increment count (finalize later)   |
    fn compute_aggregate(
        agg_map: &mut HashMap<GroupKey, AggregateValues>,
        avg_counts: &mut HashMap<(GroupKey, usize), i64>,
        aggregates: &[Arc<Expression>],
        key: GroupKey,
        tuple: &Tuple,
        schema: &Schema,
    ) -> Result<(), String> {
        debug!("Computing aggregate for key: {:?}", key);
        debug!("Number of aggregates: {}", aggregates.len());

        let agg_value = agg_map
            .entry(key.clone())
            .or_insert_with(|| AggregateValues {
                values: vec![Value::new(Val::Null); aggregates.len()],
            });

        debug!("Current aggregate values: {:?}", agg_value.values);

        // Update each aggregate value
        for (i, agg_expr) in aggregates.iter().enumerate() {
            debug!("Processing aggregate {} of type {:?}", i, agg_expr);
            match agg_expr.as_ref() {
                Expression::Aggregate(agg) => {
                    debug!("Aggregate type: {:?}", agg.get_agg_type());
                    match agg.get_agg_type() {
                        AggregationType::Sum => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                if agg_value.values[i].is_null() {
                                    agg_value.values[i] = arg_val;
                                } else {
                                    agg_value.values[i] = agg_value.values[i].add(&arg_val)?;
                                }
                            }
                        },
                        AggregationType::Count | AggregationType::CountStar => {
                            let count = if agg_value.values[i].is_null() {
                                Value::new(1i64)
                            } else {
                                agg_value.values[i].add(&Value::new(1i64))?
                            };
                            agg_value.values[i] = count;
                        },
                        AggregationType::Min => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                if agg_value.values[i].is_null() {
                                    agg_value.values[i] = arg_val;
                                } else if arg_val.compare_less_than(&agg_value.values[i])
                                    == CmpBool::CmpTrue
                                {
                                    agg_value.values[i] = arg_val
                                }
                            }
                        },
                        AggregationType::Max => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                if agg_value.values[i].is_null() {
                                    agg_value.values[i] = arg_val;
                                } else if agg_value.values[i].compare_less_than(&arg_val)
                                    == CmpBool::CmpTrue
                                {
                                    agg_value.values[i] = arg_val
                                }
                            }
                        },
                        AggregationType::Avg => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                // Track count for this average
                                let count_key = (key.clone(), i);
                                let current_count = avg_counts.entry(count_key).or_insert(0);
                                *current_count += 1;

                                if agg_value.values[i].is_null() {
                                    // First value - store as sum with proper type
                                    agg_value.values[i] = arg_val;
                                } else {
                                    // Add to the sum
                                    agg_value.values[i] = agg_value.values[i].add(&arg_val)?;
                                }
                            }
                        },
                        _ => {
                            return Err(format!(
                                "Unsupported aggregate type: {:?}",
                                agg.get_agg_type()
                            ));
                        },
                    }
                },
                Expression::ColumnRef(_) => {
                    debug!("Column reference found in values");
                    // For group by columns, store the value only on first occurrence
                    if agg_value.values[i].is_null() {
                        agg_value.values[i] = agg_expr.evaluate(tuple, schema).unwrap();
                    }
                },
                _ => {
                    debug!("Other expression type found in aggregates");
                    // For other expressions, evaluate each time
                    agg_value.values[i] = agg_expr.evaluate(tuple, schema).unwrap();
                },
            }
            debug!("Updated aggregate values: {:?}", agg_value.values);
        }
        Ok(())
    }
}

impl AbstractExecutor for AggregationExecutor {
    /// Initializes the aggregation executor by consuming all child tuples.
    ///
    /// This method is a **pipeline breaker**: it fully materializes all input
    /// from the child executor before returning. The aggregation cannot produce
    /// results until all group members have been seen.
    ///
    /// # Initialization Steps
    ///
    /// 1. Initialize the child executor
    /// 2. For each tuple from child:
    ///    - Evaluate GROUP BY expressions to compute group key
    ///    - Update aggregate values for that group
    /// 3. Handle empty input case (no GROUP BY → return identity values)
    /// 4. Sort groups for deterministic output ordering
    ///
    /// # Idempotency
    ///
    /// This method is idempotent; subsequent calls have no effect once
    /// initialization is complete.
    fn init(&mut self) {
        if !self.initialized {
            self.child.init();
            self.groups.clear();
            self.avg_counts.clear();

            // For empty input with no group by, create a single group with default values
            let mut has_rows = false;

            // Process all input tuples
            loop {
                match self.child.next() {
                    Ok(Some((tuple, _))) => {
                        has_rows = true;
                        // Get schema before computing key
                        let schema = self.child.get_output_schema();
                        let aggregates = self.aggregate_exprs.clone();

                        // Compute group by key
                        let mut key_values = Vec::new();
                        for expr in &self.group_by_exprs {
                            key_values.push(expr.evaluate(&tuple, schema).unwrap());
                        }
                        let key = GroupKey { values: key_values };

                        // Update aggregates for this group
                        if let Err(e) = Self::compute_aggregate(
                            &mut self.groups,
                            &mut self.avg_counts,
                            &aggregates,
                            key,
                            &tuple,
                            schema,
                        ) {
                            error!("Error computing aggregate: {}", e);
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        debug!("Error from child executor: {}", e);
                        return; // Initialize failed, but we don't propagate errors from init
                    },
                }
            }

            // Handle empty input case
            if !has_rows && self.group_by_exprs.is_empty() {
                // Create empty group key for no group by
                let key = GroupKey::new(vec![]);

                // Create default aggregate values
                let mut agg_values = AggregateValues::new(self.aggregate_exprs.len());

                // Set default values based on aggregate type
                for (i, agg_expr) in self.aggregate_exprs.iter().enumerate() {
                    if let Expression::Aggregate(agg) = agg_expr.as_ref() {
                        match agg.get_agg_type() {
                            AggregationType::Count | AggregationType::CountStar => {
                                agg_values.values[i] = Value::new(0i64);
                            },
                            AggregationType::Sum => {
                                agg_values.values[i] = Value::new(0); // or NULL depending on your requirements
                            },
                            AggregationType::Min | AggregationType::Max => {
                                agg_values.values[i] = Value::from(TypeId::Invalid);
                                // NULL for min/max
                            },
                            _ => {},
                        }
                    }
                }

                self.groups.insert(key, agg_values);
            }

            // Store all groups in the Vec for iteration and sort them by group key
            self.groups_to_return = self.groups.drain().collect();
            self.groups_to_return.sort_by(|(key1, _), (key2, _)| {
                // Compare each value in the group key
                for (v1, v2) in key1.values.iter().zip(key2.values.iter()) {
                    match v1.compare_less_than(v2) {
                        CmpBool::CmpTrue => return std::cmp::Ordering::Less,
                        CmpBool::CmpFalse => {
                            if let CmpBool::CmpTrue = v2.compare_less_than(v1) {
                                return std::cmp::Ordering::Greater;
                            }
                        },
                        CmpBool::CmpNull => continue,
                    }
                }
                std::cmp::Ordering::Equal
            });
            self.initialized = true;
        }
    }

    /// Returns the next aggregated group as a tuple.
    ///
    /// Each call returns one group's results until all groups are exhausted.
    /// Groups are returned in sorted order by group key for deterministic output.
    ///
    /// # Output Tuple Structure
    ///
    /// The output tuple contains:
    /// 1. Group key values (from GROUP BY expressions), in order
    /// 2. Aggregate results (from aggregate expressions), in order
    ///
    /// For example, `SELECT dept, SUM(salary), COUNT(*) ... GROUP BY dept`
    /// produces tuples like: `(dept_value, sum_value, count_value)`.
    ///
    /// # AVG Finalization
    ///
    /// AVG aggregates are finalized here by dividing the accumulated sum by
    /// the count. The result is converted to `Decimal` for precision.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next group's result tuple (RID is synthetic)
    /// * `Ok(None)` - No more groups to return
    /// * `Err(DBError)` - An error occurred during tuple construction
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        // Take the next group from the Vec
        if let Some((key, mut value)) = self.groups_to_return.pop() {
            debug!(
                "Processing group with key: {:?}, values: {:?}",
                key, value.values
            );
            // Process Average aggregates: divide sum by count
            for (i, agg_expr) in self.aggregate_exprs.iter().enumerate() {
                if let Expression::Aggregate(agg) = agg_expr.as_ref()
                    && let AggregationType::Avg = agg.get_agg_type()
                {
                    debug!("Processing AVG aggregate at index {}", i);
                    // Find the count for this average
                    if let Some(&count) = self.avg_counts.get(&(key.clone(), i)) {
                        debug!("Found count {} for group {:?}, aggregate {}", count, key, i);
                        debug!("Current value: {:?}", value.values[i]);
                        if count > 0 && !value.values[i].is_null() {
                            // Convert sum to decimal and divide by count to get average
                            let sum_as_decimal = match value.values[i].get_type_id() {
                                TypeId::Integer => {
                                    let int_val = value.values[i].as_integer()?;
                                    Value::new(int_val as f64)
                                },
                                TypeId::BigInt => {
                                    let bigint_val = value.values[i].as_bigint()?;
                                    Value::new(bigint_val as f64)
                                },
                                TypeId::Decimal => value.values[i].clone(),
                                _ => {
                                    debug!(
                                        "Unexpected sum type for AVG: {:?}",
                                        value.values[i].get_type_id()
                                    );
                                    Value::new(Val::Null)
                                },
                            };

                            let count_value = Value::new(count as f64);
                            debug!(
                                "Dividing sum_as_decimal {:?} by count {:?}",
                                sum_as_decimal, count_value
                            );

                            if !sum_as_decimal.is_null() {
                                match sum_as_decimal.divide(&count_value) {
                                    Ok(avg_result) => {
                                        debug!("Division successful: {:?}", avg_result);
                                        value.values[i] = avg_result;
                                    },
                                    Err(e) => {
                                        debug!("Division failed: {}", e);
                                        value.values[i] = Value::new(Val::Null);
                                    },
                                }
                            } else {
                                debug!("Sum is null, setting average to NULL");
                                value.values[i] = Value::new(Val::Null);
                            }
                        } else {
                            debug!("Count is 0 or value is null, setting to NULL");
                            value.values[i] = Value::new(Val::Null);
                        }
                    } else {
                        debug!("No count found for group {:?}, aggregate {}", key, i);
                        value.values[i] = Value::new(Val::Null);
                    }
                }
            }

            let mut values = Vec::new();
            values.extend(key.values.iter().cloned());
            values.extend(value.values.iter().cloned());

            debug!("Creating tuple with values: {:?}", values);
            debug!("Output schema: {:?}", self.output_schema);

            let tuple = Arc::new(Tuple::new(
                &values,
                &self.output_schema.clone(),
                RID::new(0, 0),
            ));
            Ok(Some((tuple, RID::new(0, 0))))
        } else {
            Ok(None)
        }
    }

    /// Returns the output schema for aggregated tuples.
    ///
    /// The schema consists of:
    /// 1. Columns from GROUP BY expressions (preserving original types)
    /// 2. Columns from aggregate expressions (with appropriate result types)
    ///
    /// # Type Mapping
    ///
    /// | Aggregate | Result Type                          |
    /// |-----------|--------------------------------------|
    /// | COUNT     | BigInt                               |
    /// | SUM       | Same as input column type            |
    /// | AVG       | Decimal                              |
    /// | MIN/MAX   | Same as input column type            |
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to the buffer pool manager, catalog, and transaction
    /// context needed during aggregation.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.exec_ctx.clone()
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
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::expressions::aggregate_expression::AggregateExpression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::types_db::value::Val::{BigInt, Integer};
    use chrono::Utc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(),
            )));

            // Create fresh transaction with unique ID
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let transaction = Arc::new(Transaction::new(
                timestamp.parse::<u64>().unwrap_or(0), // Unique transaction ID
                IsolationLevel::ReadUncommitted,
            ));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                catalog,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }
    }

    // Helper function to create a fresh executor context for each test
    fn create_test_executor_context(test_context: &TestContext) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&test_context.bpm),
            Arc::clone(&test_context.catalog),
            Arc::clone(&test_context.transaction_context),
        )))
    }

    #[tokio::test]
    async fn test_count_star() {
        let test_context = TestContext::new("test_count_star").await;
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(2), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(3), Value::new(30)], RID::new(1, 3)),
        ];

        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "test_count_star".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create COUNT(*) expression properly
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![],                               // No child expressions for COUNT(*)
            Column::new("count", TypeId::BigInt), // Use BigInt for count
            "COUNT".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![count_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        // Should get one result with count = 3
        let result = executor.next().unwrap();
        assert!(result.is_some());
        let (tuple, _) = result.unwrap();
        assert_eq!(tuple.get_value(0), Value::new(BigInt(3)));

        // No more results
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_group_by_sum() {
        let test_context = TestContext::new("test_group_by_sum").await;
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer), // Will be converted to BigInt
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_group_by_sum".to_string(),
            vec![],
        );
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create group by expression
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        // Create value expression for sum
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create SUM aggregate expression
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![value_expr.clone()],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr], // Group by group_id
            vec![sum_expr],   // Aggregate type
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Sort results by group_id for consistent checking
        results.sort_by(|a: &Arc<Tuple>, b: &Arc<Tuple>| {
            match a.get_value(0).compare_less_than(&b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        assert_eq!(results.len(), 2, "Should have exactly 2 groups");

        // Check first group (group_id = 1)
        assert_eq!(results[0].get_value(0), Value::new(Integer(1))); // Group ID as Integer
        assert_eq!(results[0].get_value(1), Value::new(Integer(30))); // Sum as Integer

        // Check second group (group_id = 2)
        assert_eq!(results[1].get_value(0), Value::new(Integer(2))); // Group ID as Integer
        assert_eq!(results[1].get_value(1), Value::new(Integer(70))); // Sum as Integer
    }

    #[tokio::test]
    async fn test_min_max_aggregation() {
        let test_context = TestContext::new("test_min_max_aggregation").await;
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(30)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(10)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(50)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(20)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_min_max_aggregation".to_string(),
            vec![],
        );
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create MIN aggregate
        let min_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Min,
            vec![value_expr.clone()],
            Column::new("min", TypeId::Integer),
            "MIN".to_string(),
        )));

        // Create MAX aggregate
        let max_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Max,
            vec![value_expr.clone()],
            Column::new("max", TypeId::Integer),
            "MAX".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![min_expr, max_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Sort results by group_id for consistent checking
        results.sort_by(|a: &Arc<Tuple>, b: &Arc<Tuple>| {
            match a.get_value(0).compare_less_than(&b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        assert_eq!(results.len(), 2, "Should have 2 groups");

        // Check first group
        assert_eq!(results[0].get_value(0), Value::new(1));
        assert_eq!(results[0].get_value(1), Value::new(10)); // Min stays Integer
        assert_eq!(results[0].get_value(2), Value::new(30)); // Max stays Integer

        // Check second group
        assert_eq!(results[1].get_value(0), Value::new(2));
        assert_eq!(results[1].get_value(1), Value::new(20)); // Min
        assert_eq!(results[1].get_value(2), Value::new(50)); // Max
    }

    #[tokio::test]
    async fn test_min_max_without_groupby() {
        let test_context = TestContext::new("test_min_max_without_groupby").await;
        let input_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(30)], RID::new(1, 1)),
            (vec![Value::new(10)], RID::new(1, 2)),
            (vec![Value::new(50)], RID::new(1, 3)),
            (vec![Value::new(20)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_min_max_without_groupby".to_string(),
            vec![],
        );
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create MIN aggregate
        let min_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Min,
            vec![value_expr.clone()],
            Column::new("min", TypeId::Integer),
            "MIN".to_string(),
        )));

        // Create MAX aggregate
        let max_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Max,
            vec![value_expr.clone()],
            Column::new("max", TypeId::Integer),
            "MAX".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![], // No group by
            vec![],
            vec![min_expr, max_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let result = executor.next().unwrap();
        assert!(result.is_some());

        let (tuple, _) = result.unwrap();
        assert_eq!(tuple.get_value(0), Value::new(Integer(10))); // Min
        assert_eq!(tuple.get_value(1), Value::new(Integer(50))); // Max

        // No more results
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_multiple_aggregates() {
        let test_context = TestContext::new("test_multiple_aggregates").await;
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(10)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 4)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 5)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_multiple_aggregates".to_string(),
            vec![],
        );
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create group by expression
        let group_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        // Create value expression
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![value_expr.clone()],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        )));

        // Create COUNT aggregate
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![value_expr.clone()],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_by_expr],
            vec![sum_expr, count_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Sort results by group_id for consistent checking
        results.sort_by(|a, b| {
            let a_val = a.get_value(0).as_integer().unwrap();
            let b_val = b.get_value(0).as_integer().unwrap();
            a_val.cmp(&b_val)
        });

        assert_eq!(results.len(), 2, "Should have exactly 2 groups");

        // Check first group (group_id = 1)
        assert_eq!(results[0].get_value(0).as_integer().unwrap(), 1);
        assert_eq!(results[0].get_value(1).as_integer().unwrap(), 30); // sum = 10 + 20
        assert_eq!(results[0].get_value(2).as_bigint().unwrap(), 2); // count = 2

        // Check second group (group_id = 2)
        assert_eq!(results[1].get_value(0).as_integer().unwrap(), 2);
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 80); // sum = 10 + 30 + 40
        assert_eq!(results[1].get_value(2).as_bigint().unwrap(), 3); // count = 3
    }

    #[tokio::test]
    async fn test_empty_input() {
        let test_context = TestContext::new("test_empty_input").await;
        let input_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "test_empty_input".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            vec![], // Empty input
            input_schema.clone(),
        ));

        // Create COUNT(*) expression properly
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![],                               // No child expressions for COUNT(*)
            Column::new("count", TypeId::BigInt), // Use BigInt for count
            "COUNT".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![count_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let result = executor.next().unwrap();
        assert!(
            result.is_some(),
            "Should return a result even with empty input"
        );

        let (tuple, _) = result.unwrap();
        assert_eq!(
            tuple.get_value(0),
            Value::new(BigInt(0)),
            "Count should be 0 for empty input"
        );

        // No more results
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_single_group() {
        let test_context = TestContext::new("test_single_group").await;
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(1), Value::new(30)], RID::new(1, 3)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_single_group".to_string(),
            vec![],
        );
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![value_expr],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![sum_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1, "Should have exactly one group");
        assert_eq!(results[0].get_value(0), Value::new(Integer(1)));
        assert_eq!(results[0].get_value(1), Value::new(Integer(60))); // 10 + 20 + 30
    }

    #[tokio::test]
    async fn test_aggregation_with_types() {
        let test_context = TestContext::new("test_aggregation_with_types").await;
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::VarChar), // String group
            Column::new("int_val", TypeId::Integer),  // Integer values
            Column::new("big_val", TypeId::BigInt),   // BigInt values
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        // Create test data with different numeric types
        let mock_tuples = vec![
            // Group A
            (
                vec![
                    Value::new("A"),         // group_id
                    Value::new(Integer(10)), // int_val
                    Value::new(BigInt(100)), // big_val - already BigInt
                ],
                RID::new(0, 0),
            ),
            (
                vec![
                    Value::new("A"),
                    Value::new(Integer(20)),
                    Value::new(BigInt(200)),
                ],
                RID::new(0, 1),
            ),
            // Group B
            (
                vec![
                    Value::new("B"),
                    Value::new(Integer(30)),
                    Value::new(BigInt(300)),
                ],
                RID::new(0, 2),
            ),
            (
                vec![
                    Value::new("B"),
                    Value::new(Integer(40)),
                    Value::new(BigInt(400)),
                ],
                RID::new(0, 3),
            ),
            // Group C - Single row group
            (
                vec![
                    Value::new("C"),
                    Value::new(Integer(50)),
                    Value::new(BigInt(500)),
                ],
                RID::new(0, 4),
            ),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_aggregation_with_types".to_string(),
            vec![],
        );

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("group_id", TypeId::VarChar),
            vec![],
        )));

        let int_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("int_val", TypeId::Integer),
            vec![],
        )));

        let big_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("big_val", TypeId::BigInt),
            vec![],
        )));

        // Create aggregates
        let sum_int = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![int_col.clone()],
            Column::new("sum_int", TypeId::Integer),
            "SUM".to_string(),
        )));

        let sum_big = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![big_col.clone()],
            Column::new("sum_big", TypeId::BigInt),
            "SUM".to_string(),
        )));

        let count_star = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        )));

        // Create aggregation plan
        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![sum_int, sum_big, count_star],
        ));

        // Create and execute the aggregation
        let mut executor = AggregationExecutor::new(
            exec_ctx.clone(),
            agg_plan,
            Box::new(MockExecutor::new(
                exec_ctx,
                Arc::new(mock_scan_plan),
                0,
                mock_tuples,
                input_schema,
            )),
        );

        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Sort results by group_id for consistent checking
        results.sort_by(|a, b| {
            let a_str = ToString::to_string(&a.get_value(0));
            let b_str = ToString::to_string(&b.get_value(0));
            a_str.cmp(&b_str)
        });

        assert_eq!(results.len(), 3, "Should have three groups (A, B, C)");

        // Check Group A
        assert_eq!(ToString::to_string(&results[0].get_value(0)), "A");
        assert_eq!(results[0].get_value(1).as_integer().unwrap(), 30); // sum of int_val (10 + 20)
        assert_eq!(results[0].get_value(2).as_bigint().unwrap(), 300); // sum of big_val (100 + 200)
        assert_eq!(results[0].get_value(3).as_bigint().unwrap(), 2); // count = 2

        // Check Group B
        assert_eq!(ToString::to_string(&results[1].get_value(0)), "B");
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 70); // sum of int_val (30 + 40)
        assert_eq!(results[1].get_value(2).as_bigint().unwrap(), 700); // sum of big_val (300 + 400)
        assert_eq!(results[1].get_value(3).as_bigint().unwrap(), 2); // count = 2

        // Check Group C
        assert_eq!(ToString::to_string(&results[2].get_value(0)), "C");
        assert_eq!(results[2].get_value(1).as_integer().unwrap(), 50); // int_val = 50
        assert_eq!(results[2].get_value(2).as_bigint().unwrap(), 500); // big_val = 500
        assert_eq!(results[2].get_value(3).as_bigint().unwrap(), 1); // count = 1
    }

    #[tokio::test]
    async fn test_aggregation_column_names() {
        let test_context = TestContext::new("test_aggregation_column_names").await;
        let input_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar), // Name column
            Column::new("age", TypeId::Integer),  // Age column
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        // Create test data
        let mock_tuples = vec![
            (vec![Value::new("John Doe"), Value::new(35)], RID::new(0, 0)),
            (vec![Value::new("John Doe"), Value::new(35)], RID::new(0, 1)),
            (
                vec![Value::new("Jane Smith"), Value::new(64)],
                RID::new(0, 2),
            ),
            (
                vec![Value::new("Jane Smith"), Value::new(64)],
                RID::new(0, 3),
            ),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_aggregation_column_names".to_string(),
            vec![],
        );

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![age_col.clone()],
            Column::new("SUM(age)", TypeId::Integer),
            "SUM".to_string(),
        )));

        // Create aggregation plan
        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![sum_expr],
        ));

        // Create and execute the aggregation
        let mut executor = AggregationExecutor::new(
            exec_ctx.clone(),
            agg_plan,
            Box::new(MockExecutor::new(
                exec_ctx,
                Arc::new(mock_scan_plan),
                0,
                mock_tuples,
                input_schema,
            )),
        );

        executor.init();

        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Sort results by name for consistent checking
        results.sort_by(|a, b| {
            let a_str = ToString::to_string(&a.get_value(0));
            let b_str = ToString::to_string(&b.get_value(0));
            a_str.cmp(&b_str)
        });

        assert_eq!(results.len(), 2, "Should have two groups");

        // Check output schema column names
        let output_schema = executor.get_output_schema();
        assert_eq!(
            output_schema.get_columns()[0].get_name(),
            "name",
            "First column should be named 'name'"
        );
        assert_eq!(
            output_schema.get_columns()[1].get_name(),
            "SUM_age",
            "Second column should be named 'SUM_age'"
        );

        // Check first group (Jane Smith)
        assert_eq!(ToString::to_string(&results[0].get_value(0)), "Jane Smith");
        assert_eq!(results[0].get_value(1).as_integer().unwrap(), 128); // 64 + 64

        // Check second group (John Doe)
        assert_eq!(ToString::to_string(&results[1].get_value(0)), "John Doe");
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 70); // 35 + 35
    }
}
