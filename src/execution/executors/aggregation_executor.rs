use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::aggregation_plan::{
    AggregateKey, AggregateValue, AggregationPlanNode,
};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use log::{debug, error};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use crate::common::exception::ExpressionError;

pub struct AggregationExecutor {
    child_executor: Box<dyn AbstractExecutor>,
    agg_plan: Arc<AggregationPlanNode>,
    agg_output_schema: Schema,
    exec_ctx: Arc<RwLock<ExecutorContext>>,
    // Store aggregation state
    group_values: HashMap<AggregateKey, AggregateValue>,
    has_results: bool,
}

impl AbstractExecutor for AggregationExecutor {
    /// Initialize the executor
    fn init(&mut self) {
        // Initialize child executor
        self.child_executor.init();

        // Clear any existing group values
        self.group_values.clear();

        // Reset result tracking
        self.has_results = false;
    }

    /// Get the next tuple from the executor
    fn next(&mut self) -> Option<(Tuple, RID)> {
        // Ensure all input tuples are processed
        self.process_input_tuples_if_needed()?;

        // Determine aggregation type and handle accordingly
        if self.is_global_aggregation() {
            self.handle_global_aggregation()
        } else {
            self.handle_group_by_aggregation()
        }
    }

    /// Get the output schema for this executor
    fn get_output_schema(&self) -> Schema {
        self.agg_output_schema.clone()
    }

    /// Get the executor context
    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        Arc::clone(&self.exec_ctx)
    }
}

impl AggregationExecutor {
    /// Create a new AggregationExecutor
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        agg_plan: Arc<AggregationPlanNode>,
        exec_ctx: Arc<RwLock<ExecutorContext>>,
    ) -> Self {
        // Create output schema from the aggregation plan
        let agg_output_schema = agg_plan.get_output_schema().clone();

        Self {
            child_executor,
            agg_plan,
            agg_output_schema,
            exec_ctx,
            group_values: HashMap::new(),
            has_results: false,
        }
    }

    /// Evaluate group by expressions for a tuple
    fn evaluate_group_by(
        &self,
        tuple: &Tuple,
        schema: &Schema
    ) -> Result<AggregateKey, ExpressionError> {
        // If no group by expressions, return an empty key
        if self.agg_plan.get_group_bys().is_empty() {
            return Ok(AggregateKey { group_bys: Vec::new() });
        }

        // Evaluate group by expressions
        let mut group_values = Vec::new();
        for group_by_expr in self.agg_plan.get_group_bys() {
            let group_value = group_by_expr.evaluate(tuple, schema)?;
            debug!("Group by value: {:?}", group_value);
            group_values.push(group_value);
        }

        Ok(AggregateKey { group_bys: group_values })
    }

    /// Evaluate aggregate expressions for a tuple
    fn evaluate_aggregates(
        &self,
        tuple: &Tuple,
        schema: &Schema,
    ) -> Result<Vec<Value>, ExpressionError> {
        // When group by is used, we want the aggregate column
        if !self.agg_plan.get_aggregates().is_empty() {
            // Use the first aggregate expression
            let agg_expr = &self.agg_plan.get_aggregates()[0];

            // Evaluate the aggregate expression on the tuple
            let value = agg_expr.evaluate(tuple, schema)?;

            debug!("Evaluated aggregate expression: {:?}", value);

            Ok(vec![value])
        } else {
            Err(ExpressionError::InvalidOperation("No aggregate expressions".to_string()))
        }
    }

    /// Process all input tuples if not already done
    fn process_input_tuples_if_needed(&mut self) -> Option<()> {
        // If results are already processed and group_values is empty, return None
        if self.has_results && self.group_values.is_empty() {
            debug!("Already returned all results, returning None");
            return None;
        }

        // Process tuples only if not already processed
        if !self.has_results {
            self.process_input_tuples()?;
            self.has_results = true;

            // Debug group values
            self.log_group_values();
        }

        Some(())
    }

    /// Process all input tuples from child executor
    fn process_input_tuples(&mut self) -> Option<()> {
        debug!("Starting to process input tuples");

        while let Some((tuple, _)) = self.child_executor.next() {
            self.process_single_tuple(&tuple)?;
        }

        Some(())
    }

    /// Process a single tuple
    fn process_single_tuple(&mut self, tuple: &Tuple) -> Option<()> {
        let schema = self.child_executor.get_output_schema();
        debug!("Processing tuple: {:?}", tuple.get_values());

        // Get group by key
        let group_key = self.evaluate_group_by(tuple, &schema)
            .map_err(|e| {
                error!("Failed to evaluate group key: {}", e);
                e
            });

        // Evaluate aggregate values
        let agg_values = self.evaluate_aggregates(tuple, &schema)
            .map_err(|e| {
                error!("Failed to evaluate aggregate values: {}", e);
                e
            });

        // Update group aggregates
        self.update_group_aggregates(group_key.unwrap(), &agg_values.unwrap());

        Some(())
    }

    /// Update aggregates for a specific group
    fn update_group_aggregates(&mut self, group_key: AggregateKey, agg_values: &[Value]) {
        // Get or create current aggregates for this group
        let current_aggregates = self.group_values
            .entry(group_key.clone())
            .or_insert_with(|| {
                debug!("Creating new aggregate entry");
                // Initialize based on the first aggregate type
                AggregateValue {
                    aggregates: vec![Value::new(Val::Integer(0)); agg_values.len()]
                }
            });

        // Update each aggregate value
        let mut new_aggregates = current_aggregates.aggregates.clone();
        for (i, (agg_value, agg_type)) in agg_values.iter()
            .zip(self.agg_plan.get_aggregate_types())
            .enumerate() {
            debug!(
            "Computing aggregate for index {}: Current = {:?}, New = {:?}, Type = {:?}",
            i,
            current_aggregates.aggregates[i],
            agg_value,
            agg_type
        );

            // Special handling for Sum aggregation
            if agg_type == &AggregationType::Sum {
                match (&current_aggregates.aggregates[i], agg_value) {
                    (_, Value { value_: Val::Integer(new_val), .. }) => {
                        let current_sum = match &current_aggregates.aggregates[i] {
                            Value { value_: Val::Integer(curr), .. } => *curr,
                            _ => 0,
                        };
                        new_aggregates[i] = Value::new(current_sum + *new_val);
                    },
                    _ => {
                        if let Ok(new_value) = Self::compute_aggregate_value(
                            agg_type.clone(),
                            Some(&current_aggregates.aggregates[i]),
                            agg_value,
                        ) {
                            new_aggregates[i] = new_value;
                        }
                    }
                }
            } else {
                // For other aggregation types, use the original compute method
                if let Ok(new_value) = Self::compute_aggregate_value(
                    agg_type.clone(),
                    Some(&current_aggregates.aggregates[i]),
                    agg_value,
                ) {
                    new_aggregates[i] = new_value;
                }
            }

            debug!("Updated aggregate value: {:?}", new_aggregates[i]);
        }
        current_aggregates.aggregates = new_aggregates;
    }

    /// Compute the aggregate value based on aggregation type
    fn compute_aggregate_value(
        agg_type: AggregationType,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<Value, ExpressionError> {
        match agg_type {
            AggregationType::Sum => {
                // Detailed sum computation with comprehensive type conversion
                match (old_value, new_value.get_value()) {
                    // Initial sum cases
                    (None, Val::Integer(val)) => Ok(Value::new(*val)),
                    (None, Val::BigInt(val)) => Ok(Value::new(*val as i32)),
                    (None, Val::Decimal(val)) => Ok(Value::new(*val as i32)),

                    // Summing with existing Integer value
                    (Some(Value { value_: Val::Integer(sum), .. }), Val::Integer(new_val)) => {
                        Ok(Value::new(*sum + *new_val))
                    },
                    (Some(Value { value_: Val::Integer(sum), .. }), Val::BigInt(new_val)) => {
                        Ok(Value::new(*sum + *new_val as i32))
                    },
                    (Some(Value { value_: Val::Integer(sum), .. }), Val::Decimal(new_val)) => {
                        Ok(Value::new(*sum + *new_val as i32))
                    },

                    // Summing with existing BigInt value
                    (Some(Value { value_: Val::BigInt(sum), .. }), Val::Integer(new_val)) => {
                        Ok(Value::new((*sum + *new_val as i64) as i32))
                    },
                    (Some(Value { value_: Val::BigInt(sum), .. }), Val::BigInt(new_val)) => {
                        Ok(Value::new((*sum + *new_val) as i32))
                    },
                    (Some(Value { value_: Val::BigInt(sum), .. }), Val::Decimal(new_val)) => {
                        Ok(Value::new((*sum as f64 + *new_val) as i32))
                    },

                    // Summing with existing Decimal value
                    (Some(Value { value_: Val::Decimal(sum), .. }), Val::Integer(new_val)) => {
                        Ok(Value::new((*sum + *new_val as f64) as i32))
                    },
                    (Some(Value { value_: Val::Decimal(sum), .. }), Val::BigInt(new_val)) => {
                        Ok(Value::new((*sum + *new_val as f64) as i32))
                    },
                    (Some(Value { value_: Val::Decimal(sum), .. }), Val::Decimal(new_val)) => {
                        Ok(Value::new((*sum + *new_val) as i32))
                    },

                    // Fallback for unsupported combinations
                    _ => Err(ExpressionError::InvalidOperation("Unsupported sum aggregation".to_string())),
                }
            },
            AggregationType::CountStar | AggregationType::Count => {
                match old_value {
                    Some(val) => match val.get_value() {
                        Val::Integer(count) => Ok(Value::new(count + 1)),
                        Val::BigInt(count) => Ok(Value::new((count + 1) as i32)),
                        _ => Err(ExpressionError::InvalidOperation("Invalid count value".to_string())),
                    },
                    None => Ok(Value::new(1i32)),
                }
            },
            // Rest of the implementation remains the same
            AggregationType::Min => {
                match old_value {
                    Some(old) => {
                        if old.compare_less_than(new_value) == CmpBool::CmpTrue {
                            Ok(old.clone())
                        } else {
                            Ok(new_value.clone())
                        }
                    },
                    None => Ok(new_value.clone()),
                }
            },
            AggregationType::Max => {
                match old_value {
                    Some(old) => {
                        if old.compare_greater_than(new_value) == CmpBool::CmpTrue {
                            Ok(old.clone())
                        } else {
                            Ok(new_value.clone())
                        }
                    },
                    None => Ok(new_value.clone()),
                }
            },
            AggregationType::Avg => {
                Err(ExpressionError::InvalidOperation("AVG aggregation not supported".to_string()))
            },
        }
    }

    /// Log group values for debugging
    fn log_group_values(&self) {
        debug!("Final group values:");
        for (key, value) in &self.group_values {
            debug!("Group {:?}: {:?}", key, value);
        }
    }

    /// Check if this is a global (no group by) aggregation
    fn is_global_aggregation(&self) -> bool {
        self.agg_plan.get_group_bys().is_empty()
    }

    /// Handle global aggregation
    fn handle_global_aggregation(&mut self) -> Option<(Tuple, RID)> {
        // Extract the values for global aggregation
        let global_result = self.group_values.values().next().map(|values| {
            let rid = RID::new(0, 0);
            debug!("Returning global aggregation result: {:?}", values.aggregates);
            (
                Tuple::new(&values.aggregates, self.agg_output_schema.clone(), rid),
                rid,
            )
        });

        // Clear group_values after extracting the result
        self.group_values.clear();

        global_result
    }

    /// Handle group by aggregation
    fn handle_group_by_aggregation(&mut self) -> Option<(Tuple, RID)> {
        // If we have group results
        if !self.group_values.is_empty() {
            // Create a vector of keys to avoid borrowing issues
            let group_keys: Vec<_> = self.group_values.keys().cloned().collect();

            // Take the first key
            let group_key = group_keys.first().unwrap();

            // Get the group value
            let group_value = self.group_values.get(group_key).unwrap();

            // Combine group by values with aggregate values
            let mut result_values = group_key.group_bys.clone();
            result_values.extend(group_value.aggregates.clone());

            let rid = RID::new(0, 0);
            debug!("Preparing group result: {:?}", result_values);

            // Remove this group from group_values
            self.group_values.remove(group_key);

            Some((
                Tuple::new(&result_values, self.agg_output_schema.clone(), rid),
                rid,
            ))
        } else {
            // No more results
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::executors::mock_executor::MockExecutor;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;
    use crate::types_db::value::Val::{BigInt, Integer};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        pub fn new(test_name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )));

            let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(catalog)));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));

            Self {
                bpm,
                transaction_manager,
                lock_manager,
                db_file,
                db_log_file,
            }
        }

        pub fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    fn create_test_executor_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutorContext>> {
        let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
        Arc::new(RwLock::new(ExecutorContext::new(
            transaction,
            Arc::clone(&test_context.transaction_manager),
            catalog,
            Arc::clone(&test_context.bpm),
            Arc::clone(&test_context.lock_manager),
        )))
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            Arc::clone(&ctx.bpm),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn test_count_star() {
        let test_context = TestContext::new("agg_count_star");

        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        // Create output schema for COUNT(*) - should only have one column
        let output_schema = Schema::new(vec![Column::new("count", TypeId::Integer)]);

        // Create catalog and get context
        let mut catalog = create_catalog(&test_context);
        let table_info = catalog
            .create_table("test_table", input_schema.clone())
            .expect("Failed to create table");

        let table_heap = table_info.get_table_heap();

        // Insert test data
        let test_data = vec![(1, 10), (2, 20), (3, 30)];

        for (id, value) in test_data {
            let values = vec![Value::new(id), Value::new(value)];
            let mut tuple = Tuple::new(&values, input_schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(0, false);
            table_heap.insert_tuple(&meta, &mut tuple).unwrap();
        }

        let catalog = Arc::new(RwLock::new(catalog));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // Create COUNT(*) expression
        let count_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("count", TypeId::BigInt),
            vec![],
        )));

        // Create mock child executor
        let child_executor = Box::new(MockExecutor::new(
            vec![
                (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
                (vec![Value::new(2), Value::new(20)], RID::new(1, 2)),
                (vec![Value::new(3), Value::new(30)], RID::new(1, 3)),
            ],
            input_schema,
            exec_ctx.clone(),
        ));

        // Create aggregation plan with correct output schema
        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![],
            vec![count_expr],
            vec![AggregationType::CountStar],
        ));

        // Create and execute aggregation executor
        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        // Verify results
        let (result_tuple, _) = executor.next().expect("Expected one result tuple");
        assert_eq!(*result_tuple.get_value(0), Value::from(Integer(3)));
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_group_by_sum() {
        // Create test context
        let test_context = TestContext::new("agg_group_sum");

        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        // Create output schema
        let output_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("sum", TypeId::Integer),
        ]);

        // Setup catalog and execution context
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // Prepare mock tuples
        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 4)),
        ];

        // Create mock child executor
        let child_executor = Box::new(MockExecutor::new(
            mock_tuples.clone(),
            input_schema.clone(),
            exec_ctx.clone(),
        ));

        // Create group-by expression for group_id
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // group_id column index
            0, // tuple index
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        // Create sum aggregate expression for value
        let sum_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // value column index
            0, // tuple index
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create aggregation plan
        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema.clone(),
            Vec::new(), // No children
            vec![group_expr.clone()], // Group by expressions
            vec![sum_expr.clone()],   // Aggregate expressions
            vec![AggregationType::Sum] // Aggregate types
        ));

        // Create and initialize aggregation executor
        let mut executor = AggregationExecutor::new(
            child_executor,
            agg_plan,
            exec_ctx
        );
        executor.init();

        // Collect results with additional debugging
        let mut results = Vec::new();
        let mut iteration = 0;
        while let Some((tuple, _)) = executor.next() {
            println!("Iteration {}: Collected tuple: {:?}", iteration, tuple.get_values());
            results.push(tuple.clone());
            iteration += 1;
        }

        // Print out all collected results for debugging
        println!("Total results collected: {}", results.len());
        for (i, result) in results.iter().enumerate() {
            println!("Result {}: {:?}", i, result.get_values());
        }

        // Verify number of results
        assert_eq!(results.len(), 2, "Should have exactly 2 groups");

        // Verify results
        // Sort to ensure consistent order
        results.sort_by(|a, b| {
            match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        // Check first group (group_id = 1)
        assert_eq!(
            *results[0].get_value(0),
            Value::new(1i32),
            "First group should have group_id = 1"
        );
        assert_eq!(
            *results[0].get_value(1),
            Value::new(30i32),
            "First group should have sum = 30 (10 + 20)"
        );

        // Check second group (group_id = 2)
        assert_eq!(
            *results[1].get_value(0),
            Value::new(2i32),
            "Second group should have group_id = 2"
        );
        assert_eq!(
            *results[1].get_value(1),
            Value::new(70i32),
            "Second group should have sum = 70 (30 + 40)"
        );
    }
}
