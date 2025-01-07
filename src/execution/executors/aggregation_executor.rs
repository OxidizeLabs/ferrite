use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::aggregation_plan::{
    AggregateKey, AggregateValue, AggregationPlanNode,
};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use log::{debug, error};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

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
        schema: &Schema,
    ) -> Result<AggregateKey, ExpressionError> {
        // If no group by expressions, return an empty key
        if self.agg_plan.get_group_bys().is_empty() {
            return Ok(AggregateKey {
                group_bys: Vec::new(),
            });
        }

        // Evaluate group by expressions
        let mut group_values = Vec::new();
        for group_by_expr in self.agg_plan.get_group_bys() {
            let group_value = group_by_expr.evaluate(tuple, schema)?;
            debug!("Group by value: {:?}", group_value);
            group_values.push(group_value);
        }

        Ok(AggregateKey {
            group_bys: group_values,
        })
    }

    /// Evaluate aggregate expressions for a tuple
    fn evaluate_aggregates(&self, tuple: &Tuple, schema: &Schema) -> Result<Vec<Value>, ExpressionError> {
        let mut agg_values = Vec::new();

        for (i, agg_expr) in self.agg_plan.get_aggregates().iter().enumerate() {
            let value = agg_expr.evaluate(tuple, schema)?;
            debug!("Evaluated aggregate value for {} = {:?}", i, value);
            agg_values.push(value);
        }
        Ok(agg_values)
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

        let mut has_input = false;
        while let Some((tuple, _)) = self.child_executor.next() {
            has_input = true;
            self.process_single_tuple(&tuple)?;
        }

        // For empty input with CountStar, initialize with 0
        if !has_input && self.agg_plan.get_aggregate_types().contains(&AggregationType::CountStar) {
            let empty_key = AggregateKey { group_bys: Vec::new() };
            let zero_value = Value::new(0);
            self.group_values.insert(empty_key, AggregateValue {
                aggregates: vec![zero_value],
            });
        }

        Some(())
    }

    /// Process a single tuple
    fn process_single_tuple(&mut self, tuple: &Tuple) -> Option<()> {
        let schema = self.child_executor.get_output_schema();

        let group_key = match self.evaluate_group_by(tuple, &schema) {
            Ok(key) => key,
            Err(e) => {
                error!("Failed to evaluate group key: {}", e);
                return None;
            }
        };

        let agg_values = match self.evaluate_aggregates(tuple, &schema) {
            Ok(values) => values,
            Err(e) => {
                error!("Failed to evaluate aggregate values: {}", e);
                return None;
            }
        };

        self.update_group_aggregates(group_key, &agg_values);
        Some(())
    }

    /// Update aggregates for a specific group
    fn update_group_aggregates(&mut self, group_key: AggregateKey, agg_values: &[Value]) {
        // Get or create current aggregates for this group
        let current_aggregates = self.group_values.entry(group_key).or_insert_with(|| {
            AggregateValue {
                aggregates: agg_values.iter().enumerate().map(|(i, _val)| {
                    match self.agg_plan.get_aggregate_types()[i] {
                        AggregationType::Min => Value::new(i32::MAX),
                        AggregationType::Max => Value::new(i32::MIN),
                        _ => Value::new(0)
                    }
                }).collect()
            }
        });

        for (i, (agg_value, agg_type)) in agg_values.iter()
            .zip(self.agg_plan.get_aggregate_types())
            .enumerate()
        {
            debug!(
           "Computing aggregate for index {}: Current = {:?}, New = {:?}, Type = {:?}",
           i, current_aggregates.aggregates[i], agg_value, agg_type
       );
            match agg_type {
                AggregationType::Sum => {
                    let curr = match current_aggregates.aggregates[i].get_value() {
                        Val::Integer(v) => *v,
                        _ => 0
                    };
                    let new = match agg_value.get_value() {
                        Val::Integer(v) => *v,
                        _ => 0
                    };
                    current_aggregates.aggregates[i] = Value::new(curr + new);
                }
                AggregationType::Count | AggregationType::CountStar => {
                    let count = match current_aggregates.aggregates[i].get_value() {
                        Val::Integer(v) => *v,
                        _ => 0
                    };
                    current_aggregates.aggregates[i] = Value::new(count + 1);
                }
                AggregationType::Min => {
                    let curr = match current_aggregates.aggregates[i].get_value() {
                        Val::Integer(v) => *v,
                        _ => i32::MAX
                    };
                    let new = match agg_value.get_value() {
                        Val::Integer(v) => *v,
                        _ => curr
                    };
                    current_aggregates.aggregates[i] = Value::new(curr.min(new));
                }
                AggregationType::Max => {
                    let curr = match current_aggregates.aggregates[i].get_value() {
                        Val::Integer(v) => *v,
                        _ => i32::MIN
                    };
                    let new = match agg_value.get_value() {
                        Val::Integer(v) => *v,
                        _ => curr
                    };
                    current_aggregates.aggregates[i] = Value::new(curr.max(new));
                }
                AggregationType::Avg => {
                    // Avg requires tracking count and sum separately
                    // Not implementing for now
                    current_aggregates.aggregates[i] = Value::new(0);
                }
            }
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
        let global_result = self.group_values.values().next().map(|values| {
            let rid = RID::new(0, 0);
            // Just use the aggregate values directly for global aggregation
            (Tuple::new(&values.aggregates, self.agg_output_schema.clone(), rid), rid)
        });

        // Clear after getting result
        self.group_values.clear();
        global_result
    }

    /// Handle group by aggregation
    fn handle_group_by_aggregation(&mut self) -> Option<(Tuple, RID)> {
        if self.group_values.is_empty() {
            return None;
        }

        let group_keys: Vec<_> = self.group_values.keys().cloned().collect();
        let group_key = group_keys.first().unwrap();
        let group_value = self.group_values.remove(group_key).unwrap();

        let mut values = if !self.agg_plan.get_group_bys().is_empty() {
            group_key.group_bys.clone()
        } else {
            Vec::new()
        };
        values.extend(group_value.aggregates);

        let rid = RID::new(0, 0);
        Some((Tuple::new(&values, self.agg_output_schema.clone(), rid), rid))
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
    use crate::execution::plans::mock_scan_plan::MockScanNode;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use crate::types_db::value::Val::Integer;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
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

            let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
            let transaction_manager =
                Arc::new(RwLock::new(TransactionManager::new(catalog, log_manager)));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager.clone())));

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

    fn create_mock_executor(context: Arc<RwLock<ExecutorContext>>) {
        let mock_scan_plan = MockScanNode::new(Default::default(), "mock_table".to_string(), vec![]);
        MockExecutor::new(
            context,
            Arc::new(mock_scan_plan),
            0,
            vec![],
            Default::default(),
        );
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);

        // Create mock child executor
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_scan_plan),
            0,
            vec![
                (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
                (vec![Value::new(2), Value::new(20)], RID::new(1, 2)),
                (vec![Value::new(3), Value::new(30)], RID::new(1, 3)),
            ],
            input_schema,
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);

        // Create mock child executor
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_scan_plan),
            0,
            mock_tuples,
            input_schema,
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
            0, // value column index
            1, // tuple index
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        // Create aggregation plan
        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema.clone(),
            Vec::new(),                 // No children
            vec![group_expr.clone()],   // Group by expressions
            vec![sum_expr.clone()],     // Aggregate expressions
            vec![AggregationType::Sum], // Aggregate types
        ));

        // Create and initialize aggregation executor
        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        // Collect results with additional debugging
        let mut results = Vec::new();
        let mut iteration = 0;
        while let Some((tuple, _)) = executor.next() {
            println!(
                "Iteration {}: Collected tuple: {:?}",
                iteration,
                tuple.get_values()
            );
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
        results.sort_by(
            |a, b| match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            },
        );

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

    #[test]
    fn test_min_max_aggregation() {
        let test_context = TestContext::new("agg_min_max");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("min", TypeId::Integer),
            Column::new("max", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // Prepare test data with two groups
        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(30)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(10)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(50)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(20)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::Integer), vec![],
        )));
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("value", TypeId::Integer), vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![group_expr],
            vec![value_expr.clone(), value_expr.clone()],
            vec![AggregationType::Min, AggregationType::Max],
        ));

        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            results.push(tuple);
        }

        results.sort_by(|a, b| {
            match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        assert_eq!(results.len(), 2, "Should have 2 groups");

        // Check first group
        assert_eq!(*results[0].get_value(0), Value::new(1));
        assert_eq!(*results[0].get_value(1), Value::new(10)); // Min
        assert_eq!(*results[0].get_value(2), Value::new(30)); // Max

        // Check second group
        assert_eq!(*results[1].get_value(0), Value::new(2));
        assert_eq!(*results[1].get_value(1), Value::new(20)); // Min
        assert_eq!(*results[1].get_value(2), Value::new(50)); // Max
    }

    #[test]
    fn test_min_max_without_groupby() {
        let test_context = TestContext::new("min_max_no_group");
        let input_schema = Schema::new(vec![
            Column::new("value", TypeId::Integer),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("min", TypeId::Integer),
            Column::new("max", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mock_tuples = vec![
            (vec![Value::new(30)], RID::new(1, 1)),
            (vec![Value::new(10)], RID::new(1, 2)),
            (vec![Value::new(50)], RID::new(1, 3)),
            (vec![Value::new(20)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("value", TypeId::Integer), vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![], // No group by
            vec![value_expr.clone(), value_expr.clone()],
            vec![AggregationType::Min, AggregationType::Max],
        ));

        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        let result = executor.next();
        assert!(result.is_some());

        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(10)); // Min
        assert_eq!(*tuple.get_value(1), Value::new(50)); // Max

        // No more results
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_multiple_aggregates() {
        let test_context = TestContext::new("multiple_aggs");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("sum", TypeId::Integer),
            Column::new("count", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(10)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 4)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 5)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::Integer), vec![],
        )));
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("value", TypeId::Integer), vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![group_expr],
            vec![value_expr.clone(), value_expr.clone()],
            vec![AggregationType::Sum, AggregationType::Count],
        ));

        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            results.push(tuple);
        }

        results.sort_by(|a, b| {
            match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        assert_eq!(results.len(), 2);

        // Group 1: sum=30, count=2
        assert_eq!(*results[0].get_value(0), Value::new(1));
        assert_eq!(*results[0].get_value(1), Value::new(30));
        assert_eq!(*results[0].get_value(2), Value::new(2));

        // Group 2: sum=70, count=3
        assert_eq!(*results[1].get_value(0), Value::new(2));
        assert_eq!(*results[1].get_value(1), Value::new(80));
        assert_eq!(*results[1].get_value(2), Value::new(3));
    }

    #[test]
    fn test_empty_input() {
        let test_context = TestContext::new("empty_input");
        let input_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);
        let output_schema = Schema::new(vec![Column::new("count", TypeId::Integer)]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            vec![], // Empty input
            input_schema.clone(),
        ));

        let count_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("count", TypeId::Integer),
            vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![],
            vec![count_expr],
            vec![AggregationType::CountStar],
        ));

        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        let result = executor.next();
        assert!(result.is_some(), "Should return a result even with empty input");

        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(0), "Count should be 0 for empty input");
    }

    #[test]
    fn test_single_group() {
        let test_context = TestContext::new("single_group");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("sum", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // All tuples in same group (group_id = 1)
        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(1), Value::new(30)], RID::new(1, 3)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::Integer), vec![],
        )));
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("value", TypeId::Integer), vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![group_expr],
            vec![value_expr],
            vec![AggregationType::Sum],
        ));

        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1, "Should have exactly one group");
        assert_eq!(*results[0].get_value(0), Value::new(1));
        assert_eq!(*results[0].get_value(1), Value::new(60)); // 10 + 20 + 30
    }
}
