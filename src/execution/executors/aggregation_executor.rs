use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::aggregation_plan::{
    AggregateKey, AggregateValue, AggregationPlanNode, AggregationType,
};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use log::{debug, error};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct AggregationExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<AggregationPlanNode>,
    child_executor: Box<dyn AbstractExecutor>,
    initialized: bool,
    aggregates: HashMap<AggregateKey, AggregateValue>,
    current_group: Option<AggregateKey>,
}

impl AggregationExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<AggregationPlanNode>,
        child_executor: Box<dyn AbstractExecutor>,
    ) -> Self {
        Self {
            context,
            plan,
            child_executor,
            initialized: false,
            aggregates: HashMap::new(),
            current_group: None,
        }
    }

    /// Processes a single tuple and updates the aggregates
    fn process_tuple(&mut self, tuple: &Tuple) -> Result<(), String> {
        debug!("Processing tuple: {:?}", tuple.get_values());

        // Extract group by values
        let mut group_by_values = Vec::new();
        for (idx, group_by_expr) in self.plan.get_group_bys().iter().enumerate() {
            let value = group_by_expr
                .evaluate(tuple, self.plan.get_output_schema())
                .map_err(|e| format!("Failed to evaluate group by expression {}: {}", idx, e))?;
            debug!("Group by evaluation {} -> {:?}", idx, value);
            group_by_values.push(value);
        }

        let key = AggregateKey {
            group_bys: group_by_values,
        };
        debug!("Created aggregate key with values: {:?}", key.group_bys);

        // Extract aggregate values
        let mut agg_values = Vec::new();
        for (idx, (agg_expr, agg_type)) in self
            .plan
            .get_aggregates()
            .iter()
            .zip(self.plan.get_aggregate_types())
            .enumerate()
        {
            let value = match agg_type {
                AggregationType::CountStar => {
                    debug!("COUNT(*) aggregation");
                    Value::new(1i32)
                }
                _ => {
                    let val = agg_expr
                        .evaluate(tuple, self.plan.get_output_schema())
                        .map_err(|e| {
                            format!("Failed to evaluate aggregate expression {}: {}", idx, e)
                        })?;
                    debug!("Evaluated aggregate {} -> {:?}", idx, val);
                    val
                }
            };
            agg_values.push(value);
        }

        // Update or insert the aggregate values
        let entry = self.aggregates.entry(key.clone()).or_insert_with(|| {
            debug!("Creating new aggregate entry for key: {:?}", key.group_bys);
            // Initialize aggregates based on their type
            let init_aggregates = self
                .plan
                .get_aggregate_types()
                .iter()
                .enumerate()
                .map(|(idx, agg_type)| {
                    let initial = match agg_type {
                        AggregationType::Count | AggregationType::CountStar => {
                            debug!("Initializing count at index {}", idx);
                            Value::new(0i32)
                        }
                        AggregationType::Sum => {
                            debug!("Initializing sum at index {}", idx);
                            Value::new(0i32)
                        }
                        AggregationType::Min | AggregationType::Max => {
                            debug!(
                                "Initializing min/max at index {} with {:?}",
                                idx, agg_values[idx]
                            );
                            agg_values[idx].clone()
                        }
                    };
                    debug!("Initial value for aggregate {}: {:?}", idx, initial);
                    initial
                })
                .collect();

            AggregateValue {
                aggregates: init_aggregates,
            }
        });

        // Update each aggregate based on its type
        for ((value, curr_value), agg_type) in agg_values
            .iter()
            .zip(entry.aggregates.iter_mut())
            .zip(self.plan.get_aggregate_types())
        {
            debug!("Updating aggregate of type {:?}", agg_type);
            debug!("Current value: {:?}, New value: {:?}", curr_value, value);

            match agg_type {
                AggregationType::Count | AggregationType::CountStar => {
                    match curr_value.get_value() {
                        Val::Integer(count) => {
                            let new_count = count + 1;
                            debug!("Incrementing count from {} to {}", count, new_count);
                            *curr_value = Value::new(new_count);
                        }
                        _ => {
                            debug!("Initializing count to 1");
                            *curr_value = Value::new(1i32);
                        }
                    }
                }
                AggregationType::Sum => {
                    match (curr_value.get_value(), value.get_value()) {
                        (Val::Integer(a), Val::Integer(b)) => {
                            let sum = a + b;
                            debug!("Adding integers: {} + {} = {}", a, b, sum);
                            *curr_value = Value::new(sum);
                        }
                        // ... other sum cases ...
                        _ => debug!("Skipping incompatible sum types"),
                    }
                }
                AggregationType::Min => {
                    if curr_value.compare_greater_than(value) == CmpBool::CmpTrue {
                        debug!("Updating min from {:?} to {:?}", curr_value, value);
                        *curr_value = value.clone();
                    }
                }
                AggregationType::Max => {
                    if curr_value.compare_less_than(value) == CmpBool::CmpTrue {
                        debug!("Updating max from {:?} to {:?}", curr_value, value);
                        *curr_value = value.clone();
                    }
                }
            }
        }

        Ok(())
    }
}

impl AbstractExecutor for AggregationExecutor {
    fn init(&mut self) {
        if !self.initialized {
            debug!("Initializing aggregation executor");
            self.child_executor.init();

            // Process all input tuples
            while let Some((tuple, _)) = self.child_executor.next() {
                if let Err(e) = self.process_tuple(&tuple) {
                    error!("Error processing tuple: {}", e);
                    break;
                }
            }

            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Get the next group from the aggregates
        // First, find a key to process
        let next_key = self.aggregates.keys().next().cloned()?;

        // Then, remove the entry and get its value
        let value = self.aggregates.remove(&next_key)?;

        // Combine group by values and aggregate values
        let mut values = Vec::new();
        values.extend(next_key.group_bys);
        values.extend(value.aggregates);

        // Create tuple with the combined values
        let schema = self.plan.get_output_schema();
        let tuple = Tuple::new(&values, schema.clone(), RID::default());

        Some((tuple, RID::default()))
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
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
    use log::info;
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;

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
            Column::new("count", TypeId::Integer),
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
        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        // Verify results
        let (result_tuple, _) = executor.next().expect("Expected one result tuple");
        assert_eq!(*result_tuple.get_value(0), Value::new(3i32));
        assert!(executor.next().is_none());
    }

    #[test]
    #[ignore]
    fn test_group_by_sum() {
        info!("Starting group by sum test");
        let test_context = TestContext::new("agg_group_sum");

        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        debug!("Input schema: {:?}", input_schema);

        let output_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("sum", TypeId::Integer),
        ]);
        debug!("Output schema: {:?}", output_schema);

        // Setup catalog and data
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // Test data for mock executor
        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 4)),
        ];

        debug!("Creating mock executor with {} tuples", mock_tuples.len());
        for (values, rid) in &mock_tuples {
            debug!("Mock tuple - RID {:?}: {:?}", rid, values);
        }

        let child_executor = Box::new(MockExecutor::new(
            mock_tuples.clone(),
            input_schema.clone(),
            exec_ctx.clone(),
        ));

        // Create expressions
        debug!("Creating expressions");
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // index of group_id column
            0, // tuple index
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));
        debug!("Created group by expression: {:?}", group_expr);

        let sum_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // index of value column
            0, // tuple index
            Column::new("value", TypeId::Integer),
            vec![],
        )));
        debug!("Created sum expression: {:?}", sum_expr);

        // Test expression evaluation
        let test_tuple = Tuple::new(
            &[Value::new(1), Value::new(10)],
            input_schema.clone(),
            RID::default(),
        );
        debug!(
            "Testing expression evaluation with tuple: {:?}",
            test_tuple.get_values()
        );

        match group_expr.evaluate(&test_tuple, &input_schema) {
            Ok(val) => debug!("Group expression result: {:?}", val),
            Err(e) => debug!("Failed to evaluate group expression: {}", e),
        }

        match sum_expr.evaluate(&test_tuple, &input_schema) {
            Ok(val) => debug!("Sum expression result: {:?}", val),
            Err(e) => debug!("Failed to evaluate sum expression: {}", e),
        }

        // Create aggregation plan
        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema.clone(),
            vec![],
            vec![group_expr],
            vec![sum_expr],
            vec![AggregationType::Sum],
        ));
        debug!("Created aggregation plan:");
        debug!("  Output schema: {:?}", agg_plan.get_output_schema());
        debug!("  Group by expressions: {:?}", agg_plan.get_group_bys());
        debug!("  Aggregate expressions: {:?}", agg_plan.get_aggregates());
        debug!("  Aggregate types: {:?}", agg_plan.get_aggregate_types());

        // Execute aggregation
        info!("Executing aggregation");
        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Some((tuple, rid)) = executor.next() {
            debug!(
                "Got result tuple - RID: {:?}, group_id: {:?}, sum: {:?}",
                rid,
                tuple.get_value(0),
                tuple.get_value(1)
            );
            results.push(tuple);
        }

        debug!("Sorting {} results", results.len());
        results.sort_by(
            |a, b| match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            },
        );

        // Verify results
        info!("Verifying aggregation results");
        assert_eq!(results.len(), 2, "Expected exactly two groups");

        debug!("Final results:");
        for (i, result) in results.iter().enumerate() {
            debug!(
                "Group {}: group_id={:?}, sum={:?}",
                i,
                result.get_value(0),
                result.get_value(1)
            );
        }

        // Group 1 assertions
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

        // Group 2 assertions
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

        info!("Test completed successfully");
    }
}
