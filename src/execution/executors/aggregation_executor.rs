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

impl AggregationExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        agg_plan: Arc<AggregationPlanNode>,
        exec_ctx: Arc<RwLock<ExecutorContext>>,
    ) -> Self {
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

    // Make this function static to avoid borrow checker issues
    fn compute_aggregate_value(
        agg_type: AggregationType,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<Value, ExpressionError> {
        match agg_type {
            AggregationType::CountStar | AggregationType::Count => {
                match old_value {
                    Some(val) => match val.get_value() {
                        Val::BigInt(count) => Ok(Value::new(*count + 1)),
                        _ => Err(ExpressionError::InvalidOperation("Invalid count value".to_string())),
                    },
                    None => Ok(Value::new(1i64)), // Start counting from 1
                }
            },
            AggregationType::Sum => {
                match (old_value.map(|v| v.get_value()), new_value.get_value()) {
                    (Some(Val::Integer(sum)), Val::Integer(val)) => {
                        Ok(Value::new(sum + val))
                    },
                    (Some(Val::BigInt(sum)), Val::BigInt(val)) => {
                        Ok(Value::new(sum + val))
                    },
                    (Some(Val::Decimal(sum)), Val::Decimal(val)) => {
                        Ok(Value::new(sum + val))
                    },
                    (None, _) => Ok(new_value.clone()),
                    _ => Err(ExpressionError::InvalidOperation("Invalid sum value".to_string())),
                }
            },
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

    fn evaluate_group_by(&self, tuple: &Tuple, schema: &Schema) -> Result<AggregateKey, ExpressionError> {
        let mut group_values = Vec::new();
        for group_by in self.agg_plan.get_group_bys() {
            let value = group_by.evaluate(tuple, schema)?;
            group_values.push(value);
        }
        Ok(AggregateKey { group_bys: group_values })
    }

    fn evaluate_aggregates(
        &self,
        tuple: &Tuple,
        schema: &Schema,
    ) -> Result<Vec<Value>, ExpressionError> {
        let mut agg_values = Vec::new();
        for agg_expr in self.agg_plan.get_aggregates() {
            let value = agg_expr.evaluate(tuple, schema)?;
            agg_values.push(value);
        }
        Ok(agg_values)
    }
}

impl AbstractExecutor for AggregationExecutor {
    fn init(&mut self) {
        self.child_executor.init();
        self.group_values.clear();
        self.has_results = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        // If we've already returned results, we're done
        if self.has_results {
            return None;
        }

        // Process all input tuples
        while let Some((tuple, _)) = self.child_executor.next() {
            let schema = self.child_executor.get_output_schema();

            // Get group by key (or empty key for global aggregation)
            let group_key = match self.evaluate_group_by(&tuple, &schema) {
                Ok(key) => key,
                Err(_) => continue,
            };

            // Evaluate aggregate expressions
            let agg_values = match self.evaluate_aggregates(&tuple, &schema) {
                Ok(values) => values,
                Err(_) => continue,
            };

            // Get current aggregates for this group
            let current_aggregates = self.group_values
                .entry(group_key)
                .or_insert_with(|| {
                    AggregateValue {
                        aggregates: vec![Value::new(Val::BigInt(0)); agg_values.len()]
                    }
                });

            // Update each aggregate value
            let mut new_aggregates = current_aggregates.aggregates.clone();
            for (i, (agg_value, agg_type)) in agg_values.iter()
                .zip(self.agg_plan.get_aggregate_types())
                .enumerate() {
                if let Ok(new_value) = Self::compute_aggregate_value(
                    agg_type.clone(),
                    Some(&current_aggregates.aggregates[i]),
                    agg_value,
                ) {
                    new_aggregates[i] = new_value;
                }
            }
            current_aggregates.aggregates = new_aggregates;
        }

        // Return results
        self.has_results = true;

        // For global aggregation (no GROUP BY), return single result
        if self.agg_plan.get_group_bys().is_empty() {
            if let Some(values) = self.group_values.values().next() {
                let rid = RID::new(0, 0);
                return Some((
                    Tuple::new(&values.aggregates, self.agg_output_schema.clone(), rid),
                    rid,
                ));
            }
        }

        None
    }

    fn get_output_schema(&self) -> Schema {
        self.agg_output_schema.clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.exec_ctx.clone()
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
        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
        executor.init();

        // Verify results
        let (result_tuple, _) = executor.next().expect("Expected one result tuple");
        assert_eq!(*result_tuple.get_value(0), Value::new(3i32));
        assert!(executor.next().is_none());
    }

    #[test]
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
        let mut executor = AggregationExecutor::new(child_executor, agg_plan, exec_ctx);
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
