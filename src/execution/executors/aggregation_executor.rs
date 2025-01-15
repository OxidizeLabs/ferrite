use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::types::CmpBool;
use crate::types_db::value::Val::BigInt;
use crate::types_db::value::{Val, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct AggregationExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<AggregationPlanNode>,
    child_executor: Box<dyn AbstractExecutor>,
    // Track aggregation state
    groups: HashMap<Vec<Value>, Vec<Value>>,
    initialized: bool,
    done: bool,
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
            groups: HashMap::new(),
            initialized: false,
            done: false,
        }
    }

    fn compute_group_key(&self, tuple: &Tuple) -> Result<Vec<Value>, String> {
        // Use group_by expressions from plan
        let mut key = Vec::new();
        for expr in self.plan.get_group_bys() {
            let value = expr.evaluate(tuple, &tuple.get_schema())
                .map_err(|e| format!("Error evaluating group by expression: {}", e))?;
            key.push(value);
        }
        Ok(key)
    }

    fn update_group(&mut self, key: Vec<Value>, tuple: &Tuple) -> Result<(), String> {
        let group = self.groups.entry(key).or_insert_with(|| {
            // Initialize aggregates based on types from plan
            self.plan.get_aggregate_types().iter().map(|agg_type| {
                match agg_type {
                    AggregationType::Count | AggregationType::CountStar => Value::new(BigInt(0)),
                    AggregationType::Sum => Value::new(0),
                    AggregationType::Min => Value::new(i32::MAX),
                    AggregationType::Max => Value::new(i32::MIN),
                    AggregationType::Avg => Value::new(0),
                }
            }).collect()
        });

        // Update each aggregate based on its type and expression
        for (i, (agg_type, agg_expr)) in self.plan.get_aggregate_types().iter()
            .zip(self.plan.get_aggregates().iter())
            .enumerate()
        {
            match agg_type {
                AggregationType::Sum => {
                    // Get the value to aggregate by evaluating the inner expression
                    let value = if let Expression::Aggregate(agg) = agg_expr.as_ref() {
                        // For nested aggregates, evaluate the child expression
                        if let Some(child_expr) = agg.get_children().first() {
                            child_expr.evaluate(tuple, &tuple.get_schema())
                        } else {
                            agg_expr.evaluate(tuple, &tuple.get_schema())
                        }
                    } else {
                        agg_expr.evaluate(tuple, &tuple.get_schema())
                    }.map_err(|e| format!("Error evaluating sum: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        let current_sum = match group[i].get_value() {
                            Val::Integer(n) => *n,
                            _ => 0,
                        };

                        match value.get_value() {
                            Val::Integer(n) => {
                                group[i] = Value::new(current_sum + n);
                            }
                            _ => return Err("Unsupported type for sum".to_string()),
                        }
                    }
                }
                AggregationType::Count | AggregationType::CountStar => {
                    let current_count = match group[i].get_value() {
                        Val::BigInt(n) => *n,
                        _ => 0,
                    };
                    group[i] = Value::new(BigInt(current_count + 1));
                }
                AggregationType::Min => {
                    let value = agg_expr.evaluate(tuple, &tuple.get_schema())
                        .map_err(|e| format!("Error evaluating min: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        if value.compare_less_than(&group[i]) == CmpBool::CmpTrue {
                            group[i] = value;
                        }
                    }
                }
                AggregationType::Max => {
                    let value = agg_expr.evaluate(tuple, &tuple.get_schema())
                        .map_err(|e| format!("Error evaluating max: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        if value.compare_greater_than(&group[i]) == CmpBool::CmpTrue {
                            group[i] = value;
                        }
                    }
                }
                AggregationType::Avg => {
                    // For simplicity, we'll implement Avg as Sum for now
                    let value = agg_expr.evaluate(tuple, &tuple.get_schema())
                        .map_err(|e| format!("Error evaluating avg: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        let current_sum = match group[i].get_value() {
                            Val::Integer(n) => *n,
                            _ => 0,
                        };

                        match value.get_value() {
                            Val::Integer(n) => {
                                group[i] = Value::new(current_sum + n);
                            }
                            _ => return Err("Unsupported type for avg".to_string()),
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl AbstractExecutor for AggregationExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        self.child_executor.init();
        self.initialized = true;
        self.done = false;  // Reset done flag on initialization
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        if self.done {
            return None;
        }

        // Process all input tuples first time through
        if self.groups.is_empty() {
            let mut has_input = false;
            while let Some((tuple, _)) = self.child_executor.next() {
                has_input = true;
                let key = match self.compute_group_key(&tuple) {
                    Ok(k) => k,
                    Err(_) => continue,
                };
                if let Err(_) = self.update_group(key, &tuple) {
                    continue;
                }
            }

            // For empty input with no grouping, return single row with default values
            if !has_input && self.plan.get_group_bys().is_empty() {
                let empty_key = Vec::new();
                self.groups.entry(empty_key).or_insert_with(|| {
                    self.plan.get_aggregate_types().iter().map(|agg_type| {
                        match agg_type {
                            AggregationType::Count | AggregationType::CountStar => Value::new(BigInt(0)),
                            AggregationType::Sum => match self.plan.get_aggregates()[0].get_return_type().get_type() {
                                TypeId::Integer => Value::new(0),
                                TypeId::BigInt => Value::new(BigInt(0)),
                                TypeId::Decimal => Value::new(0.0f64),
                                _ => Value::new(0),
                            },
                            AggregationType::Min => match self.plan.get_aggregates()[0].get_return_type().get_type() {
                                TypeId::Integer => Value::new(i32::MAX),
                                TypeId::BigInt => Value::new(BigInt(i64::MAX)),
                                TypeId::Decimal => Value::new(f64::MAX),
                                _ => Value::new(i32::MAX),
                            },
                            AggregationType::Max => match self.plan.get_aggregates()[0].get_return_type().get_type() {
                                TypeId::Integer => Value::new(i32::MIN),
                                TypeId::BigInt => Value::new(BigInt(i64::MIN)),
                                TypeId::Decimal => Value::new(f64::MIN),
                                _ => Value::new(i32::MIN),
                            },
                            AggregationType::Avg => Value::new(0.0f64),
                        }
                    }).collect()
                });
            }

            // If no groups were created, we're done
            if self.groups.is_empty() {
                self.done = true;
                return None;
            }
        }

        // Take ownership of the groups HashMap
        let mut remaining_groups = std::mem::take(&mut self.groups);

        // Get the first entry
        if let Some((key, values)) = remaining_groups.iter().next().map(|(k, v)| (k.clone(), v.clone())) {
            // Remove the entry we just processed
            remaining_groups.remove(&key);

            // Put the remaining groups back
            self.groups = remaining_groups;

            // Mark as done if no more groups
            if self.groups.is_empty() {
                self.done = true;
            }

            let mut combined = key;
            combined.extend(values);

            Some((
                Tuple::new(&combined, self.plan.get_output_schema().clone(), RID::new(0, 0)),
                RID::new(0, 0),
            ))
        } else {
            self.done = true;
            None
        }
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
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;

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

    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use crate::types_db::value::Val::BigInt;
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
            // initialize_logger();
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

    #[test]
    fn test_count_star() {
        let test_context = TestContext::new("count_star");
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let output_schema = Schema::new(vec![
            Column::new("count", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(2), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(3), Value::new(30)], RID::new(1, 3)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create a COUNT(*) expression
        let count_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("count", TypeId::Integer),
            vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            output_schema,
            vec![],
            vec![], // No group by
            vec![count_expr],
            vec![AggregationType::CountStar],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        // Should get one result with count = 3
        let result = executor.next();
        assert!(result.is_some());
        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(BigInt(3)));

        // No more results
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
        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
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

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
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

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
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

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
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
        assert_eq!(*results[0].get_value(2), Value::new(BigInt(2)));

        // Group 2: sum=70, count=3
        assert_eq!(*results[1].get_value(0), Value::new(2));
        assert_eq!(*results[1].get_value(1), Value::new(80));
        assert_eq!(*results[1].get_value(2), Value::new(BigInt(3)));
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

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let result = executor.next();
        assert!(result.is_some(), "Should return a result even with empty input");

        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(BigInt(0)), "Count should be 0 for empty input");
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

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
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
