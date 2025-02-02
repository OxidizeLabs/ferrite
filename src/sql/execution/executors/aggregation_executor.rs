use crate::catalog::schema::Schema;
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

#[derive(Hash, Eq, PartialEq, Clone)]
#[derive(Debug)]
struct GroupKey {
    values: Vec<Value>,  // Values for each group by expression
}

#[derive(Clone)]
struct AggregateValues {
    values: Vec<Value>,  // One value per aggregate expression
}

impl GroupKey {
    fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl AggregateValues {
    fn new(num_aggregates: usize) -> Self {
        Self {
            values: vec![Value::new(Val::Null); num_aggregates],
        }
    }

    fn update(&mut self, index: usize, value: Value) -> Result<(), String> {
        if self.values[index].is_null() {
            self.values[index] = value;
        } else {
            self.values[index] = self.values[index].add(&value)?;
        }
        Ok(())
    }
}

pub struct AggregationExecutor {
    child: Box<dyn AbstractExecutor>,
    group_by_exprs: Vec<Arc<Expression>>,
    aggregate_exprs: Vec<Arc<Expression>>,
    groups: HashMap<GroupKey, AggregateValues>,
    output_schema: Schema,
    exec_ctx: Arc<RwLock<ExecutionContext>>,
    initialized: bool,
    groups_to_return: Vec<(GroupKey, AggregateValues)>,
}

impl AggregationExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<AggregationPlanNode>,
        child_executor: Box<dyn AbstractExecutor>,
    ) -> Self {
        Self {
            exec_ctx: context,
            child: child_executor,
            group_by_exprs: plan.get_group_bys().iter().cloned().collect(),
            aggregate_exprs: plan.get_aggregates().iter().cloned().collect(),
            groups: HashMap::new(),
            groups_to_return: Vec::new(),
            output_schema: plan.get_output_schema().clone(),
            initialized: false,
        }
    }

    fn compute_aggregate(
        agg_map: &mut HashMap<GroupKey, AggregateValues>,
        aggregates: &[Arc<Expression>],
        key: GroupKey,
        tuple: &Tuple,
        schema: &Schema,
    ) -> Result<(), String> {
        debug!("Computing aggregate for key: {:?}", key);
        debug!("Number of aggregates: {}", aggregates.len());

        let agg_value = agg_map.entry(key).or_insert_with(|| {
            AggregateValues {
                values: vec![Value::from(TypeId::Invalid); aggregates.len()],
            }
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
                        }
                        AggregationType::Count | AggregationType::CountStar => {
                            let count = if agg_value.values[i].is_null() {
                                Value::new(1i64)
                            } else {
                                agg_value.values[i].add(&Value::new(1i64))?
                            };
                            agg_value.values[i] = count;
                        }
                        AggregationType::Min => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                if agg_value.values[i].is_null() {
                                    agg_value.values[i] = arg_val;
                                } else {
                                    match arg_val.compare_less_than(&agg_value.values[i]) {
                                        CmpBool::CmpTrue => agg_value.values[i] = arg_val,
                                        _ => {}
                                    }
                                }
                            }
                        }
                        AggregationType::Max => {
                            let arg_val = agg.get_arg().evaluate(tuple, schema).unwrap();
                            if !arg_val.is_null() {
                                if agg_value.values[i].is_null() {
                                    agg_value.values[i] = arg_val;
                                } else {
                                    match agg_value.values[i].compare_less_than(&arg_val) {
                                        CmpBool::CmpTrue => agg_value.values[i] = arg_val,
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => return Err(format!("Unsupported aggregate type: {:?}", agg.get_agg_type())),
                    }
                }
                Expression::ColumnRef(_) => {
                    debug!("Column reference found in values");
                    // For group by columns, store the value only on first occurrence
                    if agg_value.values[i].is_null() {
                        agg_value.values[i] = agg_expr.evaluate(tuple, schema).unwrap();
                    }
                }
                _ => {
                    debug!("Other expression type found in aggregates");
                    // For other expressions, evaluate each time
                    agg_value.values[i] = agg_expr.evaluate(tuple, schema).unwrap();
                }
            }
            debug!("Updated aggregate values: {:?}", agg_value.values);
        }
        Ok(())
    }
}

impl AbstractExecutor for AggregationExecutor {
    fn init(&mut self) {
        if !self.initialized {
            self.child.init();
            self.groups.clear();

            // For empty input with no group by, create a single group with default values
            let mut has_rows = false;

            // Process all input tuples
            while let Some((tuple, _)) = self.child.next() {
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
                    &aggregates,
                    key,
                    &tuple,
                    schema,
                ) {
                    error!("Error computing aggregate: {}", e);
                }
            }

            // Handle empty input case
            if !has_rows && self.group_by_exprs.is_empty() {
                // Create empty group key for no group by
                let key = GroupKey { values: vec![] };

                // Create default aggregate values
                let mut agg_values = AggregateValues {
                    values: vec![Value::from(TypeId::Invalid); self.aggregate_exprs.len()],
                };

                // Set default values based on aggregate type
                for (i, agg_expr) in self.aggregate_exprs.iter().enumerate() {
                    if let Expression::Aggregate(agg) = agg_expr.as_ref() {
                        match agg.get_agg_type() {
                            AggregationType::Count | AggregationType::CountStar => {
                                agg_values.values[i] = Value::new(0i64);
                            }
                            AggregationType::Sum => {
                                agg_values.values[i] = Value::new(0); // or NULL depending on your requirements
                            }
                            AggregationType::Min | AggregationType::Max => {
                                agg_values.values[i] = Value::from(TypeId::Invalid); // NULL for min/max
                            }
                            _ => {}
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
                        }
                        CmpBool::CmpNull => continue,
                    }
                }
                std::cmp::Ordering::Equal
            });
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Take the next group from the Vec
        if let Some((key, value)) = self.groups_to_return.pop() {
            let mut values = Vec::new();
            values.extend(key.values.iter().cloned());
            values.extend(value.values.iter().cloned());

            debug!("Creating tuple with values: {:?}", values);
            debug!("Output schema: {:?}", self.output_schema);

            let tuple = Tuple::new(&values, self.output_schema.clone(), RID::new(0, 0));
            Some((tuple, RID::new(0, 0)))
        } else {
            None
        }
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.exec_ctx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::expressions::aggregate_expression::AggregateExpression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::value::Val::{BigInt, Integer};
    use chrono::Utc;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_context: Arc<TransactionContext>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        pub fn new(test_name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            // Add process ID to make filenames unique even if tests run in parallel
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let pid = std::process::id();
            let db_file = format!("tests/data/{}_{}_{}.db", test_name, timestamp, pid);
            let db_log_file = format!("tests/data/{}_{}_{}.log", test_name, timestamp, pid);

            // Create fresh instances for each test
            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                transaction_manager.clone(),
            )));

            // Create fresh transaction with unique ID
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
                db_file,
                db_log_file,
            }
        }

        pub fn cleanup(&self) {
            // Ensure all resources are dropped before removing files
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    // Helper function to create a fresh executor context for each test
    fn create_test_executor_context(
        test_context: &TestContext,
    ) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&test_context.bpm),
            Arc::clone(&test_context.catalog),
            Arc::clone(&test_context.transaction_context),
        )))
    }

    #[test]
    fn test_count_star() {
        let test_context = TestContext::new("test_count_star");
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_count_star".to_string(), vec![]);
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
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("count", TypeId::BigInt), // Use BigInt for count
                vec![],
            ))),
            vec![], // No child expressions for COUNT(*)
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![count_expr]));

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
        let test_context = TestContext::new("test_group_by_sum");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),  // Will be converted to BigInt
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_group_by_sum".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_scan_plan),
            0,
            mock_tuples,
            input_schema,
        ));

        // Create group by expression
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::Integer), vec![],
        )));

        // Create value expression for sum
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("value", TypeId::Integer), vec![],
        )));

        // Create SUM aggregate expression
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            value_expr.clone(),
            vec![value_expr],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],                // Group by group_id
            vec![sum_expr],      // Aggregate type
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            results.push(tuple);
        }

        // Sort results by group_id for consistent checking
        results.sort_by(|a, b| {
            match a.get_value(0).compare_less_than(b.get_value(0)) {
                CmpBool::CmpTrue => std::cmp::Ordering::Less,
                CmpBool::CmpFalse => std::cmp::Ordering::Greater,
                CmpBool::CmpNull => std::cmp::Ordering::Equal,
            }
        });

        assert_eq!(results.len(), 2, "Should have exactly 2 groups");

        // Check first group (group_id = 1)
        assert_eq!(*results[0].get_value(0), Value::new(Integer(1)));  // Group ID as Integer
        assert_eq!(*results[0].get_value(1), Value::new(Integer(30))); // Sum as BigInt

        // Check second group (group_id = 2)
        assert_eq!(*results[1].get_value(0), Value::new(Integer(2)));  // Group ID as Integer
        assert_eq!(*results[1].get_value(1), Value::new(Integer(70))); // Sum as BigInt
    }

    #[test]
    fn test_min_max_aggregation() {
        let test_context = TestContext::new("test_min_max_aggregation");
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_min_max_aggregation".to_string(), vec![]);
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

        // Create MIN aggregate
        let min_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Min,
            value_expr.clone(),
            vec![value_expr.clone()],
        )));

        // Create MAX aggregate
        let max_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Max,
            value_expr.clone(),
            vec![value_expr],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![min_expr, max_expr],
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
        assert_eq!(*results[0].get_value(1), Value::new(10));  // Min stays Integer
        assert_eq!(*results[0].get_value(2), Value::new(30));  // Max stays Integer

        // Check second group
        assert_eq!(*results[1].get_value(0), Value::new(2));
        assert_eq!(*results[1].get_value(1), Value::new(20));  // Min
        assert_eq!(*results[1].get_value(2), Value::new(50));  // Max
    }

    #[test]
    fn test_min_max_without_groupby() {
        let test_context = TestContext::new("test_min_max_without_groupby");
        let input_schema = Schema::new(vec![
            Column::new("value", TypeId::Integer),
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_tuples = vec![
            (vec![Value::new(30)], RID::new(1, 1)),
            (vec![Value::new(10)], RID::new(1, 2)),
            (vec![Value::new(50)], RID::new(1, 3)),
            (vec![Value::new(20)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_min_max_without_groupby".to_string(), vec![]);
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

        // Create MIN aggregate
        let min_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Min,
            value_expr.clone(),
            vec![value_expr.clone()],
        )));

        // Create MAX aggregate
        let max_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Max,
            value_expr.clone(),
            vec![value_expr],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![min_expr, max_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let result = executor.next();
        assert!(result.is_some());

        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(Integer(10)));  // Min
        assert_eq!(*tuple.get_value(1), Value::new(Integer(50)));  // Max

        // No more results
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_multiple_aggregates() {
        let test_context = TestContext::new("test_multiple_aggregates");
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_multiple_aggregates".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan),
            0,
            mock_tuples,
            input_schema.clone(),
        ));

        // Create group by expression
        let group_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0,
            Column::new("group_id", TypeId::Integer),
            vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0, 1,
                Column::new("value", TypeId::Integer),
                vec![],
            ))),
            vec![],
        )));

        // Create COUNT aggregate
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0, 1,
                Column::new("value", TypeId::Integer),
                vec![],
            ))),
            vec![],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![PlanNode::MockScan(MockScanNode::new(input_schema, "test_multiple_aggregates".to_string(), vec![]))],
            vec![group_by_expr],
            vec![sum_expr, count_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
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
        assert_eq!(results[0].get_value(2).as_bigint().unwrap(), 2);   // count = 2

        // Check second group (group_id = 2)
        assert_eq!(results[1].get_value(0).as_integer().unwrap(), 2);
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 80); // sum = 10 + 30 + 40
        assert_eq!(results[1].get_value(2).as_bigint().unwrap(), 3);   // count = 3
    }

    #[test]
    fn test_empty_input() {
        let test_context = TestContext::new("test_empty_input");
        let input_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);

        let exec_ctx = create_test_executor_context(&test_context);

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_empty_input".to_string(), vec![]);
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
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("count", TypeId::BigInt), // Use BigInt for count
                vec![],
            ))),
            vec![], // No child expressions for COUNT(*)
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![count_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let result = executor.next();
        assert!(result.is_some(), "Should return a result even with empty input");

        let (tuple, _) = result.unwrap();
        assert_eq!(*tuple.get_value(0), Value::new(BigInt(0)), "Count should be 0 for empty input");

        // No more results
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_single_group() {
        let test_context = TestContext::new("test_single_group");
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

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "test_single_group".to_string(), vec![]);
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

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            value_expr.clone(),
            vec![value_expr],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![sum_expr],
        ));

        let mut executor = AggregationExecutor::new(exec_ctx, agg_plan, child_executor);
        executor.init();

        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1, "Should have exactly one group");
        assert_eq!(*results[0].get_value(0), Value::new(Integer(1)));
        assert_eq!(*results[0].get_value(1), Value::new(Integer(60))); // 10 + 20 + 30
    }

    #[test]
    fn test_aggregation_with_types() {
        let test_context = TestContext::new("test_aggregation_with_types");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::VarChar),  // String group
            Column::new("int_val", TypeId::Integer),   // Integer values
            Column::new("big_val", TypeId::BigInt),    // BigInt values
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        // Create test data with different numeric types
        let mock_tuples = vec![
            // Group A
            (vec![
                Value::new("A"),           // group_id
                Value::new(Integer(10)),            // int_val
                Value::new(BigInt(100)),   // big_val - already BigInt
            ], RID::new(0, 0)),
            (vec![
                Value::new("A"),
                Value::new(Integer(20)),
                Value::new(BigInt(200)),
            ], RID::new(0, 1)),
            // Group B
            (vec![
                Value::new("B"),
                Value::new(Integer(30)),
                Value::new(BigInt(300)),
            ], RID::new(0, 2)),
            (vec![
                Value::new("B"),
                Value::new(Integer(40)),
                Value::new(BigInt(400)),
            ], RID::new(0, 3)),
            // Group C - Single row group
            (vec![
                Value::new("C"),
                Value::new(Integer(50)),
                Value::new(BigInt(500)),
            ], RID::new(0, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_aggregation_with_types".to_string(),
            vec![],
        ).with_tuples(mock_tuples.clone());

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::VarChar), vec![],
        )));

        let int_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("int_val", TypeId::Integer), vec![],
        )));

        let big_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 2, Column::new("big_val", TypeId::BigInt), vec![],
        )));

        // Create aggregates
        let sum_int = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            int_col.clone(),
            vec![int_col.clone()],
        )));

        let sum_big = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            big_col.clone(),
            vec![big_col.clone()],
        )));  // Explicitly set BigInt type

        let count_star = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            int_col.clone(),
            vec![],
        )));

        // Create aggregation plan
        let agg_plan = AggregationPlanNode::new(
            vec![PlanNode::MockScan(mock_scan_plan.clone())],
            vec![group_expr],
            vec![sum_int, sum_big, count_star],
        );

        // Create and execute the aggregation
        let mut executor = AggregationExecutor::new(
            exec_ctx.clone(),
            Arc::new(agg_plan),
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
        while let Some((tuple, _)) = executor.next() {
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
        assert_eq!(results[0].get_value(1).as_integer().unwrap(), 30);  // sum of int_val (10 + 20)
        assert_eq!(results[0].get_value(2).as_bigint().unwrap(), 300);  // sum of big_val (100 + 200)
        assert_eq!(results[0].get_value(3).as_bigint().unwrap(), 2);    // count = 2

        // Check Group B
        assert_eq!(ToString::to_string(&results[1].get_value(0)), "B");
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 70);  // sum of int_val (30 + 40)
        assert_eq!(results[1].get_value(2).as_bigint().unwrap(), 700);  // sum of big_val (300 + 400)
        assert_eq!(results[1].get_value(3).as_bigint().unwrap(), 2);    // count = 2

        // Check Group C
        assert_eq!(ToString::to_string(&results[2].get_value(0)), "C");
        assert_eq!(results[2].get_value(1).as_integer().unwrap(), 50);  // int_val = 50
        assert_eq!(results[2].get_value(2).as_bigint().unwrap(), 500);  // big_val = 500
        assert_eq!(results[2].get_value(3).as_bigint().unwrap(), 1);    // count = 1
    }

    #[test]
    fn test_aggregation_column_names() {
        let test_context = TestContext::new("test_aggregation_column_names");
        let input_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),  // Name column
            Column::new("age", TypeId::Integer),   // Age column
        ]);

        let exec_ctx = create_test_executor_context(&test_context);

        // Create test data
        let mock_tuples = vec![
            (vec![
                Value::new("John Doe"),
                Value::new(35),
            ], RID::new(0, 0)),
            (vec![
                Value::new("John Doe"),
                Value::new(35),
            ], RID::new(0, 1)),
            (vec![
                Value::new("Jane Smith"),
                Value::new(64),
            ], RID::new(0, 2)),
            (vec![
                Value::new("Jane Smith"),
                Value::new(64),
            ], RID::new(0, 3)),
        ];

        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "test_aggregation_column_names".to_string(),
            vec![],
        ).with_tuples(mock_tuples.clone());

        // Create expressions
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("name", TypeId::VarChar), vec![],
        )));

        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("age", TypeId::Integer), vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            age_col.clone(),
            vec![age_col.clone()],
        )));

        // Create aggregation plan
        let agg_plan = AggregationPlanNode::new(
            vec![PlanNode::MockScan(mock_scan_plan.clone())],
            vec![group_expr],
            vec![sum_expr],
        );

        // Create and execute the aggregation
        let mut executor = AggregationExecutor::new(
            exec_ctx.clone(),
            Arc::new(agg_plan),
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
        while let Some((tuple, _)) = executor.next() {
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
        assert_eq!(output_schema.get_columns()[0].get_name(), "name",
                   "First column should be named 'name'");
        assert_eq!(output_schema.get_columns()[1].get_name(), "SUM(age)",
                   "Second column should be named 'SUM(age)'");

        // Check first group (Jane Smith)
        assert_eq!(ToString::to_string(&results[0].get_value(0)), "Jane Smith");
        assert_eq!(results[0].get_value(1).as_integer().unwrap(), 128); // 64 + 64

        // Check second group (John Doe)
        assert_eq!(ToString::to_string(&results[1].get_value(0)), "John Doe");
        assert_eq!(results[1].get_value(1).as_integer().unwrap(), 70);  // 35 + 35
    }
}
