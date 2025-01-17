use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::execution_context::ExecutionContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::Type;
use crate::types_db::types::CmpBool;
use crate::types_db::value::Val::BigInt;
use crate::types_db::value::{Val, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use crate::types_db::type_id::TypeId;

pub struct AggregationExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<AggregationPlanNode>,
    child_executor: Box<dyn AbstractExecutor>,
    // Track aggregation state
    groups: HashMap<Vec<Value>, Vec<Value>>,
    initialized: bool,
    done: bool,
}

impl AggregationExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
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
            // Don't convert group by values - keep them in their original type
            key.push(value);
        }
        Ok(key)
    }

    fn update_group(&mut self, key: Vec<Value>, tuple: &Tuple) -> Result<(), String> {
        let group = self.groups.entry(key).or_insert_with(|| {
            // Initialize aggregates based on types from plan
            self.plan.get_aggregate_types().iter().zip(self.plan.get_aggregates().iter()).map(|(agg_type, expr)| {
                match agg_type {
                    AggregationType::Count | AggregationType::CountStar => {
                        Value::new(BigInt(0))  // COUNT always returns BigInt
                    }
                    AggregationType::Sum => {
                        // Initialize sum with same type as input
                        if let Expression::Aggregate(agg) = expr.as_ref() {
                            if let Some(child_expr) = agg.get_children().first() {
                                match child_expr.get_return_type().get_type() {
                                    TypeId::Integer => Value::new(0),
                                    TypeId::BigInt => Value::new(BigInt(0)),
                                    _ => Value::new(0), // Default to Integer
                                }
                            } else {
                                Value::new(0)
                            }
                        } else {
                            Value::new(0)
                        }
                    }
                    AggregationType::Min | AggregationType::Max => {
                        // Use input type for min/max
                        if let Expression::Aggregate(agg) = expr.as_ref() {
                            if let Some(child_expr) = agg.get_children().first() {
                                match child_expr.get_return_type().get_type() {
                                    TypeId::Integer => Value::new(if *agg_type == AggregationType::Min { i32::MAX } else { i32::MIN }),
                                    TypeId::BigInt => Value::new(BigInt(if *agg_type == AggregationType::Min { i64::MAX } else { i64::MIN })),
                                    _ => Value::new(if *agg_type == AggregationType::Min { i32::MAX } else { i32::MIN }),
                                }
                            } else {
                                Value::new(if *agg_type == AggregationType::Min { i32::MAX } else { i32::MIN })
                            }
                        } else {
                            Value::new(if *agg_type == AggregationType::Min { i32::MAX } else { i32::MIN })
                        }
                    }
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
                    let value = match agg_expr.as_ref() {
                        Expression::Aggregate(agg) => {
                            if let Some(child_expr) = agg.get_children().first() {
                                child_expr.evaluate(tuple, &tuple.get_schema())
                            } else {
                                continue;
                            }
                        }
                        _ => agg_expr.evaluate(tuple, &tuple.get_schema())
                    }.map_err(|e| format!("Error evaluating sum: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        // Keep the same type as input
                        match (group[i].get_value(), value.get_value()) {
                            (Val::Integer(curr), Val::Integer(n)) => {
                                group[i] = Value::new(curr + n);
                            }
                            (Val::BigInt(curr), Val::BigInt(n)) => {
                                group[i] = Value::new(BigInt(curr + n));
                            }
                            _ => return Err("Type mismatch in sum".to_string()),
                        }
                    }
                }
                AggregationType::Count | AggregationType::CountStar => {
                    let current_count = match group[i].get_value() {
                        Val::BigInt(n) => *n,
                        _ => 0i64,
                    };
                    group[i] = Value::new(BigInt(current_count + 1));
                }
                AggregationType::Min | AggregationType::Max => {
                    let value = if let Expression::Aggregate(agg) = agg_expr.as_ref() {
                        if let Some(child_expr) = agg.get_children().first() {
                            child_expr.evaluate(tuple, &tuple.get_schema())
                        } else {
                            continue;
                        }
                    } else {
                        agg_expr.evaluate(tuple, &tuple.get_schema())
                    }.map_err(|e| format!("Error evaluating min/max: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        let should_update = match agg_type {
                            AggregationType::Min => value.compare_less_than(&group[i]) == CmpBool::CmpTrue,
                            AggregationType::Max => value.compare_greater_than(&group[i]) == CmpBool::CmpTrue,
                            _ => false,
                        };
                        if should_update {
                            group[i] = value;
                        }
                    }
                }
                AggregationType::Avg => {
                    let value = agg_expr.evaluate(tuple, &tuple.get_schema())
                        .map_err(|e| format!("Error evaluating avg: {}", e))?;

                    if !matches!(value.get_value(), Val::Null) {
                        let current_sum = match group[i].get_value() {
                            Val::Integer(n) => *n,
                            _ => 0,
                        };

                        match value.get_value() {
                            Val::Integer(n) => {
                                group[i] = Value::new(BigInt((current_sum + n) as i64));
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
        if self.done {
            return None;
        }

        if !self.initialized {
            self.init();
        }

        // Process all input tuples if we haven't already
        let mut has_input = false;
        if !self.done {
            while let Some((tuple, _)) = self.child_executor.next() {
                has_input = true;
                let key = self.compute_group_key(&tuple)
                    .map_err(|e| format!("Error computing group key: {}", e))
                    .unwrap();
                self.update_group(key, &tuple)
                    .map_err(|e| format!("Error updating group: {}", e))
                    .unwrap();
            }
        }

        // Handle empty input case
        if !has_input && self.groups.is_empty() && self.plan.get_group_bys().is_empty() {
            // For empty input with no grouping, return single row with default values
            let schema = self.plan.get_output_schema();
            let mut values = Vec::with_capacity(schema.get_column_count() as usize);
            
            for (i, agg_type) in self.plan.get_aggregate_types().iter().enumerate() {
                match agg_type {
                    AggregationType::Count | AggregationType::CountStar => {
                        values.push(Value::new(BigInt(0))); // COUNT returns 0 for empty input
                    }
                    _ => {
                        values.push(Value::new(schema.get_column(i)?.get_type())); // Other aggregates return NULL
                    }
                }
            }
            
            self.done = true;
            return Some((Tuple::new(&values, schema.clone(), RID::new(0, 0)), RID::new(0, 0)));
        }

        // Return next group
        if let Some((key, values)) = self.groups.iter().next().map(|(k, v)| (k.clone(), v.clone())) {
            self.groups.remove(&key);
            
            if self.groups.is_empty() {
                self.done = true;
            }

            let schema = self.plan.get_output_schema();
            let mut tuple_values = Vec::with_capacity(schema.get_column_count() as usize);

            // Add group by values with correct types from schema
            for (value, col) in key.iter().zip(schema.get_columns().iter()) {
                match value.get_value() {
                    Val::VarLen(s) => {
                        if col.get_type() == TypeId::VarChar {
                            tuple_values.push(Value::new(s.as_str()));
                        }
                    }
                    _ => tuple_values.push(value.clone()),
                }
            }

            // Add aggregate values with correct types from schema
            for (value, col) in values.iter().zip(schema.get_columns().iter().skip(key.len())) {
                match (value.get_value(), col.get_type()) {
                    (Val::Integer(n), TypeId::Integer) => tuple_values.push(Value::new(*n)),
                    (Val::Integer(n), TypeId::BigInt) => tuple_values.push(Value::new(BigInt(*n as i64))),
                    (Val::BigInt(n), TypeId::BigInt) => tuple_values.push(Value::new(BigInt(*n))),
                    _ => tuple_values.push(value.clone()),
                }
            }

            Some((Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0)), RID::new(0, 0)))
        } else {
            self.done = true;
            None
        }
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
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
    use crate::types_db::value::Val::{BigInt, Integer};
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use crate::execution::expressions::aggregate_expression::AggregateExpression;
    use crate::execution::plans::abstract_plan::PlanNode;
    use crate::execution::transaction_context::TransactionContext;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
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

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_context,
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
    ) -> Arc<RwLock<ExecutionContext>> {
        let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
        Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&test_context.bpm),
            catalog,
            Arc::clone(&test_context.transaction_context),
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
            vec![count_expr]        ));

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
        let test_context = TestContext::new("group_by_sum");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),  // Will be converted to BigInt
            Column::new("value", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mock_tuples = vec![
            (vec![Value::new(1), Value::new(10)], RID::new(1, 1)),
            (vec![Value::new(1), Value::new(20)], RID::new(1, 2)),
            (vec![Value::new(2), Value::new(30)], RID::new(1, 3)),
            (vec![Value::new(2), Value::new(40)], RID::new(1, 4)),
        ];

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
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
        let test_context = TestContext::new("agg_min_max");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

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
        let test_context = TestContext::new("min_max_no_group");
        let input_schema = Schema::new(vec![
            Column::new("value", TypeId::Integer),
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
        let test_context = TestContext::new("multiple_aggs");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
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

        // Create group by expression
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("group_id", TypeId::Integer), vec![],
        )));

        // Create value expression for aggregates
        let value_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("value", TypeId::Integer), vec![],
        )));

        // Create SUM aggregate
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            value_expr.clone(),
            vec![value_expr.clone()],
        )));

        // Create COUNT aggregate
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            value_expr.clone(),
            vec![value_expr],
        )));

        let agg_plan = Arc::new(AggregationPlanNode::new(
            vec![],
            vec![group_expr],                        // Group by group_id
            vec![sum_expr, count_expr],
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
        assert_eq!(*results[0].get_value(0), Value::new(Integer(1)));
        assert_eq!(*results[0].get_value(1), Value::new(Integer(30)));
        assert_eq!(*results[0].get_value(2), Value::new(BigInt(2)));

        // Group 2: sum=80, count=3
        assert_eq!(*results[1].get_value(0), Value::new(Integer(2)));
        assert_eq!(*results[1].get_value(1), Value::new(Integer(80)));
        assert_eq!(*results[1].get_value(2), Value::new(BigInt(3)));
    }

    #[test]
    fn test_empty_input() {
        let test_context = TestContext::new("empty_input");
        let input_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);

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
        let test_context = TestContext::new("single_group");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

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
        let test_context = TestContext::new("agg_types");
        let input_schema = Schema::new(vec![
            Column::new("group_id", TypeId::VarChar),  // String group
            Column::new("int_val", TypeId::Integer),   // Integer values
            Column::new("big_val", TypeId::BigInt),    // BigInt values
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

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
            "mock_table".to_string(),
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

        // Collect and sort results
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

        assert_eq!(results.len(), 3, "Should have exactly 3 groups");

        // Check Group A
        assert_eq!(results[0].get_value(0).get_value(), &Val::VarLen("A".to_string()));
        assert_eq!(results[0].get_value(1).get_value(), &Val::Integer(30));     // Integer sum
        assert_eq!(results[0].get_value(2).get_value(), &Val::BigInt(300));    // BigInt sum
        assert_eq!(results[0].get_value(3).get_value(), &Val::BigInt(2));      // count always BigInt

        // Check Group B
        assert_eq!(results[1].get_value(0).get_value(), &Val::VarLen("B".to_string()));
        assert_eq!(results[1].get_value(1).get_value(), &Val::Integer(70));     // Integer sum
        assert_eq!(results[1].get_value(2).get_value(), &Val::BigInt(700));    // BigInt sum
        assert_eq!(results[1].get_value(3).get_value(), &Val::BigInt(2));      // count always BigInt

        // Check Group C (single row)
        assert_eq!(results[2].get_value(0).get_value(), &Val::VarLen("C".to_string()));
        assert_eq!(results[2].get_value(1).get_value(), &Val::Integer(50));     // just 50
        assert_eq!(results[2].get_value(2).get_value(), &Val::BigInt(500));    // just 500
        assert_eq!(results[2].get_value(3).get_value(), &Val::BigInt(1));       // count(*) = 1
    }
}
