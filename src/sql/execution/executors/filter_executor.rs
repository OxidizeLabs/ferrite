use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::expressions::filter_expression::FilterType;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::filter_plan::FilterNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct FilterExecutor {
    child_executor: Box<dyn AbstractExecutor>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<FilterNode>,
    initialized: bool,
    group_tuples: Vec<Arc<Tuple>>,
    current_group_idx: usize,
}

impl FilterExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<FilterNode>,
    ) -> Self {
        Self {
            child_executor,
            context,
            plan,
            initialized: false,
            group_tuples: Vec::new(),
            current_group_idx: 0,
        }
    }

    fn apply_filter(&self, tuple: &Tuple) -> bool {
        let filter_expr = self.plan.get_filter_expression();
        let schema = self.child_executor.get_output_schema();

        match filter_expr.get_filter_type() {
            FilterType::Where => self.apply_where_filter(tuple, schema),
            FilterType::Having => self.apply_having_filter(schema),
        }
    }

    fn apply_where_filter(&self, tuple: &Tuple, schema: &Schema) -> bool {
        let filter_expr = self.plan.get_filter_expression();
        debug!(
            "Evaluating WHERE filter on tuple with values: {:?}",
            tuple.get_values()
        );

        match filter_expr.evaluate(tuple, schema) {
            Ok(value) => match value.get_val() {
                Val::Boolean(b) => {
                    debug!(
                        "WHERE filter evaluation result: {}, for tuple: {:?}",
                        b,
                        tuple.get_values()
                    );
                    *b
                }
                _ => {
                    error!(
                        "WHERE filter evaluation returned non-boolean value: {:?}",
                        value
                    );
                    false
                }
            },
            Err(e) => {
                error!("Failed to evaluate WHERE filter: {}", e);
                false
            }
        }
    }

    /// Applies a HAVING filter to a group of tuples.
    fn apply_having_filter(&self, schema: &Schema) -> bool {
        debug!(
            "Applying HAVING filter. Group tuples count: {}",
            self.group_tuples.len()
        );

        if self.group_tuples.is_empty() {
            return false;
        }

        let filter_expr = self.plan.get_filter_expression();

        // Get aggregate expression safely with clone() to avoid ownership issues
        let agg_expr = match filter_expr.get_aggregate().as_ref() {
            Some(expr) => expr.clone(),
            None => {
                error!("No aggregate expression found in HAVING filter");
                return false;
            }
        };

        debug!(
            "Processing aggregate expression for HAVING filter: {:?}",
            agg_expr
        );

        debug!(
            "First tuple values: {:?}",
            self.group_tuples[0].get_values()
        );

        // For HAVING clauses, we need to evaluate the aggregate expression across all tuples
        // and then apply the predicate to determine if any tuples should be returned.
        // If the aggregate condition is met, ALL tuples should be returned.

        // First, compute the aggregate value from all group tuples
        let agg_value = match &*agg_expr {
            Expression::Aggregate(agg) => {
                match agg.get_agg_type() {
                    AggregationType::CountStar => {
                        let count = self.group_tuples.len() as i64;
                        Value::new_with_type(Val::BigInt(count), TypeId::BigInt)
                    }
                    AggregationType::Count => {
                        let count = self.group_tuples.len() as i64;
                        Value::new_with_type(Val::BigInt(count), TypeId::BigInt)
                    }
                    AggregationType::Sum => {
                        let col_expr = agg.get_children()[0].clone();
                        let mut sum_val: Option<Value> = None;

                        for t in &self.group_tuples {
                            if let Ok(val) = col_expr.evaluate(t, schema) {
                                if let Some(ref mut sum) = sum_val {
                                    *sum = sum.add(&val).unwrap_or_else(|_| sum.clone());
                                } else {
                                    sum_val = Some(val);
                                }
                            }
                        }

                        sum_val.unwrap_or_else(|| Value::new(Val::Null))
                    }
                    AggregationType::Avg => {
                        let col_expr = agg.get_children()[0].clone();
                        let mut sum_val: Option<Value> = None;
                        let mut count = 0;

                        for t in &self.group_tuples {
                            if let Ok(val) = col_expr.evaluate(t, schema) {
                                if !val.is_null() {
                                    if let Some(ref mut sum) = sum_val {
                                        *sum = sum.add(&val).unwrap_or_else(|_| sum.clone());
                                    } else {
                                        sum_val = Some(val);
                                    }
                                    count += 1;
                                }
                            }
                        }

                        if let Some(sum) = sum_val {
                            if count > 0 {
                                match sum.get_type_id() {
                                    TypeId::Integer => {
                                        let int_val = sum.as_integer().unwrap();
                                        Value::new_with_type(
                                            Val::Decimal(int_val as f64 / count as f64),
                                            TypeId::Decimal,
                                        )
                                    }
                                    TypeId::BigInt => {
                                        let bigint_val = sum.as_bigint().unwrap();
                                        Value::new_with_type(
                                            Val::Decimal(bigint_val as f64 / count as f64),
                                            TypeId::Decimal,
                                        )
                                    }
                                    TypeId::Decimal => {
                                        let decimal_val = sum.as_decimal().unwrap();
                                        Value::new_with_type(
                                            Val::Decimal(decimal_val / count as f64),
                                            TypeId::Decimal,
                                        )
                                    }
                                    _ => Value::new(Val::Null),
                                }
                            } else {
                                Value::new(Val::Null)
                            }
                        } else {
                            Value::new(Val::Null)
                        }
                    }
                    AggregationType::Min => {
                        let col_expr = agg.get_children()[0].clone();
                        let mut min_val: Option<Value> = None;

                        for t in &self.group_tuples {
                            if let Ok(val) = col_expr.evaluate(t, schema) {
                                if !val.is_null() {
                                    if let Some(ref min) = min_val {
                                        if matches!(val.compare_less_than(min), CmpBool::CmpTrue) {
                                            min_val = Some(val);
                                        }
                                    } else {
                                        min_val = Some(val);
                                    }
                                }
                            }
                        }

                        min_val.unwrap_or_else(|| Value::new(Val::Null))
                    }
                    AggregationType::Max => {
                        let col_expr = agg.get_children()[0].clone();
                        let mut max_val: Option<Value> = None;

                        for t in &self.group_tuples {
                            if let Ok(val) = col_expr.evaluate(t, schema) {
                                if !val.is_null() {
                                    if let Some(ref max) = max_val {
                                        if matches!(val.compare_greater_than(max), CmpBool::CmpTrue)
                                        {
                                            max_val = Some(val);
                                        }
                                    } else {
                                        max_val = Some(val);
                                    }
                                }
                            }
                        }

                        max_val.unwrap_or_else(|| Value::new(Val::Null))
                    }
                    AggregationType::StdDev | AggregationType::Variance => {
                        // For testing purposes, return null for these aggregates
                        Value::new(Val::Null)
                    }
                }
            }
            _ => {
                error!("Expected aggregate expression in HAVING clause");
                return false;
            }
        };

        // Create a clone of agg_value for debug logging to avoid borrow issues
        let agg_value_for_debug = agg_value.clone();
        debug!("Computed aggregate value: {:?}", agg_value_for_debug);

        // Create a tuple with just the aggregate value for evaluation
        let agg_schema = Schema::new(vec![agg_expr.get_return_type().clone()]);
        let agg_tuple = Tuple::new(&vec![agg_value], &agg_schema, RID::new(0, 0));

        // Now evaluate the predicate on this aggregate tuple
        let predicate = filter_expr.get_predicate();
        match predicate.evaluate(&agg_tuple, &agg_schema) {
            Ok(value) => match value.get_val() {
                Val::Boolean(b) => {
                    debug!(
                        "HAVING filter evaluation result: {}, for aggregate value: {:?}",
                        b, agg_value_for_debug
                    );
                    *b
                }
                _ => {
                    error!(
                        "HAVING filter evaluation returned non-boolean value: {:?}",
                        value
                    );
                    false
                }
            },
            Err(e) => {
                error!("Failed to evaluate HAVING filter: {}", e);
                false
            }
        }
    }
}

impl AbstractExecutor for FilterExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("FilterExecutor already initialized");
            return;
        }

        debug!("Initializing FilterExecutor");
        self.child_executor.init();

        let filter_expr = self.plan.get_filter_expression();
        let schema = self.child_executor.get_output_schema();

        if matches!(filter_expr.get_filter_type(), FilterType::Having) {
            debug!("Collecting tuples for HAVING clause");
            while let Some((tuple, _)) = self.child_executor.next() {
                self.group_tuples.push(tuple);
            }
        }

        self.initialized = true;
        debug!("FilterExecutor initialized successfully");
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if !self.initialized {
            debug!("FilterExecutor not initialized, initializing now");
            self.init();
        }

        match self.plan.get_filter_expression().get_filter_type() {
            FilterType::Where => loop {
                match self.child_executor.next() {
                    Some((tuple, rid)) => {
                        debug!("Processing tuple with RID {:?}", rid);
                        if self.apply_filter(&tuple) {
                            debug!("Found matching tuple with RID {:?}", rid);
                            return Some((tuple, rid));
                        }
                        debug!("Tuple did not match filter, continuing...");
                    }
                    None => {
                        debug!("No more tuples from child executor");
                        return None;
                    }
                }
            },
            FilterType::Having => {
                while self.current_group_idx < self.group_tuples.len() {
                    let tuple = self.group_tuples.get(self.current_group_idx).unwrap();
                    let rid = tuple.get_rid();
                    self.current_group_idx += 1;

                    if self.apply_filter(tuple) {
                        debug!("Found matching tuple for HAVING clause");
                        return Some((tuple.clone(), rid));
                    }
                }
                debug!("No more tuples matching HAVING clause");
                None
            }
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
    use crate::sql::execution::executors::table_scan_executor::TableScanExecutor;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::aggregate_expression::{
        AggregateExpression, AggregationType,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::table_scan_plan::TableScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn transaction_context(&self) -> &Arc<TransactionContext> {
            &self.transaction_context
        }
    }

    struct EmptyExecutor {
        initialized: bool,
        schema: Schema,
        context: Arc<RwLock<ExecutionContext>>,
    }

    impl EmptyExecutor {
        fn new(context: Arc<RwLock<ExecutionContext>>, schema: Schema) -> Self {
            Self {
                initialized: false,
                schema,
                context,
            }
        }
    }

    impl AbstractExecutor for EmptyExecutor {
        fn init(&mut self) {
            self.initialized = true;
        }

        fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
            None
        }

        fn get_output_schema(&self) -> &Schema {
            &self.schema
        }

        fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
            self.context.clone()
        }
    }

    fn create_test_executor_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        // Create a new transaction

        Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog,
            test_context.transaction_context.clone(),
        )))
    }

    fn create_age_filter(
        age: i32,
        comparison_type: ComparisonType,
        schema: &Schema,
    ) -> Arc<FilterNode> {
        // Create column reference for age
        let col_idx = schema.get_column_index("age").unwrap();
        let age_col = schema.get_column(col_idx).unwrap().clone();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            col_idx,
            age_col.clone(),
            vec![],
        )));

        // Create constant expression for comparison
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(age),
            age_col,
            vec![],
        )));

        // Create predicate
        let predicate = Expression::Comparison(ComparisonExpression::new(
            col_expr,
            const_expr,
            comparison_type,
            vec![],
        ));

        Arc::new(FilterNode::new_where(
            schema.clone(),
            0,
            "test_table".to_string(),
            Arc::from(predicate),
            vec![PlanNode::Empty],
        ))
    }

    fn create_having_filter(
        aggregate_type: AggregationType,
        threshold: i32,
        schema: &Schema,
    ) -> Arc<FilterNode> {
        // Create aggregate expression for salary column
        let salary_col_idx = schema.get_column_index("salary").unwrap();
        let salary_col = schema.get_column(salary_col_idx).unwrap().clone();
        let agg_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            aggregate_type.clone(),
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,              // table index
                salary_col_idx, // use the actual column index
                salary_col.clone(),
                vec![],
            )))],
            salary_col.clone(),
            "".to_string(),
        )));

        // Create a new schema with just the aggregate column
        let agg_schema = Schema::new(vec![agg_expr.get_return_type().clone()]);

        debug!(
            "Created aggregate schema with {} column(s)",
            agg_schema.get_column_count()
        );

        // Create a constant with the correct type based on the aggregate function
        let agg_return_type = agg_expr.get_return_type();
        let const_value = match &aggregate_type {
            AggregationType::CountStar | AggregationType::Count => {
                Value::new_with_type(Val::BigInt(threshold as i64), TypeId::BigInt)
            }
            AggregationType::Sum => match agg_return_type.get_type() {
                TypeId::BigInt => {
                    Value::new_with_type(Val::BigInt(threshold as i64), TypeId::BigInt)
                }
                TypeId::Decimal => {
                    Value::new_with_type(Val::Decimal(threshold as f64), TypeId::Decimal)
                }
                _ => Value::new_with_type(Val::Integer(threshold), TypeId::Integer),
            },
            AggregationType::Min
            | AggregationType::Max
            | AggregationType::Avg
            | AggregationType::StdDev
            | AggregationType::Variance => match agg_return_type.get_type() {
                TypeId::BigInt => {
                    Value::new_with_type(Val::BigInt(threshold as i64), TypeId::BigInt)
                }
                TypeId::Decimal => {
                    Value::new_with_type(Val::Decimal(threshold as f64), TypeId::Decimal)
                }
                _ => Value::new_with_type(Val::Integer(threshold), TypeId::Integer),
            },
        };

        debug!(
            "Created constant value with type: {:?}",
            const_value.get_type_id()
        );

        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            const_value,
            agg_return_type.clone(),
            vec![],
        )));

        // Create column reference for the aggregate result (index 0 in the aggregate schema)
        let agg_result_col_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // table index
            0, // column index in the aggregate schema (always 0)
            agg_expr.get_return_type().clone(),
            vec![],
        )));

        debug!("Created column reference for aggregate result at index 0");

        // Create predicate using the column reference to the aggregate result
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            agg_result_col_ref,
            const_expr,
            ComparisonType::GreaterThan,
            vec![],
        )));

        Arc::new(FilterNode::new_having(
            agg_schema,
            0,
            "test_table".to_string(),
            agg_expr,
            predicate,
            vec![PlanNode::Empty],
        ))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
        ])
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm(),
            0,                               // next_index_oid
            0,                               // next_table_oid
            HashMap::new(),                  // tables
            HashMap::new(),                  // indexes
            HashMap::new(),                  // table_names
            HashMap::new(),                  // index_names
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn setup_test_table(
        txn_table_heap: &TransactionalTableHeap,
        schema: &Schema,
        transaction_context: &Arc<TransactionContext>,
    ) {
        let test_data = vec![
            (1, "Alice", 25, 50000.0),
            (2, "Bob", 30, 75000.0),
            (3, "Charlie", 35, 100000.0),
            (4, "David", 28, 65000.0),
            (5, "Eve", 32, 85000.0),
        ];

        for (id, name, age, salary) in test_data {
            let values = vec![
                Value::new(id),
                Value::new(name.to_string()),
                Value::new(age),
                Value::new(salary),
            ];
            let meta = Arc::new(TupleMeta::new(transaction_context.get_transaction_id()));
            txn_table_heap
                .insert_tuple_from_values(values, schema, transaction_context.clone())
                .expect("Failed to insert tuple");
        }
    }

    // Add this helper function for comparing floating-point numbers
    fn compare_floats(a: &f64, b: &f64) -> std::cmp::Ordering {
        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
    }

    #[test]
    fn test_filter_equals() {
        let test_context = TestContext::new("filter_eq_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table with proper cleanup
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data with transaction handling
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan first
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info.clone(),
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create filter for age = 28 (should match David)
        let filter_plan = create_age_filter(28, ComparisonType::Equal, &schema);

        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let tuple_values = tuple.get_values();
            let name = match tuple_values[1].get_val() {
                Val::VarLen(s) | Val::ConstLen(s) => s.to_string(),
                _ => "".to_string(),
            };
            let age = match tuple_values[2].get_val() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer value for age"),
            };
            results.push((name, age));
        }

        assert_eq!(results.len(), 1, "Should find exactly one match");
        assert_eq!(results[0].0, "David");
        assert_eq!(results[0].1, 28);

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_filter_greater_than() {
        let test_context = TestContext::new("filter_gt_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info,
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create filter for age > 30
        let filter_plan = create_age_filter(30, ComparisonType::GreaterThan, &schema);

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let age = match tuple.get_value(2).get_val() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer value for age"),
            };
            results.push(age);
        }

        // Sort results for consistent comparison
        results.sort();

        // Verify results
        assert_eq!(
            results,
            vec![32, 35],
            "Should find exactly two matches > 30"
        );

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_filter_no_matches() {
        let test_context = TestContext::new("filter_no_matches_test");
        let schema = create_test_schema();

        // Create catalog with the test table
        let mut catalog = create_catalog(&test_context);
        catalog.create_table("test_table".to_string(), schema.clone());
        let catalog = Arc::new(RwLock::new(catalog));

        // Create filter for age > 100 (no matches)
        let filter_plan = create_age_filter(100, ComparisonType::GreaterThan, &schema);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create an empty executor as the child executor
        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone(),
        ));

        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Should not return any results
        assert!(executor.next().is_none());

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_filter_with_transaction() {
        let test_context = TestContext::new("filter_transaction_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Insert test data within a transaction
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan with explicit schema reference
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info.clone(),
            Arc::new(schema.clone()),
            None,
        ));

        // Create filter plan for age > 25
        let filter_plan = create_age_filter(25, ComparisonType::GreaterThan, &schema);

        // Create child executor (table scan)
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect results with a safety counter to prevent infinite loops
        let mut results = Vec::new();
        let max_iterations = 100; // Reasonable upper limit
        let mut iteration_count = 0;

        while let Some((tuple, _)) = executor.next() {
            iteration_count += 1;
            if iteration_count > max_iterations {
                panic!("Possible infinite loop detected in filter execution");
            }

            let age = match tuple.get_value(2).get_val() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer for age"),
            };
            results.push(age);
        }

        // Sort and verify results
        results.sort();
        let expected_ages = vec![28, 30, 32, 35];
        assert_eq!(
            results, expected_ages,
            "Filtered results don't match expected ages"
        );

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_filter_invalid_predicate() {
        let test_context = TestContext::new("filter_invalid_test");
        let schema = create_test_schema();

        // Create a filter with an empty schema to simulate invalid predicate
        let empty_schema = Schema::new(vec![]);

        // Create a simple predicate that doesn't depend on schema columns
        let predicate = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let invalid_filter_plan = Arc::new(FilterNode::new_where(
            empty_schema,
            0,
            "test_table".to_string(),
            predicate,
            vec![PlanNode::Empty],
        ));

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone(),
        ));

        let mut executor =
            FilterExecutor::new(child_executor, executor_context, invalid_filter_plan);

        // Should initialize but return no results due to invalid predicate
        executor.init();
        assert!(executor.next().is_none());

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_having_filter() {
        let test_context = TestContext::new("having_filter_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info,
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create HAVING filter for COUNT(*) > 0 (this should match all rows in our test)
        let filter_plan = create_having_filter(AggregationType::CountStar, 0, &schema);

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let age = match tuple.get_value(2).get_val() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer value for age"),
            };
            results.push(age);
        }

        // Sort results for consistent comparison
        results.sort();

        // Verify results - should return all ages since COUNT(*) > 0
        assert_eq!(results.len(), 5, "Should find all rows since count > 0");
        assert_eq!(results, vec![25, 28, 30, 32, 35]);

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_having_filter_no_matches() {
        let test_context = TestContext::new("having_filter_no_matches_test");
        let schema = create_test_schema();

        // Create catalog with the test table
        let mut catalog = create_catalog(&test_context);
        catalog.create_table("test_table".to_string(), schema.clone());
        let catalog = Arc::new(RwLock::new(catalog));

        // Create HAVING filter for COUNT(*) > 10 (no matches, since there are no rows in the table)
        let filter_plan = create_having_filter(AggregationType::CountStar, 10, &schema);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create an empty executor as the child executor
        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone(),
        ));

        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Should not return any results
        assert!(executor.next().is_none());

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_having_avg_filter() {
        let test_context = TestContext::new("having_avg_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info,
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create HAVING filter for AVG(salary) > 50000 using the fixed create_having_filter function
        let filter_plan = create_having_filter(AggregationType::Avg, 50000, &schema);

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let salary = match tuple.get_value(3).get_val() {
                Val::Decimal(s) => *s,
                _ => panic!("Expected decimal value for salary"),
            };
            results.push(salary);
        }

        // Sort results for consistent comparison
        results.sort_by(compare_floats);

        // Verify results - should return all salaries since avg > 50000
        assert_eq!(
            results.len(),
            5,
            "Should find all rows since avg salary > 50000"
        );
        assert_eq!(results, vec![50000.0, 65000.0, 75000.0, 85000.0, 100000.0]);

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_having_max_filter() {
        let test_context = TestContext::new("having_max_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info,
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create HAVING filter for MAX(salary) > 90000 using the fixed create_having_filter
        let filter_plan = create_having_filter(AggregationType::Max, 90000, &schema);

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let salary = match tuple.get_value(3).get_val() {
                Val::Decimal(s) => *s,
                _ => panic!("Expected decimal value for salary"),
            };
            results.push(salary);
        }

        // Sort results for consistent comparison
        results.sort_by(compare_floats);

        // Verify results - should return all salaries since max > 90000
        assert_eq!(
            results.len(),
            5,
            "Should find all rows since max salary > 90000"
        );
        assert_eq!(results, vec![50000.0, 65000.0, 75000.0, 85000.0, 100000.0]);

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_where_and_having_sequence() {
        let test_context = TestContext::new("where_having_sequence_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data
        setup_test_table(&txn_table_heap, &schema, transaction_context);

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info.clone(),
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create WHERE filter for age > 30
        let where_filter = create_age_filter(30, ComparisonType::GreaterThan, &schema);

        // Create HAVING filter for AVG(salary) > 70000 using fixed create_having_filter
        let having_filter = create_having_filter(AggregationType::Avg, 70000, &schema);

        // Create and initialize filter executors
        let mut where_executor =
            FilterExecutor::new(child_executor, executor_context.clone(), where_filter);
        where_executor.init();

        let mut having_executor =
            FilterExecutor::new(Box::new(where_executor), executor_context, having_filter);
        having_executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = having_executor.next() {
            let salary = match tuple.get_value(3).get_val() {
                Val::Decimal(s) => *s,
                _ => panic!("Expected decimal value for salary"),
            };
            results.push(salary);
        }

        // Sort results for consistent comparison
        results.sort_by(compare_floats);

        // Verify results - should return salaries of people over 30 with avg > 70000
        assert_eq!(
            results.len(),
            3,
            "Should find 3 rows matching both conditions"
        );
        assert_eq!(results, vec![75000.0, 85000.0, 100000.0]);

        // Verify cleanup
        drop(having_executor);
        assert!(test_context._temp_dir.path().exists());
    }

    #[test]
    fn test_having_with_zero_values() {
        let test_context = TestContext::new("having_zero_test");
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create catalog and table
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let table_info = {
            let mut catalog_guard = catalog.write();
            catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap()
        };

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Set up test data with some NULL values
        let test_data = vec![
            (1, "Alice", 25, 50000.0),
            (2, "Bob", 30, 75000.0),
            (3, "Charlie", 35, 100000.0),
            (4, "David", 28, 65000.0),
            (5, "Eve", 32, 85000.0),
            (6, "Frank", 40, 0.0), // Zero salary
            (7, "Grace", 45, 0.0), // Zero salary
        ];

        for (id, name, age, salary) in test_data {
            let values = vec![
                Value::new(id),
                Value::new(name.to_string()),
                Value::new(age),
                Value::new(salary),
            ];
            txn_table_heap
                .insert_tuple_from_values(values, &schema, transaction_context.clone())
                .expect("Failed to insert tuple");
        }

        // Create executor context
        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create table scan plan
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info,
            Arc::new(schema.clone()),
            None,
        ));

        // Create table scan executor
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan,
        ));

        // Create HAVING filter for AVG(salary) > 50000 using fixed create_having_filter
        let filter_plan = create_having_filter(AggregationType::Avg, 50000, &schema);

        // Create and initialize filter executor
        let mut executor = FilterExecutor::new(child_executor, executor_context, filter_plan);
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let salary = match tuple.get_value(3).get_val() {
                Val::Decimal(s) => *s,
                _ => panic!("Expected decimal value for salary"),
            };
            results.push(salary);
        }

        // Sort results for consistent comparison
        results.sort_by(compare_floats);

        // Verify results - should return all non-zero salaries since avg > 50000
        assert_eq!(
            results.len(),
            5,
            "Should find 5 rows with non-zero salaries"
        );
        assert_eq!(results, vec![50000.0, 65000.0, 75000.0, 85000.0, 100000.0]);

        // Verify cleanup
        drop(executor);
        assert!(test_context._temp_dir.path().exists());
    }
}
