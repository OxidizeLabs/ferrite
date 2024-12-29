use crate::catalogue::schema::Schema;
use crate::common::config::PageId;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Val;
use log::{debug, error};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use crate::concurrency::transaction::TransactionState;
use crate::concurrency::transaction_manager::get_tuple_and_undo_link;

pub struct FilterExecutor {
    child_executor: Box<dyn AbstractExecutor>,
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<FilterNode>,
    initialized: bool,
}

impl FilterExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<FilterNode>,
    ) -> Self {
        Self {
            child_executor,
            context,
            plan,
            initialized: false,
        }
    }

    fn apply_predicate(&self, tuple: &Tuple) -> bool {
        let predicate = self.plan.get_filter_predicate();
        debug!(
            "Evaluating predicate on tuple with values: {:?}",
            tuple.get_values()
        );

        match predicate.evaluate(tuple, self.plan.get_output_schema()) {
            Ok(value) => match value.get_value() {
                Val::Boolean(b) => {
                    debug!(
                        "Predicate evaluation result: {}, for tuple: {:?}",
                        b,
                        tuple.get_values()
                    );
                    *b
                }
                _ => {
                    error!("Predicate evaluation did not return boolean value");
                    false
                }
            },
            Err(e) => {
                error!("Failed to evaluate predicate: {}", e);
                false
            }
        }
    }

    fn acquire_table_lock(&self) -> Result<(), String> {
        let txn = self.context.read().get_transaction();
        let context_guard = self.context.read();
        let lock_manager = context_guard.get_lock_manager();

        // Since txn is now Arc<Transaction>, we don't need &mut
        if let Err(e) = lock_manager.lock_table(txn, LockMode::Exclusive, self.plan.get_table_oid())
        {
            return Err(format!("Failed to acquire table lock: {:?}", e));
        }
        Ok(())
    }
}

impl AbstractExecutor for FilterExecutor {
    fn init(&mut self) {
        debug!("Initializing SeqScanExecutor");

        // Acquire table lock
        if let Err(e) = self.acquire_table_lock() {
            error!("Failed to acquire table lock: {}", e);
            return;
        }

        self.initialized = true;

        debug!("SeqScanExecutor initialized successfully");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Keep trying until we find a valid tuple or reach the end
        while let Some((tuple, rid)) = self.child_executor.next() {
            // Acquire context lock first
            let context_guard = self.context.read();
            let txn = context_guard.get_transaction();
            let txn_mgr = context_guard.get_transaction_manager();
            let txn_mgr_guard = txn_mgr.read();

            // Get table heap from catalog
            let table_heap = {
                let catalog = context_guard.get_catalog();
                let catalog_guard = catalog.read();
                let table_info = catalog_guard
                    .get_table(self.plan.get_table_name())
                    .expect("Table not found");
                table_info.get_table_heap()
            };

            // Check if tuple is visible to current transaction
            let (meta, _, _) = get_tuple_and_undo_link(
                &txn_mgr_guard,
                &table_heap,
                rid
            );

            // Check transaction visibility
            let is_visible = meta.get_timestamp() == txn.get_transaction_id() ||
                (!meta.is_deleted() && txn_mgr_guard.get_transaction(&meta.get_timestamp())
                    .map_or(false, |t| t.get_state() == TransactionState::Committed));

            if is_visible && self.apply_predicate(&tuple) {
                debug!("Found matching tuple with RID: {:?}", rid);
                return Some((tuple, rid));
            }
        }

        debug!("No more tuples match the filter criteria");
        None
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
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::execution::plans::abstract_plan::PlanNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableHeap;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;
    use std::fs;
    use crate::execution::executors::table_scan_executor::TableScanExecutor;
    use crate::execution::plans::table_scan_plan::TableScanNode;
    use crate::recovery::log_manager::LogManager;

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
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(catalog, log_manager)));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager.clone())));

            Self {
                bpm,
                transaction_manager,
                lock_manager,
                db_file,
                db_log_file,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn lock_manager(&self) -> Arc<LockManager> {
            Arc::clone(&self.lock_manager)
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    struct EmptyExecutor {
        initialized: bool,
        schema: Schema,
        context: Arc<RwLock<ExecutorContext>>,
    }

    impl EmptyExecutor {
        fn new(context: Arc<RwLock<ExecutorContext>>, schema: Schema) -> Self {
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

        fn next(&mut self) -> Option<(Tuple, RID)> {
            None
        }

        fn get_output_schema(&self) -> Schema {
            self.schema.clone()
        }

        fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
            self.context.clone()
        }
    }

    fn create_test_executor_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutorContext>> {
        // Create a new transaction
        let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

        Arc::new(RwLock::new(ExecutorContext::new(
            transaction,
            Arc::clone(&test_context.transaction_manager),
            catalog,
            test_context.bpm(),
            test_context.lock_manager(),
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

        Arc::new(FilterNode::new(
            schema.clone(),
            0,
            "test_table".to_string(),
            predicate,
            PlanNode::Empty,
        ))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm(),
            0,              // next_index_oid
            0,              // next_table_oid
            HashMap::new(), // tables
            HashMap::new(), // indexes
            HashMap::new(), // table_names
            HashMap::new(), // index_names
        )
    }

    fn setup_test_table(table_heap: &Arc<TableHeap>, schema: &Schema) {
        let test_data = vec![
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
            (4, "David", 28),
            (5, "Eve", 32),
        ];

        for (id, name, age) in test_data {
            let values = vec![
                Value::new(id),
                Value::new(name.to_string()),
                Value::new(age),
            ];
            let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(0, false);
            table_heap.insert_tuple(&meta, &mut tuple).unwrap();
        }
    }

    #[test]
    fn test_filter_equals() {
        let test_context = TestContext::new("filter_eq_test");
        let schema = create_test_schema();

        // Create catalog and table
        let mut catalog = create_catalog(&test_context);
        let table_info = catalog.create_table("test_table", schema.clone()).unwrap();

        // Set up test data
        let table_heap = table_info.get_table_heap();
        setup_test_table(&table_heap, &schema);

        let catalog = Arc::new(RwLock::new(catalog));

        // Create filter for age = 28 (should match David)
        let filter_plan = create_age_filter(28, ComparisonType::Equal, &schema);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create an empty executor as the child executor
        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone()
        ));

        let mut executor = FilterExecutor::new(
            child_executor,
            executor_context,
            filter_plan
        );
        executor.init();

        // Collect filtered results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let tuple_values = tuple.get_values();
            let name = tuple_values[1].to_string();
            let age = match tuple_values[2].get_value() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer value for age"),
            };
            results.push((name, age));
        }

        assert_eq!(results.len(), 1, "Should find exactly one match");
        assert_eq!(results[0].0, "David");
        assert_eq!(results[0].1, 28);
    }

    #[test]
    fn test_filter_greater_than() {
        let test_context = TestContext::new("filter_gt_test");
        let schema = create_test_schema();

        // Create catalog and table
        let mut catalog = create_catalog(&test_context);
        let table_info = catalog.create_table("test_table", schema.clone()).unwrap();

        // Set up test data
        let table_heap = table_info.get_table_heap();
        let test_data = vec![
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
            (4, "David", 28),
            (5, "Eve", 32),
        ];

        println!("Inserting test data:");
        for (id, name, age) in &test_data {
            let values = vec![
                Value::new(*id),
                Value::new(name.to_string()),
                Value::new(*age),
            ];
            let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(0, false);
            table_heap.insert_tuple(&meta, &mut tuple).unwrap();
            println!("Inserted: id={}, name={}, age={}", id, name, age);
        }

        let catalog = Arc::new(RwLock::new(catalog));

        // Create filter for age > 30
        let filter_plan = create_age_filter(30, ComparisonType::GreaterThan, &schema);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create an empty executor as the child executor
        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone()
        ));

        let mut executor = FilterExecutor::new(
            child_executor,
            executor_context,
            filter_plan
        );
        executor.init();

        // Collect filtered results with logging
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let tuple_values = tuple.get_values();
            let age = match tuple_values[2].get_value() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer value for age"),
            };
            println!("Found matching tuple with age: {}", age);
            results.push(age);
        }

        // Log all results before assertions
        println!("All matching ages: {:?}", results);

        assert!(
            results.iter().all(|&age| age > 30),
            "All ages should be > 30"
        );
        assert_eq!(results.len(), 2, "Should find exactly two matches > 30");
        assert!(results.contains(&35), "Should contain Charlie's age (35)");
        assert!(results.contains(&32), "Should contain Eve's age (32)");
    }

    #[test]
    fn test_filter_no_matches() {
        let test_context = TestContext::new("filter_no_matches_test");
        let schema = create_test_schema();

        // Create catalog with the test table
        let mut catalog = create_catalog(&test_context);
        catalog.create_table("test_table", schema.clone());
        let catalog = Arc::new(RwLock::new(catalog));

        // Create filter for age > 100 (no matches)
        let filter_plan = create_age_filter(100, ComparisonType::GreaterThan, &schema);

        let executor_context = create_test_executor_context(&test_context, Arc::clone(&catalog));

        // Create an empty executor as the child executor
        let child_executor = Box::new(EmptyExecutor::new(
            Arc::clone(&executor_context),
            schema.clone()
        ));

        let mut executor = FilterExecutor::new(
            child_executor,
            executor_context,
            filter_plan
        );
        executor.init();

        // Should not return any results
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_filter_with_transaction() {
        let test_context = TestContext::new("filter_transaction_test");
        let schema = create_test_schema();

        // Create catalog and wrap it in Arc<RwLock> first
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));

        // Create table within a block to ensure the write lock is dropped
        let table_heap = {
            let mut catalog_guard = catalog.write();
            let table_info = catalog_guard.create_table("test_table", schema.clone()).unwrap();
            table_info.get_table_heap().clone()
        };

        // Insert test data using a separate transaction
        let insert_txn = test_context.transaction_manager.write()
            .begin(IsolationLevel::ReadCommitted);

        let test_data = vec![
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
            (4, "David", 28),
            (5, "Eve", 32),
        ];

        // Insert the test data
        for (id, name, age) in test_data {
            let values = vec![
                Value::new(id),
                Value::new(name.to_string()),
                Value::new(age),
            ];
            let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(insert_txn.get_transaction_id(), false);
            table_heap.insert_tuple(&meta, &mut tuple).unwrap();
        }

        // Commit insert transaction
        test_context.transaction_manager.write().commit(insert_txn);

        // Create executor context with new transaction
        let txn = Arc::new(Transaction::new(1, IsolationLevel::RepeatableRead));
        let executor_context = Arc::new(RwLock::new(ExecutorContext::new(
            txn,
            Arc::clone(&test_context.transaction_manager),
            Arc::clone(&catalog),
            test_context.bpm(),
            test_context.lock_manager(),
        )));

        // Create table scan plan
        let binding = catalog.read();
        let table_info = binding.get_table("test_table").unwrap();
        let table_scan_plan = Arc::new(TableScanNode::new(
            table_info.clone(),
            Arc::new(schema.clone()),
            None
        ));

        // Create filter plan
        let filter_plan = create_age_filter(25, ComparisonType::GreaterThan, &schema);

        // Create child executor (table scan)
        let child_executor = Box::new(TableScanExecutor::new(
            Arc::clone(&executor_context),
            table_scan_plan
        ));

        // Create filter executor
        let mut executor = FilterExecutor::new(
            child_executor,
            executor_context,
            filter_plan
        );

        // Initialize the executor
        executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            let age = match tuple.get_value(2).get_value() {
                Val::Integer(a) => *a,
                _ => panic!("Expected integer for age"),
            };
            results.push(age);
        }

        // Verify results
        assert!(!results.is_empty(), "Should find matching tuples");
        assert!(results.iter().all(|&age| age > 25), "All ages should be > 25");
        assert_eq!(results.len(), 4); // Bob(30), Charlie(35), David(28), Eve(32)

        // Optional: Verify specific ages are present
        let expected_ages = vec![28, 30, 32, 35];
        let mut found_ages = results;
        found_ages.sort();
        assert_eq!(found_ages, expected_ages);
    }
}
