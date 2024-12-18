use crate::catalogue::schema::Schema;
use crate::common::config::PageId;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Val;
use log::{debug, error};
use std::sync::Arc;

pub struct SeqScanExecutor {
    context: Arc<ExecutorContext>,
    plan: Arc<SeqScanPlanNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    iterator: Option<TableIterator>,
}

impl SeqScanExecutor {
    pub fn new(context: Arc<ExecutorContext>, plan: Arc<SeqScanPlanNode>) -> Self {
        let table_name = plan.get_table_name();
        let table_heap = {
            let catalog = context.get_catalog();
            let catalog_guard = catalog.read();
            let table_info = catalog_guard
                .get_table(table_name)
                .expect("Table not found");
            table_info.get_table_heap()
        };

        Self {
            context,
            plan,
            table_heap,
            iterator: None,
            initialized: false,
        }
    }

    fn apply_predicate(&self, tuple: &Tuple) -> bool {
        if let Some(predicate) = self.plan.get_filter_predicate() {
            match predicate.evaluate(tuple, self.plan.get_output_schema()) {
                Ok(value) => match value.get_value() {
                    Val::Boolean(b) => *b,
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
        } else {
            true
        }
    }

    // fn acquire_table_lock(&self) -> Result<(), String> {
    //     let txn = self.context.get_transaction();
    //     let mut txn_guard = txn.lock();
    //     let lock_manager = self.context.get_lock_manager();
    //     // Acquire table lock in shared mode for reading
    //     lock_manager.lock_table(&mut txn_guard, LockMode::Exclusive, self.plan.get_table_oid());
    //     Ok(())
    // }
}

impl AbstractExecutor for SeqScanExecutor {
    fn init(&mut self) {
        debug!("Initializing SeqScanExecutor");

        // Acquire table lock
        // if let Err(e) = self.acquire_table_lock() {
        //     error!("Failed to acquire table lock: {}", e);
        //     return;
        // }

        // Create iterator from start to end of table
        // Start at first tuple of first page
        let start_rid = RID::new(0, 0);
        // Use max values to ensure we scan to the end
        let stop_rid = RID::new(u32::MAX as PageId, u32::MAX);

        self.iterator = Some(TableIterator::new(
            self.table_heap.clone(),
            start_rid,
            stop_rid,
        ));
        self.initialized = true;

        debug!("SeqScanExecutor initialized successfully");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Cache predicate presence to avoid borrow conflicts
        let has_predicate = self.plan.get_filter_predicate().is_some();

        // Keep trying until we find a valid tuple or reach the end
        while let Some(iter) = &mut self.iterator {
            match iter.next() {
                Some((meta, tuple)) => {
                    // Skip deleted tuples
                    if meta.is_deleted() {
                        continue;
                    }

                    // If there's no predicate or if the tuple satisfies the predicate
                    if !has_predicate || self.apply_predicate(&tuple) {
                        debug!("Found matching tuple with RID: {:?}", tuple.get_rid());
                        return Some((tuple.clone(), tuple.get_rid()));
                    }
                }
                None => {
                    debug!("No more tuples to scan");
                    return None;
                }
            }
        }

        debug!("No iterator available");
        None
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::collections::HashMap;
    use std::fs;
    use chrono::Utc;
    use parking_lot::{Mutex, RwLock};
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;

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

            // Create TransactionManager with a placeholder Catalog
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

    fn create_catalog(
        bpm: Arc<BufferPoolManager>
    ) -> Catalog {
        Catalog::new(
            bpm,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn create_test_tuple(schema: &Schema, id: i32, name: &str, age: i32) -> (TupleMeta, Tuple) {
        let values = vec![
            Value::new(id),
            Value::new(name.to_string()),
            Value::new(age),
        ];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(0, false);
        (meta, tuple)
    }

    #[test]
    fn test_seq_scan_executor_with_data() {
        let ctx = TestContext::new("test_seq_scan_executor_with_data");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let transaction_manager = ctx.transaction_manager.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(bpm.clone());
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create transaction and table
        let txn = Arc::new(Mutex::new(Transaction::new(
            0,
            IsolationLevel::Serializable,
        )));
        let table_name = "test_table";
        let table_info = catalog
            .create_table(table_name, schema.clone())
            .unwrap();
        let table_heap = table_info.get_table_heap();

        // Insert test data
        let test_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, mut tuple) = create_test_tuple(&schema, *id, name, *age);
            table_heap
                .insert_tuple(&meta, &mut tuple)
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
            None,
        ));

        let context = Arc::new(ExecutorContext::new(
            txn,
            transaction_manager,
            Arc::new(RwLock::new(catalog)),
            Arc::clone(&bpm),
            lock_manager,
        ));

        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Test scanning
        let mut found_tuples = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            // Get values from tuple using proper indexing
            let id = match tuple.get_value(0).get_value() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for id"),
            };

            let name = match tuple.get_value(1).get_value() {
                Val::VarLen(s) => s.clone(),
                _ => panic!("Expected VarLen value for name"),
            };

            let age = match tuple.get_value(2).get_value() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for age"),
            };

            found_tuples.push((id, name, age));
        }

        assert_eq!(found_tuples.len(), 3, "Should have scanned 3 tuples");

        // Verify each tuple was found
        for (id, name, age) in test_data {
            assert!(
                found_tuples.iter().any(|(t_id, t_name, t_age)| {
                    *t_id == id && t_name == name && *t_age == age
                }),
                "Did not find tuple: ({}, {}, {})",
                id,
                name,
                age
            );
        }
    }

    #[test]
    fn test_seq_scan_executor_empty_table() {
        let ctx = TestContext::new("test_seq_scan_executor_empty");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let transaction_manager = ctx.transaction_manager.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(bpm.clone());
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create transaction and empty table
        let txn = Arc::new(Mutex::new(Transaction::new(
            0,
            IsolationLevel::Serializable,
        )));
        let table_name = "empty_table";
        let table_info = catalog
            .create_table(table_name, schema.clone())
            .unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
            None,
        ));

        let context = Arc::new(ExecutorContext::new(
            txn,
            transaction_manager,
            Arc::new(RwLock::new(catalog)),
            Arc::clone(&bpm),
            lock_manager,
        ));

        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Should return None for empty table
        assert!(
            executor.next().is_none(),
            "Empty table should return no tuples"
        );
    }
}
