use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalogue::catalogue::Catalog;
use crate::catalogue::schema::Schema;
use crate::common::logger::initialize_logger;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::Tuple;
use chrono::Utc;
use parking_lot::RwLock;
use std::fs;
use std::sync::Arc;

pub struct SeqScanExecutor {
    context: ExecutorContext,
    plan: SeqScanPlanNode,
    table_heap: Arc<TableHeap>,
    initialized: bool,
}

impl SeqScanExecutor {
    pub fn new(context: ExecutorContext, plan: SeqScanPlanNode) -> Self {
        let table_oid = plan.get_table_oid();
        let catalog = context.get_catalog();
        let table_info = catalog
            .get_table_by_oid(table_oid)
            .expect("Table not found");
        let table_heap = table_info.get_table_heap();

        Self {
            context,
            plan,
            table_heap,
            initialized: false,
        }
    }

    fn apply_predicate(&self, tuple: &Tuple) -> bool {
        if let Some(predicate) = self.plan.get_filter_predicate() {
            // Evaluate the predicate on the tuple
            // This is a placeholder and should be replaced with actual predicate evaluation
            true
        } else {
            true
        }
    }
}

impl AbstractExecutor for SeqScanExecutor {
    fn init(&mut self) {
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        let mut iter = self.table_heap.make_iterator();

        while let Some((_tuple_meta, tuple)) = iter.next() {
            if self.apply_predicate(&tuple) {
                return Some((tuple.clone(), tuple.get_rid()));
            }
        }

        None
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        &self.context
    }
}

pub struct TestContext {
    bpm: Arc<BufferPoolManager>,
    transaction_manager: Arc<TransactionManager>,
    lock_manager: Arc<LockManager>,
    log_manager: Arc<LogManager>,
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
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));

        let file_disk_manager = Arc::new(FileDiskManager::new(
            "db_file.db".to_string(),
            "log_file.log".to_string(),
            10,
        ));
        let log_manager = Arc::new(LogManager::new(file_disk_manager));
        let catalog = Catalog::new(
            bpm.clone(),
            log_manager,
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );

        // Create TransactionManager with a placeholder Catalog
        let transaction_manager = Arc::new(TransactionManager::new(Arc::from(catalog)));
        let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));
        let log_manager = Arc::new(LogManager::new(Arc::clone(&disk_manager)));

        Self {
            bpm,
            transaction_manager,
            lock_manager,
            log_manager,
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

    pub fn log_manager(&self) -> Arc<LogManager> {
        Arc::clone(&self.log_manager)
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

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::types_db::type_id::TypeId;
    use std::collections::HashMap;

    fn create_catalog(
        bpm: Arc<BufferPoolManager>,
        lock_manager: Arc<LockManager>,
        log_manager: Arc<LogManager>,
    ) -> Catalog {
        Catalog::new(
            bpm,
            log_manager,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn test_seq_scan_executor() {
        let ctx = TestContext::new("test_seq_scan_executor");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();
        let transaction_manager = ctx.transaction_manager.clone();

        let mut catalog = create_catalog(bpm.clone(), lock_manager.clone(), log_manager);

        let table_name = "test_table";
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);
        let txn = Transaction::new(0, IsolationLevel::Serializable);

        // Create the table and store it in the catalog's maps
        let table_info = catalog.create_table(&txn, table_name, schema.clone(), true);

        let table_oid = table_info
            .expect("TableOID could not be found")
            .get_table_oidt();

        // Verify the table was properly registered
        assert!(
            catalog.get_table(table_name).is_some(),
            "Table should be registered in catalog"
        );

        // Create the scan plan
        let plan = SeqScanPlanNode::new(schema.clone(), table_oid, table_name.to_string(), None);

        // Create execution context
        let context = ExecutorContext::new(
            txn,
            transaction_manager,
            Arc::new(catalog),
            Arc::clone(&bpm),
            lock_manager,
        );

        // Create and initialize executor
        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Test the executor
        let mut result_count = 0;
        while let Some((_tuple, _rid)) = executor.next() {
            result_count += 1;
        }

        // Since we haven't inserted any tuples yet, result_count should be 0
        assert_eq!(result_count, 0, "Expected no tuples from empty table");
    }
}
