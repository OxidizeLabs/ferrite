use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::table_scan_plan::TableScanNode;
use crate::storage::table::table_iterator::TableScanIterator;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

/// TableScanExecutor implements a simple sequential scan over a table
/// using the Volcano-style iterator model.
pub struct TableScanExecutor {
    /// The executor context
    context: Arc<RwLock<ExecutionContext>>,
    /// The table scan plan node
    plan: Arc<TableScanNode>,
    /// Flag indicating if the executor has been initialized
    initialized: bool,
    /// The iterator over the table's tuples
    iterator: Option<TableScanIterator>,
}

impl TableScanExecutor {
    /// Create a new table scan executor
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<TableScanNode>) -> Self {
        Self {
            context,
            plan,
            iterator: None,
            initialized: false,
        }
    }
}

impl AbstractExecutor for TableScanExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing TableScanExecutor for table: {}",
            self.plan.get_table_name()
        );

        // Create a new table scan iterator from the plan
        self.iterator = Some(self.plan.scan());
        self.initialized = true;

        debug!("TableScanExecutor initialized successfully");
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        // Initialize if not already done
        if !self.initialized {
            self.init();
        }

        // Get the next tuple from the iterator
        loop {
            return match &mut self.iterator {
                Some(iter) => {
                    match iter.next() {
                        Some((meta, tuple)) => {
                            // Skip deleted tuples
                            if meta.is_deleted() {
                                continue; // Use continue instead of recursive call
                            }
                            debug!("Found tuple with RID: {:?}", tuple.get_rid());
                            Ok(Some((tuple.clone(), tuple.get_rid())))
                        }
                        None => {
                            debug!("No more tuples to scan");
                            Ok(None)
                        }
                    }
                }
                None => {
                    error!("Iterator not initialized");
                    Err(DBError::Execution("Iterator not initialized".to_string()))
                }
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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
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
            let disk_manager = AsyncDiskManager::new(db_path.clone(), log_path.clone(), DiskManagerConfig::default()).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_manager_arc.clone(),
                replacer.clone(),
            ).unwrap());

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
    }

    fn create_test_values(
        id: i32,
        name: &str,
        age: i32,
    ) -> (TupleMeta, Vec<Value>) {
        let values = vec![
            Value::new(id),
            Value::new(name.to_string()),
            Value::new(age),
        ];
        let meta = TupleMeta::new(0);
        (meta, values)
    }

    #[tokio::test]
    async fn test_table_scan_executor() {
        let ctx = TestContext::new("test_table_scan_executor").await;
        let bpm = ctx.bpm.clone();
        let transaction_manager = ctx.transaction_manager.clone();

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]));

        // Create catalog and table
        let mut catalog = Catalog::new(bpm.clone(), transaction_manager);

        let table_name = "test_table".to_string();
        let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();

        // Insert test data
        let table_heap = table_info.get_table_heap();
        let test_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, values) = create_test_values(*id, name, *age);
            table_heap
                .insert_tuple_from_values(values, &schema, Arc::from(meta))
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(TableScanNode::new(
            table_info, // Wrap in Arc
            schema.clone(),
            None,
        ));

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm,
            Arc::new(RwLock::new(catalog)),
            ctx.transaction_context,
        )));

        let mut executor = TableScanExecutor::new(context, plan);

        // Test scanning
        executor.init();
        let mut count = 0;
        while let Ok(Some(_)) = executor.next() {
            count += 1;
        }

        assert_eq!(count, 3, "Should have scanned exactly 3 tuples");
    }

    #[tokio::test]
    async fn test_table_scan_executor_empty() {
        let ctx = TestContext::new("test_table_scan_executor_empty").await;
        let bpm = ctx.bpm.clone();

        // Create schema and empty table
        let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
        let catalog_guard = Arc::new(RwLock::new(Catalog::new(
            bpm,
            ctx.transaction_manager.clone(),
        )));

        let plan;
        {
            let mut catalog = catalog_guard.write();

            let table_name = "empty_table".to_string();
            let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();

            // Create executor
            plan = Arc::new(TableScanNode::new(table_info, schema.clone(), None));
        }

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm,
            catalog_guard,
            ctx.transaction_context,
        )));

        let mut executor = TableScanExecutor::new(context, plan);
        executor.init();

        assert!(
            executor.next().unwrap().is_none(),
            "Empty table should return no tuples"
        );
    }
}
