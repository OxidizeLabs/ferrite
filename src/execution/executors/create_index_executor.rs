use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::storage::index::index::IndexType;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CreateIndexExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<CreateIndexPlanNode>,
    executed: bool,
}

impl CreateIndexExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<CreateIndexPlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateIndexExecutor for index '{}', if_not_exists={}",
            plan.get_index_name(),
            plan.if_not_exists()
        );
        debug!("Output schema: {:?}", plan.get_output_schema());

        Self {
            context,
            plan,
            executed,
        }
    }
}

impl AbstractExecutor for CreateIndexExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
        self.executed = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.executed {
            debug!("CreateIndexExecutor already executed, returning None");
            return None;
        }

        let index_name = self.plan.get_index_name();
        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema().clone();
        let key_attrs = self.plan.get_key_attrs();

        debug!("Acquiring executor context lock for index '{}'", index_name);
        let catalog = {
            let context_guard = match self.context.try_read() {
                Some(guard) => {
                    debug!("Successfully acquired context read lock");
                    guard
                }
                None => {
                    warn!("Failed to acquire context read lock - lock contention detected");
                    return None;
                }
            };
            context_guard.get_catalog().clone()
        };
        debug!("Released executor context lock");

        debug!("Acquiring catalog write lock for index '{}'", index_name);
        {
            let mut catalog_guard = match catalog.try_write() {
                Some(guard) => {
                    debug!("Successfully acquired catalog write lock");
                    guard
                }
                None => {
                    warn!("Failed to acquire catalog write lock - lock contention detected");
                    return None;
                }
            };

            // Check if index already exists
            let existing_indexes = catalog_guard.get_table_indexes(table_name);
            let index_exists = existing_indexes.iter().any(|idx| idx.get_index_name() == index_name);

            if index_exists {
                if self.plan.if_not_exists() {
                    info!(
                        "Index '{}' already exists, skipping creation (IF NOT EXISTS)",
                        index_name
                    );
                    self.executed = true;
                    return None;
                } else {
                    warn!("Index '{}' already exists", index_name);
                    self.executed = true;
                    return None;
                }
            }

            // Create the index
            debug!("Creating new index '{}' in catalog", index_name);
            match catalog_guard.create_index(
                index_name,
                table_name,
                schema,
                key_attrs.to_vec(),
                4, // TODO: Make this configurable
                false,
                IndexType::BPlusTreeIndex,
            ) {
                Some(index_info) => {
                    info!(
                        "Successfully created index '{}' with OID {}",
                        index_name,
                        index_info.get_index_oid()
                    );
                }
                None => {
                    warn!(
                        "Failed to create index '{}' - catalog creation failed",
                        index_name
                    );
                }
            }
        }
        debug!("Released catalog write lock");

        self.executed = true;
        debug!("CreateIndexExecutor execution completed");
        None
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}

impl Drop for CreateIndexExecutor {
    fn drop(&mut self) {
        debug!(
            "Dropping CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
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
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::index::index::IndexType;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::fs;

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

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ])
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm.clone(),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new()
        )
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
            test_context.bpm.clone(),
            test_context.lock_manager.clone(),
        )))
    }

    #[test]
    fn test_create_index_basic() {
        let test_context = TestContext::new("create_index_basic");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table", schema.clone());
        }

        let exec_context = create_test_executor_context(&test_context, catalog.clone());
        let key_attrs = vec![0];
        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            key_attrs,
            false
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().is_none());

        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("test_table");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].get_index_name(), "test_index");
    }

    #[test]
    fn test_create_index_multiple_columns() {
        let test_context = TestContext::new("create_index_multi_col");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table", schema.clone());
        }

        let columns = vec![0,1];

        let exec_context = create_test_executor_context(&test_context, catalog.clone());
        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            columns,
            false
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().is_none());

        let catalog_guard = catalog.read();
        let index = catalog_guard.get_table_indexes("test_table")[0];
        assert_eq!(index.get_key_schema().get_column_count(), 2);
    }

    #[test]
    fn test_create_index_if_not_exists() {
        let test_context = TestContext::new("create_index_if_not_exists");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create table and initial index
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table", schema.clone());
            catalog_guard.create_index(
                "test_index",
                "test_table",
                schema.clone(),
                vec![0],
                4,
                false,
                IndexType::BPlusTreeIndex,
            );
        }

        let exec_context = create_test_executor_context(&test_context, catalog.clone());
        let columns = vec![0];

        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_index".to_string(),
            "test_table".to_string(),
            columns,
            true
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();

        // Should not fail when index exists
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_create_index_table_not_exists() {
        let test_context = TestContext::new("create_index_no_table");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        let exec_context = create_test_executor_context(&test_context, catalog.clone());
        let columns = vec![0];
        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_index".to_string(),
            "nonexistent_table".to_string(),
            columns,
            false
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();

        assert!(executor.next().is_none());

        // Verify no index was created
        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("nonexistent_table");
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_create_index_concurrent() {
        let test_context = TestContext::new("create_index_concurrent");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create test table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table", schema.clone());
        }

        let exec_context = create_test_executor_context(&test_context, catalog.clone());

        // Create multiple index executors
        let mut executors = vec![];
        let columns = vec![0];

        for i in 0..3 {
            let plan = Arc::new(CreateIndexPlanNode::new(
                schema.clone(),
                "test_table".to_string(),
                format!("test_index_{}", i),
                columns.clone(),
                false
            ));
            executors.push(CreateIndexExecutor::new(exec_context.clone(), plan, false));
        }

        // Execute concurrently
        for executor in executors.iter_mut() {
            executor.init();
            assert!(executor.next().is_none());
        }

        // Verify all indexes were created
        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("test_table");
        assert_eq!(indexes.len(), 3);
    }

    #[test]
    fn test_create_index_duplicate() {
        let test_context = TestContext::new("create_index_duplicate");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table", schema.clone());
        }

        let exec_context = create_test_executor_context(&test_context, catalog.clone());
        let key_attrs = vec![0];

        // Create first index
        let plan1 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            key_attrs.clone(),
            false
        ));

        let mut executor1 = CreateIndexExecutor::new(exec_context.clone(), plan1, false);
        executor1.init();
        assert!(executor1.next().is_none());

        // Verify first index was created
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(indexes.len(), 1, "First index should be created");
            assert_eq!(indexes[0].get_index_name(), "test_index");
        }

        // Try to create duplicate index without IF NOT EXISTS
        let plan2 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            key_attrs.clone(),
            false
        ));

        let mut executor2 = CreateIndexExecutor::new(exec_context.clone(), plan2, false);
        executor2.init();
        assert!(executor2.next().is_none());

        // Verify no duplicate was created
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(indexes.len(), 1, "No duplicate index should be created");
        }

        // Try to create duplicate index with IF NOT EXISTS
        let plan3 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            key_attrs,
            true
        ));

        let mut executor3 = CreateIndexExecutor::new(exec_context, plan3, false);
        executor3.init();
        assert!(executor3.next().is_none());

        // Verify still only one index exists
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(indexes.len(), 1, "IF NOT EXISTS should not create duplicate");
        }
    }
}
