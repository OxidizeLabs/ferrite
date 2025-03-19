use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CreateTableExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<CreateTablePlanNode>,
    executed: bool,
}

impl CreateTableExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<CreateTablePlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateTableExecutor for table '{}', if_not_exists={}",
            plan.get_table_name(),
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

impl AbstractExecutor for CreateTableExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
        );
        self.executed = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.executed {
            debug!("CreateTableExecutor already executed, returning None");
            return None;
        }

        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema().clone();

        debug!("Acquiring executor context lock for table '{}'", table_name);
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

        debug!("Acquiring catalog write lock for table '{}'", table_name);
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

            // Check existence first
            if self.plan.if_not_exists() && catalog_guard.get_table(table_name).is_some() {
                info!(
                    "Table '{}' already exists, skipping creation (IF NOT EXISTS)",
                    table_name
                );
                self.executed = true;
                return None;
            }

            // Create the table
            debug!("Creating new table '{}' in catalog", table_name);
            let table_info = catalog_guard.create_table(table_name.to_string(), schema);
            match table_info {
                Some(table_info) => {
                    info!(
                        "Successfully created table '{}' with OID {}",
                        table_name,
                        table_info.get_table_oidt()
                    );
                }
                None => {
                    if !self.plan.if_not_exists() {
                        warn!(
                            "Failed to create table '{}' - table may already exist",
                            table_name
                        );
                    }
                }
            }
        }
        debug!("Released catalog write lock");

        self.executed = true;
        debug!("CreateTableExecutor execution completed");
        None
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

impl Drop for CreateTableExecutor {
    fn drop(&mut self) {
        debug!(
            "Dropping CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
        );
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
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
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
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create transaction manager and lock manager first
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
            0,                               // next_index_oid
            0,                               // next_table_oid
            HashMap::new(),                  // tables
            HashMap::new(),                  // indexes
            HashMap::new(),                  // table_names
            HashMap::new(),                  // index_names
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    #[test]
    fn test_create_table_basic() {
        let test_context = TestContext::new("test_create_table_basic");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create execution context with the same catalog instance
        let exec_context = Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog.clone(), // Use the same catalog instance
            test_context.transaction_context.clone(),
        )));

        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
        }

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().is_none());

        let catalog_guard = catalog.read();
        let tables = catalog_guard.get_table("test_table").unwrap();
        assert_eq!(tables.get_table_name(), "test_table");
    }
}
