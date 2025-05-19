use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::start_transaction_plan::StartTransactionPlanNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for starting a new transaction
pub struct StartTransactionExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: StartTransactionPlanNode,
    schema: Schema,
    executed: bool,
}

impl StartTransactionExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: StartTransactionPlanNode) -> Self {
        let schema = plan.get_output_schema().clone();
        Self {
            context,
            plan,
            schema,
            executed: false,
        }
    }
}

impl AbstractExecutor for StartTransactionExecutor {
    fn init(&mut self) {
        // No specific initialization needed
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if self.executed {
            return None;
        }

        self.executed = true;

        // Get transaction information from plan
        let isolation_level_str = self.plan.get_isolation_level();
        let read_only = self.plan.is_read_only();

        // Parse isolation level string
        let isolation_level = match isolation_level_str {
            Some(level_str) => {
                match IsolationLevel::from_str(level_str) {
                    Some(level) => level,
                    None => {
                        error!("Invalid isolation level: {}", level_str);
                        // Default to SERIALIZABLE if invalid
                        IsolationLevel::Serializable
                    }
                }
            }
            None => IsolationLevel::default(), // Default isolation level
        };

        let context = self.context.read();
        let txn_manager = context.get_transaction_context().get_transaction_manager();
        let lock_manager = context.get_transaction_context().get_lock_manager();

        // Begin a new transaction with the specified isolation level
        match txn_manager.begin(isolation_level) {
            Ok(transaction) => {
                // Create a new transaction context with this transaction
                let txn_context = Arc::new(TransactionContext::new(
                    transaction,
                    lock_manager,
                    txn_manager.clone(),
                ));

                debug!(
                    "Started new transaction {} with isolation level {:?}, read_only: {}",
                    txn_context.get_transaction_id(),
                    isolation_level,
                    read_only
                );

                // Update the executor context with the new transaction context
                drop(context); // Release the read lock before acquiring write lock
                let mut context = self.context.write();
                context.set_transaction_context(txn_context);

                // Transaction operations don't return data
                None
            }
            Err(err) => {
                error!("Failed to start transaction: {}", err);
                None
            }
        }
    }

    fn get_output_schema(&self) -> &Schema {
        &self.schema
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
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::Transaction;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
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

            // Use the transaction manager to create a transaction instead of hardcoding ID 0
            let transaction = transaction_manager.begin(IsolationLevel::ReadUncommitted).unwrap();
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                lock_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn create_execution_context(&self) -> Arc<RwLock<ExecutionContext>> {
            let catalog = Arc::new(RwLock::new(Catalog::new(
                self.bpm.clone(),
                self.transaction_manager.clone(),
            )));
            
            Arc::new(RwLock::new(ExecutionContext::new(
                self.bpm.clone(),
                catalog,
                self.transaction_context.clone(),
            )))
        }
    }

    #[test]
    fn test_start_transaction_executor() {
        let test_context = TestContext::new("start_transaction_test");
        let exec_context = test_context.create_execution_context();
        
        // Store the original transaction ID
        let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        
        // Create a start transaction plan with serializable isolation level
        let plan = StartTransactionPlanNode::new(Some("SERIALIZABLE".to_string()), false);
        
        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);
        
        // Initialize and execute
        executor.init();
        let result = executor.next();
        
        // Should return None (no output data)
        assert!(result.is_none());
        
        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        assert_ne!(new_txn_id, original_txn_id, "New transaction ID should be different from original");
        
        // Check isolation level
        let isolation_level = exec_context.read().get_transaction_context().get_transaction().get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::Serializable);
        
        // Execute again - should return None as it's already executed
        let result = executor.next();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_start_transaction_with_default_isolation() {
        let test_context = TestContext::new("start_transaction_default_test");
        let exec_context = test_context.create_execution_context();
        
        // Store the original transaction ID
        let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        
        // Create a start transaction plan with no specified isolation level
        let plan = StartTransactionPlanNode::new(None, true);
        
        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);
        
        // Initialize and execute
        executor.init();
        let result = executor.next();
        
        // Should return None (no output data)
        assert!(result.is_none());
        
        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        assert_ne!(new_txn_id, original_txn_id, "New transaction ID should be different from original");
        
        // Check isolation level - should be default (READ COMMITTED)
        let isolation_level = exec_context.read().get_transaction_context().get_transaction().get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::default());
    }
    
    #[test]
    fn test_start_transaction_with_invalid_isolation() {
        let test_context = TestContext::new("start_transaction_invalid_test");
        let exec_context = test_context.create_execution_context();
        
        // Store the original transaction ID
        let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        
        // Create a start transaction plan with an invalid isolation level
        let plan = StartTransactionPlanNode::new(Some("INVALID_LEVEL".to_string()), false);
        
        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);
        
        // Initialize and execute
        executor.init();
        let result = executor.next();
        
        // Should return None (no output data)
        assert!(result.is_none());
        
        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        assert_ne!(new_txn_id, original_txn_id, "New transaction ID should be different from original");
        
        // Check isolation level - should default to SERIALIZABLE for invalid levels
        let isolation_level = exec_context.read().get_transaction_context().get_transaction().get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::Serializable);
    }
} 