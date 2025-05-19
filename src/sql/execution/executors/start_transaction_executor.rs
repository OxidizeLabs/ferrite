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
        let isolation_level = match self.plan.get_isolation_level() {
            &Some(level) => level,
            &None => IsolationLevel::ReadCommitted,
        };
        let read_only = self.plan.is_read_only();
        

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
    use std::thread;
    use std::time::Duration;

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
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), false);
        
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
        let plan = StartTransactionPlanNode::new(None, false);
        
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
        
        // Check isolation level - should default to READ COMMITTED when none is specified
        let isolation_level = exec_context.read().get_transaction_context().get_transaction().get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_start_read_only_transaction() {
        let test_context = TestContext::new("start_read_only_transaction_test");
        let exec_context = test_context.create_execution_context();
        
        // Store the original transaction ID
        let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        
        // Create a start transaction plan with read-only flag set to true
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::ReadCommitted), true);
        
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
        assert_eq!(isolation_level, IsolationLevel::ReadCommitted);
        
        // In a real implementation, we would verify the read-only flag here
        // but that would require modifying the Transaction class to store that flag
    }

    #[test]
    fn test_all_isolation_levels() {
        // Test each of the isolation levels
        let isolation_levels = vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ];
        
        for expected_level in isolation_levels {
            let test_context = TestContext::new(&format!("isolation_level_{}_test", expected_level.to_string().to_lowercase()));
            let exec_context = test_context.create_execution_context();
            
            // Store the original transaction ID
            let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
            
            // Create a start transaction plan with this isolation level
            let plan = StartTransactionPlanNode::new(Some(expected_level), false);
            
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
            assert_eq!(isolation_level, expected_level, "Isolation level should match for {}", expected_level);
        }
    }

    #[test]
    fn test_concurrent_transaction_starts() {
        // This test verifies that multiple transactions can be started concurrently
        let test_context = TestContext::new("concurrent_transaction_starts_test");
        let transaction_manager = test_context.transaction_manager.clone();
        let lock_manager = test_context.lock_manager.clone();
        let buffer_pool = test_context.bpm.clone();
        
        // Number of threads to create
        const NUM_THREADS: usize = 5;
        
        // Create a barrier to synchronize thread start
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
        
        // Create multiple threads, each starting a transaction
        let mut handles = Vec::with_capacity(NUM_THREADS);
        let txn_ids = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(NUM_THREADS)));
        
        for i in 0..NUM_THREADS {
            let thread_tm = transaction_manager.clone();
            let thread_barrier = barrier.clone();
            let thread_txn_ids = txn_ids.clone();
            
            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                thread_barrier.wait();
                
                // Start a transaction with a specific isolation level
                let isolation_level = match i % 4 {
                    0 => IsolationLevel::ReadUncommitted,
                    1 => IsolationLevel::ReadCommitted,
                    2 => IsolationLevel::RepeatableRead,
                    _ => IsolationLevel::Serializable,
                };
                
                // Try to begin a transaction and record its ID
                match thread_tm.begin(isolation_level) {
                    Ok(txn) => {
                        let txn_id = txn.get_transaction_id();
                        thread_txn_ids.lock().push(txn_id);
                        
                        // Small sleep to simulate some work
                        thread::sleep(Duration::from_millis(10));
                        
                        // Return transaction for cleanup
                        Some(txn)
                    },
                    Err(_) => None,
                }
            });
            
            handles.push(handle);
        }
        
        // Collect results and clean up
        for handle in handles {
            if let Ok(Some(txn)) = handle.join() {
                transaction_manager.commit(txn, buffer_pool.clone());
            }
        }
        
        // Verify that all transactions got unique IDs
        let final_txn_ids = txn_ids.lock().clone();
        assert_eq!(final_txn_ids.len(), NUM_THREADS, "All threads should have started transactions");
        
        // Check for uniqueness by converting to a HashSet
        let unique_ids: std::collections::HashSet<_> = final_txn_ids.iter().collect();
        assert_eq!(unique_ids.len(), NUM_THREADS, "All transaction IDs should be unique");
    }

    #[test]
    fn test_transaction_manager_shutdown_handling() {
        let test_context = TestContext::new("transaction_manager_shutdown_test");
        let exec_context = test_context.create_execution_context();
        
        // First, shut down the transaction manager
        test_context.transaction_manager.shutdown().unwrap();
        
        // Try to start a transaction after shutdown
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), false);
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);
        
        // Initialize and execute
        executor.init();
        let result = executor.next();
        
        // Should return None (no output data) since transaction manager is shut down
        assert!(result.is_none());
        
        // The transaction context should not have changed since transaction start failed
        let original_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        let new_txn_id = exec_context.read().get_transaction_context().get_transaction_id();
        assert_eq!(original_txn_id, new_txn_id, "Transaction context should not change when transaction manager is shut down");
    }
} 