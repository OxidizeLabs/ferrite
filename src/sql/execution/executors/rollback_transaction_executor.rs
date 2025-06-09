use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::rollback_transaction_plan::RollbackTransactionPlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for rolling back transactions
pub struct RollbackTransactionExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: RollbackTransactionPlanNode,
    executed: bool,
}

impl RollbackTransactionExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: RollbackTransactionPlanNode) -> Self {
        debug!("Creating RollbackTransactionExecutor");
        Self {
            context,
            plan,
            executed: false,
        }
    }
}

impl AbstractExecutor for RollbackTransactionExecutor {
    fn init(&mut self) {
        debug!("Initializing RollbackTransactionExecutor");
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            return Ok(None);
        }

        debug!("Executing transaction rollback");
        self.executed = true;

        // Get transaction information from the context
        let txn_context = self.context.read().get_transaction_context();
        let transaction_id = txn_context.get_transaction_id();

        // Log whether this is a chained rollback
        if self.plan.is_chain() {
            debug!("Transaction {} rollback with chain option", transaction_id);
        }

        // Log whether this is a partial rollback to a savepoint
        if let Some(savepoint) = self.plan.get_savepoint() {
            debug!(
                "Transaction {} rollback to savepoint '{}'",
                transaction_id, savepoint
            );

            // TODO: Implement partial rollback to savepoint
            // This would require savepoint functionality in the transaction system
            warn!("Partial rollback to savepoint not yet implemented");
        } else {
            debug!("Transaction {} full rollback", transaction_id);
        }

        info!("Transaction {} rollback operation prepared", transaction_id);

        // The actual rollback will be performed by the execution engine
        // We just need to prepare for potential chaining after rollback
        if self.plan.is_chain() {
            // Store the information needed to chain transactions in the context
            debug!("Preparing for transaction chaining after rollback");
            let mut context_write = self.context.write();
            context_write.set_chain_after_transaction(true);
        }

        // Return Ok(None) as transaction operations don't produce tuples
        Ok(None)
    }

    fn get_output_schema(&self) -> &Schema {
        // Transaction operations don't have output schemas
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
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use parking_lot::RwLock;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Use the transaction manager to create a transaction instead of hardcoding ID 0
            let transaction = transaction_manager
                .begin(IsolationLevel::ReadUncommitted)
                .unwrap();
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

    #[tokio::test]
    async fn test_rollback_transaction_executor() {
        let test_context = TestContext::new("rollback_transaction_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a rollback transaction plan without chain or savepoint
        let plan = RollbackTransactionPlanNode::new(false, None);

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that the transaction ID remains the same (rollback doesn't change transaction ID)
        let current_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_eq!(
            current_txn_id, original_txn_id,
            "Transaction ID should remain the same during rollback"
        );

        // Execute again - should return None as it's already executed
        let result = executor.next();
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_rollback_transaction_with_chain() {
        let test_context = TestContext::new("rollback_transaction_chain_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a rollback transaction plan with chain flag
        let plan = RollbackTransactionPlanNode::new(true, None);

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that the transaction ID remains the same
        let current_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_eq!(
            current_txn_id, original_txn_id,
            "Transaction ID should remain the same during rollback"
        );

        // Verify that chain flag was set in the context
        let chain_flag = exec_context.read().should_chain_after_transaction();
        assert!(chain_flag, "Chain flag should be set in execution context");
    }

    #[tokio::test]
    async fn test_rollback_transaction_with_savepoint() {
        let test_context = TestContext::new("rollback_transaction_savepoint_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a rollback transaction plan with savepoint
        let savepoint_name = "test_savepoint".to_string();
        let plan = RollbackTransactionPlanNode::new(false, Some(savepoint_name.clone()));

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that the transaction ID remains the same
        let current_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_eq!(
            current_txn_id, original_txn_id,
            "Transaction ID should remain the same during rollback"
        );

        // Verify that chain flag is not set (since chain was false)
        let chain_flag = exec_context.read().should_chain_after_transaction();
        assert!(
            !chain_flag,
            "Chain flag should not be set when chain is false"
        );
    }

    #[tokio::test]
    async fn test_rollback_transaction_with_savepoint_and_chain() {
        let test_context = TestContext::new("rollback_transaction_savepoint_chain_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a rollback transaction plan with both savepoint and chain
        let savepoint_name = "test_savepoint".to_string();
        let plan = RollbackTransactionPlanNode::new(true, Some(savepoint_name.clone()));

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that the transaction ID remains the same
        let current_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_eq!(
            current_txn_id, original_txn_id,
            "Transaction ID should remain the same during rollback"
        );

        // Verify that chain flag was set in the context
        let chain_flag = exec_context.read().should_chain_after_transaction();
        assert!(
            chain_flag,
            "Chain flag should be set in execution context when chain is true"
        );
    }

    #[tokio::test]
    async fn test_rollback_executor_output_schema() {
        let test_context = TestContext::new("rollback_executor_schema_test").await;
        let exec_context = test_context.create_execution_context();

        // Create a rollback transaction plan
        let plan = RollbackTransactionPlanNode::new(false, None);

        // Create the executor
        let executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Verify output schema is empty
        let schema = executor.get_output_schema();
        assert_eq!(
            schema.get_columns().len(),
            0,
            "Rollback executor should have empty output schema"
        );
    }

    #[tokio::test]
    async fn test_rollback_executor_context() {
        let test_context = TestContext::new("rollback_executor_context_test").await;
        let exec_context = test_context.create_execution_context();

        // Create a rollback transaction plan
        let plan = RollbackTransactionPlanNode::new(false, None);

        // Create the executor
        let executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Verify executor context is the same as the one passed in
        let executor_context = executor.get_executor_context();
        assert!(
            Arc::ptr_eq(&exec_context, &executor_context),
            "Executor context should be the same as input context"
        );
    }

    #[tokio::test]
    async fn test_multiple_rollback_executions() {
        let test_context = TestContext::new("multiple_rollback_test").await;
        let exec_context = test_context.create_execution_context();

        // Create a rollback transaction plan
        let plan = RollbackTransactionPlanNode::new(false, None);

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize
        executor.init();

        // First execution should work
        let result1 = executor.next();
        assert!(
            result1.unwrap().is_none(),
            "First execution should return None"
        );

        // Second execution should also return None (already executed)
        let result2 = executor.next();
        assert!(
            result2.unwrap().is_none(),
            "Second execution should return None"
        );

        // Third execution should also return None
        let result3 = executor.next();
        assert!(
            result3.unwrap().is_none(),
            "Third execution should return None"
        );
    }

    #[tokio::test]
    async fn test_rollback_different_savepoint_names() {
        let savepoint_names = vec![
            "savepoint1".to_string(),
            "sp_test".to_string(),
            "checkpoint_a".to_string(),
            "nested_sp".to_string(),
        ];

        for savepoint_name in savepoint_names {
            let test_context =
                TestContext::new(&format!("rollback_savepoint_{}_test", savepoint_name)).await;
            let exec_context = test_context.create_execution_context();

            // Store the original transaction ID
            let original_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();

            // Create a rollback transaction plan with this savepoint
            let plan = RollbackTransactionPlanNode::new(false, Some(savepoint_name.clone()));

            // Create the executor
            let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

            // Initialize and execute
            executor.init();
            let result = executor.next();

            // Should return None (no output data)
            assert!(
                result.unwrap().is_none(),
                "Rollback to savepoint '{}' should return None",
                savepoint_name
            );

            // Verify that the transaction ID remains the same
            let current_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();
            assert_eq!(
                current_txn_id, original_txn_id,
                "Transaction ID should remain the same for savepoint '{}'",
                savepoint_name
            );
        }
    }

    #[tokio::test]
    async fn test_rollback_all_parameter_combinations() {
        // Test all combinations of chain and savepoint parameters
        let test_cases = vec![
            (false, None),
            (false, Some("sp1".to_string())),
            (true, None),
            (true, Some("sp2".to_string())),
        ];

        for (chain, savepoint) in test_cases {
            let test_name = format!(
                "rollback_combo_chain_{}_savepoint_{}_test",
                chain,
                savepoint.as_ref().map_or("none", |s| s)
            );
            let test_context = TestContext::new(&test_name).await;
            let exec_context = test_context.create_execution_context();

            // Store the original transaction ID
            let original_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();

            // Create a rollback transaction plan with these parameters
            let plan = RollbackTransactionPlanNode::new(chain, savepoint.clone());

            // Create the executor
            let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

            // Initialize and execute
            executor.init();
            let result = executor.next();

            // Should return None (no output data)
            assert!(
                result.unwrap().is_none(),
                "Rollback with chain={}, savepoint={:?} should return None",
                chain,
                savepoint
            );

            // Verify that the transaction ID remains the same
            let current_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();
            assert_eq!(
                current_txn_id, original_txn_id,
                "Transaction ID should remain the same for chain={}, savepoint={:?}",
                chain, savepoint
            );

            // Verify chain flag in context
            let chain_flag = exec_context.read().should_chain_after_transaction();
            assert_eq!(
                chain_flag, chain,
                "Chain flag should match input for chain={}, savepoint={:?}",
                chain, savepoint
            );
        }
    }

    #[tokio::test]
    async fn test_concurrent_rollback_executions() {
        // This test verifies that multiple rollback executors can run concurrently
        let test_context = TestContext::new("concurrent_rollback_test").await;

        // Number of threads to create
        const NUM_THREADS: usize = 5;

        // Create a barrier to synchronize thread start
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));

        // Create multiple threads, each executing a rollback
        let mut handles = Vec::with_capacity(NUM_THREADS);
        let results = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(NUM_THREADS)));

        for i in 0..NUM_THREADS {
            let thread_barrier = barrier.clone();
            let thread_results = results.clone();
            let thread_test_context =
                TestContext::new(&format!("concurrent_rollback_thread_{}_test", i)).await;

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                thread_barrier.wait();

                let exec_context = thread_test_context.create_execution_context();
                let original_txn_id = exec_context
                    .read()
                    .get_transaction_context()
                    .get_transaction_id();

                // Create a rollback plan with different parameters for each thread
                let chain = i % 2 == 0;
                let savepoint = if i % 3 == 0 {
                    Some(format!("sp_{}", i))
                } else {
                    None
                };
                let plan = RollbackTransactionPlanNode::new(chain, savepoint);

                // Create and execute the rollback
                let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);
                executor.init();
                let result = executor.next();

                // Small sleep to simulate some work
                thread::sleep(Duration::from_millis(10));

                // Record the result
                let current_txn_id = exec_context
                    .read()
                    .get_transaction_context()
                    .get_transaction_id();
                let chain_flag = exec_context.read().should_chain_after_transaction();

                thread_results.lock().push((
                    result.unwrap().is_none(),
                    original_txn_id == current_txn_id,
                    chain_flag == chain,
                ));
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all results
        let final_results = results.lock().clone();
        assert_eq!(
            final_results.len(),
            NUM_THREADS,
            "All threads should have completed"
        );

        for (i, (returned_none, txn_id_unchanged, chain_flag_correct)) in
            final_results.iter().enumerate()
        {
            assert!(returned_none, "Thread {} should have returned None", i);
            assert!(
                txn_id_unchanged,
                "Thread {} should have unchanged transaction ID",
                i
            );
            assert!(
                chain_flag_correct,
                "Thread {} should have correct chain flag",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_rollback_executor_initialization() {
        let test_context = TestContext::new("rollback_executor_init_test").await;
        let exec_context = test_context.create_execution_context();

        // Create a rollback transaction plan
        let plan = RollbackTransactionPlanNode::new(true, Some("test_sp".to_string()));

        // Create the executor
        let mut executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Before initialization, next() should still work (init is optional)
        let result_before_init = executor.next();
        assert!(
            result_before_init.unwrap().is_none(),
            "Should return None even before explicit init"
        );

        // After execution, init should not change behavior
        executor.init();
        let result_after_init = executor.next();
        assert!(
            result_after_init.unwrap().is_none(),
            "Should return None after init when already executed"
        );
    }

    #[tokio::test]
    async fn test_rollback_executor_plan_access() {
        let test_context = TestContext::new("rollback_executor_plan_access_test").await;
        let exec_context = test_context.create_execution_context();

        // Create a rollback transaction plan with specific parameters
        let chain = true;
        let savepoint = Some("access_test_sp".to_string());
        let plan = RollbackTransactionPlanNode::new(chain, savepoint.clone());

        // Create the executor
        let executor = RollbackTransactionExecutor::new(exec_context.clone(), plan);

        // Access the plan through the executor's schema (indirect access)
        let schema = executor.get_output_schema();
        assert_eq!(
            schema.get_columns().len(),
            0,
            "Schema should be empty for transaction operations"
        );

        // The executor should maintain the plan's properties internally
        // (We can't directly access the plan from the executor, but we can test its behavior)
    }
}
