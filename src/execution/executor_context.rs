use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::{CheckOption, CheckOptions};
use crate::execution::executors::abstract_executor::AbstractExecutor;
use log::debug;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::Arc;

pub struct ExecutorContext {
    transaction: Arc<Transaction>,
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
    lock_manager: Arc<LockManager>,
    nlj_check_exec_set: VecDeque<(Box<dyn AbstractExecutor>, Box<dyn AbstractExecutor>)>,
    check_options: Arc<CheckOptions>,
    is_delete: bool,
}

impl ExecutorContext {
    pub fn new(
        transaction: Arc<Transaction>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        debug!(
            "Creating ExecutorContext for transaction {}",
            transaction.get_transaction_id()
        );

        Self {
            transaction,
            catalog,
            buffer_pool_manager,
            transaction_manager,
            lock_manager,
            nlj_check_exec_set: VecDeque::new(),
            check_options: Arc::new(CheckOptions::new()),
            is_delete: false,
        }
    }

    pub fn get_transaction(&self) -> Arc<Transaction> {
        Arc::clone(&self.transaction)
    }

    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
    }

    pub fn get_buffer_pool_manager(&self) -> &BufferPoolManager {
        &self.buffer_pool_manager
    }

    pub fn get_transaction_manager(&self) -> &Arc<RwLock<TransactionManager>> {
        &self.transaction_manager
    }

    pub fn get_lock_manager(&self) -> &LockManager {
        self.lock_manager.as_ref()
    }

    pub fn get_nlj_check_exec_set(
        &self,
    ) -> &VecDeque<(Box<dyn AbstractExecutor>, Box<dyn AbstractExecutor>)> {
        &self.nlj_check_exec_set
    }

    pub fn get_check_options(&self) -> Arc<CheckOptions> {
        Arc::clone(&self.check_options)
    }

    pub fn set_check_options(&mut self, options: CheckOptions) {
        debug!("Setting check options");
        self.check_options = Arc::new(options);
    }

    pub fn add_check_option(
        &mut self,
        left_exec: Box<dyn AbstractExecutor>,
        right_exec: Box<dyn AbstractExecutor>,
    ) {
        self.nlj_check_exec_set.push_back((left_exec, right_exec));
    }

    pub fn init_check_options(&mut self) {
        // Initialize check options based on the current state
        let mut options = CheckOptions::new();

        // Add default checks if needed
        if !self.nlj_check_exec_set.is_empty() {
            options.add_check(CheckOption::EnableNljCheck);
        }

        self.check_options = Arc::new(options);
    }

    pub fn is_delete(&self) -> bool {
        self.is_delete
    }

    pub fn set_delete(&mut self, is_delete: bool) {
        self.is_delete = is_delete;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::executors::mock_executor::MockExecutor;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    struct TestContext {
        transaction: Arc<Transaction>,
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            // Create temporary file paths
            let timestamp = chrono::Utc::now().timestamp();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            // Initialize components
            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));
            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                10,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));
            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool_manager.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                catalog.clone(),
                log_manager,
            )));

            // Create test transaction
            let transaction = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
            let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

            Self {
                transaction,
                catalog,
                buffer_pool_manager,
                transaction_manager,
                lock_manager,
                db_file,
                log_file,
            }
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_executor_context_creation() {
        let ctx = TestContext::new("executor_creation");

        let executor_context = ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        );

        assert_eq!(executor_context.get_transaction().get_transaction_id(), 1);
        assert!(!executor_context.is_delete());
        assert!(executor_context.get_nlj_check_exec_set().is_empty());
    }

    #[test]
    fn test_check_options_management() {
        let ctx = TestContext::new("check_options");

        let mut executor_context = ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        );

        // Test initial state
        assert!(executor_context.get_check_options().is_empty());

        // Test setting new options
        let mut new_options = CheckOptions::new();
        new_options.add_check(CheckOption::EnableNljCheck);
        executor_context.set_check_options(new_options);

        assert!(executor_context
            .get_check_options()
            .has_check(&CheckOption::EnableNljCheck));
    }

    #[test]
    fn test_nlj_check_executor_management() {
        let ctx = TestContext::new("nlj_executors");

        let mut executor_context = Arc::new(RwLock::new(ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        )));

        // Add mock executors
        let left_exec = Box::new(MockExecutor::new(
            vec![],
            Default::default(),
            executor_context.clone(),
        ));
        let right_exec = Box::new(MockExecutor::new(
            vec![],
            Default::default(),
            executor_context.clone(),
        ));

        let mut executor_context_guard = executor_context.write();
        executor_context_guard.add_check_option(left_exec, right_exec);

        assert_eq!(executor_context_guard.get_nlj_check_exec_set().len(), 1);

        // Initialize check options
        executor_context_guard.init_check_options();
        assert!(executor_context_guard
            .get_check_options()
            .has_check(&CheckOption::EnableNljCheck));
    }

    #[test]
    fn test_delete_flag_management() {
        let ctx = TestContext::new("delete_flag");

        let mut executor_context = ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        );

        assert!(!executor_context.is_delete());

        executor_context.set_delete(true);
        assert!(executor_context.is_delete());

        executor_context.set_delete(false);
        assert!(!executor_context.is_delete());
    }

    #[test]
    fn test_component_access() {
        let ctx = TestContext::new("component_access");

        let executor_context = ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        );

        // Test access to components
        assert_eq!(
            Arc::as_ptr(&executor_context.get_transaction()),
            Arc::as_ptr(&ctx.transaction)
        );
        assert_eq!(
            Arc::as_ptr(executor_context.get_catalog()),
            Arc::as_ptr(&ctx.catalog)
        );
        assert_eq!(
            Arc::as_ptr(executor_context.get_transaction_manager()),
            Arc::as_ptr(&ctx.transaction_manager)
        );
    }

    #[test]
    fn test_check_options_initialization() {
        let ctx = TestContext::new("check_options_init");

        let mut executor_context = Arc::new(RwLock::new(ExecutorContext::new(
            Arc::clone(&ctx.transaction),
            Arc::clone(&ctx.transaction_manager),
            Arc::clone(&ctx.catalog),
            Arc::clone(&ctx.buffer_pool_manager),
            Arc::clone(&ctx.lock_manager),
        )));

        // Add executors and initialize
        let left_exec = Box::new(MockExecutor::new(
            vec![],
            Default::default(),
            executor_context.clone(),
        ));
        let right_exec = Box::new(MockExecutor::new(
            vec![],
            Default::default(),
            executor_context.clone(),
        ));

        let mut executor_context_guard = executor_context.write();
        executor_context_guard.add_check_option(left_exec, right_exec);
        executor_context_guard.init_check_options();

        // Verify NLJ check is enabled
        assert!(executor_context_guard
            .get_check_options()
            .has_check(&CheckOption::EnableNljCheck));

        // Clear executors and reinitialize
        executor_context_guard.nlj_check_exec_set.clear();
        executor_context_guard.init_check_options();

        // Verify NLJ check is not enabled when no executors present
        assert!(!executor_context_guard
            .get_check_options()
            .has_check(&CheckOption::EnableNljCheck));
    }
}
