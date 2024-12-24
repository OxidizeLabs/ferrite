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
    transaction_manager: Arc<Mutex<TransactionManager>>,
    lock_manager: Arc<LockManager>,
    nlj_check_exec_set: VecDeque<(Box<dyn AbstractExecutor>, Box<dyn AbstractExecutor>)>,
    check_options: Arc<CheckOptions>,
    is_delete: bool,
}

impl ExecutorContext {
    pub fn new(
        transaction: Arc<Transaction>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
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

    pub fn get_transaction_manager(&self) -> &Arc<Mutex<TransactionManager>> {
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
