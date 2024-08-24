use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::CheckOptions;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use std::collections::VecDeque;
use std::fmt::Pointer;
use std::sync::Arc;

pub struct ExecutorContext {
    transaction: Arc<Transaction>,
    catalog: Arc<Catalog>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_manager: Arc<TransactionManager>,
    lock_manager: Arc<LockManager>,
    _nlj_check_exec_set: VecDeque<(AbstractExecutor, AbstractExecutor)>,
    _check_options: Arc<CheckOptions>,
    _is_delete: bool,
}

impl ExecutorContext {
    pub fn new(&self,
               transaction: Arc<Transaction>,
               transaction_manager: Arc<TransactionManager>,
               catalog: Arc<Catalog>,
               buffer_pool_manager: Arc<BufferPoolManager>,
               lock_manager: Arc<LockManager>,
    ) -> Self {
        Self {
            transaction,
            catalog,
            buffer_pool_manager,
            transaction_manager,
            lock_manager,
            _nlj_check_exec_set: Default::default(),
            _check_options: Arc::new(CheckOptions::default()),
            _is_delete: false,
        }
    }

    pub fn get_transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn get_catalogue(&self) -> &Catalog {
        &self.catalog
    }

    pub fn get_buffer_pool_manager(&self) -> &BufferPoolManager {
        &self.buffer_pool_manager
    }

    pub fn get_transaction_manager(&self) -> &TransactionManager {
        &self.transaction_manager
    }

    pub fn get_lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    pub fn get_nlj_check_exec_set(&self) -> &VecDeque<(AbstractExecutor, AbstractExecutor)> {
        &self._nlj_check_exec_set
    }

    pub fn get_check_options(&self) -> Arc<CheckOptions> {
        Arc::clone(&self._check_options)
    }

    pub fn add_check_option(&mut self, _left_exec: AbstractExecutor, _right_exec: AbstractExecutor) {
        self._nlj_check_exec_set.push_back((_left_exec, _right_exec))
    }

    pub fn init_check_options(&self) {
        todo!()
    }

    pub fn is_delete(&self) -> bool {
        self._is_delete
    }
}

