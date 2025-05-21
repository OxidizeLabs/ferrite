use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::sql::execution::check_option::{CheckOption, CheckOptions};
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::transaction_context::TransactionContext;
use log::debug;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

pub struct ExecutionContext {
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    transaction_context: Arc<TransactionContext>,
    nlj_check_exec_set: VecDeque<(Box<dyn AbstractExecutor>, Box<dyn AbstractExecutor>)>,
    check_options: Arc<CheckOptions>,
    is_delete: bool,
    chain_after_transaction: bool,
}

impl ExecutionContext {
    pub fn new(
        buffer_pool_manager: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_context: Arc<TransactionContext>,
    ) -> Self {
        debug!(
            "Creating ExecutorContext for transaction {}",
            transaction_context.get_transaction_id()
        );

        // Create check options and enable necessary optimizations
        let mut options = CheckOptions::new();
        options.add_check(CheckOption::EnablePushdownCheck);
        options.add_check(CheckOption::EnableTopnCheck);

        Self {
            buffer_pool_manager,
            catalog,
            transaction_context,
            nlj_check_exec_set: VecDeque::new(),
            check_options: Arc::new(options),
            is_delete: false,
            chain_after_transaction: false,
        }
    }

    pub fn get_transaction_context(&self) -> Arc<TransactionContext> {
        self.transaction_context.clone()
    }

    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
    }

    pub fn get_buffer_pool_manager(&self) -> Arc<BufferPoolManager> {
        self.buffer_pool_manager.clone()
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
        let mut options = CheckOptions::new();

        if !self.nlj_check_exec_set.is_empty() {
            options.add_check(CheckOption::EnableNljCheck);
        }

        options.add_check(CheckOption::EnablePushdownCheck);
        options.add_check(CheckOption::EnableTopnCheck);

        self.check_options = Arc::new(options);
    }

    pub fn is_delete(&self) -> bool {
        self.is_delete
    }

    pub fn set_delete(&mut self, is_delete: bool) {
        self.is_delete = is_delete;
    }

    /// Sets a new transaction context
    pub fn set_transaction_context(&mut self, txn_ctx: Arc<TransactionContext>) {
        self.transaction_context = txn_ctx;
    }
    
    /// Gets whether transaction should be chained after commit/rollback
    pub fn should_chain_after_transaction(&self) -> bool {
        self.chain_after_transaction
    }
    
    /// Sets whether transaction should be chained after commit/rollback
    pub fn set_chain_after_transaction(&mut self, chain: bool) {
        self.chain_after_transaction = chain;
    }
}
