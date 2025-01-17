use crate::catalog::catalog::Catalog;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::recovery::log_manager::LogManager;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct TransactionManagerFactory {
    transaction_manager: Arc<RwLock<TransactionManager>>,
    lock_manager: Arc<LockManager>,
    catalog: Arc<RwLock<Catalog>>,
    log_manager: Arc<RwLock<LogManager>>,
}

impl TransactionManagerFactory {
    pub fn new(catalog: Arc<RwLock<Catalog>>, log_manager: Arc<RwLock<LogManager>>) -> Self {
        let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
            catalog.clone(),
            log_manager.clone(),
        )));

        let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

        Self {
            transaction_manager,
            lock_manager,
            catalog,
            log_manager,
        }
    }

    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
        let txn = self.transaction_manager.write().begin(isolation_level);

        Arc::new(TransactionContext::new(
            txn,
            self.lock_manager.clone(),
            self.transaction_manager.clone(),
        ))
    }

    pub fn commit_transaction(&self, ctx: Arc<TransactionContext>) -> bool {
        self.transaction_manager
            .write()
            .commit(ctx.get_transaction())
    }

    pub fn abort_transaction(&self, ctx: Arc<TransactionContext>) {
        self.transaction_manager
            .write()
            .abort(ctx.get_transaction())
    }

    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    pub fn get_transaction_manager(&self) -> Arc<RwLock<TransactionManager>> {
        self.transaction_manager.clone()
    }
}
