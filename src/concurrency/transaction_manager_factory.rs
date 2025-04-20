use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::transaction_context::TransactionContext;
use std::sync::Arc;

pub struct TransactionManagerFactory {
    transaction_manager: Arc<TransactionManager>,
    lock_manager: Arc<LockManager>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    wal_manager: Option<Arc<WALManager>>,
}

impl TransactionManagerFactory {
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>) -> Self {
        let transaction_manager = Arc::new(TransactionManager::new());
        let lock_manager = Arc::new(LockManager::new());

        Self {
            transaction_manager,
            lock_manager,
            buffer_pool_manager,
            wal_manager: None,
        }
    }

    pub fn with_wal_manager(
        buffer_pool_manager: Arc<BufferPoolManager>,
        wal_manager: Arc<WALManager>,
    ) -> Self {
        let transaction_manager = Arc::new(TransactionManager::new());
        let lock_manager = Arc::new(LockManager::new());

        Self {
            transaction_manager,
            lock_manager,
            buffer_pool_manager,
            wal_manager: Some(wal_manager),
        }
    }

    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
        let txn = self.transaction_manager.begin(isolation_level).unwrap();

        // Write begin record to WAL if WAL manager is available
        if let Some(wal_manager) = &self.wal_manager {
            let lsn = wal_manager.write_begin_record(txn.as_ref());
            txn.set_prev_lsn(lsn);
        }

        Arc::new(TransactionContext::new(
            txn,
            self.lock_manager.clone(),
            self.transaction_manager.clone(),
        ))
    }

    pub fn commit_transaction(&self, ctx: Arc<TransactionContext>) -> bool {
        self.transaction_manager
            .commit(ctx.get_transaction(), self.buffer_pool_manager.clone())
    }

    pub fn abort_transaction(&self, ctx: Arc<TransactionContext>) {
        self.transaction_manager.abort(ctx.get_transaction())
    }

    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    pub fn get_transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
    }
}
