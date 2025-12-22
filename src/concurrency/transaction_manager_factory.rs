use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::transaction_context::TransactionContext;
use log::{error, warn};
use std::collections::HashSet;
use std::sync::Arc;

/// Coordinator for transaction-side effects: wraps TransactionManager state
/// transitions with WAL logging, buffer flush of pages dirtied by the txn,
/// and lock release via LockManager. Keeps TransactionManager focused on
/// state/MVCC while centralizing I/O and synchronization concerns here.

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
        // Preserve existing behavior by allocating a fresh transaction manager.
        Self::with_wal_manager_and_txn(
            buffer_pool_manager,
            wal_manager,
            Arc::new(TransactionManager::new()),
        )
    }

    /// Build a factory using an externally supplied TransactionManager so catalog
    /// registration and transaction lifecycle share the same instance.
    pub fn with_wal_manager_and_txn(
        buffer_pool_manager: Arc<BufferPoolManager>,
        wal_manager: Arc<WALManager>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        let lock_manager = Arc::new(LockManager::new());

        // wal_manager.force_run_flush_thread();

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

    pub async fn commit_transaction(&self, ctx: Arc<TransactionContext>) -> bool {
        let txn = ctx.get_transaction();

        // Write commit record before persisting data (WAL-first).
        if let Some(wal_manager) = &self.wal_manager {
            let lsn = wal_manager.write_commit_record(txn.as_ref());
            txn.set_prev_lsn(lsn);
        }

        // Transition txn state to committed and set commit timestamps/metadata.
        if !self
            .transaction_manager
            .commit(txn.clone(), self.buffer_pool_manager.clone())
            .await
        {
            // Best-effort lock release even on failure.
            if let Err(e) = self.lock_manager.force_release_txn(txn.get_transaction_id()) {
                warn!("Lock release failed after commit failure: {}", e);
            }
            return false;
        }

        // Flush only the pages dirtied by this transaction.
        let mut pages_to_flush = HashSet::new();
        for (_table_oid, rid) in txn.get_write_set() {
            pages_to_flush.insert(rid.get_page_id());
        }
        for page_id in pages_to_flush {
            if let Err(e) = self.buffer_pool_manager.flush_page_async(page_id).await {
                error!(
                    "Failed to flush page {} during transaction commit: {}",
                    page_id, e
                );
                if let Err(e) = self.lock_manager.force_release_txn(txn.get_transaction_id()) {
                    warn!("Lock release failed after flush error: {}", e);
                }
                return false;
            }
        }

        // Release locks now that commit finished and pages are flushed.
        if let Err(e) = self.lock_manager.force_release_txn(txn.get_transaction_id()) {
            warn!("Lock release failed after commit: {}", e);
        }

        true
    }

    pub fn abort_transaction(&self, ctx: Arc<TransactionContext>) {
        let txn = ctx.get_transaction();

        // Log abort if WAL is available.
        if let Some(wal_manager) = &self.wal_manager {
            let lsn = wal_manager.write_abort_record(txn.as_ref());
            txn.set_prev_lsn(lsn);
        }

        self.transaction_manager.abort(txn.clone());

        // Release locks after abort.
        if let Err(e) = self.lock_manager.force_release_txn(txn.get_transaction_id()) {
            warn!("Lock release failed after abort: {}", e);
        }
    }

    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    pub fn get_transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
    }
}
