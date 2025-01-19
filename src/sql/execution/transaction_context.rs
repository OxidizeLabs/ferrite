use crate::common::config::{TableOidT, TxnId};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct TransactionContext {
    transaction: Arc<Transaction>,
    lock_manager: Arc<LockManager>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
    write_set: Mutex<Vec<(TableOidT, RID)>>, // Make write_set thread-safe
}

impl TransactionContext {
    pub fn new(
        transaction: Arc<Transaction>,
        lock_manager: Arc<LockManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
    ) -> Self {
        Self {
            transaction,
            lock_manager,
            transaction_manager,
            write_set: Mutex::new(Vec::new()),
        }
    }

    pub fn get_transaction_id(&self) -> TxnId {
        self.transaction.get_transaction_id()
    }

    pub fn get_transaction(&self) -> Arc<Transaction> {
        self.transaction.clone()
    }

    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    pub fn get_transaction_manager(&self) -> Arc<RwLock<TransactionManager>> {
        self.transaction_manager.clone()
    }

    /// Thread-safe method to append to write set
    pub fn append_write_set_atomic(&self, table_oid: TableOidT, rid: RID) {
        let mut write_set = self.write_set.lock();
        if !write_set.contains(&(table_oid, rid)) {
            write_set.push((table_oid, rid));
        }
    }

    /// Get all write operations performed in this transaction
    pub fn get_write_set(&self) -> Vec<(TableOidT, RID)> {
        self.write_set.lock().clone()
    }
}
