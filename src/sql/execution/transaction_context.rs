use crate::common::config::{TableOidT, TxnId};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug)]
pub struct TransactionContext {
    transaction: Arc<Transaction>,
    lock_manager: Arc<LockManager>,
    transaction_manager: Arc<TransactionManager>,
    write_set: Mutex<Vec<(TableOidT, RID)>>, // Make write_set thread-safe
}

impl TransactionContext {
    pub fn new(
        transaction: Arc<Transaction>,
        lock_manager: Arc<LockManager>,
        transaction_manager: Arc<TransactionManager>,
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

    pub fn get_transaction_manager(&self) -> Arc<TransactionManager> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        txn_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
        _temp_dir: TempDir, // Keep temp dir alive
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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

            // Create buffer pool
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));

            // Create log manager
            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));

            // Create transaction manager
            let txn_manager = Arc::new(TransactionManager::new(log_manager));

            // Create lock manager
            let lock_manager = Arc::new(LockManager::new(txn_manager.clone()));

            Self {
                txn_manager,
                lock_manager,
                _temp_dir: temp_dir,
            }
        }

        fn create_transaction(&self) -> Arc<Transaction> {
            self.txn_manager
                .begin(IsolationLevel::ReadCommitted)
                .unwrap()
        }

        fn create_transaction_context(&self) -> TransactionContext {
            let txn = self.create_transaction();
            TransactionContext::new(txn, self.lock_manager.clone(), self.txn_manager.clone())
        }
    }

    #[test]
    fn test_new_transaction_context() {
        let ctx = TestContext::new("test_new_transaction_context");
        let txn_context = ctx.create_transaction_context();

        assert_eq!(txn_context.get_transaction_id(), 0); // First transaction has ID 0
        assert!(Arc::ptr_eq(
            &txn_context.get_lock_manager(),
            &ctx.lock_manager
        ));
        assert!(Arc::ptr_eq(
            &txn_context.get_transaction_manager(),
            &ctx.txn_manager
        ));
        assert!(txn_context.get_write_set().is_empty());
    }

    #[test]
    fn test_write_set_operations() {
        let ctx = TestContext::new("test_write_set_operations");
        let txn_context = ctx.create_transaction_context();

        // Test appending to write set
        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        txn_context.append_write_set_atomic(2, RID::new(2, 1));

        let write_set = txn_context.get_write_set();
        assert_eq!(write_set.len(), 2);
        assert!(write_set.contains(&(1, RID::new(1, 1))));
        assert!(write_set.contains(&(2, RID::new(2, 1))));
    }

    #[test]
    fn test_write_set_duplicate_prevention() {
        let ctx = TestContext::new("test_write_set_duplicate_prevention");
        let txn_context = ctx.create_transaction_context();

        // Add same entry twice
        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        txn_context.append_write_set_atomic(1, RID::new(1, 1));

        let write_set = txn_context.get_write_set();
        assert_eq!(write_set.len(), 1);
        assert_eq!(write_set[0], (1, RID::new(1, 1)));
    }

    #[test]
    fn test_concurrent_write_set_access() {
        use std::thread;

        let ctx = TestContext::new("test_concurrent_write_set_access");
        let txn_context = Arc::new(ctx.create_transaction_context());
        let mut handles = vec![];

        // Spawn multiple threads to concurrently append to write set
        for i in 0..5 {
            let txn_context = txn_context.clone();
            handles.push(thread::spawn(move || {
                txn_context.append_write_set_atomic(i as TableOidT, RID::new((i as u32).into(), 1));
            }));
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        let write_set = txn_context.get_write_set();
        assert_eq!(write_set.len(), 5);
    }
}
