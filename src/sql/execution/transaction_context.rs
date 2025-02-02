use crate::common::config::{TableOidT, TxnId};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use std::sync::Arc;

#[derive(Debug)]
pub struct TransactionContext {
    transaction: Arc<Transaction>,
    lock_manager: Arc<LockManager>,
    transaction_manager: Arc<TransactionManager>,
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
        self.get_transaction().append_write_set(table_oid, rid);
    }

    /// Get all write operations performed in this transaction
    pub fn get_write_set(&self) -> Vec<(TableOidT, RID)> {
        self.transaction.get_write_set()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::transaction::{IsolationLevel, TransactionState};

    struct TestContext {
        txn_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
    }

    impl TestContext {
        fn new() -> Self {
            // Create transaction manager
            let txn_manager = Arc::new(TransactionManager::new());

            // Create lock manager
            let lock_manager = Arc::new(LockManager::new());

            Self {
                txn_manager,
                lock_manager,
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
        let ctx = TestContext::new();
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
        let ctx = TestContext::new();
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
        let ctx = TestContext::new();
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

        let ctx = TestContext::new();
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

    #[test]
    fn test_transaction_state_handling() {
        let ctx = TestContext::new();
        let txn_context = ctx.create_transaction_context();
        let txn = txn_context.get_transaction();

        // Test initial state
        assert_eq!(txn.get_state(), TransactionState::Running);

        // Test write set operations in different states
        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        assert_eq!(txn_context.get_write_set().len(), 1);

        // Test after abort
        txn.set_state(TransactionState::Aborted);
        txn_context.append_write_set_atomic(2, RID::new(2, 1));
        // Write set should still work even with aborted transaction
        assert_eq!(txn_context.get_write_set().len(), 2);

        // Test after commit
        txn.set_state(TransactionState::Committed);
        txn_context.append_write_set_atomic(3, RID::new(3, 1));
        assert_eq!(txn_context.get_write_set().len(), 3);
    }

    #[test]
    fn test_isolation_levels() {
        let ctx = TestContext::new();

        // Test with different isolation levels
        let txn = ctx.txn_manager
            .begin(IsolationLevel::ReadUncommitted)
            .unwrap();
        let txn_context = TransactionContext::new(
            txn,
            ctx.lock_manager.clone(),
            ctx.txn_manager.clone(),
        );

        // Operations should work regardless of isolation level
        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        assert_eq!(txn_context.get_write_set().len(), 1);

        // Test with Serializable
        let txn = ctx.txn_manager
            .begin(IsolationLevel::Serializable)
            .unwrap();
        let txn_context = TransactionContext::new(
            txn,
            ctx.lock_manager.clone(),
            ctx.txn_manager.clone(),
        );

        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        assert_eq!(txn_context.get_write_set().len(), 1);
    }
}
