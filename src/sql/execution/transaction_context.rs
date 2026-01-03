//! # Transaction Context
//!
//! This module provides the [`TransactionContext`] struct, a convenience wrapper that bundles
//! together all transaction-related components needed during query execution. It acts as a
//! handle to the current transaction and its associated concurrency control infrastructure.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                           Transaction Context                                   │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌─────────────────────────────────────────────────────────────────────────┐   │
//! │   │                         TransactionContext                              │   │
//! │   ├─────────────────────────────────────────────────────────────────────────┤   │
//! │   │                                                                         │   │
//! │   │  ┌─────────────────────┐  ┌────────────────────┐  ┌──────────────────┐  │   │
//! │   │  │   Arc<Transaction>  │  │  Arc<LockManager>  │  │ Arc<TxnManager>  │  │   │
//! │   │  │                     │  │                    │  │                  │  │   │
//! │   │  │  - txn_id           │  │  - row locks       │  │  - begin()       │  │   │
//! │   │  │  - state            │  │  - table locks     │  │  - commit()      │  │   │
//! │   │  │  - isolation_level  │  │  - deadlock detect │  │  - abort()       │  │   │
//! │   │  │  - write_set        │  │  - wait-for graph  │  │  - GC            │  │   │
//! │   │  └─────────────────────┘  └────────────────────┘  └──────────────────┘  │   │
//! │   │                                                                         │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                                                                 │
//! │   Used by:                                                                      │
//! │   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐      │
//! │   │  Executor Nodes  │  │  DML Operations  │  │  Constraint Validation   │      │
//! │   │  (read/write)    │  │  (INSERT/UPDATE) │  │  (FK checks, etc.)       │      │
//! │   └──────────────────┘  └──────────────────┘  └──────────────────────────┘      │
//! │                                                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component | Type | Purpose |
//! |-----------|------|---------|
//! | `transaction` | `Arc<Transaction>` | Current transaction state, ID, isolation level, write set |
//! | `lock_manager` | `Arc<LockManager>` | Row and table lock acquisition/release |
//! | `transaction_manager` | `Arc<TransactionManager>` | Transaction lifecycle (begin, commit, abort) |
//!
//! ## Write Set Tracking
//!
//! The context provides thread-safe access to the transaction's **write set** — a record
//! of all (table_oid, RID) pairs modified by the transaction. This is used for:
//!
//! - **Undo on abort**: Rollback all written tuples
//! - **Visibility checks**: Determine what other transactions can see
//! - **Lock escalation**: Track scope of modifications
//!
//! ```text
//! Write Set Example:
//!
//!   Transaction T1:
//!   ┌────────────┬──────────────┐
//!   │ table_oid  │     RID      │
//!   ├────────────┼──────────────┤
//!   │     1      │ (page=5, 0)  │  ← INSERT into users
//!   │     1      │ (page=5, 1)  │  ← INSERT into users
//!   │     2      │ (page=3, 7)  │  ← UPDATE orders
//!   └────────────┴──────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - All components are wrapped in `Arc` for shared ownership across threads
//! - `append_write_set_atomic()` uses internal synchronization in `Transaction`
//! - Multiple executor threads can safely share the same `TransactionContext`
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use tkdb::sql::execution::transaction_context::TransactionContext;
//!
//! // Create context during query execution setup
//! let txn_context = TransactionContext::new(
//!     transaction,
//!     lock_manager,
//!     transaction_manager,
//! );
//!
//! // Access transaction ID
//! let txn_id = txn_context.get_transaction_id();
//!
//! // Record a write operation
//! txn_context.append_write_set_atomic(table_oid, rid);
//!
//! // Get all writes for undo/commit processing
//! let writes = txn_context.get_write_set();
//! ```
//!
//! ## Isolation Level Independence
//!
//! The `TransactionContext` works with any isolation level — the isolation semantics
//! are enforced by the underlying `Transaction` and `LockManager`, not by this wrapper.

use std::sync::Arc;

use crate::common::config::{TableOidT, TxnId};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;

/// A convenience wrapper bundling transaction-related components for query execution.
///
/// Provides a unified handle to the current transaction, lock manager, and transaction
/// manager. This context is passed through the executor tree during query execution
/// to provide access to concurrency control infrastructure.
///
/// See the module-level documentation for architecture diagrams and detailed usage.
///
/// # Thread Safety
///
/// All components are wrapped in `Arc` for shared ownership. The context itself
/// can be cloned and shared across executor threads safely.
#[derive(Debug)]
pub struct TransactionContext {
    /// The current transaction being executed.
    ///
    /// Contains transaction ID, state, isolation level, and write set.
    transaction: Arc<Transaction>,

    /// The lock manager for acquiring row and table locks.
    ///
    /// Used by executors to acquire locks before reading/writing tuples.
    lock_manager: Arc<LockManager>,

    /// The transaction manager for lifecycle operations.
    ///
    /// Provides access to begin, commit, abort, and MVCC operations.
    transaction_manager: Arc<TransactionManager>,
}

impl TransactionContext {
    /// Creates a new transaction context with the given components.
    ///
    /// # Parameters
    /// - `transaction`: The transaction this context wraps.
    /// - `lock_manager`: The lock manager for concurrency control.
    /// - `transaction_manager`: The transaction manager for lifecycle operations.
    ///
    /// # Returns
    /// A new `TransactionContext` ready for use during query execution.
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

    /// Returns the ID of the current transaction.
    ///
    /// This is a convenience method that delegates to the underlying transaction.
    ///
    /// # Returns
    /// The unique transaction ID.
    pub fn get_transaction_id(&self) -> TxnId {
        self.transaction.get_transaction_id()
    }

    /// Returns a clone of the transaction `Arc`.
    ///
    /// Use this to access transaction state, isolation level, or other properties.
    ///
    /// # Returns
    /// An `Arc<Transaction>` pointing to the same transaction.
    pub fn get_transaction(&self) -> Arc<Transaction> {
        self.transaction.clone()
    }

    /// Returns a clone of the lock manager `Arc`.
    ///
    /// Use this to acquire or release locks on rows and tables.
    ///
    /// # Returns
    /// An `Arc<LockManager>` pointing to the shared lock manager.
    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    /// Returns a clone of the transaction manager `Arc`.
    ///
    /// Use this for transaction lifecycle operations (commit, abort) or
    /// MVCC-related queries (undo logs, version chains).
    ///
    /// # Returns
    /// An `Arc<TransactionManager>` pointing to the shared transaction manager.
    pub fn get_transaction_manager(&self) -> Arc<TransactionManager> {
        self.transaction_manager.clone()
    }

    /// Records a write operation in the transaction's write set (thread-safe).
    ///
    /// The write set tracks all (table_oid, RID) pairs modified by this transaction.
    /// This information is used for:
    /// - Rolling back changes on abort
    /// - Flushing dirty pages on commit
    /// - MVCC visibility checks
    ///
    /// # Parameters
    /// - `table_oid`: The OID of the table being modified.
    /// - `rid`: The record ID of the tuple being modified.
    ///
    /// # Thread Safety
    /// This method is safe to call from multiple threads concurrently.
    /// Internal synchronization is handled by the `Transaction`.
    pub fn append_write_set_atomic(&self, table_oid: TableOidT, rid: RID) {
        self.get_transaction().append_write_set(table_oid, rid);
    }

    /// Returns all write operations performed in this transaction.
    ///
    /// # Returns
    /// A vector of (table_oid, RID) pairs representing all tuples modified
    /// by this transaction. Each entry appears at most once (duplicates are
    /// automatically filtered by the underlying `Transaction`).
    ///
    /// # Use Cases
    /// - Commit: Flush pages containing these RIDs
    /// - Abort: Undo changes to these tuples
    /// - Debugging: Inspect transaction footprint
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
        let txn = ctx
            .txn_manager
            .begin(IsolationLevel::ReadUncommitted)
            .unwrap();
        let txn_context =
            TransactionContext::new(txn, ctx.lock_manager.clone(), ctx.txn_manager.clone());

        // Operations should work regardless of isolation level
        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        assert_eq!(txn_context.get_write_set().len(), 1);

        // Test with Serializable
        let txn = ctx.txn_manager.begin(IsolationLevel::Serializable).unwrap();
        let txn_context =
            TransactionContext::new(txn, ctx.lock_manager.clone(), ctx.txn_manager.clone());

        txn_context.append_write_set_atomic(1, RID::new(1, 1));
        assert_eq!(txn_context.get_write_set().len(), 1);
    }
}
