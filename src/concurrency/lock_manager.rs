use crate::common::config::{TableOidT, TxnId, INVALID_TXN_ID};
use crate::common::rid::RID;
use crate::concurrency::transaction::{Transaction, TransactionState};
use crate::concurrency::transaction_manager::TransactionManager;
use parking_lot::Mutex;
/// [LOCK_NOTE]
///
/// # General Behavior
/// - Both `lock_table` and `lock_row` are blocking methods; they should wait until the lock is granted and then return.
/// - If the transaction was aborted in the meantime, do not grant the lock and return `false`.
///
/// # Multiple Transactions
/// - `LockManager` should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
/// - If there are multiple compatible lock requests, all should be granted at the same time as long as FIFO is honored.
///
/// # Supported Lock Modes
/// - Table locking should support all lock modes.
/// - Row locking should not support Intention locks. Attempting this should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `ATTEMPTED_INTENTION_LOCK_ON_ROW`.
///
/// # Isolation Level
/// Depending on the isolation level, a transaction should attempt to take locks:
/// - Only if required, AND
/// - Only if allowed
///
/// For instance:
/// - S/IS/SIX locks are not required under `READ_UNCOMMITTED`, and any such attempt should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `LOCK_SHARED_ON_READ_UNCOMMITTED`.
/// - X/IX locks on rows are not allowed if the `TransactionState` is `SHRINKING`, and any such attempt should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `LOCK_ON_SHRINKING`.
///
/// ## Repeatable Read
/// - The transaction is required to take all locks.
/// - All locks are allowed in the `GROWING` state.
/// - No locks are allowed in the `SHRINKING` state.
///
/// ## Read Committed
/// - The transaction is required to take all locks.
/// - All locks are allowed in the `GROWING` state.
/// - Only IS and S locks are allowed in the `SHRINKING` state.
///
/// ## Read Uncommitted
/// - The transaction is required to take only IX and X locks.
/// - X and IX locks are allowed in the `GROWING` state.
/// - S, IS, and SIX locks are never allowed.
///
/// # Multilevel Locking
/// While locking rows, `lock` should ensure that the transaction has an appropriate lock on the table which the row belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either X, IX, or SIX on the table. If such a lock does not exist on the table, `lock` should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `TABLE_LOCK_NOT_PRESENT`.
///
/// # Lock Upgrade
/// Calling `lock` on a resource that is already locked should have the following behavior:
/// - If the requested lock mode is the same as that of the lock presently held, `lock` should return `true` since it already has the lock.
/// - If the requested lock mode is different, `lock` should upgrade the lock held by the transaction.
///
/// The lock upgrade should follow these general steps:
/// 1. Check the precondition of the upgrade.
/// 2. Drop the current lock, reserve the upgrade position.
/// 3. Wait to get the new lock granted.
///
/// A lock request being upgraded should be prioritized over other waiting lock requests on the same resource.
///
/// Only the following transitions are allowed during an upgrade:
/// - IS -> [S, X, IX, SIX]
/// - S -> [X, SIX]
/// - IX -> [X, SIX]
/// - SIX -> [X]
///
/// Any other upgrade is considered incompatible, and such an attempt should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `INCOMPATIBLE_UPGRADE`.
///
/// Furthermore, only one transaction should be allowed to upgrade its lock on a given resource. Multiple concurrent lock upgrades on the same resource should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `UPGRADE_CONFLICT`.
///
/// # Bookkeeping
/// If a lock is granted to a transaction, the lock manager should update its lock sets appropriately (check `transaction.rs`).
///
/// Consider which type of lock to directly apply on the table when implementing the executor later.

/// [UNLOCK_NOTE]
///
/// # General Behavior
/// - Both `unlock_table` and `unlock_row` should release the lock on the resource and return.
/// - Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock. If not, `LockManager` should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD`.
///
/// Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any row on that table. If the transaction holds locks on rows of the table, `unlock` should set the `TransactionState` as `ABORTED` and throw a `TransactionAbortException` with `TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS`.
///
/// Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
///
/// # Transaction State Update
/// `Unlock` should update the transaction state appropriately (depending upon the isolation level). Only unlocking S or X locks changes the transaction state.
///
/// ## Repeatable Read
/// - Unlocking S/X locks should set the transaction state to `SHRINKING`.
///
/// ## Read Committed
/// - Unlocking X locks should set the transaction state to `SHRINKING`.
/// - Unlocking S locks does not affect the transaction state.
///
/// ## Read Uncommitted
/// - Unlocking X locks should set the transaction state to `SHRINKING`.
/// - S locks are not permitted under `READ_UNCOMMITTED`. The behavior upon unlocking an S lock under this isolation level is undefined.
///
/// # Bookkeeping
/// After a resource is unlocked, the lock manager should update the transaction's lock sets appropriately (check `transaction.rs`).
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar};
use std::thread;
use crate::concurrency::lock_manager::LockMode::{Exclusive, IntentionExclusive, IntentionShared, Shared, SharedIntentionExclusive};
use crate::concurrency::transaction::IsolationLevel::ReadUncommitted;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
    IntentionShared,
    IntentionExclusive,
    SharedIntentionExclusive,
}

/// Structure to hold a lock request.
/// This could be a lock request on a table OR a row.
/// For table lock requests, the `rid` attribute would be unused.
pub struct LockRequest {
    txn_id: TxnId,
    lock_mode: LockMode,
    oid: TableOidT,
    rid: Option<RID>,
    granted: bool,
}

/// Structure to hold lock requests for the same resource (table or row).
pub struct LockRequestQueue {
    request_queue: VecDeque<Arc<Mutex<LockRequest>>>,
    cv: Condvar,
    upgrading: TxnId,
    latch: Mutex<()>,
}

/// LockManager handles transactions asking for locks on records.
pub struct LockManager {
    transaction_manager: Arc<Mutex<TransactionManager>>,
    table_lock_map: Mutex<HashMap<TableOidT, Arc<Mutex<LockRequestQueue>>>>,
    row_lock_map: Mutex<HashMap<RID, Arc<Mutex<LockRequestQueue>>>>,
    enable_cycle_detection: AtomicBool,
    cycle_detection_thread: Option<thread::JoinHandle<()>>,
    waits_for: Mutex<HashMap<TxnId, Vec<TxnId>>>,
}

impl LockRequest {
    /// Creates a new table lock request.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID requesting the lock.
    /// - `lock_mode`: The locking mode of the requested lock.
    /// - `oid`: The table OID.
    ///
    /// # Returns
    /// A new `LockRequest` instance for a table.
    pub fn new_table_request(txn_id: TxnId, lock_mode: LockMode, oid: TableOidT) -> Self {
        Self {
            txn_id,
            lock_mode,
            oid,
            rid: None,
            granted: false,
        }
    }

    /// Creates a new row lock request.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID requesting the lock.
    /// - `lock_mode`: The locking mode of the requested lock.
    /// - `oid`: The table OID.
    /// - `rid`: The row ID.
    ///
    /// # Returns
    /// A new `LockRequest` instance for a row.
    pub fn new_row_request(txn_id: TxnId, lock_mode: LockMode, oid: TableOidT, rid: RID) -> Self {
        Self {
            txn_id,
            lock_mode,
            oid,
            rid: Some(rid),
            granted: false,
        }
    }
}

impl LockRequestQueue {
    /// Creates a new `LockRequestQueue`.
    ///
    /// # Returns
    /// A new `LockRequestQueue` instance.
    pub fn new() -> Self {
        Self {
            request_queue: VecDeque::new(),
            cv: Condvar::new(),
            upgrading: INVALID_TXN_ID,
            latch: Mutex::new(()),
        }
    }
}

impl LockManager {
    /// Creates a new lock manager configured for the deadlock detection policy.
    ///
    /// # Parameters
    /// - `transaction_manager`: A reference to the transaction manager.
    ///
    /// # Returns
    /// A new `LockManager` instance.
    pub fn new(transaction_manager: Arc<Mutex<TransactionManager>>) -> Self {
        Self {
            transaction_manager,
            table_lock_map: Mutex::new(HashMap::new()),
            row_lock_map: Mutex::new(HashMap::new()),
            enable_cycle_detection: AtomicBool::new(false),
            cycle_detection_thread: None,
            waits_for: Mutex::new(HashMap::new()),
        }
    }

    /// Starts the deadlock detection process.
    pub fn start_deadlock_detection(&mut self) {
        use std::sync::atomic::Ordering;
        use std::thread;

        self.enable_cycle_detection.store(true, Ordering::SeqCst);
        let lock_manager = self.clone();

        self.cycle_detection_thread = Some(thread::spawn(move || {
            lock_manager.run_cycle_detection();
        }));
    }

    /// Acquires a lock on a table in the given lock mode.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock upgrade.
    /// - `lock_mode`: The lock mode for the requested lock.
    /// - `oid`: The table OID of the table to be locked.
    ///
    /// # Returns
    /// `true` if the lock is successfully acquired or upgraded, `false` otherwise.
    ///
    /// # Errors
    /// This method should abort the transaction and throw a `TransactionAbortException` under certain circumstances.
    pub fn lock_table(&self, txn: &mut Transaction, lock_mode: LockMode, oid: TableOidT) -> bool {
        // Check if transaction can take this lock
        if !self.can_txn_take_lock(txn, lock_mode) {
            txn.set_state(TransactionState::Aborted);
            return false;
        }

        // Get or create lock queue for this table
        let mut table_lock_map = self.table_lock_map.lock();
        let lock_queue = table_lock_map
            .entry(oid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        // Create lock request
        let request = Arc::new(Mutex::new(LockRequest::new_table_request(
            txn.get_transaction_id(),
            lock_mode,
            oid,
        )));

        // Add request to queue
        let mut lock_queue_guard = lock_queue.lock();
        lock_queue_guard.request_queue.push_back(request.clone());

        // Try to grant locks
        self.grant_new_locks_if_possible(&mut lock_queue_guard);

        // Wait until granted
        while !request.lock().granted {
            lock_queue_guard = lock_queue.wait(lock_queue_guard);
            if txn.get_state() == TransactionState::Aborted {
                return false;
            }
        }

        true
    }

    /// Releases the lock held on a table by the transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction releasing the lock.
    /// - `oid`: The table OID of the table to be unlocked.
    ///
    /// # Returns
    /// `true` if the unlock is successful, `false` otherwise.
    ///
    /// # Errors
    /// This method should abort the transaction and throw a `TransactionAbortException` under certain circumstances.
    pub fn unlock_table(&self, txn: &mut Transaction, oid: TableOidT) -> bool {
        let table_lock_map = self.table_lock_map.lock();

        if let Some(lock_queue) = table_lock_map.get(&oid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Find and remove the lock request
            if let Some(pos) = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                lock_queue_guard.request_queue.remove(pos);

                // Update transaction's lock set
                txn.get_table_lock_set().remove(&oid);

                // Try to grant waiting locks
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                return true;
            }
        }

        false
    }

    /// Acquires a lock on a row in the given lock mode.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock upgrade.
    /// - `lock_mode`: The lock mode for the requested lock.
    /// - `oid`: The table OID of the table the row belongs to.
    /// - `rid`: The RID of the row to be locked.
    ///
    /// # Returns
    /// `true` if the lock is successfully acquired or upgraded, `false` otherwise.
    ///
    /// # Errors
    /// This method should abort the transaction and throw a `TransactionAbortException` under certain circumstances.
    pub fn lock_row(
        &self,
        txn: &mut Transaction,
        lock_mode: LockMode,
        oid: TableOidT,
        rid: RID,
    ) -> bool {
        // Check if transaction can take this lock
        if !self.can_txn_take_lock(txn, lock_mode) {
            txn.set_state(TransactionState::Aborted);
            return false;
        }

        // Check for appropriate table lock
        if !self.check_appropriate_lock_on_table(txn, oid, lock_mode) {
            txn.set_state(TransactionState::Aborted);
            return false;
        }

        // Get or create lock queue for this row
        let mut row_lock_map = self.row_lock_map.lock();
        let lock_queue = row_lock_map
            .entry(rid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        // Create lock request
        let request = Arc::new(Mutex::new(LockRequest::new_row_request(
            txn.get_transaction_id(),
            lock_mode,
            oid,
            rid,
        )));

        // Add request to queue
        let mut lock_queue_guard = lock_queue.lock();
        lock_queue_guard.request_queue.push_back(request.clone());

        // Try to grant locks
        self.grant_new_locks_if_possible(&mut lock_queue_guard);

        // Wait until granted
        while !request.lock().granted {
            lock_queue_guard = lock_queue.wait(lock_queue_guard);
            if txn.get_state() == TransactionState::Aborted {
                return false;
            }
        }

        true
    }

    /// Releases the lock held on a row by the transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction releasing the lock.
    /// - `oid`: The table OID of the table the row belongs to.
    /// - `rid`: The RID of the row to be unlocked.
    /// - `force`: Force unlock the tuple regardless of isolation level, not changing the transaction state.
    ///
    /// # Returns
    /// `true` if the unlock is successful, `false` otherwise.
    ///
    /// # Errors
    /// This method should abort the transaction and throw a `TransactionAbortException` under certain circumstances.
    pub fn unlock_row(&self, txn: &mut Transaction, oid: TableOidT, rid: RID, force: bool) -> bool {
        let row_lock_map = self.row_lock_map.lock();

        if let Some(lock_queue) = row_lock_map.get(&rid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Find and remove the lock request
            if let Some(pos) = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                lock_queue_guard.request_queue.remove(pos);

                // Update transaction's lock set
                txn.get_row_lock_set().remove(&(oid, rid));

                // Try to grant waiting locks
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                return true;
            }
        }

        false
    }

    /// Adds an edge from `t1` to `t2` in the waits-for graph.
    ///
    /// # Parameters
    /// - `t1`: Transaction waiting for a lock.
    /// - `t2`: Transaction being waited for.
    pub fn add_edge(&self, t1: TxnId, t2: TxnId) {
        let mut waits_for = self.waits_for.lock();
        waits_for.entry(t1).or_insert_with(Vec::new).push(t2);
    }

    /// Removes an edge from `t1` to `t2` in the waits-for graph.
    ///
    /// # Parameters
    /// - `t1`: Transaction waiting for a lock.
    /// - `t2`: Transaction being waited for.
    pub fn remove_edge(&self, t1: TxnId, t2: TxnId) {
        let mut waits_for = self.waits_for.lock();
        if let Some(edges) = waits_for.get_mut(&t1) {
            if let Some(pos) = edges.iter().position(|&x| x == t2) {
                edges.remove(pos);
            }
            if edges.is_empty() {
                waits_for.remove(&t1);
            }
        }
    }

    /// Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
    ///
    /// # Parameters
    /// - `txn_id`: If the graph has a cycle, will contain the newest transaction ID.
    ///
    /// # Returns
    /// `false` if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to `txn_id`.
    pub fn has_cycle(&self, txn_id: &mut TxnId) -> bool {
        let waits_for = self.waits_for.lock();
        let vertices: HashSet<_> = waits_for.keys().cloned().collect();
        drop(waits_for);

        let mut path = Vec::new();
        let mut on_path = HashSet::new();
        let mut visited = HashSet::new();

        for &vertex in &vertices {
            if !visited.contains(&vertex) {
                if self.find_cycle(vertex, &mut path, &mut on_path, &mut visited, txn_id) {
                    return true;
                }
            }
        }

        false
    }

    /// Returns all edges in the current waits-for graph.
    ///
    /// # Returns
    /// A vector of all edges in the current waits-for graph.
    pub fn get_edge_list(&self) -> Vec<(TxnId, TxnId)> {
        let waits_for = self.waits_for.lock();
        let mut edges = Vec::new();
        for (&t1, t2s) in waits_for.iter() {
            for &t2 in t2s {
                edges.push((t1, t2));
            }
        }
        edges
    }

    /// Runs cycle detection in the background.
    pub fn run_cycle_detection(&self) {
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;

        while self.enable_cycle_detection.load(Ordering::SeqCst) {
            let mut txn_id = INVALID_TXN_ID;

            if self.has_cycle(&mut txn_id) {
                // Get the transaction manager and abort the transaction
                if let Ok(txn_mgr) = self.transaction_manager.lock() {
                    if let Some(txn) = txn_mgr.get_transaction(txn_id) {
                        txn.set_state(TransactionState::Aborted);
                    }
                }
            }

            // Sleep for a short duration before next check
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Upgrades a lock on a table.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock upgrade.
    /// - `lock_mode`: The lock mode for the requested lock.
    /// - `oid`: The table OID of the table to be locked.
    ///
    /// # Returns
    /// `true` if the upgrade is successful, `false` otherwise.
    pub fn upgrade_lock_table(
        &self,
        txn: &mut Transaction,
        lock_mode: LockMode,
        oid: TableOidT,
    ) -> bool {
        let table_lock_map = self.table_lock_map.lock();

        if let Some(lock_queue) = table_lock_map.get(&oid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Find current lock request
            let curr_request_pos = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            });

            if let Some(pos) = curr_request_pos {
                let curr_request = lock_queue_guard.request_queue[pos].lock();
                let curr_mode = curr_request.lock_mode;
                drop(curr_request);

                // Check if upgrade is allowed
                if !self.can_lock_upgrade(curr_mode, lock_mode) {
                    txn.set_state(TransactionState::Aborted);
                    return false;
                }

                // Check if there's already an upgrading transaction
                if lock_queue_guard.upgrading != INVALID_TXN_ID
                    && lock_queue_guard.upgrading != txn.get_transaction_id()
                {
                    txn.set_state(TransactionState::Aborted);
                    return false;
                }

                // Mark as upgrading
                lock_queue_guard.upgrading = txn.get_transaction_id();

                // Remove current lock and create new request
                lock_queue_guard.request_queue.remove(pos);
                let new_request = Arc::new(Mutex::new(LockRequest::new_table_request(
                    txn.get_transaction_id(),
                    lock_mode,
                    oid,
                )));

                // Insert at front of queue (priority for upgrades)
                lock_queue_guard.request_queue.push_front(new_request.clone());

                // Try to grant locks
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                // Wait until granted
                while !new_request.lock().granted {
                    lock_queue_guard = lock_queue.wait(lock_queue_guard);
                    if txn.get_state() == TransactionState::Aborted {
                        return false;
                    }
                }

                // Reset upgrading flag
                lock_queue_guard.upgrading = INVALID_TXN_ID;

                return true;
            }
        }

        false
    }

    /// Upgrades a lock on a row.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock upgrade.
    /// - `lock_mode`: The lock mode for the requested lock.
    /// - `oid`: The table OID of the table the row belongs to.
    /// - `rid`: The RID of the row to be locked.
    ///
    /// # Returns
    /// `true` if the upgrade is successful, `false` otherwise.
    pub fn upgrade_lock_row(
        &self,
        txn: &mut Transaction,
        lock_mode: LockMode,
        oid: TableOidT,
        rid: RID,
    ) -> bool {
        // First check for appropriate table lock
        if !self.check_appropriate_lock_on_table(txn, oid, lock_mode) {
            txn.set_state(TransactionState::Aborted);
            return false;
        }

        let row_lock_map = self.row_lock_map.lock();

        if let Some(lock_queue) = row_lock_map.get(&rid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Find current lock request
            let curr_request_pos = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            });

            if let Some(pos) = curr_request_pos {
                let curr_request = lock_queue_guard.request_queue[pos].lock();
                let curr_mode = curr_request.lock_mode;
                drop(curr_request);

                // Check if upgrade is allowed
                if !self.can_lock_upgrade(curr_mode, lock_mode) {
                    txn.set_state(TransactionState::Aborted);
                    return false;
                }

                // Check if there's already an upgrading transaction
                if lock_queue_guard.upgrading != INVALID_TXN_ID
                    && lock_queue_guard.upgrading != txn.get_transaction_id()
                {
                    txn.set_state(TransactionState::Aborted);
                    return false;
                }

                // Mark as upgrading
                lock_queue_guard.upgrading = txn.get_transaction_id();

                // Remove current lock and create new request
                lock_queue_guard.request_queue.remove(pos);
                let new_request = Arc::new(Mutex::new(LockRequest::new_row_request(
                    txn.get_transaction_id(),
                    lock_mode,
                    oid,
                    rid,
                )));

                // Insert at front of queue (priority for upgrades)
                lock_queue_guard.request_queue.push_front(new_request.clone());

                // Try to grant locks
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                // Wait until granted
                while !new_request.lock().granted {
                    lock_queue_guard = lock_queue.wait(lock_queue_guard);
                    if txn.get_state() == TransactionState::Aborted {
                        return false;
                    }
                }

                // Reset upgrading flag
                lock_queue_guard.upgrading = INVALID_TXN_ID;

                return true;
            }
        }

        false
    }

    /// Checks if two locks are compatible.
    ///
    /// # Parameters
    /// - `l1`: The first lock mode.
    /// - `l2`: The second lock mode.
    ///
    /// # Returns
    /// `true` if the locks are compatible, `false` otherwise.
    pub fn are_locks_compatible(&self, l1: LockMode, l2: LockMode) -> bool {
        use LockMode::*;
        match (l1, l2) {
            // IS is compatible with everything except X
            (IntentionShared, IntentionShared) => true,
            (IntentionShared, Shared) => true,
            (IntentionShared, IntentionExclusive) => true,
            (IntentionShared, SharedIntentionExclusive) => true,
            (IntentionShared, Exclusive) => false,

            // IX is compatible with IS and IX
            (IntentionExclusive, IntentionShared) => true,
            (IntentionExclusive, IntentionExclusive) => true,
            (IntentionExclusive, Shared) => false,
            (IntentionExclusive, SharedIntentionExclusive) => false,
            (IntentionExclusive, Exclusive) => false,

            // S is compatible with IS, S and SIX
            (Shared, IntentionShared) => true,
            (Shared, Shared) => true,
            (Shared, IntentionExclusive) => false,
            (Shared, SharedIntentionExclusive) => true,
            (Shared, Exclusive) => false,

            // SIX is compatible with IS only
            (SharedIntentionExclusive, IntentionShared) => true,
            (SharedIntentionExclusive, Shared) => true,
            (SharedIntentionExclusive, IntentionExclusive) => false,
            (SharedIntentionExclusive, SharedIntentionExclusive) => false,
            (SharedIntentionExclusive, Exclusive) => false,

            // X is not compatible with anything
            (Exclusive, _) => false,

            // Mirror the above cases
            (a, b) => self.are_locks_compatible(b, a),
        }
    }

    /// Checks if a transaction can take a lock.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock.
    /// - `lock_mode`: The lock mode for the requested lock.
    ///
    /// # Returns
    /// `true` if the transaction can take the lock, `false` otherwise.
    pub fn can_txn_take_lock(&self, txn: &Transaction, lock_mode: LockMode) -> bool {
        use crate::concurrency::transaction::IsolationLevel::*;
        use crate::concurrency::transaction::TransactionState::*;

        match (txn.get_isolation_level(), txn.get_state(), lock_mode) {
            // READ UNCOMMITTED
            (ReadUncommitted, _, LockMode::Shared) => false,
            (ReadUncommitted, _, LockMode::IntentionShared) => false,
            (ReadUncommitted, _, LockMode::SharedIntentionExclusive) => false,
            (ReadUncommitted, Growing, _) => true,
            (ReadUncommitted, Shrinking, _) => false,

            // READ COMMITTED
            (ReadCommitted, Growing, _) => true,
            (ReadCommitted, Shrinking, LockMode::IntentionShared) => true,
            (ReadCommitted, Shrinking, LockMode::Shared) => true,
            (ReadCommitted, Shrinking, _) => false,

            // REPEATABLE READ
            (RepeatableRead, Growing, _) => true,
            (RepeatableRead, Shrinking, _) => false,

            _ => false,
        }
    }

    /// Grants new locks if possible.
    ///
    /// # Parameters
    /// - `lock_request_queue`: The lock request queue.
    pub fn grant_new_locks_if_possible(&self, lock_request_queue: &mut LockRequestQueue) {
        let _guard = lock_request_queue.latch.lock();

        // Get current granted locks
        let granted_locks: Vec<LockMode> = lock_request_queue.request_queue
            .iter()
            .filter(|req| req.lock().granted)
            .map(|req| req.lock().lock_mode)
            .collect();

        // Try to grant each waiting lock request
        let mut i = 0;
        while i < lock_request_queue.request_queue.len() {
            let request = lock_request_queue.request_queue[i].lock();
            if !request.granted {
                // Check if this lock is compatible with all granted locks
                let can_grant = granted_locks.iter()
                    .all(|&granted_mode| self.are_locks_compatible(granted_mode, request.lock_mode));

                if can_grant {
                    // Grant the lock
                    drop(request);
                    let mut request = lock_request_queue.request_queue[i].lock();
                    request.granted = true;
                    drop(request);

                    // Update transaction's lock set
                    if let Ok(txn_mgr) = self.transaction_manager.lock() {
                        if let Some(txn) = txn_mgr.get_transaction(request.txn_id) {
                            // Update transaction's lock sets based on the type of lock
                            if request.rid.is_some() {
                                txn.get_row_lock_set().insert((request.oid, request.rid.unwrap()));
                            } else {
                                txn.get_table_lock_set().insert(request.oid);
                            }
                        }
                    }
                }
            }
            i += 1;
        }

        // Notify waiting threads
        lock_request_queue.cv.notify_all();
    }

    /// Checks if a lock can be upgraded.
    ///
    /// # Parameters
    /// - `curr_lock_mode`: The current lock mode.
    /// - `requested_lock_mode`: The requested lock mode.
    ///
    /// # Returns
    /// `true` if the lock can be upgraded, `false` otherwise.
    pub fn can_lock_upgrade(&self, curr_lock_mode: LockMode, requested_lock_mode: LockMode) -> bool {
        use LockMode::*;
        match (curr_lock_mode, requested_lock_mode) {
            // IS -> [S, X, IX, SIX]
            (IntentionShared, Shared) => true,
            (IntentionShared, Exclusive) => true,
            (IntentionShared, IntentionExclusive) => true,
            (IntentionShared, SharedIntentionExclusive) => true,

            // S -> [X, SIX]
            (Shared, Exclusive) => true,
            (Shared, SharedIntentionExclusive) => true,

            // IX -> [X, SIX]
            (IntentionExclusive, Exclusive) => true,
            (IntentionExclusive, SharedIntentionExclusive) => true,

            // SIX -> [X]
            (SharedIntentionExclusive, Exclusive) => true,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Checks if a transaction has an appropriate lock on a table.
    ///
    /// # Parameters
    /// - `txn`: The transaction requesting the lock.
    /// - `oid`: The table OID of the table the row belongs to.
    /// - `row_lock_mode`: The lock mode for the requested row lock.
    ///
    /// # Returns
    /// `true` if the transaction has an appropriate lock on the table, `false` otherwise.
    pub fn check_appropriate_lock_on_table(
        &self,
        txn: &Transaction,
        oid: TableOidT,
        row_lock_mode: LockMode,
    ) -> bool {
        use LockMode::*;
        let table_locks = txn.get_locks_held_on_table(oid);

        match row_lock_mode {
            Shared => {
                table_locks.contains(&IntentionShared)
                    || table_locks.contains(&Shared)
                    || table_locks.contains(&SharedIntentionExclusive)
            }
            Exclusive => {
                table_locks.contains(&IntentionExclusive)
                    || table_locks.contains(&SharedIntentionExclusive)
                    || table_locks.contains(&Exclusive)
            }
            _ => false, // Row locks cannot be intention locks
        }
    }

    /// Finds a cycle in the waits-for graph.
    ///
    /// # Parameters
    /// - `source_txn`: The source transaction ID.
    /// - `path`: The current path in the search.
    /// - `on_path`: The set of transaction IDs currently on the path.
    /// - `visited`: The set of visited transaction IDs.
    /// - `abort_txn_id`: The transaction ID to be aborted if a cycle is found.
    ///
    /// # Returns
    /// `true` if a cycle is found, `false` otherwise.
    pub fn find_cycle(
        &self,
        source_txn: TxnId,
        path: &mut Vec<TxnId>,
        on_path: &mut HashSet<TxnId>,
        visited: &mut HashSet<TxnId>,
        abort_txn_id: &mut TxnId,
    ) -> bool {
        // Skip if we've visited this node before
        if visited.contains(&source_txn) {
            return false;
        }

        visited.insert(source_txn);
        path.push(source_txn);
        on_path.insert(source_txn);

        let waits_for = self.waits_for.lock();
        if let Some(edges) = waits_for.get(&source_txn) {
            for &next_txn in edges {
                if on_path.contains(&next_txn) {
                    // Found a cycle - find the newest transaction in the cycle
                    *abort_txn_id = *path.iter()
                        .skip_while(|&&x| x != next_txn)
                        .max()
                        .unwrap_or(&next_txn);
                    return true;
                }

                if !visited.contains(&next_txn) {
                    if self.find_cycle(next_txn, path, on_path, visited, abort_txn_id) {
                        return true;
                    }
                }
            }
        }

        path.pop();
        on_path.remove(&source_txn);
        false
    }

    /// Unlocks all resources held by the lock manager.
    pub fn unlock_all(&self) {
        // Clear all table locks
        let mut table_lock_map = self.table_lock_map.lock();
        for (_, lock_queue) in table_lock_map.iter() {
            let mut lock_queue_guard = lock_queue.lock();
            lock_queue_guard.request_queue.clear();
            lock_queue_guard.upgrading = INVALID_TXN_ID;
            lock_queue_guard.cv.notify_all();
        }
        table_lock_map.clear();

        // Clear all row locks
        let mut row_lock_map = self.row_lock_map.lock();
        for (_, lock_queue) in row_lock_map.iter() {
            let mut lock_queue_guard = lock_queue.lock();
            lock_queue_guard.request_queue.clear();
            lock_queue_guard.upgrading = INVALID_TXN_ID;
            lock_queue_guard.cv.notify_all();
        }
        row_lock_map.clear();

        // Clear waits-for graph
        let mut waits_for = self.waits_for.lock();
        waits_for.clear();
    }
}
