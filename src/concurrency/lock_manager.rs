use crate::common::config::{TableOidT, TxnId, INVALID_TXN_ID};
use crate::common::exception::LockError;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode::{
    Exclusive, IntentionExclusive, IntentionShared, Shared, SharedIntentionExclusive,
};
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction::{Transaction, TransactionState};
use crate::concurrency::transaction_manager::TransactionManager;
use log::{debug, error, info, warn};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fmt, thread};

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
#[derive(Debug)]
pub struct LockRequest {
    txn_id: TxnId,
    lock_mode: LockMode,
    oid: TableOidT,
    rid: Option<RID>,
    granted: bool,
}

/// Structure to hold lock requests for the same resource (table or row).
#[derive(Debug)]
pub struct LockRequestQueue {
    request_queue: VecDeque<Arc<Mutex<LockRequest>>>,
    cv: Arc<Condvar>,
    upgrading: TxnId,
    latch: Mutex<()>,
}

/// LockManager handles transactions asking for locks on records.
#[derive(Debug)]
pub struct LockManager {
    transaction_manager: Arc<TransactionManager>,
    table_lock_map: Mutex<HashMap<TableOidT, Arc<Mutex<LockRequestQueue>>>>,
    txn_locks: Mutex<HashMap<TxnId, TxnLockState>>,
    row_lock_map: Mutex<HashMap<RID, Arc<Mutex<LockRequestQueue>>>>,
    enable_cycle_detection: AtomicBool,
    cycle_detection_thread: Option<thread::JoinHandle<()>>,
    waits_for: Mutex<HashMap<TxnId, Vec<TxnId>>>,
}

#[derive(Clone, Debug)]
struct TxnLockState {
    table_locks: HashSet<TableOidT>,
    row_locks: HashSet<(TableOidT, RID)>,
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
            cv: Arc::new(Condvar::new()),
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
    pub fn new(transaction_manager: Arc<TransactionManager>) -> Self {
        Self {
            transaction_manager,
            table_lock_map: Mutex::new(HashMap::new()),
            row_lock_map: Mutex::new(HashMap::new()),
            txn_locks: Mutex::new(HashMap::new()),
            enable_cycle_detection: AtomicBool::new(false),
            cycle_detection_thread: None,
            waits_for: Mutex::new(HashMap::new()),
        }
    }

    /// Starts the deadlock detection process in a separate thread.
    /// Uses a cloned Arc to avoid moving self into the thread.
    pub fn start_deadlock_detection(&mut self) {
        self.enable_cycle_detection.store(true, Ordering::SeqCst);

        // Create a clone of the necessary components for the detection thread
        let enable_detection = Arc::new(AtomicBool::new(true));
        let enable_detection_clone = enable_detection.clone();

        // Clone the transaction manager reference
        let txn_mgr = Arc::clone(&self.transaction_manager);

        // Create a new Arc<Mutex<Self>> to share the LockManager
        let lock_manager = Arc::new(Mutex::new(self.clone()));
        let lock_manager_clone = Arc::clone(&lock_manager);

        self.cycle_detection_thread = Some(thread::spawn(move || {
            info!("Starting deadlock detection cycle");

            while enable_detection_clone.load(Ordering::SeqCst) {
                let mut txn_id = INVALID_TXN_ID;

                // Access the lock manager through the Arc<Mutex>
                let lm = lock_manager_clone.lock();
                if lm.has_cycle(&mut txn_id) {
                    warn!("Deadlock detected, aborting transaction: {}", abort_txn = txn_id,);

                    if let Some(txn) = txn_mgr.get_transaction(&txn_id) {
                        txn.set_state(TransactionState::Aborted);
                    }
                }

                // Release the lock before sleeping
                drop(lm);
                thread::sleep(std::time::Duration::from_millis(50));
            }

            info!("Deadlock detection cycle stopped");
        }));
    }

    /// Safely stops the deadlock detection thread
    pub fn stop_deadlock_detection(&mut self) {
        self.enable_cycle_detection.store(false, Ordering::SeqCst);

        if let Some(handle) = self.cycle_detection_thread.take() {
            // Wait for the thread to finish
            if let Err(e) = handle.join() {
                warn!("Error joining deadlock detection thread: {:?}", e);
            }
        }
    }

    /// Check if transaction has a lock on a table
    pub fn has_table_lock(&self, txn_id: TxnId, oid: TableOidT) -> bool {
        let txn_locks = self.txn_locks.lock();
        txn_locks
            .get(&txn_id)
            .map(|state| state.table_locks.contains(&oid))
            .unwrap_or(false)
    }

    /// Check if transaction has a lock on a row
    pub fn has_row_lock(&self, txn_id: TxnId, oid: TableOidT, rid: RID) -> bool {
        let txn_locks = self.txn_locks.lock();
        txn_locks
            .get(&txn_id)
            .map(|state| state.row_locks.contains(&(oid, rid)))
            .unwrap_or(false)
    }

    /// Get all row locks held by a transaction on a table
    pub fn get_row_locks_for_table(&self, txn_id: TxnId, oid: TableOidT) -> HashSet<RID> {
        let txn_locks = self.txn_locks.lock();
        txn_locks
            .get(&txn_id)
            .map(|state| {
                state
                    .row_locks
                    .iter()
                    .filter(|(table_oid, _)| *table_oid == oid)
                    .map(|(_, rid)| *rid)
                    .collect()
            })
            .unwrap_or_default()
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
    pub fn lock_table(
        &self,
        txn: Arc<Transaction>,
        lock_mode: LockMode,
        oid: TableOidT,
    ) -> Result<bool, LockError> {
        info!(
            "Attempting to acquire table lock: {}, {}, {}",
            txn_id = txn.get_transaction_id(),
            table_oid = oid,
            lock_mode = lock_mode,
        );

        // Validate lock request
        if let Err(e) = self.validate_lock_request(&txn, lock_mode) {
            error!(
                "Lock request validation failed: {} {}",
                error = e,
                txn_id = txn.get_transaction_id(),

            );
            return Err(e);
        }

        let mut table_lock_map = self.table_lock_map.lock();
        let lock_queue = table_lock_map
            .entry(oid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        let request = Arc::new(Mutex::new(LockRequest::new_table_request(
            txn.get_transaction_id(),
            lock_mode,
            oid,
        )));

        let mut lock_queue_guard = lock_queue.lock();
        lock_queue_guard.request_queue.push_back(request.clone());

        self.grant_new_locks_if_possible(&mut lock_queue_guard);

        while !request.lock().granted {
            lock_queue.lock().cv.wait_while(&mut lock_queue_guard, |_| {
                !request.lock().granted && txn.get_state() != TransactionState::Aborted
            });

            if txn.get_state() == TransactionState::Aborted {
                return Ok(false);
            }
        }

        Ok(true)
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
    pub fn unlock_table(&self, txn: &mut Transaction, oid: TableOidT) -> Result<bool, LockError> {
        // Check if transaction holds any row locks on this table
        let has_row_locks = !self
            .get_row_locks_for_table(txn.get_transaction_id(), oid)
            .is_empty();
        if has_row_locks {
            return Err(LockError::TableUnlockedBeforeRows);
        }

        let table_lock_map = self.table_lock_map.lock();
        if let Some(lock_queue) = table_lock_map.get(&oid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Check if lock is actually held
            if !lock_queue_guard.request_queue.iter().any(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                return Err(LockError::NoLockHeld);
            }

            // Remove the lock request
            if let Some(pos) = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                lock_queue_guard.request_queue.remove(pos);
                // Release the lock in our centralized tracking
                self.release_lock(txn.get_transaction_id(), oid, None);
                self.grant_new_locks_if_possible(&mut lock_queue_guard);
                return Ok(true);
            }
        }

        Ok(false)
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
    ) -> Result<bool, LockError> {
        // Validate lock request
        self.validate_lock_request(txn, lock_mode)?;
        self.validate_row_lock(txn, lock_mode, oid)?;

        let mut row_lock_map = self.row_lock_map.lock();
        let lock_queue = row_lock_map
            .entry(rid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        let request = Arc::new(Mutex::new(LockRequest::new_row_request(
            txn.get_transaction_id(),
            lock_mode,
            oid,
            rid,
        )));

        let mut lock_queue_guard = lock_queue.lock();
        lock_queue_guard.request_queue.push_back(request.clone());

        self.grant_new_locks_if_possible(&mut lock_queue_guard);

        while !request.lock().granted {
            // Using parking_lot's wait_while
            lock_queue.lock().cv.wait_while(&mut lock_queue_guard, |_| {
                !request.lock().granted && txn.get_state() != TransactionState::Aborted
            });

            if txn.get_state() == TransactionState::Aborted {
                return Ok(false);
            }
        }

        // Lock was granted at this point
        self.grant_lock(txn.get_transaction_id(), oid, Some(rid));
        Ok(true)
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
    pub fn unlock_row(
        &self,
        txn: &mut Transaction,
        oid: TableOidT,
        rid: RID,
    ) -> Result<bool, LockError> {
        let row_lock_map = self.row_lock_map.lock();
        if let Some(lock_queue) = row_lock_map.get(&rid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Check if lock is actually held
            if !lock_queue_guard.request_queue.iter().any(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                return Err(LockError::NoLockHeld);
            }

            if let Some(pos) = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            }) {
                // Remove from request queue
                lock_queue_guard.request_queue.remove(pos);

                // Release lock in centralized tracking
                self.release_lock(txn.get_transaction_id(), oid, Some(rid));

                // Try to grant locks to waiting transactions
                self.grant_new_locks_if_possible(&mut lock_queue_guard);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Adds an edge from `t1` to `t2` in the waits-for graph.
    ///
    /// # Parameters
    /// - `t1`: Transaction waiting for a lock.
    /// - `t2`: Transaction being waited for.
    pub fn add_edge(&self, t1: TxnId, t2: TxnId) {
        info!("Adding edge to waits-for graph: {} {}", txn_from = t1, txn_to = t2);

        let mut waits_for = self.waits_for.lock();
        waits_for.entry(t1).or_insert_with(Vec::new).push(t2);

        // Log updated graph state
        self.log_waits_for_state();
    }

    /// Removes an edge from `t1` to `t2` in the waits-for graph.
    ///
    /// # Parameters
    /// - `t1`: Transaction waiting for a lock.
    /// - `t2`: Transaction being waited for.
    pub fn remove_edge(&self, t1: TxnId, t2: TxnId) {
        info!(
            "Removing edge from waits-for graph: {} {}",
            txn_from = t1,
            txn_to = t2,

        );

        let mut waits_for = self.waits_for.lock();
        if let Some(edges) = waits_for.get_mut(&t1) {
            if let Some(pos) = edges.iter().position(|&x| x == t2) {
                edges.remove(pos);
                if edges.is_empty() {
                    waits_for.remove(&t1);
                }
            }
        }

        // Log updated graph state
        self.log_waits_for_state();
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
        info!("Starting deadlock detection cycle");

        while self.enable_cycle_detection.load(Ordering::SeqCst) {
            let mut txn_id = INVALID_TXN_ID;

            if self.has_cycle(&mut txn_id) {
                warn!(
                    "Deadlock detected, aborting transaction: {}", abort_txn = txn_id,
                );

                // Using parking_lot Mutex
                if let Some(txn) =  self.transaction_manager.get_transaction(&txn_id) {
                    txn.set_state(TransactionState::Aborted);
                }
            }

            thread::sleep(std::time::Duration::from_millis(50));
        }

        info!("Deadlock detection cycle stopped");
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
    ) -> Result<bool, LockError> {
        // Validate lock request first
        self.validate_lock_request(txn, lock_mode)?;

        let table_lock_map = self.table_lock_map.lock();
        if let Some(lock_queue) = table_lock_map.get(&oid) {
            let mut lock_queue_guard = lock_queue.lock();

            // Find current lock
            let curr_request_pos = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            });

            if let Some(pos) = curr_request_pos {
                let curr_request = lock_queue_guard.request_queue[pos].lock();
                let curr_mode = curr_request.lock_mode;
                drop(curr_request);

                // Validate upgrade
                self.validate_upgrade(
                    curr_mode,
                    lock_mode,
                    &lock_queue_guard,
                    txn.get_transaction_id(),
                )?;

                // Mark as upgrading and process upgrade
                lock_queue_guard.upgrading = txn.get_transaction_id();
                lock_queue_guard.request_queue.remove(pos);

                let new_request = Arc::new(Mutex::new(LockRequest::new_table_request(
                    txn.get_transaction_id(),
                    lock_mode,
                    oid,
                )));

                lock_queue_guard
                    .request_queue
                    .push_front(new_request.clone());
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                while !new_request.lock().granted {
                    lock_queue.lock().cv.wait_while(&mut lock_queue_guard, |_| {
                        !new_request.lock().granted && txn.get_state() != TransactionState::Aborted
                    });

                    if txn.get_state() == TransactionState::Aborted {
                        return Ok(false);
                    }
                }

                lock_queue_guard.upgrading = INVALID_TXN_ID;
                return Ok(true);
            }
        }

        Ok(false)
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
    ) -> Result<bool, LockError> {
        // Validate lock request and row-specific conditions
        self.validate_lock_request(txn, lock_mode)?;
        self.validate_row_lock(txn, lock_mode, oid)?;

        let row_lock_map = self.row_lock_map.lock();
        if let Some(lock_queue) = row_lock_map.get(&rid) {
            let mut lock_queue_guard = lock_queue.lock();

            let curr_request_pos = lock_queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.txn_id == txn.get_transaction_id() && req.granted
            });

            if let Some(pos) = curr_request_pos {
                let curr_request = lock_queue_guard.request_queue[pos].lock();
                let curr_mode = curr_request.lock_mode;
                drop(curr_request);

                // Validate upgrade
                self.validate_upgrade(
                    curr_mode,
                    lock_mode,
                    &lock_queue_guard,
                    txn.get_transaction_id(),
                )?;

                lock_queue_guard.upgrading = txn.get_transaction_id();
                lock_queue_guard.request_queue.remove(pos);

                let new_request = Arc::new(Mutex::new(LockRequest::new_row_request(
                    txn.get_transaction_id(),
                    lock_mode,
                    oid,
                    rid,
                )));

                lock_queue_guard
                    .request_queue
                    .push_front(new_request.clone());
                self.grant_new_locks_if_possible(&mut lock_queue_guard);

                while !new_request.lock().granted {
                    lock_queue.lock().cv.wait_while(&mut lock_queue_guard, |_| {
                        !new_request.lock().granted && txn.get_state() != TransactionState::Aborted
                    });

                    if txn.get_state() == TransactionState::Aborted {
                        return Ok(false);
                    }
                }

                lock_queue_guard.upgrading = INVALID_TXN_ID;
                return Ok(true);
            }
        }

        Ok(false)
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
        use crate::concurrency::lock_manager::LockMode::*;
        use crate::concurrency::transaction::IsolationLevel::*;
        use crate::concurrency::transaction::TransactionState::*;

        match (txn.get_isolation_level(), txn.get_state(), lock_mode) {
            // READ UNCOMMITTED
            // S, IS, SIX are never allowed
            (ReadUncommitted, _, Shared) => false,
            (ReadUncommitted, _, IntentionShared) => false,
            (ReadUncommitted, _, SharedIntentionExclusive) => false,
            // X and IX are allowed in RUNNING state
            (ReadUncommitted, Running, Exclusive) => true,
            (ReadUncommitted, Running, IntentionExclusive) => true,
            (ReadUncommitted, Shrinking, _) => false, // Nothing allowed in SHRINKING

            // READ COMMITTED
            // All locks allowed in RUNNING
            (ReadCommitted, Running, _) => true,
            // Only S and IS allowed in SHRINKING
            (ReadCommitted, Shrinking, IntentionShared) => true,
            (ReadCommitted, Shrinking, Shared) => true,
            (ReadCommitted, Shrinking, _) => false,

            // REPEATABLE READ
            (RepeatableRead, Running, _) => true, // All locks allowed in RUNNING
            (RepeatableRead, Shrinking, _) => false, // No locks allowed in SHRINKING

            // Default case
            _ => false,
        }
    }

    /// Grants new locks if possible.
    ///
    /// # Parameters
    /// - `lock_request_queue`: The lock request queue.
    pub fn grant_new_locks_if_possible(&self, lock_request_queue: &mut LockRequestQueue) {
        let _guard = lock_request_queue.latch.lock();

        let granted_locks: Vec<LockMode> = lock_request_queue
            .request_queue
            .iter()
            .filter(|req| req.lock().granted)
            .map(|req| req.lock().lock_mode)
            .collect();

        let mut i = 0;
        while i < lock_request_queue.request_queue.len() {
            let request = lock_request_queue.request_queue[i].lock();
            if !request.granted {
                let can_grant = granted_locks
                    .iter()
                    .all(|&mode| self.are_locks_compatible(mode, request.lock_mode));

                if can_grant {
                    let txn_id = request.txn_id;
                    let oid = request.oid;
                    let rid = request.rid;
                    drop(request);

                    let mut request = lock_request_queue.request_queue[i].lock();
                    request.granted = true;

                    // Use the centralized lock tracking
                    self.grant_lock(txn_id, oid, rid);
                }
            }
            i += 1;
        }

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
    pub fn can_lock_upgrade(
        &self,
        curr_lock_mode: LockMode,
        requested_lock_mode: LockMode,
    ) -> bool {
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
        // We need to check what mode the lock is held in
        let table_lock_map = self.table_lock_map.lock();

        if let Some(lock_queue) = table_lock_map.get(&oid) {
            let lock_queue_guard = lock_queue.lock();

            // Find any granted lock for this transaction
            let granted_locks: Vec<LockMode> = lock_queue_guard
                .request_queue
                .iter()
                .filter(|req| {
                    let req = req.lock();
                    req.txn_id == txn.get_transaction_id() && req.granted
                })
                .map(|req| req.lock().lock_mode)
                .collect();

            // Check if the transaction holds appropriate locks
            match row_lock_mode {
                Shared => granted_locks.iter().any(|&mode| {
                    mode == IntentionShared || mode == Shared || mode == SharedIntentionExclusive
                }),
                Exclusive => granted_locks.iter().any(|&mode| {
                    mode == IntentionExclusive
                        || mode == SharedIntentionExclusive
                        || mode == Exclusive
                }),
                _ => false, // Row locks cannot be intention locks
            }
        } else {
            false // No lock queue means no locks on this table
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
                    *abort_txn_id = *path
                        .iter()
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

    /// Returns a debug representation of the current lock state
    pub fn debug_state(&self) -> String {
        let mut output = String::new();

        // Table locks
        output.push_str("=== Table Locks ===\n");
        let table_locks = self.table_lock_map.lock();
        for (&oid, lock_queue) in table_locks.iter() {
            let queue = lock_queue.lock();
            output.push_str(&format!("Table {}: ", oid));

            let locks: Vec<_> = queue
                .request_queue
                .iter()
                .map(|req| {
                    let req = req.lock();
                    format!(
                        "(Txn:{}, {}, {})",
                        req.txn_id,
                        req.lock_mode,
                        if req.granted { "G" } else { "W" }
                    )
                })
                .collect();
            output.push_str(&locks.join(", "));
            output.push('\n');
        }

        // Row locks
        output.push_str("\n=== Row Locks ===\n");
        let row_locks = self.row_lock_map.lock();
        for (&rid, lock_queue) in row_locks.iter() {
            let queue = lock_queue.lock();
            output.push_str(&format!("Row {}: ", rid));

            let locks: Vec<_> = queue
                .request_queue
                .iter()
                .map(|req| {
                    let req = req.lock();
                    format!(
                        "(Txn:{}, {}, {})",
                        req.txn_id,
                        req.lock_mode,
                        if req.granted { "G" } else { "W" }
                    )
                })
                .collect();
            output.push_str(&locks.join(", "));
            output.push('\n');
        }

        // Waits-for graph
        output.push_str("\n=== Waits-For Graph ===\n");
        let waits_for = self.waits_for.lock();
        for (&t1, t2s) in waits_for.iter() {
            output.push_str(&format!(
                "Txn {} waits for: {}\n",
                t1,
                t2s.iter()
                    .map(|&t2| t2.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        output
    }

    pub fn get_active_lock_count(&self) -> usize {
        self.table_lock_map.lock().len()
    }

    pub fn get_waiting_lock_count(&self) -> usize {
        self.waits_for.lock().len()
    }

    /// Validates if a transaction can take a lock based on isolation level
    fn validate_lock_request(
        &self,
        txn: &Transaction,
        lock_mode: LockMode,
    ) -> Result<(), LockError> {
        match (txn.get_isolation_level(), txn.get_state(), lock_mode) {
            // READ UNCOMMITTED validations
            (IsolationLevel::ReadUncommitted, _, LockMode::Shared)
            | (IsolationLevel::ReadUncommitted, _, LockMode::IntentionShared)
            | (IsolationLevel::ReadUncommitted, _, LockMode::SharedIntentionExclusive) => {
                Err(LockError::LockSharedOnReadUncommitted)
            }

            // SHRINKING state validations
            (IsolationLevel::ReadUncommitted, TransactionState::Shrinking, _)
            | (IsolationLevel::RepeatableRead, TransactionState::Shrinking, _)
            | (IsolationLevel::ReadCommitted, TransactionState::Shrinking, LockMode::Exclusive)
            | (
                IsolationLevel::ReadCommitted,
                TransactionState::Shrinking,
                LockMode::IntentionExclusive,
            )
            | (
                IsolationLevel::ReadCommitted,
                TransactionState::Shrinking,
                LockMode::SharedIntentionExclusive,
            ) => Err(LockError::LockOnShrinking),

            _ => Ok(()),
        }
    }

    /// Validates if a row lock request is valid
    fn validate_row_lock(
        &self,
        txn: &Transaction,
        lock_mode: LockMode,
        oid: TableOidT,
    ) -> Result<(), LockError> {
        // Check for intention locks on rows
        match lock_mode {
            LockMode::IntentionShared
            | LockMode::IntentionExclusive
            | LockMode::SharedIntentionExclusive => {
                return Err(LockError::AttemptedIntentionLockOnRow);
            }
            _ => {}
        }

        // Check for appropriate table lock
        if !self.check_appropriate_lock_on_table(txn, oid, lock_mode) {
            return Err(LockError::TableLockNotPresent);
        }

        Ok(())
    }

    /// Validates if a lock upgrade is valid
    fn validate_upgrade(
        &self,
        curr_mode: LockMode,
        requested_mode: LockMode,
        lock_queue: &LockRequestQueue,
        txn_id: TxnId,
    ) -> Result<(), LockError> {
        // Check if upgrade is compatible
        if !self.can_lock_upgrade(curr_mode, requested_mode) {
            return Err(LockError::IncompatibleUpgrade);
        }

        // Check for upgrade conflicts
        if lock_queue.upgrading != INVALID_TXN_ID && lock_queue.upgrading != txn_id {
            return Err(LockError::UpgradeConflict);
        }

        Ok(())
    }

    /// Logs the current state of a lock queue
    fn log_lock_queue_state(&self, queue: &LockRequestQueue, resource: &str) {
        let granted: Vec<_> = queue
            .request_queue
            .iter()
            .filter(|req| req.lock().granted)
            .map(|req| {
                let req = req.lock();
                format!("Txn:{} Mode:{}", req.txn_id, req.lock_mode)
            })
            .collect();

        let waiting: Vec<_> = queue
            .request_queue
            .iter()
            .filter(|req| !req.lock().granted)
            .map(|req| {
                let req = req.lock();
                format!("Txn:{} Mode:{}", req.txn_id, req.lock_mode)
            })
            .collect();

        debug!(
            "Lock queue state: {} {} {} {}",
            resource,
            granted.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "),
            waiting.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "),
            queue.upgrading,
        );
    }

    /// Logs the current state of the waits-for graph
    fn log_waits_for_state(&self) {
        let waits_for = self.waits_for.lock();
        let edges: Vec<_> = waits_for
            .iter()
            .flat_map(|(&t1, t2s)| t2s.iter().map(move |&t2| format!("{} -> {}", t1, t2)))
            .collect();

        debug!("Waits-for graph state: {}", edges.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "));
    }

    fn grant_lock(&self, txn_id: TxnId, oid: TableOidT, rid: Option<RID>) {
        let mut txn_locks = self.txn_locks.lock();
        let state = txn_locks.entry(txn_id).or_insert_with(|| TxnLockState {
            table_locks: HashSet::new(),
            row_locks: HashSet::new(),
        });

        match rid {
            Some(rid) => {
                state.row_locks.insert((oid, rid));
            }
            None => {
                state.table_locks.insert(oid);
            }
        }
    }

    fn release_lock(&self, txn_id: TxnId, oid: TableOidT, rid: Option<RID>) {
        let mut txn_locks = self.txn_locks.lock();
        if let Some(state) = txn_locks.get_mut(&txn_id) {
            match rid {
                Some(rid) => {
                    state.row_locks.remove(&(oid, rid));
                }
                None => {
                    state.table_locks.remove(&oid);
                }
            }
        }
    }
}

impl Clone for LockManager {
    fn clone(&self) -> Self {
        Self {
            transaction_manager: Arc::clone(&self.transaction_manager),
            table_lock_map: Mutex::new(self.table_lock_map.lock().clone()),
            txn_locks: Mutex::new(self.txn_locks.lock().clone()),
            row_lock_map: Mutex::new(self.row_lock_map.lock().clone()),
            enable_cycle_detection: AtomicBool::new(
                self.enable_cycle_detection.load(Ordering::SeqCst),
            ),
            cycle_detection_thread: None,
            waits_for: Mutex::new(self.waits_for.lock().clone()),
        }
    }
}

unsafe impl Send for LockManager {}
unsafe impl Sync for LockManager {}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Shared => write!(f, "S"),
            Exclusive => write!(f, "X"),
            IntentionShared => write!(f, "IS"),
            IntentionExclusive => write!(f, "IX"),
            SharedIntentionExclusive => write!(f, "SIX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::fs;

    pub struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        pub fn new(test_name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )));
            let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::clone(&log_manager)));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));

            Self {
                catalog,
                lock_manager,
                db_file,
                db_log_file,
            }
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_lock_manager_debug_state() {
        // Initialize test context
        let ctx = TestContext::new("test_lock_manager_debug_state");

        // Create a test table
        let mut catalog = ctx.catalog.write();
        let schema = crate::catalog::schema::Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let table_info = catalog.create_table("test_table".to_string(), schema).unwrap();
        let table_oid = table_info.get_table_oidt();
        drop(catalog);

        // Create transactions
        let txn1 = Arc::new(Transaction::new(1, IsolationLevel::RepeatableRead));
        let txn2 = Arc::new(Transaction::new(2, IsolationLevel::RepeatableRead));

        // Acquire locks
        let _ = ctx.lock_manager.lock_table(txn1, Exclusive, table_oid);
        let _ = ctx.lock_manager.lock_table(txn2, Shared, table_oid + 1);

        // Get debug state
        let debug_state = ctx.lock_manager.debug_state();
        println!("Lock Manager State:\n{}", debug_state);

        // Verify debug output contains expected information
        assert!(debug_state.contains(&format!("Table {}", table_oid)));
        assert!(debug_state.contains(&format!("Table {}", table_oid + 1)));
        assert!(debug_state.contains("Txn:1"));
        assert!(debug_state.contains("Txn:2"));
    }

    #[test]
    fn test_can_txn_take_lock() {
        let ctx = TestContext::new("test_can_txn_take_lock");
        let lock_manager = &ctx.lock_manager;

        // READ UNCOMMITTED Tests
        let txn_ru = Transaction::new(1, IsolationLevel::ReadUncommitted);

        // In Growing state
        assert!(
            !lock_manager.can_txn_take_lock(&txn_ru, Shared),
            "READ UNCOMMITTED should not allow S locks"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_ru, IntentionShared),
            "READ UNCOMMITTED should not allow IS locks"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_ru, SharedIntentionExclusive),
            "READ UNCOMMITTED should not allow SIX locks"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_ru, Exclusive),
            "READ UNCOMMITTED should allow X locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_ru, IntentionExclusive),
            "READ UNCOMMITTED should allow IX locks in GROWING"
        );

        // READ COMMITTED Tests
        let txn_rc = Transaction::new(2, IsolationLevel::ReadCommitted);

        // In Growing state
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, Shared),
            "READ COMMITTED should allow S locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, Exclusive),
            "READ COMMITTED should allow X locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, IntentionShared),
            "READ COMMITTED should allow IS locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, IntentionExclusive),
            "READ COMMITTED should allow IX locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, SharedIntentionExclusive),
            "READ COMMITTED should allow SIX locks in GROWING"
        );

        // In Shrinking state
        txn_rc.set_state(TransactionState::Shrinking);
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, Shared),
            "READ COMMITTED should allow S locks in SHRINKING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rc, IntentionShared),
            "READ COMMITTED should allow IS locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rc, Exclusive),
            "READ COMMITTED should not allow X locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rc, IntentionExclusive),
            "READ COMMITTED should not allow IX locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rc, SharedIntentionExclusive),
            "READ COMMITTED should not allow SIX locks in SHRINKING"
        );

        // REPEATABLE READ Tests
        let txn_rr = Transaction::new(3, IsolationLevel::RepeatableRead);

        // In Growing state
        assert!(
            lock_manager.can_txn_take_lock(&txn_rr, Shared),
            "REPEATABLE READ should allow S locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rr, Exclusive),
            "REPEATABLE READ should allow X locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rr, IntentionShared),
            "REPEATABLE READ should allow IS locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rr, IntentionExclusive),
            "REPEATABLE READ should allow IX locks in GROWING"
        );
        assert!(
            lock_manager.can_txn_take_lock(&txn_rr, SharedIntentionExclusive),
            "REPEATABLE READ should allow SIX locks in GROWING"
        );

        // In Shrinking state
        txn_rr.set_state(TransactionState::Shrinking);
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rr, Shared),
            "REPEATABLE READ should not allow any locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rr, Exclusive),
            "REPEATABLE READ should not allow any locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rr, IntentionShared),
            "REPEATABLE READ should not allow any locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rr, IntentionExclusive),
            "REPEATABLE READ should not allow any locks in SHRINKING"
        );
        assert!(
            !lock_manager.can_txn_take_lock(&txn_rr, SharedIntentionExclusive),
            "REPEATABLE READ should not allow any locks in SHRINKING"
        );
    }

    #[test]
    fn test_validate_lock_request() {
        let ctx = TestContext::new("test_validate_lock_request");
        let lock_manager = &ctx.lock_manager;

        // READ UNCOMMITTED Tests
        let txn_ru = Transaction::new(1, IsolationLevel::ReadUncommitted);

        // Should not allow S, IS, SIX locks regardless of state
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, Shared),
            Err(LockError::LockSharedOnReadUncommitted)
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, IntentionShared),
            Err(LockError::LockSharedOnReadUncommitted)
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, SharedIntentionExclusive),
            Err(LockError::LockSharedOnReadUncommitted)
        );

        // Growing state should allow X and IX
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, Exclusive),
            Ok(())
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, IntentionExclusive),
            Ok(())
        );

        // Shrinking state should not allow any locks
        txn_ru.set_state(TransactionState::Shrinking);
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, Exclusive),
            Err(LockError::LockOnShrinking)
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_ru, IntentionExclusive),
            Err(LockError::LockOnShrinking)
        );

        // REPEATABLE READ Tests
        let txn_rr = Transaction::new(2, IsolationLevel::RepeatableRead);

        // Growing state should allow all locks
        for lock_mode in [
            Shared,
            Exclusive,
            IntentionShared,
            IntentionExclusive,
            SharedIntentionExclusive,
        ] {
            assert_eq!(
                lock_manager.validate_lock_request(&txn_rr, lock_mode),
                Ok(()),
                "REPEATABLE READ should allow {:?} in GROWING state",
                lock_mode
            );
        }

        // Shrinking state should not allow any locks
        txn_rr.set_state(TransactionState::Shrinking);
        for lock_mode in [
            Shared,
            Exclusive,
            IntentionShared,
            IntentionExclusive,
            SharedIntentionExclusive,
        ] {
            assert_eq!(
                lock_manager.validate_lock_request(&txn_rr, lock_mode),
                Err(LockError::LockOnShrinking),
                "REPEATABLE READ should not allow {:?} in SHRINKING state",
                lock_mode
            );
        }

        // READ COMMITTED Tests
        let txn_rc = Transaction::new(3, IsolationLevel::ReadCommitted);

        // Growing state should allow all locks
        for lock_mode in [
            Shared,
            Exclusive,
            IntentionShared,
            IntentionExclusive,
            SharedIntentionExclusive,
        ] {
            assert_eq!(
                lock_manager.validate_lock_request(&txn_rc, lock_mode),
                Ok(()),
                "READ COMMITTED should allow {:?} in GROWING state",
                lock_mode
            );
        }

        // Shrinking state validation
        txn_rc.set_state(TransactionState::Shrinking);

        // Should allow S and IS locks
        assert_eq!(lock_manager.validate_lock_request(&txn_rc, Shared), Ok(()));
        assert_eq!(
            lock_manager.validate_lock_request(&txn_rc, IntentionShared),
            Ok(())
        );

        // Should not allow X, IX, SIX locks
        assert_eq!(
            lock_manager.validate_lock_request(&txn_rc, Exclusive),
            Err(LockError::LockOnShrinking)
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_rc, IntentionExclusive),
            Err(LockError::LockOnShrinking)
        );
        assert_eq!(
            lock_manager.validate_lock_request(&txn_rc, SharedIntentionExclusive),
            Err(LockError::LockOnShrinking)
        );
    }
}
