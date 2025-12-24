//! # Lock Manager
//!
//! This module provides a strict two-phase locking (2PL) implementation with
//! multi-level granularity (table and row locks), deadlock detection via a
//! waits-for graph, and isolation level-aware lock validation.
//!
//! ## Architecture
//!
//! ```text
//!                          ┌──────────────────────────────────────────────────┐
//!                          │                  LockManager                     │
//!                          │                                                  │
//!                          │  ┌──────────────────────────────────────────┐    │
//!                          │  │           DeadlockDetector               │    │
//!                          │  │  ┌────────────────────────────────────┐  │    │
//!                          │  │  │ waits_for: HashMap<TxnId, Vec>     │  │    │
//!                          │  │  │   T1 ──▶ [T2, T3]                  │  │    │
//!                          │  │  │   T2 ──▶ [T4]                      │  │    │
//!                          │  │  │   ...                              │  │    │
//!                          │  │  └────────────────────────────────────┘  │    │
//!                          │  │  detection_thread (background DFS)       │    │
//!                          │  └──────────────────────────────────────────┘    │
//!                          │                                                  │
//!                          │  ┌──────────────────────────────────────────┐    │
//!                          │  │           LockStateManager               │    │
//!                          │  │  ┌────────────────────────────────────┐  │    │
//!                          │  │  │ table_lock_map: HashMap            │  │    │
//!                          │  │  │   TableOid → LockRequestQueue      │  │    │
//!                          │  │  └────────────────────────────────────┘  │    │
//!                          │  │  ┌────────────────────────────────────┐  │    │
//!                          │  │  │ row_locks: HashMap                 │  │    │
//!                          │  │  │   RID → LockRequestQueue           │  │    │
//!                          │  │  └────────────────────────────────────┘  │    │
//!                          │  │  ┌────────────────────────────────────┐  │    │
//!                          │  │  │ txn_lock_sets: HashMap             │  │    │
//!                          │  │  │   TxnId → TxnLockState             │  │    │
//!                          │  │  └────────────────────────────────────┘  │    │
//!                          │  └──────────────────────────────────────────┘    │
//!                          │                                                  │
//!                          │  ┌──────────────────────────────────────────┐    │
//!                          │  │           LockValidator                  │    │
//!                          │  │  (isolation level & state validation)    │    │
//!                          │  └──────────────────────────────────────────┘    │
//!                          └──────────────────────────────────────────────────┘
//! ```
//!
//! ## Lock Modes
//!
//! | Mode | Name | Description |
//! |------|------|-------------|
//! | `IS` | Intention Shared | Intent to acquire S lock on descendant |
//! | `IX` | Intention Exclusive | Intent to acquire X lock on descendant |
//! | `S`  | Shared | Read lock, compatible with other S locks |
//! | `SIX`| Shared + Intention Exclusive | S on resource + IX for descendants |
//! | `X`  | Exclusive | Write lock, incompatible with all others |
//!
//! ## Lock Compatibility Matrix
//!
//! ```text
//!      │  IS  │  IX  │  S   │ SIX  │  X   │
//!  ────┼──────┼──────┼──────┼──────┼──────┤
//!   IS │  ✓   │  ✓   │  ✓   │  ✓   │  ✗   │
//!   IX │  ✓   │  ✓   │  ✗   │  ✗   │  ✗   │
//!   S  │  ✓   │  ✗   │  ✓   │  ✗   │  ✗   │
//!  SIX │  ✓   │  ✗   │  ✗   │  ✗   │  ✗   │
//!   X  │  ✗   │  ✗   │  ✗   │  ✗   │  ✗   │
//! ```
//!
//! ## Lock Upgrade Paths
//!
//! ```text
//!   IS ───▶ S, X, IX, SIX
//!    S ───▶ X, SIX
//!   IX ───▶ X, SIX
//!  SIX ───▶ X
//! ```
//!
//! ## Multi-Level Locking Protocol
//!
//! ```text
//!   Row Lock Requirement          Table Lock Must Hold
//!   ──────────────────────────────────────────────────
//!   S (Shared) on row       ──▶   S, IS, or SIX on table
//!   X (Exclusive) on row    ──▶   X, IX, or SIX on table
//! ```
//!
//! ## Isolation Level Lock Rules
//!
//! | Isolation Level | Growing Phase | Shrinking Phase |
//! |-----------------|---------------|-----------------|
//! | READ UNCOMMITTED | X, IX only | X, IX only → SHRINKING |
//! | READ COMMITTED | All locks | IS, S only |
//! | REPEATABLE READ | All locks | No locks allowed |
//! | SERIALIZABLE | All locks | S, X → SHRINKING |
//!
//! ## Deadlock Detection
//!
//! The lock manager uses a background thread that periodically checks for
//! cycles in the waits-for graph using depth-first search (DFS):
//!
//! ```text
//!   Waits-For Graph                 Cycle Detection
//!   ────────────────                ────────────────
//!   T1 ──▶ T2 ──▶ T3               DFS from each node
//!          │      │                Track path + visited
//!          └──────┘                Abort highest TxnId in cycle
//!         (CYCLE!)
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | `LockManager` | Main coordinator for lock operations |
//! | `LockRequest` | Single lock request (table or row) |
//! | `LockRequestQueue` | FIFO queue per resource with upgrade support |
//! | `DeadlockDetector` | Waits-for graph with background cycle detection |
//! | `LockStateManager` | Tracks all lock state and transaction lock sets |
//! | `LockValidator` | Enforces isolation level and state constraints |
//! | `LockCompatibilityChecker` | Implements compatibility matrix |
//!
//! ## Core Operations
//!
//! | Operation | Description |
//! |-----------|-------------|
//! | `lock_table(txn, mode, oid)` | Acquire table lock (blocking) |
//! | `unlock_table(txn, mode, oid)` | Release table lock |
//! | `lock_row(txn, mode, oid, rid)` | Acquire row lock (requires table lock) |
//! | `unlock_row(txn, mode, oid, rid)` | Release row lock |
//! | `force_release_txn(txn_id)` | Release all locks for transaction |
//! | `check_deadlock(txn)` | Check if transaction is victim of deadlock |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::concurrency::lock_manager::{LockManager, LockMode};
//! use crate::concurrency::transaction::{Transaction, IsolationLevel};
//!
//! let lock_manager = LockManager::new();
//! let txn = Arc::new(Transaction::new(1, IsolationLevel::RepeatableRead));
//! txn.set_state(TransactionState::Growing);
//!
//! // Acquire intention lock on table, then row lock
//! lock_manager.lock_table(txn.clone(), LockMode::IntentionExclusive, table_oid)?;
//! lock_manager.lock_row(txn.clone(), LockMode::Exclusive, table_oid, rid)?;
//!
//! // ... perform operations ...
//!
//! // Release locks (row first, then table)
//! lock_manager.unlock_row(txn.clone(), LockMode::Exclusive, table_oid, rid)?;
//! lock_manager.unlock_table(txn.clone(), LockMode::IntentionExclusive, table_oid)?;
//! ```
//!
//! ## Thread Safety
//!
//! - All lock state is protected by `parking_lot::Mutex`
//! - `Condvar` is used for blocking lock acquisition
//! - Deadlock detection runs on a separate background thread
//! - Lock manager implements `Send` and `Sync`

use crate::common::config::{INVALID_TXN_ID, TableOidT, TxnId};
use crate::common::exception::LockError;
use crate::common::rid::RID;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction::{Transaction, TransactionState};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Condvar, Mutex, RawMutex};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
/// ## Lock Compatibility Matrix
///
/// |     | IS  | IX  | S   | SIX | X   |
/// |-----|-----|-----|-----|-----|-----|
/// | IS  | Y   | Y   | Y   | Y   | N   |
/// | IX  | Y   | Y   | N   | N   | N   |
/// | S   | Y   | N   | Y   | N   | N   |
/// | SIX | Y   | N   | N   | N   | N   |
/// | X   | N   | N   | N   | N   | N   |
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
}

#[derive(Clone, Debug)]
struct TxnLockState {
    table_locks: HashSet<TableOidT>,
    row_locks: HashSet<(TableOidT, RID)>,
}

// 1. Deadlock Detection Component
#[derive(Debug)]
pub struct DeadlockDetector {
    waits_for: Arc<Mutex<HashMap<TxnId, Vec<TxnId>>>>,
    latest_abort_txn: Arc<Mutex<Option<TxnId>>>,
    enable_detection: Arc<AtomicBool>,
    detection_thread: Option<thread::JoinHandle<()>>,
}

// 2. Lock State Manager - Handles all lock state tracking and management
#[derive(Debug)]
pub struct LockStateManager {
    table_lock_map: Mutex<HashMap<TableOidT, Arc<Mutex<LockRequestQueue>>>>,
    row_locks: Mutex<HashMap<RID, Arc<Mutex<LockRequestQueue>>>>,
    txn_lock_sets: Mutex<HashMap<TxnId, TxnLockState>>,
}

// 3. Lock Validator
#[derive(Debug)]
pub struct LockValidator {}

// 4. Lock Compatibility Checker
#[derive(Debug)]
pub struct LockCompatibilityChecker;

// Main LockManager that orchestrates the components
#[derive(Debug)]
pub struct LockManager {
    deadlock_detector: DeadlockDetector,
    lock_state_manager: LockStateManager,
    lock_validator: LockValidator,
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

    /// Gets the table OID for this lock request.
    pub fn get_oid(&self) -> TableOidT {
        self.oid
    }

    /// Gets the row ID for this lock request, if it's a row lock.
    pub fn get_rid(&self) -> Option<RID> {
        self.rid
    }

    /// Gets the transaction ID for this lock request.
    pub fn get_txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Gets the lock mode for this lock request.
    pub fn get_lock_mode(&self) -> LockMode {
        self.lock_mode
    }

    /// Checks if this lock request has been granted.
    pub fn is_granted(&self) -> bool {
        self.granted
    }
}

impl Default for LockRequestQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl LockRequestQueue {
    /// Creates a new `LockRequestQueue`.
    pub fn new() -> Self {
        Self {
            request_queue: VecDeque::new(),
            cv: Arc::new(Condvar::new()),
            upgrading: INVALID_TXN_ID,
        }
    }

    /// Checks if a new lock request is compatible with existing granted locks.
    pub fn compatible_with_existing_locks(&self, request: &LockRequest) -> bool {
        let txn_id = request.get_txn_id();
        let new_mode = request.get_lock_mode();

        // If there's an upgrading transaction, no other locks can be granted
        if self.upgrading != INVALID_TXN_ID && self.upgrading != txn_id {
            return false;
        }

        // Check existing locks by this transaction
        let has_lock = false;
        for existing_req in &self.request_queue {
            let existing = existing_req.lock();
            if existing.is_granted() && existing.get_txn_id() == txn_id {
                //has_lock = true;
                // Check if this is a valid lock upgrade
                return match (existing.get_lock_mode(), new_mode) {
                    // IS -> [S, X, IX, SIX]
                    (LockMode::IntentionShared, _) => true,
                    // S -> [X, SIX]
                    (
                        LockMode::Shared,
                        LockMode::Exclusive | LockMode::SharedIntentionExclusive,
                    ) => true,
                    // IX -> [X, SIX]
                    (
                        LockMode::IntentionExclusive,
                        LockMode::Exclusive | LockMode::SharedIntentionExclusive,
                    ) => true,
                    // SIX -> [X]
                    (LockMode::SharedIntentionExclusive, LockMode::Exclusive) => true,
                    // Same mode - already granted
                    (mode1, mode2) if mode1 == mode2 => true,
                    // Invalid upgrade
                    _ => false,
                };
            }
        }

        // If this transaction doesn't have a lock yet, check compatibility with all granted locks
        if !has_lock {
            for existing_req in &self.request_queue {
                let existing = existing_req.lock();
                if existing.is_granted()
                    && !LockCompatibilityChecker::are_compatible(existing.get_lock_mode(), new_mode)
                {
                    return false;
                }
            }
        }
        true
    }

    /// Attempts to grant a lock request.
    pub fn grant_lock(&mut self, request: Arc<Mutex<LockRequest>>) -> bool {
        let txn_id = request.lock().get_txn_id();
        let lock_mode = request.lock().get_lock_mode();

        // Check for existing locks by this transaction
        let mut existing_lock = None;
        for (i, existing_req) in self.request_queue.iter().enumerate() {
            let existing = existing_req.lock();
            if existing.is_granted() && existing.get_txn_id() == txn_id {
                if existing.get_lock_mode() == lock_mode {
                    return true; // Already have this lock
                }
                existing_lock = Some((i, existing.get_lock_mode()));
                break;
            }
        }

        // Handle lock upgrade if needed
        if let Some((i, old_mode)) = existing_lock {
            // Check if upgrade is allowed
            match (old_mode, lock_mode) {
                // Keep both locks for valid combinations
                (LockMode::IntentionShared, LockMode::Shared)
                | (LockMode::IntentionExclusive, LockMode::Exclusive) => {
                    // Don't remove the intention lock, just add the new lock
                }
                // For other upgrades, remove the old lock
                _ => {
                    if self.upgrading != INVALID_TXN_ID && self.upgrading != txn_id {
                        return false; // Another transaction is upgrading
                    }
                    self.upgrading = txn_id;
                    self.request_queue.remove(i);
                }
            }
        }

        // Check compatibility with existing locks
        if self.compatible_with_existing_locks(&request.lock()) {
            // Grant the new lock
            self.request_queue.push_back(request.clone());
            request.lock().granted = true;
            if self.upgrading == txn_id {
                self.upgrading = INVALID_TXN_ID;
            }
            self.cv.notify_all();
            return true;
        }

        // Add request to queue but don't grant it yet
        self.request_queue.push_back(request);
        false
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl DeadlockDetector {
    pub fn new() -> Self {
        Self {
            waits_for: Arc::new(Mutex::new(HashMap::new())),
            latest_abort_txn: Arc::new(Mutex::new(None)),
            enable_detection: Arc::new(AtomicBool::new(false)),
            detection_thread: None,
        }
    }

    pub fn add_edge(&self, t1: TxnId, t2: TxnId) {
        let mut waits_for = self.waits_for.lock();
        waits_for.entry(t1).or_default().push(t2);
    }

    pub fn remove_edge(&self, t1: TxnId) {
        let mut waits_for = self.waits_for.lock();
        waits_for.remove(&t1);
    }

    pub fn start_detection(&mut self) {
        self.enable_detection.store(true, Ordering::SeqCst);

        let waits_for = Arc::clone(&self.waits_for);
        let enable_detection = Arc::clone(&self.enable_detection);
        let latest_abort_txn = Arc::clone(&self.latest_abort_txn);

        self.detection_thread = Some(thread::spawn(move || {
            while enable_detection.load(Ordering::SeqCst) {
                // Create a temporary copy of the graph for cycle detection
                let graph = {
                    let guard = waits_for.lock();
                    guard.clone()
                };

                // Check for cycles
                let mut abort_txn = INVALID_TXN_ID;
                if Self::find_cycle_in_graph(&graph, &mut abort_txn) {
                    // Update the latest transaction to abort
                    let mut latest = latest_abort_txn.lock();
                    *latest = Some(abort_txn);
                }

                thread::sleep(std::time::Duration::from_millis(50));
            }
        }));
    }

    pub fn get_and_clear_abort_txn(&self) -> Option<TxnId> {
        let mut latest = self.latest_abort_txn.lock();
        latest.take()
    }

    /// Checks if there is a cycle in the waits-for graph
    /// Returns true if a cycle is found and sets abort_txn to the transaction to abort
    pub fn has_cycle(&self, abort_txn: &mut TxnId) -> bool {
        let waits_for = self.waits_for.lock();
        Self::find_cycle_in_graph(&waits_for, abort_txn)
    }

    /// Helper function to find cycles in the waits-for graph using DFS
    fn find_cycle_in_graph(waits_for: &HashMap<TxnId, Vec<TxnId>>, abort_txn: &mut TxnId) -> bool {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut on_path = HashSet::new();

        for &txn_id in waits_for.keys() {
            if !visited.contains(&txn_id)
                && Self::dfs_cycle(
                    txn_id,
                    waits_for,
                    &mut visited,
                    &mut path,
                    &mut on_path,
                    abort_txn,
                )
            {
                return true;
            }
        }
        false
    }

    fn dfs_cycle(
        curr: TxnId,
        waits_for: &HashMap<TxnId, Vec<TxnId>>,
        visited: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
        on_path: &mut HashSet<TxnId>,
        abort_txn: &mut TxnId,
    ) -> bool {
        visited.insert(curr);
        path.push(curr);
        on_path.insert(curr);

        if let Some(edges) = waits_for.get(&curr) {
            for &next in edges {
                if on_path.contains(&next) {
                    // Found cycle - choose highest transaction ID to abort
                    *abort_txn = *path
                        .iter()
                        .skip_while(|&&x| x != next)
                        .max()
                        .unwrap_or(&next);
                    return true;
                }
                if !visited.contains(&next)
                    && Self::dfs_cycle(next, waits_for, visited, path, on_path, abort_txn)
                {
                    return true;
                }
            }
        }

        path.pop();
        on_path.remove(&curr);
        false
    }
}

impl Default for LockStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockStateManager {
    pub fn new() -> Self {
        Self {
            table_lock_map: Mutex::new(HashMap::new()),
            row_locks: Mutex::new(HashMap::new()),
            txn_lock_sets: Mutex::new(HashMap::new()),
        }
    }

    /// Attempts to grant a table lock
    pub fn grant_table_lock(&self, txn_id: TxnId, lock_mode: LockMode, oid: TableOidT) -> bool {
        let mut table_locks = self.table_lock_map.lock();
        let queue = table_locks
            .entry(oid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        // Create and try to grant the lock request
        let request = Arc::new(Mutex::new(LockRequest::new_table_request(
            txn_id, lock_mode, oid,
        )));
        let mut queue_guard = queue.lock();
        let granted = queue_guard.grant_lock(request);

        // If granted, update transaction's lock set
        if granted {
            let mut txn_locks = self.txn_lock_sets.lock();
            let lock_state = txn_locks.entry(txn_id).or_insert_with(|| TxnLockState {
                table_locks: HashSet::new(),
                row_locks: HashSet::new(),
            });
            lock_state.table_locks.insert(oid);
        }

        // Drop locks in reverse order to avoid deadlock
        drop(queue_guard);
        drop(table_locks);

        granted
    }

    /// Attempts to grant a row lock
    pub fn grant_row_lock(&self, txn_id: TxnId, lock_mode: LockMode, rid: RID) -> bool {
        let mut row_locks = self.row_locks.lock();
        let queue = row_locks
            .entry(rid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        let request = Arc::new(Mutex::new(LockRequest::new_row_request(
            txn_id,
            lock_mode,
            rid.get_page_id(),
            rid,
        )));
        let granted = queue.lock().grant_lock(request.clone());

        if granted {
            // Update transaction's lock set
            let mut txn_locks = self.txn_lock_sets.lock();
            let lock_state = txn_locks.entry(txn_id).or_insert_with(|| TxnLockState {
                table_locks: HashSet::new(),
                row_locks: HashSet::new(),
            });
            lock_state.row_locks.insert((rid.get_page_id(), rid));
        }

        granted
    }

    /// Releases a table lock
    pub fn release_table_lock(&self, txn_id: TxnId, oid: TableOidT) -> Result<(), LockError> {
        let table_locks = self.table_lock_map.lock();

        if let Some(queue) = table_locks.get(&oid) {
            let mut queue_guard = queue.lock();

            // Find and remove the lock request
            if let Some(pos) = queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.get_txn_id() == txn_id && req.is_granted()
            }) {
                // Remove the lock request
                queue_guard.request_queue.remove(pos);

                // Update transaction's lock set
                let mut txn_locks = self.txn_lock_sets.lock();
                if let Some(lock_state) = txn_locks.get_mut(&txn_id) {
                    lock_state.table_locks.remove(&oid);
                }

                // Try to grant waiting locks
                let mut i = 0;
                while i < queue_guard.request_queue.len() {
                    let request = queue_guard.request_queue[i].clone();
                    if !request.lock().is_granted()
                        && queue_guard.compatible_with_existing_locks(&request.lock())
                    {
                        request.lock().granted = true;
                    }
                    i += 1;
                }
                queue_guard.cv.notify_all();

                return Ok(());
            }
        }

        Err(LockError::NoLockHeld)
    }

    /// Releases a row lock
    pub fn release_row_lock(&self, txn_id: TxnId, rid: RID) -> Result<(), LockError> {
        let row_locks = self.row_locks.lock();

        if let Some(queue) = row_locks.get(&rid) {
            let mut queue_guard = queue.lock();

            // Find and remove the lock request
            if let Some(pos) = queue_guard.request_queue.iter().position(|req| {
                let req = req.lock();
                req.get_txn_id() == txn_id && req.is_granted()
            }) {
                queue_guard.request_queue.remove(pos);

                // Update transaction's lock set
                let mut txn_locks = self.txn_lock_sets.lock();
                if let Some(lock_state) = txn_locks.get_mut(&txn_id) {
                    lock_state.row_locks.remove(&(rid.get_page_id(), rid));
                }

                // Try to grant waiting locks
                self.grant_waiting_locks(&mut queue_guard);
                return Ok(());
            }
        }

        Err(LockError::NoLockHeld)
    }

    /// Force releases all locks held by the given transaction without mutating the transaction state.
    pub fn force_release_txn(&self, txn_id: TxnId) -> Result<(), LockError> {
        // Snapshot locks held by the transaction.
        let (table_locks, row_locks_for_txn) = {
            let txn_locks = self.txn_lock_sets.lock();
            if let Some(state) = txn_locks.get(&txn_id) {
                (state.table_locks.clone(), state.row_locks.clone())
            } else {
                return Ok(()); // Nothing to release
            }
        };

        // Release row locks first to avoid table-lock ordering issues.
        {
            let mut row_locks = self.row_locks.lock();
            for (_, rid) in row_locks_for_txn.iter() {
                if let Some(queue) = row_locks.get(rid) {
                    let mut queue_guard = queue.lock();
                    queue_guard
                        .request_queue
                        .retain(|req| req.lock().get_txn_id() != txn_id);
                    self.grant_waiting_locks(&mut queue_guard);
                }
            }
        }

        // Release table locks.
        {
            let mut table_locks_map = self.table_lock_map.lock();
            for oid in table_locks.iter() {
                if let Some(queue) = table_locks_map.get(oid) {
                    let mut queue_guard = queue.lock();
                    queue_guard
                        .request_queue
                        .retain(|req| req.lock().get_txn_id() != txn_id);
                    self.grant_waiting_locks(&mut queue_guard);
                }
            }
        }

        // Remove transaction lock state entry.
        let mut txn_locks = self.txn_lock_sets.lock();
        txn_locks.remove(&txn_id);

        Ok(())
    }

    /// Attempts to grant locks to waiting transactions
    fn grant_waiting_locks(&self, queue: &mut MutexGuard<RawMutex, LockRequestQueue>) {
        let mut i = 0;
        while i < queue.request_queue.len() {
            let request = queue.request_queue[i].clone();
            if !request.lock().is_granted() && queue.compatible_with_existing_locks(&request.lock())
            {
                request.lock().granted = true;
                // Don't increment i since we want to check the next request
            } else {
                i += 1;
            }
        }
        queue.cv.notify_all();
    }

    /// Checks if a transaction has a lock on a table
    pub fn has_table_lock(&self, txn_id: TxnId, oid: TableOidT) -> bool {
        let txn_locks = self.txn_lock_sets.lock();
        txn_locks
            .get(&txn_id)
            .is_some_and(|state| state.table_locks.contains(&oid))
    }

    /// Gets all row locks held by a transaction on a table
    pub fn get_row_locks_for_table(&self, txn_id: TxnId, oid: TableOidT) -> HashSet<RID> {
        let txn_locks = self.txn_lock_sets.lock();
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
}

impl LockCompatibilityChecker {
    pub fn are_compatible(l1: LockMode, l2: LockMode) -> bool {
        match (l1, l2) {
            // IS is compatible with IS, IX, S, SIX
            (LockMode::IntentionShared, LockMode::IntentionShared) => true,
            (LockMode::IntentionShared, LockMode::IntentionExclusive) => true,
            (LockMode::IntentionShared, LockMode::Shared) => true,
            (LockMode::IntentionShared, LockMode::SharedIntentionExclusive) => true,
            (LockMode::IntentionShared, LockMode::Exclusive) => false,

            // IX is compatible with IS, IX only
            (LockMode::IntentionExclusive, LockMode::IntentionShared) => true,
            (LockMode::IntentionExclusive, LockMode::IntentionExclusive) => true,
            (LockMode::IntentionExclusive, LockMode::Shared) => false,
            (LockMode::IntentionExclusive, LockMode::SharedIntentionExclusive) => false,
            (LockMode::IntentionExclusive, LockMode::Exclusive) => false,

            // S is compatible with IS, S
            (LockMode::Shared, LockMode::IntentionShared) => true,
            (LockMode::Shared, LockMode::Shared) => true,
            (LockMode::Shared, LockMode::IntentionExclusive) => false,
            (LockMode::Shared, LockMode::SharedIntentionExclusive) => false,
            (LockMode::Shared, LockMode::Exclusive) => false,

            // SIX is compatible with IS only
            (LockMode::SharedIntentionExclusive, LockMode::IntentionShared) => true,
            (LockMode::SharedIntentionExclusive, LockMode::IntentionExclusive) => false,
            (LockMode::SharedIntentionExclusive, LockMode::Shared) => false,
            (LockMode::SharedIntentionExclusive, LockMode::SharedIntentionExclusive) => false,
            (LockMode::SharedIntentionExclusive, LockMode::Exclusive) => false,

            // X is not compatible with anything
            (LockMode::Exclusive, _) => false,
            // Handle symmetric cases
            //(a, b) => Self::are_compatible(b, a),
        }
    }
}

impl Default for LockValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl LockValidator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn validate_lock_request(
        &self,
        txn: &Transaction,
        lock_mode: LockMode,
    ) -> Result<(), LockError> {
        match txn.get_isolation_level() {
            IsolationLevel::ReadUncommitted => self.validate_read_uncommitted(txn, lock_mode),
            IsolationLevel::ReadCommitted => self.validate_read_committed(txn, lock_mode),
            IsolationLevel::RepeatableRead => self.validate_repeatable_read(txn, lock_mode),
            IsolationLevel::Serializable => Ok(()),
            IsolationLevel::Snapshot => self.validate_snapshot(txn, lock_mode),
        }
    }

    /// Validates lock requests for READ UNCOMMITTED isolation level
    fn validate_read_uncommitted(
        &self,
        txn: &Transaction,
        lock_mode: LockMode,
    ) -> Result<(), LockError> {
        // READ UNCOMMITTED can only take X and IX locks
        match lock_mode {
            LockMode::Shared | LockMode::IntentionShared | LockMode::SharedIntentionExclusive => {
                Err(LockError::LockSharedOnReadUncommitted)
            }
            LockMode::Exclusive | LockMode::IntentionExclusive => {
                if txn.get_state() == TransactionState::Shrinking {
                    Err(LockError::LockOnShrinking)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Validates lock requests for READ COMMITTED isolation level
    fn validate_read_committed(
        &self,
        txn: &Transaction,
        lock_mode: LockMode,
    ) -> Result<(), LockError> {
        match txn.get_state() {
            TransactionState::Growing => Ok(()),
            TransactionState::Shrinking => match lock_mode {
                // Only S and IS locks allowed in SHRINKING state
                LockMode::Shared | LockMode::IntentionShared => Ok(()),
                _ => Err(LockError::LockOnShrinking),
            },
            TransactionState::Aborted => Err(LockError::TransactionAborted),
            TransactionState::Committed => Err(LockError::TransactionCommitted),
            TransactionState::Running => Ok(()),
            TransactionState::Tainted => Err(LockError::TransactionAborted),
        }
    }

    /// Validates lock requests for REPEATABLE READ isolation level
    fn validate_repeatable_read(
        &self,
        txn: &Transaction,
        _lock_mode: LockMode,
    ) -> Result<(), LockError> {
        match txn.get_state() {
            TransactionState::Growing => Ok(()),
            TransactionState::Shrinking => Err(LockError::LockOnShrinking),
            TransactionState::Aborted => Err(LockError::TransactionAborted),
            TransactionState::Committed => Err(LockError::TransactionCommitted),
            TransactionState::Running => Ok(()),
            TransactionState::Tainted => Err(LockError::TransactionAborted),
        }
    }

    /// Validates lock requests for SNAPSHOT isolation level
    fn validate_snapshot(&self, txn: &Transaction, lock_mode: LockMode) -> Result<(), LockError> {
        if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
            txn.set_state(TransactionState::Shrinking);
        }
        Ok(())
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        let mut detector = DeadlockDetector::new();
        detector.start_detection();

        Self {
            deadlock_detector: detector,
            lock_state_manager: LockStateManager::new(),
            lock_validator: LockValidator::new(),
        }
    }

    /// Attempts to acquire a table lock.
    pub fn lock_table(
        &self,
        txn: Arc<Transaction>,
        lock_mode: LockMode,
        oid: TableOidT,
    ) -> Result<bool, LockError> {
        // 1. Validate the lock request
        self.lock_validator.validate_lock_request(&txn, lock_mode)?;

        // 2. Create lock request
        let request = Arc::new(Mutex::new(LockRequest::new_table_request(
            txn.get_transaction_id(),
            lock_mode,
            oid,
        )));

        // 3. Get or create lock queue for this table
        let mut table_locks = self.lock_state_manager.table_lock_map.lock();
        let queue = table_locks
            .entry(oid)
            .or_insert_with(|| Arc::new(Mutex::new(LockRequestQueue::new())));

        // 4. Try to grant the lock
        let mut queue_guard = queue.lock();
        let granted = queue_guard.grant_lock(request.clone());

        // 5. If granted, update transaction's lock set
        if granted {
            let mut txn_locks = self.lock_state_manager.txn_lock_sets.lock();
            let lock_state = txn_locks
                .entry(txn.get_transaction_id())
                .or_insert_with(|| TxnLockState {
                    table_locks: HashSet::new(),
                    row_locks: HashSet::new(),
                });
            lock_state.table_locks.insert(oid);
        } else {
            // 6. If not granted, add edge to waits-for graph
            for existing_req in &queue_guard.request_queue {
                let existing = existing_req.lock();
                if existing.granted {
                    self.deadlock_detector
                        .add_edge(txn.get_transaction_id(), existing.txn_id);
                }
            }

            // 7. Check for deadlock
            if self.check_deadlock(txn.clone())? {
                return Ok(false);
            }
        }

        Ok(granted)
    }

    pub fn unlock_table(
        &self,
        txn: Arc<Transaction>,
        lock_mode: LockMode,
        oid: TableOidT,
    ) -> Result<bool, LockError> {
        // 1. Check if transaction holds the lock
        if !self
            .lock_state_manager
            .has_table_lock(txn.get_transaction_id(), oid)
        {
            txn.set_state(TransactionState::Aborted);
            return Err(LockError::NoLockHeld);
        }

        // 2. Check if there are any row locks before unlocking table
        let row_locks = self
            .lock_state_manager
            .get_row_locks_for_table(txn.get_transaction_id(), oid);
        if !row_locks.is_empty() {
            txn.set_state(TransactionState::Aborted);
            return Err(LockError::TableUnlockedBeforeRows);
        }

        // 3. Release the lock
        self.lock_state_manager
            .release_table_lock(txn.get_transaction_id(), oid)?;

        // 4. Update transaction state based on isolation level and lock mode
        match txn.get_isolation_level() {
            IsolationLevel::RepeatableRead => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::ReadCommitted => {
                if matches!(lock_mode, LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::ReadUncommitted => {
                if matches!(lock_mode, LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::Serializable => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::Snapshot => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
        }

        // 5. Remove edges from waits-for graph
        self.deadlock_detector.remove_edge(txn.get_transaction_id());

        Ok(true)
    }

    pub fn lock_row(
        &self,
        txn: Arc<Transaction>,
        lock_mode: LockMode,
        oid: TableOidT,
        rid: RID,
    ) -> Result<bool, LockError> {
        // 1. Validate the lock request
        self.lock_validator.validate_lock_request(&txn, lock_mode)?;

        // 2. Check if intention locks are being attempted on row
        match lock_mode {
            LockMode::IntentionShared
            | LockMode::IntentionExclusive
            | LockMode::SharedIntentionExclusive => {
                txn.set_state(TransactionState::Aborted);
                return Err(LockError::IntentionLockOnRow);
            }
            _ => {}
        }

        // 3. Check if appropriate table lock is held
        let has_appropriate_table_lock = {
            let table_locks = self.lock_state_manager.table_lock_map.lock();
            if let Some(queue) = table_locks.get(&oid) {
                let queue_guard = queue.lock();
                queue_guard.request_queue.iter().any(|req| {
                    let req = req.lock();
                    req.txn_id == txn.get_transaction_id()
                        && req.granted
                        && match (req.lock_mode, lock_mode) {
                            // For X lock on row, need X, IX, or SIX on table
                            (_, LockMode::Exclusive) => matches!(
                                req.lock_mode,
                                LockMode::Exclusive
                                    | LockMode::IntentionExclusive
                                    | LockMode::SharedIntentionExclusive
                            ),
                            // For S lock on row, need S, IS, or SIX on table
                            (_, LockMode::Shared) => matches!(
                                req.lock_mode,
                                LockMode::Shared
                                    | LockMode::IntentionShared
                                    | LockMode::SharedIntentionExclusive
                            ),
                            _ => false,
                        }
                })
            } else {
                false
            }
        };

        if !has_appropriate_table_lock {
            txn.set_state(TransactionState::Aborted);
            return Err(LockError::TableLockNotPresent);
        }

        // 4. Try to grant the lock
        let granted =
            self.lock_state_manager
                .grant_row_lock(txn.get_transaction_id(), lock_mode, rid);

        if !granted {
            // 5. If not granted, add edge to waits-for graph
            let row_locks = self.lock_state_manager.row_locks.lock();
            if let Some(queue) = row_locks.get(&rid) {
                let queue_guard = queue.lock();
                for request in &queue_guard.request_queue {
                    let req = request.lock();
                    if req.granted {
                        self.deadlock_detector
                            .add_edge(txn.get_transaction_id(), req.txn_id);
                    }
                }
            }

            // 6. Check for deadlock
            if self.check_deadlock(txn.clone())? {
                return Ok(false);
            }
        }

        Ok(granted)
    }

    pub fn unlock_row(
        &self,
        txn: Arc<Transaction>,
        lock_mode: LockMode,
        oid: TableOidT,
        rid: RID,
    ) -> Result<bool, LockError> {
        // 1. Check if transaction holds the lock
        let row_locks = self
            .lock_state_manager
            .get_row_locks_for_table(txn.get_transaction_id(), oid);
        if !row_locks.contains(&rid) {
            txn.set_state(TransactionState::Aborted);
            return Err(LockError::NoLockHeld);
        }

        // 2. Release the lock
        self.lock_state_manager
            .release_row_lock(txn.get_transaction_id(), rid)?;

        // 3. Update transaction state based on isolation level and lock mode
        match txn.get_isolation_level() {
            IsolationLevel::RepeatableRead => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::ReadCommitted => {
                if matches!(lock_mode, LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::ReadUncommitted => {
                if matches!(lock_mode, LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::Serializable => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
            IsolationLevel::Snapshot => {
                if matches!(lock_mode, LockMode::Shared | LockMode::Exclusive) {
                    txn.set_state(TransactionState::Shrinking);
                }
            }
        }

        // 4. Remove edges from waits-for graph
        self.deadlock_detector.remove_edge(txn.get_transaction_id());

        Ok(true)
    }

    pub fn unlock_all(&self) -> Result<bool, LockError> {
        // 1. Get all lock sets
        let mut txn_locks = self.lock_state_manager.txn_lock_sets.lock();

        // 2. Clear all lock queues
        {
            let mut table_locks = self.lock_state_manager.table_lock_map.lock();
            table_locks.clear();
        }
        {
            let mut row_locks = self.lock_state_manager.row_locks.lock();
            row_locks.clear();
        }

        // 3. Clear transaction lock sets
        txn_locks.clear();

        // 4. Clear deadlock detection graph
        {
            let mut waits_for = self.deadlock_detector.waits_for.lock();
            waits_for.clear();
        }

        Ok(true)
    }

    /// Force releases all locks held by the provided transaction ID without altering its state.
    pub fn force_release_txn(&self, txn_id: TxnId) -> Result<(), LockError> {
        // Remove waits-for edges for this transaction as it no longer blocks others.
        self.deadlock_detector.remove_edge(txn_id);
        self.lock_state_manager.force_release_txn(txn_id)
    }

    pub fn check_deadlock(&self, txn: Arc<Transaction>) -> Result<bool, LockError> {
        if let Some(abort_txn) = self.deadlock_detector.get_and_clear_abort_txn()
            && abort_txn == txn.get_transaction_id()
        {
            txn.set_state(TransactionState::Aborted);
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns a string representation of the current lock manager state
    pub fn debug_state(&self) -> String {
        let mut output = String::new();

        // Add table lock information
        output.push_str("=== Table Locks ===\n");
        let table_locks = self.lock_state_manager.table_lock_map.lock();
        for (oid, queue) in table_locks.iter() {
            output.push_str(&format!("Table {}:\n", oid));
            let queue_guard = queue.lock();
            for request in &queue_guard.request_queue {
                let req = request.lock();
                output.push_str(&format!(
                    "  Txn:{} Mode:{:?} Granted:{}\n",
                    req.txn_id, req.lock_mode, req.granted
                ));
            }
        }

        output
    }
}

impl Clone for LockManager {
    fn clone(&self) -> Self {
        Self {
            deadlock_detector: DeadlockDetector {
                waits_for: Arc::clone(&self.deadlock_detector.waits_for),
                latest_abort_txn: Arc::clone(&self.deadlock_detector.latest_abort_txn),
                enable_detection: Arc::clone(&self.deadlock_detector.enable_detection),
                detection_thread: None,
            },
            lock_state_manager: LockStateManager {
                table_lock_map: Mutex::new(self.lock_state_manager.table_lock_map.lock().clone()),
                row_locks: Mutex::new(self.lock_state_manager.row_locks.lock().clone()),
                txn_lock_sets: Mutex::new(self.lock_state_manager.txn_lock_sets.lock().clone()),
            },
            lock_validator: LockValidator {},
        }
    }
}

unsafe impl Send for LockManager {}
unsafe impl Sync for LockManager {}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockMode::Shared => write!(f, "S"),
            LockMode::Exclusive => write!(f, "X"),
            LockMode::IntentionShared => write!(f, "IS"),
            LockMode::IntentionExclusive => write!(f, "IX"),
            LockMode::SharedIntentionExclusive => write!(f, "SIX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use parking_lot::RawMutex;
    use parking_lot::lock_api::MutexGuard;

    pub struct TestContext {
        lock_manager: Arc<Mutex<LockManager>>,
    }

    impl TestContext {
        pub fn new() -> Self {
            initialize_logger();

            let lock_manager = Arc::new(Mutex::new(LockManager::new()));

            Self { lock_manager }
        }

        fn lock_manager(&self) -> MutexGuard<'_, RawMutex, LockManager> {
            self.lock_manager.lock()
        }
    }

    mod lock_mode_tests {
        use super::*;

        #[test]
        fn test_lock_mode_display() {
            assert_eq!(LockMode::Shared.to_string(), "S");
            assert_eq!(LockMode::Exclusive.to_string(), "X");
            assert_eq!(LockMode::IntentionShared.to_string(), "IS");
            assert_eq!(LockMode::IntentionExclusive.to_string(), "IX");
            assert_eq!(LockMode::SharedIntentionExclusive.to_string(), "SIX");
        }

        #[test]
        fn test_lock_compatibility_matrix() {
            let lock_modes = [
                LockMode::IntentionShared,
                LockMode::IntentionExclusive,
                LockMode::Shared,
                LockMode::SharedIntentionExclusive,
                LockMode::Exclusive,
            ];

            let compatibility = [
                [true, true, true, true, false],     // IS
                [true, true, false, false, false],   // IX
                [true, false, true, false, false],   // S
                [true, false, false, false, false],  // SIX
                [false, false, false, false, false], // X
            ];

            for (i, &mode1) in lock_modes.iter().enumerate() {
                for (j, &mode2) in lock_modes.iter().enumerate() {
                    assert_eq!(
                        LockCompatibilityChecker::are_compatible(mode1, mode2),
                        compatibility[i][j],
                        "Compatibility check failed for {:?} and {:?}",
                        mode1,
                        mode2
                    );
                }
            }
        }
    }

    mod lock_validator_tests {
        use super::*;

        #[test]
        fn test_isolation_level_validation() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            // Test READ UNCOMMITTED
            let txn_ru = Arc::new(Transaction::new(1, IsolationLevel::ReadUncommitted));
            txn_ru.set_state(TransactionState::Growing);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn_ru, LockMode::Exclusive)
                    .is_ok()
            );
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn_ru, LockMode::Shared)
                    .is_err()
            );

            // Test READ COMMITTED
            let txn_rc = Arc::new(Transaction::new(2, IsolationLevel::ReadCommitted));
            txn_rc.set_state(TransactionState::Growing);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn_rc, LockMode::Exclusive)
                    .is_ok()
            );
            txn_rc.set_state(TransactionState::Shrinking);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn_rc, LockMode::Shared)
                    .is_ok()
            );
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn_rc, LockMode::Exclusive)
                    .is_err()
            );
        }

        #[test]
        fn test_transaction_state_validation() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            let txn = Arc::new(Transaction::new(1, IsolationLevel::RepeatableRead));

            // Test different states
            txn.set_state(TransactionState::Growing);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn, LockMode::Exclusive)
                    .is_ok()
            );

            txn.set_state(TransactionState::Shrinking);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn, LockMode::Exclusive)
                    .is_err()
            );

            txn.set_state(TransactionState::Aborted);
            assert!(
                lock_manager
                    .lock_validator
                    .validate_lock_request(&txn, LockMode::Shared)
                    .is_err()
            );
        }
    }

    mod lock_state_manager_tests {
        use super::*;

        #[test]
        fn test_basic_lock_operations() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            assert!(
                lock_manager
                    .lock_state_manager
                    .grant_table_lock(1, LockMode::Shared, 1)
            );
            assert!(lock_manager.lock_state_manager.has_table_lock(1, 1));
            assert!(
                lock_manager
                    .lock_state_manager
                    .release_table_lock(1, 1)
                    .is_ok()
            );
            assert!(!lock_manager.lock_state_manager.has_table_lock(1, 1));
        }

        #[test]
        fn test_lock_conflicts() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            assert!(
                lock_manager
                    .lock_state_manager
                    .grant_table_lock(1, LockMode::Shared, 1)
            );
            assert!(
                !lock_manager
                    .lock_state_manager
                    .grant_table_lock(2, LockMode::Exclusive, 1)
            );
        }

        #[test]
        fn test_lock_upgrades() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            assert!(lock_manager.lock_state_manager.grant_table_lock(
                1,
                LockMode::IntentionShared,
                1
            ));
            assert!(
                lock_manager
                    .lock_state_manager
                    .grant_table_lock(1, LockMode::Shared, 1)
            );
        }
    }

    mod deadlock_detector_tests {
        use super::*;

        #[test]
        fn test_simple_cycle_detection() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            lock_manager.deadlock_detector.add_edge(1, 2);
            lock_manager.deadlock_detector.add_edge(2, 1);

            let mut abort_txn = INVALID_TXN_ID;
            assert!(lock_manager.deadlock_detector.has_cycle(&mut abort_txn));
            assert_eq!(abort_txn, 2);
        }

        #[test]
        fn test_complex_cycle_detection() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            // Create cycle: 1->2->3->1
            lock_manager.deadlock_detector.add_edge(1, 2);
            lock_manager.deadlock_detector.add_edge(2, 3);
            lock_manager.deadlock_detector.add_edge(3, 1);

            let mut abort_txn = INVALID_TXN_ID;
            assert!(lock_manager.deadlock_detector.has_cycle(&mut abort_txn));
            assert_eq!(abort_txn, 3);
        }
    }

    mod integration_tests {
        use super::*;

        #[test]
        fn test_complete_transaction_flow() {
            let ctx = TestContext::new();
            let lock_manager = ctx.lock_manager();

            let txn = Arc::new(Transaction::new(1, IsolationLevel::RepeatableRead));
            txn.set_state(TransactionState::Growing);

            // Test lock acquisition sequence
            assert!(
                lock_manager
                    .lock_table(txn.clone(), LockMode::IntentionShared, 1)
                    .unwrap()
            );
            assert!(
                lock_manager
                    .lock_table(txn.clone(), LockMode::Shared, 1)
                    .unwrap()
            );
            assert!(
                lock_manager
                    .lock_table(txn.clone(), LockMode::Exclusive, 2)
                    .unwrap()
            );

            // Verify final state
            let debug_state = lock_manager.debug_state();
            assert!(debug_state.contains("Txn:1"));
            assert!(debug_state.contains("Mode:IntentionShared"));
            assert!(debug_state.contains("Mode:Shared"));
            assert!(debug_state.contains("Mode:Exclusive"));
        }
    }
}
