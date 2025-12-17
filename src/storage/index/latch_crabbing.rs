//! Latch Crabbing (Lock Coupling) Implementation for B+ Tree
//!
//! This module provides thread-safe traversal protocols for B+ tree operations:
//!
//! ## Protocols
//!
//! ### Read Operations (Search, Range Scan)
//! - Acquire read latch on current node
//! - Acquire read latch on child
//! - Release parent read latch
//! - Continue until reaching leaf
//!
//! ### Write Operations - Optimistic Protocol
//! - Traverse down with read latches (like reads)
//! - Acquire write latch only on the leaf
//! - If operation causes split/merge, restart with pessimistic protocol
//!
//! ### Write Operations - Pessimistic Protocol  
//! - Acquire write latches going down
//! - Release ancestor write latches when current node is "safe"
//! - A node is "safe" if the operation won't propagate changes to ancestors
//!
//! ## Safety Criteria
//!
//! A node is considered "safe" when:
//! - **For Insert**: `size < max_size - 1` (room for one more key without split)
//! - **For Delete**: `size > min_size` OR node is root (can lose a key without underflow)

use crate::common::config::PageId;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::{
    b_plus_tree_internal_page::BPlusTreeInternalPage,
    b_plus_tree_leaf_page::BPlusTreeLeafPage,
};
use crate::storage::index::btree_observability;
use crate::storage::index::types::{KeyComparator, KeyType};
use log::trace;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use parking_lot::{RawRwLock, RwLock};
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::thread;

/// Type alias for the Arc-owned write guard.
/// This is returned by `RwLock::write_arc()` when using the `arc_lock` feature.
type InternalPageWriteGuard<K, C> = ArcRwLockWriteGuard<RawRwLock, BPlusTreeInternalPage<K, C>>;

/// Trait alias for B+ tree key type requirements.
///
/// This combines all the bounds needed for keys in B+ tree operations,
/// reducing repetition across generic implementations.
pub trait BPlusTreeKeyBound: KeyType + Send + Sync + Debug + Display + 'static 
    + bincode::Encode + bincode::Decode<()> {}

impl<T> BPlusTreeKeyBound for T where T: KeyType + Send + Sync + Debug + Display + 'static 
    + bincode::Encode + bincode::Decode<()> {}

/// Trait alias for B+ tree comparator requirements.
///
/// This combines all the bounds needed for key comparators in B+ tree operations.
/// Note: `KeyComparator<K>` already implies `Fn(&K, &K) -> Ordering`.
pub trait BPlusTreeComparatorBound<K: KeyType>: KeyComparator<K> + Send + Sync + 'static + Clone {}

impl<K: KeyType, T> BPlusTreeComparatorBound<K> for T where T: KeyComparator<K> + Send + Sync + 'static + Clone {}

/// A write-locked internal page that properly holds the write latch.
///
/// This struct owns the `PageGuard` (for buffer pool pin management) AND holds
/// the write lock via an `ArcRwLockWriteGuard`. The write lock is held for the
/// entire lifetime of this struct, which is essential for correct latch crabbing
/// behavior.
///
/// # Design
///
/// The struct stores:
/// - A `PageGuard` which keeps the page pinned in the buffer pool
/// - An `ArcRwLockWriteGuard` which actually holds the write lock
///
/// The `ArcRwLockWriteGuard` owns a cloned `Arc<RwLock<T>>` from the `PageGuard`,
/// so the lock is genuinely held and not just borrowed.
///
/// # Thread Safety
///
/// While this struct exists, no other thread can acquire a read or write
/// lock on the underlying page. This is the key invariant for pessimistic
/// latch crabbing.
pub struct HeldWriteLock<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    /// The page guard keeps the page pinned in the buffer pool.
    /// This must be dropped AFTER the write guard to ensure proper cleanup order.
    _page_guard: PageGuard<BPlusTreeInternalPage<K, C>>,
    /// The write guard that owns a cloned Arc and holds the write lock.
    /// Using ArcRwLockWriteGuard allows us to own the Arc while holding the lock,
    /// avoiding lifetime issues that would occur with a borrowed RwLockWriteGuard.
    write_guard: ArcRwLockWriteGuard<RawRwLock, BPlusTreeInternalPage<K, C>>,
    /// The page ID (cached for quick access without dereferencing)
    page_id: PageId,
    /// When the write lock was successfully acquired (for hold-time metrics)
    acquired_at: Instant,
}

impl<K, C> HeldWriteLock<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    /// Create a new held write lock from a PageGuard.
    ///
    /// This acquires the write lock immediately and holds it until the
    /// HeldWriteLock is dropped. The PageGuard ensures the page stays
    /// pinned in the buffer pool.
    ///
    /// # Arguments
    /// * `page_guard` - The page guard for the internal page
    pub fn new(page_guard: PageGuard<BPlusTreeInternalPage<K, C>>) -> Self {
        let page_id = page_guard.get_page_id();
        // Clone the Arc so the write guard owns it independently
        let arc = Arc::clone(page_guard.get_page());
        // Acquire the write lock with observability:
        // - periodically warn if we wait "too long" (potential deadlock/hang)
        // - optionally time out in tests to avoid infinite hangs
        let cfg = btree_observability::config();
        let start = Instant::now();
        let mut last_warn = start;
        let mut warned = false;
        let mut backoff = cfg.poll_interval;

        let write_guard = loop {
            if let Some(g) = RwLock::try_write_arc(&arc) {
                break g;
            }

            let waited = start.elapsed();

            if waited >= cfg.warn_after && last_warn.elapsed() >= cfg.warn_every {
                warned = true;
                last_warn = Instant::now();
                btree_observability::warn_slow_lock(
                    "internal",
                    "write",
                    page_id,
                    waited,
                    "HeldWriteLock::new",
                    &[],
                    0,
                );
            }

            if let Some(timeout) = cfg.timeout_after
                && waited >= timeout
            {
                btree_observability::record_internal_write_wait(waited, warned, true);
                panic!(
                    "B+tree internal write lock timed out (page_id={}, waited_ms={})",
                    page_id,
                    waited.as_millis()
                );
            }

            // Backoff to reduce CPU burn during contention/deadlock scenarios.
            thread::sleep(backoff);
            // Cap backoff to keep logs responsive.
            backoff = std::cmp::min(backoff * 2, Duration::from_millis(250));
        };

        let waited = start.elapsed();
        btree_observability::record_internal_write_wait(waited, warned, false);
        let acquired_at = Instant::now();
        Self {
            _page_guard: page_guard,
            write_guard,
            page_id,
            acquired_at,
        }
    }

    /// Try to create a new held write lock without blocking.
    ///
    /// Returns `None` if the lock is currently held by another thread.
    pub fn try_new(page_guard: PageGuard<BPlusTreeInternalPage<K, C>>) -> Option<Self> {
        let page_id = page_guard.get_page_id();
        let arc = Arc::clone(page_guard.get_page());
        let write_guard = RwLock::try_write_arc(&arc)?;
        let acquired_at = Instant::now();
        Some(Self {
            _page_guard: page_guard,
            write_guard,
            page_id,
            acquired_at,
        })
    }

    /// Get the page ID
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get a reference to the underlying page data
    #[inline]
    pub fn get(&self) -> &BPlusTreeInternalPage<K, C> {
        &self.write_guard
    }

    /// Get a mutable reference to the underlying page data
    #[inline]
    pub fn get_mut(&mut self) -> &mut BPlusTreeInternalPage<K, C> {
        &mut self.write_guard
    }
}

impl<K, C> Deref for HeldWriteLock<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    type Target = BPlusTreeInternalPage<K, C>;

    fn deref(&self) -> &Self::Target {
        &self.write_guard
    }
}

impl<K, C> DerefMut for HeldWriteLock<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.write_guard
    }
}

impl<K, C> Drop for HeldWriteLock<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    fn drop(&mut self) {
        trace!(
            "Releasing held write lock on internal page {}",
            self.page_id
        );
        btree_observability::record_internal_write_hold(self.acquired_at.elapsed());
        // The ArcRwLockWriteGuard automatically releases the lock when dropped.
        // Then _page_guard drops, unpinning the page in the buffer pool.
    }
}

/// Represents the type of operation being performed, used to determine safety criteria
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Search operation - read-only, uses read latches only
    Search,
    /// Insert operation - may cause splits
    Insert,
    /// Delete operation - may cause merges or redistributions
    Delete,
}

/// Represents whether to use optimistic or pessimistic locking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockingProtocol {
    /// Optimistic: Use read latches going down, write latch only on leaf
    /// Restart with pessimistic if operation fails
    Optimistic,
    /// Pessimistic: Use write latches going down, release when safe
    Pessimistic,
}

/// Result of an optimistic traversal attempt
#[derive(Debug)]
pub enum OptimisticResult<T> {
    /// Operation succeeded
    Success(T),
    /// Need to restart with pessimistic locking (e.g., split/merge needed)
    NeedRestart,
}

/// Tracks held latches during tree traversal.
/// 
/// This context manages the lifecycle of latches acquired during B+ tree operations,
/// ensuring proper release order and preventing deadlocks.
///
/// ## Field Semantics
///
/// - `path`: A read-only record of all page IDs visited during traversal. This is always
///   populated during traversal regardless of locking protocol, and is useful for debugging
///   and understanding the traversal path. It is NOT cleared by `release_safe_ancestors()`.
///
/// - `held_write_locks`: Active write locks held during pessimistic traversal. These are
///   only populated in pessimistic mode and represent pages we still hold write latches on.
///   When a "safe" node is encountered, these are cleared to release ancestor latches early.
///   
///   **Important**: Unlike the previous `PageGuard`-based implementation, these locks are
///   *actually held* - the write latch is maintained for the lifetime of the `HeldWriteLock`.
///
/// # Thread Safety
///
/// `LatchContext` is `Send` if the underlying `HeldWriteLock` is `Send`. It should only be used
/// by a single thread during a traversal operation.
pub struct LatchContext<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    /// The operation being performed
    operation: OperationType,
    /// The locking protocol being used
    protocol: LockingProtocol,
    /// Stack of held write locks (for pessimistic mode only).
    /// Pages are stored from root to current position. Cleared when a safe node is found.
    /// These locks are *actually held* until released or the context is dropped.
    held_write_locks: Vec<HeldWriteLock<K, C>>,
    /// Path of page IDs for tracking ancestors (read-only record, always populated).
    /// This is informational and not cleared by `release_safe_ancestors()`.
    path: Vec<PageId>,
}

impl<K, C> LatchContext<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    /// Create a new latch context for the given operation
    #[must_use]
    pub fn new(operation: OperationType, protocol: LockingProtocol) -> Self {
        Self {
            operation,
            protocol,
            held_write_locks: Vec::new(),
            path: Vec::new(),
        }
    }

    /// Get the operation type
    #[must_use]
    pub fn operation(&self) -> OperationType {
        self.operation
    }

    /// Get the locking protocol
    #[must_use]
    pub fn protocol(&self) -> LockingProtocol {
        self.protocol
    }

    /// Get the path of page IDs traversed (read-only record)
    #[must_use]
    pub fn path(&self) -> &[PageId] {
        &self.path
    }

    /// Add a page ID to the path
    pub fn push_path(&mut self, page_id: PageId) {
        self.path.push(page_id);
    }

    /// Hold a write lock on an internal page (for pessimistic mode).
    ///
    /// This method acquires a write lock on the page and holds it until
    /// either `release_safe_ancestors()` or `release_all()` is called,
    /// or the context is dropped.
    ///
    /// The PageGuard keeps the page pinned in the buffer pool, while the
    /// internal write guard holds the actual write lock.
    ///
    /// # Arguments
    /// * `page_guard` - The PageGuard for the internal page
    pub fn hold_write_lock(
        &mut self,
        page_guard: PageGuard<BPlusTreeInternalPage<K, C>>,
    ) {
        let page_id = page_guard.get_page_id();
        trace!(
            "Latch crabbing: acquiring and holding write lock on page {}, total held: {}",
            page_id,
            self.held_write_locks.len() + 1
        );
        let held_lock = HeldWriteLock::new(page_guard);
        self.held_write_locks.push(held_lock);
    }

    /// Hold an already-acquired write lock (for pessimistic mode).
    ///
    /// Use this when you've already created a HeldWriteLock and want to
    /// transfer ownership to this context.
    pub fn hold_existing_lock(&mut self, held_lock: HeldWriteLock<K, C>) {
        trace!(
            "Latch crabbing: holding existing write lock on page {}, total held: {}",
            held_lock.page_id(),
            self.held_write_locks.len() + 1
        );
        self.held_write_locks.push(held_lock);
    }

    /// Release all ancestor latches that are safe to release.
    /// 
    /// In pessimistic mode, once we determine a node is "safe" (won't propagate
    /// changes upward), we can release all ancestor latches.
    ///
    /// Note: This only clears `held_write_locks`. The `path` is preserved
    /// as it serves as a read-only traversal record.
    pub fn release_safe_ancestors(&mut self) {
        let count = self.held_write_locks.len();
        if count > 0 {
            trace!(
                "Latch crabbing: releasing {} ancestor write locks (node is safe)",
                count
            );
        }
        // Dropping the HeldWriteLock releases the write lock
        self.held_write_locks.clear();
    }

    /// Release all held latches and clear the path
    pub fn release_all(&mut self) {
        let count = self.held_write_locks.len();
        if count > 0 {
            trace!("Latch crabbing: releasing all {} held write locks", count);
        }
        self.held_write_locks.clear();
        self.path.clear();
    }

    /// Get the number of held write locks
    #[must_use]
    pub fn held_count(&self) -> usize {
        self.held_write_locks.len()
    }

    /// Get a reference to the held write locks.
    ///
    /// This allows reading or modifying the pages while keeping the locks held.
    #[must_use]
    pub fn held_locks(&self) -> &[HeldWriteLock<K, C>] {
        &self.held_write_locks
    }

    /// Get a mutable reference to the held write locks.
    ///
    /// This allows modifying the pages while keeping the locks held.
    pub fn held_locks_mut(&mut self) -> &mut [HeldWriteLock<K, C>] {
        &mut self.held_write_locks
    }

    /// Take ownership of all held write locks.
    ///
    /// After calling this, `held_count()` will return 0.
    #[must_use]
    pub fn take_held_locks(&mut self) -> Vec<HeldWriteLock<K, C>> {
        std::mem::take(&mut self.held_write_locks)
    }
}

impl<K, C> Drop for LatchContext<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    fn drop(&mut self) {
        // Log if we're dropping with held locks (might indicate a bug)
        let count = self.held_write_locks.len();
        if count > 0 {
            trace!(
                "LatchContext dropped with {} held write locks - releasing all",
                count
            );
        }
        // Ensure all latches are released when context is dropped
        // (Vec::drop will drop each HeldWriteLock, releasing the locks)
        self.release_all();
    }
}

/// Helper trait for checking node safety.
///
/// A node is "safe" when an operation on it won't require changes to ancestors.
/// This is used in latch crabbing to determine when ancestor latches can be released.
pub trait NodeSafety {
    /// Check if this node is safe for the given operation.
    /// 
    /// Safety criteria:
    /// - **Search**: Always safe (read-only)
    /// - **Insert**: Has room for at least one more key (won't split)
    /// - **Delete**: Has more than minimum keys (won't underflow) OR is root
    fn is_safe_for(&self, operation: OperationType) -> bool;
}

impl<K, V, C> NodeSafety for BPlusTreeLeafPage<K, V, C>
where
    K: BPlusTreeKeyBound,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: BPlusTreeComparatorBound<K>,
{
    fn is_safe_for(&self, operation: OperationType) -> bool {
        match operation {
            OperationType::Search => true, // Reads are always safe
            OperationType::Insert => {
                // Safe if we have room for one more key without splitting.
                // We need max_size - 1 to ensure we won't need to split.
                // Using saturating_sub to handle edge case where max_size < 2.
                let threshold = self.get_max_size().saturating_sub(1);
                self.get_size() < threshold
            }
            OperationType::Delete => {
                // Safe if we have more than minimum keys OR we're the root.
                // Root leaf can have any number of keys >= 0.
                self.is_root() || self.get_size() > self.get_min_size()
            }
        }
    }
}

impl<K, C> NodeSafety for BPlusTreeInternalPage<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    fn is_safe_for(&self, operation: OperationType) -> bool {
        match operation {
            OperationType::Search => true, // Reads are always safe
            OperationType::Insert => {
                // Safe if we have room for one more key without splitting.
                // Using saturating_sub to handle edge case where max_size < 2.
                let threshold = self.get_max_size().saturating_sub(1);
                self.get_size() < threshold
            }
            OperationType::Delete => {
                // Safe if we have more than minimum keys OR we're the root.
                // Root internal node with one child is a special case (tree height reduction).
                self.is_root() || self.get_size() > self.get_min_size()
            }
        }
    }
}

/// Result of a traversal to find a leaf page.
///
/// Contains the target leaf page, the latch context with traversal information,
/// and a cached safety status.
pub struct TraversalResult<K, V, C>
where
    K: BPlusTreeKeyBound,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: BPlusTreeComparatorBound<K>,
{
    /// The leaf page found
    pub leaf_page: PageGuard<BPlusTreeLeafPage<K, V, C>>,
    /// The latch context with path information
    pub context: LatchContext<K, C>,
    /// Whether the leaf was safe for the operation at traversal time.
    ///
    /// **Important**: This is a snapshot taken during traversal. If the leaf page
    /// is modified after traversal (e.g., by concurrent operations), this value
    /// may become stale. For the most accurate safety check, call
    /// `leaf_page.is_safe_for(context.operation())` directly on the guard.
    pub is_safe: bool,
}

impl<K, V, C> TraversalResult<K, V, C>
where
    K: BPlusTreeKeyBound,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: BPlusTreeComparatorBound<K>,
{
    /// Create a new traversal result.
    ///
    /// # Arguments
    /// * `leaf_page` - The leaf page guard found during traversal
    /// * `context` - The latch context with path information
    /// * `is_safe` - Whether the leaf was safe for the operation at traversal time
    #[must_use]
    pub fn new(
        leaf_page: PageGuard<BPlusTreeLeafPage<K, V, C>>,
        context: LatchContext<K, C>,
        is_safe: bool,
    ) -> Self {
        Self {
            leaf_page,
            context,
            is_safe,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use super::*;

    // Test LatchContext basic functionality
    #[test]
    fn test_latch_context_new() {
        let ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Insert, LockingProtocol::Optimistic);
        
        assert_eq!(ctx.operation(), OperationType::Insert);
        assert_eq!(ctx.protocol(), LockingProtocol::Optimistic);
        assert_eq!(ctx.held_count(), 0);
        assert!(ctx.path().is_empty());
    }

    #[test]
    fn test_latch_context_path_tracking() {
        let mut ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Search, LockingProtocol::Optimistic);
        
        ctx.push_path(1);
        ctx.push_path(2);
        ctx.push_path(3);
        
        assert_eq!(ctx.path(), &[1, 2, 3]);
    }

    #[test]
    fn test_latch_context_release_safe_ancestors_preserves_path() {
        let mut ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Insert, LockingProtocol::Pessimistic);
        
        ctx.push_path(1);
        ctx.push_path(2);
        
        // release_safe_ancestors should NOT clear the path
        ctx.release_safe_ancestors();
        
        assert_eq!(ctx.path(), &[1, 2]);
        assert_eq!(ctx.held_count(), 0);
    }

    #[test]
    fn test_latch_context_release_all_clears_everything() {
        let mut ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Delete, LockingProtocol::Pessimistic);
        
        ctx.push_path(1);
        ctx.push_path(2);
        
        ctx.release_all();
        
        assert!(ctx.path().is_empty());
        assert_eq!(ctx.held_count(), 0);
    }

    // Test OperationType
    #[test]
    fn test_operation_type_equality() {
        assert_eq!(OperationType::Search, OperationType::Search);
        assert_eq!(OperationType::Insert, OperationType::Insert);
        assert_eq!(OperationType::Delete, OperationType::Delete);
        
        assert_ne!(OperationType::Search, OperationType::Insert);
        assert_ne!(OperationType::Insert, OperationType::Delete);
        assert_ne!(OperationType::Search, OperationType::Delete);
    }

    #[test]
    fn test_operation_type_debug() {
        assert_eq!(format!("{:?}", OperationType::Search), "Search");
        assert_eq!(format!("{:?}", OperationType::Insert), "Insert");
        assert_eq!(format!("{:?}", OperationType::Delete), "Delete");
    }

    // Test LockingProtocol
    #[test]
    fn test_locking_protocol() {
        assert_eq!(LockingProtocol::Optimistic, LockingProtocol::Optimistic);
        assert_eq!(LockingProtocol::Pessimistic, LockingProtocol::Pessimistic);
        assert_ne!(LockingProtocol::Optimistic, LockingProtocol::Pessimistic);
    }

    // Test OptimisticResult
    #[test]
    fn test_optimistic_result_success() {
        let result: OptimisticResult<i32> = OptimisticResult::Success(42);
        match result {
            OptimisticResult::Success(v) => assert_eq!(v, 42),
            OptimisticResult::NeedRestart => panic!("Expected Success"),
        }
    }

    #[test]
    fn test_optimistic_result_need_restart() {
        let result: OptimisticResult<i32> = OptimisticResult::NeedRestart;
        match result {
            OptimisticResult::Success(_) => panic!("Expected NeedRestart"),
            OptimisticResult::NeedRestart => {} // Expected
        }
    }

    #[test]
    fn test_optimistic_result_debug() {
        let success: OptimisticResult<i32> = OptimisticResult::Success(42);
        let restart: OptimisticResult<i32> = OptimisticResult::NeedRestart;
        
        assert!(format!("{:?}", success).contains("Success"));
        assert!(format!("{:?}", restart).contains("NeedRestart"));
    }

    #[test]
    fn test_latch_context_take_held_locks() {
        let mut ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Insert, LockingProtocol::Pessimistic);
        
        // Initially empty
        assert_eq!(ctx.held_count(), 0);
        
        // Take should return empty vec and leave context unchanged
        let locks = ctx.take_held_locks();
        assert!(locks.is_empty());
        assert_eq!(ctx.held_count(), 0);
        
        // Note: Testing with actual HeldWriteLocks requires buffer pool setup.
        // The key difference from before is that HeldWriteLock *actually* holds 
        // the write lock, whereas PageGuard only provided access to acquire locks.
    }

    #[test]
    fn test_latch_context_held_locks_accessors() {
        let ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Delete, LockingProtocol::Pessimistic);
        
        // Verify held_locks() accessor works
        assert!(ctx.held_locks().is_empty());
        assert_eq!(ctx.held_count(), 0);
    }
}
