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
use crate::storage::index::types::{KeyComparator, KeyType};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};

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
pub trait BPlusTreeComparatorBound<K: KeyType>: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone {}

impl<K: KeyType, T> BPlusTreeComparatorBound<K> for T where T: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone {}

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
/// - `held_internal_pages`: Active page guards held during pessimistic traversal. These are
///   only populated in pessimistic mode and represent pages we still hold write latches on.
///   When a "safe" node is encountered, these are cleared to release ancestor latches early.
///
/// # Thread Safety
///
/// `LatchContext` is `Send` if the underlying `PageGuard` is `Send`. It should only be used
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
    /// Stack of held internal page guards (for pessimistic mode only).
    /// Pages are stored from root to current position. Cleared when a safe node is found.
    held_internal_pages: Vec<PageGuard<BPlusTreeInternalPage<K, C>>>,
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
            held_internal_pages: Vec::new(),
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

    /// Hold an internal page guard (for pessimistic mode)
    pub fn hold_internal_page(&mut self, page: PageGuard<BPlusTreeInternalPage<K, C>>) {
        self.held_internal_pages.push(page);
    }

    /// Release all ancestor latches that are safe to release.
    /// 
    /// In pessimistic mode, once we determine a node is "safe" (won't propagate
    /// changes upward), we can release all ancestor latches.
    ///
    /// Note: This only clears `held_internal_pages`. The `path` is preserved
    /// as it serves as a read-only traversal record.
    pub fn release_safe_ancestors(&mut self) {
        // Release all held internal pages - they are no longer needed
        // since the current node is safe
        self.held_internal_pages.clear();
    }

    /// Release all held latches and clear the path
    pub fn release_all(&mut self) {
        self.held_internal_pages.clear();
        self.path.clear();
    }

    /// Get the number of held internal pages
    #[must_use]
    pub fn held_count(&self) -> usize {
        self.held_internal_pages.len()
    }

    /// Take ownership of all held internal pages.
    ///
    /// After calling this, `held_count()` will return 0.
    #[must_use]
    pub fn take_held_pages(&mut self) -> Vec<PageGuard<BPlusTreeInternalPage<K, C>>> {
        std::mem::take(&mut self.held_internal_pages)
    }
}

impl<K, C> Drop for LatchContext<K, C>
where
    K: BPlusTreeKeyBound,
    C: BPlusTreeComparatorBound<K>,
{
    fn drop(&mut self) {
        // Ensure all latches are released when context is dropped
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
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()> + Debug,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
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
    K: Clone + Sync + Send + Debug + 'static + Display,
    C: Fn(&K, &K) -> Ordering + Sync + Send + 'static + Clone,
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
    /// `leaf_page.read().is_safe_for(context.operation())` directly.
    pub is_safe: bool,
}

#[cfg(test)]
mod tests {
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
    fn test_latch_context_take_held_pages() {
        let mut ctx: LatchContext<i32, fn(&i32, &i32) -> Ordering> = 
            LatchContext::new(OperationType::Insert, LockingProtocol::Pessimistic);
        
        // Initially empty
        assert_eq!(ctx.held_count(), 0);
        
        // Take should return empty vec and leave context unchanged
        let pages = ctx.take_held_pages();
        assert!(pages.is_empty());
        assert_eq!(ctx.held_count(), 0);
        
        // Note: Can't easily test with actual PageGuards without buffer pool setup,
        // but we verify the method works correctly with empty state
    }
}
