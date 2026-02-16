//! # B+ Tree Index Implementation
//!
//! This module provides a disk-based **B+ tree index** for efficient key-value lookups,
//! range scans, and ordered data access. It integrates with the buffer pool manager
//! for page-level I/O and supports concurrent access through latch crabbing protocols.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         B+ Tree Structure                               │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │                        ┌──────────────────┐                             │
//! │                        │   Header Page    │ ← Tree metadata             │
//! │                        │  (root_id, h, n) │   (height, key count)       │
//! │                        └────────┬─────────┘                             │
//! │                                 │                                       │
//! │                        ┌────────▼─────────┐                             │
//! │                        │  Internal Page   │ ← Guide posts (keys only)   │
//! │                        │   [5 | 10 | 15]  │                             │
//! │                        └──┬────┬────┬──┬──┘                             │
//! │                   ┌──────┘    │    │   └──────┐                         │
//! │                   ▼           ▼    ▼          ▼                         │
//! │              ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
//! │              │  Leaf   │→│  Leaf   │→│  Leaf   │→│  Leaf   │            │
//! │              │ [1,2,3] │ │ [5,6,7] │ │[10,11]  │ │[15,16]  │            │
//! │              └─────────┘ └─────────┘ └─────────┘ └─────────┘            │
//! │                   ↑                                                     │
//! │              Doubly-linked for range scans                              │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! - **[`BPlusTreeIndex`]**: The main generic B+ tree index structure parameterized
//!   by key type `K`, value type `V`, and comparator `C`
//! - **[`TypedBPlusTreeIndex`]**: Type-erased wrapper supporting runtime type dispatch
//! - **[`BPlusTreeError`]**: Comprehensive error type for tree operations
//! - **[`ValidationStats`]**: Statistics collected during tree validation
//!
//! ## Features
//!
//! ### Core Operations
//! - **Insert**: O(log n) insertion with automatic page splitting
//! - **Search**: O(log n) point lookups
//! - **Remove**: O(log n) deletion with automatic rebalancing (merge/redistribute)
//! - **Range Scan**: Efficient range queries using leaf page linked list
//!
//! ### Concurrency Control (Latch Crabbing)
//!
//! Two protocols are implemented for concurrent tree access:
//!
//! 1. **Optimistic Protocol** (fast path):
//!    - Acquire read latches traversing down
//!    - Acquire write latch only on leaf
//!    - Restart if leaf is unsafe (would split/merge)
//!
//! 2. **Pessimistic Protocol** (safe path):
//!    - Acquire write latches traversing down
//!    - Release ancestor latches when a "safe" node is found
//!    - Safe = node won't split (insert) or underflow (delete)
//!
//! ```text
//! Optimistic Insert (no split):     Pessimistic Insert (with split):
//! ┌───────────────────────────┐     ┌───────────────────────────┐
//! │ Read → Read → Read → Write│     │ Write → Write → Write     │
//! │   ↓      ↓      ↓     ↓   │     │   ↓       ↓       ↓       │
//! │ Root → Int → Int → Leaf   │     │ Root → Int → Leaf         │
//! │ (release immediately)     │     │ (hold until safe)         │
//! └───────────────────────────┘     └───────────────────────────┘
//! ```
//!
//! ### Tree Maintenance
//! - **Splitting**: When a node overflows, split into two and promote middle key
//! - **Merging**: When a node underflows, merge with sibling
//! - **Redistribution**: Borrow keys from sibling to avoid merge
//! - **Root Collapse**: When root has single child, make child the new root
//!
//! ### Validation & Debugging
//! - `validate_tree()`: Verify structural invariants (ordering, size, balance)
//! - `validate_leaf_links()`: Check for cycles in leaf linked list
//! - `health_check()`: Comprehensive tree validation
//! - `print_tree()`: Level-order visualization
//! - Traversal methods: in-order, pre-order, post-order
//!
//! ## Page Types Used
//!
//! | Page Type | Purpose |
//! |-----------|---------|
//! | `BPlusTreeHeaderPage` | Stores root page ID, tree height, key count, order |
//! | `BPlusTreeInternalPage` | Guide posts with keys and child pointers |
//! | `BPlusTreeLeafPage` | Actual key-value pairs, linked list pointers |
//!
//! ## Example Usage
//!
//! ```ignore
//! // Create and initialize the index
//! let mut tree = BPlusTreeIndex::<i32, RID, I32Comparator>::new(
//!     i32_comparator,
//!     metadata,
//!     buffer_pool_manager,
//! );
//! tree.init_with_order(4)?;  // Order 4 B+ tree
//!
//! // Insert key-value pairs
//! tree.insert(42, RID::new(1, 5))?;
//! tree.insert(10, RID::new(1, 2))?;
//!
//! // Point lookup
//! if let Some(rid) = tree.search(&42)? {
//!     println!("Found: {:?}", rid);
//! }
//!
//! // Range scan
//! let results = tree.range_scan(&10, &50)?;
//! for (key, value) in results {
//!     println!("{}: {:?}", key, value);
//! }
//!
//! // Remove
//! tree.remove(&42)?;
//!
//! // Concurrent access with latch crabbing
//! tree.insert_with_latch_crabbing(100, RID::new(2, 1))?;
//! tree.remove_with_latch_crabbing(&10)?;
//! ```
//!
//! ## Path-Based Parent Lookup
//!
//! Operations that may modify ancestors (split/merge) track the traversal path
//! for O(1) parent lookup instead of O(n) BFS:
//!
//! ```text
//! find_leaf_page_with_path(&key) → (LeafPage, [root_id, internal_id, ...])
//!                                            └── Path from root to parent
//! ```
//!
//! ## Generics
//!
//! The tree is generic over:
//! - `K: KeyType` - Key type (must be `bincode` serializable)
//! - `V: ValueType` - Value type (typically `RID`)
//! - `C: KeyComparator<K>` - Comparison function for keys

use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;

use log::debug;
use thiserror::Error;

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::common::config::{INVALID_PAGE_ID, PageId};
use crate::common::rid::RID;
use crate::storage::index::IndexInfo;
use crate::storage::index::latch_crabbing::{
    HeldWriteLock, LatchContext, LockingProtocol, NodeSafety, OperationType, OptimisticResult,
    TraversalResult,
};
use crate::storage::index::types::comparators::{I32Comparator, i32_comparator};
use crate::storage::index::types::{KeyComparator, KeyType, ValueType};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::b_plus_tree_header_page::BPlusTreeHeaderPage;
use crate::storage::page::page_types::b_plus_tree_internal_page::BPlusTreeInternalPage;
use crate::storage::page::page_types::b_plus_tree_leaf_page::BPlusTreeLeafPage;
use crate::types_db::type_id::TypeId;

// Error type for B+Tree operations
#[derive(Debug, Error)]
pub enum BPlusTreeError {
    #[error("Page {0} not found")]
    PageNotFound(PageId),

    #[error("Page allocation failed")]
    PageAllocationFailed,

    #[error("Invalid page type")]
    InvalidPageType,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Buffer pool error: {0}")]
    BufferPoolError(String),

    #[error("Concurrent modification error")]
    ConcurrentModificationError,

    #[error("Conversion error: {0}")]
    ConversionError(String),
}

impl From<String> for BPlusTreeError {
    fn from(error: String) -> Self {
        BPlusTreeError::ConversionError(error)
    }
}

pub struct TypedBPlusTreeIndex {
    // Index metadata, etc.
    type_id: TypeId,
    // Store the actual implementation based on type
    implementation: Box<dyn Any + Send + Sync>,
}

/// Tracks statistics during B+ tree validation
pub struct ValidationStats {
    /// Maximum depth encountered during validation
    max_depth: u32,
    /// Total number of keys counted in the tree
    total_keys: usize,
    /// Level at which leaf nodes are found (to verify all leaves are at same level)
    leaf_level: Option<u32>,
}

impl TypedBPlusTreeIndex {
    pub fn new_i32_index(
        comparator: I32Comparator,
        metadata: IndexInfo,
        buffer_pool_manager: Arc<BufferPoolManager>,
    ) -> Self {
        let index = BPlusTreeIndex::<i32, RID, I32Comparator>::new(
            comparator,
            metadata,
            buffer_pool_manager,
        );

        Self {
            type_id: TypeId::Integer,
            implementation: Box::new(index),
        }
    }

    /// Get the type ID of this index
    pub fn get_type_id(&self) -> TypeId {
        self.type_id
    }

    /// Get a reference to the underlying implementation
    pub fn get_implementation(&self) -> &Box<dyn Any + Send + Sync> {
        &self.implementation
    }

    /// Get a mutable reference to the underlying implementation
    pub fn get_implementation_mut(&mut self) -> &mut Box<dyn Any + Send + Sync> {
        &mut self.implementation
    }

    /// Attempt to downcast to the specific index type
    pub fn as_i32_index(&self) -> Option<&BPlusTreeIndex<i32, RID, I32Comparator>> {
        if self.type_id == TypeId::Integer {
            self.implementation
                .downcast_ref::<BPlusTreeIndex<i32, RID, I32Comparator>>()
        } else {
            None
        }
    }

    /// Attempt to downcast to the specific index type (mutable)
    pub fn as_i32_index_mut(&mut self) -> Option<&mut BPlusTreeIndex<i32, RID, I32Comparator>> {
        if self.type_id == TypeId::Integer {
            self.implementation
                .downcast_mut::<BPlusTreeIndex<i32, RID, I32Comparator>>()
        } else {
            None
        }
    }
}

/// Main B+ tree index structure
pub struct BPlusTreeIndex<K, V, C>
where
    K: KeyType + Send + Sync + Debug + Display,
    V: ValueType + Send + Sync,
    C: KeyComparator<K> + Send + Sync,
{
    /// The key comparator
    comparator: C,
    /// Index metadata
    metadata: Box<IndexInfo>,
    /// Buffer pool manager for page operations
    buffer_pool_manager: Arc<BufferPoolManager>,
    /// ID of the header page for this B+ tree
    header_page_id: PageId,
    /// Marker for generic types
    _marker: PhantomData<(K, V)>,
}

impl<K, V, C> BPlusTreeIndex<K, V, C>
where
    K: KeyType
        + Send
        + Sync
        + Debug
        + Display
        + 'static
        + serde::Serialize
        + serde::de::DeserializeOwned,
    V: ValueType
        + Send
        + Sync
        + 'static
        + PartialEq
        + serde::Serialize
        + serde::de::DeserializeOwned,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
{
    /// Create a new B+ Tree index
    pub fn new(
        comparator: C,
        metadata: IndexInfo,
        buffer_pool_manager: Arc<BufferPoolManager>,
    ) -> Self {
        Self {
            comparator,
            metadata: Box::new(metadata),
            buffer_pool_manager,
            header_page_id: INVALID_PAGE_ID,
            _marker: PhantomData,
        }
    }

    /// Initialize a new B+ tree with specified order
    pub fn init_with_order(&mut self, order: u32) -> Result<(), BPlusTreeError> {
        // If header_page_id is valid, tree is already initialized
        if self.header_page_id != INVALID_PAGE_ID {
            return Ok(());
        }

        // Allocate a new header page
        let header_page = self
            .buffer_pool_manager
            .new_page::<BPlusTreeHeaderPage>()
            .ok_or(BPlusTreeError::PageAllocationFailed)?;

        // Set initial values for the header
        {
            let mut header = header_page.write();
            header.set_root_page_id(INVALID_PAGE_ID);
            header.set_tree_height(0);
            header.set_num_keys(0);
            header.set_order(order);
            // The PageGuard will handle marking as dirty and unpinning when dropped
        }

        // Store the header page ID for future reference
        self.header_page_id = header_page.get_page_id();

        // The page will be automatically unpinned when header_page is dropped
        Ok(())
    }

    /// Initialize a new B+ tree with default order
    pub fn init(&mut self) -> Result<(), BPlusTreeError> {
        // Default order of 4
        self.init_with_order(4)
    }

    /// Calculate the maximum size of leaf nodes based on order
    fn calculate_leaf_max_size(&self) -> usize {
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return 0, // Default to 0 if header page not found
        };

        let order = header_page.read().get_order();
        order as usize
    }

    /// Calculate the maximum size of internal nodes based on order
    fn calculate_internal_max_size(&self) -> usize {
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return 0, // Default to 0 if header page not found
        };

        let order = header_page.read().get_order();
        order as usize
    }

    /// Insert a key-value pair into the B+ tree
    ///
    /// # Thread Safety
    /// This implementation provides basic thread safety through page-level locking,
    /// but does not implement full latch crabbing. For high-concurrency workloads,
    /// consider using external synchronization or a concurrent B+ tree variant.
    pub fn insert(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // If tree is not initialized, initialize it first
        if self.header_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError(
                "Tree not initialized".to_string(),
            ));
        }

        // Read header info and release immediately to reduce contention
        let root_page_id = {
            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
                })?;
            header_page.read().get_root_page_id()
        };

        // If tree is empty (root is INVALID_PAGE_ID), create first leaf page as root
        if root_page_id == INVALID_PAGE_ID {
            return self.insert_into_empty_tree(key, value);
        }

        // For existing tree, find the leaf page and track the path for potential splits
        let (leaf_page, path) = self.find_leaf_page_with_path(&key)?;

        // Handle the key insertion
        let mut inserted = false;
        let mut need_split = false;
        let mut new_key_added = false;

        {
            let mut leaf_write = leaf_page.write();

            // Check if key already exists
            let key_index = leaf_write.find_key_index(&key);
            if key_index < leaf_write.get_size() {
                // Check if key at this position matches our key
                if let Some(existing_key) = leaf_write.get_key_at(key_index)
                    && (self.comparator)(&key, existing_key) == Ordering::Equal
                {
                    // Update existing value
                    leaf_write.set_value_at(key_index, value.clone());
                    inserted = true;
                    // No new key added, just updated existing value
                }
            }

            // If key doesn't exist yet
            if !inserted {
                // If leaf has space, insert directly
                if leaf_write.get_size() < self.calculate_leaf_max_size() {
                    leaf_write.insert_key_value(key.clone(), value.clone());
                    inserted = true;
                    new_key_added = true; // We added a new key
                } else {
                    // Mark for split
                    need_split = true;
                }
            }
        }

        // If we inserted without splitting
        if inserted {
            // Update num_keys in header if we added a new key
            if new_key_added {
                self.increment_num_keys()?;
            }
            return Ok(());
        }

        // If leaf is full, we need to split
        if need_split {
            // First insert the key-value pair into the leaf page (allowing overflow)
            {
                let mut leaf_write = leaf_page.write();
                leaf_write.insert_key_value_with_overflow(key, value);
            }

            // Then split the page, passing the path for O(1) parent lookup
            self.split_leaf_page(&leaf_page, &path)?;

            // Update num_keys in header
            self.increment_num_keys()?;
        }

        Ok(())
    }

    /// Helper method to insert into an empty tree (creates the first root leaf)
    fn insert_into_empty_tree(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // Allocate a new leaf page with the comparator
        let comparator = self.comparator.clone();
        let leaf_max_size = self.calculate_leaf_max_size();
        let new_leaf_page = self
            .buffer_pool_manager
            .new_page_with_options(|page_id| {
                BPlusTreeLeafPage::<K, V, C>::new_with_options(page_id, leaf_max_size, comparator)
            })
            .ok_or(BPlusTreeError::PageAllocationFailed)?;

        let new_page_id = new_leaf_page.get_page_id();

        // Initialize the leaf page
        {
            let mut leaf_write = new_leaf_page.write();
            leaf_write.insert_key_value(key, value);
            leaf_write.set_root_status(true);
        }

        // Update the header page with the new root
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        {
            let mut header_write = header_page.write();
            header_write.set_root_page_id(new_page_id);
            header_write.set_tree_height(1);
            header_write.set_num_keys(1);
        }

        Ok(())
    }

    /// Helper method to atomically increment num_keys in the header
    fn increment_num_keys(&self) -> Result<(), BPlusTreeError> {
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let mut header_write = header_page.write();
        let current = header_write.get_num_keys();
        header_write.set_num_keys(current + 1);
        Ok(())
    }

    /// Search for a key in the B+ tree
    pub fn search(&self, key: &K) -> Result<Option<V>, BPlusTreeError> {
        // Check if the tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }

        // Find the leaf page that would contain this key
        let leaf_page_guard = match self.find_leaf_page(key) {
            Ok(page) => page,
            Err(BPlusTreeError::KeyNotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        let leaf_page_read = leaf_page_guard.read();
        // Search for the key in the leaf page
        let key_index = leaf_page_read.find_key_index(key);

        // Check if key exists in the leaf
        if key_index < leaf_page_read.get_size() {
            let key_at_index = leaf_page_read.get_key_at(key_index).unwrap();
            if (self.comparator)(key, key_at_index) == Ordering::Equal {
                // Key found, return the value
                return match leaf_page_read.get_value_at(key_index) {
                    Some(value) => Ok(Some(value.clone())),
                    None => Ok(None),
                };
            }
        }

        // Key not found
        Ok(None)
    }

    /// Remove a key-value pair from the B+ tree
    pub fn remove(&self, key: &K) -> Result<bool, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(false);
        }

        // Fetch the header page and read basic info
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".into()))?;
        let (root_page_id, tree_height, mut num_keys) = {
            let header = header_page.read();
            if header.is_empty() {
                return Ok(false);
            }
            (
                header.get_root_page_id(),
                header.get_tree_height(),
                header.get_num_keys(),
            )
        };
        drop(header_page); // Release header page

        // If tree is empty, nothing to remove
        if root_page_id == INVALID_PAGE_ID {
            return Ok(false);
        }

        // Find the leaf page containing the key, tracking path for potential rebalancing
        let (leaf_page, path) = match self.find_leaf_page_with_path(key) {
            Ok(result) => result,
            Err(BPlusTreeError::KeyNotFound) => return Ok(false),
            Err(e) => return Err(e),
        };

        // Search for the key in the leaf
        let mut leaf = leaf_page.write();
        let key_index = leaf.find_key_index(key);
        if key_index >= leaf.get_size() {
            return Ok(false); // Key not found
        }
        let existing_key = leaf.get_key_at(key_index).unwrap();
        if (self.comparator)(key, existing_key) != Ordering::Equal {
            return Ok(false); // Key not found
        }

        // Remove the key-value pair
        leaf.remove_key_value_at(key_index);
        num_keys = num_keys.saturating_sub(1); // Protect against underflow just in case

        // Update header to decrement num_keys
        self.update_header(root_page_id, tree_height, num_keys)?;

        // If leaf is root and now empty, update tree to be empty
        if leaf.is_root() && leaf.get_size() == 0 {
            self.update_header(INVALID_PAGE_ID, 0, 0)?;
            return Ok(true);
        }

        // Check if the leaf page needs rebalancing
        let leaf_page_id = leaf_page.get_page_id();
        let leaf_size = leaf.get_size();
        let min_size = self.calculate_leaf_max_size() / 2;
        let needs_rebalancing = !leaf.is_root() && leaf_size < min_size;

        // Release the lock on the leaf page
        drop(leaf);
        // Explicitly drop the leaf page guard to ensure it's unpinned
        drop(leaf_page);

        // Perform rebalancing if needed
        if needs_rebalancing {
            // Call the rebalancing logic with the path for O(1) parent lookup
            let _ = self.check_and_handle_underflow(leaf_page_id, true, &path)?;
        }

        Ok(true)
    }

    /// Custom range scan to find all keys in a given range [start, end]
    pub fn range_scan(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, BPlusTreeError> {
        // Validate input range
        if (self.comparator)(start, end) == Ordering::Greater {
            return Ok(Vec::new()); // Empty range, return empty vector
        }

        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(Vec::new());
        }

        // Initialize result vector
        let mut result = Vec::new();

        // Find the leaf containing the start key
        let mut current_leaf_guard = match self.find_leaf_page(start) {
            Ok(page) => page,
            Err(BPlusTreeError::KeyNotFound) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        loop {
            let next_page_id;
            {
                let leaf_page = current_leaf_guard.read();
                // Find the index of the first key >= start
                let mut key_index = leaf_page.find_key_index(start);
                let size = leaf_page.get_size();

                // Scan through keys in this leaf
                while key_index < size {
                    if let Some(key) = leaf_page.get_key_at(key_index) {
                        // Check if we've exceeded the end of the range
                        if (self.comparator)(key, end) == Ordering::Greater {
                            return Ok(result); // Done with range scan
                        }

                        // Add key-value pair to result if value exists
                        if let Some(value) = leaf_page.get_value_at(key_index) {
                            result.push((key.clone(), value.clone()));
                        }
                    }

                    key_index += 1;
                }

                // Get next page id before dropping the read guard
                next_page_id = match leaf_page.get_next_page_id() {
                    Some(id) if id != INVALID_PAGE_ID => id,
                    _ => break, // No more leaves or invalid next page
                };
            } // Read guard is dropped here

            // Explicitly drop the current page guard before fetching the next one
            drop(current_leaf_guard);

            // Get the next leaf page
            current_leaf_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(next_page_id)
                .ok_or(BPlusTreeError::PageNotFound(next_page_id))?;
        }

        Ok(result)
    }

    /// Find the leaf page that should contain the key
    fn find_leaf_page(
        &self,
        key: &K,
    ) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // Use the path-tracking version but discard the path
        let (leaf_page, _path) = self.find_leaf_page_with_path(key)?;
        Ok(leaf_page)
    }

    /// Find the leaf page that should contain the key, also returning the path of ancestor page IDs.
    ///
    /// The path is ordered from root to the parent of the leaf (not including the leaf itself).
    /// This enables O(1) parent lookups instead of O(n) BFS traversal.
    #[allow(clippy::type_complexity)] // Return type reflects distinct semantic components
    fn find_leaf_page_with_path(
        &self,
        key: &K,
    ) -> Result<(PageGuard<BPlusTreeLeafPage<K, V, C>>, Vec<PageId>), BPlusTreeError> {
        // Fetch the header page to get the root_page_id and tree_height
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let (root_page_id, tree_height) = {
            let header = header_page.read();
            (header.get_root_page_id(), header.get_tree_height())
        };

        // Drop header page early to avoid holding it during traversal
        drop(header_page);

        // Check if root_page_id is valid
        if root_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::KeyNotFound);
        }

        // If tree height is 1, the root is a leaf (no ancestors in path)
        if tree_height == 1 {
            let leaf = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(root_page_id)
                .ok_or(BPlusTreeError::PageNotFound(root_page_id))?;
            return Ok((leaf, Vec::new()));
        }

        // Track the path of ancestors (internal pages visited)
        let mut path = Vec::with_capacity((tree_height - 1) as usize);

        // Traverse down to leaf using explicit depth tracking
        let mut current_page_id = root_page_id;
        let mut current_depth = 1; // Start at depth 1 (root level)

        while current_depth < tree_height {
            // Record this internal page in the path
            path.push(current_page_id);

            // At this depth, we're at an internal page
            let internal_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id)
                .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

            // Find the child page id for the key
            let child_page_id = {
                let internal = internal_page.read();
                internal.find_child_for_key(key)
            };

            // Get the child page id before dropping the internal page
            let next_page_id = child_page_id.ok_or(BPlusTreeError::InvalidPageType)?;

            // Drop the internal page before continuing to next level
            drop(internal_page);

            current_page_id = next_page_id;
            current_depth += 1;
        }

        // At this point, current_depth == tree_height, so current_page_id is a leaf
        let leaf = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id)
            .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

        Ok((leaf, path))
    }

    /// Get the parent page ID from a pre-computed path.
    ///
    /// The path should be ordered from root to ancestors (as returned by find_leaf_page_with_path).
    /// Returns the last element of the path (immediate parent) or an error if the path is empty.
    fn get_parent_from_path(path: &[PageId]) -> Result<PageId, BPlusTreeError> {
        path.last().copied().ok_or_else(|| {
            BPlusTreeError::BufferPoolError("Path is empty - node is root".to_string())
        })
    }

    /// Get the path to a parent (all ancestors except the immediate parent).
    ///
    /// Useful for recursive operations that need to propagate up the tree.
    fn get_grandparent_path(path: &[PageId]) -> Vec<PageId> {
        if path.len() <= 1 {
            Vec::new()
        } else {
            path[..path.len() - 1].to_vec()
        }
    }

    // ============================================================================
    // LATCH CRABBING IMPLEMENTATION
    // ============================================================================

    /// Traverse to leaf page using optimistic latch crabbing protocol.
    ///
    /// This method acquires read latches going down, then upgrades to write latch
    /// only on the leaf page. If the leaf is not safe for the operation, the caller
    /// should retry with pessimistic protocol.
    ///
    /// # Protocol
    /// 1. Acquire read latch on header, read root_page_id and tree_height
    /// 2. Release header latch
    /// 3. For each level: acquire read latch on child, release parent
    /// 4. At leaf: acquire write latch (for Insert/Delete) or keep read (for Search)
    ///
    /// # Returns
    /// - `Ok(TraversalResult)` with the leaf page, context, and safety status
    /// - `Err` if traversal fails
    fn traverse_optimistic(
        &self,
        key: &K,
        operation: OperationType,
    ) -> Result<TraversalResult<K, V, C>, BPlusTreeError> {
        debug!(
            "Starting optimistic traversal for key {:?}, operation {:?}",
            key, operation
        );

        let mut context = LatchContext::new(operation, LockingProtocol::Optimistic);

        // Step 1: Read header to get root and height
        let (root_page_id, tree_height) = {
            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError(format!(
                        "Failed to fetch header page {}",
                        self.header_page_id
                    ))
                })?;
            let header = header_page.read();
            (header.get_root_page_id(), header.get_tree_height())
            // Header page guard dropped here, releasing read latch
        };

        // Check if tree is empty
        if root_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::KeyNotFound);
        }

        // Step 2: If tree height is 1, root is a leaf
        if tree_height == 1 {
            let leaf = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(root_page_id)
                .ok_or(BPlusTreeError::PageNotFound(root_page_id))?;

            let is_safe = {
                let leaf_read = leaf.read();
                leaf_read.is_safe_for(operation)
            };

            debug!(
                "Optimistic traversal reached root leaf, is_safe={}",
                is_safe
            );

            return Ok(TraversalResult {
                leaf_page: leaf,
                context,
                is_safe,
            });
        }

        // Step 3: Traverse internal pages with read latches
        let mut current_page_id = root_page_id;
        let mut current_depth = 1;

        while current_depth < tree_height {
            context.push_path(current_page_id);

            // Fetch internal page with read latch
            let internal_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id)
                .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

            // Find child while holding read latch
            let child_page_id = {
                let internal = internal_page.read();
                internal.find_child_for_key(key)
            };
            // Read latch released when internal_page goes out of scope

            let next_page_id = child_page_id.ok_or(BPlusTreeError::InvalidPageType)?;

            // Drop internal page guard (releases read latch)
            drop(internal_page);

            current_page_id = next_page_id;
            current_depth += 1;
        }

        // Step 4: Fetch leaf page
        let leaf = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id)
            .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

        let is_safe = {
            let leaf_read = leaf.read();
            leaf_read.is_safe_for(operation)
        };

        debug!(
            "Optimistic traversal reached leaf {}, is_safe={}",
            current_page_id, is_safe
        );

        Ok(TraversalResult {
            leaf_page: leaf,
            context,
            is_safe,
        })
    }

    /// Traverse to leaf page using pessimistic latch crabbing protocol.
    ///
    /// This method acquires write latches going down, but releases ancestor
    /// latches once a "safe" node is found.
    ///
    /// # Protocol
    /// 1. Acquire read latch on header, read root_page_id and tree_height
    /// 2. For each internal node:
    ///    - Acquire write latch on current node
    ///    - Check if node is safe for the operation
    ///    - If safe: release all ancestor latches and drop current latch
    ///    - If not safe: keep holding current latch (store in context)
    /// 3. At leaf: return with any held ancestor latches
    ///
    /// # Thread Safety
    ///
    /// This implementation uses `HeldWriteLock` to properly hold write latches.
    /// Unlike read guards which are temporarily acquired and released, the
    /// `HeldWriteLock` maintains the write lock for its entire lifetime.
    ///
    /// # Returns
    /// - `Ok(TraversalResult)` with the leaf page, context with held ancestors, and safety status
    /// - `Err` if traversal fails
    ///
    /// **NOTE**: This is intentionally `pub` to support black-box integration
    /// tests that validate latch crabbing behavior across threads. It is not
    /// considered a stable public API.
    #[doc(hidden)]
    pub fn traverse_pessimistic(
        &self,
        key: &K,
        operation: OperationType,
    ) -> Result<TraversalResult<K, V, C>, BPlusTreeError> {
        debug!(
            "Starting pessimistic traversal for key {:?}, operation {:?}",
            key, operation
        );

        let mut context = LatchContext::new(operation, LockingProtocol::Pessimistic);

        // Step 1: Read header to get root and height
        // We use read latch for header since we don't modify it during traversal
        let (root_page_id, tree_height) = {
            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError(format!(
                        "Failed to fetch header page {}",
                        self.header_page_id
                    ))
                })?;
            let header = header_page.read();
            (header.get_root_page_id(), header.get_tree_height())
        };

        if root_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::KeyNotFound);
        }

        // Step 2: If tree height is 1, root is a leaf
        if tree_height == 1 {
            let leaf = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(root_page_id)
                .ok_or(BPlusTreeError::PageNotFound(root_page_id))?;

            // For pessimistic mode, we always consider safe since we hold write latch
            debug!("Pessimistic traversal reached root leaf");

            return Ok(TraversalResult {
                leaf_page: leaf,
                context,
                is_safe: true, // Root leaf is always safe (can have any size)
            });
        }

        // Step 3: Traverse internal pages with write latches
        let mut current_page_id = root_page_id;
        let mut current_depth = 1;

        debug!(
            "Starting internal traversal: root={}, tree_height={}",
            root_page_id, tree_height
        );

        while current_depth < tree_height {
            context.push_path(current_page_id);

            // Fetch internal page
            let internal_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id)
                .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

            // Create a HeldWriteLock to properly hold the write latch.
            // This acquires the write lock and holds it for the lifetime of held_lock.
            let held_lock = HeldWriteLock::new(internal_page);

            // Check if this node is safe and find the child (while holding write latch)
            let is_safe = held_lock.is_safe_for(operation);
            let child_page_id = held_lock.find_child_for_key(key);

            let next_page_id = child_page_id.ok_or(BPlusTreeError::InvalidPageType)?;

            // Decide whether to hold or release the current node's latch
            if is_safe {
                // Node is safe - release all ancestor latches
                debug!(
                    "Node {} is safe, releasing {} ancestors",
                    current_page_id,
                    context.held_count()
                );
                context.release_safe_ancestors();
                debug!(
                    "Released ancestors, about to drop held_lock for page {}",
                    current_page_id
                );
                // Explicitly drop held_lock to release the write latch on current node immediately.
                // Without this explicit drop, the lock would only be released at the end of the
                // loop iteration, which could cause issues if we need to access this page again.
                drop(held_lock);
                debug!("Dropped held_lock for page {}", current_page_id);
            } else {
                // Node is NOT safe - keep holding it for potential modifications
                debug!(
                    "Node {} is NOT safe, holding write latch (ancestors held: {})",
                    current_page_id,
                    context.held_count()
                );
                // Transfer ownership of the held lock to the context
                context.hold_existing_lock(held_lock);
            }

            current_page_id = next_page_id;
            current_depth += 1;
            debug!(
                "Moving to next level: page={}, depth={}, tree_height={}",
                current_page_id, current_depth, tree_height
            );
        }

        debug!(
            "Exited internal traversal loop, about to fetch leaf page {}",
            current_page_id
        );

        // Step 4: Fetch leaf page
        let leaf = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id)
            .ok_or(BPlusTreeError::PageNotFound(current_page_id))?;

        debug!(
            "Fetched leaf page {}, about to acquire write lock",
            current_page_id
        );

        // Check if leaf is safe
        let is_safe = {
            let leaf_write = leaf.write();
            debug!("Acquired write lock on leaf page {}", current_page_id);
            leaf_write.is_safe_for(operation)
        };

        if is_safe {
            debug!(
                "Leaf {} is safe, releasing {} ancestors",
                current_page_id,
                context.held_count()
            );
            context.release_safe_ancestors();
        }

        debug!(
            "Pessimistic traversal reached leaf {}, ancestors still held: {}",
            current_page_id,
            context.held_count()
        );

        Ok(TraversalResult {
            leaf_page: leaf,
            context,
            is_safe,
        })
    }

    /// Insert a key-value pair using latch crabbing protocol.
    ///
    /// This method uses optimistic latch crabbing for insertions that don't cause splits.
    /// When splits are needed, it falls back to the standard insert path which provides
    /// consistent behavior.
    ///
    /// # Thread Safety
    /// - **Optimistic case (no split)**: Uses read latches going down, write latch only on leaf.
    ///   This is safe for concurrent reads and non-splitting writes.
    /// - **Pessimistic case (split needed)**: Falls back to standard insert which provides
    ///   page-level locking during tree modifications.
    ///
    /// # Note
    /// For fully concurrent access with guaranteed isolation, consider using external
    /// synchronization or a lock-based wrapper around the tree.
    pub fn insert_with_latch_crabbing(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        if self.header_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError(
                "Tree not initialized".to_string(),
            ));
        }

        // Check if tree is empty first
        let root_page_id = {
            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
                })?;
            header_page.read().get_root_page_id()
        };

        if root_page_id == INVALID_PAGE_ID {
            return self.insert_into_empty_tree(key, value);
        }

        // Try optimistic insert first (fast path for non-splitting inserts)
        match self.try_optimistic_insert(key.clone(), value.clone())? {
            OptimisticResult::Success(()) => Ok(()),
            OptimisticResult::NeedRestart => {
                // Fall back to standard insert which handles splits correctly
                debug!("Optimistic insert not safe, using standard insert path");
                self.insert(key, value)
            },
        }
    }

    /// Attempt optimistic insert (read latches down, write latch on leaf only)
    ///
    /// This is the fast path for insertions that don't cause splits.
    fn try_optimistic_insert(
        &self,
        key: K,
        value: V,
    ) -> Result<OptimisticResult<()>, BPlusTreeError> {
        let traversal = self.traverse_optimistic(&key, OperationType::Insert)?;

        if !traversal.is_safe {
            // Leaf would split, need to use standard insert path
            return Ok(OptimisticResult::NeedRestart);
        }

        // Leaf is safe (won't split), perform insert under write latch
        let leaf_page = traversal.leaf_page;

        {
            let mut leaf = leaf_page.write();

            // Safety is a snapshot taken during traversal; re-check it under the write latch.
            // If the leaf is no longer safe, restart and fall back to the standard insert path.
            if !leaf.is_safe_for(OperationType::Insert) {
                return Ok(OptimisticResult::NeedRestart);
            }

            // Check if key already exists
            let key_index = leaf.find_key_index(&key);
            if key_index < leaf.get_size()
                && let Some(existing_key) = leaf.get_key_at(key_index)
                && (self.comparator)(&key, existing_key) == Ordering::Equal
            {
                // Update existing value
                leaf.set_value_at(key_index, value);
                return Ok(OptimisticResult::Success(()));
            }

            // Insert new key-value pair
            let inserted = leaf.insert_key_value(key, value);
            if !inserted {
                // Leaf was unexpectedly full (concurrent change or corrupted invariants).
                // Restart so the caller can use the standard insert (which handles splits).
                return Ok(OptimisticResult::NeedRestart);
            }
        }

        // If we got here, we successfully inserted a new key (not an update).
        self.increment_num_keys()?;

        Ok(OptimisticResult::Success(()))
    }

    /// Remove a key-value pair using latch crabbing protocol.
    ///
    /// This method uses optimistic latch crabbing for removals that don't cause underflow.
    /// When rebalancing is needed, it falls back to the standard remove path.
    ///
    /// # Thread Safety
    /// - **Optimistic case (no underflow)**: Uses read latches going down, write latch only on leaf.
    /// - **Pessimistic case (underflow)**: Falls back to standard remove which handles rebalancing.
    pub fn remove_with_latch_crabbing(&self, key: &K) -> Result<bool, BPlusTreeError> {
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(false);
        }

        // Check if tree is empty
        {
            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
                })?;
            let header = header_page.read();
            if header.is_empty() {
                return Ok(false);
            }
        }

        // Try optimistic remove first (fast path for non-underflowing removals)
        match self.try_optimistic_remove(key)? {
            OptimisticResult::Success(removed) => Ok(removed),
            OptimisticResult::NeedRestart => {
                // Fall back to standard remove which handles rebalancing correctly
                debug!("Optimistic remove not safe, using standard remove path");
                self.remove(key)
            },
        }
    }

    /// Attempt optimistic remove (read latches down, write latch on leaf only)
    ///
    /// This is the fast path for removals that don't cause underflow.
    fn try_optimistic_remove(&self, key: &K) -> Result<OptimisticResult<bool>, BPlusTreeError> {
        let traversal = match self.traverse_optimistic(key, OperationType::Delete) {
            Ok(t) => t,
            Err(BPlusTreeError::KeyNotFound) => return Ok(OptimisticResult::Success(false)),
            Err(e) => return Err(e),
        };

        if !traversal.is_safe {
            // Leaf would underflow, need to use standard remove path
            return Ok(OptimisticResult::NeedRestart);
        }

        // Leaf is safe (won't underflow), perform remove under write latch
        let leaf_page = traversal.leaf_page;

        {
            let mut leaf = leaf_page.write();

            // Find the key
            let key_index = leaf.find_key_index(key);
            if key_index >= leaf.get_size() {
                return Ok(OptimisticResult::Success(false));
            }

            if let Some(existing_key) = leaf.get_key_at(key_index) {
                if (self.comparator)(key, existing_key) != Ordering::Equal {
                    return Ok(OptimisticResult::Success(false));
                }
            } else {
                return Ok(OptimisticResult::Success(false));
            }

            // Remove the key
            leaf.remove_key_value_at(key_index);
        }

        // Decrement key count
        self.decrement_num_keys()?;

        Ok(OptimisticResult::Success(true))
    }

    /// Helper method to decrement num_keys in the header
    fn decrement_num_keys(&self) -> Result<(), BPlusTreeError> {
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let mut header = header_page.write();
        let current = header.get_num_keys();
        header.set_num_keys(current.saturating_sub(1));
        Ok(())
    }

    // ============================================================================
    // END LATCH CRABBING IMPLEMENTATION
    // ============================================================================

    /// Split a leaf page when it becomes full
    ///
    /// # Arguments
    /// * `leaf_page` - The leaf page to split
    /// * `path` - The path of ancestor page IDs from root to parent of this leaf
    fn split_leaf_page(
        &self,
        leaf_page: &PageGuard<BPlusTreeLeafPage<K, V, C>>,
        path: &[PageId],
    ) -> Result<(), BPlusTreeError> {
        // Get the max size based on order
        let leaf_max_size = self.calculate_leaf_max_size();
        if leaf_max_size == 0 {
            return Err(BPlusTreeError::BufferPoolError(
                "Invalid leaf max size".to_string(),
            ));
        }

        // Allocate a new leaf page with the comparator
        let comparator = self.comparator.clone();
        let new_page = self
            .buffer_pool_manager
            .new_page_with_options(|page_id| {
                BPlusTreeLeafPage::<K, V, C>::new_with_options(page_id, leaf_max_size, comparator)
            })
            .ok_or(BPlusTreeError::PageAllocationFailed)?;

        let new_page_id = new_page.get_page_id();

        // Perform the split operation
        let separator_key;
        {
            // Get write access to both pages
            let mut leaf_write = leaf_page.write();
            let mut new_leaf_write = new_page.write();

            // Initialize the new leaf page if needed
            new_leaf_write.set_root_status(false);
            new_leaf_write.set_next_page_id(None);

            // Determine split point - ceiling(max_size/2)
            let split_point = leaf_max_size.div_ceil(2);
            let current_size = leaf_write.get_size();

            // Copy keys/values after split point to the new page
            for i in split_point..current_size {
                if let (Some(key), Some(value)) =
                    (leaf_write.get_key_at(i), leaf_write.get_value_at(i))
                {
                    new_leaf_write.insert_key_value(key.clone(), value.clone());
                }
            }

            // Update the linked list structure
            new_leaf_write.set_next_page_id(leaf_write.get_next_page_id());
            leaf_write.set_next_page_id(Some(new_page_id));

            // Get the first key in the new page to use as separator
            separator_key = match new_leaf_write.get_key_at(0) {
                Some(key) => key.clone(),
                None => return Err(BPlusTreeError::InvalidPageType), // Should never happen
            };

            // Remove the moved keys from the original leaf
            for _i in split_point..current_size {
                leaf_write.remove_key_value_at(split_point);
            }
        }

        // Now handle the parent insertion - this may require creating a new root
        let is_root;
        let original_page_id = leaf_page.get_page_id();

        {
            let leaf_read = leaf_page.read();
            is_root = leaf_read.is_root();
        }

        if is_root {
            // Create a new root with the separator key
            self.create_new_root(original_page_id, separator_key, new_page_id)?;

            // Update the leaf to no longer be root
            let mut leaf_write = leaf_page.write();
            leaf_write.set_root_status(false);
        } else {
            // Get parent page ID from the path (O(1) instead of O(n) BFS)
            let parent_page_id = Self::get_parent_from_path(path)?;
            let parent_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(parent_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch parent page".to_string())
                })?;

            // Insert the separator key into the parent
            let mut parent_write = parent_page.write();
            parent_write.insert_key_value(separator_key, new_page_id);

            // Check if parent is now full and needs splitting
            if parent_write.get_size() >= self.calculate_internal_max_size() {
                // Release parent write lock first
                drop(parent_write);

                // Get the path to the parent's ancestors for recursive split
                let grandparent_path = Self::get_grandparent_path(path);

                // Recursively split the parent
                self.split_internal_page(&parent_page, &grandparent_path)?;
            }
        }

        Ok(())
    }

    /// Split an internal page when it becomes full
    ///
    /// # Arguments
    /// * `internal_page` - The internal page to split
    /// * `path` - The path of ancestor page IDs from root to parent of this internal page
    fn split_internal_page(
        &self,
        internal_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        path: &[PageId],
    ) -> Result<(), BPlusTreeError> {
        // Get the max size based on order
        let internal_max_size = self.calculate_internal_max_size();
        if internal_max_size == 0 {
            return Err(BPlusTreeError::BufferPoolError(
                "Invalid internal max size".to_string(),
            ));
        }

        // Allocate a new internal page with the comparator
        let comparator = self.comparator.clone();
        let new_page = self
            .buffer_pool_manager
            .new_page_with_options(|page_id| {
                BPlusTreeInternalPage::<K, C>::new_with_options(
                    page_id,
                    internal_max_size,
                    comparator,
                )
            })
            .ok_or(BPlusTreeError::PageAllocationFailed)?;

        let new_page_id = new_page.get_page_id();

        // Perform the split operation
        let middle_key;
        {
            let mut internal_write = internal_page.write();
            let mut new_internal_write = new_page.write();

            // Initialize the new internal page
            new_internal_write.set_root_status(false);

            // Determine split point - floor(max_size/2)
            let split_point = internal_max_size / 2;
            let current_size = internal_write.get_size();

            // The middle key (at split_point) will be promoted to the parent
            middle_key = internal_write
                .get_key_at(split_point)
                .ok_or(BPlusTreeError::InvalidPageType)?
                .clone();

            // Get the child that will be the leftmost pointer in the new page
            // (This is the child pointer that goes immediately after the middle key)
            let child_after_middle = internal_write
                .get_value_at(split_point + 1)
                .ok_or(BPlusTreeError::InvalidPageType)?;

            // First insert the leftmost child pointer in the new page
            // For an internal page, we need to setup the first child before any keys
            new_internal_write.insert_key_value(middle_key.clone(), child_after_middle);
            // This added a key and two pointers, but we only wanted one pointer (leftmost)
            // We'll overwrite with the remaining keys and values

            // Now move the remaining keys and values
            for i in (split_point + 1)..current_size {
                if let Some(key) = internal_write.get_key_at(i)
                    && let Some(child_id) = internal_write.get_value_at(i + 1)
                {
                    new_internal_write.insert_key_value(key.clone(), child_id);
                }
            }

            // Now remove keys from original page: remove the middle key and all keys after it
            for _ in split_point..current_size {
                internal_write.remove_key_value_at(split_point);
            }
        }

        // Now handle the parent insertion
        let is_root;
        let original_page_id = internal_page.get_page_id();

        {
            let internal_read = internal_page.read();
            is_root = internal_read.is_root();
        }

        if is_root {
            // Current page is no longer the root
            {
                let mut internal_write = internal_page.write();
                internal_write.set_root_status(false);
            }

            // Create a new root with the middle key
            self.create_new_root(original_page_id, middle_key, new_page_id)?;
        } else {
            // Get parent page ID from the path (O(1) instead of O(n) BFS)
            let parent_page_id = Self::get_parent_from_path(path)?;
            let parent_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(parent_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch parent page".to_string())
                })?;

            // Insert the middle key into the parent, associating it with the new page ID
            let mut parent_write = parent_page.write();
            parent_write.insert_key_value(middle_key, new_page_id);

            // Check if parent is now full and needs splitting
            if parent_write.get_size() >= self.calculate_internal_max_size() {
                // Release parent write lock first
                drop(parent_write);

                // Get the path to the parent's ancestors for recursive split
                let grandparent_path = Self::get_grandparent_path(path);

                // Recursively split the parent
                self.split_internal_page(&parent_page, &grandparent_path)?;
            }
        }

        Ok(())
    }

    /// Merge two leaf pages when they become too empty
    ///
    /// # Arguments
    /// * `grandparent_path` - Path from root to grandparent (parent's ancestors)
    fn merge_leaf_pages(
        &self,
        left_page: PageGuard<BPlusTreeLeafPage<K, V, C>>,
        right_page: PageGuard<BPlusTreeLeafPage<K, V, C>>,
        parent_page: PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
        grandparent_path: &[PageId],
    ) -> Result<(), BPlusTreeError> {
        // Store page IDs before acquiring any locks
        let left_page_id = left_page.get_page_id();
        let right_page_id = right_page.get_page_id();
        let parent_page_id = parent_page.get_page_id();

        // 1. Acquire write locks on all pages
        let mut left_write = left_page.write();
        let right_write = right_page.write();
        let mut parent_write = parent_page.write();

        // 2. Validate preconditions for merging
        // Ensure pages are adjacently linked in the parent
        if parent_write.get_value_at(parent_key_index) != Some(left_page_id)
            || parent_write.get_value_at(parent_key_index + 1) != Some(right_page_id)
        {
            return Err(BPlusTreeError::BufferPoolError(
                "Leaf pages are not adjacent siblings in parent".to_string(),
            ));
        }

        // Check if combined size fits in a single page
        let combined_size = left_write.get_size() + right_write.get_size();
        if combined_size > left_write.get_max_size() {
            return Err(BPlusTreeError::BufferPoolError(
                "Combined leaf pages are too large to merge".to_string(),
            ));
        }

        // Verify parent_key_index is valid
        if parent_key_index >= parent_write.get_size() {
            return Err(BPlusTreeError::BufferPoolError(
                "Invalid parent key index".to_string(),
            ));
        }

        // 3. Copy all key-value pairs from right_page to left_page
        for i in 0..right_write.get_size() {
            if let (Some(key), Some(value)) =
                (right_write.get_key_at(i), right_write.get_value_at(i))
            {
                left_write.insert_key_value(key.clone(), value.clone());
            }
        }

        // 4. Update the linked list structure
        // Set left_page's next_page_id to right_page's next_page_id
        left_write.set_next_page_id(right_write.get_next_page_id());

        // 5. Remove the separator key and right child pointer from parent
        parent_write.remove_key_value_at(parent_key_index);

        // 6. Handle parent's status after removal
        let is_parent_root = parent_write.is_root();
        let parent_size = parent_write.get_size();

        // If parent is root and becomes empty (with only one child), update tree
        if is_parent_root && parent_size == 0 {
            // Get the only remaining child (left_page) and promote it to root
            left_write.set_root_status(true);

            // Update header page to point to the new root and reduce height
            // Release locks before getting header page
            drop(left_write);
            drop(right_write);
            drop(parent_write);

            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
                })?;

            {
                let mut header_write = header_page.write();
                let current_height = header_write.get_tree_height();
                header_write.set_root_page_id(left_page_id);
                header_write.set_tree_height(current_height - 1);
            }

            // Release the parent + right page guards before deleting pages.
            // Holding a PageGuard keeps the page pinned in the buffer pool.
            drop(parent_page);
            drop(right_page);

            // Release the parent page (it's no longer needed)
            self.buffer_pool_manager
                .delete_page(parent_page_id)
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

            // Release the right page (it's been merged)
            self.buffer_pool_manager
                .delete_page(right_page_id)
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

            return Ok(());
        }

        // 7. If we haven't returned yet, the locks will be released when the guards go out of scope
        // The buffer pool will handle marking the pages as dirty

        // Store parent information before dropping the write guard
        let parent_needs_rebalancing =
            !is_parent_root && parent_size < parent_write.get_max_size() / 2;

        // Release all write locks
        drop(left_write);
        drop(parent_write);
        drop(right_write);

        // Drop page guards we no longer need before deleting pages.
        // (Deleting a pinned page will fail with "Page pinned - PageID: X".)
        drop(parent_page);
        drop(right_page);

        // Now safe to delete the right page
        self.buffer_pool_manager
            .delete_page(right_page_id)
            .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

        // Check if parent needs rebalancing after removing a key
        if parent_needs_rebalancing {
            // Recursively handle parent underflow using grandparent_path
            self.check_and_handle_underflow(parent_page_id, false, grandparent_path)?;
        }

        Ok(())
    }

    /// Redistribute keys between two leaf pages to avoid merge
    fn redistribute_leaf_pages(
        &self,
        left_page: &PageGuard<BPlusTreeLeafPage<K, V, C>>,
        right_page: &PageGuard<BPlusTreeLeafPage<K, V, C>>,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Acquire write locks on all pages
        let mut left_write = left_page.write();
        let mut right_write = right_page.write();
        let mut parent_write = parent_page.write();

        // 2. Verify preconditions
        // Ensure pages are adjacent siblings in parent
        if parent_write.get_value_at(parent_key_index) != Some(left_page.get_page_id())
            || parent_write.get_value_at(parent_key_index + 1) != Some(right_page.get_page_id())
        {
            return Err(BPlusTreeError::BufferPoolError(
                "Leaf pages are not adjacent siblings in parent".to_string(),
            ));
        }

        // 3. Determine sizes and target redistribution
        let left_size = left_write.get_size();
        let right_size = right_write.get_size();
        let total_size = left_size + right_size;
        let min_size = left_write.get_max_size() / 2; // Assuming min_size is half of max_size

        // Check if redistribution is possible
        if total_size < min_size * 2 {
            return Err(BPlusTreeError::BufferPoolError(
                "Not enough keys for redistribution".to_string(),
            ));
        }

        // Calculate desired sizes after redistribution (approximately balanced)
        let target_size = total_size / 2;

        // 4. Determine direction of redistribution
        if left_size < right_size {
            // Move keys from right to left
            let keys_to_move = right_size - target_size;

            // Move keys from the beginning of right page to end of left page
            for i in 0..keys_to_move {
                // Retrieve key and value first to avoid borrowing issues
                let key_to_move = right_write.get_key_at(i).cloned();
                let value_to_move = right_write.get_value_at(i).cloned();

                if let (Some(key), Some(value)) = (key_to_move, value_to_move) {
                    left_write.insert_key_value(key, value);
                }
            }

            // Remove the moved keys from right page
            for _ in 0..keys_to_move {
                right_write.remove_key_value_at(0);
            }

            // Update the separator key in parent to the first key in right page
            let new_separator = right_write.get_key_at(0).cloned();
            if let Some(new_sep) = new_separator {
                // Fix: Replace set_key_at with proper method to update a key
                if parent_write.get_key_at(parent_key_index).is_some() {
                    parent_write.remove_key_value_at(parent_key_index);
                    parent_write.insert_key_value(new_sep, right_page.get_page_id());
                }
            } else {
                return Err(BPlusTreeError::BufferPoolError(
                    "Right page is unexpectedly empty after redistribution".to_string(),
                ));
            }
        } else {
            // Move keys from left to right - O(n) implementation
            let keys_to_move = left_size - target_size;
            let start_index = left_size - keys_to_move;

            // Step 1: Collect keys to move from left page (O(k) where k = keys_to_move)
            let mut keys_to_insert = Vec::with_capacity(keys_to_move);
            for i in start_index..left_size {
                if let (Some(key), Some(value)) = (
                    left_write.get_key_at(i).cloned(),
                    left_write.get_value_at(i).cloned(),
                ) {
                    keys_to_insert.push((key, value));
                }
            }

            // Step 2: Collect existing keys from right page ONCE (O(r) where r = right_size)
            let right_size = right_write.get_size();
            let mut existing_right_keys = Vec::with_capacity(right_size);
            for i in 0..right_size {
                if let (Some(key), Some(value)) = (
                    right_write.get_key_at(i).cloned(),
                    right_write.get_value_at(i).cloned(),
                ) {
                    existing_right_keys.push((key, value));
                }
            }

            // Step 3: Clear the right page ONCE (O(r))
            for _ in 0..right_size {
                right_write.remove_key_value_at(0);
            }

            // Step 4: Insert moved keys first (they go at the beginning) (O(k))
            for (key, value) in keys_to_insert {
                right_write.insert_key_value(key, value);
            }

            // Step 5: Re-insert original right page keys (O(r))
            for (key, value) in existing_right_keys {
                right_write.insert_key_value(key, value);
            }

            // Step 6: Remove moved keys from left page (O(k))
            for _ in 0..keys_to_move {
                left_write.remove_key_value_at(start_index);
            }

            // Step 7: Update the separator key in parent to the first key in right page
            let new_separator = right_write.get_key_at(0).cloned();
            if let Some(new_sep) = new_separator {
                if parent_write.get_key_at(parent_key_index).is_some() {
                    parent_write.remove_key_value_at(parent_key_index);
                    parent_write.insert_key_value(new_sep, right_page.get_page_id());
                }
            } else {
                return Err(BPlusTreeError::BufferPoolError(
                    "Right page is unexpectedly empty after redistribution".to_string(),
                ));
            }
        }

        // The locks will be released when the write guards are dropped
        Ok(())
    }

    /// Merge two internal pages when they become too empty
    ///
    /// # Arguments
    /// * `grandparent_path` - Path from root to grandparent (parent's ancestors)
    fn merge_internal_pages(
        &self,
        left_page: PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
        grandparent_path: &[PageId],
    ) -> Result<(), BPlusTreeError> {
        // Store page IDs before acquiring any locks
        let left_page_id = left_page.get_page_id();
        let right_page_id = right_page.get_page_id();
        let parent_page_id = parent_page.get_page_id();

        // 1. Acquire write locks on all pages
        let mut left_write = left_page.write();
        let right_write = right_page.write();
        let mut parent_write = parent_page.write();

        // 2. Validate preconditions for merging
        // Ensure the pages are adjacently linked in the parent
        if parent_write.get_value_at(parent_key_index) != Some(left_page_id)
            || parent_write.get_value_at(parent_key_index + 1) != Some(right_page_id)
        {
            return Err(BPlusTreeError::BufferPoolError(
                "Internal pages are not adjacent siblings in parent".to_string(),
            ));
        }

        // Get the separator key from the parent
        let separator_key = parent_write
            .get_key_at(parent_key_index)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Invalid parent key index".to_string()))?
            .clone();

        // Calculate combined size (including the separator key from parent)
        let combined_size = left_write.get_size() + right_write.get_size() + 1;
        if combined_size > left_write.get_max_size() {
            return Err(BPlusTreeError::BufferPoolError(
                "Combined internal pages are too large to merge".to_string(),
            ));
        }

        // 3. Move the separator key from parent to the left page
        let right_first_child = right_write.get_value_at(0).ok_or_else(|| {
            BPlusTreeError::BufferPoolError("Right page has no children".to_string())
        })?;
        left_write.insert_key_value(separator_key, right_first_child);

        // 4. Copy all keys and child pointers from right page to left page
        // Skip the first child pointer of right page as it's already handled with the separator key
        for i in 0..right_write.get_size() {
            if let Some(key) = right_write.get_key_at(i)
                && let Some(child_id) = right_write.get_value_at(i + 1)
            {
                left_write.insert_key_value(key.clone(), child_id);
            }
        }

        // 5. Remove the separator key and right child pointer from parent
        parent_write.remove_key_value_at(parent_key_index);

        // 6. Handle parent's status after removal
        let is_parent_root = parent_write.is_root();
        let parent_size = parent_write.get_size();

        // If parent is root and becomes empty, make left page the new root
        if is_parent_root && parent_size == 0 {
            // Left page becomes the new root
            left_write.set_root_status(true);

            // Update header page
            // First release locks on the pages
            drop(left_write);
            drop(right_write);
            drop(parent_write);

            let header_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
                })?;

            {
                let mut header_write = header_page.write();
                let current_height = header_write.get_tree_height();
                header_write.set_root_page_id(left_page_id);
                header_write.set_tree_height(current_height - 1);
            }

            // Release the parent + right page guards before deleting pages.
            // Holding a PageGuard keeps the page pinned in the buffer pool.
            drop(parent_page);
            drop(right_page);

            // Release the parent page and right page
            self.buffer_pool_manager
                .delete_page(parent_page_id)
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

            self.buffer_pool_manager
                .delete_page(right_page_id)
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

            return Ok(());
        }

        // 7. If we haven't returned yet, the locks will be released when the guards go out of scope

        // Store parent information before dropping the write guard
        let parent_needs_rebalancing =
            !is_parent_root && parent_size < parent_write.get_max_size() / 2;

        // Release all locks
        drop(left_write);
        drop(parent_write);
        drop(right_write);

        // Drop page guards we no longer need before deleting pages.
        drop(parent_page);
        drop(right_page);

        // Delete the right page as it's no longer needed
        self.buffer_pool_manager
            .delete_page(right_page_id)
            .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

        // 8. Check if parent needs rebalancing after removing a key
        if parent_needs_rebalancing {
            // Recursively handle parent underflow using grandparent_path
            self.check_and_handle_underflow(parent_page_id, false, grandparent_path)?;
        }

        Ok(())
    }

    /// Redistribute keys between two internal pages to avoid merge
    fn redistribute_internal_pages(
        &self,
        left_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Acquire write locks on all pages
        let mut left_write = left_page.write();
        let mut right_write = right_page.write();
        let mut parent_write = parent_page.write();

        // 2. Verify preconditions
        // Ensure pages are adjacent siblings in parent
        if parent_write.get_value_at(parent_key_index) != Some(left_page.get_page_id())
            || parent_write.get_value_at(parent_key_index + 1) != Some(right_page.get_page_id())
        {
            return Err(BPlusTreeError::BufferPoolError(
                "Internal pages are not adjacent siblings in parent".to_string(),
            ));
        }

        // Get separator key from parent
        let separator_key = parent_write
            .get_key_at(parent_key_index)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Invalid parent key index".to_string()))?
            .clone();

        // 3. Determine sizes and target redistribution
        let left_size = left_write.get_size();
        let right_size = right_write.get_size();
        let total_entries = left_size + right_size + 1; // +1 for separator key
        let min_size = left_write.get_max_size() / 2; // Assuming min_size is half of max_size

        // Check if redistribution is possible
        if total_entries < min_size * 2 {
            return Err(BPlusTreeError::BufferPoolError(
                "Not enough keys for redistribution".to_string(),
            ));
        }

        // Calculate target sizes after redistribution (approximately balanced)
        let target_size = total_entries / 2;

        // 4. Determine direction of redistribution
        if left_size < right_size {
            // Move entries from right to left
            // First, get the leftmost child from right page
            let right_first_child = right_write.get_value_at(0).ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Right page has no children".to_string())
            })?;

            // Move separator key from parent to left page, with rightmost child pointer
            left_write.insert_key_value(separator_key, right_first_child);

            // Calculate how many more keys to move (excluding the first child already moved)
            let additional_keys_to_move = target_size - left_size - 1;

            // Move additional keys and children from right to left
            for i in 0..additional_keys_to_move {
                let key_to_move = right_write.get_key_at(i);
                let child_id_to_move = right_write.get_value_at(i + 1);

                if let (Some(key), Some(child_id)) = (key_to_move, child_id_to_move) {
                    left_write.insert_key_value(key, child_id);
                }
            }

            // Get the new separator key from right page
            let new_separator = right_write
                .get_key_at(additional_keys_to_move)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Not enough keys in right page".to_string())
                })?
                .clone();

            // Update parent with new separator key
            parent_write.remove_key_value_at(parent_key_index);
            parent_write.insert_key_value(new_separator, right_page.get_page_id());

            // Update right page: remove moved keys and update leftmost child
            // We need to keep the old rightmost child of right page as its new leftmost child
            right_write
                .get_value_at(additional_keys_to_move + 1)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Invalid right page layout".to_string())
                })?;

            // Remove the transferred keys/values from right page
            for _ in 0..=additional_keys_to_move {
                right_write.remove_key_value_at(0);
            }
        } else {
            // Move entries from left to right

            // First, get the rightmost key and child from left page
            let left_size = left_write.get_size();
            let left_last_key = left_write
                .get_key_at(left_size - 1)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Left page has no keys".to_string())
                })?
                .clone();
            let left_last_child = left_write.get_value_at(left_size).ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Left page has invalid child count".to_string())
            })?;

            // Calculate how many keys to move
            let keys_to_move = left_size - target_size;

            // Save right page's leftmost child
            right_write.get_value_at(0).ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Right page has no children".to_string())
            })?;

            // Shift everything in right page to make room
            // To avoid modifying while iterating, we'll collect all keys/values first
            let right_size = right_write.get_size();
            let mut right_kv_pairs = Vec::with_capacity(right_size);

            for i in 0..right_size {
                let key = right_write.get_key_at(i);
                let child_id = right_write.get_value_at(i + 1);

                if let (Some(k), Some(v)) = (key, child_id) {
                    right_kv_pairs.push((k, v));
                }
            }

            // Clear right page except for its leftmost child pointer
            while right_write.get_size() > 0 {
                right_write.remove_key_value_at(0);
            }

            // Insert separator key from parent with left's rightmost child
            right_write.insert_key_value(separator_key, left_last_child);

            // Move the keys to move from left to right (except the last one that becomes the new separator)
            let start_idx = left_size - keys_to_move;
            let end_idx = left_size - 1; // Exclude the last key which becomes the separator

            // Create a temporary vector of keys to move
            let mut keys_to_move_vec = Vec::with_capacity(keys_to_move);
            for i in start_idx..end_idx {
                let key = left_write.get_key_at(i);
                let child_id = left_write.get_value_at(i + 1);

                if let (Some(k), Some(v)) = (key, child_id) {
                    keys_to_move_vec.push((k, v));
                }
            }

            // Insert the keys to move
            for (key, child_id) in keys_to_move_vec {
                right_write.insert_key_value(key, child_id);
            }

            // Put back the original keys in right page
            for (key, child_id) in right_kv_pairs {
                right_write.insert_key_value(key, child_id);
            }

            // Update parent with last key from left as new separator
            parent_write.remove_key_value_at(parent_key_index);
            parent_write.insert_key_value(left_last_key, right_page.get_page_id());

            // Remove transferred keys from left page
            // Store current size to avoid borrowing issues
            let current_left_size = left_write.get_size();
            for i in 0..keys_to_move {
                left_write.remove_key_value_at(current_left_size - 1 - i);
            }
        }

        // The locks will be released when the write guards are dropped
        Ok(())
    }

    /// Create a new root page
    fn create_new_root(
        &self,
        left_child_id: PageId,
        key: K,
        right_child_id: PageId,
    ) -> Result<(), BPlusTreeError> {
        // Allocate a new internal page with the comparator
        let comparator = self.comparator.clone();
        let internal_max_size = self.calculate_internal_max_size();
        let new_page = self
            .buffer_pool_manager
            .new_page_with_options(|page_id| {
                BPlusTreeInternalPage::<K, C>::new_with_options(
                    page_id,
                    internal_max_size,
                    comparator,
                )
            })
            .ok_or(BPlusTreeError::PageAllocationFailed)?;

        let new_root_id = new_page.get_page_id();

        // Initialize as internal page and set up as root
        {
            let mut internal_write = new_page.write();

            // Use the populate_new_root method to set up the internal page with the separator key and child pointers
            if !internal_write.populate_new_root(left_child_id, key, right_child_id) {
                return Err(BPlusTreeError::BufferPoolError(
                    "Failed to populate new root page".to_string(),
                ));
            }
        }

        // Update the header page to point to the new root
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        {
            let mut header_write = header_page.write();

            // Update the header to point to the new root and increment tree height
            let current_height = header_write.get_tree_height();
            header_write.set_root_page_id(new_root_id);
            header_write.set_tree_height(current_height + 1);
        }

        Ok(())
    }

    /// Update header page after operations
    fn update_header(
        &self,
        root_id: PageId,
        height: u32,
        num_keys: usize,
    ) -> Result<(), BPlusTreeError> {
        // Fetch the header page
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        // Update the header fields
        {
            let mut header_write = header_page.write();
            header_write.set_root_page_id(root_id);
            header_write.set_tree_height(height);
            header_write.set_num_keys(num_keys);
        }

        Ok(())
    }

    /// Check if the B+ tree is empty
    pub fn is_empty(&self) -> bool {
        if self.header_page_id == INVALID_PAGE_ID {
            return true;
        }

        // Try to fetch the header page
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return true, // If we can't fetch the header, consider tree empty
        };

        // Check root page ID
        let root_page_id = header_page.read().get_root_page_id();

        // If root page ID is invalid, tree is empty
        root_page_id == INVALID_PAGE_ID
    }

    /// Get the current height of the B+ tree
    pub fn get_height(&self) -> u32 {
        if self.header_page_id == INVALID_PAGE_ID {
            return 0;
        }

        // Try to fetch the header page
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return 0,
        };

        header_page.read().get_tree_height()
    }

    /// Get the root page ID
    pub fn get_root_page_id(&self) -> PageId {
        if self.header_page_id == INVALID_PAGE_ID {
            return INVALID_PAGE_ID;
        }

        // Try to fetch the header page
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return INVALID_PAGE_ID,
        };

        header_page.read().get_root_page_id()
    }

    /// Get the total number of keys in the B+ tree
    pub fn get_size(&self) -> usize {
        if self.header_page_id == INVALID_PAGE_ID {
            return 0;
        }

        // Try to fetch the header page
        let header_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return 0,
        };

        header_page.read().get_num_keys()
    }

    /// Find a sibling page for merging or redistribution
    fn find_sibling(
        &self,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        child_index: usize,
        prefer_left: bool,
    ) -> Result<(usize, PageId), BPlusTreeError> {
        let parent_read = parent_page.read();

        // Get the number of child pointers in parent_page
        let num_children = parent_read.get_size() + 1; // Internal page has (size + 1) children

        // Try to find a sibling in the preferred direction first
        if prefer_left && child_index > 0 {
            // Return left sibling (child_index - 1) and the separator key index (child_index - 1)
            if let Some(sibling_id) = parent_read.get_value_at(child_index - 1) {
                return Ok((child_index - 1, sibling_id));
            }
        } else if !prefer_left && child_index < num_children - 1 {
            // Return right sibling (child_index + 1) and the separator key index (child_index)
            if let Some(sibling_id) = parent_read.get_value_at(child_index + 1) {
                return Ok((child_index, sibling_id));
            }
        }

        // If no sibling in preferred direction, try the other direction
        if prefer_left && child_index < num_children - 1 {
            // Couldn't find left sibling, try right sibling
            if let Some(sibling_id) = parent_read.get_value_at(child_index + 1) {
                return Ok((child_index, sibling_id));
            }
        } else if !prefer_left && child_index > 0 {
            // Couldn't find right sibling, try left sibling
            if let Some(sibling_id) = parent_read.get_value_at(child_index - 1) {
                return Ok((child_index - 1, sibling_id));
            }
        }

        // If we get here, no siblings exist (should only happen for root with single child)
        Err(BPlusTreeError::BufferPoolError(
            "No sibling found for the given child".to_string(),
        ))
    }

    /// Check if a page needs rebalancing after removal
    ///
    /// # Arguments
    /// * `page_id` - The page that may need rebalancing
    /// * `is_leaf` - Whether the page is a leaf page
    /// * `path` - The path of ancestor page IDs from root to parent of this page
    pub fn check_and_handle_underflow(
        &self,
        page_id: PageId,
        is_leaf: bool,
        path: &[PageId],
    ) -> Result<bool, BPlusTreeError> {
        // 1. Fetch the header page to get basic tree info
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let (root_page_id, tree_height) = {
            let header = header_page.read();
            (header.get_root_page_id(), header.get_tree_height())
        };

        // Drop header page to avoid resource conflicts
        drop(header_page);

        // 2. Check if page is the root (path would be empty)
        if page_id == root_page_id || path.is_empty() {
            // If this is the root page, handle special cases
            return if is_leaf {
                // If root is a leaf, it can have any number of keys (no underflow)
                Ok(false)
            } else {
                // If root is internal and has only one child, collapse the tree
                let page = self
                    .buffer_pool_manager
                    .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
                    .ok_or_else(|| {
                        BPlusTreeError::BufferPoolError("Failed to fetch internal page".to_string())
                    })?;

                let size;
                let only_child_id;

                {
                    let internal_read = page.read();
                    size = internal_read.get_size();

                    if size == 0 {
                        // Root has no keys, check if it has one child
                        only_child_id = internal_read.get_value_at(0);
                    } else {
                        // Root has keys, no need to collapse
                        return Ok(false);
                    }
                }

                // If root has no keys but one child, make the child the new root
                if let Some(child_id) = only_child_id {
                    // Update the child to be the new root
                    if is_leaf {
                        // Child is a leaf page
                        let child_page = self
                            .buffer_pool_manager
                            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(child_id)
                            .ok_or_else(|| {
                                BPlusTreeError::BufferPoolError(
                                    "Failed to fetch leaf page".to_string(),
                                )
                            })?;

                        {
                            let mut child_write = child_page.write();
                            child_write.set_root_status(true);
                        }
                    } else {
                        // Child is an internal page
                        let child_page = self
                            .buffer_pool_manager
                            .fetch_page::<BPlusTreeInternalPage<K, C>>(child_id)
                            .ok_or_else(|| {
                                BPlusTreeError::BufferPoolError(
                                    "Failed to fetch internal page".to_string(),
                                )
                            })?;

                        {
                            let mut child_write = child_page.write();
                            child_write.set_root_status(true);
                        }
                    }

                    // Update header to point to new root and decrement tree height
                    let header_page = self
                        .buffer_pool_manager
                        .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                        .ok_or_else(|| {
                            BPlusTreeError::BufferPoolError(
                                "Failed to fetch header page".to_string(),
                            )
                        })?;

                    {
                        let mut header_write = header_page.write();
                        header_write.set_root_page_id(child_id);
                        header_write.set_tree_height(tree_height - 1);
                    }

                    // Delete the old root page
                    self.buffer_pool_manager
                        .delete_page(page_id)
                        .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;

                    return Ok(true); // Tree structure changed
                }

                // If we get here, root has no keys and no children, which should never happen
                Err(BPlusTreeError::BufferPoolError(
                    "Invalid root page state".to_string(),
                ))
            };
        }

        // 3. For non-root pages, check minimum size requirement
        let min_size = if is_leaf {
            self.calculate_leaf_max_size() / 2
        } else {
            self.calculate_internal_max_size() / 2
        };

        let page_size;

        // Check the current size of the page
        if is_leaf {
            let page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch leaf page".to_string())
                })?;

            page_size = page.read().get_size();

            // If page has enough keys, no rebalancing needed
            if page_size >= min_size {
                return Ok(false);
            }

            // Otherwise, drop the page before proceeding to rebalancing
            drop(page);
        } else {
            let page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch internal page".to_string())
                })?;

            page_size = page.read().get_size();

            // If page has enough keys, no rebalancing needed
            if page_size >= min_size {
                return Ok(false);
            }

            // Otherwise, drop the page before proceeding to rebalancing
            drop(page);
        }

        // 4. Page needs rebalancing - get parent from path (O(1) instead of O(n) BFS)
        let parent_page_id = Self::get_parent_from_path(path)?;
        let parent_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(parent_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch parent page".to_string())
            })?;

        // 5. Find the index of the current page in the parent's child pointers
        let parent_read = parent_page.read();
        let mut child_index = 0;
        let mut found = false;

        for i in 0..=parent_read.get_size() {
            if parent_read.get_value_at(i) == Some(page_id) {
                child_index = i;
                found = true;
                break;
            }
        }

        if !found {
            return Err(BPlusTreeError::BufferPoolError(
                "Page not found in parent".to_string(),
            ));
        }

        // Drop the parent read lock before modifying
        drop(parent_read);

        // 6. Find a sibling for redistribution or merging
        // find_sibling returns (separator_key_index, sibling_page_id)
        // where separator_key_index is the index of the key in parent that separates
        // the current page from its sibling
        let (separator_key_index, sibling_page_id) =
            self.find_sibling(&parent_page, child_index, true)?;

        // Determine if sibling is to the left or right of current page
        // If separator_key_index < child_index, sibling is to the left
        let sibling_is_left = separator_key_index < child_index;

        // Compute grandparent_path once for use in merge operations and recursive rebalancing
        let grandparent_path = Self::get_grandparent_path(path);

        // 7. Decide whether to merge or redistribute
        if is_leaf {
            // Get leaf pages
            let current_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch leaf page".to_string())
                })?;

            let sibling_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(sibling_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch sibling leaf page".to_string())
                })?;

            // Calculate total size
            let total_size = current_page.read().get_size() + sibling_page.read().get_size();
            let max_size = current_page.read().get_max_size();

            // Determine if merge or redistribution is needed
            if total_size <= max_size {
                // Merge the pages - ensure left page is always first argument
                if sibling_is_left {
                    self.merge_leaf_pages(
                        sibling_page,
                        current_page,
                        parent_page,
                        separator_key_index,
                        &grandparent_path,
                    )?;
                } else {
                    self.merge_leaf_pages(
                        current_page,
                        sibling_page,
                        parent_page,
                        separator_key_index,
                        &grandparent_path,
                    )?;
                }
            } else {
                // Redistribute keys - ensure left page is always first argument
                if sibling_is_left {
                    self.redistribute_leaf_pages(
                        &sibling_page,
                        &current_page,
                        &parent_page,
                        separator_key_index,
                    )?;
                } else {
                    self.redistribute_leaf_pages(
                        &current_page,
                        &sibling_page,
                        &parent_page,
                        separator_key_index,
                    )?;
                }
            }
        } else {
            // Handle internal pages
            let current_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError("Failed to fetch internal page".to_string())
                })?;

            let sibling_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(sibling_page_id)
                .ok_or_else(|| {
                    BPlusTreeError::BufferPoolError(
                        "Failed to fetch sibling internal page".to_string(),
                    )
                })?;

            // Calculate total entries (keys + separator from parent)
            let total_entries = current_page.read().get_size() + sibling_page.read().get_size() + 1;
            let max_size = current_page.read().get_max_size();

            // Determine if merge or redistribution is needed
            if total_entries <= max_size {
                // Merge the pages - ensure left page is always first argument
                if sibling_is_left {
                    self.merge_internal_pages(
                        sibling_page,
                        current_page,
                        parent_page,
                        separator_key_index,
                        &grandparent_path,
                    )?;
                } else {
                    self.merge_internal_pages(
                        current_page,
                        sibling_page,
                        parent_page,
                        separator_key_index,
                        &grandparent_path,
                    )?;
                }
            } else {
                // Redistribute keys - ensure left page is always first argument
                if sibling_is_left {
                    self.redistribute_internal_pages(
                        &sibling_page,
                        &current_page,
                        &parent_page,
                        separator_key_index,
                    )?;
                } else {
                    self.redistribute_internal_pages(
                        &current_page,
                        &sibling_page,
                        &parent_page,
                        separator_key_index,
                    )?;
                }
            }
        }

        Ok(true) // Rebalancing occurred
    }

    /// Perform a level-order traversal (breadth-first) for debugging or visualization
    pub fn print_tree(&self) -> Result<(), BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            println!("Empty tree - not initialized");
            return Ok(());
        }

        // Get header page to check if tree is empty
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let root_page_id = header_page.read().get_root_page_id();
        let tree_height = header_page.read().get_tree_height();
        let num_keys = header_page.read().get_num_keys();

        // Print header information
        println!("B+ Tree Information:");
        println!("  Height: {}", tree_height);
        println!("  Size (total keys): {}", num_keys);
        println!("  Root Page ID: {}", root_page_id);

        // If tree is empty, return early
        if root_page_id == INVALID_PAGE_ID {
            println!("Empty tree - no root");
            return Ok(());
        }

        println!("\nTree Structure:");

        // Initialize queue for BFS
        let mut queue = VecDeque::new();
        queue.push_back((root_page_id, 0)); // (page_id, level)

        let mut current_level = 0;

        // Perform BFS
        while let Some((page_id, level)) = queue.pop_front() {
            // Print level separation if needed
            if level > current_level {
                println!("\nLevel {}:", level);
                current_level = level;
            }

            // Print indentation
            let indent = "  ".repeat(level as usize + 1);

            // Try to get the page as a leaf page
            if let Some(leaf_page) = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id)
            {
                let leaf_read = leaf_page.read();

                // Print leaf information
                println!(
                    "{}[Leaf {}]: Size={}, Next={:?}",
                    indent,
                    page_id,
                    leaf_read.get_size(),
                    leaf_read.get_next_page_id()
                );

                // Print keys/values
                if leaf_read.get_size() > 0 {
                    print!("{}Keys: ", indent);
                    for i in 0..leaf_read.get_size() {
                        if let Some(key) = leaf_read.get_key_at(i) {
                            print!("{} ", key);
                            if i < leaf_read.get_size() - 1 {
                                print!(", ");
                            }
                        }
                    }
                    println!();
                }
                continue;
            }

            // Try to get the page as an internal page
            if let Some(internal_page) = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
            {
                let internal_read = internal_page.read();

                // Print internal node information
                println!(
                    "{}[Internal {}]: Size={}, IsRoot={}",
                    indent,
                    page_id,
                    internal_read.get_size(),
                    internal_read.is_root()
                );

                // Print keys
                if internal_read.get_size() > 0 {
                    print!("{}Keys: ", indent);
                    for i in 0..internal_read.get_size() {
                        if let Some(key) = internal_read.get_key_at(i) {
                            print!("{} ", key);
                            if i < internal_read.get_size() - 1 {
                                print!(", ");
                            }
                        }
                    }
                    println!();

                    // Print child pointers
                    print!("{}Children: ", indent);
                    for i in 0..=internal_read.get_size() {
                        // One more child than keys
                        if let Some(child_id) = internal_read.get_value_at(i) {
                            print!("{} ", child_id);
                            if i < internal_read.get_size() {
                                print!(", ");
                            }

                            // Add child to queue for next level
                            queue.push_back((child_id, level + 1));
                        }
                    }
                    println!();
                }
                continue;
            }

            // If we couldn't fetch the page as either type, print unknown
            println!("{}[Unknown page type]: {}", indent, page_id);
        }

        Ok(())
    }

    /// Perform an in-order traversal of all keys (debug/verification)
    pub fn in_order_traversal(&self) -> Result<Vec<K>, BPlusTreeError> {
        // If tree is empty, return empty vector
        if self.is_empty() {
            return Ok(Vec::new());
        }

        // Find the leftmost leaf (containing smallest keys)
        let leaf_guard = self.find_leftmost_leaf()?;

        // Initialize result vector
        let mut result = Vec::new();
        let mut current_leaf = leaf_guard;

        // Traverse all leaf pages using next_page_id pointers
        loop {
            let leaf_read = current_leaf.read();

            // Collect all keys from current leaf
            for i in 0..leaf_read.get_size() {
                if let Some(key) = leaf_read.get_key_at(i) {
                    result.push(key.clone());
                }
            }

            // Get next leaf page id
            let next_page_id = match leaf_read.get_next_page_id() {
                Some(id) if id != INVALID_PAGE_ID => id,
                _ => break, // No more leaves
            };

            // Drop the current read guard
            drop(leaf_read);

            // Fetch the next leaf page
            let next_page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(next_page_id)
                .ok_or(BPlusTreeError::PageNotFound(next_page_id))?;

            // Update current leaf
            current_leaf = next_page;
        }

        Ok(result)
    }

    /// Find the leftmost leaf page in the tree (for range scans or in-order traversal)
    fn find_leftmost_leaf(&self) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError(
                "Tree not initialized".to_string(),
            ));
        }

        // Get header page to check if tree is empty
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let root_page_id = header_page.read().get_root_page_id();

        // If tree is empty, return error
        if root_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError("Tree is empty".to_string()));
        }

        // Start at the root
        let mut current_page_id = root_page_id;

        // Keep going to the leftmost child until reaching a leaf
        loop {
            // Try to fetch as a leaf page first
            if let Some(leaf_page) = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id)
            {
                // Found a leaf page, return it
                return Ok(leaf_page);
            }

            // If not a leaf, try as an internal page
            if let Some(internal_page) = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id)
            {
                // Get the leftmost child (index 0)
                let leftmost_child_id = internal_page
                    .read()
                    .get_value_at(0)
                    .ok_or(BPlusTreeError::InvalidPageType)?;

                // Continue with this child
                current_page_id = leftmost_child_id;
            } else {
                // Neither a leaf nor an internal page, return error
                return Err(BPlusTreeError::InvalidPageType);
            }
        }
    }

    /// Perform a pre-order traversal (useful for serialization)
    pub fn pre_order_traversal(&self) -> Result<Vec<PageId>, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(Vec::new());
        }

        // Get header page to check if tree is empty
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let root_page_id = header_page.read().get_root_page_id();

        // If tree is empty, return empty vector
        if root_page_id == INVALID_PAGE_ID {
            return Ok(Vec::new());
        }

        // Initialize result vector and stack for traversal
        let mut result = Vec::new();
        let mut stack = Vec::new();
        stack.push(root_page_id);

        // Iterative pre-order traversal
        while let Some(page_id) = stack.pop() {
            // Add current page to result (visit before children)
            result.push(page_id);

            // Check if this is an internal page - if so, process its children
            if let Some(internal_page) = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
            {
                let internal_read = internal_page.read();
                let size = internal_read.get_size();

                // Push children to stack in reverse order so leftmost is processed first
                for i in (0..=size).rev() {
                    if let Some(child_id) = internal_read.get_value_at(i) {
                        stack.push(child_id);
                    }
                }
            }
            // If not an internal page, it's a leaf or unknown - just continue with the next stack item
        }

        Ok(result)
    }

    /// Perform a post-order traversal (useful for safe deletion)
    pub fn post_order_traversal(&self) -> Result<Vec<PageId>, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(Vec::new());
        }

        // Get header page to check if tree is empty
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let root_page_id = header_page.read().get_root_page_id();

        // If tree is empty, return empty vector
        if root_page_id == INVALID_PAGE_ID {
            return Ok(Vec::new());
        }

        // Initialize result vector
        let mut result = Vec::new();

        // Call recursive helper function
        self.post_order_helper(root_page_id, &mut result)?;

        Ok(result)
    }

    /// Helper function for post-order traversal
    fn post_order_helper(
        &self,
        page_id: PageId,
        result: &mut Vec<PageId>,
    ) -> Result<(), BPlusTreeError> {
        // Check if this is an internal page
        if let Some(internal_page) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
        {
            let internal_read = internal_page.read();
            let size = internal_read.get_size();

            // Process all children first (post-order)
            for i in 0..=size {
                if let Some(child_id) = internal_read.get_value_at(i) {
                    self.post_order_helper(child_id, result)?;
                }
            }
        }

        // After processing all children (or if leaf), add this page
        result.push(page_id);

        Ok(())
    }

    /// Validate the B+Tree structure and return validation statistics
    pub fn validate_tree(&self) -> Result<ValidationStats, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Ok(ValidationStats::new());
        }

        // Get header page information
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| {
                BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string())
            })?;

        let (root_page_id, expected_height, expected_keys) = {
            let header = header_page.read();
            (
                header.get_root_page_id(),
                header.get_tree_height(),
                header.get_num_keys(),
            )
        };

        // If tree is empty, return empty stats
        if root_page_id == INVALID_PAGE_ID {
            let stats = ValidationStats::new();
            if expected_keys != 0 || expected_height != 0 {
                return Err(BPlusTreeError::ConversionError(
                    "Header indicates non-empty tree but root is invalid".to_string(),
                ));
            }
            return Ok(stats);
        }

        // Initialize validation stats
        let mut stats = ValidationStats::new();

        // Validate tree structure recursively
        self.validate_node(root_page_id, 0, &mut stats)?;

        // Verify consistency with header information
        if stats.total_keys != expected_keys {
            return Err(BPlusTreeError::ConversionError(format!(
                "Key count mismatch: header says {}, validation found {}",
                expected_keys, stats.total_keys
            )));
        }

        if stats.max_depth + 1 != expected_height {
            return Err(BPlusTreeError::ConversionError(format!(
                "Height mismatch: header says {}, validation found {}",
                expected_height,
                stats.max_depth + 1
            )));
        }

        // Validate that all leaves are at the same level
        if let Some(leaf_level) = stats.leaf_level
            && leaf_level != stats.max_depth
        {
            return Err(BPlusTreeError::ConversionError(format!(
                "Leaves not at same level: found leaves at level {} but max depth is {}",
                leaf_level, stats.max_depth
            )));
        }

        Ok(stats)
    }

    /// Recursively validate a node and update statistics
    fn validate_node(
        &self,
        page_id: PageId,
        current_depth: u32,
        stats: &mut ValidationStats,
    ) -> Result<(), BPlusTreeError> {
        // Update max depth
        stats.max_depth = stats.max_depth.max(current_depth);

        // Try to fetch as leaf page first
        if let Some(leaf_page) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id)
        {
            return self.validate_leaf_page(leaf_page, current_depth, stats);
        }

        // Try to fetch as internal page
        if let Some(internal_page) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id)
        {
            return self.validate_internal_page(internal_page, current_depth, stats);
        }

        Err(BPlusTreeError::InvalidPageType)
    }

    /// Validate a leaf page and update statistics
    fn validate_leaf_page(
        &self,
        leaf_page: PageGuard<BPlusTreeLeafPage<K, V, C>>,
        current_depth: u32,
        stats: &mut ValidationStats,
    ) -> Result<(), BPlusTreeError> {
        let leaf_read = leaf_page.read();

        // Check if this is the first leaf we've encountered
        if stats.leaf_level.is_none() {
            stats.leaf_level = Some(current_depth);
        } else if stats.leaf_level != Some(current_depth) {
            return Err(BPlusTreeError::ConversionError(format!(
                "Leaves at different levels: expected {:?}, found {}",
                stats.leaf_level, current_depth
            )));
        }

        // Add keys to total count
        stats.total_keys += leaf_read.get_size();

        // Validate leaf page structure
        let size = leaf_read.get_size();
        let max_size = leaf_read.get_max_size();

        // Check size constraints
        if size > max_size {
            return Err(BPlusTreeError::ConversionError(format!(
                "Leaf page {} exceeds max size: {} > {}",
                leaf_page.get_page_id(),
                size,
                max_size
            )));
        }

        // Validate key ordering in leaf
        for i in 1..size {
            if let (Some(prev_key), Some(curr_key)) =
                (leaf_read.get_key_at(i - 1), leaf_read.get_key_at(i))
                && (self.comparator)(prev_key, curr_key) != Ordering::Less
            {
                return Err(BPlusTreeError::ConversionError(format!(
                    "Keys not in order in leaf page {}: {} >= {}",
                    leaf_page.get_page_id(),
                    prev_key,
                    curr_key
                )));
            }
        }

        // Check that each key has a corresponding value
        for i in 0..size {
            if leaf_read.get_key_at(i).is_none() || leaf_read.get_value_at(i).is_none() {
                return Err(BPlusTreeError::ConversionError(format!(
                    "Missing key or value at index {} in leaf page {}",
                    i,
                    leaf_page.get_page_id()
                )));
            }
        }

        Ok(())
    }

    /// Validate an internal page and update statistics
    fn validate_internal_page(
        &self,
        internal_page: PageGuard<BPlusTreeInternalPage<K, C>>,
        current_depth: u32,
        stats: &mut ValidationStats,
    ) -> Result<(), BPlusTreeError> {
        let internal_read = internal_page.read();
        let page_id = internal_page.get_page_id();

        // Validate internal page structure
        let size = internal_read.get_size();
        let max_size = internal_read.get_max_size();

        // Check size constraints
        if size > max_size {
            return Err(BPlusTreeError::ConversionError(format!(
                "Internal page {} exceeds max size: {} > {}",
                page_id, size, max_size
            )));
        }

        // Internal page should have (size + 1) child pointers
        for i in 0..=size {
            if internal_read.get_value_at(i).is_none() {
                return Err(BPlusTreeError::ConversionError(format!(
                    "Missing child pointer at index {} in internal page {}",
                    i, page_id
                )));
            }
        }

        // Validate key ordering in internal page
        for i in 1..size {
            if let (Some(prev_key), Some(curr_key)) =
                (internal_read.get_key_at(i - 1), internal_read.get_key_at(i))
                && (self.comparator)(&prev_key, &curr_key) != Ordering::Less
            {
                return Err(BPlusTreeError::ConversionError(format!(
                    "Keys not in order in internal page {}: {} >= {}",
                    page_id, prev_key, curr_key
                )));
            }
        }

        // Recursively validate all children
        for i in 0..=size {
            if let Some(child_id) = internal_read.get_value_at(i) {
                self.validate_node(child_id, current_depth + 1, stats)?;
            }
        }

        Ok(())
    }

    /// Validate the linked list structure of leaf pages
    pub fn validate_leaf_links(&self) -> Result<(), BPlusTreeError> {
        // Find the leftmost leaf
        let leftmost_leaf = match self.find_leftmost_leaf() {
            Ok(leaf) => leaf,
            Err(BPlusTreeError::BufferPoolError(_)) => return Ok(()), // Empty tree
            Err(e) => return Err(e),
        };

        let mut current_leaf = leftmost_leaf;
        let mut visited_pages = std::collections::HashSet::new();

        // Traverse the leaf link list
        loop {
            let page_id = current_leaf.get_page_id();

            // Check for cycles
            if visited_pages.contains(&page_id) {
                return Err(BPlusTreeError::ConversionError(format!(
                    "Cycle detected in leaf links at page {}",
                    page_id
                )));
            }
            visited_pages.insert(page_id);

            let next_page_id = {
                let leaf_read = current_leaf.read();
                leaf_read.get_next_page_id()
            };

            // If no next page, we're done
            let next_id = match next_page_id {
                Some(id) if id != INVALID_PAGE_ID => id,
                _ => break,
            };

            // Fetch next page
            current_leaf = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(next_id)
                .ok_or(BPlusTreeError::PageNotFound(next_id))?;
        }

        Ok(())
    }

    /// Get metadata information
    pub fn get_metadata(&self) -> &IndexInfo {
        &self.metadata
    }

    /// Comprehensive tree health check
    pub fn health_check(&self) -> Result<ValidationStats, BPlusTreeError> {
        // Validate tree structure
        let stats = self.validate_tree()?;

        // Validate leaf links
        self.validate_leaf_links()?;

        // Additional checks could be added here:
        // - Page utilization statistics
        // - Key distribution analysis
        // - Performance metrics

        println!("B+Tree Health Check Results:");
        println!("  Max Depth: {}", stats.max_depth);
        println!("  Total Keys: {}", stats.total_keys);
        println!("  Leaf Level: {:?}", stats.leaf_level);
        println!("  Tree Height: {}", self.get_height());
        println!("  All validations passed ✓");

        Ok(stats)
    }
}

impl ValidationStats {
    fn new() -> Self {
        Self {
            max_depth: 0,
            total_keys: 0,
            leaf_level: None,
        }
    }

    /// Get the maximum depth encountered during validation
    pub fn get_max_depth(&self) -> u32 {
        self.max_depth
    }

    /// Get the total number of keys counted
    pub fn get_total_keys(&self) -> usize {
        self.total_keys
    }

    /// Get the level at which leaf nodes are found
    pub fn get_leaf_level(&self) -> Option<u32> {
        self.leaf_level
    }

    /// Check if the tree is balanced (all leaves at same level)
    pub fn is_balanced(&self) -> bool {
        self.leaf_level.is_none_or(|level| level == self.max_depth)
    }
}

pub fn create_index(
    key_type: TypeId,
    buffer_pool_manager: Arc<BufferPoolManager>,
    metadata: IndexInfo,
) -> Result<TypedBPlusTreeIndex, String> {
    match key_type {
        TypeId::Integer => Ok(TypedBPlusTreeIndex::new_i32_index(
            i32_comparator,
            metadata,
            buffer_pool_manager,
        )),
        TypeId::VarChar => {
            // VarChar index not yet implemented
            Err(format!(
                "VarChar index type not yet implemented (key_type: {:?})",
                key_type
            ))
        },
        // Other types
        _ => Err(format!("Unsupported index key type: {:?}", key_type)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::TempDir;

    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::index::{IndexInfo, IndexType};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            Self::with_pool_size(name, 10).await
        }

        pub async fn with_pool_size(name: &str, pool_size: usize) -> Self {
            initialize_logger();
            const K: usize = 2;

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
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool_size, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    pool_size,
                    Arc::from(disk_manager.unwrap()),
                    replacer.clone(),
                )
                .unwrap(),
            );

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }
    }

    #[tokio::test]
    async fn test_basic_tree_operations() {
        let ctx = TestContext::new("test_basic_tree_operations").await;
        let bpm = ctx.bpm;

        // Create a simple schema
        let key_schema = Schema::new(vec![]);

        // Create metadata for index
        let metadata = IndexInfo::new(
            key_schema,
            "test_index".to_string(),
            1, // index_oid
            "test_table".to_string(),
            4,     // key_size (4 bytes for i32)
            false, // is_primary_key
            IndexType::BPlusTreeIndex,
            vec![0], // key_attrs
        );

        // Create B+ tree
        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);

        // Initialize tree with order 4
        println!("Initializing tree...");
        assert!(tree.init_with_order(4).is_ok());
        println!("Tree initialized. Is empty: {}", tree.is_empty());

        // Insert a test key-value pair
        let key = 42;
        let value = RID::new(1, 1);
        println!("Inserting key {} with value {:?}", key, value);
        let insert_result = tree.insert(key, value);
        if let Err(ref e) = insert_result {
            println!("Insert failed with error: {}", e);
        }
        assert!(insert_result.is_ok());
        println!(
            "Insert successful. Tree height: {}, size: {}",
            tree.get_height(),
            tree.get_size()
        );

        // Search for the key
        println!("Searching for key {}", key);
        let search_result = tree.search(&key);
        match &search_result {
            Ok(Some(found_value)) => println!("Found value: {:?}", found_value),
            Ok(None) => println!("Key not found"),
            Err(e) => println!("Search failed with error: {}", e),
        }
        let result = search_result.unwrap();
        assert!(
            result.is_some(),
            "Expected to find key {} but got None",
            key
        );
        assert_eq!(result.unwrap(), value);
        println!("Search test passed");

        // Remove the key
        println!("Removing key {}", key);
        let remove_result = tree.remove(&key).unwrap();
        assert!(remove_result);
        println!("Remove successful");

        // Verify key is removed
        let search_result = tree.search(&key).unwrap();
        assert!(search_result.is_none());
        println!("Verification after removal passed");

        // Add several keys to potentially trigger page splits and merges
        println!("Adding multiple keys...");
        for i in 1..10 {
            if i == 5 {
                println!("\n=== DETAILED DEBUG FOR KEY 5 ===");
                println!("About to insert key 5...");

                // Print tree before inserting key 5
                println!("Tree before inserting key 5:");
                let _ = tree.print_tree();

                // Print keys in order before key 5
                match tree.in_order_traversal() {
                    Ok(keys) => println!("Current keys in tree: {:?}", keys),
                    Err(e) => println!("Failed to get keys: {}", e),
                }
            }

            println!("Inserting key {}", i);
            assert!(tree.insert(i, RID::new(1, i as u32)).is_ok());

            if i == 5 {
                println!("Key 5 inserted successfully!");

                // Print tree after inserting key 5
                println!("Tree after inserting key 5:");
                let _ = tree.print_tree();

                // Print keys in order after key 5
                match tree.in_order_traversal() {
                    Ok(keys) => println!("Keys after inserting 5: {:?}", keys),
                    Err(e) => println!("Failed to get keys: {}", e),
                }

                // Try to search for key 5
                match tree.search(&5) {
                    Ok(Some(v)) => println!("Key 5 found with value: {:?}", v),
                    Ok(None) => println!("WARNING: Key 5 not found immediately after insertion!"),
                    Err(e) => println!("Error searching for key 5: {}", e),
                }
                println!("=== END DETAILED DEBUG FOR KEY 5 ===\n");
            }

            // Print tree structure after key insertions to debug
            if i == 4 {
                println!("\n=== Tree structure after inserting key {} ===", i);
                let _ = tree.print_tree();
                println!("=======================================\n");
            }
            if i == 6 {
                println!(
                    "\n=== Tree structure after inserting key {} (before split) ===",
                    i
                );
                let _ = tree.print_tree();
                println!("=======================================\n");
            }
            if i == 7 {
                println!(
                    "\n=== Tree structure after inserting key {} (after split) ===",
                    i
                );
                let _ = tree.print_tree();
                println!("=======================================\n");
            }
        }
        println!(
            "Tree height after inserts: {}, size: {}",
            tree.get_height(),
            tree.get_size()
        );

        println!("\n=== Final tree structure ===");
        let _ = tree.print_tree();
        println!("=======================================\n");

        // Verify all keys exist
        println!("Verifying all keys exist...");
        for i in 1..10 {
            let result = tree.search(&i).unwrap();
            if result.is_none() {
                println!("Key {} not found!", i);
                // Print tree again when we find the missing key
                println!("\n=== Tree structure when key {} was not found ===", i);
                let _ = tree.print_tree();
                println!("=======================================\n");

                // Also print in-order traversal to see all keys that exist
                match tree.in_order_traversal() {
                    Ok(keys) => println!("Keys in tree (in-order): {:?}", keys),
                    Err(e) => println!("Failed to get in-order traversal: {}", e),
                }
            }
            assert!(result.is_some(), "Key {} should exist but was not found", i);
            assert_eq!(result.unwrap(), RID::new(1, i as u32));
        }
        println!("All keys verification passed");

        // Test range scan
        let range_result = tree.range_scan(&3, &7).unwrap();
        assert_eq!(range_result.len(), 5); // Should include 3, 4, 5, 6, 7

        // Remove keys to potentially trigger merges
        for i in 3..7 {
            assert!(tree.remove(&i).unwrap());
        }

        // Verify keys were removed
        for i in 3..7 {
            assert!(tree.search(&i).unwrap().is_none());
        }

        // Verify remaining keys still exist
        for i in 1..3 {
            assert!(tree.search(&i).unwrap().is_some());
        }
        for i in 7..10 {
            assert!(tree.search(&i).unwrap().is_some());
        }

        println!("All tests passed");
    }

    #[tokio::test]
    async fn test_debug_key_5_split() {
        let ctx = TestContext::new("test_debug_key_5_split").await;
        let bpm = ctx.bpm;

        // Create a simple schema
        let key_schema = Schema::new(vec![]);

        // Create metadata for index
        let metadata = IndexInfo::new(
            key_schema,
            "test_index".to_string(),
            1, // index_oid
            "test_table".to_string(),
            4,     // key_size (4 bytes for i32)
            false, // is_primary_key
            IndexType::BPlusTreeIndex,
            vec![0], // key_attrs
        );

        // Create B+ tree
        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);

        // Initialize tree with order 4
        assert!(tree.init_with_order(4).is_ok());

        // Insert keys 1,2,3,4 to fill up the leaf
        for i in 1..=4 {
            println!("Inserting key {}", i);
            assert!(tree.insert(i, RID::new(1, i as u32)).is_ok());
        }

        println!("\n=== Tree before inserting key 5 (should be full) ===");
        let _ = tree.print_tree();

        // Get the leaf page directly to examine it
        let root_page_id = tree.get_root_page_id();
        let leaf_page = tree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<i32, RID, I32Comparator>>(root_page_id)
            .unwrap();

        {
            let leaf_read = leaf_page.read();
            println!("Leaf page before split:");
            println!(
                "  Size: {}, Max size: {}",
                leaf_read.get_size(),
                leaf_read.get_max_size()
            );
            print!("  Keys: [");
            for i in 0..leaf_read.get_size() {
                if let Some(key) = leaf_read.get_key_at(i) {
                    print!("{}", key);
                    if i < leaf_read.get_size() - 1 {
                        print!(", ");
                    }
                }
            }
            println!("]");
            print!("  Values: [");
            for i in 0..leaf_read.get_size() {
                if let Some(value) = leaf_read.get_value_at(i) {
                    print!("{:?}", value);
                    if i < leaf_read.get_size() - 1 {
                        print!(", ");
                    }
                }
            }
            println!("]");
        }

        println!("\n=== Inserting key 5 (should trigger split) ===");
        assert!(tree.insert(5, RID::new(1, 5)).is_ok());

        println!("\n=== Tree after inserting key 5 ===");
        let _ = tree.print_tree();

        // Verify key 5 can be found
        match tree.search(&5) {
            Ok(Some(v)) => println!("SUCCESS: Key 5 found with value: {:?}", v),
            Ok(None) => println!("ERROR: Key 5 not found!"),
            Err(e) => println!("ERROR searching for key 5: {}", e),
        }

        // Print all keys in order
        match tree.in_order_traversal() {
            Ok(keys) => println!("All keys in tree: {:?}", keys),
            Err(e) => println!("Failed to get keys: {}", e),
        }
    }

    #[tokio::test]
    async fn test_validation_stats_integration() {
        let ctx = TestContext::new("test_validation_stats").await;
        let bpm = ctx.bpm;

        // Create a simple schema
        let key_schema = Schema::new(vec![]);

        // Create metadata for index
        let metadata = IndexInfo::new(
            key_schema,
            "test_validation_index".to_string(),
            1, // index_oid
            "test_table".to_string(),
            4,     // key_size (4 bytes for i32)
            false, // is_primary_key
            IndexType::BPlusTreeIndex,
            vec![0], // key_attrs
        );

        // Create B+ tree
        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm.clone());

        // Initialize tree with order 4
        assert!(tree.init_with_order(4).is_ok());

        // Validate empty tree
        let stats = tree.validate_tree().unwrap();
        assert_eq!(stats.get_total_keys(), 0);
        assert_eq!(stats.get_max_depth(), 0);
        assert!(stats.get_leaf_level().is_none());
        assert!(stats.is_balanced());

        // Insert some keys
        for i in 1..=10 {
            assert!(tree.insert(i, RID::new(1, i as u32)).is_ok());
        }

        // Validate tree structure
        let stats = tree.validate_tree().unwrap();
        assert_eq!(stats.get_total_keys(), 10);
        assert!(stats.get_max_depth() > 0);
        assert!(stats.get_leaf_level().is_some());
        assert!(stats.is_balanced());

        println!("Validation stats after inserting 10 keys:");
        println!("  Total keys: {}", stats.get_total_keys());
        println!("  Max depth: {}", stats.get_max_depth());
        println!("  Leaf level: {:?}", stats.get_leaf_level());
        println!("  Is balanced: {}", stats.is_balanced());

        // Test health check functionality
        let health_stats = tree.health_check().unwrap();
        assert_eq!(health_stats.get_total_keys(), stats.get_total_keys());
        assert_eq!(health_stats.get_max_depth(), stats.get_max_depth());

        // Test typed index functionality
        let typed_index = TypedBPlusTreeIndex::new_i32_index(
            i32_comparator,
            IndexInfo::new(
                Schema::new(vec![]),
                "typed_test".to_string(),
                2,
                "test_table".to_string(),
                4,
                false,
                IndexType::BPlusTreeIndex,
                vec![0],
            ),
            bpm.clone(),
        );

        assert_eq!(typed_index.get_type_id(), TypeId::Integer);
        assert!(typed_index.as_i32_index().is_some());

        // Test metadata access
        let metadata_ref = tree.get_metadata();
        assert_eq!(metadata_ref.get_index_name(), "test_validation_index");

        println!("All validation tests passed!");
    }

    // ============================================================================
    // LATCH CRABBING TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_latch_crabbing_optimistic_insert() {
        // Test the optimistic fast path for latch crabbing
        // This uses read latches going down and write latch only on leaf
        let ctx = TestContext::new("test_latch_crabbing_optimistic").await;
        let bpm = ctx.bpm;

        let key_schema = Schema::new(vec![]);
        let metadata = IndexInfo::new(
            key_schema,
            "latch_crabbing_optimistic_test".to_string(),
            1,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0],
        );

        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);
        // Use larger order to ensure most inserts use optimistic path
        assert!(tree.init_with_order(10).is_ok());

        // Insert keys - with order 10, most inserts will use optimistic path
        for i in 1..=8 {
            let result = tree.insert_with_latch_crabbing(i, RID::new(1, i as u32));
            assert!(result.is_ok(), "Failed to insert key {}", i);
        }

        // Verify all keys exist
        for i in 1..=8 {
            let result = tree.search(&i).unwrap();
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap(), RID::new(1, i as u32));
        }

        println!("Latch crabbing optimistic insert test passed!");
    }

    #[tokio::test]
    async fn test_latch_crabbing_optimistic_remove() {
        // Test the optimistic fast path for remove
        let ctx = TestContext::new("test_latch_crabbing_remove").await;
        let bpm = ctx.bpm;

        let key_schema = Schema::new(vec![]);
        let metadata = IndexInfo::new(
            key_schema,
            "latch_crabbing_remove_test".to_string(),
            1,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0],
        );

        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);
        // Use larger order to ensure removes use optimistic path
        assert!(tree.init_with_order(10).is_ok());

        // Insert keys
        for i in 1..=8 {
            assert!(
                tree.insert_with_latch_crabbing(i, RID::new(1, i as u32))
                    .is_ok()
            );
        }

        // Remove keys using latch crabbing (optimistic path since leaf won't underflow)
        for i in [1, 3, 5] {
            let result = tree.remove_with_latch_crabbing(&i);
            assert!(result.is_ok(), "Failed to remove key {}", i);
            assert!(result.unwrap(), "Key {} should have been removed", i);
        }

        // Verify removed keys don't exist
        for i in [1, 3, 5] {
            assert!(
                tree.search(&i).unwrap().is_none(),
                "Key {} should be gone",
                i
            );
        }

        // Verify remaining keys exist
        for i in [2, 4, 6, 7, 8] {
            assert!(tree.search(&i).unwrap().is_some(), "Key {} should exist", i);
        }

        println!("Latch crabbing optimistic remove test passed!");
    }

    #[tokio::test]
    async fn test_latch_crabbing_fallback_to_standard() {
        // Test that when optimistic path fails, we correctly fall back to standard insert
        let ctx = TestContext::new("test_latch_crabbing_fallback").await;
        let bpm = ctx.bpm;

        let key_schema = Schema::new(vec![]);
        let metadata = IndexInfo::new(
            key_schema,
            "latch_crabbing_fallback_test".to_string(),
            1,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0],
        );

        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);
        assert!(tree.init_with_order(4).is_ok());

        // Insert keys using latch crabbing - will use optimistic when possible,
        // standard insert when splits are needed
        for i in 1..=6 {
            let result = tree.insert_with_latch_crabbing(i, RID::new(1, i as u32));
            assert!(result.is_ok(), "Failed to insert key {}", i);
        }

        // Verify all keys
        for i in 1..=6 {
            let result = tree.search(&i).unwrap();
            assert!(result.is_some(), "Key {} should exist", i);
        }

        println!("Latch crabbing fallback test passed!");
    }

    #[tokio::test]
    async fn test_latch_crabbing_update_existing_key() {
        // Test that updating an existing key works correctly
        let ctx = TestContext::new("test_latch_crabbing_update").await;
        let bpm = ctx.bpm;

        let key_schema = Schema::new(vec![]);
        let metadata = IndexInfo::new(
            key_schema,
            "latch_crabbing_update_test".to_string(),
            1,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0],
        );

        let mut tree =
            BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm);
        assert!(tree.init_with_order(10).is_ok());

        // Insert a key
        assert!(tree.insert_with_latch_crabbing(5, RID::new(1, 100)).is_ok());
        assert_eq!(tree.search(&5).unwrap(), Some(RID::new(1, 100)));

        // Update the same key with a different value
        assert!(tree.insert_with_latch_crabbing(5, RID::new(1, 200)).is_ok());
        assert_eq!(tree.search(&5).unwrap(), Some(RID::new(1, 200)));

        println!("Latch crabbing update test passed!");
    }
}
