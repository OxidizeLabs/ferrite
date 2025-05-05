use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::{
    config::{PageId, INVALID_PAGE_ID},
    rid::RID,
};
use crate::storage::index::index::IndexInfo;
use crate::storage::index::types::comparators::{i32_comparator, I32Comparator};
use crate::storage::index::types::{KeyComparator, KeyType, ValueType};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::{
    b_plus_tree_internal_page::BPlusTreeInternalPage, b_plus_tree_leaf_page::BPlusTreeLeafPage,
};
use crate::types_db::type_id::TypeId;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

pub struct TypedBPlusTreeIndex {
    // Index metadata, etc.
    type_id: TypeId,
    // Store the actual implementation based on type
    implementation: Box<dyn Any + Send + Sync>,
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
            100,
            100,
        );

        Self {
            type_id: TypeId::Integer,
            implementation: Box::new(index),
        }
    }

    // pub fn insert(&self, key: &Value, value: &Value) -> Result<(), BPlusTreeError> {
    //     match self.type_id {
    //         TypeId::Integer => {
    //             let index = self
    //                 .implementation
    //                 .downcast_ref::<BPlusTreeIndex<i32, RID, I32Comparator>>()
    //                 .ok_or(BPlusTreeError::InvalidPageType)?;
    //             let key = i32::from_value(key)?;
    //             let value = RID::from_value(value)?;
    //             index.insert(key, value)
    //         }
    //         // Handle other types
    //         _ => Err(BPlusTreeError::InvalidPageType),
    //     }
    // }

    // Similar implementations for search, remove, range_scan, etc.
}

impl From<String> for BPlusTreeError {
    fn from(error: String) -> Self {
        BPlusTreeError::ConversionError(error) // Assuming you have a ConversionError variant
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
    leaf_max_size: usize,
    internal_max_size: usize,
}

impl<K, V, C> BPlusTreeIndex<K, V, C>
where
    K: KeyType + Send + Sync + Debug + Display + 'static,
    V: ValueType + Send + Sync + 'static + PartialEq,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
{
    pub fn new(
        comparator: C,
        metadata: IndexInfo,
        buffer_pool_manager: Arc<BufferPoolManager>,
        leaf_max_size: usize,
        internal_max_size: usize,
    ) -> Self {
        Self {
            comparator,
            metadata: Box::new(metadata),
            buffer_pool_manager,
            header_page_id: INVALID_PAGE_ID,
            _marker: PhantomData,
            leaf_max_size,
            internal_max_size,
        }
    }
}

impl<K, V, C> BPlusTreeIndex<K, V, C>
where
    K: KeyType + Send + Sync + Debug + Display + Serialize + for<'de> Deserialize<'de> + 'static,
    V: ValueType + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static + PartialEq,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
{
    /// Initialize a new B+ tree
    pub fn init(&mut self) -> Result<(), BPlusTreeError> {
        // 1. Check if header_page_id is valid - if so, tree is already initialized
        // 2. Allocate a new header page using buffer_pool_manager
        // 3. Initialize header with: root_page_id = INVALID_PAGE_ID, tree_height = 0, num_keys = 0
        // 4. Store the header_page_id in self
        // 5. Mark header page as dirty and unpin it
        // 6. Return Ok(())
        unimplemented!()
    }

    /// Insert a key-value pair into the B+ tree
    pub fn insert(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // 1. If tree is not initialized (header_page_id is INVALID_PAGE_ID), initialize it first
        // 2. If root is INVALID_PAGE_ID, create first leaf page as root:
        //    a. Allocate a new leaf page with buffer_pool_manager.new_page()
        //    b. Cast to leaf page type and initialize with leaf_max_size and comparator
        //    c. Insert key-value pair into leaf using insert_key_value()
        //    d. Update header page: set root_page_id to new page, tree_height = 1, num_keys = 1
        //    e. Mark leaf page as dirty and unpin it
        // 3. Otherwise, for an existing tree:
        //    a. Fetch the header page to get root_page_id
        //    b. Call find_leaf_page to traverse down to correct leaf for the key
        //    c. Check if key already exists in leaf:
        //       - If exists, update the value (replace) and return
        //       - If not, continue with insertion
        //    d. Try to insert the key-value pair into the leaf:
        //       - If leaf has space (size < max_size), insert directly
        //       - If successful, update header (increment num_keys)
        //       - If leaf is full, initiate the split process:
        //         * Call split_leaf_page which may trigger recursive splits
        //         * This may cause a new root to be created if splits reach the root
        // 4. Handle any concurrent modification issues with proper locking/latching
        // 5. Ensure all pages are properly unpinned in all cases (both success and error)
        unimplemented!()
    }

    /// Search for a key in the B+ tree
    pub fn search(&self, key: &K) -> Result<Option<V>, BPlusTreeError> {
        // 1. Check if the tree is initialized:
        //    a. If header_page_id is INVALID_PAGE_ID, initialize or return None
        // 2. Fetch the header page to get the root_page_id
        //    a. If root_page_id is INVALID_PAGE_ID, tree is empty, return None
        // 3. Call find_leaf_page to navigate to the appropriate leaf:
        //    a. This traverses from root to leaf following appropriate pointers
        //    b. Handles all navigation through internal nodes
        // 4. Once at the leaf page, search for the key:
        //    a. Use find_key_index to get potential position in the leaf
        //    b. Check if the index is valid and the key matches:
        //       - If the index is out of bounds or the key doesn't match, key not found
        //       - If key matches, retrieve the value from the leaf at that index
        // 5. Create a copy of the found value (if any) to return to caller
        // 6. Unpin the leaf page (very important to prevent buffer pool exhaustion)
        // 7. Return the value if found, or None if not found
        // 8. Handle any errors during the process (page not found, invalid type, etc.)
        unimplemented!()
    }

    /// Remove a key-value pair from the B+ tree
    pub fn remove(&self, key: &K) -> Result<bool, BPlusTreeError> {
        // 1. Check if tree is initialized; if not, return false (nothing to remove)
        // 2. Get root page ID from header page; if INVALID_PAGE_ID, return false
        // 3. Find the leaf page containing the key:
        //    a. Start at root page
        //    b. Traverse down to leaf using find_leaf_page
        //    c. Track parent pages during traversal (for rebalancing)
        // 4. Search for the key in the leaf page:
        //    a. Use find_key_index to locate exact position
        //    b. If key not found (index out of bounds or key mismatch), return false
        //    c. If key found, call remove_key_value_at to delete from leaf
        // 5. Update header page metadata (decrement num_keys)
        // 6. Check if leaf underflow occurred:
        //    a. If leaf is root and now empty:
        //       - Update header to mark tree as empty (set root_page_id = INVALID_PAGE_ID)
        //       - Decrement tree height to 0 (tree is now empty)
        //    b. If leaf is root but still has keys, no rebalancing needed
        //    c. If leaf is not root and underflow occurred (size < min_size):
        //       - Call check_and_handle_underflow to rebalance the tree
        // 7. Mark all modified pages as dirty
        // 8. Unpin all pages accessed during deletion
        // 9. Return true (indicating successful deletion)
        unimplemented!()
    }

    /// Custom range scan to find all keys in a given range [start, end]
    pub fn range_scan(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, BPlusTreeError> {
        // 1. Validate input: if start > end (using comparator), return empty vector
        // 2. Check if tree is empty, if so return empty vector
        // 3. Initialize result vector to collect key-value pairs
        // 4. Find the leaf containing the start key:
        //    a. Call find_leaf_page(start) to get to starting leaf
        // 5. Linear scan within the first leaf:
        //    a. Find index of first key >= start using binary search
        //    b. Scan from this index through all keys in the leaf:
        //       - For each key <= end, add (key, value) to result
        //       - If key > end, we're done, break and return result
        // 6. If we've processed all keys in the current leaf:
        //    a. Check if there's a next leaf (next_page_id)
        //    b. If no next leaf, we're done, return result
        //    c. Otherwise, fetch the next leaf page
        //    d. Unpin the current leaf
        //    e. Continue scanning this next leaf from beginning
        //    f. Repeat steps 5b-6e until either:
        //       - We find a key > end
        //       - We reach the end of the leaf chain
        // 7. Unpin the final leaf page
        // 8. Return the collected key-value pairs
        // 9. This traversal uses the leaf node chain for efficient range scans,
        //    which is a key advantage of B+ Trees over regular B-Trees
        unimplemented!()
    }

    /// Find the leaf page that should contain the key
    fn find_leaf_page(
        &self,
        key: &K,
    ) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // 1. Fetch the header page to get the root_page_id
        //    a. If header page doesn't exist, return appropriate error
        // 2. Check if root_page_id is valid:
        //    a. If INVALID_PAGE_ID, return error (tree is empty)
        // 3. Fetch the root page using buffer_pool_manager.fetch_page
        //    a. Start with the root page as current_page
        // 4. Determine the page type (internal or leaf):
        //    a. Check the page type field in the page header
        // 5. While current_page is an internal page:
        //    a. Cast current_page to BPlusTreeInternalPage<K, C>
        //    b. Find the appropriate child pointer for the key:
        //       - Use internal_page.find_child_for_key(key)
        //       - This compares the key with the keys in the internal node
        //       - Returns the page_id of the child to follow
        //    c. Fetch the child page from buffer pool
        //    d. Unpin the current (parent) page - no longer needed
        //    e. Update current_page to the child page
        //    f. Repeat until reaching a leaf page
        // 6. When a leaf page is reached:
        //    a. Cast current_page to BPlusTreeLeafPage<K, V, C>
        //    b. Return the leaf page (still pinned for caller's use)
        // 7. Handle any errors (page not found, invalid page type, etc.)
        //    a. Ensure all pages are unpinned even in error cases
        unimplemented!()
    }

    /// Split a leaf page when it becomes full
    fn split_leaf_page(
        &self,
        leaf_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new leaf page with buffer_pool_manager.new_page()
        // 2. Determine split point at ceiling(max_size/2):
        //    a. For a B+ tree, we typically split at the midpoint
        //    b. This ensures balanced nodes after split (at least half-full)
        // 3. Copy keys/values after split point to the new page:
        //    a. For each position i from split_point to max_size-1:
        //       - Get key/value at position i from original leaf
        //       - Insert into new leaf page in the same order
        //       - Remove from original leaf (or mark for removal)
        //    b. Maintain sorted order in both pages
        // 4. Update the linked list structure of leaf pages:
        //    a. New page's next_page_id = original page's next_page_id
        //    b. Original page's next_page_id = new page's page_id
        //    c. This maintains the sequential access property of B+ Trees
        // 5. Get the first key in the new page to use as separator key
        //    a. Unlike B-Trees, in B+ Trees the separator goes to parent but also stays in leaf
        // 6. Insert separator key and new page's page_id into parent:
        //    a. If leaf_page is root (no parent):
        //       - Call create_new_root with separator key
        //       - This increases the tree height by 1
        //    b. Otherwise:
        //       - Find the parent page (track during tree traversal)
        //       - Insert separator key and new page's page_id into parent
        //       - If parent becomes full, recursively call split_internal_page
        // 7. Update size fields in both leaf pages
        // 8. Mark both pages as dirty
        // 9. Unpin the new page (keep original locked if needed by caller)
        unimplemented!()
    }

    /// Split an internal page when it becomes full
    fn split_internal_page(
        &self,
        internal_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new internal page with buffer_pool_manager.new_page()
        // 2. Determine the split point at floor(max_size/2):
        //    a. For internal nodes, we typically split at middle key
        //    b. The middle key will be promoted to the parent
        // 3. Get the middle key at split point (index = max_size/2)
        //    a. Unlike leaf splits, the middle key goes up to parent and is removed from children
        // 4. Copy keys/child pointers after middle key to the new page:
        //    a. For keys, copy from (split_point+1) to (max_size-1)
        //    b. For values (pointers), ensure proper transfer of child pointers:
        //       - The child pointer immediately after middle key (at split_point+1)
        //       - Goes to the leftmost position in the new page
        //       - The remaining pointers follow their respective keys
        // 5. Remove the middle key and all copied keys/pointers from original page
        // 6. Insert the middle key and new page's page_id into parent:
        //    a. If internal_page is root (no parent):
        //       - Call create_new_root with middle key
        //       - This increases tree height by 1
        //    b. Otherwise:
        //       - Find the parent page (track during tree traversal)
        //       - Insert middle key and new page's page_id into parent
        //       - If parent becomes full, recursively split it
        // 7. Update child pointers - inform children about their new parent if necessary
        // 8. Update size fields in both internal pages
        // 9. Mark both pages as dirty
        // 10. Unpin the new page (keep original locked if needed by caller)
        unimplemented!()
    }

    /// Merge two leaf pages when they become too empty
    fn merge_leaf_pages(
        &self,
        left_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
        right_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
        parent_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Verify merge is possible (left_size + right_size <= max_size)
        //    a. If not possible, use redistribute_leaf_pages instead
        // 2. Get current sizes of both pages
        // 3. Get the separator key from parent at parent_key_index
        //    a. In B+ trees, this key doesn't go to leaves (unlike B-trees)
        // 4. Copy all keys/values from right page to left page:
        //    a. Get each key/value pair from right page
        //    b. Append to left page's keys/values arrays
        //    c. Update left page's size accordingly
        // 5. Update leaf page linked list:
        //    a. Set left_page.next_page_id = right_page.next_page_id
        //    b. This maintains the sequential access property
        // 6. Remove the separator key from parent at parent_key_index
        // 7. Remove right page's page_id from parent's child pointers
        //    a. This is typically at index parent_key_index + 1
        // 8. Check if parent needs rebalancing:
        //    a. If parent is root and now has no keys:
        //       - Left page becomes new root (collapse a level)
        //       - Update header page (decrement tree height)
        //    b. Otherwise, parent underflow will be handled by caller
        // 9. Mark left page and parent page as dirty
        // 10. Delete right page from buffer pool (or mark for reuse)
        // 11. Return success
        unimplemented!()
    }

    /// Merge two internal pages when they become too empty
    fn merge_internal_pages(
        &self,
        left_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Verify merge is possible (left_size + right_size + 1 <= max_size)
        //    a. Add 1 because parent key will move down to left page
        //    b. If not possible, use redistribute_internal_pages instead
        // 2. Get current sizes of both pages
        // 3. Get the separator key from parent at parent_key_index
        //    a. Unlike leaf merges, in internal merges this key moves down
        // 4. Append the separator key to left page:
        //    a. This becomes the divider between left's and right's children
        // 5. Copy all keys from right page to left page:
        //    a. Get each key from right page
        //    b. Append to left page's keys array
        // 6. Copy all child pointers from right page to left page:
        //    a. Get each child pointer from right page
        //    b. Append to left page's values array
        //    c. Each copied child now has left_page as its parent
        // 7. Remove the separator key from parent at parent_key_index
        // 8. Remove right page's page_id from parent's child pointers
        //    a. This is typically at index parent_key_index + 1
        // 9. Update left page's size and right page is now unused
        // 10. Check if parent needs rebalancing:
        //     a. If parent is root and now has no keys:
        //        - Left page becomes new root (collapse a level)
        //        - Update header page (decrement tree height)
        //     b. Otherwise, parent underflow will be handled by caller
        // 11. Mark left page and parent page as dirty
        // 12. Delete right page from buffer pool (or mark for reuse)
        // 13. Return success
        unimplemented!()
    }

    /// Redistribute keys between two leaf pages to avoid merge
    fn redistribute_leaf_pages(
        &self,
        left_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
        right_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
        parent_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Calculate total keys in both pages
        // 2. Calculate target size after redistribution:
        //    a. Aim for roughly equal distribution (total ÷ 2)
        //    b. Ensure both pages will be at least min_size
        // 3. Determine which way to move keys:
        //    a. If left_size > right_size, move from left to right
        //    b. If right_size > left_size, move from right to left
        // 4. If moving from left to right:
        //    a. Calculate how many keys to move
        //    b. For each key to move (starting from end of left):
        //       - Get key/value from left page
        //       - Insert at beginning of right page
        //       - Remove from left page
        // 5. If moving from right to left:
        //    a. Calculate how many keys to move
        //    b. For each key to move (starting from beginning of right):
        //       - Get key/value from right page
        //       - Insert at end of left page
        //       - Remove from right page
        // 6. Update the separator key in parent:
        //    a. The separator should be the first key in right page
        //    b. Get first key from right page
        //    c. Replace parent key at parent_key_index
        // 7. Update sizes of both leaf pages
        // 8. Mark all three pages as dirty
        // 9. Return success
        unimplemented!()
    }

    /// Redistribute keys between two internal pages to avoid merge
    fn redistribute_internal_pages(
        &self,
        left_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Calculate total keys in both pages (including parent separator)
        //    a. Total = left_size + right_size + 1 (for parent key)
        // 2. Calculate target size after redistribution:
        //    a. Aim for roughly equal distribution
        //    b. Ensure both pages will be at least min_size
        // 3. Get the current separator key from parent at parent_key_index
        // 4. If moving from left to right (left_size > right_size):
        //    a. Calculate how many keys to move
        //    b. Save the last key of left page (will become new separator)
        //    c. Move parent separator key to beginning of right page
        //    d. For each key to move (except the last):
        //       - Move key from left to right page
        //       - Move corresponding child pointer from left to right
        //    e. Move the last child pointer associated with the new separator
        //    f. Update parent key at parent_key_index with saved last key
        // 5. If moving from right to left (right_size > left_size):
        //    a. Calculate how many keys to move
        //    b. Move parent separator key to end of left page
        //    c. Save the first key of right page (will become new separator)
        //    d. Move first child pointer from right to left
        //    e. For each additional key to move:
        //       - Move key from right to left page
        //       - Move corresponding child pointer from right to left
        //    f. Update parent key at parent_key_index with saved first key
        // 6. Update sizes of both internal pages
        // 7. Mark all three pages as dirty
        // 8. Return success
        unimplemented!()
    }

    /// Create a new root page
    fn create_new_root(
        &self,
        left_child_id: PageId,
        key: K,
        right_child_id: PageId,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new internal page with buffer_pool_manager.new_page()
        // 2. Initialize the new page as a root node using new_root_page:
        //    a. Set is_root flag to true
        //    b. Configure with internal_max_size and comparator
        // 3. Populate the new root page:
        //    a. Call populate_new_root with:
        //       - left_child_id: ID of the original page (pre-split)
        //       - key: The separator/middle key
        //       - right_child_id: ID of the newly created page from split
        //    b. This sets up a parent with one key and two child pointers
        // 4. Update children to no longer be root nodes (if they were):
        //    a. Fetch left_child and set is_root = false if it's an internal page
        //    b. Fetch right_child and set is_root = false if it's an internal page
        //    c. Mark both as dirty if changed
        // 5. Get the header page and update:
        //    a. Set root_page_id to the new root's page_id
        //    b. Increment tree_height by 1
        //    c. num_keys stays the same (splits don't add new keys, just reorganize)
        // 6. Mark the header page and new root page as dirty
        // 7. Unpin all pages (header, new root, and any children fetched)
        // 8. Note: After this operation, the tree height increases by 1
        unimplemented!()
    }

    /// Update header page after operations
    fn update_header(
        &self,
        root_id: PageId,
        height: u32,
        num_keys: usize,
    ) -> Result<(), BPlusTreeError> {
        // 1. Fetch the header page using header_page_id
        // 2. Update the header page fields:
        //    a. Set root_page_id to root_id
        //    b. Set tree_height to height
        //    c. Set num_keys to num_keys
        // 3. Mark the header page as dirty
        // 4. Unpin the header page
        unimplemented!()
    }

    /// Check if the B+ tree is empty
    pub fn is_empty(&self) -> bool {
        // 1. Fetch the header page
        // 2. Check if root_page_id is INVALID_PAGE_ID
        // 3. Unpin the header page
        // 4. Return the result
        unimplemented!()
    }

    /// Get the current height of the B+ tree
    pub fn get_height(&self) -> u32 {
        // 1. Fetch the header page
        // 2. Get the tree_height value
        // 3. Unpin the header page
        // 4. Return the height
        unimplemented!()
    }

    /// Get the root page ID
    pub fn get_root_page_id(&self) -> PageId {
        // 1. Fetch the header page
        // 2. Get the root_page_id value
        // 3. Unpin the header page
        // 4. Return the root_page_id
        unimplemented!()
    }

    /// Get the total number of keys in the B+ tree
    pub fn get_size(&self) -> usize {
        // 1. Fetch the header page
        // 2. Get the num_keys value
        // 3. Unpin the header page
        // 4. Return the num_keys
        unimplemented!()
    }

    /// Find a sibling page for merging or redistribution
    fn find_sibling(
        &self,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        child_index: usize,
        prefer_left: bool,
    ) -> Result<(usize, PageId), BPlusTreeError> {
        // 1. Get the number of child pointers in parent_page
        // 2. If prefer_left and child_index > 0:
        //    a. Return (child_index-1, parent_page.get_value_at(child_index-1))
        // 3. If !prefer_left and child_index < num_children-1:
        //    a. Return (child_index, parent_page.get_value_at(child_index+1))
        // 4. If no sibling found in preferred direction, try the other direction
        // 5. If no sibling found in either direction, return an error
        unimplemented!()
    }

    /// Check if a page needs rebalancing after removal
    fn check_and_handle_underflow(
        &self,
        page_id: PageId,
        is_leaf: bool,
    ) -> Result<bool, BPlusTreeError> {
        // 1. Fetch the page (type depends on is_leaf)
        // 2. Check if page is root:
        //    a. If root and no keys but has children (only for internal pages):
        //       - Get the only child page ID
        //       - Update header to make this child the new root
        //       - Decrement tree height
        //       - Return true (tree structure changed)
        //    b. If root but doesn't meet collapse criteria, return false
        // 3. Check if page violates minimum size requirement:
        //    a. Get min_size for page type (typically max_size/2)
        //    b. If page size >= min_size, return false (no rebalancing needed)
        // 4. Find the parent page:
        //    a. Either via tracking during traversal or searching from root
        // 5. Find the index of page_id in parent's child pointers
        // 6. Try to find a sibling page:
        //    a. Prefer left sibling if available
        //    b. Otherwise, use right sibling
        //    c. Get sibling page ID from parent
        // 7. Fetch the sibling page
        // 8. Decide whether to merge or redistribute:
        //    a. Calculate total keys in both pages (+ separator for internal)
        //    b. If total <= max_size, perform merge
        //    c. Otherwise, redistribute keys
        // 9. If is_leaf, call appropriate leaf page function:
        //    a. merge_leaf_pages or redistribute_leaf_pages
        // 10. If not is_leaf, call appropriate internal page function:
        //     a. merge_internal_pages or redistribute_internal_pages
        // 11. Check if parent needs rebalancing after operation:
        //     a. If parent size < min_size, recursively call check_and_handle_underflow
        // 12. Return true (rebalancing occurred)
        unimplemented!()
    }

    /// Validate the B+ tree structure with in-order traversal
    pub fn validate(&self) -> Result<(), BPlusTreeError> {
        // 1. If tree is empty, return Ok (nothing to validate)
        // 2. Perform multi-level validation:
        //    a. Structure validation (B+ tree properties)
        //    b. Key ordering validation (in-order traversal)
        //    c. Balance validation (all leaf nodes at same depth)
        // 3. Start with the root page:
        //    a. Fetch the header page to get root_page_id
        //    b. If root is invalid, tree is empty, return Ok
        //    c. Fetch the root page
        // 4. Validate root node specifically:
        //    a. If root is a leaf, check it has no parent
        //    b. If root is internal and not leaf, ensure it has at least one child
        //    c. Root can have fewer than min_size keys (unlike other nodes)
        // 5. For recursive validation of the tree structure:
        //    a. Call validate_subtree(root_page, min_key, max_key, level, stats)
        //    b. This recursively validates each node and its children
        // 6. Verify all leaf nodes form a linked list:
        //    a. Traverse leaf nodes using next_page_id pointers
        //    b. Ensure keys are strictly increasing across leaves
        // 7. Verify tree height matches header:
        //    a. Track max depth to any leaf during traversal
        //    b. Compare with tree_height from header
        // 8. Count total keys and verify matches header num_keys
        // 9. Return Ok if all validations pass, or appropriate error
        unimplemented!()
    }

    /// Helper method to recursively validate a subtree
    fn validate_subtree(
        &self,
        page_id: PageId,
        min_key: Option<&K>,
        max_key: Option<&K>,
        level: u32,
        stats: &mut ValidationStats,
    ) -> Result<(), BPlusTreeError> {
        // 1. Fetch the page with page_id
        // 2. Determine if it's a leaf or internal page
        // 3. For all nodes (both leaf and internal):
        //    a. Verify all keys are in sorted order
        //    b. Verify all keys are within the min_key and max_key range:
        //       - All keys > min_key (if min_key is Some)
        //       - All keys < max_key (if max_key is Some)
        //    c. If not root, verify node has at least min_size keys
        //    d. Verify node has at most max_size keys
        // 4. For leaf nodes:
        //    a. If this is the first leaf at this level, record the level in stats
        //    b. Otherwise, verify this leaf is at the same level as other leaves
        //    c. Add to the total key count in stats
        //    d. If tracking leaf chain, store this leaf for later verification
        // 5. For internal nodes:
        //    a. For each child pointer:
        //       - Determine the valid key range for this child
        //       - Recursively validate the child subtree with updated range
        //    b. Verify each key correctly separates its children's key ranges
        // 6. Unpin the page when done
        // 7. Return Ok if all validations pass, or appropriate error
        unimplemented!()
    }

    /// Perform a level-order traversal (breadth-first) for debugging or visualization
    pub fn print_tree(&self) -> Result<(), BPlusTreeError> {
        // 1. If tree is empty, print "Empty tree" and return
        // 2. Print header information: height, size, root_id
        // 3. Initialize a queue with (root_page_id, level=0)
        // 4. While the queue is not empty:
        //    a. Dequeue a (page_id, level) pair
        //    b. Fetch the page
        //    c. Determine if it's a leaf or internal page
        //    d. Print level-appropriate indentation
        //    e. Print page information:
        //       - For internal nodes: keys and child pointers
        //       - For leaf nodes: keys, values, and next_page_id
        //    f. If internal node, enqueue all child pointers with level+1
        //    g. Unpin the page
        // 5. This traversal visits nodes level by level (breadth-first)
        //    which is useful for visualizing the tree structure
        unimplemented!()
    }

    /// Perform an in-order traversal of all keys (debug/verification)
    pub fn in_order_traversal(&self) -> Result<Vec<K>, BPlusTreeError> {
        // 1. If tree is empty, return empty vector
        // 2. Start with root page
        // 3. If root is a leaf, collect all keys and return
        // 4. For a B+ tree, in-order traversal can be done two ways:
        //    a. Recursive approach (for small trees):
        //       - For each internal node, recursively visit each child subtree
        //       - The recursive call is made between each key
        //       - This works but can cause stack overflow for large trees
        //    b. Iterative approach using leaf-chain (for B+ trees):
        //       - Find leftmost leaf using find_leftmost_leaf method
        //       - Collect all keys from this leaf
        //       - Follow next_page_id pointers collecting keys from each leaf
        //       - This is more efficient as all values are in leaf nodes
        // 5. Return the collected keys in sorted order
        unimplemented!()
    }

    /// Find the leftmost leaf page in the tree (for range scans or in-order traversal)
    fn find_leftmost_leaf(&self) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // 1. Fetch the header page to get root_page_id
        // 2. Check if root_page_id is valid:
        //    a. If INVALID_PAGE_ID, return error (tree is empty)
        // 3. Fetch the root page
        // 4. While the current page is an internal page:
        //    a. Cast the page to BPlusTreeInternalPage
        //    b. Get the leftmost child pointer (index 0)
        //    c. Fetch this child page
        //    d. Unpin the current page
        //    e. Update current page to the child
        // 5. When a leaf page is reached:
        //    a. Cast the page to BPlusTreeLeafPage
        //    b. Return this leaf page (still pinned)
        // 6. This leaf will contain the smallest keys in the tree
        unimplemented!()
    }

    /// Perform a pre-order traversal (useful for serialization)
    pub fn pre_order_traversal(&self) -> Result<Vec<PageId>, BPlusTreeError> {
        // 1. If tree is empty, return empty vector
        // 2. Initialize result vector to collect page IDs
        // 3. Initialize a stack with root_page_id
        // 4. While stack is not empty:
        //    a. Pop page_id from stack
        //    b. Add page_id to result vector (visit before children)
        //    c. Fetch the page
        //    d. If internal page:
        //       - Push all child pointers to stack in reverse order
        //         (so leftmost child is processed first)
        //    e. Unpin the page
        // 5. Return the collected page IDs in pre-order
        // 6. Note: Pre-order traversal visits a node before its children,
        //    which can be useful for serializing the tree structure
        unimplemented!()
    }

    /// Perform a post-order traversal (useful for safe deletion)
    pub fn post_order_traversal(&self) -> Result<Vec<PageId>, BPlusTreeError> {
        // 1. If tree is empty, return empty vector
        // 2. Initialize result vector to collect page IDs
        // 3. Use a recursive helper function to perform traversal:
        //    fn post_order_helper(page_id, result):
        //       a. Fetch the page
        //       b. If leaf page, add to result and return
        //       c. If internal page:
        //          - For each child pointer:
        //            * Recursively call post_order_helper on child
        //          - After all children processed, add this page_id to result
        //       d. Unpin the page
        // 4. Call post_order_helper starting with root_page_id
        // 5. Return the collected page IDs in post-order
        // 6. Note: Post-order traversal visits children before parents,
        //    which ensures safe deletion (children deleted before parents)
        unimplemented!()
    }
}

/// Tracks statistics during B+ tree validation
struct ValidationStats {
    /// Maximum depth encountered during validation
    max_depth: u32,
    /// Total number of keys counted in the tree
    total_keys: usize,
    /// Level at which leaf nodes are found (to verify all leaves are at same level)
    leaf_level: Option<u32>,
}

impl ValidationStats {
    fn new() -> Self {
        Self {
            max_depth: 0,
            total_keys: 0,
            leaf_level: None,
        }
    }
}

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
            // Create string index
            todo!()
        }
        // Other types
        _ => Err(format!("Unsupported index key type: {:?}", key_type)),
    }
}
