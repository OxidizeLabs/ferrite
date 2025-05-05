use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::DB_PAGE_SIZE;
use crate::common::{
    config::{PageId, INVALID_PAGE_ID},
    rid::RID,
};
use crate::storage::index::index::IndexInfo;
use crate::storage::page::page_types::{
    b_plus_tree_internal_page::BPlusTreeInternalPage,
    b_plus_tree_leaf_page::BPlusTreeLeafPage,
};
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use crate::storage::page::page_guard::PageGuard;

/// Trait for types that can be used as keys in indexes
pub trait KeyType: Sized + Clone + PartialEq + PartialOrd {
    /// Convert this key to a Value for storage
    fn to_value(&self) -> Value;

    /// Create a key from a Value
    fn from_value(value: &Value) -> Result<Self, String>;

    /// Get the TypeId for this key type
    fn type_id() -> TypeId;
}

impl KeyType for i32 {
    fn to_value(&self) -> Value {
        Value::new_with_type(Val::Integer(*self), TypeId::Integer)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        value.as_integer()
    }

    fn type_id() -> TypeId {
        TypeId::Integer
    }
}

// Similar implementations for other types like String, bool, etc.
impl KeyType for String {
    fn to_value(&self) -> Value {
        Value::new_with_type(Val::VarLen(self.clone()), TypeId::VarChar)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        match value.get_val() {
            Val::VarLen(s) => Ok(s.clone()),
            Val::ConstLen(s) => Ok(s.clone()),
            _ => Err(format!("Cannot convert {:?} to String", value)),
        }
    }

    fn type_id() -> TypeId {
        TypeId::VarChar
    }
}

/// Trait for types that can be used as values in indexes
pub trait ValueType: Sized + Clone {
    /// Convert this value to a Value for storage
    fn to_value(&self) -> Value;

    /// Create a value from a Value
    fn from_value(value: &Value) -> Result<Self, String>;
}

// Implement for RID (Row ID) or whatever you use for values
impl ValueType for RID {
    fn to_value(&self) -> Value {
        // Assuming RID can be represented as a BigInt or Struct
        Value::new_with_type(Val::BigInt(self.to_i64()), TypeId::BigInt)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        let id = value.as_bigint()?;
        Ok(RID::from_i64(id))
    }
}

/// Type for key comparators
pub type I32Comparator = fn(&i32, &i32) -> Ordering;

// Helper functions
fn i32_comparator(a: &i32, b: &i32) -> Ordering {
    a.cmp(b)
}

// KeyComparator trait
pub trait KeyComparator<K: KeyType>: Fn(&K, &K) -> Ordering {
    // Additional functionality can go here
}

// Implement KeyComparator for function types

// This would be in the module where KeyComparator is defined
impl<K> KeyComparator<K> for fn(&K, &K) -> Ordering
where
    K: KeyType,
{
    // Implement the required methods from the KeyComparator trait
    // ...
}

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
        // 1. If tree is not initialized (header_page_id is INVALID_PAGE_ID), initialize it
        // 2. If root is INVALID_PAGE_ID, create first leaf page as root:
        //    a. Allocate a new leaf page
        //    b. Insert key-value pair into leaf
        //    c. Update header page: set root_page_id, tree_height = 1, increment num_keys
        // 3. Otherwise, find the leaf page that should contain the key
        // 4. Try to insert the key-value pair into the leaf:
        //    a. If insertion successful and no split required, update header (increment num_keys) and return
        //    b. If leaf is full, call split_leaf_page and propagate split upward if necessary
        // 5. Handle any errors that occur during insertion
        unimplemented!()
    }

    /// Search for a key in the B+ tree
    pub fn search(&self, key: &K) -> Result<Option<V>, BPlusTreeError> {
        // 1. If tree is empty (header shows INVALID_PAGE_ID as root), return None
        // 2. Call find_leaf_page to locate the leaf page that would contain the key
        // 3. Find the index of the key in the leaf using find_key_index:
        //    a. If key exists at that index, get and return the corresponding value
        //    b. If key doesn't exist, return None
        // 4. Unpin the leaf page when done
        // 5. Handle any errors during search
        unimplemented!()
    }

    /// Remove a key-value pair from the B+ tree
    pub fn remove(&self, key: &K) -> Result<bool, BPlusTreeError> {
        // 1. If tree is empty, return false (nothing to remove)
        // 2. Find the leaf page that would contain the key
        // 3. Find the key's index in the leaf:
        //    a. If key not found, unpin page and return false
        //    b. If key found, remove the key-value pair using remove_key_value_at
        // 4. Update header page (decrement num_keys)
        // 5. Check if the leaf became underfull (less than min_size keys):
        //    a. If root leaf with no keys, update header to mark tree as empty
        //    b. If non-root underfull leaf, call check_and_handle_underflow
        // 6. Unpin all pages and return true (removal successful)
        unimplemented!()
    }

    /// Scan a range of keys in the B+ tree
    pub fn range_scan(&self, start: &K, end: &K) -> Result<Vec<(K, V)>, BPlusTreeError> {
        // 1. If start key > end key, return empty vector
        // 2. If tree is empty, return empty vector
        // 3. Find the leaf page containing the start key
        // 4. Create result vector for collected key-value pairs
        // 5. Scan the leaf for keys in range:
        //    a. For each key >= start and <= end, add (key, value) to result
        // 6. While leaf has next_page_id:
        //    a. Fetch the next leaf page
        //    b. For each key <= end, add (key, value) to result
        //    c. If all keys > end, break the loop
        //    d. Unpin the current leaf page
        // 7. Unpin the final leaf page
        // 8. Return the collected result
        unimplemented!()
    }

    /// Find the leaf page that should contain the key
    fn find_leaf_page(&self, key: &K) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // 1. Fetch the header page to get the root_page_id
        // 2. If root_page_id is invalid, return error (tree is empty)
        // 3. Fetch the root page
        // 4. While the page is an internal page:
        //    a. Cast the page to BPlusTreeInternalPage
        //    b. Use find_child_for_key to determine which child to follow
        //    c. Fetch the child page
        //    d. Unpin the parent page
        // 5. Cast the final page to BPlusTreeLeafPage
        // 6. Return the leaf page
        unimplemented!()
    }

    /// Split a leaf page when it becomes full
    fn split_leaf_page(
        &self,
        leaf_page: &mut PageGuard<BPlusTreeLeafPage<K, V, C>>,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new leaf page
        // 2. Determine split point (usually at half the max_size)
        // 3. Copy keys/values after split point to the new page:
        //    a. Get each key/value after split point from original page
        //    b. Insert each key/value into the new page
        //    c. Remove these keys/values from the original page
        // 4. Update the next_page_id pointers:
        //    a. New page's next_page_id = original page's next_page_id
        //    b. Original page's next_page_id = new page's page_id
        // 5. Get the first key in the new page as the separator key
        // 6. Insert separator key and new page's page_id into parent:
        //    a. If leaf_page is root, create a new root with separator key
        //    b. Otherwise, fetch parent and insert separator key
        // 7. Mark both pages as dirty and unpin the new page
        unimplemented!()
    }

    /// Split an internal page when it becomes full
    fn split_internal_page(
        &self,
        internal_page: &mut PageGuard<BPlusTreeInternalPage<K, C>>,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new internal page
        // 2. Determine split point (usually at half the max_size)
        // 3. Get the middle key at split point (this will go to parent)
        // 4. Copy keys/child pointers after split point to the new page:
        //    a. For each key after the middle key, insert into new page
        //    b. For each child pointer after middle key, insert into new page
        //    c. Remove these keys and pointers from the original page
        //    d. Handle the child pointer immediately after middle key correctly
        // 5. Remove the middle key from the original page (it goes to parent)
        // 6. Insert middle key and new page's page_id into parent:
        //    a. If internal_page is root, create a new root with middle key
        //    b. Otherwise, fetch parent and insert middle key
        // 7. Mark both pages as dirty and unpin the new page
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
        // 1. Check if merged page would exceed max_size, if so use redistribution instead
        // 2. Get the parent key that separates left and right pages
        // 3. Copy all keys/values from right page to left page:
        //    a. For each key/value in right page, insert at end of left page
        // 4. Update the next_page_id of left page to point to right page's next_page_id
        // 5. Remove the parent key at parent_key_index from parent_page
        // 6. Remove the child pointer to right_page from parent_page
        // 7. Check if parent became underfull or empty after removal:
        //    a. If parent is root and has no keys, adjust tree height
        //    b. If parent is underfull but not root, it will be handled in caller
        // 8. Mark left_page and parent_page as dirty, right_page will be deleted
        // 9. Delete the right page from buffer pool
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
        // 1. Check if merged page would exceed max_size, if so use redistribution instead
        // 2. Get the parent key at parent_key_index
        // 3. Add the parent key to the end of left_page's keys
        // 4. Copy all keys from right_page to left_page:
        //    a. For each key in right_page, add to left_page
        // 5. Copy all child pointers from right_page to left_page:
        //    a. For each child pointer in right_page, add to left_page
        // 6. Remove the parent key at parent_key_index from parent_page
        // 7. Remove the child pointer to right_page from parent_page
        // 8. Check if parent became underfull or empty after removal:
        //    a. If parent is root and has no keys, adjust tree height
        //    b. If parent is underfull but not root, it will be handled in caller
        // 9. Mark left_page and parent_page as dirty, right_page will be deleted
        // 10. Delete the right page from buffer pool
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
        // 1. Determine which page has more keys and which is underfull
        // 2. Determine how many keys need to be moved to balance the pages
        // 3. If left page has more keys:
        //    a. Move keys/values from left to right (starting from end of left)
        //    b. Insert each key/value at beginning of right page
        //    c. Remove these keys/values from left page
        // 4. If right page has more keys:
        //    a. Move keys/values from right to left (starting from beginning of right)
        //    b. Insert each key/value at end of left page
        //    c. Remove these keys/values from right page
        // 5. Update the parent key at parent_key_index to be the first key in right page
        // 6. Mark all pages as dirty
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
        // 1. Determine which page has more keys and which is underfull
        // 2. Determine how many keys need to be moved to balance the pages
        // 3. Get the parent key at parent_key_index that separates the nodes
        // 4. If redistributing from left to right:
        //    a. Move parent key down to front of right page
        //    b. Move last key from left page up to parent
        //    c. Move corresponding child pointers from left to right
        // 5. If redistributing from right to left:
        //    a. Move parent key down to end of left page
        //    b. Move first key from right page up to parent
        //    c. Move corresponding child pointers from right to left
        // 6. Update the parent key at parent_key_index
        // 7. Mark all pages as dirty
        unimplemented!()
    }

    /// Create a new root page
    fn create_new_root(
        &self,
        left_child_id: PageId,
        key: K,
        right_child_id: PageId,
    ) -> Result<(), BPlusTreeError> {
        // 1. Allocate a new internal page to be the root
        // 2. Initialize the new page as a root node
        // 3. Call populate_new_root to set up the key and child pointers:
        //    a. Insert left_child_id as the first child pointer
        //    b. Insert the key as the only key
        //    c. Insert right_child_id as the second child pointer
        // 4. Get the current header page
        // 5. Update header with new root page ID
        // 6. Increment tree height in header
        // 7. Mark both the root page and header page as dirty
        // 8. Unpin both pages
        unimplemented!()
    }

    /// Update header page after operations
    fn update_header(&self, root_id: PageId, height: u32, num_keys: usize) -> Result<(), BPlusTreeError> {
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
        // 1. Fetch the page
        // 2. Check if page is root:
        //    a. If root with no keys and has a child, collapse tree (decrease height)
        //    b. If root, but doesn't need collapsing, return false (no rebalancing needed)
        // 3. Check if page size < min_size:
        //    a. If size >= min_size, return false (no rebalancing needed)
        // 4. Fetch the parent page
        // 5. Find the index of this page in the parent
        // 6. Try to find a sibling page (prefer left)
        // 7. Fetch the sibling page
        // 8. If is_leaf:
        //    a. If left + right keys fit in one page, merge_leaf_pages
        //    b. Otherwise, redistribute_leaf_pages
        // 9. If not is_leaf:
        //    a. If left + right keys fit in one page, merge_internal_pages
        //    b. Otherwise, redistribute_internal_pages
        // 10. If parent became underfull, recursively call check_and_handle_underflow
        // 11. Return true (rebalancing occurred)
        unimplemented!()
    }

    /// Validate the B+ tree structure
    pub fn validate(&self) -> Result<(), BPlusTreeError> {
        // 1. If tree is empty, return Ok
        // 2. Fetch the root page
        // 3. If root is a leaf, verify it has no next_page_id
        // 4. If root is internal, validate each child recursively:
        //    a. For each child pointer, fetch the child page
        //    b. Verify keys in child are properly ordered relative to parent
        //    c. Verify child has proper min/max number of keys
        //    d. Recursively validate each child
        // 5. For leaf pages:
        //    a. Verify all leaf pages form a linked list
        //    b. Verify keys across adjacent leaves are properly ordered
        // 6. Verify all pages at same level have same distance to leaves
        // 7. Count total keys in tree and verify it matches header
        unimplemented!()
    }

    /// Print the B+ tree structure (for debugging)
    pub fn print_tree(&self) -> Result<(), BPlusTreeError> {
        // 1. If tree is empty, print "Empty tree" and return
        // 2. Print header information: height, size, root_id
        // 3. Create a queue starting with the root page
        // 4. While queue is not empty:
        //    a. Dequeue a page and its level
        //    b. Print indentation based on level
        //    c. If page is leaf, print its keys and values
        //    d. If page is internal, print its keys and child pointers
        //    e. For internal pages, enqueue all children with level+1
        // 5. Print a summary of the tree structure
        unimplemented!()
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
