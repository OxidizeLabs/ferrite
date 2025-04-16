use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::{
    config::{PageId, INVALID_PAGE_ID},
    rid::RID,
};
use crate::storage::index::index::IndexInfo;
use crate::storage::page::page::PageTrait;
use crate::storage::page::page::PageType;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::b_plus_tree_page::BPlusTreePage;
use crate::storage::page::page_types::{
    b_plus_tree_header_page::BPlusTreeHeaderPage, b_plus_tree_internal_page::BPlusTreeInternalPage,
    b_plus_tree_leaf_page::BPlusTreeLeafPage,
};
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use parking_lot::{RwLock, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use crate::common::config::DB_PAGE_SIZE;

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

// impl<F> KeyComparator<i32> for F where F: Fn(&i32, &i32) -> Ordering {}

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
            metadata, buffer_pool_manager,
            100,
            100
        );

        Self {
            type_id: TypeId::Integer,
            implementation: Box::new(index),
        }
    }

    pub fn insert(&self, key: &Value, value: &Value) -> Result<(), BPlusTreeError> {
        match self.type_id {
            TypeId::Integer => {
                let index = self
                    .implementation
                    .downcast_ref::<BPlusTreeIndex<i32, RID, I32Comparator>>()
                    .ok_or(BPlusTreeError::InvalidPageType)?;
                let key = i32::from_value(key)?;
                let value = RID::from_value(value)?;
                index.insert(key, value)
            }
            // Handle other types
            _ => Err(BPlusTreeError::InvalidPageType),
        }
    }

    // Similar implementations for search, remove, range_scan, etc.
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
    K: Clone + Sync + Send + Debug + 'static + Display + KeyType + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Sync + Send + 'static + ValueType + Serialize + for<'de> Deserialize<'de>,
    C: Fn(&K, &K) -> Ordering + Sync + Send + 'static + Clone + KeyComparator<K>,
{
    // Create a new leaf page with the appropriate comparator
    fn create_leaf_page(&self, buffer_pool: &BufferPoolManager) -> Option<PageId> {
        let page_id = buffer_pool.allocate_page();
        let mut page: BPlusTreeLeafPage<_, V, _> = BPlusTreeLeafPage::new_with_options(
            page_id,
            self.leaf_max_size,
            self.comparator.clone()
        );

        // Serialize page data to prepare for storage
        let mut data = [0u8; DB_PAGE_SIZE as usize];
        page.serialize(&mut data);

        // Write the page to the buffer pool
        buffer_pool.write_page(page_id, data);

        Some(page_id)
    }

    // Create a new internal page with the appropriate comparator
    fn create_internal_page(&self, buffer_pool: &BufferPoolManager) -> Option<PageId> {
        let page_id = buffer_pool.allocate_page();
        let mut page = BPlusTreeInternalPage::new_with_options(
            page_id,
            self.internal_max_size,
            self.comparator.clone()
        );

        // Serialize page data to prepare for storage
        let mut data = [0u8; DB_PAGE_SIZE as usize];
        page.serialize(&mut data);

        // Write the page to the buffer pool
        buffer_pool.write_page(page_id, data);

        Some(page_id)
    }
}

impl<K, V, C> BPlusTreeIndex<K, V, C>
where
    K: KeyType + Send + Sync + Debug + Display + Serialize + for<'de> Deserialize<'de> + 'static,
    V: ValueType + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static + PartialEq,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
{
    // Insert method implementation
    pub fn insert(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // Handle empty tree case by creating a new root
        if self.is_empty() {
            return self.initialize_tree_with_first_key_value(key, value);
        }

        // Start from the root and find the leaf node for this key
        let mut current_pid = self.get_root_page_id();
        let mut parent_stack = Vec::new();

        // Traverse the tree to find the leaf node
        loop {
            // First, fetch the page as a BPlusTreePage to check its type
            let page_guard = match self
                .buffer_pool_manager
                .fetch_page::<BPlusTreePage<K, V, C>>(current_pid)
            {
                Some(page) => page,
                None => return Err(BPlusTreeError::PageNotFound(current_pid)),
            };

            let page_type = page_guard.get_page_type();

            // Release the generic page guard before fetching with specific type
            drop(page_guard);

            // Now fetch with the appropriate concrete type based on the page type
            if page_type == PageType::BTreeLeaf {
                // Fetch as leaf page
                let leaf_page_guard = match self
                    .buffer_pool_manager
                    .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_pid)
                {
                    Some(page) => page,
                    None => return Err(BPlusTreeError::PageNotFound(current_pid)),
                };

                let leaf_page = leaf_page_guard.get_page().clone();

                return self.insert_into_leaf(leaf_page, key, value, parent_stack);
            } else if page_type == PageType::BTreeInternal {
                // Fetch as internal page
                let internal_page_guard = match self
                    .buffer_pool_manager
                    .fetch_page::<BPlusTreeInternalPage<K, C>>(current_pid)
                {
                    Some(page) => page,
                    None => return Err(BPlusTreeError::PageNotFound(current_pid)),
                };

                // Remember this page for potential split propagation
                parent_stack.push(current_pid);

                // Find the next child to follow based on the key
                let internal_page = internal_page_guard.get_page();
                let internal_guard = internal_page.read();

                let next_pid = match internal_guard.find_child_for_key(&key) {
                    Some(pid) => pid,
                    None => return Err(BPlusTreeError::KeyNotFound),
                };

                // Release the guard before continuing
                drop(internal_guard);

                current_pid = next_pid;
            } else {
                // Unexpected page type
                return Err(BPlusTreeError::InvalidPageType);
            }
        }
    }

    // Remove a key from the B+Tree
    pub fn remove(&self, key: &K) -> Result<bool, BPlusTreeError> {
        // Check if tree is empty
        if self.is_empty() {
            return Ok(false); // Nothing to remove, return false indicating no removal
        }

        // Get root page ID
        let root_page_id = self.get_root_page_id();

        // Start from root and find the leaf page containing the key
        let (leaf_page_id, parent_stack) = self.find_leaf_page(root_page_id, key)?;

        // Check if leaf page exists
        if leaf_page_id == INVALID_PAGE_ID {
            return Ok(false);
        }

        // Fetch the leaf page
        if let Some(leaf_page_guard) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(leaf_page_id)
        {
            let leaf_page = leaf_page_guard.get_page().clone();

            // Attempt to remove the key from the leaf
            let removed = self.remove_from_leaf(leaf_page, key, parent_stack)?;

            // Return whether removal was successful
            return Ok(removed);
        }

        // Failed to fetch leaf page
        Err(BPlusTreeError::PageNotFound(leaf_page_id))
    }

    // Search for a key in the B+Tree
    pub fn search(&self, key: &K) -> Result<Option<V>, BPlusTreeError> {
        let root_page_id = self.get_root_page_id();

        // If tree is empty
        if root_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }

        // Start from root and find the leaf page containing the key
        let (leaf_page_id, _parent_stack) = self.find_leaf_page(root_page_id, key)?;

        // Check if leaf page exists
        if leaf_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }

        // Fetch the leaf page and search for the key
        let leaf_page_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeLeafPage<K, V, C>>(leaf_page_id)
            .unwrap();
        let leaf_page = leaf_page_guard.get_page().clone();

        // Search for the key in the leaf page
        let leaf_guard = leaf_page.read();

        // Find the index of the key
        let index = leaf_guard.find_key_index(key);

        // Check if the key exists at this index
        if index < leaf_guard.get_size() {
            // Need to verify it's the exact key we're looking for using the comparator
            if let Some(found_key) = leaf_guard.get_key_at(index) {
                if (self.comparator)(found_key, key) == std::cmp::Ordering::Equal {
                    // Found the key, return its value
                    return Ok(leaf_guard.get_value_at(index).cloned());
                }
            }
        }

        // Key not found
        Ok(None)
    }

    // Range scan implementation
    pub fn range_scan(&self, start_key: &K, end_key: &K) -> Result<Vec<(K, V)>, BPlusTreeError> {
        let mut result = Vec::new();

        // Check if range is valid (start_key <= end_key)
        if (self.comparator)(start_key, end_key) == Ordering::Greater {
            return Ok(result); // Empty result for invalid range
        }

        // Get root page ID from the header page
        let root_page_id = self.get_root_page_id();

        // Find the leaf page containing the start_key
        // Discard the parent_stack since we don't need it for a read-only range scan
        let (leaf_page_id, _parent_stack) = self.find_leaf_page(root_page_id, start_key)?;

        // Start scanning from this leaf page
        let mut current_leaf_id = leaf_page_id;

        // Continue scanning through leaf pages until we go past the end_key
        while current_leaf_id != INVALID_PAGE_ID {
            let leaf_page_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(leaf_page_id)
                .unwrap();
            let leaf_page = leaf_page_guard.get_page().read();

            // Scan this leaf page and collect keys in range
            for i in 0..leaf_page.get_size() {
                let key = leaf_page.get_key_at(i).unwrap();

                // If we've gone past the end_key, we're done
                if (self.comparator)(&key, end_key) == Ordering::Greater {
                    return Ok(result);
                }

                // If the key is >= start_key, include it in results
                if (self.comparator)(&key, start_key) != Ordering::Less {
                    let value = leaf_page.get_value_at(i).unwrap();
                    result.push((key.clone(), value.clone()));
                }
            }

            // Move to the next leaf page
            current_leaf_id = leaf_page.get_next_page_id().unwrap();
        }

        Ok(result)
    }

    // Handle the case when the tree is empty
    fn initialize_tree_with_first_key_value(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // Create a new leaf page as the root
        let leaf_page = match self
            .buffer_pool_manager
            .new_page::<BPlusTreeLeafPage<K, V, C>>()
        {
            Some(page) => page,
            None => return Err(BPlusTreeError::PageAllocationFailed),
        };

        // Insert the key-value pair
        {
            let mut leaf_guard = leaf_page.write();
            leaf_guard.insert_key_value(key, value);
            leaf_guard.set_dirty(true);
        }

        // Update root page ID in the tree metadata
        let page_id = leaf_page.get_page_id();
        self.update_root_page_id(page_id);

        // Update tree metadata (height = 1, num_keys = 1)
        self.update_tree_metadata(Some(1), Some(1));

        Ok(())
    }

    fn update_root_page_id(&self, new_root_page_id: PageId) {
        // Use if let Some(...) instead of if let Ok(...)
        if let Some(header_page_guard) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            let mut header_page = header_page_guard.write();
            header_page.set_root_page_id(new_root_page_id);
        }
    }

    fn update_tree_metadata(&self, height: Option<usize>, size: Option<usize>) {
        // Use if let Some(...) instead of if let Ok(...)
        if let Some(header_page_guard) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            let mut header_page = header_page_guard.write();

            // Update height if provided
            if let Some(h) = height {
                header_page.set_tree_height(h.try_into().unwrap());
            }

            // Update size if provided
            if let Some(s) = size {
                header_page.set_tree_height(s.try_into().unwrap());
            }
        }
    }

    // Insert a key-value pair into a leaf node
    fn insert_into_leaf(
        &self,
        leaf_page: Arc<RwLock<BPlusTreeLeafPage<K, V, C>>>,
        key: K,
        value: V,
        parent_stack: Vec<PageId>,
    ) -> Result<(), BPlusTreeError> {
        let mut leaf_guard = leaf_page.write();

        // Check if the key already exists
        let index = leaf_guard.find_key_index(&key);
        if index < leaf_guard.get_size() {
            if (self.comparator)(&key, leaf_guard.get_key_at(index).unwrap()) == Ordering::Equal {
                // Update existing key's value
                leaf_guard.set_value_at(index, value);
                leaf_guard.set_dirty(true);
                return Ok(());
            }
        }

        // Insert new key-value
        if leaf_guard.get_size() < leaf_guard.get_max_size() {
            // Leaf has space, simply insert
            leaf_guard.insert_key_value(key, value);
            leaf_guard.set_dirty(true);
            self.update_tree_metadata(None, Some(1)); // Increment key count
            return Ok(());
        }

        // Leaf is full, need to split
        let (new_leaf_page, promoted_key) = self.split_leaf_page(&mut leaf_guard, key, value)?;

        if parent_stack.is_empty() {
            // Current leaf is the root, create a new root
            self.create_new_root(
                leaf_page.read().get_page_id(),
                new_leaf_page.get_page_id(),
                promoted_key,
            )?;
        } else {
            // Propagate the split upward
            self.insert_into_parent(
                parent_stack,
                leaf_page.read().get_page_id(),
                new_leaf_page.get_page_id(),
                promoted_key,
            )?;
        }

        Ok(())
    }

    // Split a leaf page that is full
    fn split_leaf_page(
        &self,
        leaf_guard: &mut RwLockWriteGuard<BPlusTreeLeafPage<K, V, C>>,
        key: K,
        value: V,
    ) -> Result<(PageGuard<BPlusTreeLeafPage<K, V, C>>, K), BPlusTreeError> {
        // Create a new leaf page
        let new_leaf_page = match self
            .buffer_pool_manager
            .new_page::<BPlusTreeLeafPage<K, V, C>>()
        {
            Some(page) => page,
            None => return Err(BPlusTreeError::PageAllocationFailed),
        };

        // Calculate the middle point for splitting
        let mid_index = (leaf_guard.get_size() + 1) / 2;

        // Move half of the keys to the new page
        let mut new_leaf_guard = new_leaf_page.write();

        // Handle the case where the new key should go to the new page
        let inserted_at_new =
            match (self.comparator)(&key, leaf_guard.get_key_at(mid_index - 1).unwrap()) {
                Ordering::Greater => true,
                _ => false,
            };

        // Move keys from mid to end to the new page
        for i in mid_index..leaf_guard.get_size() {
            let moved_key = leaf_guard.get_key_at(i).unwrap().clone();
            let moved_value = leaf_guard.get_value_at(i).unwrap().clone();
            new_leaf_guard.insert_key_value(moved_key, moved_value);
        }

        // Update the next page pointers
        new_leaf_guard.set_next_page_id(leaf_guard.get_next_page_id());
        leaf_guard.set_next_page_id(Some(new_leaf_page.get_page_id()));

        // Remove the moved keys from the original page
        for _ in mid_index..leaf_guard.get_size() {
            leaf_guard.remove_key_value_at(mid_index);
        }

        // Insert the new key-value pair into the appropriate page
        if inserted_at_new {
            new_leaf_guard.insert_key_value(key, value);
        } else {
            leaf_guard.insert_key_value(key, value);
        }

        // Mark both pages as dirty
        leaf_guard.set_dirty(true);
        new_leaf_guard.set_dirty(true);

        // Get the key to be promoted to the parent
        let promoted_key = new_leaf_guard.get_key_at(0).unwrap().clone();

        drop(new_leaf_guard); // Release the lock on the new page

        Ok((new_leaf_page, promoted_key))
    }

    // Insert a key and child pointer into an internal node
    fn insert_into_parent(
        &self,
        mut parent_stack: Vec<PageId>,
        left_child_pid: PageId,
        right_child_pid: PageId,
        key: K,
    ) -> Result<(), BPlusTreeError> {
        let parent_pid = parent_stack.pop().unwrap();
        let parent_page = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(parent_pid)
        {
            Some(page) => page,
            None => return Err(BPlusTreeError::PageNotFound(parent_pid)),
        };

        let mut parent_guard = parent_page.write();

        // Check if parent has space for the new entry
        if parent_guard.get_size() < parent_guard.get_max_size() {
            // Insert the key and right child pointer
            parent_guard.insert_key_value(key, right_child_pid);
            parent_guard.set_dirty(true);
            return Ok(());
        }

        // Parent is full, need to split
        let (new_parent_page, promoted_key) =
            self.split_internal_page(&mut parent_guard, key, right_child_pid)?;

        if parent_stack.is_empty() {
            // Current parent is the root, create a new root
            self.create_new_root(
                parent_page.get_page_id(),
                new_parent_page.get_page_id(),
                promoted_key,
            )?;
        } else {
            // Continue propagating the split upward
            self.insert_into_parent(
                parent_stack,
                parent_page.get_page_id(),
                new_parent_page.get_page_id(),
                promoted_key,
            )?;
        }

        Ok(())
    }

    // Split an internal page that is full
    fn split_internal_page(
        &self,
        parent_guard: &mut RwLockWriteGuard<BPlusTreeInternalPage<K, C>>,
        key: K,
        right_child_pid: PageId,
    ) -> Result<(PageGuard<BPlusTreeInternalPage<K, C>>, K), BPlusTreeError> {
        // Create a new internal page
        let new_internal_page = match self
            .buffer_pool_manager
            .new_page::<BPlusTreeInternalPage<K, C>>()
        {
            Some(page) => page,
            None => return Err(BPlusTreeError::PageAllocationFailed),
        };

        // Calculate the middle point for splitting (for internal nodes, we exclude middle key)
        let mid_index = parent_guard.get_size() / 2;

        // Get the middle key that will be promoted
        let promoted_key = parent_guard.get_key_at(mid_index).unwrap().clone();

        // Create a temporary buffer for the key we're inserting
        let mut temp_keys = Vec::with_capacity(parent_guard.get_size() + 1);
        let mut temp_page_ids = Vec::with_capacity(parent_guard.get_size() + 2);

        // Fill the temporary buffers with existing keys and page IDs
        for i in 0..parent_guard.get_size() {
            temp_keys.push(parent_guard.get_key_at(i).unwrap().clone());
            temp_page_ids.push(parent_guard.get_value_at(i).unwrap());
        }
        temp_page_ids.push(parent_guard.get_value_at(parent_guard.get_size()).unwrap());

        // Insert the new key and page ID at the correct position
        let mut insert_pos = 0;
        while insert_pos < temp_keys.len()
            && (self.comparator)(&key, &temp_keys[insert_pos]) == Ordering::Greater
        {
            insert_pos += 1;
        }

        temp_keys.insert(insert_pos, key);
        temp_page_ids.insert(insert_pos + 1, right_child_pid);

        // Clear the original page
        for i in (0..parent_guard.get_size()).rev() {
            parent_guard.remove_key_value_at(i);
        }

        // Move keys and page IDs to the appropriate pages
        let mut new_internal_guard = new_internal_page.write();

        // First half goes to the original page
        for i in 0..mid_index {
            parent_guard.insert_key_value(temp_keys[i].clone(), temp_page_ids[i + 1]);
        }

        // Set the leftmost child of the original page
        parent_guard.insert_key_value(temp_keys[0].clone(), temp_page_ids[0]);
        parent_guard.remove_key_value_at(0);

        // Second half goes to the new page
        for i in (mid_index + 1)..temp_keys.len() {
            new_internal_guard.insert_key_value(temp_keys[i].clone(), temp_page_ids[i + 1]);
        }

        // Set the leftmost child of the new page
        new_internal_guard.insert_key_value(temp_keys[0].clone(), temp_page_ids[mid_index + 1]);
        new_internal_guard.remove_key_value_at(0);

        // Mark both pages as dirty
        parent_guard.set_dirty(true);
        new_internal_guard.set_dirty(true);

        drop(new_internal_guard); // Release the lock on the new page

        Ok((new_internal_page, promoted_key))
    }

    // Create a new root when the tree needs to grow in height
    fn create_new_root(
        &self,
        left_child_pid: PageId,
        right_child_pid: PageId,
        key: K,
    ) -> Result<(), BPlusTreeError> {
        // Create a new internal page as the root
        let root_page = match self
            .buffer_pool_manager
            .new_page::<BPlusTreeInternalPage<K, C>>()
        {
            Some(page) => page,
            None => return Err(BPlusTreeError::PageAllocationFailed),
        };

        // Add the children to the new root
        let mut root_guard = root_page.write();

        // Insert a dummy key for the leftmost child (will be removed)
        root_guard.insert_key_value(key.clone(), left_child_pid);
        // Insert the actual key and right child
        root_guard.insert_key_value(key, right_child_pid);
        // Remove the dummy key entry but keep its page ID
        root_guard.remove_key_value_at(0);

        root_guard.set_dirty(true);

        // Update root page ID in the tree metadata
        let new_root_id = root_page.get_page_id();
        self.update_root_page_id(new_root_id);

        // Update tree height in metadata
        self.update_tree_metadata(Some((self.get_height() + 1).try_into().unwrap()), None);

        Ok(())
    }

    // Remove a key from a leaf node
    fn remove_from_leaf(
        &self,
        leaf_page: Arc<RwLock<BPlusTreeLeafPage<K, V, C>>>,
        key: &K,
        parent_stack: Vec<PageId>,
    ) -> Result<bool, BPlusTreeError> {
        let mut leaf_guard = leaf_page.write();

        // Find the position for this key
        let index = leaf_guard.find_key_index(key);

        // Check if the key exists
        if index >= leaf_guard.get_size()
            || (self.comparator)(key, leaf_guard.get_key_at(index).unwrap()) != Ordering::Equal
        {
            return Ok(false); // Key not found
        }

        // Store the size before removal for empty check
        let size_before_removal = leaf_guard.get_size();

        // Remove the key-value pair
        leaf_guard.remove_key_value_at(index);
        leaf_guard.set_dirty(true);

        self.update_tree_metadata(None, Some(usize::MAX)); // Decrement key count

        // Check if we need to rebalance the tree
        if leaf_guard.get_size() < leaf_guard.get_min_size() && !parent_stack.is_empty() {
            // Need to handle underflow - complex operation requiring sibling access
            // and potential redistribution or merging
            let page_id = leaf_page.read().get_page_id();
            drop(leaf_guard); // Release lock before handling underflow
            self.handle_underflow(page_id, parent_stack.clone(), true)?;
        }

        // Check if the root is now empty
        if parent_stack.is_empty() && size_before_removal == 1 {
            // Tree is now empty
            self.update_tree_metadata(Some(0), None); // Set height to 0
        }

        Ok(true)
    }

    // Handle underflow after deletion
    fn handle_underflow(
        &self,
        page_id: PageId,
        mut parent_stack: Vec<PageId>,
        is_leaf: bool,
    ) -> Result<(), BPlusTreeError> {
        // Complex implementation that handles:
        // 1. Redistribution from siblings if possible
        // 2. Merging with siblings if redistribution isn't possible
        // 3. Propagating underflow up the tree if needed
        // 4. Adjusting the root if necessary

        // This is a simplified placeholder - a full implementation would be more complex

        let parent_pid = parent_stack.pop().unwrap();
        let parent_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(parent_pid)
            .ok_or(BPlusTreeError::PageNotFound(parent_pid))?;

        let parent_guard = parent_page.read();

        // Find the index of the current page in the parent
        let mut index = 0;
        while index <= parent_guard.get_size() {
            if parent_guard.get_value_at(index).unwrap() == page_id {
                break;
            }
            index += 1;
        }

        // Find a sibling to borrow from or merge with
        let sibling_idx = if index == 0 { 1 } else { index - 1 };
        let sibling_pid = parent_guard.get_value_at(sibling_idx).unwrap();

        // Implementation would continue with either redistribution or merging
        // based on the sizes of the current page and its sibling

        Ok(())
    }

    fn find_leaf_page(
        &self,
        page_id: PageId,
        key: &K,
    ) -> Result<(PageId, Vec<PageId>), BPlusTreeError> {
        let mut current_page_id = page_id;
        let mut parent_stack = Vec::new();

        loop {
            let page = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreePage<K, V, C>>(current_page_id)
                .unwrap();
            let page_guard = page.read();

            match page_guard.get_page_type() {
                PageType::BTreeLeaf => {
                    // We found a leaf page - return its ID and the parent stack
                    return Ok((current_page_id, parent_stack));
                }
                PageType::BTreeInternal => {
                    // Add current page to parent stack before descending
                    parent_stack.push(current_page_id);

                    // Release the current page before fetching it with the correct type
                    drop(page_guard);

                    // Fetch the page again, but this time as an internal page
                    let internal_page_guard = match self
                        .buffer_pool_manager
                        .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id)
                    {
                        Some(page) => page,
                        None => return Err(BPlusTreeError::PageNotFound(current_page_id)),
                    };

                    let internal_guard = internal_page_guard.get_page().read();

                    // Find the next child based on the key
                    let next_page_id = match internal_guard.find_child_for_key(key) {
                        Some(pid) => pid,
                        None => return Err(BPlusTreeError::KeyNotFound),
                    };

                    // Release the guard before continuing
                    drop(internal_guard);

                    // Update current_page_id and continue looping
                    current_page_id = next_page_id;
                }
                _ => return Err(BPlusTreeError::InvalidPageType),
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        // Implementation depends on your data structure's specifics
        // For example, you might check if the root page is invalid or
        // if the tree height is zero
        // Example:
        self.get_root_page_id() == INVALID_PAGE_ID
    }

    fn get_root_page_id(&self) -> PageId {
        // Fetch page with correct type
        let header_page = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .expect("Failed to fetch header page");

        // Now you can directly use the correctly typed page
        let header_page_data = header_page.read();
        header_page_data.get_root_page_id()
    }

    // Get the current height of the tree
    fn get_height(&self) -> u32 {
        // Fetch the header page
        if let Some(header_page_guard) = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
        {
            let header_page_read = header_page_guard.read();
            return header_page_read.get_tree_height();
        }

        // If header page can't be fetched, return 0
        0
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::schema::Schema;
    use crate::common::config::INVALID_PAGE_ID;
    use crate::common::rid::RID;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::index::index::{IndexInfo, IndexType};
    use std::sync::Arc;
    use tempfile::tempdir;

    // Helper to initialize buffer pool for tests
    fn create_buffer_pool() -> Arc<BufferPoolManager> {
        let temp_dir = tempdir().unwrap();
        let db_file_path = temp_dir.path().join("test_bplus_tree.db");
        let log_file_path = temp_dir.path().join("test_bplus_tree.log");

        // Create disk manager
        let disk_manager = Arc::new(FileDiskManager::new(
            db_file_path.to_str().unwrap().to_string(),
            log_file_path.to_str().unwrap().to_string(),
            10, // buffer size
        ));

        // Create disk scheduler
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

        // Create LRU-K replacer
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));

        // Create buffer pool manager
        Arc::new(BufferPoolManager::new(
            10, // pool size
            disk_scheduler,
            disk_manager,
            replacer,
        ))
    }

    // Helper to set up B+ tree index for tests with i32 keys and RID values
    // Helper to set up B+ tree index for tests with i32 keys and RID values
    fn create_i32_index() -> BPlusTreeIndex<i32, RID, I32Comparator> {
        let buffer_pool_manager = create_buffer_pool();

        // Create header page
        let header_page = buffer_pool_manager
            .new_page::<BPlusTreeHeaderPage>()
            .unwrap();
        let header_page_id = header_page.get_page_id();
        {
            let mut header_guard = header_page.write();
            header_guard.set_root_page_id(INVALID_PAGE_ID);
            header_guard.set_tree_height(0);
            header_guard.set_dirty(true);
        }

        // Create index metadata
        let metadata = Box::new(IndexInfo::new(
            Schema::new(vec![]),       // key_schema
            "test_index".to_string(),  // index_name
            0,                         // index_oid
            "test_table".to_string(),  // table_name
            4,                         // key_size
            false,                     // is_primary_key
            IndexType::BPlusTreeIndex, // index_type
            vec![0],                   // key_attrs (column indices)
        ));

        BPlusTreeIndex {
            comparator: i32_comparator, // Remove the {} here
            metadata,
            buffer_pool_manager,
            header_page_id,
            _marker: PhantomData,
            leaf_max_size: 100,
            internal_max_size: 100,
        }
    }

    // Test that a newly created index is empty
    #[test]
    fn test_index_is_empty() {
        let index = create_i32_index();
        assert!(index.is_empty());
    }

    // Test inserting a single key-value pair
    #[test]
    fn test_insert_single_key() {
        let index = create_i32_index();

        // Insert a key-value pair
        let rid = RID::new(1, 1);
        assert!(index.insert(42, rid).is_ok());

        // Verify the index is no longer empty
        assert!(!index.is_empty());

        // Verify we can find the inserted key
        match index.search(&42) {
            Ok(Some(found_rid)) => {
                assert_eq!(found_rid, rid);
            }
            _ => panic!("Failed to find inserted key"),
        }
    }

    // Test inserting multiple key-value pairs
    #[test]
    fn test_insert_multiple_keys() {
        let index = create_i32_index();

        // Insert multiple key-value pairs
        for i in 0..100 {
            let rid = RID::new(i as PageId, i as u32);
            assert!(index.insert(i, rid).is_ok());
        }

        // Verify we can find all inserted keys
        for i in 0..100 {
            match index.search(&i) {
                Ok(Some(found_rid)) => {
                    assert_eq!(found_rid, RID::new(i as PageId, i as u32));
                }
                _ => panic!("Failed to find inserted key {}", i),
            }
        }

        // Verify a non-existent key isn't found
        match index.search(&1000) {
            Ok(None) => (), // Expected
            _ => panic!("Should not find key 1000"),
        }
    }

    // Test updating an existing key
    #[test]
    fn test_update_existing_key() {
        let index = create_i32_index();

        // Insert a key-value pair
        let rid1 = RID::new(1, 1);
        assert!(index.insert(42, rid1).is_ok());

        // Update with a new value
        let rid2 = RID::new(2, 2);
        assert!(index.insert(42, rid2).is_ok());

        // Verify the updated value
        match index.search(&42) {
            Ok(Some(found_rid)) => {
                assert_eq!(found_rid, rid2);
                assert_ne!(found_rid, rid1);
            }
            _ => panic!("Failed to find updated key"),
        }
    }

    // Test removing a key
    #[test]
    fn test_remove_key() {
        let index = create_i32_index();

        // Insert a key-value pair
        let rid = RID::new(1, 1);
        assert!(index.insert(42, rid).is_ok());

        // Verify it exists
        assert!(index.search(&42).unwrap().is_some());

        // Remove it
        assert!(index.remove(&42).unwrap());

        // Verify it no longer exists
        assert!(index.search(&42).unwrap().is_none());
    }

    // Test removing a non-existent key
    #[test]
    fn test_remove_nonexistent_key() {
        let index = create_i32_index();

        // Try to remove a key that doesn't exist
        assert!(!index.remove(&42).unwrap());
    }

    // Test that inserting enough keys to cause a split works correctly
    #[test]
    fn test_leaf_node_split() {
        let index = create_i32_index();

        // Insert enough keys to cause at least one split
        // The exact number depends on the B+ tree implementation
        for i in 0..200 {
            let rid = RID::new(i as PageId, i as u32);
            assert!(index.insert(i, rid).is_ok());
        }

        // Verify all keys can still be found
        for i in 0..200 {
            match index.search(&i) {
                Ok(Some(found_rid)) => {
                    assert_eq!(found_rid, RID::new(i as PageId, i as u32));
                }
                _ => panic!("Failed to find inserted key {}", i),
            }
        }

        // Verify the tree height is greater than 1 after splits
        assert!(index.get_height() > 1);
    }

    // Test range scan functionality
    #[test]
    fn test_range_scan() {
        let index = create_i32_index();

        // Insert sequential keys
        for i in 0..100 {
            let rid = RID::new(i as PageId, i as u32);
            assert!(index.insert(i, rid).is_ok());
        }

        // Test a range scan
        let scan_result = index.range_scan(&20, &29).unwrap();

        // Verify we got the expected number of results
        assert_eq!(scan_result.len(), 10);

        // Verify the results are in ascending order
        for i in 0..scan_result.len() {
            assert_eq!(scan_result[i].0, i as i32 + 20);
            assert_eq!(
                scan_result[i].1,
                RID::new((i + 20) as PageId, (i + 20) as u32)
            );
        }
    }

    // Test empty range scan
    #[test]
    fn test_empty_range_scan() {
        let index = create_i32_index();

        // Range scan with start > end should return empty results
        let scan_result = index.range_scan(&30, &20).unwrap();
        assert_eq!(scan_result.len(), 0);

        // Insert some keys
        for i in 0..100 {
            let rid = RID::new(i as PageId, i as u32);
            assert!(index.insert(i, rid).is_ok());
        }

        // Range scan for non-existent range
        let scan_result = index.range_scan(&1000, &1010).unwrap();
        assert_eq!(scan_result.len(), 0);
    }

    // Test GenericKey with a simple key type
    // #[test]
    // fn test_generic_key() {
    //     // Create a buffer pool manager
    //     let buffer_pool_manager = create_buffer_pool();
    //
    //     // Create header page
    //     let header_page = buffer_pool_manager.new_page::<BPlusTreeHeaderPage>().unwrap();
    //     let header_page_id = header_page.get_page_id();
    //     {
    //         let mut header_guard = header_page.write();
    //         header_guard.set_root_page_id(INVALID_PAGE_ID);
    //         header_guard.set_tree_height(0);
    //         header_guard.set_dirty(true);
    //     }
    //
    //     // Create index metadata
    //     let metadata = Box::new(IndexInfo::new(
    //         Schema::new(vec![]),               // key_schema
    //         "test_generic_index".to_string(), // index_name
    //         0,                                // index_oid
    //         "test_table".to_string(),         // table_name
    //         4,                                // key_size
    //         false,                            // is_primary_key
    //         IndexType::BPlusTreeIndex,        // index_type
    //         vec![0],                          // key_attrs (column indices)
    //     ));
    //
    //     // Create a GenericKeyComparator
    //     let comparator = GenericKeyComparatorWrapper::<i32, 4>::new();
    //
    //     // Create a B+ tree index with GenericKey
    //     let index = BPlusTreeIndex::<GenericKey<i32, 4>, RID, GenericKeyComparatorWrapper<i32, 4>> {
    //         comparator,
    //         metadata,
    //         buffer_pool_manager,
    //         header_page_id,
    //         _marker: PhantomData,
    //     };
    //
    //     // Insert a key-value pair
    //     let mut key = GenericKey::<i32, 4>::new();
    //     key.set_from_bytes(&42i32.to_le_bytes());
    //     let rid = RID::new(1, 1);
    //
    //     assert!(index.insert(key.clone(), rid).is_ok());
    //
    //     // Search for the key
    //     match index.search(&key) {
    //         Ok(Some(found_rid)) => {
    //             assert_eq!(found_rid, rid);
    //         }
    //         _ => panic!("Failed to find inserted generic key"),
    //     }
    // }

    // Test concurrent operations on the B+ tree
    #[test]
    fn test_concurrent_operations() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let index = Arc::new(create_i32_index());
        let num_threads = 10;
        let keys_per_thread = 100;

        // Barrier to ensure all threads start at the same time
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let thread_index = Arc::clone(&index);
            let thread_barrier = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                thread_barrier.wait();

                // Each thread inserts its own range of keys
                let start_key = thread_id * keys_per_thread;
                for i in 0..keys_per_thread {
                    let key = start_key + i;
                    let rid = RID::new(key as PageId, thread_id as u32);
                    assert!(thread_index.insert(key as i32, rid).is_ok());
                }

                // Each thread verifies its own inserts
                for i in 0..keys_per_thread as i32 {
                    let key = start_key as i32 + i;
                    match thread_index.search(&key) {
                        Ok(Some(found_rid)) => {
                            assert_eq!(found_rid.get_page_id(), key as PageId);
                            assert_eq!(found_rid.get_slot_num(), thread_id as u32);
                        }
                        _ => panic!("Thread {} failed to find key {}", thread_id, key),
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all keys from all threads can be found
        for thread_id in 0..num_threads {
            let start_key = thread_id * keys_per_thread;
            for i in 0..keys_per_thread {
                let key = start_key as i32 + i as i32;
                match index.search(&key) {
                    Ok(Some(found_rid)) => {
                        assert_eq!(found_rid.get_page_id() as i32, key);
                        assert_eq!(found_rid.get_slot_num(), thread_id as u32);
                    }
                    _ => panic!("Failed to find key {} after concurrent inserts", key),
                }
            }
        }
    }

    // Test recovery of B+ tree state after reloading from disk
    #[test]
    fn test_persistence_and_recovery() {
        let temp_dir = tempdir().unwrap();
        let db_file_path = temp_dir.path().join("persistence_test.db");
        let log_file_path = temp_dir.path().join("persistence_test.log");

        // Scope for first buffer pool manager
        {
            // Create disk manager
            let disk_manager = Arc::new(FileDiskManager::new(
                db_file_path.to_str().unwrap().to_string(),
                log_file_path.to_str().unwrap().to_string(),
                10, // buffer size
            ));

            // Create disk scheduler and replacer
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));

            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                10, // pool size
                disk_scheduler,
                disk_manager,
                replacer,
            ));

            // Create header page
            let header_page = buffer_pool_manager
                .new_page::<BPlusTreeHeaderPage>()
                .unwrap();
            let header_page_id = header_page.get_page_id();
            {
                let mut header_guard = header_page.write();
                header_guard.set_root_page_id(INVALID_PAGE_ID);
                header_guard.set_tree_height(0);
                header_guard.set_dirty(true);
            }

            // Create index metadata
            let metadata = Box::new(IndexInfo::new(
                Schema::new(vec![]),            // key_schema
                "persistence_test".to_string(), // index_name
                0,                              // index_oid
                "test_table".to_string(),       // table_name
                4,                              // key_size
                false,                          // is_primary_key
                IndexType::BPlusTreeIndex,      // index_type
                vec![0],                        // key_attrs (column indices)
            ));

            // Create index and insert data
            let index = BPlusTreeIndex {
                comparator: i32_comparator as for<'a, 'b> fn(&'a i32, &'b i32) -> Ordering,

                metadata,
                buffer_pool_manager,
                header_page_id,
                _marker: PhantomData,
                leaf_max_size: 100,
                internal_max_size: 100,
            };

            // Insert some keys
            for i in 0..100 {
                let rid = RID::new(i as PageId, i as u32);
                assert!(index.insert(i, rid).is_ok());
            }

            // Flush all pages to disk by dropping the buffer pool manager
        }

        // Create new buffer pool manager using same file
        let disk_manager = Arc::new(FileDiskManager::new(
            db_file_path.to_str().unwrap().to_string(),
            log_file_path.to_str().unwrap().to_string(),
            10, // buffer size
        ));

        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));

        let buffer_pool_manager = Arc::new(BufferPoolManager::new(
            10, // pool size
            disk_scheduler,
            disk_manager,
            replacer,
        ));

        // Recreate index with same header page ID (should be 0 for first created page)
        let header_page_id = 0; // First page created in previous scope

        // Create index metadata
        let metadata = Box::new(IndexInfo::new(
            Schema::new(vec![]),            // key_schema
            "persistence_test".to_string(), // index_name
            0,                              // index_oid
            "test_table".to_string(),       // table_name
            4,                              // key_size
            false,                          // is_primary_key
            IndexType::BPlusTreeIndex,      // index_type
            vec![0],                        // key_attrs (column indices)
        ));

        // Recreate index
        let index = BPlusTreeIndex::<i32, RID, I32Comparator> {
            comparator: i32_comparator,
            metadata,
            buffer_pool_manager,
            header_page_id,
            _marker: PhantomData,
            leaf_max_size: 100,
            internal_max_size: 100,
        };

        // Verify data was persisted by checking some keys
        for i in 0..100 {
            match index.search(&i) {
                Ok(Some(found_rid)) => {
                    assert_eq!(found_rid, RID::new(i as PageId, i as u32));
                }
                _ => panic!("Failed to find key {} after recovery", i),
            }
        }
    }
}
