use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::{
    config::{PageId, INVALID_PAGE_ID},
    rid::RID,
};
use crate::storage::index::index::IndexInfo;
use crate::storage::index::types::comparators::{i32_comparator, I32Comparator};
use crate::storage::index::types::{KeyComparator, KeyType, ValueType};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page::{PageTrait, PageType};
use crate::storage::page::page_types::{
    b_plus_tree_internal_page::BPlusTreeInternalPage, b_plus_tree_leaf_page::BPlusTreeLeafPage,
    b_plus_tree_header_page::BPlusTreeHeaderPage,
};
use crate::types_db::type_id::TypeId;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;
use std::collections::VecDeque;
use tokio::io::AsyncReadExt;

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
        // If header_page_id is valid, tree is already initialized
        if self.header_page_id != INVALID_PAGE_ID {
            return Ok(());
        }

        // Allocate a new header page
        let header_page = self.buffer_pool_manager
            .new_page::<BPlusTreeHeaderPage>()
            .ok_or_else(|| BPlusTreeError::PageAllocationFailed)?;
        
        // Set initial values for the header
        {
            let mut header = header_page.write();
            header.set_root_page_id(INVALID_PAGE_ID);
            header.set_tree_height(0);
            header.set_num_keys(0);
            // The PageGuard will handle marking as dirty and unpinning when dropped
        }
        
        // Store the header page ID for future reference
        self.header_page_id = header_page.get_page_id();
        
        // The page will be automatically unpinned when header_page is dropped
        Ok(())
    }

    /// Insert a key-value pair into the B+ tree
    pub fn insert(&self, key: K, value: V) -> Result<(), BPlusTreeError> {
        // If tree is not initialized, initialize it first
        if self.header_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError("Tree not initialized".to_string()));
        }
        
        // Get header page to determine root page id
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
        let root_page_id;
        let tree_height;
        let num_keys;
        
        {
            let header = header_page.read();
            root_page_id = header.get_root_page_id();
            tree_height = header.get_tree_height();
            num_keys = header.get_num_keys();
        }

        // If tree is empty (root is INVALID_PAGE_ID), create first leaf page as root
        if root_page_id == INVALID_PAGE_ID {
            // Allocate a new leaf page
            let new_leaf_page = self.buffer_pool_manager
                .new_page::<BPlusTreeLeafPage<K, V, C>>()
                .ok_or_else(|| BPlusTreeError::PageAllocationFailed)?;
            
            let new_page_id = new_leaf_page.get_page_id();
            
            // Initialize the leaf page
            {
                let mut leaf_write = new_leaf_page.write();
                // Insert the key-value pair
                leaf_write.insert_key_value(key, value);
                // Other initialization would happen here if needed
                // Note: There might need to be a set_root method in the leaf page
            }
            
            // Update the header page with the new root
            {
                let mut header_write = header_page.write();
                header_write.set_root_page_id(new_page_id);
                header_write.set_tree_height(1);  // Height is 1 with just a root leaf
                header_write.set_num_keys(1);     // We inserted 1 key
            }
            
            return Ok(());
        }
        
        // For existing tree, find the leaf page where the key should be inserted
        let leaf_page = self.find_leaf_page(&key)?;
        
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
                if let Some(existing_key) = leaf_write.get_key_at(key_index) {
                    if (self.comparator)(&key, existing_key) == Ordering::Equal {
                        // Update existing value
                        leaf_write.set_value_at(key_index, value.clone());
                        inserted = true;
                        // No new key added, just updated existing value
                    }
                }
            }
            
            // If key doesn't exist yet
            if !inserted {
                // If leaf has space, insert directly
                if leaf_write.get_size() < leaf_write.get_max_size() {
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
                let mut header_write = header_page.write();
                header_write.set_num_keys(num_keys + 1);
            }
            return Ok(());
        }
        
        // If leaf is full, we need to split
        if need_split {
            // First insert the key-value pair into the leaf page
            {
                let mut leaf_write = leaf_page.write();
                leaf_write.insert_key_value(key, value);
            }
            
            // Then split the page
            self.split_leaf_page(&leaf_page)?;
            
            // Update num_keys in header
            {
                let mut header_write = header_page.write();
                header_write.set_num_keys(num_keys + 1);
            }
        }
        
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
            if (self.comparator)(key, &key_at_index) == Ordering::Equal {
                // Key found, return the value
                match leaf_page_read.get_value_at(key_index) {
                    Some(value) => return Ok(Some(value.clone())),
                    None => return Ok(None),
                }
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

        // Find the leaf page containing the key
        let leaf_page = match self.find_leaf_page(key) {
            Ok(page) => page,
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

        // Rebalancing is omitted in this snippet
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
        let leaf_page_guard = match self.find_leaf_page(start) {
            Ok(page) => page,
            Err(BPlusTreeError::KeyNotFound) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        
        // Start scanning from this leaf
        let mut current_leaf_guard = leaf_page_guard;
        
        loop {
            let leaf_page = current_leaf_guard.read();
            // Find the index of the first key >= start
            let mut key_index = leaf_page.find_key_index(start);
            let size = leaf_page.get_size();
            
            // Scan through keys in this leaf
            while key_index < size {
                if let Some(key) = leaf_page.get_key_at(key_index) {
                    // Check if we've exceeded the end of the range
                    if (self.comparator)(&key, end) == Ordering::Greater {
                        return Ok(result); // Done with range scan
                    }
                    
                    // Add key-value pair to result if value exists
                    if let Some(value) = leaf_page.get_value_at(key_index) {
                        result.push((key.clone(), value.clone()));
                    }
                }
                
                key_index += 1;
            }
            
            // Move to next leaf if available
            let next_page_id = match leaf_page.get_next_page_id() {
                Some(id) if id != INVALID_PAGE_ID => id,
                _ => break, // No more leaves or invalid next page
            };
            
            // Drop current leaf before fetching next one
            drop(leaf_page);
            
            // Get the next leaf page
            let next_page = self.buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(next_page_id)
                .ok_or_else(|| BPlusTreeError::PageNotFound(next_page_id))?;
            
            // Replace current_leaf_guard with the new page
            current_leaf_guard = next_page;
        }
        
        Ok(result)
    }

    /// Find the leaf page that should contain the key
    fn find_leaf_page(
        &self,
        key: &K,
    ) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // Fetch the header page to get the root_page_id
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
        let root_page_id;
        
        {
            let header = header_page.read();
            root_page_id = header.get_root_page_id();
            
            // Check if root_page_id is valid
            if root_page_id == INVALID_PAGE_ID {
                return Err(BPlusTreeError::KeyNotFound);
            }
        }
        
        // Start with the root page
        let mut current_page_id = root_page_id;
        
        // Traverse down to leaf
        loop {
            // Try to fetch the current page as a leaf page first
            if let Some(leaf_page) = self.buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id) {
                // If we successfully fetched a leaf page, return it
                return Ok(leaf_page);
            }
            
            // If not a leaf page, try as an internal page
            if let Some(internal_page) = self.buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id) {
                
                // Find the child page id for the key
                let child_page_id;
                {
                    let internal = internal_page.read();
                    // Find the appropriate child page for the key
                    child_page_id = internal.find_child_for_key(key);
                }
                
                // Drop the internal page before continuing
                drop(internal_page);
                
                // Continue with this child
                current_page_id = child_page_id.ok_or(BPlusTreeError::InvalidPageType)?;
            } else {
                // Neither a leaf nor an internal page, return error
                return Err(BPlusTreeError::PageNotFound(current_page_id));
            }
        }
    }

    /// Split a leaf page when it becomes full
    fn split_leaf_page(
        &self,
        leaf_page: &PageGuard<BPlusTreeLeafPage<K, V, C>>,
    ) -> Result<(), BPlusTreeError> {
        // Allocate a new leaf page
        let new_page = self.buffer_pool_manager
            .new_page::<BPlusTreeLeafPage<K, V, C>>()
            .ok_or_else(|| BPlusTreeError::PageAllocationFailed)?;
        
        let new_page_id = new_page.get_page_id();
        
        // Perform the split operation
        let separator_key;
        {
            // Get write access to both pages
            let mut leaf_write = leaf_page.write();
            let mut new_leaf_write = new_page.write();
            
            // Initialize the new leaf page if needed
            // (depending on implementation, this might be done automatically)
            
            // Determine split point - ceiling(max_size/2)
            let split_point = (leaf_write.get_max_size() + 1) / 2;
            let current_size = leaf_write.get_size();
            
            // Copy keys/values after split point to the new page
            for i in split_point..current_size {
                if let (Some(key), Some(value)) = (leaf_write.get_key_at(i), leaf_write.get_value_at(i)) {
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
            for i in split_point..current_size {
                leaf_write.remove_key_value_at(split_point);
            }
        }

        // Now handle the parent insertion - this may require creating a new root
        // For now, let's simplify - if leaf is root, create a new root
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
            // Handle insertion into parent
            // This would require tracking the parent during traversal
            // For now, return an error as this needs additional implementation
            return Err(BPlusTreeError::BufferPoolError("Parent insertion not implemented for non-root splits".to_string()));
        }
        
        Ok(())
    }

    /// Split an internal page when it becomes full
    fn split_internal_page(
        &self,
        internal_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
    ) -> Result<(), BPlusTreeError> {
        // Allocate a new internal page
        let new_page = self.buffer_pool_manager
            .new_page::<BPlusTreeInternalPage<K, C>>()
            .ok_or_else(|| BPlusTreeError::PageAllocationFailed)?;
        
        let new_page_id = new_page.get_page_id();
        
        // Perform the split operation
        let middle_key;
        {
            let mut internal_write = internal_page.write();
            let mut new_internal_write = new_page.write();
            
            // Determine split point - floor(max_size/2)
            let split_point = internal_write.get_max_size() / 2;
            let current_size = internal_write.get_size();
            
            // The middle key (at split_point) will be promoted to the parent
            middle_key = internal_write.get_key_at(split_point)
                .ok_or_else(|| BPlusTreeError::InvalidPageType)?.clone();
            
            // Get the child that will be the leftmost pointer in the new page
            // (This is the child pointer that goes immediately after the middle key)
            let child_after_middle = internal_write.get_value_at(split_point + 1)
                .ok_or_else(|| BPlusTreeError::InvalidPageType)?;
            
            // First insert the leftmost child pointer in the new page
            // For an internal page, we need to setup the first child before any keys
            new_internal_write.insert_key_value(middle_key.clone(), child_after_middle);
            // This added a key and two pointers, but we only wanted one pointer (leftmost)
            // We'll overwrite with the remaining keys and values
            
            // Now move the remaining keys and values
            for i in (split_point + 1)..current_size {
                if let Some(key) = internal_write.get_key_at(i) {
                    if let Some(child_id) = internal_write.get_value_at(i + 1) {
                        new_internal_write.insert_key_value(key.clone(), child_id);
                    }
                }
            }
            
            // Now remove keys from original page: remove the middle key and all keys after it
            for _ in (split_point)..current_size {
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
            // If not root, we need to insert the middle key and new page id into the parent
            // This requires tracking the parent during traversal
            return Err(BPlusTreeError::BufferPoolError("Parent insertion not implemented for internal pages".to_string()));
        }
        
        Ok(())
    }

    /// Merge two leaf pages when they become too empty
    fn merge_leaf_pages(
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
        
        // 2. Validate preconditions for merging
        // Ensure pages are adjacently linked in the parent
        if parent_write.get_value_at(parent_key_index) != Some(left_page.get_page_id()) ||
           parent_write.get_value_at(parent_key_index + 1) != Some(right_page.get_page_id()) {
            return Err(BPlusTreeError::BufferPoolError(
                "Leaf pages are not adjacent siblings in parent".to_string()
            ));
        }
        
        // Check if combined size fits in a single page
        let combined_size = left_write.get_size() + right_write.get_size();
        if combined_size > left_write.get_max_size() {
            return Err(BPlusTreeError::BufferPoolError(
                "Combined leaf pages are too large to merge".to_string()
            ));
        }
        
        // Verify parent_key_index is valid
        if parent_key_index >= parent_write.get_size() {
            return Err(BPlusTreeError::BufferPoolError(
                "Invalid parent key index".to_string()
            ));
        }
        
        // 3. Copy all key-value pairs from right_page to left_page
        for i in 0..right_write.get_size() {
            if let (Some(key), Some(value)) = (right_write.get_key_at(i), right_write.get_value_at(i)) {
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
            
            let header_page = self.buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
            
            {
                let mut header_write = header_page.write();
                let current_height = header_write.get_tree_height();
                header_write.set_root_page_id(left_page.get_page_id());
                header_write.set_tree_height(current_height - 1);
            }
            
            // Release the parent page (it's no longer needed)
            self.buffer_pool_manager.delete_page(parent_page.get_page_id())
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;
            
            // Release the right page (it's been merged)
            self.buffer_pool_manager.delete_page(right_page.get_page_id())
                .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;
            
            return Ok(());
        }
        
        // 7. If we haven't returned yet, the locks will be released when the guards go out of scope
        // The buffer pool will handle marking the pages as dirty
        
        // Store parent information before dropping the write guard
        let parent_needs_rebalancing = !is_parent_root && parent_size < parent_write.get_max_size() / 2;
        
        // Release the right page (it's been merged)
        drop(left_write);
        drop(parent_write);
        drop(right_write);
        self.buffer_pool_manager.delete_page(right_page.get_page_id())
            .map_err(|e| BPlusTreeError::BufferPoolError(e.to_string()))?;
        
        // Check if parent needs rebalancing after removing a key
        if parent_needs_rebalancing {
            // We'd need to handle parent underflow recursively here
            // But we'll return success for now and assume it'll be handled separately
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
        // Implementation to be filled in
        unimplemented!("redistribute_leaf_pages not implemented")
    }

    /// Merge two internal pages when they become too empty
    fn merge_internal_pages(
        &self,
        left_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // Implementation to be filled in
        unimplemented!("merge_internal_pages not implemented")
    }

    /// Redistribute keys between two internal pages to avoid merge
    fn redistribute_internal_pages(
        &self,
        left_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        right_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_page: &PageGuard<BPlusTreeInternalPage<K, C>>,
        parent_key_index: usize,
    ) -> Result<(), BPlusTreeError> {
        // Implementation to be filled in
        unimplemented!("redistribute_internal_pages not implemented")
    }

    /// Create a new root page
    fn create_new_root(
        &self,
        left_child_id: PageId,
        key: K,
        right_child_id: PageId,
    ) -> Result<(), BPlusTreeError> {
        // Allocate a new internal page
        let new_page = self.buffer_pool_manager
            .new_page::<BPlusTreeInternalPage<K, C>>()
            .ok_or_else(|| BPlusTreeError::PageAllocationFailed)?;
        
        let new_root_id = new_page.get_page_id();
        
        // Initialize as internal page and set up as root
        {
            let mut internal_write = new_page.write();
            
            // Use the populate_new_root method to set up the internal page with the separator key and child pointers
            if !internal_write.populate_new_root(left_child_id, key, right_child_id) {
                return Err(BPlusTreeError::BufferPoolError("Failed to populate new root page".to_string()));
            }
        }
        
        // Update the header page to point to the new root
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
        let header_page = match self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id) {
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
        let header_page = match self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id) {
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
        let header_page = match self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id) {
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
        let header_page = match self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id) {
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
        Err(BPlusTreeError::BufferPoolError("No sibling found for the given child".to_string()))
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

    // /// Validate the B+ tree structure with in-order traversal
    // pub fn validate(&self) -> Result<(), BPlusTreeError> {
    //     // If tree is not initialized, return Ok (nothing to validate)
    //     if self.header_page_id == INVALID_PAGE_ID {
    //         return Ok(());
    //     }
    //     
    //     // Fetch the header page to get tree metadata
    //     let header_page = self.buffer_pool_manager
    //         .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
    //         .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
    //     
    //     let (root_page_id, tree_height, num_keys) = {
    //         let header = header_page.read();
    //         (
    //             header.get_root_page_id(),
    //             header.get_tree_height(),
    //             header.get_num_keys(),
    //         )
    //     };
    //     
    //     // If root is invalid, tree is empty, return Ok
    //     if root_page_id == INVALID_PAGE_ID {
    //         return Ok(());
    //     }
    //     
    //     // Initialize validation stats
    //     let mut stats = ValidationStats::new();
    //     
    //     // Recursively validate the tree structure starting from root
    //     self.validate_subtree(root_page_id, None, None, 0, &mut stats)?;
    //     
    //     // Verify tree height
    //     if stats.max_depth + 1 != tree_height {
    //         return Err(BPlusTreeError::BufferPoolError(format!(
    //             "Tree height mismatch: header says {}, but actual is {}",
    //             tree_height, stats.max_depth + 1
    //         )));
    //     }
    //     
    //     // Verify total key count
    //     if stats.total_keys != num_keys {
    //         return Err(BPlusTreeError::BufferPoolError(format!(
    //             "Key count mismatch: header says {}, but actual is {}",
    //             num_keys, stats.total_keys
    //         )));
    //     }
    //     
    //     // Verify all leaf nodes are at the same level
    //     if stats.leaf_level.is_none() {
    //         // No leaves found, but we have a root - this shouldn't happen
    //         return Err(BPlusTreeError::BufferPoolError("No leaf nodes found in tree".to_string()));
    //     }
    //     
    //     // Additional validation: verify leaf node chain
    //     // (This would require additional traversal through next_page_id pointers)
    //     
    //     Ok(())
    // }
    // 
    // /// Helper method to recursively validate a subtree
    // fn validate_subtree(
    //     &self,
    //     page_id: PageId,
    //     min_key: Option<&K>,
    //     max_key: Option<&K>,
    //     level: u32,
    //     stats: &mut ValidationStats,
    // ) -> Result<(), BPlusTreeError> {
    //     // Update max depth if this level is deeper
    //     if level > stats.max_depth {
    //         stats.max_depth = level;
    //     }
    //     
    //     // First, try to fetch the page as a leaf page
    //     if let Some(leaf_page) = self.buffer_pool_manager.fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id) {
    //         let leaf_read = leaf_page.read();
    //         
    //         // Check if this is a leaf page
    //         // Record leaf level if this is the first leaf we've seen
    //         if stats.leaf_level.is_none() {
    //             stats.leaf_level = Some(level);
    //         } else if stats.leaf_level != Some(level) {
    //             // All leaves must be at the same level
    //             return Err(BPlusTreeError::BufferPoolError(format!(
    //                 "Leaf nodes at different levels: expected {}, found {}",
    //                 stats.leaf_level.unwrap(), level
    //             )));
    //         }
    //         
    //         // Verify key ordering and min/max constraints
    //         let size = leaf_read.get_size();
    //         stats.total_keys += size;
    //         
    //         // Check all keys are sorted and within range
    //         let mut prev_key: Option<&K> = None;
    //         for i in 0..size {
    //             if let Some(key) = leaf_read.get_key_at(i) {
    //                 // Check if key is greater than min_key (if specified)
    //                 if let Some(min) = min_key {
    //                     if (self.comparator)(key, min) != Ordering::Greater {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation: key {} not greater than min_key {}",
    //                             key, min
    //                         )));
    //                     }
    //                 }
    //                 
    //                 // Check if key is less than max_key (if specified)
    //                 if let Some(max) = max_key {
    //                     if (self.comparator)(key, max) != Ordering::Less {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation: key {} not less than max_key {}",
    //                             key, max
    //                         )));
    //                     }
    //                 }
    //                 
    //                 // Check keys are in ascending order within the node
    //                 if let Some(prev) = prev_key {
    //                     if (self.comparator)(key, prev) != Ordering::Greater {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation within leaf: key {} not greater than previous key {}",
    //                             key, prev
    //                         )));
    //                     }
    //                 }
    //                 
    //                 prev_key = Some(key);
    //             }
    //         }
    //         
    //         // Verify leaf is at least min_size (unless it's the root)
    //         if !leaf_read.is_root() && size < leaf_read.get_max_size() / 2 {
    //             return Err(BPlusTreeError::BufferPoolError(format!(
    //                 "Leaf page {} has {} keys, which is less than min_size {}",
    //                 page_id, size, leaf_read.get_max_size() / 2
    //             )));
    //         }
    //         
    //         // Verify leaf is at most max_size
    //         if size > leaf_read.get_max_size() {
    //             return Err(BPlusTreeError::BufferPoolError(format!(
    //                 "Leaf page {} has {} keys, which exceeds max_size {}",
    //                 page_id, size, leaf_read.get_max_size()
    //             )));
    //         }
    //         
    //         return Ok(());
    //     }
    //     
    //     // If not a leaf, try as an internal page
    //     if let Some(internal_page) = self.buffer_pool_manager.fetch_page::<BPlusTreeInternalPage<K, C>>(page_id) {
    //         let internal_read = internal_page.read();
    //         
    //         // Verify internal node properties
    //         let size = internal_read.get_size();
    //         
    //         // Check keys are sorted and within range
    //         let mut prev_key: Option<K> = None;
    //         for i in 0..size {
    //             if let Some(key) = internal_read.get_key_at(i) {
    //                 // Check against min/max constraints
    //                 if let Some(min) = min_key {
    //                     if (self.comparator)(&key, &min) != Ordering::Greater {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation: key {} not greater than min_key {}",
    //                             key, min
    //                         )));
    //                     }
    //                 }
    //                 
    //                 if let Some(max) = max_key {
    //                     if (self.comparator)(&key, &max) != Ordering::Less {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation: key {} not less than max_key {}",
    //                             key, max
    //                         )));
    //                     }
    //                 }
    //                 
    //                 // Check keys are in ascending order
    //                 if let Some(prev) = prev_key.as_ref() {
    //                     if (self.comparator)(&key, &prev) != Ordering::Greater {
    //                         return Err(BPlusTreeError::BufferPoolError(format!(
    //                             "Key ordering violation within internal node: key {} not greater than previous key {}",
    //                             key, prev
    //                         )));
    //                     }
    //                 }
    //                 
    //                 prev_key = Some(key.clone());
    //             }
    //         }
    //         
    //         // Verify node has at least min_size keys (unless it's the root)
    //         if !internal_read.is_root() && size < internal_read.get_max_size() / 2 {
    //             return Err(BPlusTreeError::BufferPoolError(format!(
    //                 "Internal page {} has {} keys, which is less than min_size {}",
    //                 page_id, size, internal_read.get_max_size() / 2
    //             )));
    //         }
    //         
    //         // Verify node has at most max_size keys
    //         if size > internal_read.get_max_size() {
    //             return Err(BPlusTreeError::BufferPoolError(format!(
    //                 "Internal page {} has {} keys, which exceeds max_size {}",
    //                 page_id, size, internal_read.get_max_size()
    //             )));
    //         }
    //         
    //         // Root with zero keys should only happen in specific cases
    //         if internal_read.is_root() && size == 0 {
    //             // Root with no keys should have exactly one child
    //             if internal_read.get_value_at(0).is_none() {
    //                 return Err(BPlusTreeError::BufferPoolError(
    //                     "Root node with no keys has no children".to_string()
    //                 ));
    //             }
    //         }
    //         
    //         // Recursively validate each child
    //         for i in 0..=size {
    //             if let Some(child_id) = internal_read.get_value_at(i) {
    //                 // Determine the valid key range for this child
    //                 let child_min_key: Option<&K> = if i == 0 {
    //                     min_key  // Leftmost child inherits parent's min_key
    //                 } else {
    //                     // Get the reference from the internal page
    //                     let key = internal_read.get_key_at(i - 1);
    //                     key.as_ref()
    //                 };
    //                 
    //                 let child_max_key: Option<&K> = if i == size {
    //                     max_key  // Rightmost child inherits parent's max_key
    //                 } else {
    //                     // Get the reference from the internal page
    //                     internal_read.get_key_at(i).as_ref()
    //                 };
    //                 
    //                 // Recursively validate the child subtree
    //                 self.validate_subtree(
    //                     child_id,
    //                     child_min_key,
    //                     child_max_key,
    //                     level + 1,
    //                     stats
    //                 )?;
    //             } else {
    //                 return Err(BPlusTreeError::BufferPoolError(format!(
    //                     "Missing child pointer at index {} in internal page {}",
    //                     i, page_id
    //                 )));
    //             }
    //         }
    //         
    //         return Ok(());
    //     }
    //     
    //     // If we reach here, the page is neither leaf nor internal
    //     Err(BPlusTreeError::InvalidPageType)
    // }

    /// Perform a level-order traversal (breadth-first) for debugging or visualization
    pub fn print_tree(&self) -> Result<(), BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            println!("Empty tree - not initialized");
            return Ok(());
        }
        
        // Get header page to check if tree is empty
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
            if let Some(leaf_page) = self.buffer_pool_manager.fetch_page::<BPlusTreeLeafPage<K, V, C>>(page_id) {
                let leaf_read = leaf_page.read();
                
                // Print leaf information
                println!("{}[Leaf {}]: Size={}, Next={:?}", 
                    indent, page_id, leaf_read.get_size(), leaf_read.get_next_page_id());
                
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
            if let Some(internal_page) = self.buffer_pool_manager.fetch_page::<BPlusTreeInternalPage<K, C>>(page_id) {
                let internal_read = internal_page.read();
                
                // Print internal node information
                println!("{}[Internal {}]: Size={}, IsRoot={}", 
                    indent, page_id, internal_read.get_size(), internal_read.is_root());
                
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
                    for i in 0..=internal_read.get_size() { // One more child than keys
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
            let next_page = self.buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(next_page_id)
                .ok_or_else(|| BPlusTreeError::PageNotFound(next_page_id))?;
            
            // Update current leaf
            current_leaf = next_page;
        }
        
        Ok(result)
    }

    /// Find the leftmost leaf page in the tree (for range scans or in-order traversal)
    fn find_leftmost_leaf(&self) -> Result<PageGuard<BPlusTreeLeafPage<K, V, C>>, BPlusTreeError> {
        // Check if tree is initialized
        if self.header_page_id == INVALID_PAGE_ID {
            return Err(BPlusTreeError::BufferPoolError("Tree not initialized".to_string()));
        }
        
        // Get header page to check if tree is empty
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
            if let Some(leaf_page) = self.buffer_pool_manager
                .fetch_page::<BPlusTreeLeafPage<K, V, C>>(current_page_id) {
                // Found a leaf page, return it
                return Ok(leaf_page);
            }
            
            // If not a leaf, try as an internal page
            if let Some(internal_page) = self.buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(current_page_id) {
                
                // Get the leftmost child (index 0)
                let leftmost_child_id = internal_page.read().get_value_at(0)
                    .ok_or_else(|| BPlusTreeError::InvalidPageType)?;
                
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
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
            if let Some(internal_page) = self.buffer_pool_manager
                .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id) {
                
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
        let header_page = self.buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .ok_or_else(|| BPlusTreeError::BufferPoolError("Failed to fetch header page".to_string()))?;
        
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
    fn post_order_helper(&self, page_id: PageId, result: &mut Vec<PageId>) -> Result<(), BPlusTreeError> {
        // Check if this is an internal page
        if let Some(internal_page) = self.buffer_pool_manager
            .fetch_page::<BPlusTreeInternalPage<K, C>>(page_id) {
            
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
