//! Internal (non-leaf) node implementation for B+ tree indexes.
//!
//! This module provides [`BPlusTreeInternalPage`], which represents an internal node
//! in a B+ tree. Internal nodes store keys and child pointers, guiding searches
//! down to the leaf level where actual data resides.
//!
//! # Structure
//!
//! An internal node with `n` keys has `n+1` child pointers:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Ptr₀ │ Key₀ │ Ptr₁ │ Key₁ │ Ptr₂ │ ... │ Keyₙ₋₁ │ Ptrₙ    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! - `Ptr₀` points to keys less than `Key₀`
//! - `Ptrᵢ` (for i > 0) points to keys in range `[Keyᵢ₋₁, Keyᵢ)`
//! - `Ptrₙ` points to keys greater than or equal to `Keyₙ₋₁`
//!
//! # Key Operations
//!
//! - **Search**: [`find_child_for_key`](BPlusTreeInternalPage::find_child_for_key) returns
//!   the child page ID to descend into for a given search key.
//! - **Insert**: [`insert_key_value`](BPlusTreeInternalPage::insert_key_value) adds a
//!   separator key and child pointer, maintaining sorted order.
//! - **Delete**: [`remove_key_value_at`](BPlusTreeInternalPage::remove_key_value_at) removes
//!   a key and its associated child pointer.
//!
//! # Balancing Operations
//!
//! When nodes become underfull after deletions, the tree must be rebalanced:
//!
//! - **Redistribution**: Borrow a key from a sibling via
//!   [`borrow_from_left`](BPlusTreeInternalPage::borrow_from_left) or
//!   [`borrow_from_right`](BPlusTreeInternalPage::borrow_from_right).
//! - **Merge**: Combine with a sibling using
//!   [`merge_with_right_sibling`](BPlusTreeInternalPage::merge_with_right_sibling).
//!
//! # Root Node Handling
//!
//! Root nodes have special rules:
//! - Can have fewer than `min_size` keys
//! - When reduced to zero keys with one child, should be collapsed
//!   (see [`should_collapse_root`](BPlusTreeInternalPage::should_collapse_root))
//!
//! # Generics
//!
//! The page is generic over:
//! - `KeyType`: The type of keys stored (must be `Clone + Send + Sync + Debug`)
//! - `KeyComparator`: A comparison function for key ordering
//!
//! # Invariant Checking
//!
//! Use [`check_invariants`](BPlusTreeInternalPage::check_invariants) to validate
//! that the node maintains all B+ tree properties (sorted keys, correct pointer
//! count, size constraints).

use std::any::Any;
use std::fmt::{Debug, Formatter};

use log::debug;

use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::common::exception::PageError;
use crate::storage::page::{PAGE_TYPE_OFFSET, Page, PageTrait, PageType, PageTypeId};

/// Internal page structure for B+ Tree
pub struct BPlusTreeInternalPage<KeyType, KeyComparator> {
    // Array of keys
    keys: Vec<KeyType>,
    // Array of child page IDs (values)
    values: Vec<PageId>,
    // KeyComparator function
    comparator: KeyComparator,
    // Page data
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    // Page ID
    page_id: PageId,
    // Pin count
    pin_count: i32,
    // Dirty flag
    is_dirty: bool,
    // Current size
    size: usize,
    // Maximum size
    max_size: usize,
    // Flag indicating if this is a root node
    is_root: bool,
}

impl<
    KeyType: Clone + Sync + Send + Debug + 'static + std::fmt::Display,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + Sync + Send + 'static + Clone,
> BPlusTreeInternalPage<KeyType, KeyComparator>
{
    pub fn new_with_options(page_id: PageId, max_size: usize, comparator: KeyComparator) -> Self {
        let mut page = Self {
            keys: Vec::with_capacity(max_size),
            values: Vec::with_capacity(max_size + 1), // Internal nodes have n+1 children for n keys
            comparator,
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            size: 0,
            max_size,
            is_root: false,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }

    /// Create a new internal page that is a root node
    pub fn new_root_page(page_id: PageId, max_size: usize, comparator: KeyComparator) -> Self {
        let mut page = Self::new_with_options(page_id, max_size, comparator);
        page.is_root = true;
        page
    }

    /// Set whether this internal page is a root node
    pub fn set_root_status(&mut self, is_root: bool) {
        self.is_root = is_root;
        self.is_dirty = true;
    }

    /// Check if this internal page is a root node
    pub fn is_root(&self) -> bool {
        self.is_root
    }

    /// Check if this root node should be collapsed during deletion
    /// In B+ trees, when a root has only one child left, it should be removed
    /// to maintain the height balance property
    pub fn should_collapse_root(&self) -> bool {
        self.is_root && self.keys.is_empty()
    }

    /// Get the only child of the root (when root should be collapsed)
    pub fn get_only_child_page_id(&self) -> Option<PageId> {
        if self.should_collapse_root() && !self.values.is_empty() {
            Some(self.values[0])
        } else {
            None
        }
    }

    /// Insert a new key-value pair into the internal page
    /// In a B+ tree, for a node with N keys, there should be N+1 pointers
    /// - The 0th pointer (leftmost) points to keys less than the 0th key
    /// - The ith pointer points to keys between the (i-1)th and ith keys
    /// - The Nth pointer (rightmost) points to keys greater than the (N-1)th key
    pub fn insert_key_value(&mut self, key: KeyType, value: PageId) -> bool {
        debug!(
            "Attempting to insert key {:?}, value {} into page {}",
            key, value, self.page_id
        );
        debug!(
            "Current state - size: {}, keys: {:?}, values: {:?}",
            self.size, self.keys, self.values
        );

        if self.size >= self.max_size {
            debug!(
                "Insert failed: page {} is at maximum capacity ({})",
                self.page_id, self.max_size
            );
            return false;
        }

        // First insertion case: special handling
        if self.size == 0 {
            debug!(
                "First insertion in page {}: adding key {:?} and value {}",
                self.page_id, key, value
            );
            // For the first key, we need two pointers
            // For now, set both to the same value - the caller will update one of them
            self.keys.push(key);
            self.values.push(value); // Left pointer
            self.values.push(value); // Right pointer
            self.size = 1;
            self.is_dirty = true;
            debug!(
                "After first insertion - size: {}, keys: {:?}, values: {:?}",
                self.size, self.keys, self.values
            );
            return true;
        }

        // Find the correct position for the key to maintain sorted order
        let key_pos = self.find_key_index(&key);
        debug!("Found key insertion position {} for key {:?}", key_pos, key);

        // Check for duplicate
        if key_pos < self.keys.len()
            && (self.comparator)(&self.keys[key_pos], &key) == std::cmp::Ordering::Equal
        {
            // If key already exists, replace the corresponding value
            if key_pos + 1 < self.values.len() {
                self.values[key_pos + 1] = value;
                self.is_dirty = true;
                return true;
            }
            return false;
        }

        // Insert the key at the proper position
        self.keys.insert(key_pos, key);

        // The value (pointer) should be inserted after the key
        // If we're inserting at position i, the new pointer goes at position i+1
        let value_pos = key_pos + 1;

        if value_pos <= self.values.len() {
            self.values.insert(value_pos, value);
        } else {
            // This shouldn't normally happen, but just in case
            self.values.push(value);
        }

        self.size += 1;
        self.is_dirty = true;

        debug!(
            "After insertion - size: {}, keys: {:?}, values: {:?}",
            self.size, self.keys, self.values
        );
        true
    }

    /// Get key at the specified index
    pub fn get_key_at(&self, index: usize) -> Option<KeyType> {
        debug!("Retrieving key at index {}", index);

        if index >= self.keys.len() {
            debug!(
                "Index {} is out of bounds for keys (len: {}), returning None",
                index,
                self.keys.len()
            );
            return None;
        }

        debug!(
            "Keys array: {:?}, returning: {:?}",
            self.keys,
            self.keys.get(index)
        );
        Some(self.keys[index].clone())
    }

    /// Get value (pointer) at the specified index
    pub fn get_value_at(&self, index: usize) -> Option<PageId> {
        debug!("Retrieving value at index {}", index);

        if index >= self.values.len() {
            debug!(
                "Index {} is out of bounds for values (len: {}), returning None",
                index,
                self.values.len()
            );
            return None;
        }

        debug!(
            "Values array: {:?}, returning: {:?}",
            self.values,
            self.values.get(index)
        );
        Some(self.values[index])
    }

    /// Find the index of the first key greater than or equal to the target key
    pub fn find_key_index(&self, key: &KeyType) -> usize {
        debug!(
            "Finding insertion index for key {:?} in {:?}",
            key, self.keys
        );

        // Handle the case when keys is empty
        if self.keys.is_empty() {
            debug!("Keys array is empty, returning index 0");
            return 0;
        }

        // Binary search to find the insertion point
        let mut left = 0;
        let mut right = self.keys.len() - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            debug!(
                "Binary search: left={}, mid={}, right={}, comparing with key={:?}",
                left, mid, right, self.keys[mid]
            );

            match (self.comparator)(key, &self.keys[mid]) {
                std::cmp::Ordering::Less => {
                    debug!("Key {:?} is less than {:?}", key, self.keys[mid]);
                    if mid == 0 {
                        debug!("Reached leftmost position, returning index 0");
                        return 0;
                    }
                    right = mid - 1;
                },
                std::cmp::Ordering::Greater => {
                    debug!("Key {:?} is greater than {:?}", key, self.keys[mid]);
                    left = mid + 1;
                },
                std::cmp::Ordering::Equal => {
                    debug!(
                        "Key {:?} is equal to {:?}, returning index {}",
                        key, self.keys[mid], mid
                    );
                    return mid;
                },
            }
        }

        debug!("Final insertion index for key {:?}: {}", key, left);
        left
    }

    /// Find child the page that should contain the key
    pub fn find_child_for_key(&self, key: &KeyType) -> Option<PageId> {
        if self.size == 0 || self.values.is_empty() {
            return None;
        }

        // Special case: if there's only one key, return the appropriate pointer
        if self.keys.len() == 1 {
            // For a single key K, compare the target key with K
            return match (self.comparator)(key, &self.keys[0]) {
                std::cmp::Ordering::Less => {
                    // Key is less than K, go to the left pointer
                    Some(self.values[0])
                },
                _ => {
                    // Key is greater than or equal to K, go to the right pointer
                    if self.values.len() > 1 {
                        Some(self.values[1])
                    } else {
                        // If we only have one pointer, use it for all keys
                        Some(self.values[0])
                    }
                },
            };
        }

        // In B+ tree internal nodes, the keys divide the range of keys
        // that should go to each child pointer

        // First check if key is less than the first key
        if (self.comparator)(key, &self.keys[0]) == std::cmp::Ordering::Less {
            return Some(self.values[0]); // Leftmost pointer
        }

        // Check each key to find where the search key falls
        for i in 0..self.keys.len() - 1 {
            // If key >= keys[i] and key < keys[i+1], go to pointer i+1
            if (self.comparator)(key, &self.keys[i]) != std::cmp::Ordering::Less
                && (self.comparator)(key, &self.keys[i + 1]) == std::cmp::Ordering::Less
            {
                return if i + 1 < self.values.len() {
                    Some(self.values[i + 1])
                } else {
                    Some(self.values[self.values.len() - 1])
                };
            }
        }

        // If key >= the last key, go to the rightmost pointer
        if !self.values.is_empty() {
            Some(self.values[self.values.len() - 1])
        } else {
            None
        }
    }

    /// Remove a key and its corresponding value (child pointer) from the internal page
    /// In a B+ tree internal node:
    /// - When removing key at index i, we remove the child pointer at index i+1
    /// - This maintains the invariant that N keys have N+1 pointers
    pub fn remove_key_value_at(&mut self, index: usize) -> bool {
        debug!(
            "Attempting to remove key-value at index {} from page {}",
            index, self.page_id
        );

        if index >= self.keys.len() {
            debug!(
                "Invalid index: {} exceeds keys length {}",
                index,
                self.keys.len()
            );
            return false;
        }

        // Special case: removing the last key
        if self.keys.len() == 1 {
            // Clear all keys
            self.keys.clear();

            // Special handling for root nodes
            if self.is_root {
                // For a root node with only one key, when that key is removed,
                // we keep the leftmost pointer (which becomes the only child)
                if self.values.len() > 1 {
                    // Keep only the leftmost pointer
                    let leftmost_pointer = self.values[0];
                    self.values.clear();
                    self.values.push(leftmost_pointer);
                }
            } else {
                // For non-root nodes, we clear all values - the parent will handle this
                self.values.clear();
            }

            self.size = 0;
            self.is_dirty = true;
            debug!("Removed last key-value pair, node is now empty or has single child");
            return true;
        }

        // Remove the key at the specified index
        self.keys.remove(index);

        // In a B+ tree internal node, when removing key at index i,
        // we remove the child pointer at index i+1
        if index + 1 < self.values.len() {
            self.values.remove(index + 1);
        }

        self.size -= 1;
        self.is_dirty = true;

        debug!(
            "After removal - size: {}, keys: {:?}, values: {:?}",
            self.size, self.keys, self.values
        );

        true
    }

    /// Get the current size of this page
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Get the maximum size of this page
    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    /// Get the minimum size (50% of the maximum size) of this page
    pub fn get_min_size(&self) -> usize {
        // Internal node with N keys has N+1 children; min children is ceil((N+1)/2),
        // so min keys is that minus 1 => floor(N/2).
        self.max_size / 2
    }

    /// Serialize the internal page to bytes
    pub fn serialize(&self, buffer: &mut [u8]) {
        // First serialize the header (12 bytes)
        // Format: PageType (4) | CurrentSize (4) | MaxSize (4)
        buffer[0..4].copy_from_slice(&(Self::TYPE_ID as u32).to_le_bytes());
        buffer[4..8].copy_from_slice(&(self.size as u32).to_le_bytes());
        buffer[8..12].copy_from_slice(&(self.max_size as u32).to_le_bytes());

        // Serialize the keys and values
        // This is a simplified version - in real implementation,
        // you would need to know the size of KeyType and properly serialize it
        // For this example, we'll assume serialization logic for keys and values
        // which would depend on the actual types used

        // The actual serialization would use a format like:
        // [header: 12 bytes][keys: variable size][values: variable size]
    }

    /// Deserialize the internal page from bytes
    pub fn deserialize(&mut self, buffer: &[u8]) {
        // First deserialize the header (12 bytes)
        // Format: PageType (4) | CurrentSize (4) | MaxSize (4)
        self.size = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;
        self.max_size = u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize;

        // Deserialize the keys and values
        // This would depend on the actual KeyType and ValueType

        // The actual deserialization would read the keys and values
        // from their respective sections in the buffer
    }

    /// Visualizes the internal page structure in a human-readable format
    pub fn visualize(&self) -> String {
        let mut result = format!(
            "Internal Page [ID: {}, Size: {}/{}]\n",
            self.page_id, self.size, self.max_size
        );
        result.push_str("┌─────────────────────────────────────────────────────────────┐\n");
        result.push_str("│ Ptr0 │ Key0 │ Ptr1 │ Key1 │ Ptr2 │ ... │ KeyN-1 │ PtrN     │\n");
        result.push_str("├─────────────────────────────────────────────────────────────┤\n");

        let mut line = "│".to_string();

        // For each key, we print the pointer before it and the key itself
        for i in 0..self.keys.len() {
            // Add the pointer
            if i < self.values.len() {
                line.push_str(&format!(" {:^4} │", self.values[i]));
            } else {
                line.push_str(" ???? │");
            }

            // Add the key
            line.push_str(&format!(" {:^4} │", self.keys[i]));
        }

        // Add the last pointer (after all keys)
        if self.keys.len() < self.values.len() {
            line.push_str(&format!(" {:^4} │", self.values[self.keys.len()]));
        } else {
            line.push_str(" ???? │");
        }

        result.push_str(&line);
        result.push_str("\n└─────────────────────────────────────────────────────────────┘\n");

        // Add B-tree structure explanation
        result.push_str("\nB-tree Internal Node Structure:\n");
        result.push_str("- Each internal node with k keys has k+1 pointers\n");
        result.push_str("- Ptr_i points to subtree with keys < Key_i\n");
        result.push_str("- Ptr_{i+1} points to subtree with keys ≥ Key_i\n");

        // Add debug info
        result.push_str(&format!("\nKeys: {:?}\n", self.keys));
        result.push_str(&format!("Values (pointers): {:?}\n", self.values));

        result
    }

    /// Converts the internal page to DOT format for visualization with Graphviz
    pub fn to_dot(&self) -> String {
        let node_id = format!("internal_{}", self.page_id);
        let mut dot = format!("  {} [shape=record, label=\"{{", node_id);

        // Add node information
        dot.push_str(&format!(
            "Internal Page ID: {}|Size: {}/{}",
            self.page_id, self.size, self.max_size
        ));

        // Add all keys and their corresponding pointers
        dot.push_str("|{");
        for i in 0..self.size {
            if i > 0 {
                dot.push('|');
            }
            if let Some(key) = self.get_key_at(i) {
                dot.push_str(&format!("<key{}>K:{:?}", i, key));
            }
            if let Some(value) = self.get_value_at(i) {
                dot.push_str(&format!("|<ptr{}>P:{}", i, value));
            }
        }
        dot.push_str("}}\"]; // end of node\n");

        dot
    }

    /// Validates that the internal page maintains all B+ tree invariants
    /// Returns a Result with either () for success or a detailed error message
    pub fn check_invariants(&self) -> Result<(), String> {
        // 1. Check that size is accurate
        if self.size != self.keys.len() {
            return Err(format!(
                "Size mismatch: size field is {} but actual keys count is {}",
                self.size,
                self.keys.len()
            ));
        }

        // 2. Check the relationship between keys and values (pointers)
        // In a B+ tree internal node, for n keys there must be n+1 pointers
        if self.values.len() != self.keys.len() + 1 && !self.keys.is_empty() {
            return Err(format!(
                "Key-value count invariant violated: keys={}, values={}, expected values={}",
                self.keys.len(),
                self.values.len(),
                self.keys.len() + 1
            ));
        }

        // 3. Check that keys are in sorted order
        for i in 1..self.keys.len() {
            if (self.comparator)(&self.keys[i - 1], &self.keys[i]) != std::cmp::Ordering::Less {
                return Err(format!(
                    "Keys not in sorted order: key[{}]={:?} is not less than key[{}]={:?}",
                    i - 1,
                    self.keys[i - 1],
                    i,
                    self.keys[i]
                ));
            }
        }

        // 4. Check size constraints
        // - Internal node should have at most max_size keys
        if self.keys.len() > self.max_size {
            return Err(format!(
                "Node exceeds max size: contains {} keys but max is {}",
                self.keys.len(),
                self.max_size
            ));
        }

        // For non-root internal nodes, we enforce minimum size requirements
        if !self.is_root {
            // Internal node should have at least min_size keys
            let min_size = self.get_min_size();
            if self.keys.len() < min_size && !self.keys.is_empty() {
                return Err(format!(
                    "Non-root node has too few keys: contains {} keys but min is {}",
                    self.keys.len(),
                    min_size
                ));
            }
        } else {
            // Root specific checks
            // - Root can have fewer than min_size keys
            // - But if it has keys, it must follow the key-value relationship rules
            if !self.keys.is_empty() && self.values.len() != self.keys.len() + 1 {
                return Err(format!(
                    "Root node key-value count invariant violated: keys={}, values={}, expected values={}",
                    self.keys.len(),
                    self.values.len(),
                    self.keys.len() + 1
                ));
            }
        }

        // 5. Check that no child pointers are invalid (implementation dependent)
        // This is a basic check - you might want to enhance based on your specific invalid pointer definition
        for (i, &pointer) in self.values.iter().enumerate() {
            if pointer == 0 && self.size > 0 {
                // Assuming 0 is an invalid pointer when node is not empty
                return Err(format!(
                    "Invalid child pointer: values[{}] is 0 which is invalid for non-empty node",
                    i
                ));
            }
        }

        // All invariants satisfied
        Ok(())
    }

    /// Helper method for tests to check invariants and return a boolean
    pub fn is_valid(&self) -> bool {
        self.check_invariants().is_ok()
    }

    /// Populate a new root node with two children
    /// Used when a child node splits and a new root needs to be created
    ///
    /// Parameters:
    /// - first_child_page_id: The left child page ID
    /// - middle_key: The key that separates the two children
    /// - second_child_page_id: The right child page ID
    pub fn populate_new_root(
        &mut self,
        first_child_page_id: PageId,
        middle_key: KeyType,
        second_child_page_id: PageId,
    ) -> bool {
        // Ensure this is an empty page
        if !self.keys.is_empty() || !self.values.is_empty() {
            debug!("Cannot populate non-empty page as new root");
            return false;
        }

        // Mark this as a root node
        self.is_root = true;

        // Add the middle key
        self.keys.push(middle_key);

        // Add the two child pointers
        self.values.push(first_child_page_id);
        self.values.push(second_child_page_id);

        self.size = 1;
        self.is_dirty = true;

        debug!(
            "Populated new root - key: {:?}, left: {}, right: {}",
            self.keys[0], first_child_page_id, second_child_page_id
        );

        true
    }

    /// Determines if this node is underfull (has fewer than min_size keys)
    /// but not empty. Empty nodes are handled separately.
    pub fn is_underfull(&self) -> bool {
        !self.is_root && !self.keys.is_empty() && self.keys.len() < self.get_min_size()
    }

    /// Determines if this node can afford to give away a key
    /// (has more than min_size keys)
    pub fn can_donate_key(&self) -> bool {
        self.keys.len() > self.get_min_size()
    }

    /// Borrow a key from the left sibling and a key from the parent to maintain B+ tree properties
    ///
    /// Parameters:
    /// - parent_key: The key to borrow from the parent
    /// - sibling_rightmost_child: The rightmost child pointer from the left sibling (only for internal nodes)
    ///
    /// Returns true if the borrow operation was successful
    pub fn borrow_from_left(
        &mut self,
        parent_key: KeyType,
        sibling_rightmost_child: PageId,
    ) -> bool {
        if self.size >= self.max_size {
            return false; // No space to borrow
        }

        // Insert the parent key at the beginning of this node's keys
        self.keys.insert(0, parent_key);

        // If this is an internal node, we need to update child pointers
        // The sibling's rightmost child becomes this node's leftmost child
        if !self.values.is_empty() {
            self.values.insert(0, sibling_rightmost_child);
        }

        self.size += 1;
        self.is_dirty = true;

        true
    }

    /// Borrow a key from the right sibling and a key from the parent to maintain B+ tree properties
    ///
    /// Parameters:
    /// - parent_key: The key to borrow from the parent
    /// - sibling_leftmost_child: The leftmost child pointer from the right sibling (only for internal nodes)
    ///
    /// Returns true if the borrow operation was successful
    pub fn borrow_from_right(
        &mut self,
        parent_key: KeyType,
        sibling_leftmost_child: PageId,
    ) -> bool {
        if self.size >= self.max_size {
            return false; // No space to borrow
        }

        // Add the parent key to the end of this node's keys
        self.keys.push(parent_key);

        // If this is an internal node, we need to update child pointers
        // The sibling's leftmost child becomes this node's rightmost child
        if !self.values.is_empty() {
            self.values.push(sibling_leftmost_child);
        }

        self.size += 1;
        self.is_dirty = true;

        true
    }

    /// Merges this node with a right sibling using a parent key
    ///
    /// Parameters:
    /// - parent_key: The key from the parent that separates the two nodes
    /// - right_sibling_keys: The keys from the right sibling
    /// - right_sibling_values: The child pointers from the right sibling
    ///
    /// Returns true if the merge operation was successful
    pub fn merge_with_right_sibling(
        &mut self,
        parent_key: KeyType,
        right_sibling_keys: Vec<KeyType>,
        right_sibling_values: Vec<PageId>,
    ) -> bool {
        debug!(
            "Attempting to merge with right sibling - current keys: {:?}, values: {:?}",
            self.keys, self.values
        );
        debug!(
            "Parent key: {:?}, right sibling keys: {:?}, values: {:?}",
            parent_key, right_sibling_keys, right_sibling_values
        );

        // Check if the merged node would exceed max capacity
        if self.keys.len() + 1 + right_sibling_keys.len() > self.max_size {
            debug!(
                "Merge would exceed capacity: {} + 1 + {} > {}",
                self.keys.len(),
                right_sibling_keys.len(),
                self.max_size
            );
            return false;
        }

        // Add the parent key
        self.keys.push(parent_key);

        // Add all keys from the right sibling
        for key in right_sibling_keys {
            self.keys.push(key);
        }

        // For internal nodes, we need to update child pointers
        // The rightmost pointer of this node should already point to the same
        // location as the leftmost pointer of the right sibling, so we skip
        // the first value from the right sibling
        if !self.values.is_empty() && !right_sibling_values.is_empty() {
            // Skip the first pointer of the right sibling (p1') because it should be
            // the same as the last pointer of the left node
            for i in right_sibling_values.iter().skip(1) {
                self.values.push(*i);
            }
        }

        // Make sure we have n+1 pointers for n keys
        if !self.keys.is_empty() && self.values.len() != self.keys.len() + 1 {
            debug!(
                "After merge - keys: {}, values: {} - invariant requires {} values",
                self.keys.len(),
                self.values.len(),
                self.keys.len() + 1
            );

            // If we're missing the last pointer, add it from the right sibling's last pointer
            if self.values.len() == self.keys.len() && !right_sibling_values.is_empty() {
                self.values
                    .push(right_sibling_values[right_sibling_values.len() - 1]);
                debug!(
                    "Added missing last pointer: {}",
                    right_sibling_values[right_sibling_values.len() - 1]
                );
            }
        }

        // Update the size to match the actual number of keys
        self.size = self.keys.len();
        self.is_dirty = true;

        debug!(
            "After merge - size: {}, keys: {:?}, values: {:?}",
            self.size, self.keys, self.values
        );

        true
    }

    /// Gets the first key (smallest key) in this node
    pub fn get_first_key(&self) -> Option<KeyType> {
        if self.keys.is_empty() {
            None
        } else {
            Some(self.keys[0].clone())
        }
    }

    /// Gets the last key (largest key) in this node
    pub fn get_last_key(&self) -> Option<KeyType> {
        if self.keys.is_empty() {
            None
        } else {
            Some(self.keys[self.keys.len() - 1].clone())
        }
    }
}

impl<
    KeyType: Clone + Send + Sync + 'static,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync,
> Debug for BPlusTreeInternalPage<KeyType, KeyComparator>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BPlusTreeInternalPage {{ page_id: {}, size: {} }}",
            self.page_id, self.size
        )
    }
}

impl<
    KeyType: Clone + Send + Sync + 'static,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync,
> PageTypeId for BPlusTreeInternalPage<KeyType, KeyComparator>
{
    const TYPE_ID: PageType = PageType::BTreeInternal;
}

impl<
    KeyType: Clone + Send + Sync + 'static,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync,
> Page for BPlusTreeInternalPage<KeyType, KeyComparator>
{
    fn new(_page_id: PageId) -> Self {
        // This implementation is not supported as we need a comparator
        // The caller should use new_with_options instead
        unimplemented!(
            "BPlusTreeInternalPage::new() is not supported. Use new_with_options() instead."
        )
    }
}

/// Trait implementation for B+ Tree pages to work with BufferPoolManager
impl<
    KeyType: Clone + Send + Sync + 'static,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync,
> PageTrait for BPlusTreeInternalPage<KeyType, KeyComparator>
{
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::from_u8(self.data[PAGE_TYPE_OFFSET]).unwrap_or(PageType::Invalid)
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }

    fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        &self.data
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        &mut self.data
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        if offset + new_data.len() > self.data.len() {
            return Err(PageError::InvalidOffset {
                offset,
                page_size: self.data.len(),
            });
        }
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        self.is_dirty = true;
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data.fill(0);
        self.keys.clear();
        self.values.clear();
        self.size = 0;
        self.is_dirty = false;
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::logger::initialize_logger;

    // Helper function to create a test comparator for integers
    fn int_comparator(a: &i32, b: &i32) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[test]
    fn test_new_page() {
        let page = BPlusTreeInternalPage::new_with_options(1, 4, int_comparator);
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_max_size(), 4);
        assert_eq!(page.get_min_size(), 2);
        assert!(!page.is_dirty());
        assert_eq!(page.get_pin_count(), 1);
    }

    #[test]
    fn test_insert_and_get_key_value() {
        initialize_logger();
        // Initialize a new internal page with maximum size 4
        let mut page = BPlusTreeInternalPage::new_with_options(1, 4, int_comparator);

        // Test initial empty state
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), None);

        // Insert first key-value pair: this sets up the first key and both left/right pointers
        assert!(page.insert_key_value(1, 2));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(1)); // Key at index 0
        assert_eq!(page.get_value_at(0), Some(2)); // First (leftmost) pointer
        assert_eq!(page.get_value_at(1), Some(2)); // Second pointer

        // Insert second key-value pair (3, 4)
        // This adds the second key and its corresponding pointer
        assert!(page.insert_key_value(3, 4));
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), Some(1)); // First key
        assert_eq!(page.get_key_at(1), Some(3)); // Second key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(2)); // Middle pointer (associated with key 1)
        assert_eq!(page.get_value_at(2), Some(4)); // Rightmost pointer (associated with key 3)

        // Insert third key-value pair (5, 6) - should maintain sorted order
        assert!(page.insert_key_value(5, 6));
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), Some(1)); // First key
        assert_eq!(page.get_key_at(1), Some(3)); // Second key
        assert_eq!(page.get_key_at(2), Some(5)); // Third key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(2)); // Second pointer
        assert_eq!(page.get_value_at(2), Some(4)); // Third pointer
        assert_eq!(page.get_value_at(3), Some(6)); // Rightmost pointer

        // Insert key between existing keys to test sorted ordering
        assert!(page.insert_key_value(4, 8));
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), Some(1)); // First key
        assert_eq!(page.get_key_at(1), Some(3)); // Second key
        assert_eq!(page.get_key_at(2), Some(4)); // Third key (inserted between 3 and 5)
        assert_eq!(page.get_key_at(3), Some(5)); // Fourth key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(2)); // Second pointer
        assert_eq!(page.get_value_at(2), Some(4)); // Third pointer
        assert_eq!(page.get_value_at(3), Some(8)); // Fourth pointer (for key 4)
        assert_eq!(page.get_value_at(4), Some(6)); // Rightmost pointer (for key 5)

        // Try to insert when page is full
        assert!(!page.insert_key_value(7, 10));
        assert_eq!(page.get_size(), 4); // Size shouldn't change
    }

    #[test]
    fn test_insert_ordering() {
        let mut page = BPlusTreeInternalPage::new_with_options(1, 4, int_comparator);

        // Insert keys in random order
        assert!(page.insert_key_value(4, 5));
        assert!(page.insert_key_value(1, 2));
        assert!(page.insert_key_value(3, 4));
        assert!(page.insert_key_value(2, 3));

        // Verify they are stored in sorted order
        assert_eq!(page.get_key_at(0), Some(1));
        assert_eq!(page.get_key_at(1), Some(2));
        assert_eq!(page.get_key_at(2), Some(3));
        assert_eq!(page.get_key_at(3), Some(4));
    }

    #[test]
    fn test_page_full() {
        let mut page = BPlusTreeInternalPage::new_with_options(1, 2, int_comparator);

        // Insert up to max size
        assert!(page.insert_key_value(1, 2));
        assert!(page.insert_key_value(2, 3));

        // Try to insert when full
        assert!(!page.insert_key_value(3, 4));
        assert_eq!(page.get_size(), 2);
    }

    #[test]
    fn test_single_item_page() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            3, // max_size
            int_comparator,
        );

        // Insert a single item
        assert!(page.insert_key_value(10, 100));
        assert!(page.is_valid());

        // Verify state
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_value_at(0), Some(100)); // Left pointer
        assert_eq!(page.get_value_at(1), Some(100)); // Right pointer

        // Finding a child for various keys
        assert_eq!(page.find_child_for_key(&5), Some(100)); // < 10, go to left pointer
        assert_eq!(page.find_child_for_key(&10), Some(100)); // = 10, go to right pointer
        assert_eq!(page.find_child_for_key(&20), Some(100)); // > 10, go to right pointer

        // Removing the only item
        assert!(page.remove_key_value_at(0));
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), None);

        // Page should still be valid, just empty
        assert!(page.is_valid());
    }

    #[test]
    fn test_capacity_limit() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            4, // max_size (4 keys maximum)
            int_comparator,
        );

        // First insertion sets up key and both pointers
        assert!(page.insert_key_value(10, 100));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // Rightmost pointer

        // Additional insertions
        assert!(page.insert_key_value(20, 200));
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // Middle pointer
        assert_eq!(page.get_value_at(2), Some(200)); // Rightmost pointer

        assert!(page.insert_key_value(30, 300));
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(30));
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // After key 10
        assert_eq!(page.get_value_at(2), Some(200)); // After key 20
        assert_eq!(page.get_value_at(3), Some(300)); // Rightmost pointer

        assert!(page.insert_key_value(40, 400));
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(30));
        assert_eq!(page.get_key_at(3), Some(40));
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // After key 10
        assert_eq!(page.get_value_at(2), Some(200)); // After key 20
        assert_eq!(page.get_value_at(3), Some(300)); // After key 30
        assert_eq!(page.get_value_at(4), Some(400)); // Rightmost pointer

        // Page should be at max capacity
        assert_eq!(page.get_size(), 4);

        // Additional insertions should fail
        assert!(!page.insert_key_value(50, 500));
        assert_eq!(page.get_size(), 4); // Size should remain unchanged

        // Existing elements should be unaffected
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(30));
        assert_eq!(page.get_key_at(3), Some(40));

        assert!(page.is_valid());
    }

    #[test]
    fn test_basic_insertion_order() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // First insertion (sets key and both child pointers)
        assert!(page.insert_key_value(50, 100));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(50));
        assert_eq!(page.get_value_at(0), Some(100)); // Left pointer
        assert_eq!(page.get_value_at(1), Some(100)); // Right pointer

        // Second insertion (adds second key and third pointer)
        assert!(page.insert_key_value(60, 200));
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), Some(50));
        assert_eq!(page.get_key_at(1), Some(60));
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // Middle pointer
        assert_eq!(page.get_value_at(2), Some(200)); // Rightmost pointer

        // Third insertion
        assert!(page.insert_key_value(70, 300));
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), Some(50));
        assert_eq!(page.get_key_at(1), Some(60));
        assert_eq!(page.get_key_at(2), Some(70));
        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(100));
        assert_eq!(page.get_value_at(2), Some(200));
        assert_eq!(page.get_value_at(3), Some(300));

        assert!(page.is_valid());
    }

    #[test]
    fn test_find_child_for_key() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // Set up a page with keys [20, 50, 80]
        assert!(page.insert_key_value(50, 500)); // First insertion (leftmost pointer)
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(80, 800));

        // Page structure:
        // [ptr=500] [key=20] [ptr=200] [key=50] [ptr=500] [key=80] [ptr=800]

        // Test finding the appropriate child for different keys
        assert_eq!(page.find_child_for_key(&10), Some(500)); // < 20, so leftmost
        assert_eq!(page.find_child_for_key(&20), Some(200)); // = 20
        assert_eq!(page.find_child_for_key(&35), Some(200)); // Between 20 and 50
        assert_eq!(page.find_child_for_key(&50), Some(500)); // = 50
        assert_eq!(page.find_child_for_key(&65), Some(500)); // Between 50 and 80
        assert_eq!(page.find_child_for_key(&80), Some(800)); // = 80
        assert_eq!(page.find_child_for_key(&90), Some(800)); // > 80, so rightmost

        assert!(page.is_valid());
    }

    #[test]
    fn test_find_key_index() {
        // initialize_logger();
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        log::info!("{}", page.visualize());

        // Create a page with keys [20, 40, 60]
        assert!(page.insert_key_value(40, 400));
        log::info!("{}", page.visualize());
        assert!(page.insert_key_value(20, 200));
        log::info!("{}", page.visualize());
        assert!(page.insert_key_value(60, 600));
        log::info!("{}", page.visualize());

        // Test finding index for existing keys
        assert_eq!(page.find_key_index(&20), 0);
        assert_eq!(page.find_key_index(&40), 1);
        assert_eq!(page.find_key_index(&60), 2);

        // Test finding index for keys that would be inserted
        assert_eq!(page.find_key_index(&10), 0); // Should be inserted before 20
        assert_eq!(page.find_key_index(&30), 1); // Should be inserted between 20 and 40
        assert_eq!(page.find_key_index(&50), 2); // Should be inserted between 40 and 60
        assert_eq!(page.find_key_index(&70), 3); // Should be inserted after 60

        assert!(page.is_valid());
    }

    #[test]
    fn test_edge_cases() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            2, // min possible max_size
            int_comparator,
        );

        // Page with minimum possible size (just leftmost pointer)
        assert!(page.insert_key_value(10, 100));
        assert_eq!(page.get_size(), 1);

        // Add one more to reach capacity
        assert!(page.insert_key_value(20, 200));
        assert_eq!(page.get_size(), 2);

        // Should be at capacity
        assert!(!page.insert_key_value(30, 300));

        // Test the min_size property
        assert_eq!(page.get_min_size(), 1); // Should be max_size/2 rounded down

        // Remove an element to test size constraints
        assert!(page.remove_key_value_at(1));
        assert_eq!(page.get_size(), 1);

        assert!(page.is_valid());
    }

    #[test]
    fn test_delete_keys() {
        // Create a new internal page with sufficient capacity
        let mut page = BPlusTreeInternalPage::new_with_options(1, 6, int_comparator);

        // Set as root node to allow fewer than min_size keys
        page.set_root_status(true);

        // Setup: Insert several key-value pairs
        assert!(page.insert_key_value(10, 100));
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(30, 300));
        assert!(page.insert_key_value(40, 400));
        assert!(page.insert_key_value(50, 500));

        // Verify initial state
        assert_eq!(page.get_size(), 5);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(30));
        assert_eq!(page.get_key_at(3), Some(40));
        assert_eq!(page.get_key_at(4), Some(50));

        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // After key 10
        assert_eq!(page.get_value_at(2), Some(200)); // After key 20
        assert_eq!(page.get_value_at(3), Some(300)); // After key 30
        assert_eq!(page.get_value_at(4), Some(400)); // After key 40
        assert_eq!(page.get_value_at(5), Some(500)); // Rightmost pointer

        assert!(page.is_valid());

        // Test 1: Remove a key from the middle (index 2 = key 30)
        assert!(page.remove_key_value_at(2));

        // Verify state after removing middle key
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), Some(10));
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(40)); // Key 30 is gone
        assert_eq!(page.get_key_at(3), Some(50));

        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100)); // After key 10
        assert_eq!(page.get_value_at(2), Some(200)); // After key 20
        assert_eq!(page.get_value_at(3), Some(400)); // After key 40 (pointer 300 is gone)
        assert_eq!(page.get_value_at(4), Some(500)); // Rightmost pointer

        assert!(page.is_valid());

        // Test 2: Remove a key from the beginning (index 0 = key 10)
        assert!(page.remove_key_value_at(0));

        // Verify state after removing first key
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), Some(20)); // Now 20 is first
        assert_eq!(page.get_key_at(1), Some(40));
        assert_eq!(page.get_key_at(2), Some(50));

        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer stays
        assert_eq!(page.get_value_at(1), Some(200)); // After key 20
        assert_eq!(page.get_value_at(2), Some(400)); // After key 40
        assert_eq!(page.get_value_at(3), Some(500)); // Rightmost pointer

        assert!(page.is_valid());

        // Test 3: Remove a key from the end (index 2 = key 50)
        assert!(page.remove_key_value_at(2));

        // Verify state after removing last key
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), Some(20));
        assert_eq!(page.get_key_at(1), Some(40));
        assert_eq!(page.get_key_at(2), None); // No key here anymore

        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(200)); // After key 20
        assert_eq!(page.get_value_at(2), Some(400)); // After key 40 (rightmost pointer)
        assert_eq!(page.get_value_at(3), None); // No pointer here anymore

        assert!(page.is_valid());

        // Test 4: Remove a key when there's only 2 left (index 1 = key 40)
        assert!(page.remove_key_value_at(1));

        // Verify state with just one key
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(20));
        assert_eq!(page.get_key_at(1), None);

        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(200)); // After key 20 (rightmost pointer)
        assert_eq!(page.get_value_at(2), None); // No third pointer

        assert!(page.is_valid());

        // Test 5: Remove the last key
        assert!(page.remove_key_value_at(0));

        // Verify state with no keys (empty page)
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);

        // For a root node with no keys, we should retain only the leftmost pointer
        assert_eq!(page.get_value_at(0), Some(100)); // Leftmost pointer is preserved
        assert_eq!(page.get_value_at(1), None); // No second pointer

        // Empty root page with one child should be valid and marked for collapse
        assert!(page.is_valid());
        assert!(page.should_collapse_root());
        assert_eq!(page.get_only_child_page_id(), Some(100));

        // Test 6: Attempt to remove from empty page
        assert!(!page.remove_key_value_at(0));
        assert_eq!(page.get_size(), 0);
        assert!(page.is_valid());
    }

    #[test]
    fn test_populate_new_root() {
        // Create a new internal page
        let mut page = BPlusTreeInternalPage::new_with_options(1, 6, int_comparator);

        // Initially the page should be empty
        assert_eq!(page.get_size(), 0);
        assert!(!page.is_root());

        // Populate the page as a new root with two children
        let left_child_id = 100;
        let separator_key = 50;
        let right_child_id = 200;

        assert!(page.populate_new_root(left_child_id, separator_key, right_child_id));

        // Verify the page is now a root with proper structure
        assert!(page.is_root());
        assert_eq!(page.get_size(), 1);

        // Should have exactly one key
        assert_eq!(page.get_key_at(0), Some(50));

        // Should have exactly two pointers
        assert_eq!(page.get_value_at(0), Some(100)); // Left pointer
        assert_eq!(page.get_value_at(1), Some(200)); // Right pointer

        // Verify the page maintains all B+ tree invariants
        assert!(page.is_valid());

        // Attempt to populate an already populated root should fail
        assert!(!page.populate_new_root(300, 75, 400));

        // State should remain unchanged
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(50));
    }

    #[test]
    fn test_borrow_and_merge_operations() {
        // Create nodes for testing with increased capacity (8 instead of 5)
        let mut node = BPlusTreeInternalPage::new_with_options(1, 8, int_comparator);

        // Set up the node with some initial keys and values
        assert!(node.insert_key_value(20, 200));
        assert!(node.insert_key_value(40, 400));
        assert_eq!(node.get_size(), 2);

        debug!(
            "After initial setup - node keys: {:?}, values: {:?}",
            node.keys, node.values
        );

        // Test getting first and last keys
        assert_eq!(node.get_first_key(), Some(20));
        assert_eq!(node.get_last_key(), Some(40));

        // Test borrowing from left sibling
        let parent_key = 15;
        let sibling_rightmost_child = 150;

        assert!(node.borrow_from_left(parent_key, sibling_rightmost_child));
        assert_eq!(node.get_size(), 3);
        assert_eq!(node.get_first_key(), Some(15)); // Parent key should now be first
        assert_eq!(node.get_value_at(0), Some(150)); // Sibling's rightmost child becomes leftmost

        debug!(
            "After borrowing from left - node keys: {:?}, values: {:?}",
            node.keys, node.values
        );

        // Test borrowing from right sibling
        let parent_key = 50;
        let sibling_leftmost_child = 600;

        assert!(node.borrow_from_right(parent_key, sibling_leftmost_child));
        assert_eq!(node.get_size(), 4);
        assert_eq!(node.get_last_key(), Some(50)); // Parent key should now be last
        assert_eq!(node.get_value_at(4), Some(600)); // Sibling's leftmost child becomes rightmost

        debug!(
            "After borrowing from right - node keys: {:?}, values: {:?}",
            node.keys, node.values
        );

        // Create a right sibling to test merge
        let mut right_sibling = BPlusTreeInternalPage::new_with_options(2, 8, int_comparator);
        assert!(right_sibling.insert_key_value(70, 700));
        assert!(right_sibling.insert_key_value(90, 900));

        debug!(
            "Right sibling - keys: {:?}, values: {:?}",
            right_sibling.keys, right_sibling.values
        );

        // Merge node with right sibling
        let parent_key = 60;

        debug!(
            "Node max_size: {}, current keys: {}, right sibling keys: {}, would exceed? {}",
            node.max_size,
            node.keys.len(),
            right_sibling.keys.len(),
            node.keys.len() + 1 + right_sibling.keys.len() > node.max_size
        );

        let result = node.merge_with_right_sibling(
            parent_key,
            right_sibling.keys.clone(),
            right_sibling.values.clone(),
        );

        debug!("Merge result: {}", result);

        assert!(result, "Merge operation should succeed");

        // Verify merged state
        assert_eq!(node.get_size(), 7); // 4 + 1 + 2 = 7 keys
        assert_eq!(node.get_key_at(4), Some(60)); // Parent key
        assert_eq!(node.get_key_at(5), Some(70)); // Right sibling's first key
        assert_eq!(node.get_key_at(6), Some(90)); // Right sibling's second key

        debug!(
            "After merge - node keys: {:?}, values: {:?}",
            node.keys, node.values
        );

        // Verify child pointers
        assert!(node.is_valid()); // Check that B+ tree invariants are maintained
    }
}
