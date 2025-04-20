use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};
use log::debug;
use std::any::Any;
use std::fmt::{Debug, Formatter};

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
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }

    /// Insert a new key-value pair into the internal page
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

        // First insertion case: only add the leftmost pointer
        if self.size == 0 {
            debug!(
                "First insertion in page {}: adding leftmost pointer {}",
                self.page_id, value
            );
            self.values.push(value);
            self.size += 1;
            debug!(
                "After first insertion - size: {}, values: {:?}",
                self.size, self.values
            );
            return true;
        }

        // Find the correct position to maintain sorted order
        let pos = self.find_key_index(&key);
        debug!("Found insertion position {} for key {:?}", pos, key);

        // Insert the key at the correct position
        self.keys.insert(pos, key);

        // Insert the value pointer at position+1
        // This maintains the B+ tree property where:
        // - values[0] is the leftmost pointer (no associated key)
        // - values[i+1] corresponds to keys[i] for all other positions
        self.values.insert(pos + 1, value);

        self.size += 1;
        debug!(
            "After insertion - size: {}, keys: {:?}, values: {:?}",
            self.size, self.keys, self.values
        );
        true
    }

    /// Get key at the specified index
    pub fn get_key_at(&self, index: usize) -> Option<KeyType> {
        debug!("Retrieving key at logical index {}", index);

        // Index 0 has no key (it's the leftmost pointer)
        if index == 0 || index > self.size - 1 {
            debug!(
                "Index {} is invalid for keys (size: {}), returning None",
                index, self.size
            );
            return None;
        }

        // Adjust index to access keys array (subtract 1)
        // since keys array is 0-indexed but conceptually keys start at index 1
        let adjusted_index = index - 1;
        debug!("Adjusted index for physical array: {}", adjusted_index);
        debug!(
            "Keys array: {:?}, returning: {:?}",
            self.keys,
            self.keys.get(adjusted_index)
        );

        Some(self.keys[adjusted_index].clone())
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
                }
                std::cmp::Ordering::Greater => {
                    debug!("Key {:?} is greater than {:?}", key, self.keys[mid]);
                    left = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    debug!(
                        "Key {:?} is equal to {:?}, returning index {}",
                        key, self.keys[mid], mid
                    );
                    return mid;
                }
            }
        }

        debug!("Final insertion index for key {:?}: {}", key, left);
        left
    }

    /// Find child page that should contain the key
    pub fn find_child_for_key(&self, key: &KeyType) -> Option<PageId> {
        if self.size == 0 {
            return None;
        }

        // If we only have a leftmost pointer, return it
        if self.size == 1 {
            return Some(self.values[0].clone());
        }

        // Binary search to find the correct child
        let mut left = 0;
        let mut right = self.keys.len() - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            match (self.comparator)(key, &self.keys[mid]) {
                std::cmp::Ordering::Less => {
                    if mid == 0 {
                        return Some(self.values[0].clone()); // Key is less than all keys
                    }
                    right = mid - 1;
                }
                std::cmp::Ordering::Equal => {
                    return Some(self.values[mid + 1].clone()); // Exact match
                }
                std::cmp::Ordering::Greater => {
                    if mid == self.keys.len() - 1 {
                        return Some(self.values[mid + 1].clone()); // Key is greater than all keys
                    }
                    left = mid + 1;
                }
            }
        }

        // The key falls between keys[right] and keys[right+1]
        Some(self.values[left].clone())
    }

    /// Remove a key and its corresponding value (child pointer) from the internal page
    pub fn remove_key_value_at(&mut self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }

        // Remove key
        self.keys.remove(index);

        // Remove corresponding pointer (at index+1)
        self.values.remove(index + 1);

        self.size -= 1;
        self.is_dirty = true;
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
        result.push_str("┌──────────────────────────────────────────────┐\n");
        result.push_str("│ Index │     Key     │     Child Page ID      │\n");
        result.push_str("├──────────────────────────────────────────────┤\n");

        // Debug: Print the raw vectors
        let debug_keys = format!("DEBUG: Keys: {:?}", self.keys);
        let debug_values = format!("DEBUG: Values: {:?}", self.values);

        for i in 0..self.size {
            let key_str = if i == 0 {
                "     -       ".to_string()
            } else if i - 1 < self.keys.len() {
                format!("           {} ", self.keys[i - 1])
            } else {
                "     ?       ".to_string()
            };

            let value_str = if i < self.values.len() {
                format!("{:20} ", self.values[i])
            } else {
                "          ?           ".to_string()
            };

            result.push_str(&format!("│ {:^5} │ {} │ {} │\n", i, key_str, value_str));
        }

        result.push_str("└──────────────────────────────────────────────┘\n");

        // Add debug info
        result.push_str(&format!("\n{}\n{}\n", debug_keys, debug_values));

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

        // Add all pointers and keys
        dot.push_str("|{");
        for i in 0..self.size {
            if i > 0 {
                if let Some(key) = self.get_key_at(i - 1) {
                    dot.push_str(&format!("|<key{}>K:{:?}", i - 1, key));
                }
            }
            if let Some(value) = self.get_value_at(i) {
                dot.push_str(&format!("|<ptr{}>P:{}", i, value));
            }
        }
        dot.push_str("}}\"]; // end of node\n");

        dot
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

    // Helper function to create a test comparator for integers
    fn int_comparator(a: &i32, b: &i32) -> std::cmp::Ordering {
        a.cmp(b)
    }

    // Helper function to verify key-pointer relationships
    fn verify_key_pointer_relationships(
        page: &BPlusTreeInternalPage<i32, impl Fn(&i32, &i32) -> std::cmp::Ordering>,
    ) {
        // Verify we have n+1 pointers for n keys
        assert_eq!(page.values.len(), page.keys.len() + 1);

        // Verify each key's right pointer is at index + 1
        for i in 0..page.keys.len() {
            let key = page.keys[i];
            let right_pointer = page.values[i + 1];

            // Verify the key's right pointer exists
            assert!(
                right_pointer != 0,
                "Key {} should have a valid right pointer",
                key
            );
        }
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
        // Initialize a new internal page with maximum size 4
        let mut page = BPlusTreeInternalPage::new_with_options(1, 4, int_comparator);

        // Test initial empty state
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), None);

        // Insert first key-value pair: this should only add a leftmost pointer
        // In B+ tree, the first insertion sets only the leftmost pointer (no key)
        assert_eq!(page.insert_key_value(1, 2), true);
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), None); // Index 0 has no key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer is value 2

        // Insert second key-value pair (3, 4)
        // This adds the first key (3) and its corresponding pointer (4)
        assert_eq!(page.insert_key_value(3, 4), true);
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), None); // Index 0 still has no key
        assert_eq!(page.get_key_at(1), Some(3)); // First key at index 1
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer still 2
        assert_eq!(page.get_value_at(1), Some(4)); // Second pointer is 4

        // Insert third key-value pair (5, 6) - should maintain sorted order
        assert_eq!(page.insert_key_value(5, 6), true);
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), None); // Index 0 still has no key
        assert_eq!(page.get_key_at(1), Some(3)); // First key
        assert_eq!(page.get_key_at(2), Some(5)); // Second key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(4)); // Second pointer
        assert_eq!(page.get_value_at(2), Some(6)); // Third pointer

        // Insert key between existing keys to test sorted ordering
        assert_eq!(page.insert_key_value(4, 8), true);
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), None); // Index 0 still has no key
        assert_eq!(page.get_key_at(1), Some(3)); // First key
        assert_eq!(page.get_key_at(2), Some(4)); // Second key (inserted between 3 and 5)
        assert_eq!(page.get_key_at(3), Some(5)); // Third key
        assert_eq!(page.get_value_at(0), Some(2)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(4)); // Second pointer
        assert_eq!(page.get_value_at(2), Some(8)); // Third pointer (for key 4)
        assert_eq!(page.get_value_at(3), Some(6)); // Fourth pointer (for key 5)

        // Try to insert when page is full
        assert_eq!(page.insert_key_value(7, 10), false);
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

        // Verify key-pointer relationships
        verify_key_pointer_relationships(&page);

        // Verify they are stored in sorted order
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(1));
        assert_eq!(page.get_key_at(2), Some(2));
        assert_eq!(page.get_key_at(3), Some(3));
    }

    #[test]
    fn test_page_full() {
        let mut page = BPlusTreeInternalPage::new_with_options(1, 2, int_comparator);

        // Insert up to max size
        assert!(page.insert_key_value(1, 2));
        verify_key_pointer_relationships(&page);
        assert!(page.insert_key_value(2, 3));
        verify_key_pointer_relationships(&page);

        // Try to insert when full
        assert!(!page.insert_key_value(3, 4));
        assert_eq!(page.get_size(), 2);
    }

    #[test]
    fn test_b_plus_tree_internal_page() {
        let mut page = BPlusTreeInternalPage::new_with_options(1, 5, int_comparator);

        // Test initial state
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), None);

        // First insert (should only add leftmost pointer, no key)
        assert!(page.insert_key_value(5, 100));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), Some(100));

        // Second insert (adds first key and second pointer)
        assert!(page.insert_key_value(10, 200));
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(10));
        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(200));

        // Third insert (adds second key and third pointer)
        assert!(page.insert_key_value(15, 300));
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(10));
        assert_eq!(page.get_key_at(2), Some(15));
        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(200));
        assert_eq!(page.get_value_at(2), Some(300));

        // Insert in the middle (keys should stay sorted)
        assert!(page.insert_key_value(7, 400));
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(7));
        assert_eq!(page.get_key_at(2), Some(10));
        assert_eq!(page.get_key_at(3), Some(15));
        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(400));
        assert_eq!(page.get_value_at(2), Some(200));
        assert_eq!(page.get_value_at(3), Some(300));

        // Test finding child for a key
        assert_eq!(page.find_child_for_key(&3), Some(100)); // 3 < 7, so leftmost
        assert_eq!(page.find_child_for_key(&7), Some(400)); // Equal to 7
        assert_eq!(page.find_child_for_key(&8), Some(400)); // 7 <= 8 < 10
        assert_eq!(page.find_child_for_key(&12), Some(200)); // 10 <= 12 < 15
        assert_eq!(page.find_child_for_key(&20), Some(300)); // 20 >= 15, so rightmost
    }
}

#[cfg(test)]
mod more_tests {
    use super::*;
    use std::cmp::Ordering;

    fn int_comparator(a: &i32, b: &i32) -> Ordering {
        a.cmp(b)
    }

    fn verify_internal_node_invariants<C>(page: &BPlusTreeInternalPage<i32, C>) -> bool
    where
        C: Fn(&i32, &i32) -> Ordering + Send + Sync + 'static + Clone,
    {
        // Check size constraints
        if page.get_size() == 0 {
            return true; // Empty page is valid
        }

        // Check that size matches actual elements
        if page.get_size() != page.values.len() {
            println!(
                "Size mismatch: get_size()={}, values.len()={}",
                page.get_size(),
                page.values.len()
            );
            return false;
        }

        // Check key count is exactly one less than value count
        if page.get_size() > 0 && page.keys.len() != page.values.len() - 1 {
            println!(
                "Key-value relationship violated: keys.len()={}, values.len()={}",
                page.keys.len(),
                page.values.len()
            );
            return false;
        }

        // Check keys are in sorted order
        for i in 1..page.keys.len() {
            let prev_key = &page.keys[i - 1];
            let curr_key = &page.keys[i];
            if (page.comparator)(prev_key, curr_key) != Ordering::Less {
                println!("Keys not in sorted order at indices {} and {}", i - 1, i);
                return false;
            }
        }

        true
    }

    #[test]
    fn test_basic_insertion_order() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // First insertion (leftmost pointer only)
        assert!(page.insert_key_value(50, 100));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), Some(100));

        // Sequential insertions in ascending order
        assert!(page.insert_key_value(60, 200));
        assert!(page.insert_key_value(70, 300));
        assert!(page.insert_key_value(80, 400));

        // Check final state
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(60));
        assert_eq!(page.get_key_at(2), Some(70));
        assert_eq!(page.get_key_at(3), Some(80));

        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(200));
        assert_eq!(page.get_value_at(2), Some(300));
        assert_eq!(page.get_value_at(3), Some(400));

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_insertion_in_reverse_order() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // First insertion (leftmost pointer only)
        assert!(page.insert_key_value(80, 400));

        // Sequential insertions in descending order
        assert!(page.insert_key_value(70, 300));
        assert!(page.insert_key_value(60, 200));
        assert!(page.insert_key_value(50, 100));

        // Check final state - should be in ascending key order regardless
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(50));
        assert_eq!(page.get_key_at(2), Some(60));
        assert_eq!(page.get_key_at(3), Some(70));
        assert_eq!(page.get_key_at(4), Some(80));

        // Values should match their corresponding keys
        assert_eq!(page.get_value_at(0), Some(400)); // Leftmost pointer (from first insert)
        assert_eq!(page.get_value_at(1), Some(100));
        assert_eq!(page.get_value_at(2), Some(200));
        assert_eq!(page.get_value_at(3), Some(300));
        assert_eq!(page.get_value_at(4), Some(400));

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_insertion_with_random_order() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            6, // max_size
            int_comparator,
        );

        // First insertion (leftmost pointer only)
        assert!(page.insert_key_value(30, 300));

        // Random order insertions
        assert!(page.insert_key_value(50, 500));
        assert!(page.insert_key_value(10, 100));
        assert!(page.insert_key_value(40, 400));
        assert!(page.insert_key_value(20, 200));

        // Check final state - should be in ascending key order
        assert_eq!(page.get_size(), 5);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(10));
        assert_eq!(page.get_key_at(2), Some(20));
        assert_eq!(page.get_key_at(3), Some(30));
        assert_eq!(page.get_key_at(4), Some(40));
        assert_eq!(page.get_key_at(5), Some(50));

        // Values should correspond to keys
        assert_eq!(page.get_value_at(0), Some(300)); // Leftmost pointer
        assert_eq!(page.get_value_at(1), Some(100));
        assert_eq!(page.get_value_at(2), Some(200));
        assert_eq!(page.get_value_at(3), Some(300));
        assert_eq!(page.get_value_at(4), Some(400));
        assert_eq!(page.get_value_at(5), Some(500));

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_capacity_limit() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            4, // max_size (4 children maximum)
            int_comparator,
        );

        // Fill the page to capacity
        assert!(page.insert_key_value(10, 100)); // First insertion (leftmost)
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(30, 300));
        assert!(page.insert_key_value(40, 400));

        // Page should be at max capacity
        assert_eq!(page.get_size(), 4);

        // Additional insertions should fail
        assert!(!page.insert_key_value(50, 500));
        assert_eq!(page.get_size(), 4); // Size should remain unchanged

        // Existing elements should be unaffected
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(30));
        assert_eq!(page.get_key_at(3), Some(40));

        assert!(verify_internal_node_invariants(&page));
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

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_remove_key_value() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            6, // max_size
            int_comparator,
        );

        // Set up a page with multiple key-value pairs
        assert!(page.insert_key_value(10, 100)); // First insertion (leftmost pointer)
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(30, 300));
        assert!(page.insert_key_value(40, 400));
        assert!(page.insert_key_value(50, 500));

        // Initial state: [ptr=100] [key=20] [ptr=200] [key=30] [ptr=300] [key=40] [ptr=400] [key=50] [ptr=500]
        assert_eq!(page.get_size(), 5);

        // Remove the middle key-value pair (at index 2)
        assert!(page.remove_key_value_at(2));

        // After removal: [ptr=100] [key=20] [ptr=200] [key=40] [ptr=400] [key=50] [ptr=500]
        assert_eq!(page.get_size(), 4);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(20));
        assert_eq!(page.get_key_at(2), Some(40));
        assert_eq!(page.get_key_at(3), Some(50));

        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(200));
        assert_eq!(page.get_value_at(2), Some(400));
        assert_eq!(page.get_value_at(3), Some(500));

        // Remove first key-value pair (at index 1)
        assert!(page.remove_key_value_at(1));

        // After removal: [ptr=100] [key=40] [ptr=400] [key=50] [ptr=500]
        assert_eq!(page.get_size(), 3);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(40));
        assert_eq!(page.get_key_at(2), Some(50));

        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(400));
        assert_eq!(page.get_value_at(2), Some(500));

        // Remove last key-value pair (at index 2)
        assert!(page.remove_key_value_at(2));

        // After removal: [ptr=100] [key=40] [ptr=400]
        assert_eq!(page.get_size(), 2);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_key_at(1), Some(40));

        assert_eq!(page.get_value_at(0), Some(100));
        assert_eq!(page.get_value_at(1), Some(400));

        // Invalid index removal should fail
        assert!(!page.remove_key_value_at(5)); // Out of bounds
        assert_eq!(page.get_size(), 2); // Size unchanged

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_single_item_page() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            3, // max_size
            int_comparator,
        );

        // Insert only a single item (leftmost pointer)
        assert!(page.insert_key_value(10, 100));

        // Verify state
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), Some(100));

        // Finding a child should always return the leftmost pointer
        assert_eq!(page.find_child_for_key(&5), Some(100));
        assert_eq!(page.find_child_for_key(&10), Some(100));
        assert_eq!(page.find_child_for_key(&20), Some(100));

        // Removing the only item
        assert!(page.remove_key_value_at(0));
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_key_at(0), None);
        assert_eq!(page.get_value_at(0), None);

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_duplicate_keys() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // Insert initial keys
        assert!(page.insert_key_value(10, 100));
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(30, 300));

        // Try to insert a duplicate key
        // Behavior depends on implementation - might reject or replace
        // This test assumes rejection (returns false)
        let result = page.insert_key_value(20, 250);

        // If implementation rejects duplicates:
        if !result {
            assert_eq!(page.get_size(), 3);
            assert_eq!(page.get_key_at(2), Some(30));
            assert_eq!(page.get_value_at(2), Some(300));
        }
        // If implementation replaces existing entries:
        else {
            assert_eq!(page.get_size(), 3);
            assert_eq!(page.get_key_at(1), Some(20));
            assert_eq!(page.get_value_at(1), Some(250)); // Value should be updated
        }

        assert!(verify_internal_node_invariants(&page));
    }

    #[test]
    fn test_find_key_index() {
        let mut page = BPlusTreeInternalPage::new_with_options(
            1, // page_id
            5, // max_size
            int_comparator,
        );

        // Create a page with keys [20, 40, 60]
        assert!(page.insert_key_value(40, 400));
        assert!(page.insert_key_value(20, 200));
        assert!(page.insert_key_value(60, 600));

        // Test finding index for existing keys
        assert_eq!(page.find_key_index(&20), 0);
        assert_eq!(page.find_key_index(&40), 1);
        assert_eq!(page.find_key_index(&60), 2);

        // Test finding index for keys that would be inserted
        assert_eq!(page.find_key_index(&10), 0); // Should be inserted before 20
        assert_eq!(page.find_key_index(&30), 1); // Should be inserted between 20 and 40
        assert_eq!(page.find_key_index(&50), 2); // Should be inserted between 40 and 60
        assert_eq!(page.find_key_index(&70), 3); // Should be inserted after 60

        assert!(verify_internal_node_invariants(&page));
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

        assert!(verify_internal_node_invariants(&page));
    }
}
