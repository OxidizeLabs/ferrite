use std::any::Any;
use std::fmt::{Debug, Formatter};
use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};

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

impl<KeyType: Clone + Sync + Send + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + Sync + Send + 'static>
BPlusTreeInternalPage<KeyType, KeyComparator> {
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

    /// Get the key at the given index
    pub fn get_key_at(&self, index: usize) -> Option<&KeyType> {
        if index < self.size {
            Some(&self.keys[index])
        } else {
            None
        }
    }

    /// Get the value/child page id at the given index
    pub fn get_value_at(&self, index: usize) -> Option<PageId> {
        if index <= self.size { // Internal nodes have n+1 children for n keys
            Some(self.values[index])
        } else {
            None
        }
    }

    /// Set the key at the given index
    pub fn set_key_at(&mut self, index: usize, key: KeyType) -> bool {
        if index < self.size {
            self.keys[index] = key;
            true
        } else {
            false
        }
    }

    /// Set the value/child page id at the given index
    pub fn set_value_at(&mut self, index: usize, value: PageId) -> bool {
        if index <= self.size { // Internal nodes have n+1 children for n keys
            self.values[index] = value;
            true
        } else {
            false
        }
    }

    /// Find the index of the first key greater than or equal to the target key
    pub fn find_key_index(&self, key: &KeyType) -> usize {
        match self.keys[0..self.size].binary_search_by(|k| (self.comparator)(k, key)) {
            Ok(index) => index, // Exact match
            Err(index) => index, // Insert position
        }
    }

    /// Insert a new key-value pair into the internal page
    pub fn insert_key_value(&mut self, key: KeyType, value: PageId) -> bool {
        if self.size >= self.max_size {
            return false; // Page is full
        }

        let index = self.find_key_index(&key);

        // Insert key at appropriate position
        self.keys.insert(index, key);

        // For internal nodes, value is the right child pointer
        self.values.insert(index + 1, value);

        // Increase page size
        self.size += 1;

        true
    }

    /// Remove a key and its corresponding value (child pointer) from the internal page
    pub fn remove_key_value_at(&mut self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }

        // Remove key
        self.keys.remove(index);

        // Remove child pointer (value)
        self.values.remove(index + 1); // +1 because values are offset by 1

        // Decrease page size
        self.size -= 1;

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
        self.size = u32::from_le_bytes([
            buffer[4], buffer[5], buffer[6], buffer[7]
        ]) as usize;
        self.max_size = u32::from_le_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11]
        ]) as usize;

        // Deserialize the keys and values
        // This would depend on the actual KeyType and ValueType

        // The actual deserialization would read the keys and values
        // from their respective sections in the buffer
    }
}

impl<KeyType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
Debug for BPlusTreeInternalPage<KeyType, KeyComparator> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BPlusTreeInternalPage {{ page_id: {}, size: {} }}", 
            self.page_id, self.size)
    }
}

impl<KeyType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
PageTypeId for BPlusTreeInternalPage<KeyType, KeyComparator> {
    const TYPE_ID: PageType = PageType::BTreeInternal;
}

impl<KeyType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
Page for BPlusTreeInternalPage<KeyType, KeyComparator> {
    fn new(page_id: PageId) -> Self {
        // This implementation is not supported as we need a comparator
        // The caller should use new_with_options instead
        unimplemented!("BPlusTreeInternalPage::new() is not supported. Use new_with_options() instead.")
    }
}

/// Trait implementation for B+ Tree pages to work with BufferPoolManager
impl<KeyType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
PageTrait for BPlusTreeInternalPage<KeyType, KeyComparator> {
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
