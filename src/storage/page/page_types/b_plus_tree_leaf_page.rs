use std::any::Any;
use std::fmt::{Debug, Formatter};
use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};

/// Leaf page structure for B+ Tree
pub struct BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    // Array of keys
    keys: Vec<KeyType>,
    // Array of values (RIDs in most cases)
    values: Vec<ValueType>,
    // Next leaf page ID for linked list of leaves
    next_page_id: Option<PageId>,
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

impl<KeyType: Clone + Sync + Send + 'static, ValueType: Clone + Sync + Send + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + Sync + Send + 'static>
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    pub fn new_with_options(page_id: PageId, max_size: usize, comparator: KeyComparator) -> Self {
        let mut page = Self {
            keys: Vec::with_capacity(max_size),
            values: Vec::with_capacity(max_size),
            next_page_id: None,
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

    /// Get the value at the given index
    pub fn get_value_at(&self, index: usize) -> Option<&ValueType> {
        if index < self.size {
            Some(&self.values[index])
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

    /// Set the value at the given index
    pub fn set_value_at(&mut self, index: usize, value: ValueType) -> bool {
        if index < self.size {
            self.values[index] = value;
            true
        } else {
            false
        }
    }

    /// Get next leaf page id (for leaf node linked list traversal)
    pub fn get_next_page_id(&self) -> Option<PageId> {
        self.next_page_id
    }

    /// Set next leaf page id
    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        self.next_page_id = page_id;
    }

    /// Find the index of the first key greater than or equal to the target key
    pub fn find_key_index(&self, key: &KeyType) -> usize {
        match self.keys[0..self.size].binary_search_by(|k| (self.comparator)(k, key)) {
            Ok(index) => index, // Exact match
            Err(index) => index, // Insert position
        }
    }

    /// Insert a new key-value pair into the leaf page
    pub fn insert_key_value(&mut self, key: KeyType, value: ValueType) -> bool {
        if self.size >= self.max_size {
            return false; // Page is full
        }

        let index = self.find_key_index(&key);

        // Insert key and value at appropriate position
        self.keys.insert(index, key);
        self.values.insert(index, value);

        // Increase page size
        self.size += 1;

        true
    }

    /// Remove a key-value pair at the given index from the leaf page
    pub fn remove_key_value_at(&mut self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }

        // Remove key and value
        self.keys.remove(index);
        self.values.remove(index);

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

    /// Serialize the leaf page to bytes
    pub fn serialize(&self, buffer: &mut [u8]) {
        // First serialize the header (12 bytes)
        // Format: PageType (4) | CurrentSize (4) | MaxSize (4)
        buffer[0..4].copy_from_slice(&(Self::TYPE_ID as u32).to_le_bytes());
        buffer[4..8].copy_from_slice(&(self.size as u32).to_le_bytes());
        buffer[8..12].copy_from_slice(&(self.max_size as u32).to_le_bytes());

        // Serialize the next_page_id (8 bytes)
        let next_page_bytes = match self.next_page_id {
            Some(id) => id.to_le_bytes(),
            None => [0u8; 8], // 0 for None
        };
        buffer[12..20].copy_from_slice(&next_page_bytes);

        // Serialize the keys and values
        // This is a simplified version - in real implementation,
        // you would need to know the size of KeyType and ValueType and properly serialize them
    }

    /// Deserialize the leaf page from bytes
    pub fn deserialize(&mut self, buffer: &[u8]) {
        // First deserialize the header (12 bytes)
        // Format: PageType (4) | CurrentSize (4) | MaxSize (4)
        self.size = u32::from_le_bytes([
            buffer[4], buffer[5], buffer[6], buffer[7]
        ]) as usize;
        self.max_size = u32::from_le_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11]
        ]) as usize;

        // Deserialize the next_page_id (8 bytes)
        let next_page_id_bytes = [
            buffer[12], buffer[13], buffer[14], buffer[15],
            buffer[16], buffer[17], buffer[18], buffer[19],
        ];
        let next_page_id = u64::from_le_bytes(next_page_id_bytes);
        self.next_page_id = if next_page_id == 0 { None } else { Some(next_page_id) };

        // Deserialize the keys and values
        // This would depend on the actual KeyType and ValueType
    }
}

impl<KeyType: Clone + Send + Sync + 'static, ValueType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
Debug for BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BPlusTreeLeafPage {{ page_id: {}, size: {}, next_page_id: {:?} }}", 
            self.page_id, self.size, self.next_page_id)
    }
}

impl<KeyType: Clone + Send + Sync + 'static, ValueType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
PageTypeId for BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    const TYPE_ID: PageType = PageType::BTreeLeaf;
}

impl<KeyType: Clone + Send + Sync + 'static, ValueType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
PageTrait for BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
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
        self.next_page_id = None;
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

impl<KeyType: Clone + Send + Sync + 'static, ValueType: Clone + Send + Sync + 'static, KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering + 'static + Send + Sync>
Page for BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    fn new(page_id: PageId) -> Self {
        // This implementation is not supported as we need a comparator and max_size
        // The caller should use new_with_options instead
        unimplemented!("BPlusTreeLeafPage::new() is not supported. Use new_with_options() instead.")
    }
}
