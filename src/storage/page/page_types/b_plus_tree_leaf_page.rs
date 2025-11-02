use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::common::exception::PageError;
use crate::storage::index::types::{KeyComparator, KeyType};
use crate::storage::page::{Page, PageTrait, PageType, PageTypeId};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

/// Leaf page structure for B+ Tree
pub struct BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()>,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
{
    // Array of keys
    keys: Vec<K>,
    // Array of values (RIDs in most cases)
    values: Vec<V>,
    // Next leaf page ID for linked list of leaves
    next_page_id: Option<PageId>,
    // KeyComparator function
    comparator: C,
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
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    C: Fn(&K, &K) -> Ordering + 'static + Send + Sync,
> PageTypeId for BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()>,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
{
    const TYPE_ID: PageType = PageType::BTreeLeaf;
}

impl<K, V, C> BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()> + Debug,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
{
    pub fn new_with_options(page_id: PageId, max_size: usize, comparator: C) -> Self {
        Self {
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
            is_root: false,
        }
    }

    /// Create a new leaf page that is a root node
    pub fn new_root_page(page_id: PageId, max_size: usize, comparator: C) -> Self {
        let mut page = Self::new_with_options(page_id, max_size, comparator);
        page.is_root = true;
        page
    }

    /// Set whether this leaf page is a root node
    pub fn set_root_status(&mut self, is_root: bool) {
        self.is_root = is_root;
        self.is_dirty = true;
    }

    /// Check if this leaf page is a root node
    pub fn is_root(&self) -> bool {
        self.is_root
    }

    pub fn get_key_at(&self, index: usize) -> Option<&K> {
        if index < self.size {
            Some(&self.keys[index])
        } else {
            None
        }
    }

    pub fn get_value_at(&self, index: usize) -> Option<&V> {
        if index < self.size {
            Some(&self.values[index])
        } else {
            None
        }
    }

    pub fn set_key_at(&mut self, index: usize, key: K) -> bool {
        if index < self.size {
            self.keys[index] = key;
            self.is_dirty = true;
            true
        } else {
            false
        }
    }

    pub fn set_value_at(&mut self, index: usize, value: V) -> bool {
        if index < self.size {
            self.values[index] = value;
            self.is_dirty = true;
            true
        } else {
            false
        }
    }

    pub fn get_next_page_id(&self) -> Option<PageId> {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        self.next_page_id = page_id;
        self.is_dirty = true;
    }

    pub fn find_key_index(&self, key: &K) -> usize {
        // Binary search implementation for better performance
        let mut low = 0;
        let mut high = self.size;

        while low < high {
            let mid = low + (high - low) / 2;
            let cmp = (self.comparator)(key, &self.keys[mid]);

            if cmp == Ordering::Equal {
                return mid;
            } else if cmp == Ordering::Less {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        low
    }

    pub fn insert_key_value(&mut self, key: K, value: V) -> bool {
        if self.size >= self.max_size {
            return false;
        }

        let index = self.find_key_index(&key);

        // Check if key already exists
        if index < self.size && (self.comparator)(&key, &self.keys[index]) == Ordering::Equal {
            // Update value if key exists
            self.values[index] = value;
        } else {
            // Insert new key-value pair
            self.keys.insert(index, key);
            self.values.insert(index, value);
            self.size += 1;
        }

        self.is_dirty = true;
        true
    }

    /// Insert a key-value pair allowing temporary overflow (used during splits)
    pub fn insert_key_value_with_overflow(&mut self, key: K, value: V) -> bool {
        let index = self.find_key_index(&key);

        // Check if key already exists
        if index < self.size && (self.comparator)(&key, &self.keys[index]) == Ordering::Equal {
            // Update value if key exists
            self.values[index] = value;
        } else {
            // Insert new key-value pair (allowing overflow)
            self.keys.insert(index, key);
            self.values.insert(index, value);
            self.size += 1;
        }

        self.is_dirty = true;
        true
    }

    pub fn remove_key_value_at(&mut self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }

        self.keys.remove(index);
        self.values.remove(index);
        self.size -= 1;
        self.is_dirty = true;

        true
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    pub fn get_min_size(&self) -> usize {
        // B+ tree common practice is to keep at least half full
        self.max_size.div_ceil(2)
    }

    pub fn serialize(&self, buffer: &mut [u8]) {
        // Simple serialization using bincode for keys and values
        // Format:
        // [size: u32][max_size: u32][next_page_id: Option<PageId>][keys][values]
        let mut cursor = 0;

        // Write size
        let size_bytes = self.size.to_ne_bytes();
        buffer[cursor..cursor + size_bytes.len()].copy_from_slice(&size_bytes);
        cursor += size_bytes.len();

        // Write max_size
        let max_size_bytes = self.max_size.to_ne_bytes();
        buffer[cursor..cursor + max_size_bytes.len()].copy_from_slice(&max_size_bytes);
        cursor += max_size_bytes.len();

        // Write next_page_id
        let next_page_id_bytes =
            bincode::encode_to_vec(self.next_page_id, bincode::config::standard())
                .expect("Failed to serialize next_page_id");
        let next_page_id_len = next_page_id_bytes.len() as u32;
        buffer[cursor..cursor + 4].copy_from_slice(&next_page_id_len.to_ne_bytes());
        cursor += 4;
        buffer[cursor..cursor + next_page_id_bytes.len()].copy_from_slice(&next_page_id_bytes);
        cursor += next_page_id_bytes.len();

        // Write keys and values
        for i in 0..self.size {
            // This is a simplified approach and assumes KeyType and ValueType can be serialized
            // In a real implementation, you might need type-specific serialization logic
            let key_bytes = bincode::encode_to_vec(&self.keys[i], bincode::config::standard())
                .expect("Failed to serialize key");
            let key_len = key_bytes.len() as u32;
            buffer[cursor..cursor + 4].copy_from_slice(&key_len.to_ne_bytes());
            cursor += 4;
            buffer[cursor..cursor + key_bytes.len()].copy_from_slice(&key_bytes);
            cursor += key_bytes.len();

            let value_bytes = bincode::encode_to_vec(&self.values[i], bincode::config::standard())
                .expect("Failed to serialize value");
            let value_len = value_bytes.len() as u32;
            buffer[cursor..cursor + 4].copy_from_slice(&value_len.to_ne_bytes());
            cursor += 4;
            buffer[cursor..cursor + value_bytes.len()].copy_from_slice(&value_bytes);
            cursor += value_bytes.len();
        }
    }

    pub fn deserialize(&mut self, buffer: &[u8]) {
        let mut cursor = 0;

        // Read size
        let mut size_bytes = [0u8; size_of::<usize>()];
        size_bytes.copy_from_slice(&buffer[cursor..cursor + size_of::<usize>()]);
        self.size = usize::from_ne_bytes(size_bytes);
        cursor += size_of::<usize>();

        // Read max_size
        let mut max_size_bytes = [0u8; size_of::<usize>()];
        max_size_bytes.copy_from_slice(&buffer[cursor..cursor + size_of::<usize>()]);
        self.max_size = usize::from_ne_bytes(max_size_bytes);
        cursor += size_of::<usize>();

        // Read next_page_id
        let mut next_page_id_len_bytes = [0u8; 4];
        next_page_id_len_bytes.copy_from_slice(&buffer[cursor..cursor + 4]);
        let next_page_id_len = u32::from_ne_bytes(next_page_id_len_bytes) as usize;
        cursor += 4;
        let (next_page_id, _): (Option<PageId>, _) = bincode::decode_from_slice(
            &buffer[cursor..cursor + next_page_id_len],
            bincode::config::standard(),
        )
        .expect("Failed to deserialize next_page_id");
        self.next_page_id = next_page_id;
        cursor += next_page_id_len;

        // Clear existing keys and values
        self.keys.clear();
        self.values.clear();

        // Ensure capacity
        self.keys.reserve(self.size);
        self.values.reserve(self.size);

        // Read keys and values
        for _ in 0..self.size {
            let mut key_len_bytes = [0u8; 4];
            key_len_bytes.copy_from_slice(&buffer[cursor..cursor + 4]);
            let key_len = u32::from_ne_bytes(key_len_bytes) as usize;
            cursor += 4;
            let (key, _): (K, _) = bincode::decode_from_slice(
                &buffer[cursor..cursor + key_len],
                bincode::config::standard(),
            )
            .expect("Failed to deserialize key");
            cursor += key_len;

            let mut value_len_bytes = [0u8; 4];
            value_len_bytes.copy_from_slice(&buffer[cursor..cursor + 4]);
            let value_len = u32::from_ne_bytes(value_len_bytes) as usize;
            cursor += 4;
            let (value, _): (V, _) = bincode::decode_from_slice(
                &buffer[cursor..cursor + value_len],
                bincode::config::standard(),
            )
            .expect("Failed to deserialize value");
            cursor += value_len;

            self.keys.push(key);
            self.values.push(value);
        }
    }

    /// Validates that the leaf page maintains all B+ tree invariants
    /// Returns a Result with either () for success or a detailed error message
    pub fn check_invariants(&self) -> Result<(), String>
    where
        K: Debug,
    {
        // 1. Check size constraints
        if self.size > self.max_size {
            return Err(format!(
                "Leaf page size {} exceeds max size {}",
                self.size, self.max_size
            ));
        }

        // 2. If not root, ensure size is at least the minimum
        if !self.is_root && self.size < self.get_min_size() {
            return Err(format!(
                "Leaf page size {} is smaller than the min size {} for non-root",
                self.size,
                self.get_min_size()
            ));
        }

        // 3. Check that keys and values have the correct length matching size
        if self.keys.len() != self.size || self.values.len() != self.size {
            return Err(format!(
                "Keys/Values length ({}/{}) does not match size {}",
                self.keys.len(),
                self.values.len(),
                self.size
            ));
        }

        // 4. Check that keys are sorted according to the comparator
        for i in 1..self.size {
            let cmp = (self.comparator)(&self.keys[i - 1], &self.keys[i]);
            if cmp == Ordering::Greater {
                return Err(format!(
                    "Keys are out of order at index {} (key {:?} > key {:?})",
                    i - 1,
                    self.keys[i - 1],
                    self.keys[i]
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
}

impl<K, V, C> Debug for BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()> + Debug,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BPlusTreeLeafPage {{ page_id: {}, size: {}/{}, next_page_id: {:?}, keys: [",
            self.page_id, self.size, self.max_size, self.next_page_id
        )?;

        // Only print up to 10 keys to avoid cluttering the debug output
        let max_display = std::cmp::min(10, self.size);
        for i in 0..max_display {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", &self.keys[i])?;
        }

        if self.size > max_display {
            write!(f, ", ... ({} more)", self.size - max_display)?;
        }

        write!(f, "] }}")
    }
}

impl<K, V, C> PageTrait for BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()> + Debug,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static,
{
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::BTreeLeaf
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
        if offset + new_data.len() > DB_PAGE_SIZE as usize {
            return Err(PageError::OffsetOutOfBounds);
        }

        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        self.is_dirty = true;

        // Create a temporary copy to avoid the borrowing conflict
        let data_copy = self.data.clone();
        self.deserialize(&*data_copy);

        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.keys.clear();
        self.values.clear();
        self.next_page_id = None;
        self.size = 0;
        self.is_dirty = false;
        self.pin_count = 0;
        self.data.fill(0);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<K, V, C> Page for BPlusTreeLeafPage<K, V, C>
where
    K: Clone + Send + Sync + 'static + KeyType + bincode::Encode + bincode::Decode<()> + Debug,
    V: Clone + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
    C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
{
    fn new(_page_id: PageId) -> Self {
        // This approach requires the caller to provide the comparator
        unimplemented!("Please use new_with_options instead to provide a comparator")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test page
    fn create_test_page() -> BPlusTreeLeafPage<i32, i32, fn(&i32, &i32) -> Ordering> {
        BPlusTreeLeafPage::new_with_options(1, 4, i32::cmp)
    }

    #[test]
    fn test_new_page() {
        let page = create_test_page();
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_max_size(), 4);
        assert_eq!(page.get_min_size(), 2);
        assert_eq!(page.get_pin_count(), 1);
        assert!(!page.is_dirty());
    }

    #[test]
    fn test_insert_and_get() {
        let mut page = create_test_page();

        // Test inserting values
        assert!(page.insert_key_value(1, 100));
        assert!(page.insert_key_value(2, 200));
        assert!(page.insert_key_value(3, 300));

        // Test getting values
        assert_eq!(page.get_key_at(0), Some(&1));
        assert_eq!(page.get_value_at(0), Some(&100));
        assert_eq!(page.get_key_at(1), Some(&2));
        assert_eq!(page.get_value_at(1), Some(&200));
        assert_eq!(page.get_key_at(2), Some(&3));
        assert_eq!(page.get_value_at(2), Some(&300));

        // Test out of bounds
        assert_eq!(page.get_key_at(3), None);
        assert_eq!(page.get_value_at(3), None);
    }

    #[test]
    fn test_set_and_remove() {
        let mut page = create_test_page();

        // Insert initial values
        page.insert_key_value(1, 100);
        page.insert_key_value(2, 200);

        // Test setting values
        assert!(page.set_key_at(0, 10));
        assert!(page.set_value_at(0, 1000));
        assert_eq!(page.get_key_at(0), Some(&10));
        assert_eq!(page.get_value_at(0), Some(&1000));

        // Test removing values
        assert!(page.remove_key_value_at(0));
        assert_eq!(page.get_size(), 1);
        assert_eq!(page.get_key_at(0), Some(&2));
        assert_eq!(page.get_value_at(0), Some(&200));
    }

    #[test]
    fn test_next_page_id() {
        let mut page = create_test_page();

        // Test setting and getting next page ID
        page.set_next_page_id(Some(2));
        assert_eq!(page.get_next_page_id(), Some(2));

        page.set_next_page_id(None);
        assert_eq!(page.get_next_page_id(), None);
    }

    #[test]
    fn test_find_key_index() {
        let mut page = create_test_page();

        // Insert sorted values
        page.insert_key_value(1, 100);
        page.insert_key_value(3, 300);
        page.insert_key_value(5, 500);

        // Test finding indices
        assert_eq!(page.find_key_index(&0), 0); // Before first key
        assert_eq!(page.find_key_index(&1), 0); // Exact match
        assert_eq!(page.find_key_index(&2), 1); // Between keys
        assert_eq!(page.find_key_index(&3), 1); // Exact match
        assert_eq!(page.find_key_index(&4), 2); // Between keys
        assert_eq!(page.find_key_index(&5), 2); // Exact match
        assert_eq!(page.find_key_index(&6), 3); // After last key
    }

    #[test]
    fn test_page_full() {
        let mut page = create_test_page();

        // Fill the page
        assert!(page.insert_key_value(1, 100));
        assert!(page.insert_key_value(2, 200));
        assert!(page.insert_key_value(3, 300));
        assert!(page.insert_key_value(4, 400));

        // Try to insert when full
        assert!(!page.insert_key_value(5, 500));
        assert_eq!(page.get_size(), 4);
    }

    #[test]
    fn test_reset_memory() {
        let mut page = create_test_page();

        // Fill the page with data
        page.insert_key_value(1, 100);
        page.insert_key_value(2, 200);
        page.set_next_page_id(Some(2));
        page.set_dirty(true);

        // Reset the page
        page.reset_memory();

        // Verify reset state
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_next_page_id(), None);
        assert!(!page.is_dirty());
        assert_eq!(page.get_page_type(), PageType::BTreeLeaf);
    }

    #[test]
    fn test_pin_count() {
        let mut page = create_test_page();

        // Test pin count operations
        assert_eq!(page.get_pin_count(), 1);
        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 2);
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 1);

        // Test pin count doesn't go below 0
        page.decrement_pin_count();
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 0);
    }
}
