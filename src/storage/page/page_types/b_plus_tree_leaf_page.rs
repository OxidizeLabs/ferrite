use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::storage::page::page_types::b_plus_tree_page::{BPlusTreePage, IndexPageType};

/// Represents a pair of key and value in the leaf node
pub type MappingType<K, V> = (K, V);

/// Size of the leaf page header in bytes
pub const LEAF_PAGE_HEADER_SIZE: usize = 16;

/// B+ tree leaf page that stores indexed keys and record values.
///
/// Leaf page format (keys are stored in order):
/// -----------------------------------------------------------------------
/// | HEADER | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)  |
/// -----------------------------------------------------------------------
///
/// Header format (size in bytes, 16 bytes in total):
/// -----------------------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4) | ... |
/// -----------------------------------------------------------------------
pub struct BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    /// Common B+ tree page header
    page: BPlusTreePage<KeyType, ValueType, KeyComparator>,
    /// Page ID of the next leaf page
    next_page_id: PageId,
    /// Array of key-value pairs
    array: Vec<MappingType<KeyType, ValueType>>,
}

impl<KeyType, ValueType, KeyComparator> BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> {
    /// Creates a new leaf page
    pub fn new(max_size: usize) -> Self {
        let mut page = Self {
            page: BPlusTreePage::new(IndexPageType::LeafPage, max_size),
            next_page_id: INVALID_PAGE_ID,
            array: Vec::with_capacity(max_size),
        };
        page.array.resize_with(max_size, Default::default);
        page
    }

    /// Initializes a leaf page with the specified maximum size
    pub fn init(&mut self, max_size: usize) {
        self.page.set_page_type(IndexPageType::LeafPage);
        self.page.set_size(0);
        self.page.set_max_size(max_size);
        self.next_page_id = INVALID_PAGE_ID;
        self.array.clear();
        self.array.resize_with(max_size, Default::default);
    }

    /// Returns the number of key-value pairs currently stored in this leaf page
    pub fn get_size(&self) -> usize {
        self.page.get_size()
    }

    /// Returns the maximum number of key-value pairs this leaf page can store
    pub fn get_max_size(&self) -> usize {
        self.page.get_max_size()
    }

    /// Gets the minimum number of key-value pairs that must be stored in this leaf page
    pub fn get_min_size(&self) -> usize {
        self.page.get_min_size()
    }

    /// Returns the page ID of the next leaf page
    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Sets the page ID of the next leaf page
    pub fn set_next_page_id(&mut self, next_page_id: PageId) {
        self.next_page_id = next_page_id;
    }

    /// Returns a reference to the key at the specified index
    pub fn key_at(&self, index: usize) -> &KeyType {
        assert!(index < self.get_size(), "Index out of bounds");
        &self.array[index].0
    }

    /// Returns a reference to the value at the specified index
    pub fn value_at(&self, index: usize) -> &ValueType {
        assert!(index < self.get_size(), "Index out of bounds");
        &self.array[index].1
    }

    /// Returns a mutable reference to the value at the specified index
    pub fn value_at_mut(&mut self, index: usize) -> &mut ValueType {
        assert!(index < self.get_size(), "Index out of bounds");
        &mut self.array[index].1
    }

    /// Returns a reference to the key-value pair at the specified index
    pub fn pair_at(&self, index: usize) -> &MappingType<KeyType, ValueType> {
        assert!(index < self.get_size(), "Index out of bounds");
        &self.array[index]
    }

    /// Sets the key-value pair at the specified index
    pub fn set_pair_at(&mut self, index: usize, key: KeyType, value: ValueType) {
        assert!(index < self.get_max_size(), "Index out of bounds");
        self.array[index] = (key, value);
        if index >= self.get_size() {
            self.page.set_size(index + 1);
        }
    }

    /// Inserts a new key-value pair into this leaf page
    /// Returns the index where the pair was inserted
    pub fn insert(&mut self, key: KeyType, value: ValueType, comparator: &KeyComparator) -> usize
    where
        KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering
    {
        assert!(self.get_size() < self.get_max_size(), "Leaf page is full");

        // Find the insertion position using binary search
        let mut low = 0;
        let mut high = self.get_size();

        while low < high {
            let mid = low + (high - low) / 2;
            match comparator(self.key_at(mid), &key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => {
                    // Key already exists, return its index
                    return mid;
                }
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        // Shift all elements to the right
        let index = low;
        for i in (index..self.get_size()).rev() {
            self.array[i + 1] = self.array[i].clone();
        }

        // Insert the new key-value pair
        self.array[index] = (key, value);
        self.page.increase_size(1);

        index
    }

    /// Removes a key-value pair from this leaf page
    /// Returns true if the key was found and removed
    pub fn remove(&mut self, index: usize) -> bool {
        if index >= self.get_size() {
            return false;
        }

        // Shift all elements to the left
        for i in index..self.get_size() - 1 {
            self.array[i] = self.array[i + 1].clone();
        }

        self.page.set_size(self.get_size() - 1);
        true
    }

    /// Lookup a key and returns its corresponding value if found
    pub fn lookup(&self, key: &KeyType, comparator: &KeyComparator) -> Option<&ValueType>
    where
        KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering
    {
        // Binary search to find the key
        let mut low = 0;
        let mut high = self.get_size();

        while low < high {
            let mid = low + (high - low) / 2;
            match comparator(self.key_at(mid), key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return Some(self.value_at(mid)),
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        None
    }
}