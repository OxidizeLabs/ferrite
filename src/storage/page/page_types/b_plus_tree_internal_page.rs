use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::storage::page::page_types::b_plus_tree_page::{BPlusTreePage, IndexPageType};

/// Size of the internal page header in bytes
pub const INTERNAL_PAGE_HEADER_SIZE: usize = 12;

/// B+ tree internal page that stores keys and child pointers.
///
/// Internal page format (keys are stored in ascending order):
/// -----------------------------------------------------------------------
/// | HEADER | PGID(0) | KEY(1) | PGID(1) | KEY(2) | ... | KEY(n) | PGID(n) |
/// -----------------------------------------------------------------------
///
/// Header format (size in bytes, 12 bytes in total):
/// -----------------------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) | ... |
/// -----------------------------------------------------------------------
///
/// Note: For an internal page, we maintain one more page ID than key.
/// This is because each key corresponds to a pivot value that separates
/// the ranges of keys handled by the child nodes.
pub struct BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> {
    /// Common B+ tree page header
    page: BPlusTreePage<KeyType, ValueType, KeyComparator>,
    /// Array of keys
    keys: Vec<KeyType>,
    /// Array of child page IDs
    children: Vec<PageId>,
}

impl<KeyType, ValueType, KeyComparator> BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> {
    /// Creates a new internal page
    pub fn new(max_size: usize) -> Self {
        let mut page = Self {
            page: BPlusTreePage::new(IndexPageType::InternalPage, max_size),
            keys: Vec::with_capacity(max_size),
            children: Vec::with_capacity(max_size + 1),
        };
        page.keys.resize_with(max_size, Default::default);
        page.children.resize(max_size + 1, INVALID_PAGE_ID);
        page
    }

    /// Initializes an internal page with the specified maximum size
    pub fn init(&mut self, max_size: usize) {
        self.page.set_page_type(IndexPageType::InternalPage);
        self.page.set_size(0);
        self.page.set_max_size(max_size);
        self.keys.clear();
        self.keys.resize_with(max_size, Default::default);
        self.children.clear();
        self.children.resize(max_size + 1, INVALID_PAGE_ID);
    }

    /// Returns the number of keys currently stored in this internal page
    pub fn get_size(&self) -> usize {
        self.page.get_size()
    }

    /// Returns the maximum number of keys this internal page can store
    pub fn get_max_size(&self) -> usize {
        self.page.get_max_size()
    }

    /// Gets the minimum number of keys that must be stored in this internal page
    pub fn get_min_size(&self) -> usize {
        self.page.get_min_size()
    }

    /// Returns a reference to the key at the specified index
    pub fn key_at(&self, index: usize) -> &KeyType {
        assert!(index < self.get_size(), "Index out of bounds");
        &self.keys[index]
    }

    /// Sets the key at the specified index
    pub fn set_key_at(&mut self, index: usize, key: KeyType) {
        assert!(index < self.get_max_size(), "Index out of bounds");
        self.keys[index] = key;
    }

    /// Returns the child page ID at the specified index
    pub fn value_at(&self, index: usize) -> PageId {
        assert!(index <= self.get_size(), "Index out of bounds");
        self.children[index]
    }

    /// Sets the child page ID at the specified index
    pub fn set_value_at(&mut self, index: usize, value: PageId) {
        assert!(index <= self.get_max_size(), "Index out of bounds");
        self.children[index] = value;
    }

    /// Inserts a new key and right child pointer into this internal page
    /// Returns the index where the key was inserted
    pub fn insert(&mut self, key: KeyType, right_child_pgid: PageId, comparator: &KeyComparator) -> usize
    where
        KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering
    {
        assert!(self.get_size() < self.get_max_size(), "Internal page is full");

        // Find the insertion position using binary search
        let mut low = 0;
        let mut high = self.get_size();

        while low < high {
            let mid = low + (high - low) / 2;
            match comparator(&self.keys[mid], &key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => {
                    // Key already exists, update child pointer and return
                    self.children[mid + 1] = right_child_pgid;
                    return mid;
                }
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        // Shift all keys and child pointers to the right
        let index = low;
        for i in (index..self.get_size()).rev() {
            self.keys[i + 1] = self.keys[i].clone();
            self.children[i + 2] = self.children[i + 1];
        }

        // Insert the new key and right child pointer
        self.keys[index] = key;
        self.children[index + 1] = right_child_pgid;
        self.page.increase_size(1);

        index
    }

    /// Removes a key and its corresponding right child pointer from this internal page
    /// Returns true if the key was found and removed
    pub fn remove(&mut self, index: usize) -> bool {
        if index >= self.get_size() {
            return false;
        }

        // Shift all keys and child pointers to the left
        for i in index..self.get_size() - 1 {
            self.keys[i] = self.keys[i + 1].clone();
            self.children[i + 1] = self.children[i + 2];
        }

        self.page.set_size(self.get_size() - 1);
        true
    }

    /// Finds the index of the child which should contain the given key
    pub fn lookup(&self, key: &KeyType, comparator: &KeyComparator) -> usize
    where
        KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering
    {
        // Binary search to find the child index
        let mut low = 0;
        let mut high = self.get_size();

        while low < high {
            let mid = low + (high - low) / 2;
            match comparator(&self.keys[mid], key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return mid + 1, // Return the right child for exact match
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        // If we didn't find an exact match, return the appropriate child index
        low
    }

    /// Updates the first child pointer (used when this is the root page)
    pub fn populate_new_root(&mut self, first_child_pgid: PageId, key: KeyType, second_child_pgid: PageId) {
        assert_eq!(self.get_size(), 0, "Root page must be empty when populating");

        self.children[0] = first_child_pgid;
        self.keys[0] = key;
        self.children[1] = second_child_pgid;
        self.page.increase_size(1);
    }
}