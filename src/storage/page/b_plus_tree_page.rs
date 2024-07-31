use std::marker::PhantomData;

/// Enumeration for page types in a B+ tree.
#[derive(Debug, PartialEq, Eq)]
pub enum IndexPageType {
    InvalidIndexPage = 0,
    LeafPage,
    InternalPage,
}

/// The `BPlusTreePage` struct serves as a header part for each B+ tree page
/// and contains information shared by both leaf page and internal page.
///
/// The header format (size in bytes, 12 bytes in total):
/// ---------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
/// ---------------------------------------------------------
pub struct BPlusTreePage<KeyType, ValueType, KeyComparator> {
    page_type: IndexPageType,
    size: usize,
    max_size: usize,
    _marker: PhantomData<(KeyType, ValueType, KeyComparator)>,
}

impl<KeyType, ValueType, KeyComparator> BPlusTreePage<KeyType, ValueType, KeyComparator> {
    /// Prevents creating BPlusTreePage instances directly.
    fn new(page_type: IndexPageType, max_size: usize) -> Self {
        Self {
            page_type,
            size: 0,
            max_size,
            _marker: PhantomData,
        }
    }

    /// Returns true if this is a leaf page.
    pub fn is_leaf_page(&self) -> bool {
        self.page_type == IndexPageType::LeafPage
    }

    /// Sets the page type of this B+ tree page.
    pub fn set_page_type(&mut self, page_type: IndexPageType) {
        self.page_type = page_type;
    }

    /// Gets the current size of this page.
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Sets the size of this page.
    pub fn set_size(&mut self, size: usize) {
        assert!(size <= self.max_size, "Size cannot exceed max_size.");
        self.size = size;
    }

    /// Increases the size of this page by a specified amount.
    pub fn increase_size(&mut self, amount: usize) {
        assert!(self.size + amount <= self.max_size, "Exceeding max size of the page.");
        self.size += amount;
    }

    /// Gets the maximum size of this page.
    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    /// Gets the minimum size (50% of the maximum size) of this page.
    pub fn get_min_size(&self) -> usize {
        self.max_size / 2
    }

    /// Sets the size of this page.
    pub fn set_max_size(&mut self, size: usize) {
        self.max_size = size;
    }
}

// Usage of PhantomData to associate generic types with struct is common in Rust
// to ensure that these types are part of the type signature of BPlusTreePage.
