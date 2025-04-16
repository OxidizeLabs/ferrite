use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

/// Enumeration for page types in a B+ tree.
#[derive(Debug, PartialEq, Eq)]
pub enum IndexPageType {
    InvalidIndexPage = 0,
    BPlusTreeLeafPage,
    BPlusTreeInternalPage,
}

/// The `BPlusTreePage` struct serves as a header part for each B+ tree page
/// and contains information shared by both leaf page and internal page.
///
/// The header format (size in bytes, 12 bytes in total):
/// ---------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
/// ---------------------------------------------------------
pub struct BPlusTreePage<KeyType, ValueType, KeyComparator> {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    page_type: IndexPageType,
    size: usize,
    max_size: usize,
    _marker: PhantomData<(KeyType, ValueType, KeyComparator)>,
}

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync>
    BPlusTreePage<KeyType, ValueType, KeyComparator>
{
    /// Prevents creating BPlusTreePage instances directly.
    pub fn new(page_type: IndexPageType, max_size: usize) -> Self {
        Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: false,
            page_type,
            size: 0,
            max_size,
            _marker: PhantomData,
        }
    }

    /// Returns true if this is a leaf page.
    pub fn is_leaf_page(&self) -> bool {
        self.page_type == IndexPageType::BPlusTreeLeafPage
    }

    /// Sets the page type of this B+ tree page.
    pub fn set_page_type(&mut self, page_type: IndexPageType) {
        self.page_type = page_type;
        self.is_dirty = true;
    }

    /// Gets the current size of this page.
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Sets the size of this page.
    pub fn set_size(&mut self, size: usize) {
        assert!(size <= self.max_size, "Size cannot exceed max_size.");
        self.size = size;
        self.is_dirty = true;
    }

    /// Increases the size of this page by a specified amount.
    pub fn increase_size(&mut self, amount: usize) {
        assert!(
            self.size + amount <= self.max_size,
            "Exceeding max size of the page."
        );
        self.size += amount;
        self.is_dirty = true;
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
        self.is_dirty = true;
    }

    /// Serializes the page header into bytes
    pub fn serialize_header(&self, buffer: &mut [u8]) {
        // Format: PageType (4) | CurrentSize (4) | MaxSize (4)
        let page_type_value = match self.page_type {
            IndexPageType::InvalidIndexPage => 0u32,
            IndexPageType::BPlusTreeLeafPage => 1u32,
            IndexPageType::BPlusTreeInternalPage => 2u32,
        };

        // Write page type (4 bytes)
        buffer[0..4].copy_from_slice(&page_type_value.to_le_bytes());

        // Write current size (4 bytes)
        buffer[4..8].copy_from_slice(&(self.size as u32).to_le_bytes());

        // Write max size (4 bytes)
        buffer[8..12].copy_from_slice(&(self.max_size as u32).to_le_bytes());
    }

    /// Deserializes the page header from bytes
    pub fn deserialize_header(&mut self, buffer: &[u8]) {
        // Read page type (4 bytes)
        let page_type_value = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        self.page_type = match page_type_value {
            0 => IndexPageType::InvalidIndexPage,
            1 => IndexPageType::BPlusTreeLeafPage,
            2 => IndexPageType::BPlusTreeInternalPage,
            _ => IndexPageType::InvalidIndexPage, // Default for invalid values
        };

        // Read current size (4 bytes)
        self.size = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;

        // Read max size (4 bytes)
        self.max_size = u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize;
    }
}

impl<
        KeyType: Send + Sync + 'static,
        ValueType: Send + Sync + 'static,
        KeyComparator: Send + Sync + 'static,
    > PageTrait for BPlusTreePage<KeyType, ValueType, KeyComparator>
{
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        match self.page_type {
            IndexPageType::BPlusTreeLeafPage => PageType::BTreeLeaf,
            IndexPageType::BPlusTreeInternalPage => PageType::BTreeInternal,
            IndexPageType::InvalidIndexPage => PageType::Invalid,
        }
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
        self.page_id = INVALID_PAGE_ID;
        self.size = 0;
        self.max_size = 0;
        self.page_type = IndexPageType::InvalidIndexPage;
        self.is_dirty = false;
        self.pin_count = 0;
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<
        KeyType: Send + Sync + 'static,
        ValueType: Send + Sync + 'static,
        KeyComparator: Send + Sync + 'static,
    > Page for BPlusTreePage<KeyType, ValueType, KeyComparator>
{
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            page_type: IndexPageType::InvalidIndexPage,
            size: 0,
            max_size: 0,
            _marker: PhantomData,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }
}

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync> PageTypeId
    for BPlusTreePage<KeyType, ValueType, KeyComparator>
{
    const TYPE_ID: PageType = PageType::BTreeLeaf;
}

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync> Debug
    for BPlusTreePage<KeyType, ValueType, KeyComparator>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BPlusTreePage")
            .field("page_id", &self.page_id)
            .field("page_type", &self.page_type)
            .field("size", &self.size)
            .field("max_size", &self.max_size)
            .field("pin_count", &self.pin_count)
            .field("is_dirty", &self.is_dirty)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dummy types for testing
    #[derive(Debug)]
    struct TestKey;
    #[derive(Debug)]
    struct TestValue;
    #[derive(Debug)]
    struct TestComparator;

    fn create_test_page() -> BPlusTreePage<TestKey, TestValue, TestComparator> {
        BPlusTreePage::new(IndexPageType::BPlusTreeLeafPage, 100)
    }

    #[test]
    fn test_page_creation() {
        let page = create_test_page();
        assert_eq!(page.get_page_type(), PageType::BTreeLeaf);
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_max_size(), 100);
        assert_eq!(page.get_min_size(), 50);
        assert!(!page.is_dirty());
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn test_page_type_operations() {
        let mut page = create_test_page();

        // Test leaf page type
        page.set_page_type(IndexPageType::BPlusTreeLeafPage);
        assert!(page.is_leaf_page());
        assert_eq!(page.get_page_type(), PageType::BTreeLeaf);

        // Test internal page type
        page.set_page_type(IndexPageType::BPlusTreeInternalPage);
        assert!(!page.is_leaf_page());
        assert_eq!(page.get_page_type(), PageType::BTreeInternal);
    }

    #[test]
    fn test_size_operations() {
        let mut page = create_test_page();

        // Test setting size
        page.set_size(50);
        assert_eq!(page.get_size(), 50);

        // Test increasing size
        page.increase_size(30);
        assert_eq!(page.get_size(), 80);

        // Test setting max size
        page.set_max_size(200);
        assert_eq!(page.get_max_size(), 200);
        assert_eq!(page.get_min_size(), 100);
    }

    #[test]
    #[should_panic(expected = "Size cannot exceed max_size")]
    fn test_size_exceed_max() {
        let mut page = create_test_page();
        page.set_size(150); // Should panic as it exceeds max_size of 100
    }

    #[test]
    #[should_panic(expected = "Exceeding max size of the page")]
    fn test_increase_size_exceed_max() {
        let mut page = create_test_page();
        page.increase_size(150); // Should panic as it would exceed max_size
    }

    #[test]
    fn test_pin_count_operations() {
        let mut page = create_test_page();

        // Test increment
        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 1);

        // Test decrement
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 0);

        // Test decrement below zero
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn test_dirty_flag() {
        let mut page = create_test_page();

        // Test setting dirty flag
        page.set_dirty(true);
        assert!(page.is_dirty());

        page.set_dirty(false);
        assert!(!page.is_dirty());
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut original_page = create_test_page();
        original_page.set_page_type(IndexPageType::BPlusTreeLeafPage);
        original_page.set_size(50);
        original_page.set_max_size(100);

        // Create a buffer for serialization
        let mut buffer = [0u8; 12];

        // Test serialization
        original_page.serialize_header(&mut buffer);

        // Create a new page for deserialization
        let mut new_page = create_test_page();

        // Test deserialization
        new_page.deserialize_header(&buffer);

        // Verify all fields match
        assert_eq!(new_page.get_page_type(), original_page.get_page_type());
        assert_eq!(new_page.get_size(), original_page.get_size());
        assert_eq!(new_page.get_max_size(), original_page.get_max_size());
    }

    #[test]
    fn test_page_data_operations() {
        let mut page = create_test_page();
        let test_data = [1, 2, 3, 4, 5];

        // Test setting data
        assert!(page.set_data(0, &test_data).is_ok());
        assert_eq!(&page.get_data()[0..5], &test_data);
        assert!(page.is_dirty());

        // Test setting data with invalid offset
        assert!(page
            .set_data(DB_PAGE_SIZE as usize - 4, &[1, 2, 3, 4, 5])
            .is_err());
    }

    #[test]
    fn test_reset_memory() {
        let mut page = create_test_page();

        // Set some initial state
        page.set_page_type(IndexPageType::BPlusTreeLeafPage);
        page.set_size(50);
        page.set_dirty(true);
        page.increment_pin_count();

        // Reset memory
        page.reset_memory();

        // Verify reset state
        assert_eq!(page.get_page_type(), PageType::Invalid);
        assert_eq!(page.get_size(), 0);
        assert_eq!(page.get_max_size(), 0);
        assert!(!page.is_dirty());
        assert_eq!(page.get_pin_count(), 0);
        assert_eq!(page.get_page_id(), INVALID_PAGE_ID);
    }
}
