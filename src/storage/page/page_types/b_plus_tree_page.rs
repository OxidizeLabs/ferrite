use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};

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
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    page_type: IndexPageType,
    size: usize,
    max_size: usize,
    _marker: PhantomData<(KeyType, ValueType, KeyComparator)>,
}

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync> BPlusTreePage<KeyType, ValueType, KeyComparator> {
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
        self.page_type == IndexPageType::LeafPage
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
            IndexPageType::LeafPage => 1u32,
            IndexPageType::InternalPage => 2u32,
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
        let page_type_value = u32::from_le_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3]
        ]);
        self.page_type = match page_type_value {
            0 => IndexPageType::InvalidIndexPage,
            1 => IndexPageType::LeafPage,
            2 => IndexPageType::InternalPage,
            _ => IndexPageType::InvalidIndexPage, // Default for invalid values
        };

        // Read current size (4 bytes)
        self.size = u32::from_le_bytes([
            buffer[4], buffer[5], buffer[6], buffer[7]
        ]) as usize;

        // Read max size (4 bytes)
        self.max_size = u32::from_le_bytes([
            buffer[8], buffer[9], buffer[10], buffer[11]
        ]) as usize;
    }
}

impl<KeyType: Send + Sync + 'static, ValueType: Send + Sync + 'static, KeyComparator: Send + Sync + 'static> PageTrait for BPlusTreePage<KeyType, ValueType, KeyComparator> {
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        match self.page_type {
            IndexPageType::LeafPage => PageType::BTreeLeaf,
            IndexPageType::InternalPage => PageType::BTreeInternal,
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
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<KeyType: Send + Sync + 'static, ValueType: Send + Sync + 'static, KeyComparator: Send + Sync + 'static> Page for BPlusTreePage<KeyType, ValueType, KeyComparator> {
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

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync> PageTypeId for BPlusTreePage<KeyType, ValueType, KeyComparator> {
    const TYPE_ID: PageType = PageType::BTreeLeaf;
}

impl<KeyType: Send + Sync, ValueType: Send + Sync, KeyComparator: Send + Sync> Debug for BPlusTreePage<KeyType, ValueType, KeyComparator> {
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