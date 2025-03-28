use crate::common::config::*;
use crate::common::exception::PageError;
use std::any::Any;
use std::fmt::Debug;

// Constants for page type identification
pub const PAGE_TYPE_INVALID: u8 = 0;
pub const PAGE_TYPE_BASIC: u8 = 1;
pub const PAGE_TYPE_TABLE: u8 = 2;
pub const PAGE_TYPE_HASH_TABLE_DIRECTORY: u8 = 3;
pub const PAGE_TYPE_HASH_TABLE_BUCKET: u8 = 4;
pub const PAGE_TYPE_HASH_TABLE_HEADER: u8 = 5;
pub const PAGE_TYPE_BTREE_HEADER: u8 = 6;
pub const PAGE_TYPE_BTREE_INTERNAL: u8 = 7;
pub const PAGE_TYPE_BTREE_LEAF: u8 = 8;



// Page header format (first few bytes of data)
pub const PAGE_TYPE_OFFSET: usize = 0;
pub const PAGE_ID_OFFSET: usize = 1;
// ... other header offsets

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PageType {
    Invalid = PAGE_TYPE_INVALID as isize,
    Basic = PAGE_TYPE_BASIC as isize,
    Table = PAGE_TYPE_TABLE as isize,
    HashTableDirectory = PAGE_TYPE_HASH_TABLE_DIRECTORY as isize,
    HashTableBucket = PAGE_TYPE_HASH_TABLE_BUCKET as isize,
    HashTableHeader = PAGE_TYPE_HASH_TABLE_HEADER as isize,
    BTreeHeader = PAGE_TYPE_BTREE_HEADER as isize,
    BTreeInternal = PAGE_TYPE_BTREE_INTERNAL as isize,
    BTreeLeaf = PAGE_TYPE_BTREE_LEAF as isize,
}

// Keep PageTrait as the main trait for dynamic dispatch
pub trait PageTrait: Send + Sync + Debug {
    fn get_page_id(&self) -> PageId;
    fn get_page_type(&self) -> PageType;
    fn is_dirty(&self) -> bool;
    fn set_dirty(&mut self, is_dirty: bool);
    fn get_pin_count(&self) -> i32;
    fn increment_pin_count(&mut self);
    fn decrement_pin_count(&mut self);
    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize];
    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize];
    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError>;
    fn set_pin_count(&mut self, pin_count: i32);
    fn reset_memory(&mut self);
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// Keep PageTypeId as a separate trait for static type information
pub trait PageTypeId {
    const TYPE_ID: PageType;
}

// Add new requirement to Page trait
pub trait Page: PageTrait + PageTypeId + 'static {
    // Add new associated function requirement
    fn new(page_id: PageId) -> Self;
}

// Implement Page for BasicPage
impl Page for BasicPage {
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
        };
        // Set the page type in the data immediately
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }
}

// Basic page implementation
#[derive(Debug)]
pub struct BasicPage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
}

impl PageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            PAGE_TYPE_INVALID => Some(Self::Invalid),
            PAGE_TYPE_BASIC => Some(Self::Basic),
            PAGE_TYPE_TABLE => Some(Self::Table),
            PAGE_TYPE_HASH_TABLE_DIRECTORY => Some(Self::HashTableDirectory),
            PAGE_TYPE_HASH_TABLE_BUCKET => Some(Self::HashTableBucket),
            PAGE_TYPE_HASH_TABLE_HEADER => Some(Self::HashTableHeader),
            PAGE_TYPE_BTREE_HEADER => Some(Self::BTreeHeader),
            PAGE_TYPE_BTREE_INTERNAL => Some(Self::BTreeInternal),
            PAGE_TYPE_BTREE_LEAF => Some(Self::BTreeLeaf),
            _ => None
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

impl PageTypeId for BasicPage {
    const TYPE_ID: PageType = PageType::Basic;
}

impl PageTrait for BasicPage {
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

        // Copy the data
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);

        // Always preserve the page type byte at PAGE_TYPE_OFFSET
        if offset <= PAGE_TYPE_OFFSET && offset + new_data.len() > PAGE_TYPE_OFFSET {
            self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        }

        self.is_dirty = true;
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data.fill(0);
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

    #[test]
    fn test_page_type_conversions() {
        // Test conversion from u8 to PageType
        assert_eq!(PageType::from_u8(PAGE_TYPE_BASIC), Some(PageType::Basic));
        assert_eq!(PageType::from_u8(PAGE_TYPE_TABLE), Some(PageType::Table));
        assert_eq!(
            PageType::from_u8(PAGE_TYPE_HASH_TABLE_DIRECTORY),
            Some(PageType::HashTableDirectory)
        );
        assert_eq!(
            PageType::from_u8(PAGE_TYPE_HASH_TABLE_BUCKET),
            Some(PageType::HashTableBucket)
        );
        assert_eq!(
            PageType::from_u8(PAGE_TYPE_HASH_TABLE_HEADER),
            Some(PageType::HashTableHeader)
        );
        assert_eq!(
            PageType::from_u8(PAGE_TYPE_INVALID),
            Some(PageType::Invalid)
        );

        // Test invalid conversion
        assert_eq!(PageType::from_u8(255), None);

        // Test conversion from PageType to u8
        assert_eq!(PageType::Basic.to_u8(), PAGE_TYPE_BASIC);
        assert_eq!(PageType::Table.to_u8(), PAGE_TYPE_TABLE);
        assert_eq!(
            PageType::HashTableDirectory.to_u8(),
            PAGE_TYPE_HASH_TABLE_DIRECTORY
        );
        assert_eq!(
            PageType::HashTableBucket.to_u8(),
            PAGE_TYPE_HASH_TABLE_BUCKET
        );
        assert_eq!(
            PageType::HashTableHeader.to_u8(),
            PAGE_TYPE_HASH_TABLE_HEADER
        );
        assert_eq!(PageType::Invalid.to_u8(), PAGE_TYPE_INVALID);
    }

    #[test]
    fn test_basic_page_type() {
        let mut page = BasicPage {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id: 1,
            pin_count: 0,
            is_dirty: false,
        };

        // Test that the static type ID is correct
        assert_eq!(BasicPage::TYPE_ID, PageType::Basic);

        // Set the page type in the data
        page.data[PAGE_TYPE_OFFSET] = PageType::Basic.to_u8();

        // Test that get_page_type returns the correct type
        assert_eq!(page.get_page_type(), PageType::Basic);

        // Test invalid page type handling
        page.data[PAGE_TYPE_OFFSET] = 255; // Invalid page type
        assert_eq!(page.get_page_type(), PageType::Invalid);
    }

    #[test]
    fn test_page_type_equality() {
        assert!(PageType::Basic == PageType::Basic);
        assert!(PageType::Table != PageType::Basic);

        // Test that the enum values match their corresponding constants
        assert_eq!(PageType::Basic as u8, PAGE_TYPE_BASIC);
        assert_eq!(PageType::Table as u8, PAGE_TYPE_TABLE);
        assert_eq!(
            PageType::HashTableDirectory as u8,
            PAGE_TYPE_HASH_TABLE_DIRECTORY
        );
    }

    #[test]
    fn test_basic_page_operations() {
        let mut page = BasicPage::new(1);

        // Test initial state
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.get_pin_count(), 1);
        assert!(!page.is_dirty());
        assert_eq!(
            page.get_page_type(),
            PageType::Basic,
            "Page type should be set in new()"
        );
        assert_eq!(
            page.data[PAGE_TYPE_OFFSET],
            PageType::Basic.to_u8(),
            "Page type should be set in data"
        );

        // Test pin count operations
        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 2);
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 1);

        // Test dirty flag
        page.set_dirty(true);
        assert!(page.is_dirty());
        page.set_dirty(false);
        assert!(!page.is_dirty());

        // Test page type persistence
        assert_eq!(page.get_page_type(), PageType::Basic);
        page.data[PAGE_TYPE_OFFSET] = PageType::Table.to_u8();
        assert_eq!(page.get_page_type(), PageType::Table);
    }

    #[test]
    fn test_page_data_operations() {
        let mut page = BasicPage::new(1);

        // Test data writing and reading
        let test_data = [42u8; 128];
        page.set_data(0, &test_data).expect("Failed to set data");

        // Create expected data array that accounts for preserved page type
        let mut expected_data = [42u8; 128];
        expected_data[PAGE_TYPE_OFFSET] = PageType::Basic.to_u8();

        assert_eq!(&page.get_data()[0..128], &expected_data);

        // Test data boundaries
        assert!(page
            .set_data(DB_PAGE_SIZE as usize - 10, &[1u8; 20])
            .is_err());
        assert!(page.set_data(DB_PAGE_SIZE as usize, &[1u8]).is_err());
    }

    // Add a new test specifically for page type preservation
    #[test]
    fn test_page_type_preservation() {
        let mut page = BasicPage::new(1);

        // Try to overwrite the entire page including page type
        let test_data = [0xFF; DB_PAGE_SIZE as usize];
        page.set_data(0, &test_data).expect("Failed to set data");

        // Verify that page type is preserved
        assert_eq!(page.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());

        // Try to overwrite just the page type byte
        page.set_data(PAGE_TYPE_OFFSET, &[0xFF])
            .expect("Failed to set data");
        assert_eq!(page.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());

        // Write data that includes page type byte
        let test_data = [0xFF; 4];
        page.set_data(PAGE_TYPE_OFFSET, &test_data)
            .expect("Failed to set data");
        assert_eq!(page.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());

        // Write data starting at offset 0
        let test_data = [0xFF; 2];
        page.set_data(0, &test_data).expect("Failed to set data");
        assert_eq!(page.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
    }

    #[test]
    fn test_page_type_persistence() {
        let mut page = BasicPage::new(1);

        // Set page type
        page.get_data_mut()[PAGE_TYPE_OFFSET] = PageType::Basic.to_u8();

        // Verify type is preserved
        assert_eq!(page.get_page_type(), PageType::Basic);

        // Change type and verify
        page.get_data_mut()[PAGE_TYPE_OFFSET] = PageType::Table.to_u8();
        assert_eq!(page.get_page_type(), PageType::Table);
    }

    #[test]
    fn test_page_reset() {
        let mut page = BasicPage::new(1);

        // Fill page with non-zero data
        page.get_data_mut().fill(42);
        page.set_dirty(true);
        page.set_pin_count(5);

        // Reset memory
        page.reset_memory();

        // Verify data is zeroed except for page type byte
        assert!(page.get_data().iter().enumerate().all(|(i, &x)| {
            if i == PAGE_TYPE_OFFSET {
                x == PageType::Basic.to_u8()
            } else {
                x == 0
            }
        }));

        // Verify page type is preserved
        assert_eq!(page.get_page_type(), PageType::Basic);

        // Verify metadata is reset
        assert!(!page.is_dirty());
        assert_eq!(page.get_pin_count(), 5); // Pin count should be preserved
    }

    #[test]
    fn test_invalid_page_operations() {
        let mut page = BasicPage::new(1);

        // Test invalid pin count operations
        page.set_pin_count(0);
        page.decrement_pin_count(); // Should not go below 0
        assert_eq!(page.get_pin_count(), 0);

        // Test invalid data operations
        let result = page.set_data(DB_PAGE_SIZE as usize - 10, &[1u8; 20]);
        assert!(matches!(result, Err(PageError::InvalidOffset { .. })));

        let result = page.set_data(DB_PAGE_SIZE as usize, &[1u8]);
        assert!(matches!(result, Err(PageError::InvalidOffset { .. })));
    }

    #[test]
    fn test_page_type_conversion() {
        // Test all valid page types
        let valid_types = [
            (PAGE_TYPE_BASIC, PageType::Basic),
            (PAGE_TYPE_TABLE, PageType::Table),
            (PAGE_TYPE_HASH_TABLE_DIRECTORY, PageType::HashTableDirectory),
            (PAGE_TYPE_HASH_TABLE_BUCKET, PageType::HashTableBucket),
            (PAGE_TYPE_HASH_TABLE_HEADER, PageType::HashTableHeader),
            (PAGE_TYPE_INVALID, PageType::Invalid),
        ];

        for (byte, expected_type) in valid_types {
            assert_eq!(PageType::from_u8(byte), Some(expected_type));
            assert_eq!(expected_type.to_u8(), byte);
        }

        // Test invalid conversions
        assert_eq!(PageType::from_u8(255), None);
        assert_eq!(PageType::from_u8(100), None);
    }

    #[test]
    fn test_concurrent_pin_operations() {
        use parking_lot::RwLock;
        use std::sync::Arc;
        use std::thread;

        let page = Arc::new(RwLock::new(BasicPage::new(1)));
        let mut handles = vec![];

        // Spawn multiple threads to increment pin count
        for _ in 0..10 {
            let page_clone = Arc::clone(&page);
            let handle = thread::spawn(move || {
                let mut page = page_clone.write();
                page.increment_pin_count();
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final pin count
        assert_eq!(page.read().get_pin_count(), 11); // Initial 1 + 10 increments
    }
}
