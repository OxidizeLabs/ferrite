use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};
use std::any::Any;
use std::mem;
use serde::{Serialize, Deserialize};

/// The header page for a B+ tree.
/// This page keeps track of the root page ID and other metadata about the B+ tree.
#[derive(Debug, Clone)]
pub struct BPlusTreeHeaderPage {
    /// The ID of the root page
    root_page_id: PageId,
    /// The height of the B+ tree
    tree_height: u32,
    /// The number of keys in the B+ tree
    num_keys: usize,
    /// Page data
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    /// Page ID
    page_id: PageId,
    /// Pin count
    pin_count: i32,
    /// Dirty flag
    is_dirty: bool,
}

impl BPlusTreeHeaderPage {
    /// Create a new header page with options
    pub fn new_with_options(page_id: PageId) -> Self {
        let mut page = Self {
            root_page_id: INVALID_PAGE_ID,
            tree_height: 0,
            num_keys: 0,
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }

    /// Get the root page ID
    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Set the root page ID
    pub fn set_root_page_id(&mut self, page_id: PageId) {
        self.root_page_id = page_id;
    }

    /// Get the current height of the B+ tree
    pub fn get_tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Set the height of the B+ tree
    pub fn set_tree_height(&mut self, height: u32) {
        self.tree_height = height;
    }

    /// Increment the height of the B+ tree by 1
    pub fn increment_tree_height(&mut self) {
        self.tree_height += 1;
    }

    /// Decrement the height of the B+ tree by 1
    pub fn decrement_tree_height(&mut self) {
        if self.tree_height > 0 {
            self.tree_height -= 1;
        }
    }

    /// Get the number of keys in the B+ tree
    pub fn get_num_keys(&self) -> usize {
        self.num_keys
    }

    /// Set the number of keys in the B+ tree
    pub fn set_num_keys(&mut self, num: usize) {
        self.num_keys = num;
    }

    /// Increment the number of keys in the B+ tree
    pub fn increment_num_keys(&mut self) {
        self.num_keys += 1;
    }

    /// Decrement the number of keys in the B+ tree
    pub fn decrement_num_keys(&mut self) {
        if self.num_keys > 0 {
            self.num_keys -= 1;
        }
    }

    /// Check if the B+ tree is empty
    pub fn is_empty(&self) -> bool {
        self.root_page_id == INVALID_PAGE_ID
    }

    /// Serialize the header page to bytes for storage
    pub fn serialize(&self) -> Vec<u8> {
        // We're only serializing the essential data fields
        let header_data = HeaderData {
            root_page_id: self.root_page_id,
            tree_height: self.tree_height,
            num_keys: self.num_keys,
        };
        
        bincode::serialize(&header_data).expect("Failed to serialize BPlusTreeHeaderPage")
    }

    /// Deserialize bytes into a header page
    pub fn deserialize(data: &[u8], page_id: PageId) -> Self {
        // Deserialize just the data fields
        let header_data: HeaderData = bincode::deserialize(data)
            .expect("Failed to deserialize BPlusTreeHeaderPage");
        
        // Create a new page with the deserialized data
        let mut page = Self::new_with_options(page_id);
        page.root_page_id = header_data.root_page_id;
        page.tree_height = header_data.tree_height;
        page.num_keys = header_data.num_keys;
        
        page
    }
}

impl PageTypeId for BPlusTreeHeaderPage {
    const TYPE_ID: PageType = PageType::BTreeHeader;
}

impl Page for BPlusTreeHeaderPage {
    fn new(page_id: PageId) -> Self {
        Self::new_with_options(page_id)
    }
}

impl PageTrait for BPlusTreeHeaderPage {
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
        self.root_page_id = INVALID_PAGE_ID;
        self.tree_height = 0;
        self.num_keys = 0;
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
    fn test_new_header_page_is_empty() {
        let header_page = BPlusTreeHeaderPage::new(1);
        assert!(header_page.is_empty());
        assert_eq!(header_page.get_root_page_id(), INVALID_PAGE_ID);
        assert_eq!(header_page.get_tree_height(), 0);
        assert_eq!(header_page.get_num_keys(), 0);
    }

    #[test]
    fn test_root_page_id_operations() {
        let mut header_page = BPlusTreeHeaderPage::new(1);
        let test_page_id: PageId = 42;

        // Test setting
        header_page.set_root_page_id(test_page_id);
        assert_eq!(header_page.get_root_page_id(), test_page_id);
        assert!(!header_page.is_empty());

        // Test resetting
        header_page.set_root_page_id(INVALID_PAGE_ID);
        assert_eq!(header_page.get_root_page_id(), INVALID_PAGE_ID);
        assert!(header_page.is_empty());
    }

    #[test]
    fn test_tree_height_operations() {
        let mut header_page = BPlusTreeHeaderPage::new(1);

        // Test initial height
        assert_eq!(header_page.get_tree_height(), 0);

        // Test setting
        header_page.set_tree_height(3);
        assert_eq!(header_page.get_tree_height(), 3);

        // Test increment
        header_page.increment_tree_height();
        assert_eq!(header_page.get_tree_height(), 4);

        // Test decrement
        header_page.decrement_tree_height();
        assert_eq!(header_page.get_tree_height(), 3);

        // Test decrement at zero
        header_page.set_tree_height(0);
        header_page.decrement_tree_height();
        assert_eq!(header_page.get_tree_height(), 0);
    }

    #[test]
    fn test_num_keys_operations() {
        let mut header_page = BPlusTreeHeaderPage::new(1);

        // Test initial count
        assert_eq!(header_page.get_num_keys(), 0);

        // Test setting
        header_page.set_num_keys(100);
        assert_eq!(header_page.get_num_keys(), 100);

        // Test increment
        header_page.increment_num_keys();
        assert_eq!(header_page.get_num_keys(), 101);

        // Test decrement
        header_page.decrement_num_keys();
        assert_eq!(header_page.get_num_keys(), 100);

        // Test decrement at zero
        header_page.set_num_keys(0);
        header_page.decrement_num_keys();
        assert_eq!(header_page.get_num_keys(), 0);
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut original_page = BPlusTreeHeaderPage::new(1);
        original_page.set_root_page_id(123);
        original_page.set_tree_height(5);
        original_page.set_num_keys(1000);

        let serialized = original_page.serialize();
        let deserialized = BPlusTreeHeaderPage::deserialize(&serialized, 1);

        assert_eq!(
            deserialized.get_root_page_id(),
            original_page.get_root_page_id()
        );
        assert_eq!(
            deserialized.get_tree_height(),
            original_page.get_tree_height()
        );
        assert_eq!(deserialized.get_num_keys(), original_page.get_num_keys());
    }

    #[test]
    fn test_is_empty_behavior() {
        let mut header_page = BPlusTreeHeaderPage::new(1);

        // Initially empty
        assert!(header_page.is_empty());

        // Not empty after setting root page ID
        header_page.set_root_page_id(42);
        assert!(!header_page.is_empty());

        // Empty again after setting invalid root page ID
        header_page.set_root_page_id(INVALID_PAGE_ID);
        assert!(header_page.is_empty());
    }

    #[test]
    fn test_header_page_persistence() {
        // Test that header page data persists correctly through serialization/deserialization
        {
            // Initialize a new header page
            let mut header = BPlusTreeHeaderPage::new(1);
            header.set_root_page_id(42);
            header.set_tree_height(3);
            header.set_num_keys(100);

            // Serialize the header page
            let serialized = header.serialize();

            // Deserialize into a new header page
            let deserialized = BPlusTreeHeaderPage::deserialize(&serialized, 1);

            // Verify all values are preserved
            assert_eq!(deserialized.get_root_page_id(), 42);
            assert_eq!(deserialized.get_tree_height(), 3);
            assert_eq!(deserialized.get_num_keys(), 100);
        }

        // Test with different values
        {
            let mut header = BPlusTreeHeaderPage::new(1);
            header.set_root_page_id(100);
            header.set_tree_height(5);
            header.set_num_keys(1000);

            let serialized = header.serialize();
            let deserialized = BPlusTreeHeaderPage::deserialize(&serialized, 1);

            assert_eq!(deserialized.get_root_page_id(), 100);
            assert_eq!(deserialized.get_tree_height(), 5);
            assert_eq!(deserialized.get_num_keys(), 1000);
        }

        // Test with zero/empty values
        {
            let mut header = BPlusTreeHeaderPage::new(1);
            header.set_root_page_id(INVALID_PAGE_ID);
            header.set_tree_height(0);
            header.set_num_keys(0);

            let serialized = header.serialize();
            let deserialized = BPlusTreeHeaderPage::deserialize(&serialized, 1);

            assert_eq!(deserialized.get_root_page_id(), INVALID_PAGE_ID);
            assert_eq!(deserialized.get_tree_height(), 0);
            assert_eq!(deserialized.get_num_keys(), 0);
            assert!(deserialized.is_empty());
        }
    }
}

#[cfg(test)]
mod integration_tests {
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::page::page::{Page, PageTrait};
    use crate::storage::page::page_guard::PageGuard;
    use crate::storage::page::page_types::b_plus_tree_header_page::BPlusTreeHeaderPage;
    use crate::storage::page::page_types::b_plus_tree_header_page::INVALID_PAGE_ID;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::tempdir;
    use super::HeaderData;

    #[test]
    fn test_header_page_in_buffer_pool() {
        // Create temporary directory for disk manager
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let db_file = temp_dir.path().join("test.db");

        // Create dependencies for buffer pool manager
        let db_file_path = db_file.to_string_lossy().to_string();
        let log_file_path = temp_dir
            .path()
            .join("test.log")
            .to_string_lossy()
            .to_string();
        let disk_manager = Arc::new(FileDiskManager::new(
            db_file_path,
            log_file_path,
            4096, // Standard page size as buffer size
        ));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2))); // 10 frames, K=2

        // Create buffer pool manager with all required parameters
        let buffer_pool_manager = BufferPoolManager::new(
            10,             // pool_size
            disk_scheduler, // disk_scheduler
            disk_manager,   // disk_manager
            replacer,       // replacer
        );

        // Allocate a page for the header page
        let page: PageGuard<BPlusTreeHeaderPage> = buffer_pool_manager
            .new_page()
            .expect("Failed to allocate new page");
        let header_page_id = page.get_page_id();

        // Create and write a header page
        {
            // Initialize a new header page
            let mut header = BPlusTreeHeaderPage::new(header_page_id);
            header.set_root_page_id(42);
            header.set_tree_height(3);
            header.set_num_keys(100);

            // Write the header page data to the page
            let header_data = header.serialize();
            let mut page_data = page.write();
            
            // Copy only what fits in the page
            let copy_len = std::cmp::min(header_data.len(), page_data.get_data_mut().len());
            page_data.get_data_mut()[..copy_len].copy_from_slice(&header_data[..copy_len]);

            // Mark the page as dirty and unpin it
            buffer_pool_manager.unpin_page(header_page_id, true, AccessType::Lookup);
        }

        // Fetch and verify the header page
        {
            let page: PageGuard<BPlusTreeHeaderPage> = buffer_pool_manager
                .fetch_page(header_page_id)
                .expect("Failed to fetch header page");

            // Get the raw data
            let page_data = page.read();
            let header_data = page_data.get_data();
            
            // Deserialize
            let deserialized: HeaderData = bincode::deserialize(&header_data[..])
                .expect("Failed to deserialize HeaderData");
                
            // Verify the header page values
            assert_eq!(deserialized.root_page_id, 42);
            assert_eq!(deserialized.tree_height, 3);
            assert_eq!(deserialized.num_keys, 100);

            buffer_pool_manager.unpin_page(header_page_id, false, AccessType::Lookup);
        }
    }
}

// #[cfg(test)]
// mod e2e_tests {
//     use crate::buffer::buffer_pool_manager::BufferPoolManager;
//     use crate::common::config::INVALID_PAGE_ID;
//     use crate::storage::index::b_plus_tree_index::BPlusTreeIndex;
//
//     #[test]
//     fn test_header_page_updates_during_tree_operations() {
//         let buffer_pool_manager = BufferPoolManager::new(100);
//         let mut tree = BPlusTreeIndex::new(buffer_pool_manager);
//
//         // Get initial state
//         let initial_height = tree.get_height();
//         assert_eq!(initial_height, 0);
//
//         // Insert enough elements to cause splits and height increases
//         for i in 0..1000 {
//             tree.insert(&i, i + 1000);
//         }
//
//         // Verify height increased
//         let new_height = tree.get_height();
//         assert!(new_height > initial_height);
//
//         // Verify key count
//         assert_eq!(tree.get_size(), 1000);
//
//         // Verify header page root ID points to valid page
//         assert_ne!(tree.get_root_page_id(), INVALID_PAGE_ID);
//     }
//
//     #[test]
//     fn test_empty_tree_after_deletion() {
//         let buffer_pool_manager = BufferPoolManager::new(100);
//         let mut tree = BPlusTreeIndex::new(buffer_pool_manager);
//
//         // Insert and then delete a key
//         tree.insert(&1, 1001);
//         assert_eq!(tree.get_size(), 1);
//         assert!(!tree.is_empty());
//
//         // Delete the key
//         tree.remove(&1);
//
//         // Verify tree is empty
//         assert_eq!(tree.get_size(), 0);
//         assert!(tree.is_empty());
//
//         // Verify header page reflects emptiness
//         assert_eq!(tree.get_root_page_id(), INVALID_PAGE_ID);
//     }
// }

// Helper struct for serialization, containing only the fields we need to persist
#[derive(Serialize, Deserialize)]
pub struct HeaderData {
    pub root_page_id: PageId,
    pub tree_height: u32,
    pub num_keys: usize,
}
