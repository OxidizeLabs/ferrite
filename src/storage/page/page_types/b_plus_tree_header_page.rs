use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType, PageTypeId, PAGE_TYPE_OFFSET};
use std::any::Any;

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
    /// Order of the B+ tree
    order: u32,
}

#[derive(Debug, Clone, PartialEq)]
#[derive(bincode::Encode, bincode::Decode)]
pub struct HeaderData {
    pub root_page_id: PageId,
    pub tree_height: u32,
    pub num_keys: usize,
    pub order: u32,
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
            order: 0,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }

    /// Set the order of the B+ tree
    pub fn set_order(&mut self, order: u32) {
        self.order = order;
    }

    /// Get the order of the B+ tree
    pub fn get_order(&self) -> u32 {
        self.order
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

    /// Serialize the header page into bytes
    pub fn serialize(&self) -> Vec<u8> {
        // We're only serializing the essential data fields
        let header_data = HeaderData {
            root_page_id: self.root_page_id,
            tree_height: self.tree_height,
            num_keys: self.num_keys,
            order: self.order,
        };

        // Use bincode 2.0 API
        bincode::encode_to_vec(&header_data, bincode::config::standard())
            .expect("Failed to serialize BPlusTreeHeaderPage")
    }

    /// Deserialize bytes into a header page
    pub fn deserialize(data: &[u8], page_id: PageId) -> Self {
        // Deserialize just the data fields
        let (header_data, _): (HeaderData, usize) = bincode::decode_from_slice(
            data, 
            bincode::config::standard()
        ).expect("Failed to deserialize BPlusTreeHeaderPage");

        // Create a new page with the deserialized data
        let mut page = Self::new_with_options(page_id);
        page.root_page_id = header_data.root_page_id;
        page.tree_height = header_data.tree_height;
        page.num_keys = header_data.num_keys;
        page.order = header_data.order;

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
    use std::sync::Arc;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                Arc::from(disk_manager.unwrap()),
                replacer.clone(),
            ).unwrap());

            Self {
                bpm
            }
        }
    }

    #[cfg(test)]
    mod unit_tests {
        use crate::common::config::{PageId, INVALID_PAGE_ID};
        use crate::storage::page::page::Page;
        use crate::storage::page::page_types::b_plus_tree_header_page::BPlusTreeHeaderPage;

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

        #[test]
        fn test_order_operations() {
            let mut header_page = BPlusTreeHeaderPage::new(1);

            // Test initial order
            assert_eq!(header_page.get_order(), 0);

            // Test setting order
            header_page.set_order(5);
            assert_eq!(header_page.get_order(), 5);

            // Test setting different order
            header_page.set_order(10);
            assert_eq!(header_page.get_order(), 10);

            // Test setting zero order
            header_page.set_order(0);
            assert_eq!(header_page.get_order(), 0);
        }

        #[test]
        fn test_order_serialization() {
            let mut original_page = BPlusTreeHeaderPage::new(1);
            original_page.set_root_page_id(123);
            original_page.set_tree_height(5);
            original_page.set_num_keys(1000);
            original_page.set_order(7);

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
            assert_eq!(deserialized.get_order(), original_page.get_order());
        }
    }

    #[cfg(test)]
    mod integration_tests {
        use crate::storage::page::page::PageTrait;
        use crate::storage::page::page_guard::PageGuard;
        use crate::storage::page::page_types::b_plus_tree_header_page::BPlusTreeHeaderPage;
        use crate::storage::page::page_types::b_plus_tree_header_page::tests::TestContext;

        #[tokio::test]
        async fn test_header_page_in_buffer_pool() {
            let ctx = TestContext::new("test_header_page_in_buffer_pool").await;
            let bpm = ctx.bpm;
            // Allocate a page for the header page and store its ID
            let header_page_id = {
                let page = bpm
                    .new_page::<BPlusTreeHeaderPage>()
                    .expect("Failed to allocate new page");

                // Get the page ID before modifying
                let page_id = page.get_page_id();

                // Modify the header page directly through the page guard
                {
                    let mut page_data = page.write();
                    page_data.set_root_page_id(42);
                    page_data.set_tree_height(3);
                    page_data.set_num_keys(100);

                    // Mark the page as dirty
                    page_data.set_dirty(true);
                }

                // Page guard will be dropped here, automatically decrementing pin count
                page_id
            };

            // Force a flush to ensure data is written to disk
            bpm
                .flush_page(header_page_id)
                .expect("Failed to flush page");

            // Fetch and verify the header page
            {
                let page: PageGuard<BPlusTreeHeaderPage> = bpm
                    .fetch_page(header_page_id)
                    .expect("Failed to fetch header page");

                // Read the header page values directly using the accessor methods
                let page_data = page.read();

                // Verify the header page values
                assert_eq!(page_data.get_root_page_id(), 42);
                assert_eq!(page_data.get_tree_height(), 3);
                assert_eq!(page_data.get_num_keys(), 100);

                // Page guard will be dropped here, automatically handling the pin count
            }
        }
    }
}