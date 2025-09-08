use crate::common::config::{DB_PAGE_SIZE, INVALID_PAGE_ID, PageId};
use crate::common::exception::PageError;
use crate::storage::page::{PAGE_TYPE_OFFSET, Page, PageTrait, PageType, PageTypeId};
use log::{debug, info};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::mem::size_of;

pub const HTABLE_HEADER_PAGE_METADATA_SIZE: usize = size_of::<u32>();
pub const HTABLE_HEADER_MAX_DEPTH: u32 = 9;
pub const HTABLE_HEADER_ARRAY_SIZE: usize = 1 << HTABLE_HEADER_MAX_DEPTH;

#[derive(Clone)]
pub struct ExtendableHTableHeaderPage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    directory_page_ids: Vec<PageId>,
    global_depth: u32,
}

impl ExtendableHTableHeaderPage {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            directory_page_ids: vec![INVALID_PAGE_ID; HTABLE_HEADER_ARRAY_SIZE],
            global_depth: 0,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }

    /// Initializes a new header page with the specified maximum depth.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth in the header page.
    pub fn init(&mut self, global_depth: u32) {
        info!(
            "Initializing ExtendableHTableHeaderPage with global depth: {}",
            global_depth
        );
        self.global_depth = global_depth;
        self.directory_page_ids = vec![INVALID_PAGE_ID; HTABLE_HEADER_ARRAY_SIZE];
        debug!("All directory page IDs initialized to INVALID_PAGE_ID: -1.");
    }

    /// Returns the directory index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The directory index the key is hashed to.
    pub fn hash_to_directory_index(&self, hash: u32) -> u32 {
        // Cap max_depth to a valid range [0, 31]
        let capped_max_depth = self.global_depth.min(31);

        // Handle the case where max_depth is 0 explicitly
        if capped_max_depth == 0 {
            debug!(
                "Max depth is 0, returning directory index 0 for hash {:#034b}",
                hash
            );
            return 0;
        }

        // Number of bits to shift right to get the upper bits
        let shift_amount = 32 - capped_max_depth;
        debug!(
            "Calculating directory index: hash={:#034b}, max_depth={}, capped_max_depth={}, shift_amount={}",
            hash, self.global_depth, capped_max_depth, shift_amount
        );

        // Shift right to get the upper bits
        let upper_bits = hash >> shift_amount;

        // Apply mask to keep only max_depth bits
        let directory_index = upper_bits & ((1 << capped_max_depth) - 1);
        debug!(
            "Computed directory index: hash={:#034b}, upper_bits={}, directory_index={}",
            hash, upper_bits, directory_index
        );

        directory_index
    }

    /// Returns the directory page ID at an index.
    ///
    /// # Parameters
    /// - `directory_idx`: The index in the directory page ID array.
    ///
    /// # Returns
    /// The directory page ID at the specified index.
    pub fn get_directory_page_id(&self, directory_idx: usize) -> Option<PageId> {
        if directory_idx < self.directory_page_ids.len() {
            let page_id = self.directory_page_ids.get(directory_idx);
            debug!(
                "Retrieved directory page ID at index {}: {:?}",
                directory_idx, page_id
            );
            return page_id.cloned();
        }
        None
    }

    /// Sets the directory page ID at an index.
    ///
    /// # Parameters
    /// - `directory_idx`: The index in the directory page ID array.
    /// - `directory_page_id`: The page ID of the directory.
    pub fn set_directory_page_id(&mut self, directory_idx: u32, directory_page_id: PageId) {
        if (directory_idx as usize) < self.directory_page_ids.len() {
            self.directory_page_ids[directory_idx as usize] = directory_page_id;
        } else {
            self.directory_page_ids.push(directory_page_id);
        }
        debug!(
            "Directory page ID at index {} set to {}",
            directory_idx, directory_page_id
        );
    }

    /// Returns the maximum number of directory page IDs the header page can handle.
    ///
    /// # Returns
    /// The maximum size.
    fn max_size(&self) -> u32 {
        let max_size = 2_u32.pow(self.global_depth);
        debug!("Computed max size of directory page IDs: {}", max_size);
        max_size
    }

    /// Returns the maximum depth the header page could handle.
    ///
    /// # Returns
    /// The global depth.
    pub fn global_depth(&self) -> u32 {
        self.global_depth
    }

    /// Prints the header's occupancy information.
    pub fn print_header(&self) {
        // Define the column headers
        let header_idx = "directory_idx";
        let header_pid = "page_id";

        // Calculate the maximum width for the directory index and page_id columns
        let max_idx_width = std::cmp::max(
            header_idx.len(),
            self.directory_page_ids.len().to_string().len(),
        );
        let max_page_id_width = std::cmp::max(
            header_pid.len(),
            self.directory_page_ids
                .iter()
                .map(|&page_id| page_id.to_string().len())
                .max()
                .unwrap_or(0),
        );

        println!(
            "======== HEADER (max_size: {}) (max_depth: {}) ========",
            self.max_size(),
            self.global_depth()
        );
        println!(
            "| {:<width_idx$} | {:<width_pid$} |",
            header_idx,
            header_pid,
            width_idx = max_idx_width,
            width_pid = max_page_id_width
        );
        for (idx, &page_id) in self.directory_page_ids.iter().enumerate() {
            println!(
                "| {:<width_idx$} | {:<width_pid$} |",
                idx,
                page_id,
                width_idx = max_idx_width,
                width_pid = max_page_id_width
            );
        }
        println!("======== END HEADER ========");
    }
}

impl PageTypeId for ExtendableHTableHeaderPage {
    const TYPE_ID: PageType = PageType::HashTableHeader;
}

impl Page for ExtendableHTableHeaderPage {
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            directory_page_ids: vec![INVALID_PAGE_ID; HTABLE_HEADER_ARRAY_SIZE],
            global_depth: 0,
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }
}

impl PageTrait for ExtendableHTableHeaderPage {
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
        self.directory_page_ids.clear();
        self.directory_page_ids
            .resize(HTABLE_HEADER_ARRAY_SIZE, INVALID_PAGE_ID);
        self.global_depth = 0;
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

impl Debug for ExtendableHTableHeaderPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for ExtendableHTableHeaderPage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ExtendableHTableHeaderPage")
    }
}

#[cfg(test)]
mod basic_behavior {
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::page::PageTrait;
    use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
    use log::info;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
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
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    Arc::from(disk_manager.unwrap()),
                    replacer.clone(),
                )
                .unwrap(),
            );

            Self { bpm }
        }
    }

    #[tokio::test]
    async fn header_page_integrity() {
        let ctx = TestContext::new("header_page_integrity").await;
        let bpm = &ctx.bpm;

        info!("Creating ExtendedHashTableHeader page");
        let header_page = bpm
            .new_page::<ExtendableHTableHeaderPage>()
            .expect("Failed to create header page");

        {
            let mut page = header_page.write();
            info!("Successfully created ExtendableHTableHeaderPage");
            info!("ExtendableHTableHeaderPage ID: {}", page.get_page_id());

            // Initialize the header page with a max depth of 2
            page.init(2);
            info!(
                "Initialized header page with global depth: {}",
                page.global_depth()
            );

            // Test hashes that will produce different upper bits
            let hashes = [
                0b00000000000000000000000000000000, // Should map to 0
                0b01000000000000000000000000000000, // Should map to 1
                0b10000000000000000000000000000000, // Should map to 2
                0b11000000000000000000000000000000, // Should map to 3
            ];

            for (i, &hash) in hashes.iter().enumerate() {
                let index = page.hash_to_directory_index(hash);
                info!("Hash {:#034b} mapped to index {}", hash, index);
                assert_eq!(
                    index, i as u32,
                    "Hash {:#034b} should map to index {}",
                    hash, i
                );
            }
        }

        info!("Header page tests completed successfully");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_page_type() {
        let page = ExtendableHTableHeaderPage::new(1);
        assert_eq!(page.get_page_type(), PageType::HashTableHeader);

        // Test after reset
        let mut page = ExtendableHTableHeaderPage::new(1);
        page.reset_memory();
        assert_eq!(page.get_page_type(), PageType::HashTableHeader);
    }

    #[test]
    fn test_data_operations() {
        let mut page = ExtendableHTableHeaderPage::new(1);

        // Test data setting
        let test_data = vec![1, 2, 3, 4];
        page.set_data(10, &test_data).unwrap();

        // Verify data was set
        assert_eq!(&page.get_data()[10..14], &test_data);
        assert!(page.is_dirty());
    }

    #[test]
    fn test_pin_count() {
        let mut page = ExtendableHTableHeaderPage::new(1);
        assert_eq!(page.get_pin_count(), 1);

        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 2);

        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 1);
    }
}
