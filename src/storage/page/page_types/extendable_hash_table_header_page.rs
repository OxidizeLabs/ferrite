use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{Page, PageTrait, PageType};
use log::{debug, info};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::mem::size_of;

pub const HTABLE_HEADER_PAGE_METADATA_SIZE: usize = size_of::<u32>();
pub const HTABLE_HEADER_MAX_DEPTH: u32 = 9;
pub const HTABLE_HEADER_ARRAY_SIZE: usize = 1 << HTABLE_HEADER_MAX_DEPTH;

#[derive(Clone)]
pub struct ExtendableHTableHeaderPage {
    base: Page,
    directory_page_ids: Vec<PageId>,
    global_depth: u32,
}

impl ExtendableHTableHeaderPage {
    pub fn new(page_id: PageId) -> Self {
        let instance = ExtendableHTableHeaderPage {
            base: Page::new(page_id),
            directory_page_ids: vec![INVALID_PAGE_ID; HTABLE_HEADER_ARRAY_SIZE],
            global_depth: 0,
        };
        debug!("New ExtendableHTableHeaderPage created with page id: {} at address {:p}", instance.get_page_id(), &instance);
        instance
    }

    /// Initializes a new header page with the specified maximum depth.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth in the header page.
    pub fn init(&mut self, global_depth: u32) {
        info!("Initializing ExtendableHTableHeaderPage with global depth: {}", global_depth);
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
        debug!(
            "Setting directory page ID at index {} to {}",
            directory_idx, directory_page_id
        );
        self.directory_page_ids.push(directory_page_id);
        info!("Directory page ID at index {} set to {}", directory_idx, directory_page_id);
        self.print_header();
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
        let max_idx_width = std::cmp::max(header_idx.len(), self.directory_page_ids.len().to_string().len());
        let max_page_id_width = std::cmp::max(header_pid.len(), self.directory_page_ids.iter().map(|&page_id| page_id.to_string().len()).max().unwrap_or(0));

        println!("======== HEADER (max_size: {}) (max_depth: {}) ========", self.max_size(), self.global_depth());
        println!(
            "| {:<width_idx$} | {:<width_pid$} |",
            header_idx, header_pid,
            width_idx = max_idx_width,
            width_pid = max_page_id_width
        );
        for (idx, &page_id) in self.directory_page_ids.iter().enumerate() {
            println!(
                "| {:<width_idx$} | {:<width_pid$} |",
                idx, page_id,
                width_idx = max_idx_width,
                width_pid = max_page_id_width
            );
        }
        println!("======== END HEADER ========");
    }
}

impl PageTrait for ExtendableHTableHeaderPage {
    fn get_page_id(&self) -> PageId {
        self.base.get_page_id()
    }

    fn is_dirty(&self) -> bool {
        self.base.is_dirty()
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.base.set_dirty(is_dirty)
    }

    fn get_pin_count(&self) -> i32 {
        self.base.get_pin_count()
    }

    fn increment_pin_count(&mut self) {
        self.base.increment_pin_count()
    }

    fn decrement_pin_count(&mut self) {
        self.base.decrement_pin_count()
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        &*self.base.get_data()
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        &mut *self.base.get_data_mut()
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        self.base.set_data(offset, new_data)
    }

    /// Sets the pin count of this page.
    fn set_pin_count(&mut self, pin_count: i32) {
        self.base.set_pin_count(pin_count);
        debug!("Setting pin count for Page ID {}: {}", self.get_page_id(), pin_count);
    }

    fn reset_memory(&mut self) {
        self.base.reset_memory()
    }
}

impl Debug for ExtendableHTableHeaderPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Display for ExtendableHTableHeaderPage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ExtendableHTableHeaderPage")
    }
}

impl TryFrom<PageType> for ExtendableHTableHeaderPage {
    type Error = ();

    fn try_from(page_type: PageType) -> Result<Self, Self::Error> {
        match page_type {
            PageType::ExtendedHashTableHeader(page) => Ok(page),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod basic_behavior {
    use crate::buffer::buffer_pool_manager::{BufferPoolManager, NewPageType};
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::page::page::PageTrait;
    use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
    use chrono::Utc;
    use log::info;
    use std::sync::Arc;
    use parking_lot::RwLock;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            const K: usize = 2;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(
                &disk_manager,
            ))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));
            Self {
                bpm,
                db_file,
                db_log_file
            }
        }

        fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup()
        }
    }


    #[test]
    fn header_page_integrity() {
        let ctx = TestContext::new("header_page_integrity");
        let bpm = &ctx.bpm;

        info!("Creating ExtendedHashTableHeader page");
        let header_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableHeader).unwrap();
        info!("Created page type: {}", header_guard.get_page_type());

        match header_guard.into_specific_type::<ExtendableHTableHeaderPage, 8>() {
            Some(mut ext_guard) => {
                info!("Successfully converted to ExtendableHTableHeaderPage");

                ext_guard.access(|page| {
                    info!("ExtendableHTableHeaderPage ID: {}", page.get_page_id());
                });

                // Initialize the header page with a max depth of 2
                ext_guard.access_mut(|page| {
                    page.init(2);
                    info!("Initialized header page with global depth: {}", page.global_depth());
                });

                // Test hashes that will produce different upper bits
                let hashes = [
                    0b00000000000000000000000000000000, // Should map to 0
                    0b01000000000000000000000000000000, // Should map to 1
                    0b10000000000000000000000000000000, // Should map to 2
                    0b11000000000000000000000000000000, // Should map to 3
                ];

                for (i, &hash) in hashes.iter().enumerate() {
                    ext_guard.access_mut(|page| {
                        let index = page.hash_to_directory_index(hash);
                        info!("Hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, i as u32, "Hash {:#034b} should map to index {}", hash, i);
                    });
                }
                info!("All hash to index mappings verified successfully");

                // Test with max_depth 0
                ext_guard.access_mut(|page| {
                    page.init(0);
                    for &hash in &hashes {
                        let index = page.hash_to_directory_index(hash);
                        info!("With max_depth 0, hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, 0, "With max_depth 0, all hashes should map to index 0");
                    }
                });
                info!("Max depth 0 tests completed successfully");

                // Test with max_depth 31 (maximum allowed)
                ext_guard.access_mut(|page| {
                    page.init(31);
                    for (_i, &hash) in hashes.iter().enumerate() {
                        let index = page.hash_to_directory_index(hash);
                        info!("With max_depth 31, hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, hash >> 1, "With max_depth 31, hash {:#034b} should map to index {}", hash, hash >> 1);
                    }
                });
                info!("Max depth 31 tests completed successfully");
            }
            None => {
                panic!("Failed to convert to ExtendableHTableHeaderPage");
            }
        }

        info!("Header page tests completed successfully");
    }
}