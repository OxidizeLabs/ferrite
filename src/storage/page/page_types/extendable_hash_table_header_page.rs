use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use crate::common::config::{INVALID_PAGE_ID, PageId, DB_PAGE_SIZE};
use log::{info, debug, warn};
use std::mem::size_of;
use crate::common::exception::PageError;
use crate::storage::page::page::{AsAny, Page, PageTrait, PageType};
use crate::storage::page::page_types::extendable_hash_table_bucket_page::TypeErasedBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::{ExtendableHTableDirectoryPage, HTABLE_DIRECTORY_ARRAY_SIZE};

pub const HTABLE_HEADER_PAGE_METADATA_SIZE: usize = size_of::<u32>();
pub const HTABLE_HEADER_MAX_DEPTH: u32 = 9;
pub const HTABLE_HEADER_ARRAY_SIZE: usize = 1 << HTABLE_HEADER_MAX_DEPTH;

#[derive(Clone)]
pub struct ExtendableHTableHeaderPage {
    base: Page,
    directory_page_ids: Vec<PageId>,
    global_depth: u32
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
            return page_id.cloned()
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

        println!("======== HEADER (max_size: {}) (max_depth: {}) ========", self.max_size(),  self.global_depth());
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

impl AsAny for ExtendableHTableHeaderPage {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}