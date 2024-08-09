use crate::common::config::PageId;
use log::{info, debug, warn};
use std::mem::size_of;

pub const HTABLE_HEADER_PAGE_METADATA_SIZE: usize = size_of::<u32>();
pub const HTABLE_HEADER_MAX_DEPTH: u32 = 9;
pub const HTABLE_HEADER_ARRAY_SIZE: usize = 1 << HTABLE_HEADER_MAX_DEPTH;

pub struct ExtendableHTableHeaderPage {
    directory_page_ids: [PageId; HTABLE_HEADER_ARRAY_SIZE],
    max_depth: u32,
}

impl ExtendableHTableHeaderPage {
    /// Initializes a new header page with the specified maximum depth.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth in the header page.
    pub fn init(&mut self, max_depth: u32) {
        info!("Initializing ExtendableHTableHeaderPage with max depth: {}", max_depth);
        self.max_depth = max_depth;
        self.directory_page_ids.fill(PageId::default());
        debug!("All directory page IDs initialized to default PageId.");
    }

    /// Returns the directory index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The directory index the key is hashed to.
    pub fn hash_to_directory_index(&self, hash: u32) -> u32 {
        if self.max_depth >= 32 {
            warn!(
                "Max depth is too large: {}. It should be less than 32 to avoid overflow.",
                self.max_depth
            );
            return 0; // Handle the overflow case gracefully
        }

        // Number of bits to shift right to get the upper bits
        let shift_amount = 32 - self.max_depth;
        debug!(
            "Calculating directory index: hash={}, max_depth={}, shift_amount={}",
            hash, self.max_depth, shift_amount
        );

        // Shift right to get the upper bits
        let upper_bits = hash >> shift_amount;

        // Apply mask to keep only max_depth bits
        let directory_index = upper_bits & ((1 << self.max_depth) - 1);
        debug!(
            "Computed directory index: hash={}, upper_bits={}, directory_index={}",
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
    pub fn get_directory_page_id(&self, directory_idx: u32) -> PageId {
        let page_id = self.directory_page_ids[directory_idx as usize];
        debug!("Retrieved directory page ID at index {}: {}", directory_idx, page_id);
        page_id
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
        self.directory_page_ids[directory_idx as usize] = directory_page_id;
        info!("Directory page ID at index {} set to {}", directory_idx, directory_page_id);
    }

    /// Returns the maximum number of directory page IDs the header page can handle.
    ///
    /// # Returns
    /// The maximum size.
    pub fn max_size(&self) -> u32 {
        let max_size = HTABLE_HEADER_ARRAY_SIZE as u32;
        debug!("Computed max size of directory page IDs: {}", max_size);
        max_size
    }

    /// Prints the header's occupancy information.
    pub fn print_header(&self) {
        info!("Printing ExtendableHTableHeaderPage information:");
        info!("Max depth: {}", self.max_depth);
        for (i, &page_id) in self.directory_page_ids.iter().enumerate() {
            if page_id != PageId::default() {
                info!("Index {}: Page ID {}", i, page_id);
            } else {
                debug!("Index {}: Page ID is default (empty slot)", i);
            }
        }
    }
}
