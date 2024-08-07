use crate::common::config::PageId;
use log::info;

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
        self.max_depth = max_depth;
        self.directory_page_ids.fill(PageId::default());
    }

    /// Returns the directory index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The directory index the key is hashed to.
    pub fn hash_to_directory_index(&self, hash: u32) -> u32 {
        // Number of bits to shift right to get the upper bits
        let shift_amount = 32 - self.max_depth;
        // Shift right to get the upper bits
        let upper_bits = hash >> shift_amount;
        // Apply mask to keep only max_depth bits
        upper_bits & ((1 << self.max_depth) - 1)
    }

    /// Returns the directory page ID at an index.
    ///
    /// # Parameters
    /// - `directory_idx`: The index in the directory page ID array.
    ///
    /// # Returns
    /// The directory page ID at the specified index.
    pub fn get_directory_page_id(&self, directory_idx: u32) -> PageId {
        self.directory_page_ids[directory_idx as usize]
    }

    /// Sets the directory page ID at an index.
    ///
    /// # Parameters
    /// - `directory_idx`: The index in the directory page ID array.
    /// - `directory_page_id`: The page ID of the directory.
    pub fn set_directory_page_id(&mut self, directory_idx: u32, directory_page_id: PageId) {
        self.directory_page_ids[directory_idx as usize] = directory_page_id;
    }

    /// Returns the maximum number of directory page IDs the header page can handle.
    ///
    /// # Returns
    /// The maximum size.
    pub fn max_size(&self) -> u32 {
        HTABLE_HEADER_ARRAY_SIZE as u32
    }

    /// Prints the header's occupancy information.
    pub fn print_header(&self) {
        info!("ExtendableHTableHeaderPage:");
        info!("Max depth: {}", self.max_depth);
        for (i, &page_id) in self.directory_page_ids.iter().enumerate() {
            info!("Index {}: Page ID {}", i, page_id);
        }
    }
}
