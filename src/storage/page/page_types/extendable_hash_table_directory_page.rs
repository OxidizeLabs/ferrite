use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::{AsAny, Page, PageTrait, PageType};
use crate::storage::page::page_types::extendable_hash_table_header_page::HTABLE_HEADER_ARRAY_SIZE;
use log::{debug, info};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};

pub const HTABLE_DIRECTORY_MAX_DEPTH: u64 = 9;
pub const HTABLE_DIRECTORY_ARRAY_SIZE: u64 = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

// Static variables to hold the instance and access counts
static INSTANCE_COUNT: AtomicUsize = AtomicUsize::new(0);
static ACCESS_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct ExtendableHTableDirectoryPage {
    base: Page,
    max_depth: u32,
    global_depth: u32,
    local_depths: [u32; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
    bucket_page_ids: [PageId; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
}

impl ExtendableHTableDirectoryPage {
    pub fn new(page_id: PageId) -> Self {
        let instance = ExtendableHTableDirectoryPage {
            base: Page::new(page_id),
            max_depth: 0,
            global_depth: 0,
            local_depths: [0; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
            bucket_page_ids: [INVALID_PAGE_ID; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
        };
        debug!("New ExtendableHTableDirectoryPage created with page id: {} at address {:p}", instance.get_page_id(), &instance);
        instance
    }

    /// Initializes a new directory page with the specified maximum depth.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth in the directory page.
    pub fn init(&mut self, max_depth: u32) {
        self.max_depth = max_depth;
        self.global_depth = 0;
        self.local_depths = [0; HTABLE_DIRECTORY_ARRAY_SIZE as usize];
        self.bucket_page_ids = [INVALID_PAGE_ID; HTABLE_DIRECTORY_ARRAY_SIZE as usize];
        debug!("ExtendableHTableDirectoryPage initialized with page id: {}, max depth: {} at address {:p}", self.get_page_id(), max_depth, self);
        assert_eq!(self.max_depth, max_depth, "max_depth should match the initialized value.");
    }

    /// Returns the bucket index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The bucket index the key is hashed to.
    pub fn hash_to_bucket_index(&self, hash: u32) -> u32 {
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        hash & ((1 << self.global_depth) - 1)
    }

    /// Looks up a bucket page using a directory index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the directory to look up.
    ///
    /// # Returns
    /// The bucket page ID corresponding to the bucket index.
    pub fn get_bucket_page_id(&self, bucket_idx: usize) -> Option<PageId> {
        assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        if bucket_idx < self.bucket_page_ids.len() {
            let page_id = self.bucket_page_ids[bucket_idx];
            debug!(
                "Retrieved bucket page ID at index {}: {}",
                bucket_idx, page_id
            );
            return Some(page_id);
        }
        None
    }

    /// Updates the directory index using a bucket index and page ID.
    ///
    /// # Parameters
    /// - `bucket_idx`: The directory index at which to insert the page ID.
    /// - `bucket_page_id`: The page ID to insert.
    pub fn set_bucket_page_id(&mut self, bucket_idx: usize, bucket_page_id: PageId) {

        // Increment the instance counter
        ACCESS_COUNT.fetch_add(1, Ordering::SeqCst);
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        debug!(
        "Before setting: max_depth = {}, bucket_idx = {}, bucket_page_id = {}",
        self.max_depth, bucket_idx, bucket_page_id
    );

        self.bucket_page_ids[bucket_idx] = bucket_page_id;

        debug!(
        "After setting: max_depth = {}, bucket_idx = {}, bucket_page_id = {}",
        self.max_depth, bucket_idx, bucket_page_id
    );

        info!("Bucket page ID at index {} set to {}", bucket_idx, bucket_page_id);
    }

    /// Method to get the current instance count
    pub fn get_instance_count() -> usize {
        INSTANCE_COUNT.load(Ordering::SeqCst)
    }

    /// Method to get the current access count
    pub fn get_access_count() -> usize {
        ACCESS_COUNT.load(Ordering::SeqCst)
    }

    /// Gets the split image of an index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The directory index for which to find the split image.
    ///
    /// # Returns
    /// The directory index of the split image.
    pub fn get_split_image_index(&self, bucket_idx: u32) -> u32 {
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        // The split image index is determined by flipping the most significant bit
        // used in the local depth of the bucket index.

        let local_depth = self.get_local_depth(bucket_idx);
        let bit_to_flip = 1 << (local_depth - 1);

        // XOR the bucket_idx with the bit to flip to get the split image index
        bucket_idx ^ bit_to_flip
    }

    /// Returns a mask of global depth 1's and the rest 0's.
    ///
    /// # Returns
    /// A mask of global depth 1's and the rest 0's.
    pub fn get_global_depth_mask(&self) -> u32 {
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        (1 << self.global_depth) - 1
    }

    /// Returns a mask of local depth 1's and the rest 0's.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index to use for looking up local depth.
    ///
    /// # Returns
    /// A mask of local depth 1's and the rest 0's.
    pub fn get_local_depth_mask(&self, bucket_idx: u32) -> u32 {

        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        (1 << self.local_depths[bucket_idx as usize]) - 1
    }

    /// Returns the global depth of the hash table directory.
    ///
    /// # Returns
    /// The global depth of the directory.
    pub fn get_global_depth(&self) -> u32 {
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        self.global_depth
    }

    /// Returns the maximum depth of the directory.
    ///
    /// # Returns
    /// The maximum depth of the directory.
    pub fn get_max_depth(&self) -> u32 {
        // assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        debug!("Max depth: {}", self.max_depth);
        self.max_depth
    }

    /// Increments the global depth of the directory.
    pub fn incr_global_depth(&mut self) {
        assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        if self.global_depth == self.max_depth {
            panic!("Cannot increase global depth past max_depth: {}", self.max_depth)
        }

        // Step 1: Double the size of the directory
        let old_size = self.get_size();  // Current size of the directory
        self.global_depth += 1;      // Increment global depth
        let new_size = self.get_size();  // New size of the directory

        // Step 2: Copy existing entries to the new slots
        for i in 0..old_size {
            // The new entry is a copy of the current entry
            self.set_bucket_page_id(i as usize + old_size as usize, self.get_bucket_page_id(i as usize).unwrap());
            self.set_local_depth(i as usize + old_size as usize, self.get_local_depth(i));
        }

        // Step 3: Split buckets that need splitting after global depth increase
        // for i in 0..old_size {
        //     let local_depth = self.get_local_depth(i);
        //
        //     // If the local depth is equal to the old global depth, this bucket needs to be split
        //     if local_depth == self.global_depth - 1 {
        //         let new_bucket_page_id = self.get_split_image_index(i);
        //         self.split_bucket(i as usize, new_bucket_page_id as PageId);
        //     }
        // }

        debug!("Global depth increased. New size of the directory: {}", self.get_size());
    }

    /// Decrements the global depth of the directory.
    pub fn decr_global_depth(&mut self) {
        assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        self.global_depth -= 1;
    }

    /// Returns true if the directory can be shrunk.
    ///
    /// # Returns
    /// True if the directory can be shrunk, false otherwise.
    pub fn can_shrink(&self) -> bool {
        for local_depth in self.local_depths {
            if local_depth >= self.global_depth {
                return false;
            }
        }
        true
    }

    /// Returns the current directory size.
    ///
    /// # Returns
    /// The current directory size.
    pub fn get_size(&self) -> u32 {
        assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        assert!(self.global_depth < 32, "Global depth {} is too large!", self.global_depth);
        1 << self.global_depth
    }

    /// Returns the maximum directory size.
    ///
    /// # Returns
    /// The maximum directory size.
    pub fn max_size(&self) -> u32 {
        assert!(self.max_depth > 0, "max_depth should be initialized and greater than 0");

        HTABLE_DIRECTORY_ARRAY_SIZE as u32
    }

    /// Gets the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to look up.
    ///
    /// # Returns
    /// The local depth of the bucket at the specified index.
    pub fn get_local_depth(&self, bucket_idx: u32) -> u32 {
        self.local_depths[bucket_idx as usize]
    }

    /// Sets the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to update.
    /// - `local_depth`: The new local depth.
    pub fn set_local_depth(&mut self, bucket_idx: usize, local_depth: u32) {
        self.local_depths[bucket_idx] = local_depth;
    }

    /// Increments the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to increment.
    pub fn incr_local_depth(&mut self, bucket_idx: usize) {
        self.local_depths[bucket_idx] += 1;
    }

    /// Decrements the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to decrement.
    pub fn decr_local_depth(&mut self, bucket_idx: usize) {
        self.local_depths[bucket_idx] -= 1;
    }

    /// Splits the bucket at the given index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index of the bucket to be split.
    /// - `new_page_id`: The PageId of the new bucket created by the split.
    ///
    /// # Returns
    /// None.
    pub fn split_bucket(&mut self, bucket_idx: usize, new_page_id: PageId) {
        let local_depth = self.get_local_depth(bucket_idx as u32);

        // Check if the local depth equals the global depth
        if local_depth == self.global_depth {
            self.incr_global_depth();
        }

        // Calculate the split image index
        let split_image_index = self.get_split_image_index(bucket_idx as u32);

        // Set the new page ID for the split image index
        self.set_bucket_page_id(split_image_index as usize, new_page_id);

        // Increment the local depth of both the original bucket and the split bucket
        self.incr_local_depth(bucket_idx);
        self.set_local_depth(split_image_index as usize, self.get_local_depth(bucket_idx as u32));

        // Update the directory entries to point to the correct buckets
        let mask = self.get_local_depth_mask(bucket_idx as u32);
        let size = self.get_size();
        for i in 0..size {
            if i & mask == split_image_index {
                self.set_bucket_page_id(i as usize, new_page_id);
            } else if i & mask == bucket_idx as u32 {
                self.set_bucket_page_id(i as usize, self.get_bucket_page_id(bucket_idx).unwrap());
            }
        }

        debug!("Bucket at index {} split. New bucket ID {} set at index {}", bucket_idx, new_page_id, split_image_index);
    }

    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE] {
        let mut buffer = [0u8; DB_PAGE_SIZE];
        buffer[0..4].copy_from_slice(&self.global_depth.to_le_bytes());
        buffer[4..].copy_from_slice(self.base.get_data());
        buffer
    }

    pub fn deserialize(&mut self, buffer: &[u8]) {
        self.global_depth = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        self.base.set_data(0, &buffer[8..]).expect("Failed to set data");
    }

    /// Verifies the integrity of the directory.
    ///
    /// Ensures that:
    /// 1. All local depths are less than or equal to the global depth.
    /// 2. Each bucket has precisely 2^(global depth - local depth) pointers pointing to it.
    /// 3. The local depth is the same at each index with the same bucket page ID.
    pub fn verify_integrity(&self) {
        let size = self.get_size();
        debug!("Verifying integrity of directory with size: {}", size);

        // Ensure all local depths are less than or equal to the global depth
        debug!("Ensure all local depths are less than or equal to the global depth: {}", self.global_depth);
        assert_eq!(
            size,
            1 << self.global_depth,
            "Directory size {} does not match expected size for global depth {}",
            size,
            self.global_depth
        );

        for bucket_idx in 0..size {
            let local_depth = self.get_local_depth(bucket_idx);
            debug!(
            "Bucket idx: {}, Local depth: {}, Global depth: {}",
            bucket_idx, local_depth, self.global_depth
        );
            assert!(
                local_depth <= self.global_depth,
                "Local depth {} exceeds global depth {} at bucket_idx: {}",
                local_depth,
                self.global_depth,
                bucket_idx
            );
        }

        // Ensure each bucket has the correct number of pointers
        debug!("Ensure each bucket has the correct number of pointers:");

        let mut page_id_counts: HashMap<PageId, usize> = HashMap::new();

        for bucket_idx in 0..size {
            if let Some(page_id) = self.get_bucket_page_id(bucket_idx as usize) {
                *page_id_counts.entry(page_id).or_insert(0) += 1;
            }
        }

        // Now verify that each page_id has the expected number of pointers
        for (page_id, &count) in &page_id_counts {
            // Find the correct bucket index for this page ID
            let correct_bucket_idx = self.find_bucket_index_for_page_id(*page_id);
            let local_depth = self.get_local_depth(correct_bucket_idx.try_into().unwrap());
            let expected_count = 1 << (self.global_depth - local_depth);
            debug!(
            "Page ID: {}, Expected count: {}, Actual count: {}",
            page_id, expected_count, count
        );
            assert_eq!(
                count, expected_count,
                "Count mismatch for page_id: {}. Expected: {}, Found: {}",
                page_id, expected_count, count
            );
        }

        // Ensure the local depth is the same for all indices pointing to the same bucket
        debug!("Ensure the local depth is the same for all indices pointing to the same bucket:");
        for bucket_idx in 0..size {
            if let Some(page_id) = self.get_bucket_page_id(bucket_idx as usize) {
                for other_idx in 0..size {
                    if self.get_bucket_page_id(other_idx as usize) == Some(page_id) {
                        let local_depth_bucket = self.get_local_depth(bucket_idx);
                        let local_depth_other = self.get_local_depth(other_idx);
                        debug!(
                        "Bucket idx: {}, Other idx: {}, Page ID: {:?}, Local depth bucket: {}, Local depth other: {}",
                        bucket_idx, other_idx, page_id, local_depth_bucket, local_depth_other
                    );
                        assert_eq!(
                            local_depth_bucket,
                            local_depth_other,
                            "Local depth mismatch between bucket_idx: {} and other_idx: {} for page_id: {}",
                            bucket_idx,
                            other_idx,
                            page_id
                        );
                    }
                }
            }
        }
    }

    /// Prints the current directory.
    pub fn print_directory(&self) {
        let size = self.get_size();
        let header_bucket_idx = "bucket_idx";
        let header_page_id = "page_id";
        let header_local_depth = "local_depth";

        let max_bucket_idx_width = std::cmp::max(header_bucket_idx.len(), size.to_string().len());
        let max_page_id_width = std::cmp::max(header_page_id.len(), self.bucket_page_ids.iter().take(size as usize).map(|&page_id| page_id.to_string().len()).max().unwrap_or(0));
        let max_local_depth_width = std::cmp::max(header_local_depth.len(), self.local_depths.iter().take(size as usize).map(|&local_depth| local_depth.to_string().len()).max().unwrap_or(0));

        println!(
            "======== DIRECTORY (size: {} | global_depth: {} | max_depth: {} | local_depths: {}) ========",
            size, self.get_global_depth(), self.get_max_depth(), self.local_depths.len()
        );
        println!(
            "| {:<width_bucket_idx$} | {:<width_page_id$} | {:<width_local_depth$} |",
            header_bucket_idx, header_page_id, header_local_depth,
            width_bucket_idx = max_bucket_idx_width,
            width_page_id = max_page_id_width,
            width_local_depth = max_local_depth_width
        );

        for ((idx, &page_id), &local_depth) in self.bucket_page_ids.iter().take(size as usize).enumerate().zip(self.local_depths.iter().take(size as usize)) {
            println!(
                "| {:<width_bucket_idx$} | {:<width_page_id$} | {:<width_local_depth$} |",
                idx, page_id, local_depth,
                width_bucket_idx = max_bucket_idx_width,
                width_page_id = max_page_id_width,
                width_local_depth = max_local_depth_width
            );
        }
        println!("================ END DIRECTORY ================");
    }

    // Helper method to find the correct bucket index for a given page ID
    fn find_bucket_index_for_page_id(&self, page_id: PageId) -> usize {
        for idx in 0..self.get_size() {
            if self.get_bucket_page_id(idx.try_into().unwrap()) == Some(page_id) {
                return idx.try_into().unwrap();
            }
        }
        panic!("Page ID {} not found in directory", page_id);
    }
}

impl PageTrait for ExtendableHTableDirectoryPage {
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

impl Debug for ExtendableHTableDirectoryPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl TryFrom<PageType> for ExtendableHTableDirectoryPage {
    type Error = ();

    fn try_from(page_type: PageType) -> Result<Self, Self::Error> {
        match page_type {
            PageType::ExtendedHashTableDirectory(page) => Ok(page),
            _ => Err(()),
        }
    }
}

impl AsAny for ExtendableHTableDirectoryPage {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
