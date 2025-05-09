use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::storage::page::page::Page;
use crate::storage::page::page::PAGE_TYPE_OFFSET;
use crate::storage::page::page::{PageTrait, PageType, PageTypeId};
use log::{debug, info, warn};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const HTABLE_DIRECTORY_MAX_DEPTH: u64 = 9;
pub const HTABLE_DIRECTORY_ARRAY_SIZE: u64 = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

// Static variables to hold the instance and access counts
static INSTANCE_COUNT: AtomicUsize = AtomicUsize::new(0);
static ACCESS_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
pub struct ExtendableHTableDirectoryPage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    max_depth: u32,
    global_depth: u32,
    local_depths: [u32; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
    bucket_page_ids: [PageId; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
}

impl ExtendableHTableDirectoryPage {
    /// Initializes a new directory page.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth of the directory.
    pub fn init(&mut self, max_depth: u32) {
        self.max_depth = max_depth;
        self.global_depth = 0;

        // Initialize all local depths to 0
        for depth in self.local_depths.iter_mut() {
            *depth = 0;
        }

        // Initialize all bucket page IDs to INVALID_PAGE_ID
        for page_id in self.bucket_page_ids.iter_mut() {
            *page_id = INVALID_PAGE_ID;
        }

        debug!(
            "ExtendableHTableDirectoryPage initialized with max_depth: {}, global_depth: {}",
            max_depth, self.global_depth
        );
    }

    /// Returns the bucket index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The bucket index the key is hashed to.
    pub fn hash_to_bucket_index(&self, hash: u32) -> u32 {
        hash & self.get_global_depth_mask()
    }

    /// Looks up a bucket page using a directory index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the directory to look up.
    ///
    /// # Returns
    /// The bucket page ID corresponding to the bucket index.
    pub fn get_bucket_page_id(&self, bucket_idx: usize) -> Option<PageId> {
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

        info!(
            "Bucket page ID at index {} set to {}",
            bucket_idx, bucket_page_id
        );
    }

    /// Method to get the current instance count
    pub fn get_instance_count() -> usize {
        INSTANCE_COUNT.load(Ordering::SeqCst)
    }

    /// Method to get the current access count
    pub fn get_access_count() -> usize {
        ACCESS_COUNT.load(Ordering::SeqCst)
    }

    /// Gets the split image index for a bucket at the given index
    pub fn get_split_image_index(&self, bucket_idx: u32) -> u32 {
        let local_depth = self.get_local_depth(bucket_idx);

        // Handle case where local_depth is 0
        if local_depth == 0 {
            debug!(
                "Local depth is 0 for bucket_idx={}, returning bucket_idx + 1",
                bucket_idx
            );
            return bucket_idx + 1;
        }

        // Calculate split image using the distinguishing bit at the local depth position
        let distinguishing_bit = 1 << (local_depth - 1);
        let split_idx = bucket_idx ^ distinguishing_bit;

        debug!(
            "Calculating split image for bucket_idx={}, local_depth={}, distinguishing_bit={}, split_idx={}",
            bucket_idx, local_depth, distinguishing_bit, split_idx
        );

        split_idx
    }

    /// Returns a mask of global depth 1's and the rest 0's.
    ///
    /// # Returns
    /// A mask of global depth 1's and the rest 0's.
    pub fn get_global_depth_mask(&self) -> u32 {
        (1 << self.global_depth) - 1
    }

    /// Gets the mask for the local depth of a bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index of the bucket to get the mask for.
    ///
    /// # Returns
    /// The mask for the local depth.
    pub fn get_local_depth_mask(&self, bucket_idx: u32) -> u32 {
        let local_depth = self.get_local_depth(bucket_idx);
        if local_depth == 0 {
            return 0;
        }
        (1 << local_depth) - 1
    }

    /// Returns the global depth of the hash table directory.
    ///
    /// # Returns
    /// The global depth of the directory.
    pub fn get_global_depth(&self) -> u32 {
        self.global_depth
    }

    /// Returns the maximum depth of the directory.
    ///
    /// # Returns
    /// The maximum depth of the directory.
    pub fn get_max_depth(&self) -> u32 {
        debug!("Max depth: {}", self.max_depth);
        self.max_depth
    }

    /// Updates directory entries for a bucket split
    pub fn update_directory_for_split(
        &mut self,
        bucket_idx: usize,
        new_page_id: PageId,
    ) -> Option<usize> {
        let local_depth = self.get_local_depth(bucket_idx as u32);
        let new_local_depth = local_depth + 1;

        debug!(
            "Updating directory for split: bucket_idx={}, local_depth={}, new_local_depth={}",
            bucket_idx, local_depth, new_local_depth
        );

        // Check if we need to grow directory
        while new_local_depth > self.global_depth {
            if !self.grow_directory() {
                debug!("Failed to grow directory during split");
                return None;
            }
        }

        // Calculate masks for redistribution
        let mask = (1 << local_depth) - 1;
        let distinguishing_bit = 1 << local_depth;

        debug!(
            "Split masks: mask={}, distinguishing_bit={}, global_depth={}",
            mask, distinguishing_bit, self.global_depth
        );

        // Get the old bucket page ID
        let old_bucket_page_id = self
            .get_bucket_page_id(bucket_idx)
            .expect("Bucket being split must exist");

        // Update directory entries
        for i in 0..(1 << self.global_depth) {
            if (i & mask) == (bucket_idx & mask) {
                // Update local depth for all affected entries
                self.set_local_depth(i, new_local_depth);

                // Determine which entries point to new bucket
                if i & distinguishing_bit != 0 {
                    self.set_bucket_page_id(i, new_page_id);
                    debug!(
                        "Directory entry {} now points to new bucket {}",
                        i, new_page_id
                    );
                } else {
                    self.set_bucket_page_id(i, old_bucket_page_id);
                    debug!(
                        "Directory entry {} keeps old bucket {}",
                        i, old_bucket_page_id
                    );
                }
            }
        }

        // Get split image index for returning
        let split_idx = self.get_split_image_index(bucket_idx as u32) as usize;

        debug!(
            "Directory split complete: bucket_idx={}, split_idx={}, new_local_depth={}",
            bucket_idx, split_idx, new_local_depth
        );

        Some(split_idx)
    }

    /// Increments the global depth (now just a thin wrapper)
    pub fn incr_global_depth(&mut self) {
        self.grow_directory();
    }

    /// Decrements the global depth of the directory.
    pub fn decr_global_depth(&mut self) {
        if self.global_depth == 0 {
            warn!("Global depth is already at zero!")
        } else {
            self.global_depth -= 1;
        }
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
        assert!(
            self.global_depth < 32,
            "Global depth {} is too large!",
            self.global_depth
        );
        1 << self.global_depth
    }

    /// Returns the maximum directory size.
    ///
    /// # Returns
    /// The maximum directory size.
    pub fn max_size(&self) -> u32 {
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

    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE as usize] {
        *self.data
    }

    pub fn deserialize(&mut self, buffer: &[u8]) {
        self.data.copy_from_slice(buffer);
        self.global_depth = u32::from_le_bytes(buffer[4..8].try_into().unwrap());

        // Consider deserializing other fields from the buffer as well
        // This might include local_depths and bucket_page_ids
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
        debug!(
            "Ensure all local depths are less than or equal to the global depth: {}",
            self.global_depth
        );
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
                            local_depth_bucket, local_depth_other,
                            "Local depth mismatch between bucket_idx: {} and other_idx: {} for page_id: {}",
                            bucket_idx, other_idx, page_id
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
        let max_page_id_width = std::cmp::max(
            header_page_id.len(),
            self.bucket_page_ids
                .iter()
                .take(size as usize)
                .map(|&page_id| page_id.to_string().len())
                .max()
                .unwrap_or(0),
        );
        let max_local_depth_width = std::cmp::max(
            header_local_depth.len(),
            self.local_depths
                .iter()
                .take(size as usize)
                .map(|&local_depth| local_depth.to_string().len())
                .max()
                .unwrap_or(0),
        );

        println!(
            "======== DIRECTORY (size: {} | global_depth: {} | max_depth: {} | local_depths: {}) ========",
            size,
            self.get_global_depth(),
            self.get_max_depth(),
            self.local_depths.len()
        );
        println!(
            "| {:<width_bucket_idx$} | {:<width_page_id$} | {:<width_local_depth$} |",
            header_bucket_idx,
            header_page_id,
            header_local_depth,
            width_bucket_idx = max_bucket_idx_width,
            width_page_id = max_page_id_width,
            width_local_depth = max_local_depth_width
        );

        for ((idx, &page_id), &local_depth) in self
            .bucket_page_ids
            .iter()
            .take(size as usize)
            .enumerate()
            .zip(self.local_depths.iter().take(size as usize))
        {
            println!(
                "| {:<width_bucket_idx$} | {:<width_page_id$} | {:<width_local_depth$} |",
                idx,
                page_id,
                local_depth,
                width_bucket_idx = max_bucket_idx_width,
                width_page_id = max_page_id_width,
                width_local_depth = max_local_depth_width
            );
        }
        println!("================ END DIRECTORY ================");
    }

    /// Splits a bucket by incrementing its local depth and redistributing entries
    pub fn split_bucket(&mut self, bucket_index: usize, new_bucket_page_id: PageId) {
        let local_depth = self.get_local_depth(bucket_index as u32);

        // Check if we need to grow the directory
        if local_depth >= self.global_depth {
            if !self.grow_directory() {
                debug!("Failed to grow directory during split");
                return;
            }
        }

        // Calculate masks for redistribution
        let mask = (1 << local_depth) - 1;
        let split_point = 1 << local_depth;
        let new_local_depth = local_depth + 1;

        debug!(
            "Splitting bucket {} with local_depth {} to new_local_depth {}, split_point {}",
            bucket_index, local_depth, new_local_depth, split_point
        );

        // Get the old bucket page ID
        let old_bucket_page_id = self
            .get_bucket_page_id(bucket_index)
            .expect("Bucket being split must exist");

        // Update directory entries
        for i in 0..(1 << self.global_depth) {
            if (i & mask) == (bucket_index & mask) {
                // Update local depth for all affected entries
                self.set_local_depth(i, new_local_depth);

                // Determine which entries point to new bucket
                if i & split_point != 0 {
                    self.set_bucket_page_id(i, new_bucket_page_id);
                    debug!(
                        "Directory entry {} now points to new bucket {}",
                        i, new_bucket_page_id
                    );
                } else {
                    self.set_bucket_page_id(i, old_bucket_page_id);
                    debug!(
                        "Directory entry {} keeps old bucket {}",
                        i, old_bucket_page_id
                    );
                }
            }
        }
    }

    /// Merges a bucket with its buddy bucket when possible
    pub fn merge_bucket(&mut self, bucket_index: usize) -> Option<PageId> {
        let local_depth = self.get_local_depth(bucket_index as u32);
        if local_depth == 0 {
            return None;
        }

        // Find buddy bucket index by flipping the highest bit of local depth
        let buddy_bucket_index = bucket_index ^ (1 << (local_depth - 1));
        let buddy_local_depth = self.get_local_depth(buddy_bucket_index as u32);

        // Can only merge if buddy has same local depth
        if buddy_local_depth != local_depth {
            return None;
        }

        let buddy_page_id = self.get_bucket_page_id(buddy_bucket_index)?;

        // Update all entries pointing to this bucket to point to buddy
        let mask = (1 << local_depth) - 1;
        for i in 0..(1 << self.get_global_depth()) {
            if (i & mask) == bucket_index {
                self.set_bucket_page_id(i, buddy_page_id);
                self.set_local_depth(i, local_depth - 1);
            }
        }

        // Return the page ID of the bucket being removed
        Some(self.get_bucket_page_id(bucket_index)?)
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

    /// Grows the directory by doubling its size
    fn grow_directory(&mut self) -> bool {
        if self.global_depth >= self.max_depth {
            debug!(
                "Cannot increase global depth {} past max depth {}",
                self.global_depth, self.max_depth
            );
            return false;
        }

        let old_size = self.get_size();
        self.global_depth += 1;
        self.copy_entries_for_growth(old_size);
        true
    }

    /// Copies entries when growing the directory
    fn copy_entries_for_growth(&mut self, old_size: u32) {
        // First clear the new section
        for i in old_size..self.get_size() {
            self.set_bucket_page_id(i as usize, INVALID_PAGE_ID);
        }

        // Then copy from end to start to avoid overwriting
        for i in (0..old_size).rev() {
            if let Some(bucket_page_id) = self.get_bucket_page_id(i as usize) {
                let new_idx = i as usize + old_size as usize;
                let local_depth = self.get_local_depth(i);
                self.set_bucket_page_id(new_idx, bucket_page_id);
                self.set_local_depth(new_idx, local_depth);
            }
        }
    }
}

impl PageTypeId for ExtendableHTableDirectoryPage {
    const TYPE_ID: PageType = PageType::HashTableDirectory;
}

impl PageTrait for ExtendableHTableDirectoryPage {
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
        self.max_depth = 0;
        self.global_depth = 0;
        self.local_depths = [0; HTABLE_DIRECTORY_ARRAY_SIZE as usize];
        self.bucket_page_ids = [INVALID_PAGE_ID; HTABLE_DIRECTORY_ARRAY_SIZE as usize];
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

impl Page for ExtendableHTableDirectoryPage {
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            max_depth: 0,
            global_depth: 0,
            local_depths: [0; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
            bucket_page_ids: [INVALID_PAGE_ID; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::logger::initialize_logger;

    fn create_directory_page() -> ExtendableHTableDirectoryPage {
        let mut page = ExtendableHTableDirectoryPage {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id: 0,
            pin_count: 1,
            is_dirty: false,
            max_depth: 0,
            global_depth: 0,
            local_depths: [0; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
            bucket_page_ids: [INVALID_PAGE_ID; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
        };
        page.data[PAGE_TYPE_OFFSET] = PageType::HashTableDirectory.to_u8();
        page
    }

    #[test]
    fn test_directory_update_for_split() {
        initialize_logger();
        let mut page = create_directory_page();

        // Initial setup
        page.init(4); // max_depth = 4
        page.set_bucket_page_id(0, 100);
        page.set_local_depth(0, 1);
        page.global_depth = 1; // Start with global depth 1

        // Test basic directory update
        let split_result = page.update_directory_for_split(0, 101);
        assert!(split_result.is_some());
        let split_idx = split_result.unwrap();

        // Verify directory state after split
        assert_eq!(page.get_local_depth(0), 2); // Original bucket depth increased
        assert_eq!(page.get_local_depth(split_idx as u32), 2); // Split bucket has same depth
        assert_eq!(page.get_global_depth(), 2); // Global depth increased
    }

    #[test]
    fn test_directory_growth_during_split() {
        initialize_logger();
        let mut page = create_directory_page();

        // Setup with global_depth = 0
        page.init(4);
        page.set_bucket_page_id(0, 100);
        page.set_local_depth(0, 1);
        assert_eq!(page.get_global_depth(), 0);
        assert_eq!(page.get_size(), 1);

        // Update should trigger directory growth
        let split_result = page.update_directory_for_split(0, 101);
        assert!(split_result.is_some());

        // Verify directory grew
        assert_eq!(page.get_global_depth(), 2); // Grew to accommodate new local depth
        assert_eq!(page.get_size(), 4);
    }

    #[test]
    fn test_max_depth_limit_for_split() {
        initialize_logger();
        let mut page = create_directory_page();

        // Setup with small max_depth
        page.init(2); // max_depth = 2
        page.set_bucket_page_id(0, 100);
        page.set_local_depth(0, 1);

        // First split should succeed
        let split_result1 = page.update_directory_for_split(0, 101);
        assert!(split_result1.is_some());
        assert_eq!(page.get_global_depth(), 2);

        // Second split should fail due to max depth
        let split_result2 = page.update_directory_for_split(0, 102);
        assert!(split_result2.is_none()); // Should return None when can't grow
        assert_eq!(page.get_global_depth(), 2); // Should not change
    }

    #[test]
    fn test_split_image_calculation() {
        initialize_logger();
        let mut page = create_directory_page();

        // Setup
        page.init(4);
        page.global_depth = 2;
        page.set_bucket_page_id(0, 100);

        // Test case 1: local_depth = 1
        page.set_local_depth(0, 1);
        let split_idx = page.get_split_image_index(0);
        assert_eq!(
            split_idx, 1,
            "With local_depth=1, split image of 0 should be 1 (0^1)"
        );

        // Test case 2: local_depth = 2
        page.set_local_depth(0, 2);
        let split_idx = page.get_split_image_index(0);
        assert_eq!(
            split_idx, 2,
            "With local_depth=2, split image of 0 should be 2 (0^2)"
        );

        // Test case 3: bucket_idx = 1, local_depth = 2
        page.set_local_depth(1, 2);
        let split_idx = page.get_split_image_index(1);
        assert_eq!(
            split_idx, 3,
            "With local_depth=2, split image of 1 should be 3 (1^2)"
        );

        // Test case 4: bucket_idx = 2, local_depth = 2
        page.set_local_depth(2, 2);
        let split_idx = page.get_split_image_index(2);
        assert_eq!(
            split_idx, 0,
            "With local_depth=2, split image of 2 should be 0 (2^2)"
        );
    }

    #[test]
    fn test_directory_state_consistency() {
        initialize_logger();
        let mut page = create_directory_page();

        // Setup
        page.init(4);
        page.set_bucket_page_id(0, 100);
        page.set_local_depth(0, 1);

        // Perform series of splits
        let split1 = page.update_directory_for_split(0, 101).unwrap();
        let split2 = page.update_directory_for_split(split1, 102).unwrap();

        // Verify directory state
        assert!(page.get_global_depth() >= page.get_local_depth(0));
        assert!(page.get_global_depth() >= page.get_local_depth(split1 as u32));
        assert!(page.get_global_depth() >= page.get_local_depth(split2 as u32));

        // Verify size is consistent with global depth
        assert_eq!(page.get_size(), 1 << page.get_global_depth());
    }
}
