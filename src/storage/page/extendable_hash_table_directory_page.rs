use crate::common::config::{PageId, INVALID_PAGE_ID};
use log::{debug, info};

pub const HTABLE_DIRECTORY_MAX_DEPTH: u64 = 9;
pub const HTABLE_DIRECTORY_ARRAY_SIZE: u64 = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

#[derive(Clone)]
pub struct ExtendableHTableDirectoryPage {
    max_depth: u32,
    global_depth: u32,
    local_depths: [u32; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
    bucket_page_ids: [PageId; HTABLE_DIRECTORY_ARRAY_SIZE as usize],
}

impl ExtendableHTableDirectoryPage {
    /// Initializes a new directory page with the specified maximum depth.
    ///
    /// # Parameters
    /// - `max_depth`: The maximum depth in the directory page.
    pub fn init(&mut self, max_depth: u32) {
        self.max_depth = max_depth;
        self.global_depth = 0;
        self.local_depths.fill(0);
        self.bucket_page_ids.fill(PageId::default());
    }

    /// Returns the bucket index that the key is hashed to.
    ///
    /// # Parameters
    /// - `hash`: The hash of the key.
    ///
    /// # Returns
    /// The bucket index the key is hashed to.
    pub fn hash_to_bucket_index(&self, hash: u32) -> u32 {
        hash & ((1 << self.global_depth) - 1)
    }

    /// Looks up a bucket page using a directory index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the directory to look up.
    ///
    /// # Returns
    /// The bucket page ID corresponding to the bucket index.
    pub fn get_bucket_page_id(&self, bucket_idx: u32) -> PageId {
        self.bucket_page_ids[bucket_idx as usize]
    }

    /// Updates the directory index using a bucket index and page ID.
    ///
    /// # Parameters
    /// - `bucket_idx`: The directory index at which to insert the page ID.
    /// - `bucket_page_id`: The page ID to insert.
    pub fn set_bucket_page_id(&mut self, bucket_idx: u32, bucket_page_id: PageId) {
        self.bucket_page_ids[bucket_idx as usize] = bucket_page_id;
    }

    /// Gets the split image of an index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The directory index for which to find the split image.
    ///
    /// # Returns
    /// The directory index of the split image.
    pub fn get_split_image_index(&self, bucket_idx: u32) -> u32 {
        bucket_idx ^ (1 << (self.get_local_depth(bucket_idx) - 1))
    }

    /// Returns a mask of global depth 1's and the rest 0's.
    ///
    /// # Returns
    /// A mask of global depth 1's and the rest 0's.
    pub fn get_global_depth_mask(&self) -> u32 {
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
        (1 << self.local_depths[bucket_idx as usize]) - 1
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
        self.max_depth
    }

    /// Increments the global depth of the directory.
    pub fn incr_global_depth(&mut self) {
        self.global_depth += 1;
    }

    /// Decrements the global depth of the directory.
    pub fn decr_global_depth(&mut self) {
        self.global_depth -= 1;
    }

    /// Returns true if the directory can be shrunk.
    ///
    /// # Returns
    /// True if the directory can be shrunk, false otherwise.
    pub fn can_shrink(&self) -> bool {
        self.global_depth > 0
    }

    /// Returns the current directory size.
    ///
    /// # Returns
    /// The current directory size.
    pub fn size(&self) -> u32 {
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
        self.local_depths[bucket_idx as usize] as u32
    }

    /// Sets the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to update.
    /// - `local_depth`: The new local depth.
    pub fn set_local_depth(&mut self, bucket_idx: u32, local_depth: u32) {
        self.local_depths[bucket_idx as usize] = local_depth;
    }

    /// Increments the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to increment.
    pub fn incr_local_depth(&mut self, bucket_idx: u32) {
        self.local_depths[bucket_idx as usize] += 1;
    }

    /// Decrements the local depth of the bucket at the specified index.
    ///
    /// # Parameters
    /// - `bucket_idx`: The bucket index to decrement.
    pub fn decr_local_depth(&mut self, bucket_idx: u32) {
        self.local_depths[bucket_idx as usize] -= 1;
    }

    /// Verifies the integrity of the directory.
    ///
    /// Ensures that:
    /// 1. All local depths are less than or equal to the global depth.
    /// 2. Each bucket has precisely 2^(global depth - local depth) pointers pointing to it.
    /// 3. The local depth is the same at each index with the same bucket page ID.
    /// Verifies the integrity of the directory.
    ///
    /// Ensures that:
    /// 1. All local depths are less than or equal to the global depth.
    /// 2. Each bucket has precisely 2^(global depth - local depth) pointers pointing to it.
    /// 3. The local depth is the same at each index with the same bucket page ID.
    pub fn verify_integrity(&self) {
        let size = self.size();
        debug!("Verifying integrity of directory with size: {}", size);

        // Ensure all local depths are less than or equal to the global depth
        debug!("Ensure all local depths are less than or equal to the global depth:");
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
        let mut count = vec![0; HTABLE_DIRECTORY_ARRAY_SIZE as usize];
        for bucket_idx in 0..size {
            let page_id = self.get_bucket_page_id(bucket_idx);
            if page_id != INVALID_PAGE_ID {
                count[page_id as usize] += 1;
            }
        }

        for bucket_idx in 0..size {
            let page_id = self.get_bucket_page_id(bucket_idx);
            if page_id != INVALID_PAGE_ID {
                let expected_count = 1 << (self.global_depth - self.get_local_depth(bucket_idx));
                debug!(
                    "Bucket idx: {}, Page ID: {}, Expected count: {}, Actual count: {}",
                    bucket_idx, page_id, expected_count, count[page_id as usize]
                );
                assert_eq!(
                    count[page_id as usize], expected_count,
                    "Count mismatch at bucket_idx: {} for page_id: {}. Expected: {}, Found: {}",
                    bucket_idx, page_id, expected_count, count[page_id as usize]
                );
            }
        }

        // Ensure the local depth is the same for all indices pointing to the same bucket
        debug!("Ensure the local depth is the same for all indices pointing to the same bucket:");
        for bucket_idx in 0..size {
            let page_id = self.get_bucket_page_id(bucket_idx);
            for other_idx in 0..size {
                if self.get_bucket_page_id(other_idx) == page_id {
                    let local_depth_bucket = self.get_local_depth(bucket_idx);
                    let local_depth_other = self.get_local_depth(other_idx);
                    debug!("Bucket idx: {}, Other idx: {}, Page ID: {}, Local depth bucket: {}, Local depth other: {}", bucket_idx, other_idx, page_id, local_depth_bucket, local_depth_other);
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

    /// Prints the current directory.
    pub fn print_directory(&self) {
        info!("ExtendableHTableDirectoryPage:");
        info!("Max depth: {}", self.max_depth);
        info!("Global depth: {}", self.global_depth);
        for (i, &page_id) in self.bucket_page_ids.iter().enumerate() {
            info!(
                "Bucket {}: Page ID {} (Local depth {})",
                i, page_id, self.local_depths[i]
            );
        }
    }
}
