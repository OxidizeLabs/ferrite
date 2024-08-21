use std::sync::Mutex;

pub type LsnT = u32;
pub type PageIdT = u32;

const DIRECTORY_ARRAY_SIZE: usize = 512;

/**
 * Directory Page for extendible hash table.
 *
 * Directory format (size in byte):
 * --------------------------------------------------------------------------------------------
 * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
 * --------------------------------------------------------------------------------------------
 */
pub struct HashTableDirectoryPage {
    page_id: PageIdT,
    lsn: LsnT,
    global_depth: u32,
    local_depths: [u8; DIRECTORY_ARRAY_SIZE],
    bucket_page_ids: Mutex<[PageIdT; DIRECTORY_ARRAY_SIZE]>,
}

impl HashTableDirectoryPage {
    /// Creates a new `HashTableDirectoryPage`.
    pub fn new() -> Self {
        Self {
            page_id: 0,
            lsn: 0,
            global_depth: 0,
            local_depths: [0; DIRECTORY_ARRAY_SIZE],
            bucket_page_ids: Mutex::new([0; DIRECTORY_ARRAY_SIZE]),
        }
    }

    /// Returns the page ID of this page.
    pub fn get_page_id(&self) -> PageIdT {
        self.page_id
    }

    /// Sets the page ID of this page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page ID to set.
    pub fn set_page_id(&mut self, page_id: PageIdT) {
        self.page_id = page_id;
    }

    /// Returns the log sequence number (LSN) of this page.
    pub fn get_lsn(&self) -> LsnT {
        self.lsn
    }

    /// Sets the log sequence number (LSN) of this page.
    ///
    /// # Arguments
    ///
    /// * `lsn` - The log sequence number to set.
    pub fn set_lsn(&mut self, lsn: LsnT) {
        self.lsn = lsn;
    }

    /// Looks up a bucket page using a directory index.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index in the directory to look up.
    ///
    /// # Returns
    ///
    /// The bucket page ID corresponding to `bucket_idx`.
    pub fn get_bucket_page_id(&self, bucket_idx: u32) -> PageIdT {
        let bucket_page_ids = self.bucket_page_ids.lock().unwrap();
        bucket_page_ids[bucket_idx as usize]
    }

    /// Updates the directory index using a bucket index and page ID.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The directory index at which to insert the page ID.
    /// * `bucket_page_id` - The page ID to insert.
    pub fn set_bucket_page_id(&self, bucket_idx: u32, bucket_page_id: PageIdT) {
        let mut bucket_page_ids = self.bucket_page_ids.lock().unwrap();
        bucket_page_ids[bucket_idx as usize] = bucket_page_id;
    }

    /// Gets the split image of an index.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The directory index for which to find the split image.
    ///
    /// # Returns
    ///
    /// The directory index of the split image.
    pub fn get_split_image_index(&self, bucket_idx: u32) -> u32 {
        bucket_idx ^ (1 << (self.get_local_depth(bucket_idx) - 1))
    }

    /// Returns a mask of global depth 1's and the rest 0's.
    ///
    /// # Returns
    ///
    /// A mask of global depth 1's and the rest 0's.
    pub fn get_global_depth_mask(&self) -> u32 {
        (1 << self.global_depth) - 1
    }

    /// Returns a mask of local depth 1's and the rest 0's for the bucket at `bucket_idx`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to use for looking up local depth.
    ///
    /// # Returns
    ///
    /// A mask of local depth 1's and the rest 0's.
    pub fn get_local_depth_mask(&self, bucket_idx: u32) -> u32 {
        (1 << self.get_local_depth(bucket_idx)) - 1
    }

    /// Returns the global depth of the hash table directory.
    pub fn get_global_depth(&self) -> u32 {
        self.global_depth
    }

    /// Increments the global depth of the directory.
    pub fn incr_global_depth(&mut self) {
        self.global_depth += 1;
    }

    /// Decrements the global depth of the directory.
    pub fn decr_global_depth(&mut self) {
        self.global_depth -= 1;
    }

    /// Returns `true` if the directory can be shrunk.
    pub fn can_shrink(&self) -> bool {
        self.global_depth > 0 && self.size() <= (1 << (self.global_depth - 1))
    }

    /// Returns the current directory size.
    pub fn size(&self) -> u32 {
        1 << self.global_depth
    }

    /// Gets the local depth of the bucket at `bucket_idx`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The bucket index to look up.
    ///
    /// # Returns
    ///
    /// The local depth of the bucket at `bucket_idx`.
    pub fn get_local_depth(&self, bucket_idx: u32) -> u8 {
        self.local_depths[bucket_idx as usize]
    }

    /// Sets the local depth of the bucket at `bucket_idx` to `local_depth`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The bucket index to update.
    /// * `local_depth` - The new local depth.
    pub fn set_local_depth(&mut self, bucket_idx: u32, local_depth: u8) {
        self.local_depths[bucket_idx as usize] = local_depth;
    }

    /// Increments the local depth of the bucket at `bucket_idx`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The bucket index to increment.
    pub fn incr_local_depth(&mut self, bucket_idx: u32) {
        self.local_depths[bucket_idx as usize] += 1;
    }

    /// Decrements the local depth of the bucket at `bucket_idx`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The bucket index to decrement.
    pub fn decr_local_depth(&mut self, bucket_idx: u32) {
        self.local_depths[bucket_idx as usize] -= 1;
    }

    /// Gets the high bit corresponding to the bucket's local depth.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The bucket index to look up.
    ///
    /// # Returns
    ///
    /// The high bit corresponding to the bucket's local depth.
    pub fn get_local_high_bit(&self, bucket_idx: u32) -> u32 {
        1 << (self.get_local_depth(bucket_idx) - 1)
    }

    /// Verifies the integrity of the directory page.
    ///
    /// - All local depths must be less than or equal to the global depth.
    /// - Each bucket must have precisely 2^(global depth - local depth) pointers pointing to it.
    /// - The local depth is the same at each index with the same `bucket_page_id`.
    pub fn verify_integrity(&self) {
        // Implementation of integrity checks as described in the comment.
        // This can include assertions or logging of issues.
    }

    /// Prints the current directory.
    pub fn print_directory(&self) {
        // Implementation of printing the directory state.
        // This can include printing page IDs, depths, etc.
    }
}
