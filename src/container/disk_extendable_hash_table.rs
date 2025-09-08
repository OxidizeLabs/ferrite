use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::common::config::INVALID_PAGE_ID;
use crate::common::config::PageId;
use crate::common::rid::RID;
use crate::container::hash_function::HashFunction;
use crate::storage::page::PageTrait;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::types_db::value::Value;
use log::{debug, info};
use std::sync::Arc;

/// Implementation of an extendable hash table backed by a buffer pool manager.
/// Non-unique keys are supported. Supports insert and delete. The table grows/shrinks
/// dynamically as buckets become full/empty.
pub struct DiskExtendableHashTable {
    bpm: Arc<BufferPoolManager>,
    hash_fn: HashFunction<Value>,
    directory_max_depth: u32,
    bucket_max_size: u32,
    header_page_id: PageId,
}

impl DiskExtendableHashTable {
    /// Creates a new `DiskExtendableHashTable`.
    pub fn new(
        name: String,
        bpm: Arc<BufferPoolManager>,
        hash_fn: HashFunction<Value>,
        header_max_depth: u32,
        directory_max_depth: u32,
        bucket_max_size: u32,
    ) -> Result<Self, String> {
        info!("Creating DiskExtendableHashTable with index name: {}", name);

        // Create header page
        let header_page = bpm
            .new_page::<ExtendableHTableHeaderPage>()
            .ok_or_else(|| "Failed to create header page".to_string())?;
        let header_page_id = {
            let header = header_page.read();
            header.get_page_id()
        };

        // Initialize header page
        {
            let mut header = header_page.write();
            header.init(header_max_depth);
            debug!(
                "Initialized header page with max depth: {}",
                header_max_depth
            );
        }

        // Create directory page
        let directory_page = bpm
            .new_page::<ExtendableHTableDirectoryPage>()
            .ok_or_else(|| "Failed to create directory page".to_string())?;
        let directory_page_id = directory_page.read().get_page_id();

        // Initialize directory page
        {
            let mut directory = directory_page.write();
            directory.init(directory_max_depth);

            // Create first bucket page
            let bucket_page = bpm
                .new_page::<ExtendableHTableBucketPage>()
                .ok_or_else(|| "Failed to create bucket page".to_string())?;
            let bucket_page_id = bucket_page.read().get_page_id();

            // Initialize bucket page
            {
                let mut bucket = bucket_page.write();
                bucket.init(bucket_max_size as u16);
            }

            // Set up initial directory entry
            directory.set_bucket_page_id(0, bucket_page_id);
            debug!(
                "Set initial bucket page {} in directory at index 0",
                bucket_page_id
            );
        }

        // Update header page with directory page ID
        {
            let mut header = header_page.write();
            // Set directory page ID for all possible directory indices initially
            for i in 0..(1 << header_max_depth) {
                header.set_directory_page_id(i, directory_page_id);
            }
            debug!(
                "Set directory page {} in header for all indices",
                directory_page_id
            );
        }

        Ok(Self {
            bpm,
            hash_fn,
            directory_max_depth,
            bucket_max_size,
            header_page_id,
        })
    }

    const MAX_INSERT_RETRIES: usize = 10;

    /// Public insert API (uses a helper with retry count).
    pub fn insert(&mut self, key: Value, value: RID) -> bool {
        debug!("Starting insert operation for key: {:?}", key);
        let mut attempt = 0;
        loop {
            debug!("Insert attempt {} for key: {:?}", attempt, key);
            let hash = self.hash_fn.get_hash(&key) as u32;
            debug!("Computed hash: {} for key: {:?}", hash, key);

            // Get directory index from header page
            let header_page = match self
                .bpm
                .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)
            {
                Some(page) => page,
                None => return false,
            };

            let directory_index = {
                let header = header_page.read();
                header.hash_to_directory_index(hash)
            } as usize;

            debug!("Mapped hash to directory index: {}", directory_index);

            // Get directory page and find bucket index
            let directory_page = match self.get_directory_page(directory_index) {
                Some(page) => page,
                None => {
                    debug!("Failed to get directory page for index {}", directory_index);
                    return false;
                }
            };

            let bucket_index = {
                let directory = directory_page.read();
                directory.hash_to_bucket_index(hash)
            } as usize;

            debug!("Mapped hash to bucket index: {}", bucket_index);

            // Get or create bucket page
            let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
                Some(page) => page,
                None => {
                    debug!("Failed to get bucket page for index {}", bucket_index);
                    return false;
                }
            };

            // Try to insert into bucket
            let mut bucket = bucket_page.write();
            if !bucket.is_full() {
                if bucket.insert(key.clone(), value) {
                    debug!("Successfully inserted key: {:?}", key);
                    return true;
                }
            } else {
                // Bucket is full, need to split
                debug!("Bucket is full, splitting required");
                drop(bucket);
                if self.split_bucket_internal(directory_index, bucket_index) {
                    attempt += 1;
                    if attempt < Self::MAX_INSERT_RETRIES {
                        continue;
                    }
                }
            }
            return false;
        }
    }

    /// Gets the value associated with a key.
    pub fn get_value(&self, key: &Value) -> Option<RID> {
        debug!("Get value called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;
        debug!("Computed hash: {} for key: {:?}", hash, key);

        // Get directory index from header
        let header_page = self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)?;
        let directory_index = {
            let header = header_page.read();
            header.hash_to_directory_index(hash)
        } as usize;

        // Get bucket index from directory
        let directory_page = self.get_directory_page(directory_index)?;
        let bucket_index = {
            let directory = directory_page.read();
            directory.hash_to_bucket_index(hash)
        } as usize;

        // Get bucket and lookup key
        let bucket_page = self.get_bucket_page(directory_index, bucket_index)?;
        let bucket = bucket_page.read();
        bucket.lookup(key)
    }

    /// Removes a key-value pair from the hash table.
    pub fn remove(&mut self, key: &Value) -> bool {
        debug!("Remove called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;

        // Get directory index from header
        let header_page = match self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return false,
        };

        let directory_index = {
            let header = header_page.read();
            header.hash_to_directory_index(hash)
        } as usize;

        // Get bucket index from directory
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        let bucket_index = {
            let directory = directory_page.read();
            directory.hash_to_bucket_index(hash)
        } as usize;

        // Remove from bucket
        let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        let removed = {
            let mut bucket = bucket_page.write();
            bucket.remove(key)
        };

        // If removal was successful and bucket is empty, try to merge
        if removed {
            let is_empty = bucket_page.read().is_empty();
            if is_empty {
                self.merge_bucket(directory_index, bucket_index);
            }
        }

        removed
    }

    // Helper functions
    fn get_directory_page(
        &self,
        directory_index: usize,
    ) -> Option<PageGuard<ExtendableHTableDirectoryPage>> {
        let header_page = self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)?;
        let directory_page_id = header_page.read().get_directory_page_id(directory_index)?;

        if directory_page_id == INVALID_PAGE_ID {
            None
        } else {
            self.bpm
                .fetch_page::<ExtendableHTableDirectoryPage>(directory_page_id)
        }
    }

    fn get_bucket_page(
        &self,
        directory_index: usize,
        bucket_index: usize,
    ) -> Option<PageGuard<ExtendableHTableBucketPage>> {
        let directory_page = self.get_directory_page(directory_index)?;
        let bucket_page_id = directory_page.read().get_bucket_page_id(bucket_index)?;

        if bucket_page_id == INVALID_PAGE_ID {
            None
        } else {
            self.bpm
                .fetch_page::<ExtendableHTableBucketPage>(bucket_page_id)
        }
    }

    // New method: split_bucket_internal performs directory updates and re-distribution
    fn split_bucket_internal(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        debug!(
            "Splitting bucket for directory index {} and bucket index {}",
            directory_index, bucket_index
        );

        // Get directory page
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Check if we've hit the maximum local depth
        let local_depth = {
            let directory = directory_page.read();
            directory.get_local_depth(bucket_index as u32)
        };

        if local_depth >= self.directory_max_depth {
            debug!("Cannot split bucket: maximum local depth reached");
            return false;
        }

        // Create new bucket page
        let new_bucket_page = match self.bpm.new_page::<ExtendableHTableBucketPage>() {
            Some(page) => page,
            None => return false,
        };
        let new_bucket_page_id = new_bucket_page.read().get_page_id();

        // Initialize the new bucket
        {
            let mut new_bucket = new_bucket_page.write();
            new_bucket.init(self.bucket_max_size as u16);
        }

        // Get the old bucket page
        let old_bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        // Update directory and redistribute entries
        {
            let mut directory = directory_page.write();

            // Use the new directory page method to handle the split
            if let Some(_split_bucket_index) =
                directory.update_directory_for_split(bucket_index, new_bucket_page_id)
            {
                // Redistribute entries between old and new buckets
                let mut old_bucket = old_bucket_page.write();
                let mut new_bucket = new_bucket_page.write();

                let entries = old_bucket.get_all_entries();
                old_bucket.clear();

                // Redistribute entries based on the new local depth
                let new_local_depth = directory.get_local_depth(bucket_index as u32);
                let mask = (1 << new_local_depth) - 1;

                for (key, value) in entries {
                    let hash = self.hash_fn.get_hash(&key) as u32;
                    let target_bucket_index = hash & mask;

                    if target_bucket_index == bucket_index as u32 {
                        old_bucket.insert(key, value);
                    } else {
                        new_bucket.insert(key, value);
                    }
                }

                return true;
            }
        }

        false
    }

    // Add a new method to handle bucket merging
    fn merge_bucket(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Try to merge buckets using the new directory page method
        let removed_bucket_id = {
            let mut directory = directory_page.write();
            directory.merge_bucket(bucket_index)
        };

        // If merge was successful, delete the removed bucket page
        if let Some(page_id) = removed_bucket_id {
            let _ = self.bpm.delete_page(page_id);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
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

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let test_context = TestContext::new("test_insert_and_get").await;
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht =
            DiskExtendableHashTable::new("test_table".to_string(), bpm, hash_fn, 4, 4, 4).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let key2 = Value::from(2);
        let key3 = Value::from(3);
        let rid1 = RID::new(1, 0);
        let rid2 = RID::new(2, 0);

        // Test insert
        assert!(ht.insert(key1.clone(), rid1));
        assert!(ht.insert(key2.clone(), rid2));

        // Test get
        assert_eq!(ht.get_value(&key1), Some(rid1));
        assert_eq!(ht.get_value(&key2), Some(rid2));
        assert_eq!(ht.get_value(&key3), None);
    }

    #[tokio::test]
    async fn test_remove() {
        let test_context = TestContext::new("test_remove").await;
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht =
            DiskExtendableHashTable::new("test_table".to_string(), bpm, hash_fn, 4, 4, 4).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let rid1 = RID::new(1, 1);

        // Test insert and remove
        assert!(ht.insert(key1.clone(), rid1));
        assert!(ht.remove(&key1));
        assert_eq!(ht.get_value(&key1), None);
    }

    #[tokio::test]
    async fn test_full_bucket() {
        initialize_logger();
        let test_context = TestContext::new("test_full_bucket").await;
        let bpm = test_context.bpm.clone();
        let hash_fn = HashFunction::new();

        debug!("Creating hash table...");
        let mut ht = DiskExtendableHashTable::new(
            "test_table".to_string(),
            bpm,
            hash_fn,
            4,
            4,
            2, // Small bucket size to test splitting
        )
        .unwrap();

        debug!("Starting insertions...");
        // Insert enough items to cause bucket split
        for i in 0..5 {
            debug!("Inserting item {}", i);
            let key = Value::new(i);
            let rid = RID::new(i, 0);
            assert!(ht.insert(key, rid), "Failed to insert item {}", i);
        }

        debug!("Verifying insertions...");
        // Verify all values can still be retrieved
        for i in 0..5 {
            let key = Value::from(i);
            assert_eq!(
                ht.get_value(&key),
                Some(RID::new(i, 0)),
                "Failed to retrieve item {}",
                i
            );
        }
        debug!("Test completed successfully");
    }
}
