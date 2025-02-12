use crate::common::config::INVALID_PAGE_ID;
use crate::storage::page::page::PageType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::PageId;
use crate::concurrency::transaction::Transaction;
use crate::container::hash_function::HashFunction;
use std::sync::Arc;
use crate::types_db::value::Value;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use log::{debug, info};
use crate::storage::page::page_guard::PageGuard;
use std::default::Default;
use crate::catalog::schema::Schema;
use crate::common::rid::RID;

/// Implementation of an extendable hash table backed by a buffer pool manager.
/// Non-unique keys are supported. Supports insert and delete. The table grows/shrinks
/// dynamically as buckets become full/empty.
pub struct DiskExtendableHashTable {
    index_name: String,
    bpm: Arc<BufferPoolManager>,
    hash_fn: HashFunction<Value>,
    schema: Schema,
    header_max_depth: u32,
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
        schema: Schema,
        header_max_depth: u32,
        directory_max_depth: u32,
        bucket_max_size: u32,
    ) -> Result<Self, String> {
        info!("Creating DiskExtendableHashTable with index name: {}", name);
        // Create header page
        let header_page = bpm.new_page_guarded(PageType::ExtendedHashTableHeader(
            ExtendableHTableHeaderPage::new(INVALID_PAGE_ID)
        )).ok_or("Failed to create header page")?;

        let header_page_id = header_page.get_page_id();
        debug!("Created header page with id: {}", header_page_id);

        // Initialize the header page
        {
            let mut header_guard = header_page.write();
            if let PageType::ExtendedHashTableHeader(ref mut header_page) = *header_guard {
                header_page.init(header_max_depth);
                debug!("Initialized header page with max depth: {}", header_max_depth);
            }
        }

        Ok(Self {
            index_name: name,
            bpm,
            hash_fn,
            schema,
            header_max_depth,
            directory_max_depth,
            bucket_max_size,
            header_page_id,
        })
    }

    const MAX_INSERT_RETRIES: usize = 10;

    /// Public insert API (uses a helper with retry count).
    pub fn insert(&mut self, key: Value, value: RID, transaction: Option<&Transaction>) -> bool {
        debug!("Starting insert operation for key: {:?}", key);
        let mut attempt = 0;
        loop {
            debug!("Insert attempt {} for key: {:?}", attempt, key);
            let hash = self.hash_fn.get_hash(&key) as u32;
            debug!("Computed hash: {} for key: {:?}", hash, key);

            // Get directory index from header page
            let header_page = match self.bpm.fetch_page_guarded(self.header_page_id) {
                Some(page) => page,
                None => return false,
            };

            let directory_index = {
                let header_guard = header_page.read();
                if let PageType::ExtendedHashTableHeader(ref header_page) = *header_guard {
                    Some(header_page.hash_to_directory_index(hash))
                } else {
                    None
                }
            }.unwrap_or(0) as usize;

            // Get directory page and find bucket index
            let directory_page = match self.get_directory_page(directory_index) {
                Some(page) => page,
                None => return false,
            };

            let bucket_index = {
                let dir_guard = directory_page.read();
                if let PageType::ExtendedHashTableDirectory(ref dir_page) = *dir_guard {
                    dir_page.hash_to_bucket_index(hash)
                } else {
                    0
                }
            } as usize;

            // Get or create bucket page
            let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
                Some(page) => page,
                None => match self.create_bucket_page(directory_index, bucket_index) {
                    Some(page) => page,
                    None => return false,
                },
            };

            // Try to insert into bucket
            let mut bucket_guard = bucket_page.write();
            if let PageType::ExtendedHashTableBucket(ref mut bucket) = *bucket_guard {
                if !bucket.is_full() {
                    if bucket.insert(key.clone(), value) {
                        debug!("Successfully inserted key: {:?}", key);
                        return true;
                    }
                } else {
                    // Bucket is full, need to split
                    debug!("Bucket is full, splitting required");
                    drop(bucket_guard);
                    if self.split_bucket_internal(directory_index, bucket_index) {
                        attempt += 1;
                        if attempt < Self::MAX_INSERT_RETRIES {
                            continue;
                        }
                    }
                }
            }
            return false;
        }
    }

    /// Gets the value associated with a key.
    pub fn get_value(&self, key: &Value, transaction: Option<&Transaction>) -> Option<RID> {
        debug!("Get value called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;
        debug!("Computed hash: {} for key: {:?}", hash, key);

        // Get directory index from header
        let header_page = self.bpm.fetch_page_guarded(self.header_page_id)?;
        let directory_index = {
            let header_guard = header_page.read();
            if let PageType::ExtendedHashTableHeader(ref header_page) = *header_guard {
                header_page.hash_to_directory_index(hash)
            } else {
                0
            }
        } as usize;

        // Get bucket index from directory
        let directory_page = self.get_directory_page(directory_index)?;
        let bucket_index = {
            let dir_guard = directory_page.read();
            if let PageType::ExtendedHashTableDirectory(ref dir_page) = *dir_guard {
                dir_page.hash_to_bucket_index(hash)
            } else {
                0
            }
        } as usize;

        // Get bucket and lookup key
        let bucket_page = self.get_bucket_page(directory_index, bucket_index)?;
        let bucket_guard = bucket_page.read();
        if let PageType::ExtendedHashTableBucket(ref bucket) = *bucket_guard {
            let result = bucket.lookup(key);
            debug!("Lookup result for key {:?}: {:?}", key, result);
            result
        } else {
            None
        }
    }

    /// Removes a key-value pair from the hash table.
    pub fn remove(&mut self, key: &Value, transaction: Option<&Transaction>) -> bool {
        debug!("Remove called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;

        // Get directory index from header
        let header_page = match self.bpm.fetch_page_guarded(self.header_page_id) {
            Some(page) => page,
            None => return false,
        };

        let directory_index = {
            let header_guard = header_page.read();
            if let PageType::ExtendedHashTableHeader(ref header_page) = *header_guard {
                Some(header_page.hash_to_directory_index(hash))
            } else {
                None
            }
        }.unwrap_or(0) as usize;

        // Get bucket index from directory
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        let bucket_index = {
            let dir_guard = directory_page.read();
            if let PageType::ExtendedHashTableDirectory(ref dir_page) = *dir_guard {
                dir_page.hash_to_bucket_index(hash)
            } else {
                0
            }
        } as usize;

        // Remove from bucket
        let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        let removed = {
            let mut bucket_guard = bucket_page.write();
            if let PageType::ExtendedHashTableBucket(ref mut bucket) = *bucket_guard {
                bucket.remove(key)
            } else {
                false
            }
        };

        // If removal was successful and bucket is empty, try to merge
        if removed {
            let is_empty = {
                let bucket_guard = bucket_page.read();
                if let PageType::ExtendedHashTableBucket(ref bucket) = *bucket_guard {
                    bucket.is_empty()
                } else {
                    false
                }
            };

            if is_empty {
                self.merge_bucket(directory_index, bucket_index);
            }
        }

        removed
    }

    // Helper functions
    fn get_directory_page(&self, directory_index: usize) -> Option<PageGuard> {
        let header_page = self.bpm.fetch_page_guarded(self.header_page_id)?;
        let (directory_page_id, global_depth) = {
            let header_guard = header_page.read();
            if let PageType::ExtendedHashTableHeader(ref header) = *header_guard {
                (header.get_directory_page_id(directory_index), header.global_depth())
            } else {
                (None, 0)
            }
        };

        match directory_page_id {
            Some(INVALID_PAGE_ID) => {
                // Create new directory page
                let directory_page = self.bpm.new_page_guarded(PageType::ExtendedHashTableDirectory(
                    ExtendableHTableDirectoryPage::new(INVALID_PAGE_ID)
                ))?;

                // Initialize directory page with correct max depth
                {
                    let mut dir_guard = directory_page.write();
                    if let PageType::ExtendedHashTableDirectory(ref mut dir_page) = *dir_guard {
                        dir_page.init(global_depth);
                    }
                }

                // Update header page with new directory page ID
                {
                    let mut header_guard = header_page.write();
                    if let PageType::ExtendedHashTableHeader(ref mut header) = *header_guard {
                        header.set_directory_page_id(directory_index as u32, directory_page.get_page_id());
                    }
                }

                Some(directory_page)
            }
            Some(page_id) => {
                // Return existing directory page without reinitializing
                debug!("Existing directory page found for index {}: {}", directory_index, page_id);
                self.bpm.fetch_page_guarded(page_id)
            }
            None => None
        }
    }

    fn get_bucket_page(&self, directory_index: usize, bucket_index: usize) -> Option<PageGuard> {
        // Get directory page
        let directory_page = self.get_directory_page(directory_index)?;

        // Get bucket page ID from directory using read guard
        let bucket_page_id = {
            let dir_guard = directory_page.read();
            if let PageType::ExtendedHashTableDirectory(ref dir_page) = *dir_guard {
                dir_page.get_bucket_page_id(bucket_index)
            } else {
                None
            }
        };

        // Return bucket page if it exists and is valid
        match bucket_page_id {
            Some(INVALID_PAGE_ID) => None,
            Some(page_id) => {
                debug!(
                    "Existing bucket page found for bucket index {} in directory index {}: {}",
                    bucket_index, directory_index, page_id
                );
                self.bpm.fetch_page_guarded(page_id)
            }
            None => None
        }
    }

    fn create_bucket_page(&self, directory_index: usize, bucket_index: usize) -> Option<PageGuard> {
        // Get directory page
        let directory_page = self.get_directory_page(directory_index)?;

        // Create new bucket page
        let bucket_page = self.bpm.new_page_guarded(PageType::ExtendedHashTableBucket(
            ExtendableHTableBucketPage::new(INVALID_PAGE_ID)
        ))?;
        let bucket_page_id = bucket_page.get_page_id();
        debug!("Created new bucket page with ID {}", bucket_page_id);

        // Initialize bucket page
        {
            let mut bucket_guard = bucket_page.write();
            if let PageType::ExtendedHashTableBucket(ref mut bucket) = *bucket_guard {
                debug!("Initializing new bucket page {} with max size {}", bucket_page_id, self.bucket_max_size);
                bucket.init(self.bucket_max_size as u16);
                bucket.set_local_depth(0);
            }
        }

        // Update directory to point to new bucket page
        {
            let mut dir_guard = directory_page.write();
            if let PageType::ExtendedHashTableDirectory(ref mut dir_page) = *dir_guard {
                dir_page.set_bucket_page_id(bucket_index, bucket_page_id);
                debug!("Updated directory to point bucket index {} to new page {}",
                    bucket_index, bucket_page_id);
            }
        }

        debug!("Successfully created and initialized bucket page {}", bucket_page_id);
        Some(bucket_page)
    }

    // New method: split_bucket_internal performs directory updates and re-distribution
    fn split_bucket_internal(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        debug!("Splitting bucket for directory index {} and bucket index {}", directory_index, bucket_index);
        
        // Get directory page
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Check if we've hit the maximum local depth
        let local_depth = {
            let dir_guard = directory_page.read();
            if let PageType::ExtendedHashTableDirectory(ref dir_page) = *dir_guard {
                dir_page.get_local_depth(bucket_index as u32)
            } else {
                return false;
            }
        };

        if local_depth >= self.directory_max_depth {
            debug!("Cannot split bucket: maximum local depth reached");
            return false;
        }

        // Create new bucket page
        let new_bucket_page = match self.bpm.new_page_guarded(PageType::ExtendedHashTableBucket(
            ExtendableHTableBucketPage::new(INVALID_PAGE_ID)
        )) {
            Some(page) => page,
            None => return false,
        };

        let new_bucket_page_id = new_bucket_page.get_page_id();
        
        // Initialize the new bucket
        {
            let mut new_bucket_guard = new_bucket_page.write();
            if let PageType::ExtendedHashTableBucket(ref mut new_bucket) = *new_bucket_guard {
                new_bucket.init(self.bucket_max_size as u16);
                new_bucket.set_local_depth((local_depth + 1) as u8);
            }
        }

        // Get the old bucket page
        let old_bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        // Update directory and redistribute entries
        {
            let mut dir_guard = directory_page.write();
            if let PageType::ExtendedHashTableDirectory(ref mut dir_page) = *dir_guard {
                dir_page.split_bucket(bucket_index, new_bucket_page_id);
                
                // Redistribute entries
                let mut old_bucket_guard = old_bucket_page.write();
                let mut new_bucket_guard = new_bucket_page.write();
                
                if let (PageType::ExtendedHashTableBucket(ref mut old_bucket),
                        PageType::ExtendedHashTableBucket(ref mut new_bucket)) = 
                    (&mut *old_bucket_guard, &mut *new_bucket_guard) 
                {
                    let entries = old_bucket.get_all_entries();
                    old_bucket.clear();
                    
                    // Set new local depth for old bucket
                    old_bucket.set_local_depth((local_depth + 1) as u8);

                    // Redistribute entries based on the new local depth
                    for (key, value) in entries {
                        let hash = self.hash_fn.get_hash(&key) as u32;
                        let mask = (1 << (local_depth + 1)) - 1;
                        let target_bucket_index = hash & mask;
                        
                        if target_bucket_index == bucket_index as u32 {
                            old_bucket.insert(key, value);
                        } else {
                            new_bucket.insert(key, value);
                        }
                    }
                }
            }
        }

        true
    }

    // Add a new method to handle bucket merging
    fn merge_bucket(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Try to merge buckets
        let removed_bucket_id = {
            let mut dir_guard = directory_page.write();
            if let PageType::ExtendedHashTableDirectory(ref mut dir_page) = *dir_guard {
                dir_page.merge_bucket(bucket_index)
            } else {
                None
            }
        };

        // If merge was successful, delete the removed bucket page
        if let Some(page_id) = removed_bucket_id {
            self.bpm.delete_page(page_id);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    #[test]
    fn test_insert_and_get() {
        let test_context = TestContext::new("test_insert_and_get");
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht = DiskExtendableHashTable::new(
            "test_table".to_string(),
            bpm,
            hash_fn,
            Default::default(),  // Supply a proper schema instance
            4,
            4,
            4
        ).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let key2 = Value::from(2);
        let key3 = Value::from(3);
        // Use RID as the stored value. For example, create a RID with page_id and slot.
        let rid1 = RID::new(1, 0);
        let rid2 = RID::new(2, 0);

        // Test insert
        assert!(ht.insert(key1.clone(), rid1, None));
        assert!(ht.insert(key2.clone(), rid2, None));

        // Test get
        assert_eq!(ht.get_value(&key1, None), Some(rid1));
        assert_eq!(ht.get_value(&key2, None), Some(rid2));
        assert_eq!(ht.get_value(&key3, None), None);
    }

    #[test]
    fn test_remove() {
        let test_context = TestContext::new("test_remove");
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht = DiskExtendableHashTable::new(
            "test_table".to_string(),
            bpm,
            hash_fn,
            Default::default(),  // Supply a proper schema instance
            4,
            4,
            4
        ).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let rid1 = RID::new(1, 1);

        // Test insert and remove
        assert!(ht.insert(key1.clone(), rid1, None));
        assert!(ht.remove(&key1, None));
        assert_eq!(ht.get_value(&key1, None), None);
    }

    #[test]
    fn test_full_bucket() {
        initialize_logger();
        let test_context = TestContext::new("test_full_bucket");
        let bpm = test_context.bpm.clone();
        let hash_fn = HashFunction::new();

        debug!("Creating hash table...");
        let mut ht = DiskExtendableHashTable::new(
            "test_table".to_string(),
            bpm,
            hash_fn,
            Default::default(),
            4,
            4,
            2  // Small bucket size to test splitting
        ).unwrap();

        debug!("Starting insertions...");
        // Insert enough items to cause bucket split
        for i in 0..5 {
            debug!("Inserting item {}", i);
            let key = Value::new(i);
            let rid = RID::new(i, 0);
            assert!(ht.insert(key, rid, None), "Failed to insert item {}", i);
        }

        debug!("Verifying insertions...");
        // Verify all values can still be retrieved
        for i in 0..5 {
            let key = Value::from(i);
            assert_eq!(ht.get_value(&key, None), Some(RID::new(i, 0)),
                      "Failed to retrieve item {}", i);
        }
        debug!("Test completed successfully");
    }
}
