use log::debug;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::storage::page::page::{AsAny, Page, PageTrait};
use crate::types_db::value::Value;
use crate::common::rid::RID;
use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of;
use log;

pub const HTABLE_BUCKET_PAGE_METADATA_SIZE: usize = size_of::<u32>() * 2;
pub const BUCKET_HEADER_SIZE: usize = size_of::<PageId>() + size_of::<u16>() * 2;

#[derive(Debug)]
pub struct ExtendableHTableBucketPage {
    base: Page,
    size: u16,
    max_size: u16,
    local_depth: u8,
    array: Vec<Option<(Value, RID)>>,
}

pub trait BucketPageTrait: PageTrait + AsAny + Send + Sync {
    fn lookup(&self, key: &Value) -> Option<RID>;
    fn insert(&mut self, key: Value, value: RID) -> bool;
    fn remove(&mut self, key: &Value) -> bool;
    fn is_full(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn get_size(&self) -> u16;
    fn get_local_depth(&self) -> u8;
    fn set_local_depth(&mut self, depth: u8);
}

impl ExtendableHTableBucketPage {
    pub fn new(page_id: PageId) -> Self {
        let entry_size = size_of::<Value>() + size_of::<RID>();
        let max_entries = (DB_PAGE_SIZE as usize - BUCKET_HEADER_SIZE) / entry_size;
        debug!("Creating new bucket page with ID {} and max_entries {}", page_id, max_entries);
        
        let mut instance = Self {
            base: Page::new(page_id),
            size: 0,
            max_size: max_entries as u16,
            local_depth: 0,
            array: Vec::with_capacity(max_entries),
        };
        
        instance.array.resize(max_entries, None);
        debug!("Initialized bucket page array with {} slots", max_entries);
        
        instance
    }

    pub fn init(&mut self, size: u16) {
        debug!("Initializing bucket page with max size {}", size);
        self.size = 0;
        self.max_size = size;
        self.local_depth = 0;
        self.array.clear();
        self.array.resize(size as usize, None);
        debug!("Bucket page initialized with {} slots and max_size {}", size, self.max_size);
    }

    pub fn lookup(&self, key: &Value) -> Option<RID> {
        for entry in self.array.iter().take(self.size as usize) {
            if let Some((k, v)) = entry {
                if k == key {
                    return Some(*v);
                }
            }
        }
        None
    }

    pub fn insert(&mut self, key: Value, value: RID) -> bool {
        debug!("Attempting to insert into bucket page. Current size: {}, max_size: {}", self.size, self.max_size);
        
        if self.is_full() {
            debug!("Bucket is full, cannot insert");
            return false;
        }
        
        // Check for duplicate key
        if self.lookup(&key).is_some() {
            debug!("Key already exists in bucket");
            return false;
        }
        
        // Insert the new entry
        if (self.size as usize) < self.array.len() {
            debug!("Inserting at index {}", self.size);
            self.array[self.size as usize] = Some((key, value));
            self.size += 1;
            debug!("Successfully inserted. New size: {}", self.size);
            return true;
        }
        
        debug!("Insert failed - array bounds check failed");
        false
    }

    pub fn remove(&mut self, key: &Value) -> bool {
        if let Some(index) = (0..self.size as usize).find(|&i| {
            self.array[i].as_ref().map_or(false, |(k, _)| k == key)
        }) {
            self.array[index] = None;
            self.array[index..self.size as usize].rotate_left(1);
            self.size -= 1;
            true
        } else {
            false
        }
    }

    pub fn is_full(&self) -> bool {
        let is_full = self.size as usize >= self.array.len();
        debug!("Checking if bucket is full: {} (size: {}, capacity: {})", 
            is_full, self.size, self.array.len());
        is_full
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn get_size(&self) -> u16 {
        self.size
    }

    pub fn get_local_depth(&self) -> u8 {
        self.local_depth
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.local_depth = depth;
    }

    /// Removes and returns the first entry from the bucket.
    /// Returns `None` if the bucket is empty.
    pub fn remove_first(&mut self) -> Option<(Value, RID)> {
        if self.size == 0 {
            return None;
        }

        // Remove the first entry and log it.
        let removed_entry = self.array[0].take();

        // Shift remaining valid entries to the left.
        self.array[..self.size as usize].rotate_left(1);
        // Clear the last valid slot.
        self.array[self.size as usize - 1] = None;
        // Update bucket size.
        self.size -= 1;

        removed_entry
    }

    /// Gets all entries from the bucket.
    pub fn get_all_entries(&self) -> Vec<(Value, RID)> {
        self.array
            .iter()
            .take(self.size as usize)
            .filter_map(|entry| entry.clone())
            .collect()
    }

    /// Clears all entries from the bucket.
    pub fn clear(&mut self) {
        for entry in self.array.iter_mut() {
            *entry = None;
        }
        self.size = 0;
    }
}

impl BucketPageTrait for ExtendableHTableBucketPage {
    fn lookup(&self, key: &Value) -> Option<RID> {
        self.lookup(key)
    }

    fn insert(&mut self, key: Value, value: RID) -> bool {
        self.insert(key, value)
    }

    fn remove(&mut self, key: &Value) -> bool {
        self.remove(key)
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn get_size(&self) -> u16 {
        self.get_size()
    }

    fn get_local_depth(&self) -> u8 {
        self.get_local_depth()
    }

    fn set_local_depth(&mut self, depth: u8) {
        self.set_local_depth(depth)
    }
}

impl PageTrait for ExtendableHTableBucketPage {
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

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        self.base.get_data()
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        self.base.get_data_mut()
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        self.base.set_data(offset, new_data)
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.base.set_pin_count(pin_count)
    }

    fn reset_memory(&mut self) {
        self.base.reset_memory()
    }
}

impl AsAny for ExtendableHTableBucketPage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod basic_behavior {
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::config::INVALID_PAGE_ID;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
    use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
    use chrono::Utc;
    use log::{error, info};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::thread;
    use crate::storage::page::page::PageType;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, 2)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));
            Self {
                bpm,
                db_file,
                db_log_file,
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
    fn bucket_page_integrity() {
        let ctx = TestContext::new("bucket_page_integrity");
        let bpm = &ctx.bpm;

        let bucket_page_guard = match bpm.new_page_guarded(PageType::ExtendedHashTableBucket(
            ExtendableHTableBucketPage::new(0)
        )) {
            Some(guard) => guard,
            None => {
                error!("Failed to create new bucket page");
                panic!("Failed to create new bucket page");
            }
        };

        if let Some(page_type) = bucket_page_guard.into_specific_type() {
            match page_type {
                PageType::ExtendedHashTableBucket(mut bucket_page) => {
                    let bucket_page_size = bucket_page.get_size();
                    info!("bucket page size: {}", bucket_page_size);

                    bucket_page.init(10);
                    info!("Initialized bucket page with max size: {}", bucket_page.max_size);
                    assert_eq!(bucket_page.max_size, 10, "Max size should be 10");
                    assert_eq!(bucket_page.get_size(), 0, "Initial size should be 0");
                }
                _ => panic!("Wrong page type returned"),
            }
        }
    }

    #[test]
    fn end_to_end() {
        let ctx = TestContext::new("test_basic_behaviour");
        let bpm = &ctx.bpm;

        info!("Starting basic behaviour tests");

        let mut bucket_page_ids = [INVALID_PAGE_ID; 4];

        // Create bucket pages
        for i in 0..4 {
            let bucket_page_guard = match bpm.new_page_guarded(PageType::ExtendedHashTableBucket(ExtendableHTableBucketPage::new(0)))
            {
                Some(guard) => guard,
                None => {
                    error!("Failed to create new bucket page");
                    panic!("Failed to create new bucket page");
                }
            };
            bucket_page_ids[i] = bucket_page_guard.get_page_id();
            if let Some(page_type) = bucket_page_guard.into_specific_type() {
                match page_type {
                    PageType::ExtendedHashTableBucket(mut bucket_page) => {
                        bucket_page.init(10);
                        info!("Initialized bucket page with max size: {}", bucket_page.get_size());
                    }
                    _ => panic!("Wrong page type returned"),
                }
            }
        }

        // Create directory page
        let directory_guard = bpm
            .new_page_guarded(PageType::ExtendedHashTableDirectory(
                ExtendableHTableDirectoryPage::new(0)
            ))
            .unwrap();
        let directory_page_id = directory_guard.get_page_id();
        info!("Created directory page with ID: {}", directory_page_id);

        if let Some(page_type) = directory_guard.into_specific_type() {
            match page_type {
                PageType::ExtendedHashTableDirectory(mut directory_page) => {
                    directory_page.init(3);
                    info!(
                        "Initialized bucket page with max size: {}",
                        directory_page.get_size()
                    );

                    directory_page.set_bucket_page_id(0, bucket_page_ids[0]);

                    /*
                    ======== DIRECTORY (global_depth_: 0) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    0    |    0        |
                    ================ END DIRECTORY ================
                    */
                    directory_page.print_directory();
                    directory_page.verify_integrity();
                    assert_eq!(directory_page.get_size(), 1);
                    assert_eq!(
                        directory_page.get_bucket_page_id(0),
                        Some(bucket_page_ids[0])
                    );

                    // grow the directory, local depths should change!
                    directory_page.set_local_depth(0, 1);
                    directory_page.incr_global_depth();
                    directory_page.set_bucket_page_id(1, bucket_page_ids[1]);
                    directory_page.set_local_depth(1, 1);

                    /*
                    ======== DIRECTORY (global_depth_: 1) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    0    |    1        |
                    |    1       |    1    |    1        |
                    ================ END DIRECTORY ================
                    */

                    directory_page.print_directory();
                    directory_page.verify_integrity();
                    assert_eq!(directory_page.get_size(), 2);
                    assert_eq!(
                        directory_page.get_bucket_page_id(0),
                        Some(bucket_page_ids[0])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(1),
                        Some(bucket_page_ids[1])
                    );

                    for i in 0..100 {
                        assert_eq!(directory_page.hash_to_bucket_index(i), i % 2);
                    }

                    directory_page.set_local_depth(0, 2);
                    directory_page.incr_global_depth();
                    directory_page.set_bucket_page_id(2, bucket_page_ids[2]);

                    /*
                    ======== DIRECTORY (global_depth_: 2) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    0    |    2        |
                    |    1       |    1    |    1        |
                    |    2       |    2    |    1        |
                    |    3       |    1    |    1        |
                    ================ END DIRECTORY ================
                    */

                    directory_page.print_directory();
                    directory_page.verify_integrity();
                    assert_eq!(directory_page.get_size(), 4);
                    assert_eq!(
                        directory_page.get_bucket_page_id(0),
                        Some(bucket_page_ids[0])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(1),
                        Some(bucket_page_ids[1])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(2),
                        Some(bucket_page_ids[2])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(3),
                        Some(bucket_page_ids[1])
                    );

                    for i in 0..100 {
                        assert_eq!(directory_page.hash_to_bucket_index(i), i % 4);
                    }

                    directory_page.set_local_depth(0, 3);
                    directory_page.incr_global_depth();
                    directory_page.set_bucket_page_id(4, bucket_page_ids[3]);

                    /*
                    ======== DIRECTORY (global_depth_: 3) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    1    |    3        |
                    |    1       |    2    |    1        |
                    |    2       |    3    |    2        |
                    |    3       |    2    |    1        |
                    |    4       |    4    |    3        |
                    |    5       |    2    |    1        |
                    |    6       |    3    |    2        |
                    |    7       |    2    |    1        |
                    ================ END DIRECTORY ================
                    */

                    directory_page.print_directory();
                    directory_page.verify_integrity();
                    assert_eq!(directory_page.get_size(), 8);
                    assert_eq!(
                        directory_page.get_bucket_page_id(0),
                        Some(bucket_page_ids[0])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(1),
                        Some(bucket_page_ids[1])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(2),
                        Some(bucket_page_ids[2])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(3),
                        Some(bucket_page_ids[1])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(4),
                        Some(bucket_page_ids[3])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(5),
                        Some(bucket_page_ids[1])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(6),
                        Some(bucket_page_ids[2])
                    );
                    assert_eq!(
                        directory_page.get_bucket_page_id(7),
                        Some(bucket_page_ids[1])
                    );

                    for i in 0..100 {
                        assert_eq!(directory_page.hash_to_bucket_index(i), i % 8);
                    }

                    // uncommenting this code line below should cause an "Assertion failed"
                    // since this would be exceeding the max depth we initialized
                    // directory_page.incr_global_depth();

                    // at this time, we cannot shrink the directory since we have ld = gd = 3
                    directory_page.print_directory();
                    assert_eq!(directory_page.can_shrink(), false);

                    directory_page.set_local_depth(0, 2);
                    directory_page.set_local_depth(4, 2);
                    directory_page.set_bucket_page_id(0, bucket_page_ids[3]);

                    /*
                    ======== DIRECTORY (global_depth_: 3) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    4    |    2        |
                    |    1       |    2    |    1        |
                    |    2       |    3    |    2        |
                    |    3       |    2    |    1        |
                    |    4       |    4    |    2        |
                    |    5       |    2    |    1        |
                    |    6       |    3    |    2        |
                    |    7       |    2    |    1        |
                    ================ END DIRECTORY ================
                    */

                    directory_page.print_directory();
                    assert_eq!(directory_page.can_shrink(), true);
                    directory_page.decr_global_depth();

                    /*
                    ======== DIRECTORY (global_depth_: 2) ========
                    | bucket_idx | page_id | local_depth |
                    |    0       |    4    |    2        |
                    |    1       |    2    |    1        |
                    |    2       |    3    |    2        |
                    |    3       |    2    |    1        |
                    ================ END DIRECTORY ================
                    */

                    directory_page.print_directory();
                    directory_page.verify_integrity();
                    assert_eq!(directory_page.get_size(), 4);
                    assert_eq!(directory_page.can_shrink(), false);

                    info!("Basic behaviour tests completed successfully");
                }
                _ => panic!("Wrong page type returned"),
            }
        } else {
            panic!("Failed to convert to ExtendableHTableDirectoryPage");
        }
    }

    #[test]
    fn concurrent_access() {
        let ctx = Arc::new(TestContext::new("test_concurrent_access"));
        let bpm = &ctx.bpm;

        info!("Starting concurrent access tests");

        let num_threads = 4;
        let operations_per_thread = 100;

        let mut handles = vec![];

        for i in 0..num_threads {
            let bpm_clone = Arc::clone(&bpm);
            let handle = thread::spawn(move || {
                for j in 0..operations_per_thread {
                    let bucket_page_guard = match bpm_clone.new_page_guarded(PageType::ExtendedHashTableBucket(
                        ExtendableHTableBucketPage::new(0)  // Using 0 as temporary page_id
                    )) {
                        Some(guard) => guard,
                        None => {
                            error!("Failed to create new bucket page");
                            panic!("Failed to create new bucket page");
                        }
                    };
                    let bucket_page_id = bucket_page_guard.get_page_id();
                    info!(
                        "Thread {} created bucket page with ID: {} (operation {})",
                        i, bucket_page_id, j
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Failed to join thread");
        }

        info!("Concurrent access tests completed successfully");
    }
}
