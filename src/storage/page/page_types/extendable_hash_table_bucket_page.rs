use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::storage::index::generic_key::{GenericKey, GenericKeyComparator};
use crate::storage::page::page::{AsAny, Page, PageTrait};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::mem::size_of;
use crate::storage::index::generic_key;

pub const HTABLE_BUCKET_PAGE_METADATA_SIZE: usize = size_of::<u32>() * 2;
pub const BUCKET_HEADER_SIZE: usize = size_of::<PageId>() + size_of::<u16>() * 2;

#[derive(Clone)]
pub struct ExtendableHTableBucketPage<T: Clone, const KEY_SIZE: usize> {
    base: Page,
    size: u16,
    max_size: u16,
    local_depth: u8,
    array: Vec<Option<(GenericKey<T, KEY_SIZE>, PageId)>>,
}

pub struct TypeErasedBucketPage {
    inner: Arc<RwLock<dyn BucketPageTrait>>,
}

pub trait BucketPageTrait: PageTrait + AsAny + Send + Sync {
    fn get_key_size(&self) -> usize;
    fn lookup(&self, key: &[u8], comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> Option<PageId>;
    fn insert(&mut self, key: &[u8], value: PageId, comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> bool;
    fn remove(&mut self, key: &[u8], comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> bool;
    fn is_full(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn get_size(&self) -> u16;
    fn get_local_depth(&self) -> u8;
    fn set_local_depth(&mut self, depth: u8);
}

impl<T: Clone + 'static, const KEY_SIZE: usize> ExtendableHTableBucketPage<T, KEY_SIZE> {
    pub fn new(page_id: PageId) -> Self {
        let entry_size = KEY_SIZE + size_of::<PageId>();
        let max_entries = (DB_PAGE_SIZE - BUCKET_HEADER_SIZE) / entry_size;
        let mut instance = Self {
            base: Page::new(page_id),
            size: 0,
            max_size: max_entries as u16,
            local_depth: 0,
            array: Vec::with_capacity(max_entries),
        };

        instance
    }

    pub fn init(&mut self, size: u16) {
        self.size = size;
        self.local_depth = 0;
        self.array.clear();
        self.array.resize(self.max_size as usize, None);
    }

    pub fn lookup(&self, key: &GenericKey<T, KEY_SIZE>, comparator: &GenericKeyComparator<T, KEY_SIZE>) -> Option<PageId> {
        for entry in self.array.iter().take(self.size as usize) {
            if let Some((k, v)) = entry {
                if comparator.compare(k, key) == Ordering::Equal {
                    return Some(*v);
                }
            }
        }
        None
    }

    pub fn insert(&mut self, key: GenericKey<T, KEY_SIZE>, value: PageId, comparator: &GenericKeyComparator<T, KEY_SIZE>) -> bool {
        if self.is_full() {
            return false;
        }
        if self.lookup(&key, comparator).is_some() {
            return false;
        }
        self.array[self.size as usize] = Some((key, value));
        self.size += 1;
        true
    }

    pub fn remove(&mut self, key: &GenericKey<T, KEY_SIZE>, comparator: &GenericKeyComparator<T, KEY_SIZE>) -> bool {
        if let Some(index) = (0..self.size as usize)
            .find(|&i| self.array[i].as_ref().map_or(false, |(k, _)| comparator.compare(k, key) == Ordering::Equal))
        {
            self.array[index] = None;
            self.array[index..self.size as usize].rotate_left(1);
            self.size -= 1;
            true
        } else {
            false
        }
    }

    pub fn is_full(&self) -> bool {
        self.size as usize == self.max_size as usize
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
}

impl TypeErasedBucketPage {
    pub fn new<T: BucketPageTrait + 'static>(page: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(page)),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<dyn BucketPageTrait> {
        self.inner.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<dyn BucketPageTrait> {
        self.inner.write()
    }

    pub fn with_downcast<T, const KEY_SIZE: usize, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&ExtendableHTableBucketPage<T, KEY_SIZE>) -> R, T: Clone + 'static
    {
        let read_guard = self.inner.read();
        read_guard.as_any().downcast_ref::<ExtendableHTableBucketPage<T, KEY_SIZE>>().map(f)
    }

    pub fn with_downcast_mut<T, const KEY_SIZE: usize, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut ExtendableHTableBucketPage<T, KEY_SIZE>) -> R, T: std::clone::Clone + 'static
    {
        let mut write_guard = self.inner.write();
        write_guard.as_any_mut().downcast_mut::<ExtendableHTableBucketPage<T, KEY_SIZE>>().map(f)
    }

    pub fn get_key_size(&self) -> usize {
        self.inner.read().get_key_size()
    }

    pub fn with_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; DB_PAGE_SIZE]) -> R,
    {
        let guard = self.inner.read();
        f(guard.get_data())
    }

    pub fn with_data_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; DB_PAGE_SIZE]) -> R,
    {
        let mut guard = self.inner.write();
        f(guard.get_data_mut())
    }
}

// Implement Send and Sync for TypeErasedBucketPage
unsafe impl Send for TypeErasedBucketPage {}
unsafe impl Sync for TypeErasedBucketPage {}

// Update ExtendableHTableBucketPage to implement Send and Sync
unsafe impl<T: Clone + 'static, const KEY_SIZE: usize> Send for ExtendableHTableBucketPage<T, KEY_SIZE> {}
unsafe impl<T: Clone + 'static, const KEY_SIZE: usize> Sync for ExtendableHTableBucketPage<T, KEY_SIZE> {}

impl<T: Clone + 'static, const KEY_SIZE: usize> BucketPageTrait for ExtendableHTableBucketPage<T, KEY_SIZE> {
    fn get_key_size(&self) -> usize {
        KEY_SIZE
    }

    fn lookup(&self, key: &[u8], comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> Option<PageId> {
        let mut generic_key = GenericKey::<T, KEY_SIZE>::new();
        generic_key.set_from_bytes(key);
        let generic_comparator = GenericKeyComparator::<T, KEY_SIZE>::new();

        self.lookup(&generic_key, &generic_comparator)
    }

    fn insert(&mut self, key: &[u8], value: PageId, comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> bool {
        let mut generic_key = GenericKey::<T, KEY_SIZE>::new();
        generic_key.set_from_bytes(key);
        let generic_comparator = GenericKeyComparator::<T, KEY_SIZE>::new();

        self.insert(generic_key, value, &generic_comparator)
    }

    fn remove(&mut self, key: &[u8], comparator: &dyn Fn(&[u8], &[u8]) -> Ordering) -> bool {
        let mut generic_key = GenericKey::<T, KEY_SIZE>::new();
        generic_key.set_from_bytes(key);
        let generic_comparator = GenericKeyComparator::<T, KEY_SIZE>::new();

        self.remove(&generic_key, &generic_comparator)
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

impl<T: Clone + 'static, const KEY_SIZE: usize> PageTrait for ExtendableHTableBucketPage<T, KEY_SIZE> {
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
        self.base.get_data()
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
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

impl PageTrait for TypeErasedBucketPage {
    fn get_page_id(&self) -> PageId {
        self.inner.read().get_page_id()
    }

    fn is_dirty(&self) -> bool {
        self.inner.read().is_dirty()
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.inner.write().set_dirty(is_dirty);
    }

    fn get_pin_count(&self) -> i32 {
        self.inner.read().get_pin_count()
    }

    fn increment_pin_count(&mut self) {
        self.inner.write().increment_pin_count();
    }

    fn decrement_pin_count(&mut self) {
        self.inner.write().decrement_pin_count();
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        // This method can't be implemented safely for TypeErasedBucketPage
        // because we can't return a reference that outlives the RwLockReadGuard.
        // Instead, we'll panic with an explanation.
        panic!("get_data() is not supported for TypeErasedBucketPage. Use with_data() instead.")
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        // This method can't be implemented safely for TypeErasedBucketPage
        // because we can't return a reference that outlives the RwLockWriteGuard.
        // Instead, we'll panic with an explanation.
        panic!("get_data_mut() is not supported for TypeErasedBucketPage. Use with_data_mut() instead.")
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        self.inner.write().set_data(offset, new_data)
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.inner.write().set_pin_count(pin_count);
    }

    fn reset_memory(&mut self) {
        self.inner.write().reset_memory();
    }
}

impl<T: Clone + 'static, const KEY_SIZE: usize> AsAny for ExtendableHTableBucketPage<T, KEY_SIZE> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl AsAny for TypeErasedBucketPage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Debug for TypeErasedBucketPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Display for TypeErasedBucketPage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TypeErasedBucketPage")
    }
}

#[cfg(test)]
mod basic_behavior {
    use std::sync::Arc;
    use std::thread;
    use chrono::Utc;
    use log::{error, info};
    use spin::RwLock;
    use crate::buffer::buffer_pool_manager::{BufferPoolManager, NewPageType};
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::config::PageId;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::common::logger::initialize_logger;
    use crate::storage::page::page::PageTrait;
    use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
    use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
    use crate::types_db::integer_type::IntegerType;

    const PAGE_ID_SIZE: usize = size_of::<PageId>();

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
        buffer_pool_size: usize,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            const K: usize = 2;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(
                &disk_manager,
            ))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new((BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            )));
            Self {
                bpm,
                db_file,
                db_log_file,
                buffer_pool_size,
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
    #[ignore]
    fn bucket_page_integrity() {
        let ctx = TestContext::new("bucket_page_integrity");
        let bpm = &ctx.bpm;

        let bucket_guard = match bpm.new_page_guarded(NewPageType::ExtendedHashTableBucket) {
            Some(guard) => guard,
            None => {
                error!("Failed to create new bucket page");
                panic!("Failed to create new bucket page");
            }
        };
        let bucket_page_id = bucket_guard.get_page_id();
        match bucket_guard.into_specific_type::<ExtendableHTableBucketPage<IntegerType, 8>, 8>() {
            Some(mut bucket_guard_1) => {
                bucket_guard_1.access_mut(|page| {
                    page.init(10);
                    info!("Initialized bucket page with max size: {}", page.get_size());
                });
            }
            None => {
                error!("Failed to convert to ExtendableHTableBucketPage<8>");
                // Print more information about the page
                error!("Page ID: {}", bucket_page_id);
                // error!("Page type: {}", bucket_guard_1.get_page_type());
                panic!("Conversion to ExtendableHTableBucketPage<8> failed");
            }
        }
    }

    // #[test]
    // #[ignore]
    // fn concurrent_access() {
    //     let ctx = Arc::new(TestContext::new("test_concurrent_access"));
    //     let bpm = Arc::clone(&ctx.bpm);
    //
    //     info!("Starting concurrent access test");
    //
    //     let num_threads = 4;
    //     let operations_per_thread = 100;
    //
    //     let mut handles = vec![];
    //
    //     for i in 0..num_threads {
    //         let bpm_clone = Arc::clone(&bpm);
    //         let handle = thread::spawn(move || {
    //             for j in 0..operations_per_thread {
    //                 let bucket_guard = bpm_clone.new_page_guarded(NewPageType::ExtendedHashTableBucket).unwrap();
    //                 let bucket_page_id = bucket_guard.get_page_id();
    //                 info!("Thread {} created bucket page with ID: {} (operation {})", i, bucket_page_id, j);
    //
    //                 if let Some(mut bucket_guard) = bucket_guard.into_specific_type::<ExtendableHTableBucketPage<IntegerType, 8>, 8>() {
    //                     bucket_guard.access_mut(|bucket_page| {
    //                         bucket_page.init(10);
    //                         assert_eq!(bucket_page.get_size(), 0, "Initial bucket size should be 0");
    //                         info!("Thread {} initialized bucket page {} (operation {})", i, bucket_page_id, j);
    //                     });
    //                 } else {
    //                     panic!("Thread {} failed to convert to ExtendableHTableBucketPage<8> (operation {})", i, j);
    //                 }
    //             }
    //         });
    //         handles.push(handle);
    //     }
    //
    //     for handle in handles {
    //         handle.join().unwrap();
    //     }
    //
    //     info!("Concurrent access test completed successfully");
    // }
}
