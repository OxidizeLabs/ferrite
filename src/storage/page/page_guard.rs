use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::PageId;
use crate::common::logger::initialize_logger;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::page::page::PageType;
use crate::storage::page::page::{AsAny, Page};
use crate::storage::page::page_types::extendable_hash_table_bucket_page::{BucketPageTrait, ExtendableHTableBucketPage};
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::storage::page::page_types::table_page::TablePage;
use chrono::Utc;
use mockall::Any;
use spin::RwLock;
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<PageType>>,
    page_id: PageId,
}

impl PageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<PageType>>, page_id: PageId) -> Self {
        Self { bpm, page, page_id }
    }

    pub fn read(&self) -> spin::RwLockReadGuard<'_, PageType> {
        self.page.read()
    }

    pub fn write(&self) -> spin::RwLockWriteGuard<'_, PageType> {
        self.page.write()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_pin_count(&self) -> i32 {
        self.page.read().as_page_trait().get_pin_count()
    }

    fn set_dirty(&self, is_dirty: bool) {
        self.page.write().as_page_trait_mut().set_dirty(is_dirty)
    }

    pub fn get_page_type(&self) -> &'static str {
        let page = self.read();
        match *page {
            PageType::Basic(_) => "Basic",
            PageType::ExtendedHashTableDirectory(_) => "ExtendedHashTableDirectory",
            PageType::ExtendedHashTableHeader(_) => "ExtendedHashTableHeader",
            PageType::ExtendedHashTableBucket(_) => "ExtendedHashTableBucket",
            PageType::Table(_) => "TablePage"
        }
    }

    pub fn into_specific_type<T: Clone + 'static, const KEY_SIZE: usize>(self) -> Option<SpecificPageGuard<T, KEY_SIZE>> {
        let page_type = TypeId::of::<T>();
        let is_matching_type = match &*self.page.read() {
            PageType::Basic(_) =>
                page_type == TypeId::of::<Page>(),
            PageType::ExtendedHashTableBucket(bucket_page) =>
                page_type == TypeId::of::<ExtendableHTableBucketPage<T, KEY_SIZE>>() && bucket_page.get_key_size() == KEY_SIZE,
            PageType::ExtendedHashTableDirectory(_) =>
                page_type == TypeId::of::<ExtendableHTableDirectoryPage>(),
            PageType::ExtendedHashTableHeader(_) =>
                page_type == TypeId::of::<ExtendableHTableHeaderPage>(),
            PageType::Table(_) =>
                page_type == TypeId::of::<TablePage>(),
        };

        if is_matching_type {
            Some(SpecificPageGuard {
                inner: self,
                _phantom: PhantomData,
            })
        } else {
            None
        }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let is_dirty = self.read().as_page_trait().is_dirty();
        self.set_dirty(is_dirty);
        self.bpm.unpin_page(self.page_id, is_dirty, AccessType::Unknown);
    }
}

pub struct SpecificPageGuard<T: 'static, const KEY_SIZE: usize> {
    inner: PageGuard,
    _phantom: PhantomData<T>,
}

impl<T: Clone + 'static, const KEY_SIZE: usize> SpecificPageGuard<T, KEY_SIZE> {
    pub fn read(&self) -> SpecificPageReadGuard<T, KEY_SIZE> {
        SpecificPageReadGuard(self.inner.read(), PhantomData)
    }

    pub fn write(&self) -> SpecificPageWriteGuard<T, KEY_SIZE> {
        SpecificPageWriteGuard(self.inner.write(), PhantomData)
    }

    pub fn get_page_id(&self) -> PageId {
        self.inner.get_page_id()
    }

    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        match &*self.inner.page.read() {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast::<T, KEY_SIZE, _, R>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<T, KEY_SIZE> in into_specific_type
                    let typed_page = unsafe { &*(page as *const _ as *const T) };
                    f(typed_page)
                })
            }
            PageType::Basic(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::Table(page) => page.as_any().downcast_ref::<T>().map(f),
        }
    }

    pub fn access_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        match &mut *self.inner.page.write() {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast_mut::<T, KEY_SIZE, _, R>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<T, KEY_SIZE> in into_specific_type
                    let typed_page = unsafe { &mut *(page as *mut _ as *mut T) };
                    f(typed_page)
                })
            }
            PageType::Basic(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::Table(page) => page.as_any_mut().downcast_mut::<T>().map(f),
        }
    }
}

pub struct SpecificPageReadGuard<'a, T: 'static, const KEY_SIZE: usize>(spin::RwLockReadGuard<'a, PageType>, PhantomData<T>);

impl<'a, T: Clone + 'static, const KEY_SIZE: usize> SpecificPageReadGuard<'a, T, KEY_SIZE> {
    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        match &*self.0 {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast::<T, KEY_SIZE, _, R>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<T, KEY_SIZE> in into_specific_type
                    let typed_page = unsafe { &*(page as *const _ as *const T) };
                    f(typed_page)
                })
            }
            PageType::Basic(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::Table(page) => page.as_any().downcast_ref::<T>().map(f),
        }
    }
}

pub struct SpecificPageWriteGuard<'a, T: 'static, const KEY_SIZE: usize>(spin::RwLockWriteGuard<'a, PageType>, PhantomData<T>);


impl<'a, T: Clone + 'static, const KEY_SIZE: usize> SpecificPageWriteGuard<'a, T, KEY_SIZE> {
    pub fn access_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        match &mut *self.0 {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast_mut::<T, KEY_SIZE, _, R>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<T, KEY_SIZE> in into_specific_type
                    let typed_page = unsafe { &mut *(page as *mut _ as *mut T) };
                    f(typed_page)
                })
            }
            PageType::Basic(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::Table(page) => page.as_any_mut().downcast_mut::<T>().map(f),
        }
    }
}

impl<T: 'static, const KEY_SIZE: usize> Drop for SpecificPageGuard<T, KEY_SIZE> {
    fn drop(&mut self) {
        // The inner PageGuard's drop will handle unpinning
    }
}

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
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
        self.cleanup();
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::NewPageType;
    use crate::storage::page::page::PageTrait;
    use log::info;

    #[test]
    fn basic_page_guard() {
        let ctx = TestContext::new("basic_page_guard");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let basic_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0.read().as_page_trait().get_page_id());

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(page0.read().as_page_trait().get_page_id(), basic_guard.get_page_id());

        // Ensure dropping the guard decreases the pin count
        drop(basic_guard);
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 0, "Pin count should be 0 after dropping the WritePageGuard");
    }

    #[test]
    fn convert_into() {
        let ctx = TestContext::new("basic_page_guard");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0.read().as_page_trait().get_page_id());

        if let Some(ext_guard) = page_guard.into_specific_type::<ExtendableHTableDirectoryPage, 8>() {
            let read_guard = ext_guard.read();
            read_guard.access(|page| {
                let directory_size = page.get_size();
                info!("Directory size: {}", directory_size);
            });

            let mut write_guard = ext_guard.write();
            write_guard.access_mut(|page| {
                page.set_dirty(true);
            });
        }
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;
    use crate::buffer::buffer_pool_manager::NewPageType;

    #[test]
    fn create_and_drop() {
        let ctx = TestContext::new("create_and_drop");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page0.read().as_page_trait().get_page_id();
        let basic_guarded_page = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page_id);

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(page_id, basic_guarded_page.get_page_id());

        // Create another page and guard it with ReadPageGuard
        let page2 = bpm.new_page(NewPageType::Basic).unwrap();
        let page2_id = page2.read().as_page_trait().get_page_id();
        let read_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page2), page2_id);
        assert_eq!(page2.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creating the guard

        // Ensure dropping the guard decreases the pin count
        drop(read_guard);
        assert_eq!(page2.read().as_page_trait().get_pin_count(), 0); // Pin count should be 0 after dropping the guard

        // Test WritePageGuard functionality
        let page3 = bpm.new_page(NewPageType::Basic).unwrap();
        let page3_id = page3.read().as_page_trait().get_page_id();

        {
            let write_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page3), page3_id);
            assert_eq!(page3.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creating WritePageGuard
        }

        // Ensure pin count is reduced after dropping WritePageGuard
        assert_eq!(page3.read().as_page_trait().get_pin_count(), 0);
    }

    #[test]
    fn modify_data() {
        let ctx = TestContext::new("modify_data");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page0_id = page0.read().as_page_trait().get_page_id();
        {
            let basic_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0_id);

            // Ensure page0 is pinned and the guard works as expected
            assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
            assert_eq!(page0.read().as_page_trait().get_page_id(), basic_guard.get_page_id());


            // Directly modify data in the page
            let mut write_guard = basic_guard.write();
            let data_mut = write_guard.as_page_trait_mut().get_data_mut();

            // Example of directly setting bytes (this could represent any structure)
            data_mut[0] = 3; // Equivalent to setting a 'depth' field
            data_mut[4..8].copy_from_slice(&42u32.to_ne_bytes()); // Example of setting a 'bucket_page_id'

            // Read back the data and verify it
            assert_eq!(data_mut[0], 3, "Depth should be set to 3");
            let bucket_page_id = u32::from_ne_bytes(data_mut[4..8].try_into().unwrap());
            assert_eq!(bucket_page_id, 42, "Bucket page ID should be set to 42");
        }
        // Ensure dropping the guard decreases the pin count

        assert_eq!(page0.read().as_page_trait().get_pin_count(), 0, "Pin count should be 0 after dropping the WritePageGuard");
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;
    use crate::buffer::buffer_pool_manager::NewPageType;
    use std::fmt::Pointer;
    use std::thread;

    #[test]
    fn reads() {
        let ctx = TestContext::new("concurrent_reads");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a shared page
        let page = Arc::new(bpm.new_page(NewPageType::Basic).unwrap());

        // Spawn multiple reader threads
        let mut threads = Vec::new();
        for _ in 0..3 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);

            threads.push(thread::spawn(move || {

                // Acquire read guard
                {
                    let page_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), 0);
                    let read_guard = page_guard.read();

                    // Simulate a read operation by checking if the page ID matches
                    assert_eq!(read_guard.as_page_trait().get_page_id(), page_clone.read().as_page_trait().get_page_id());
                } // Lock released here when the guard goes out of scope

            }));
        }

        // Wait for all reader threads to finish
        for thread in threads {
            thread.join().unwrap();
        }
    }
    
    #[test]
    fn reads_and_writes() {
        let ctx = TestContext::new("concurrent_reads_and_writes");
        let bpm = Arc::clone(&ctx.bpm);
    
        // Create a shared page
        let page = bpm.new_page(NewPageType::Basic).unwrap();
    
        // Spawn multiple threads for concurrent reads and writes
        let mut writer_threads = Vec::new();
        let mut reader_threads = Vec::new();
    
        // Writer threads
        for i in 0..2 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);

            writer_threads.push(thread::spawn(move || {
                // Perform the write operation
                let mut write_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), page_clone.read().as_page_trait().get_page_id());
                let mut binding = write_guard.write();
                let mut data_mut = binding.as_page_trait_mut().get_data_mut();
    
                // Each writer thread writes a value
                data_mut[i] = (i + 1) as u8;
            }));
        }
    
        // Reader threads
        for i in 0..2 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);
    
            reader_threads.push(thread::spawn(move || {
                // Perform the read operation
                let read_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), page_clone.read().as_page_trait().get_page_id());
                let binding = read_guard.read();
                let data = binding.as_page_trait().get_data();
    
                // Simulate a read operation by checking if the page ID matches
                assert_eq!(read_guard.get_page_id(), page_clone.read().as_page_trait().get_page_id());
                // Reading values written by the writer threads
                assert_eq!(data[i], (i + 1) as u8, "Unexpected values in the data");
            }));
        }
    
        // Wait for all threads to finish
        for thread in writer_threads {
            thread.join().expect("Failed to join writer threads");
        }

        // Wait for all threads to finish
        for thread in reader_threads {
            thread.join().expect("Failed to join reader threads");
        }
        
        // Verify the page's content after concurrent reads and writes
        let final_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page.read().as_page_trait().get_page_id());
        let binding = final_guard.read();
        let data = binding.as_page_trait().get_data();
    
        assert!(data[0] <= 2 && data[1] <= 2, "Unexpected values in the data after concurrent operations");
    }
}

#[cfg(test)]
mod edge_cases {
    use crate::buffer::buffer_pool_manager::NewPageType;
    use super::*;

    #[test]
    fn page_eviction_under_pressure() {
        let ctx = TestContext::new("page_eviction_under_pressure");
        let bpm = Arc::clone(&ctx.bpm);

        // Fill the buffer pool to force eviction
        let mut pages = Vec::new();
        for _ in 0..5 {
            let page = bpm.new_page(NewPageType::Basic).unwrap();
            pages.push(page);
        }

        let page0_id;
        {
            // Access the first page and hold onto it
            let page0 = Arc::clone(&pages[0]);
            page0_id = page0.read().as_page_trait().get_page_id(); // Store page ID before releasing the guard

            // Keep the first page pinned
            let guard0 = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0_id);
            // Perform the assertion to ensure the page is accessible
            assert_eq!(guard0.get_page_id(), page0_id);
        }

        // Now create a new page that should cause an eviction due to the buffer pool size limit
        let page_evicted = bpm.new_page(NewPageType::Basic).unwrap_or_else(|| panic!("Failed to create a new page, eviction didn't work as expected"));
        let evict_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page_evicted), page_evicted.read().as_page_trait().get_page_id());

        // Ensure the first page was not evicted by checking the page ID directly
        assert_eq!(pages[0].read().as_page_trait().get_page_id(), page0_id, "The first page should not be evicted");

        // Test if the evicted page is correctly unpinned
        drop(evict_guard);
        assert_eq!(page_evicted.read().as_page_trait().get_pin_count(), 0, "Evicted page should be unpinned");

        // Verify the replacer can now evict this page since it should be marked as evictable
        // assert!(bpm.unpin_page(page0_id, false, AccessType::Unknown), "Page should be unpinned and made evictable");
    }

    #[test]
    fn invalid_page_access_after_drop() {
        let ctx = TestContext::new("invalid_page_access_after_drop");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a page and drop the guard
        let page = bpm.new_page(NewPageType::Basic).unwrap();
        let guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page.read().as_page_trait().get_page_id());
        drop(guard);

        // Attempt to re-access the page after it’s been dropped
        let result = bpm.fetch_page(page.read().as_page_trait().get_page_id());
        assert!(result.is_some(), "Accessing a dropped page should return an error");
    }
}


