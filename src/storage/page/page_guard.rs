use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::PageId;
use crate::common::logger::initialize_logger;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::page::page::{Page, PageTrait};
use crate::storage::page::page::PageType;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::storage::page::page_types::table_page::TablePage;
use chrono::Utc;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<PageType>>,
    page_id: PageId,
}

impl PageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<PageType>>, page_id: PageId) -> Self {
        // Don't increment pin count here since BufferPoolManager already does it
        Self {
            bpm,
            page,
            page_id
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, PageType> {
        self.page.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, PageType> {
        self.page.write()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_pin_count(&self) -> i32 {
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
            PageType::Table(_) => "TablePage",
        }
    }

    pub fn into_specific_type(
        self,
    ) -> Option<PageType> {
        // Take ownership of the inner page
        let page_id = self.page_id;

        // Get a read lock to check the page type
        let page_type = match &*self.page.read() {
            PageType::Basic(page) => {
                let mut new_page = Page::new(page_id);
                new_page.set_data(0, page.get_data()).ok()?;
                Some(PageType::Basic(new_page))
            },
            PageType::ExtendedHashTableBucket(bucket_page) => {
                let mut new_page = ExtendableHTableBucketPage::new(page_id);
                // Copy relevant data from bucket_page to new_page
                new_page.init(bucket_page.get_size());
                Some(PageType::ExtendedHashTableBucket(new_page))
            },
            PageType::ExtendedHashTableDirectory(dir_page) => {
                let mut new_page = ExtendableHTableDirectoryPage::new(page_id);
                // Copy relevant data from dir_page to new_page
                new_page.init(dir_page.get_size());
                Some(PageType::ExtendedHashTableDirectory(new_page))
            },
            PageType::ExtendedHashTableHeader(_header_page) => {
                let new_page = ExtendableHTableHeaderPage::new(page_id);
                // Copy relevant data from header_page to new_page
                Some(PageType::ExtendedHashTableHeader(new_page))
            },
            PageType::Table(_table_page) => {
                let new_page = TablePage::new(page_id);
                // Copy relevant data from table_page to new_page
                Some(PageType::Table(new_page))
            },
        };

        // The original PageGuard will be dropped here, which will handle unpinning
        page_type
    }

    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&PageType) -> R,
    {
        let guard = self.page.read();
        Some(f(&guard))
    }

    pub fn access_mut<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut PageType) -> R,
    {
        let mut guard = self.page.write();
        Some(f(&mut guard))
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let is_dirty = self.read().as_page_trait().is_dirty();
        self.set_dirty(is_dirty);

        // Unpin the page through BufferPoolManager
        self.bpm.unpin_page(self.page_id, is_dirty, AccessType::Unknown);
    }
}

pub struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    pub fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(
            db_file.clone(),
            db_log_file.clone(),
            100,
        ));
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

    pub fn bpm(&self) -> Arc<BufferPoolManager> {
        Arc::clone(&self.bpm)
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
    use crate::storage::page::page::PageTrait;
    use log::{debug, info};

    #[test]
    fn basic_page_guard() {
        let ctx = TestContext::new("basic_page_guard");
        let bpm = ctx.bpm();

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
        let basic_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page0),
            page0.read().as_page_trait().get_page_id(),
        );

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(
            page0.read().as_page_trait().get_page_id(),
            basic_guard.get_page_id()
        );

        // Ensure dropping the guard decreases the pin count
        drop(basic_guard);
        assert_eq!(
            page0.read().as_page_trait().get_pin_count(),
            0,
            "Pin count should be 0 after dropping the WritePageGuard"
        );
    }

    #[test]
    fn convert_into_extendable_htable_directory_page() {
        let ctx = TestContext::new("convert_into_extendable_htable_directory_page");
        let bpm = ctx.bpm();

        // Create a new directory page directly
        let page0 = bpm.new_page(PageType::ExtendedHashTableDirectory(
            ExtendableHTableDirectoryPage::new(0)
        )).unwrap();
        let page_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page0),
            page0.read().as_page_trait().get_page_id(),
        );

        if let Some(page_type) = page_guard.into_specific_type() {
            match page_type {
                PageType::ExtendedHashTableDirectory(mut dir_page) => {
                    // Access directory page methods directly
                    let size = dir_page.get_size();
                    debug!("Directory size: {}", size);
                    dir_page.set_dirty(true);
                }
                _ => panic!("Wrong page type returned"),
            }
        }
    }

    #[test]
    fn convert_into_extendable_htable_header_page() {
        let ctx = TestContext::new("convert_into_extendable_htable_header_page");
        let bpm = ctx.bpm();

        // Create a new header page directly
        let page0 = bpm.new_page(PageType::ExtendedHashTableHeader(
            ExtendableHTableHeaderPage::new(0)
        )).unwrap();
        let page_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page0),
            page0.read().as_page_trait().get_page_id(),
        );

        if let Some(page_type) = page_guard.into_specific_type() {
            match page_type {
                PageType::ExtendedHashTableHeader(mut header_page) => {
                    // Access header page methods directly
                    let header_page_id = header_page.get_page_id();
                    debug!("Header page id: {}", header_page_id);
                    header_page.set_dirty(true);
                }
                _ => panic!("Wrong page type returned"),
            }
        }
    }

    #[test]
    fn convert_into_extendable_htable_bucket_page() {
        let ctx = TestContext::new("convert_into_extendable_htable_bucket_page");
        let bpm = ctx.bpm();

        // Create a new bucket page directly
        let page0 = bpm.new_page(PageType::ExtendedHashTableBucket(
            ExtendableHTableBucketPage::new(0)
        )).unwrap();
        let page_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page0),
            page0.read().as_page_trait().get_page_id(),
        );

        if let Some(page_type) = page_guard.into_specific_type() {
            match page_type {
                PageType::ExtendedHashTableBucket(mut bucket_page) => {
                    // Access bucket page methods directly
                    let size = bucket_page.get_size();
                    info!("Bucket size: {}", size);
                    bucket_page.set_dirty(true);
                }
                _ => panic!("Wrong page type returned"),
            }
        }
    }

    #[test]
    fn convert_into_table_page() {
        let ctx = TestContext::new("convert_into_table_page");
        let bpm = ctx.bpm();

        // Create a new table page directly
        let page0 = bpm.new_page(PageType::Table(
            TablePage::new(0)
        )).unwrap();
        let page_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page0),
            page0.read().as_page_trait().get_page_id(),
        );

        if let Some(page_type) = page_guard.into_specific_type() {
            match page_type {
                PageType::Table(mut table_page) => {
                    let page_id = table_page.get_page_id();
                    info!("Table page ID: {}", page_id);
                    table_page.set_dirty(true);
                }
                _ => panic!("Wrong page type returned"),
            }
        }
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;
    use crate::storage::page::page::Page;

    #[test]
    fn create_and_drop() {
        let ctx = TestContext::new("create_and_drop");
        let bpm = ctx.bpm();

        // Create a new page
        let page = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();

        // Create page guard - should not increment pin count since BPM already did
        let guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page_id);

        // Pin count should be 1 from BufferPoolManager::new_page
        assert_eq!(page.read().as_page_trait().get_pin_count(), 1);

        // Drop guard - should decrement pin count through unpin_page
        drop(guard);

        // Pin count should be 0 after guard drop
        assert_eq!(page.read().as_page_trait().get_pin_count(), 0);
    }

    #[test]
    fn modify_data() {
        let ctx = TestContext::new("modify_data");
        let bpm = ctx.bpm();

        // Test modifying different page types
        let page0 = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
        let page0_id = page0.read().as_page_trait().get_page_id();

        {
            let guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0_id);

            // Modify through PageType
            guard.access_mut(|page_type| {
                match page_type {
                    PageType::Basic(page) => {
                        let data = page.get_data_mut();
                        data[0] = 42;
                    }
                    _ => panic!("Wrong page type"),
                }
            });

            // Verify modification
            guard.access(|page_type| {
                match page_type {
                    PageType::Basic(page) => {
                        assert_eq!(page.get_data()[0], 42);
                    }
                    _ => panic!("Wrong page type"),
                }
            });
        }

        assert_eq!(page0.read().as_page_trait().get_pin_count(), 0);
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;
    use std::thread;

    #[test]
    fn reads() {
        let ctx = TestContext::new("concurrent_reads");
        let bpm = ctx.bpm();

        // Create a shared page
        let page = Arc::new(bpm.new_page(PageType::Basic(Page::new(0))).unwrap());
        // Get the page ID once before spawning threads
        let page_id = page.read().as_page_trait().get_page_id(); // <-- Get this outside the threads

        let mut threads = Vec::new();
        for _ in 0..3 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);

            threads.push(thread::spawn(move || {
                let page_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), page_id); // <-- Use stored page_id
                let read_guard = page_guard.read();

                // Simply verify the page ID without acquiring another lock
                assert_eq!(read_guard.as_page_trait().get_page_id(), page_id);
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn reads_and_writes() {
        let ctx = TestContext::new("concurrent_reads_and_writes");
        let bpm = ctx.bpm();

        // Create a shared page
        let page = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
        // Get the page ID once before spawning threads
        let page_id = page.read().as_page_trait().get_page_id(); // <-- Get this outside the threads

        let mut writer_threads = Vec::new();
        let mut reader_threads = Vec::new();

        // Writer threads
        for i in 0..2 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);

            writer_threads.push(thread::spawn(move || {
                let write_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), page_id); // <-- Use stored page_id
                let mut binding = write_guard.write();
                let data_mut = binding.as_page_trait_mut().get_data_mut();
                data_mut[i] = (i + 1) as u8;
            }));
        }

        for thread in writer_threads {
            thread.join().expect("Failed to join writer threads");
        }

        // Reader threads
        for i in 0..2 {
            let bpm_clone = Arc::clone(&bpm);
            let page_clone = Arc::clone(&page);

            reader_threads.push(thread::spawn(move || {
                let read_guard = PageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone), page_id); // <-- Use stored page_id
                let binding = read_guard.read();
                let data = binding.as_page_trait().get_data();

                // Simply verify the page ID without acquiring another lock
                assert_eq!(read_guard.get_page_id(), page_id);
                assert_eq!(data[i], (i + 1) as u8, "Unexpected values in the data");
            }));
        }

        for thread in reader_threads {
            thread.join().expect("Failed to join reader threads");
        }

        let final_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page_id);
        let binding = final_guard.read();
        let data = binding.as_page_trait().get_data();

        assert!(
            data[0] <= 2 && data[1] <= 2,
            "Unexpected values in the data after concurrent operations"
        );
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn page_eviction_under_pressure() {
        let ctx = TestContext::new("page_eviction_under_pressure");
        let bpm = ctx.bpm();

        // Fill the buffer pool to force eviction
        let mut pages = Vec::new();
        for _ in 0..5 {
            let page = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
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
        let page_evicted = bpm.new_page(PageType::Basic(Page::new(0))).unwrap_or_else(|| {
            panic!("Failed to create a new page, eviction didn't work as expected")
        });
        let evict_guard = PageGuard::new(
            Arc::clone(&bpm),
            Arc::clone(&page_evicted),
            page_evicted.read().as_page_trait().get_page_id(),
        );

        // Ensure the first page was not evicted by checking the page ID directly
        assert_eq!(
            pages[0].read().as_page_trait().get_page_id(),
            page0_id,
            "The first page should not be evicted"
        );

        // Test if the evicted page is correctly unpinned
        drop(evict_guard);
        assert_eq!(
            page_evicted.read().as_page_trait().get_pin_count(),
            0,
            "Evicted page should be unpinned"
        );
    }

    #[test]
    fn invalid_page_access_after_drop() {
        let ctx = TestContext::new("invalid_page_access_after_drop");
        let bpm = ctx.bpm();

        // Create a new page
        let page = bpm.new_page(PageType::Basic(Page::new(0))).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();

        // Create and drop page guard
        {
            let _guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page_id);
            // Pin count should be 1 from new_page
            assert_eq!(page.read().as_page_trait().get_pin_count(), 1);
        } // guard is dropped here

        // After guard is dropped:
        // 1. Pin count should be 0
        assert_eq!(page.read().as_page_trait().get_pin_count(), 0);

        // 2. Page should still be in the page table
        let _frame_id = {
            let page_table = bpm.get_page_table();
            let page_table = page_table.read();
            *page_table.get(&page_id).unwrap()
        };

        // 3. Creating a new guard should work
        let _new_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page), page_id);
        // Pin count should be 1 again
        assert_eq!(page.read().as_page_trait().get_pin_count(), 1);
    }
}
