use crate::common::config::PageId;
use crate::storage::page::page::{Page, PageTrait, PageType};
use log::trace;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

pub struct PageGuard<P: ?Sized + PageTrait> {
    page: Arc<RwLock<P>>,
    page_id: PageId,
}

impl<P: Page + 'static> PageGuard<P> {
    /// Creates a new page guard for an existing page, incrementing the pin count
    pub fn new(page: Arc<RwLock<P>>, page_id: PageId) -> Self {
        // Increment pin count when creating a guard for an existing page
        page.write().increment_pin_count(); // Increment instead of setting to 1
        trace!(
            "Created new page guard for page {} with pin count {}",
            page_id,
            page.read().get_pin_count()
        );

        Self { page, page_id }
    }

    /// Creates a new page guard for a newly created page (pin count already set)
    pub fn new_for_new_page(page: Arc<RwLock<P>>, page_id: PageId) -> Self {
        trace!(
            "Created new page guard for new page {} with pin count {}",
            page_id,
            page.read().get_pin_count()
        );

        Self { page, page_id }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, P> {
        self.page.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, P> {
        self.page.write()
    }

    pub fn get_page_type(&self) -> PageType {
        self.read().get_page_type()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_page(&self) -> &Arc<RwLock<P>> {
        &self.page
    }
}

impl<P: PageTrait + ?Sized> PageGuard<P> {
    fn unpin(&self, is_dirty: bool) {
        let pin_count = self.page.read().get_pin_count();
        trace!(
            "Unpinning page {} with pin count {}, dirty: {}",
            self.page_id,
            pin_count,
            is_dirty
        );

        // Decrement pin count
        let mut page = self.page.write();
        page.decrement_pin_count();

        if is_dirty {
            page.set_dirty(true);
        }

        // If pin count reaches zero, mark page as evictable
        if page.get_pin_count() == 0 {
            trace!("Page {} is now evictable", self.page_id);
            // The buffer pool manager will handle marking the page as evictable
            // when it processes the unpin
        }
    }
}

impl<P: PageTrait + ?Sized> Drop for PageGuard<P> {
    fn drop(&mut self) {
        let initial_pin_count = self.page.read().get_pin_count();
        let was_dirty = self.page.read().is_dirty();

        trace!(
            "Dropping page {} with pin count {}, dirty: {}",
            self.page_id,
            initial_pin_count,
            was_dirty
        );

        // If pin count reaches zero, mark page as dirty to ensure it's written back
        let should_mark_dirty = initial_pin_count == 1;

        // Unpin the page
        self.unpin(should_mark_dirty);

        let final_state = self.page.read();
        trace!(
            "Page {} dropped, pin count: {} -> {}, dirty: {} -> {}",
            self.page_id,
            initial_pin_count,
            final_state.get_pin_count(),
            was_dirty,
            final_state.is_dirty()
        );
    }
}

impl PageGuard<dyn PageTrait> {
    pub fn new_untyped(page: Arc<RwLock<dyn PageTrait>>, page_id: PageId) -> Self {
        Self { page, page_id }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, dyn PageTrait> {
        self.page.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, dyn PageTrait> {
        self.page.write()
    }

    pub fn get_page_type(&self) -> PageType {
        self.read().get_page_type()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_page(&self) -> &Arc<RwLock<dyn PageTrait>> {
        &self.page
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::page::page::PAGE_TYPE_OFFSET;
    use crate::storage::page::page::{BasicPage, PageTrait, PageType};
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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

            // Create tests/data directory if it doesn't exist
            std::fs::create_dir_all("tests/data").expect("Failed to create tests/data directory");

            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
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

    #[test]
    fn test_basic_page_operations() {
        let ctx = TestContext::new("basic_page_operations");
        let bpm = ctx.bpm();

        // Create a new page using the buffer pool manager
        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");

        // Test read operations
        {
            let data = page_guard.read();
            assert_eq!(data.get_page_type(), PageType::Basic);
        }

        // Test write operations
        {
            let mut data = page_guard.write();
            data.get_data_mut()[0] = 42;
            assert_eq!(data.get_data()[0], 42);
        }
    }

    #[test]
    fn test_page_guard_type_safety() {
        let ctx = TestContext::new("page_guard_type_safety");
        let bpm = ctx.bpm();

        // Create pages of different types
        let basic_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create basic page");

        // Verify correct type identification
        assert_eq!(basic_guard.get_page_type(), PageType::Basic);

        // Verify type persistence across guard creation/drop
        let page_id = basic_guard.get_page_id();
        drop(basic_guard);

        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        assert_eq!(fetched_guard.get_page_type(), PageType::Basic);
    }

    #[test]
    fn test_concurrent_access() {
        let ctx = TestContext::new("concurrent_access");
        let bpm = ctx.bpm();

        // Create a new page
        let page_guard1 = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");
        let page_id = page_guard1.read().get_page_id();

        // Create another guard for the same page
        let page_guard2 = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Test concurrent reads
        {
            let data1 = page_guard1.read();
            let data2 = page_guard2.read();
            assert_eq!(data1.get_page_id(), data2.get_page_id());
        }

        // Test write followed by read
        {
            let mut data = page_guard1.write();
            data.get_data_mut()[0] = 42;
            drop(data);

            let data = page_guard2.read();
            assert_eq!(data.get_data()[0], 42);
        }
    }

    #[test]
    fn test_buffer_pool_interaction() {
        let ctx = TestContext::new("buffer_pool_interaction");
        let bpm = ctx.bpm();

        // Fill buffer pool
        let mut pages = Vec::new();
        for _ in 0..5 {
            let page_guard = bpm
                .new_page::<BasicPage>()
                .expect("Failed to create new page");
            pages.push(page_guard);
        }

        // Verify all pages are pinned
        for guard in &pages {
            assert_eq!(
                guard.read().get_pin_count(),
                1,
                "New page should have pin count 1"
            );
        }

        // Drop guards one by one
        for guard in pages {
            let page_id = guard.read().get_page_id();
            drop(guard);
            // Verify the page can be fetched again
            if let Some(fetched_page) = bpm.fetch_page::<BasicPage>(page_id) {
                assert_eq!(
                    fetched_page.read().get_pin_count(),
                    1,
                    "Fetched page should have pin count 1"
                );
            }
        }
    }

    #[test]
    fn test_page_guard_drop_behavior() {
        let ctx = TestContext::new("page_guard_drop");
        let bpm = ctx.bpm();

        let page_id = {
            // Create a new page in a nested scope
            let page_guard = bpm
                .new_page::<BasicPage>()
                .expect("Failed to create new page");

            // Write some data (to a non-type byte location)
            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = 42; // Write to offset 1 instead of 0
                data.set_dirty(true);

                // Verify type byte is preserved
                assert_eq!(
                    data.get_data()[PAGE_TYPE_OFFSET],
                    PageType::Basic.to_u8(),
                    "Page type should be preserved"
                );
            }

            let page_id = page_guard.get_page_id();

            // Verify pin count before drop
            assert_eq!(
                page_guard.read().get_pin_count(),
                1,
                "Initial pin count should be 1"
            );

            page_id
        }; // page_guard is dropped here

        // Fetch the page again and verify state
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify pin count is 1 (fetching a page pins it)
        assert_eq!(
            fetched_guard.read().get_pin_count(),
            1,
            "Fetched page should have pin count 1"
        );

        // Verify data persisted and type byte is preserved
        let data = fetched_guard.read();
        assert_eq!(data.get_data()[1], 42, "Data should be preserved");
        assert_eq!(
            data.get_data()[PAGE_TYPE_OFFSET],
            PageType::Basic.to_u8(),
            "Page type should be preserved"
        );
    }

    #[test]
    fn test_page_guard_concurrent_access_patterns() {
        let ctx = TestContext::new("page_guard_concurrent");
        let bpm = ctx.bpm();

        // Create a new page
        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");
        let page_id = page_guard.get_page_id();

        // Spawn multiple threads to access the page
        let mut handles = vec![];
        for i in 0..5 {
            let bpm_clone = Arc::clone(&bpm);
            let handle = thread::spawn(move || {
                // Each thread fetches the same page
                let guard = bpm_clone
                    .fetch_page::<BasicPage>(page_id)
                    .expect("Failed to fetch page");

                // Write unique data
                {
                    let mut data = guard.write();
                    data.get_data_mut()[i] = (i + 1) as u8;
                    data.set_dirty(true);
                }

                // Small delay to increase chance of concurrent access
                thread::sleep(Duration::from_millis(10));

                // Read back the data
                let data = guard.read();
                assert_eq!(data.get_data()[i], (i + 1) as u8);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state
        let final_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        let data = final_guard.read();
        for i in 0..5 {
            assert_eq!(data.get_data()[i], (i + 1) as u8);
        }
    }

    #[test]
    fn test_page_guard_dirty_flag() {
        let ctx = TestContext::new("page_guard_dirty");
        let bpm = ctx.bpm();

        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");

        // Initially not dirty
        assert!(!page_guard.read().is_dirty());

        // Modify data and explicitly set dirty flag
        {
            let mut data = page_guard.write();
            data.get_data_mut()[1] = 42; // Write to non-type byte
            data.set_dirty(true); // Explicitly set dirty flag
            assert!(data.is_dirty(), "Page should be dirty after modification");
        }

        // Verify dirty flag persists after write lock is released
        assert!(
            page_guard.read().is_dirty(),
            "Dirty flag should persist after write lock release"
        );

        // Test dirty flag persistence across guard drop and fetch
        let page_id = page_guard.get_page_id();
        drop(page_guard);

        // Fetch the page again and verify dirty flag
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        assert!(
            fetched_guard.read().is_dirty(),
            "Dirty flag should persist after page fetch"
        );
    }

    #[test]
    fn test_page_guard_multiple_guards() {
        let ctx = TestContext::new("page_guard_multiple");
        let bpm = ctx.bpm();

        // Create initial page
        let guard1 = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");
        let page_id = guard1.get_page_id();

        // Verify initial pin count
        assert_eq!(
            guard1.read().get_pin_count(),
            1,
            "Initial pin count should be 1"
        );

        // Create additional guards for the same page
        let guard2 = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify pin count after second guard
        assert_eq!(
            guard1.read().get_pin_count(),
            2,
            "Pin count should be 2 after second guard"
        );

        let guard3 = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify all guards point to the same page
        assert_eq!(guard1.get_page_id(), guard2.get_page_id());
        assert_eq!(guard2.get_page_id(), guard3.get_page_id());

        // Verify pin count reflects multiple guards
        assert_eq!(
            guard1.read().get_pin_count(),
            3,
            "Pin count should be 3 with three guards"
        );
        assert_eq!(
            guard2.read().get_pin_count(),
            3,
            "All guards should see same pin count"
        );
        assert_eq!(
            guard3.read().get_pin_count(),
            3,
            "All guards should see same pin count"
        );

        // Drop guards one by one and verify pin count
        drop(guard3);
        thread::sleep(Duration::from_millis(1)); // Small delay to ensure pin count update
        assert_eq!(
            guard1.read().get_pin_count(),
            2,
            "Pin count should be 2 after dropping first guard"
        );

        drop(guard2);
        thread::sleep(Duration::from_millis(1)); // Small delay to ensure pin count update
        assert_eq!(
            guard1.read().get_pin_count(),
            1,
            "Pin count should be 1 after dropping second guard"
        );

        drop(guard1);

        // Verify final state
        let final_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        assert_eq!(
            final_guard.read().get_pin_count(),
            1,
            "Pin count should be 1 for newly fetched guard"
        );
    }

    #[test]
    fn test_page_guard_drop_writeback() {
        let ctx = TestContext::new("page_guard_writeback");
        let bpm = ctx.bpm();

        let page_id = {
            // Create a new page
            let page_guard = bpm
                .new_page::<BasicPage>()
                .expect("Failed to create new page");

            // Write some data but don't mark as dirty
            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = 42; // Write to offset 1 to preserve page type
                assert!(!data.is_dirty());

                // Verify page type is preserved
                assert_eq!(
                    data.get_data()[PAGE_TYPE_OFFSET],
                    PageType::Basic.to_u8(),
                    "Page type should be preserved"
                );
            }

            let id = page_guard.get_page_id();

            // Verify initial state
            assert_eq!(page_guard.read().get_pin_count(), 1);
            assert!(!page_guard.read().is_dirty());

            id
        }; // page_guard is dropped here - should mark as dirty since pin count reaches 0

        // Fetch the page and verify state
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Page should be marked dirty after being unpinned
        assert!(
            fetched_guard.read().is_dirty(),
            "Page should be marked dirty when unpinned"
        );
        assert_eq!(fetched_guard.read().get_pin_count(), 1);

        // Verify data and type are preserved
        let data = fetched_guard.read();
        assert_eq!(data.get_data()[1], 42, "Data should be preserved");
        assert_eq!(
            data.get_data()[PAGE_TYPE_OFFSET],
            PageType::Basic.to_u8(),
            "Page type should be preserved"
        );
    }
}
