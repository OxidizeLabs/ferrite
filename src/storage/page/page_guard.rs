//! # Page Guard
//!
//! This module provides [`PageGuard`], an RAII-based smart pointer that manages
//! page pinning/unpinning and dirty tracking. Guards ensure pages cannot be evicted
//! from the buffer pool while in use and automatically propagate dirty state on drop.
//!
//! ## Architecture
//!
//! ```text
//!   Buffer Pool Manager                           Page Guard
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────┐          ┌─────────────────────────────────────┐
//!   │   BufferPoolManager     │          │            PageGuard<P>             │
//!   │                         │ creates  │                                     │
//!   │  fetch_page() ──────────┼─────────►│  page: Arc<RwLock<P>>               │
//!   │  new_page()             │          │  page_id: PageId                    │
//!   │                         │          │  unpinner: Option<Arc<PageUnpinner>>│
//!   │  ┌───────────────────┐  │          │  dirty: AtomicBool                  │
//!   │  │  PageUnpinner     │◄─┼──────────┤                                     │
//!   │  │  (callback)       │  │  on drop │  ┌───────────────────────────────┐  │
//!   │  └───────────────────┘  │          │  │       Arc<RwLock<P>>          │  │
//!   │                         │          │  │  ┌─────────────────────────┐  │  │
//!   │  page_table: HashMap    │          │  │  │         Page            │  │  │
//!   │    page_id → Arc<...>   │          │  │  │  - data: [u8; 4096]     │  │  │
//!   │                         │          │  │  │  - pin_count: i32       │  │  │
//!   └─────────────────────────┘          │  │  │  - is_dirty: bool       │  │  │
//!                                        │  │  └─────────────────────────┘  │  │
//!                                        │  └───────────────────────────────┘  │
//!                                        └─────────────────────────────────────┘
//! ```
//!
//! ## RAII Lifecycle
//!
//! ```text
//!   Guard Creation                          Guard Usage                 Guard Drop
//!   ══════════════════════════════════════════════════════════════════════════════
//!
//!   fetch_page(id)                         read() / write()            drop(guard)
//!        │                                      │                           │
//!        ▼                                      ▼                           ▼
//!   ┌──────────────┐                     ┌─────────────┐              ┌───────────┐
//!   │ Increment    │                     │ Acquire     │              │ Check     │
//!   │ pin_count    │                     │ RwLock      │              │ dirty     │
//!   └──────┬───────┘                     └──────┬──────┘              └─────┬─────┘
//!          │                                    │                           │
//!          ▼                                    ▼                           ▼
//!   ┌──────────────┐                     ┌─────────────┐              ┌───────────┐
//!   │ Create guard │                     │ write()     │              │ Unpinner  │
//!   │ with unpinner│                     │ sets dirty  │              │ callback  │
//!   │ callback     │                     │ flag        │              │ OR local  │
//!   └──────────────┘                     └─────────────┘              │ decrement │
//!                                                                     └───────────┘
//!          │                                                                │
//!   Page CANNOT be                                              Page CAN be evicted
//!   evicted (pinned)                                            (if pin_count = 0)
//! ```
//!
//! ## Key Components
//!
//! | Component       | Description                                              |
//! |-----------------|----------------------------------------------------------|
//! | `PageGuard<P>`  | Generic guard for typed pages (`P: Page`)                |
//! | `PageGuard<dyn>`| Untyped guard for trait object pages                     |
//! | `PageUnpinner`  | Callback trait for buffer pool unpin delegation          |
//! | `dirty`         | AtomicBool tracking if guard performed write access      |
//!
//! ## Core Operations
//!
//! | Method           | Description                                             |
//! |------------------|---------------------------------------------------------|
//! | `new()`          | Create guard for existing page (increments pin count)  |
//! | `new_for_new_page()` | Create guard for newly allocated page              |
//! | `read()`         | Acquire shared read lock (`RwLockReadGuard`)           |
//! | `write()`        | Acquire exclusive write lock, marks dirty              |
//! | `get_page_id()`  | Return the page's ID                                   |
//! | `get_page_type()`| Read page type from underlying page                    |
//! | `get_page()`     | Expose raw `Arc<RwLock<P>>` for advanced use cases     |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
//! use crate::storage::page::{BasicPage, PageTrait};
//!
//! // Fetch an existing page (creates a guard, increments pin count)
//! let guard = bpm.fetch_page::<BasicPage>(page_id)?;
//!
//! // Multiple concurrent reads are allowed
//! {
//!     let data1 = guard.read();
//!     let data2 = guard.read();  // Would deadlock! Use separate guards
//! }
//!
//! // Write access (exclusive lock, auto-marks dirty)
//! {
//!     let mut data = guard.write();
//!     data.get_data_mut()[10] = 42;
//!     // dirty flag is automatically set
//! }
//!
//! // Multiple guards for the same page
//! let guard2 = bpm.fetch_page::<BasicPage>(page_id)?;
//! assert_eq!(guard.read().get_pin_count(), 2);  // Both guards pin the page
//!
//! // Guards dropped here → unpin callbacks invoked
//! // When pin_count reaches 0, page becomes eligible for eviction
//! ```
//!
//! ## Dirty Flag Semantics
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        Dirty Flag Propagation                           │
//!   │                                                                         │
//!   │   guard.write()                                                         │
//!   │        │                                                                │
//!   │        ├──► guard.dirty.store(true)     (guard-level flag)              │
//!   │        │                                                                │
//!   │        └──► page.set_dirty(true)        (page-level flag)               │
//!   │                                                                         │
//!   │   drop(guard)                                                           │
//!   │        │                                                                │
//!   │        └──► unpinner.unpin_page(id, is_dirty)                           │
//!   │                  │                                                      │
//!   │                  └──► Buffer pool schedules write-back if dirty         │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - **RwLock**: Guards use `parking_lot::RwLock` for read/write synchronization
//! - **AtomicBool**: The `dirty` flag uses atomic operations for thread-safe updates
//! - **Multiple guards**: Multiple threads can hold guards to the same page
//! - **Pin count**: Protected by the RwLock, updated atomically with page access
//!
//! ## Untyped Guards
//!
//! For cases where the concrete page type is unknown at compile time, use
//! `PageGuard<dyn PageTrait>`:
//!
//! ```rust,ignore
//! // Create untyped guard (used internally by buffer pool)
//! let untyped_guard = PageGuard::new_untyped(page_arc, page_id, Some(unpinner));
//!
//! // Access via trait methods
//! let data = untyped_guard.read();
//! let page_type = data.get_page_type();
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use log::trace;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::config::PageId;
use crate::storage::page::{Page, PageTrait, PageType};

/// A callback interface used by `PageGuard` to notify the buffer pool manager
/// when the guard is dropped (i.e., when the page should be unpinned).
///
/// This is intentionally minimal to avoid tight coupling between the storage
/// layer and a specific buffer pool implementation.
pub trait PageUnpinner: Send + Sync {
    /// Unpin the page.
    ///
    /// Returns `true` if the page was successfully unpinned.
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool;
}

pub struct PageGuard<P: ?Sized + PageTrait> {
    page: Arc<RwLock<P>>,
    page_id: PageId,
    unpinner: Option<Arc<dyn PageUnpinner>>,
    /// Tracks whether *this guard* performed a mutable access (and therefore
    /// should mark the page dirty on unpin).
    dirty: AtomicBool,
}

impl<P: Page + 'static> PageGuard<P> {
    /// Creates a new page guard for an existing page, incrementing the pin count
    pub(crate) fn new(
        page: Arc<RwLock<P>>,
        page_id: PageId,
        unpinner: Option<Arc<dyn PageUnpinner>>,
    ) -> Self {
        // Increment pin count when creating a guard for an existing page
        let pin_count = {
            let mut page_guard = page.write();
            page_guard.increment_pin_count(); // Increment instead of setting to 1
            page_guard.get_pin_count()
        };
        trace!(
            "Created new page guard for page {} with pin count {}",
            page_id, pin_count
        );

        Self {
            page,
            page_id,
            unpinner,
            dirty: AtomicBool::new(false),
        }
    }

    /// Creates a new page guard for a newly created page (pin count already set)
    pub(crate) fn new_for_new_page(
        page: Arc<RwLock<P>>,
        page_id: PageId,
        unpinner: Option<Arc<dyn PageUnpinner>>,
    ) -> Self {
        let pin_count = page.read().get_pin_count();
        trace!(
            "Created new page guard for new page {} with pin count {}",
            page_id, pin_count
        );

        Self {
            page,
            page_id,
            unpinner,
            dirty: AtomicBool::new(false),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, P> {
        self.page.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, P> {
        // "Typical" DB buffer-pool semantics: if you obtain a mutable page guard,
        // we conservatively treat the page as dirty.
        self.dirty.store(true, Ordering::Relaxed);
        let mut guard = self.page.write();
        guard.set_dirty(true);
        guard
    }

    pub fn get_page_type(&self) -> PageType {
        self.read().get_page_type()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Exposes the underlying page Arc.
    ///
    /// This is primarily intended for advanced locking/observability use cases
    /// (e.g., latch-crabbing tests) where the raw lock needs to be probed.
    /// Normal callers should prefer `read()` / `write()` to interact with page
    /// data to ensure pin/unpin semantics remain intact.
    pub fn get_page(&self) -> &Arc<RwLock<P>> {
        &self.page
    }
}

impl<P: PageTrait + ?Sized> PageGuard<P> {
    // NOTE: We intentionally do not expose unpin publicly. Dropping the guard
    // is the only supported way to unpin.
}

impl<P: PageTrait + ?Sized> Drop for PageGuard<P> {
    fn drop(&mut self) {
        let is_dirty = self.dirty.load(Ordering::Relaxed);

        if let Some(unpinner) = &self.unpinner {
            trace!(
                "Dropping page {} (delegating unpin; dirty={})",
                self.page_id, is_dirty
            );
            let ok = unpinner.unpin_page(self.page_id, is_dirty);
            trace!("Unpin delegated for page {} -> {}", self.page_id, ok);
            return;
        }

        // Fallback: maintain pin count in-page (used for tests or non-BPM pages).
        // Do not auto-dirty on last unpin; dirty is determined by write access.
        let mut page = self.page.write();
        let initial_pin_count = page.get_pin_count();
        let was_dirty = page.is_dirty();
        page.decrement_pin_count();
        if is_dirty {
            page.set_dirty(true);
        }
        trace!(
            "Page {} dropped (local unpin), pin count: {} -> {}, dirty: {} -> {}",
            self.page_id,
            initial_pin_count,
            page.get_pin_count(),
            was_dirty,
            page.is_dirty()
        );
    }
}

impl PageGuard<dyn PageTrait> {
    pub fn read(&self) -> RwLockReadGuard<'_, dyn PageTrait> {
        self.page.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, dyn PageTrait> {
        self.dirty.store(true, Ordering::Relaxed);
        let mut guard = self.page.write();
        guard.set_dirty(true);
        guard
    }

    pub fn get_page_type(&self) -> PageType {
        self.read().get_page_type()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Exposes the underlying page Arc for untyped pages.
    ///
    /// Intended for advanced locking/observability use cases. Prefer the
    /// typed `read()`/`write()` accessors in most code paths.
    pub fn get_page(&self) -> &Arc<RwLock<dyn PageTrait>> {
        &self.page
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use parking_lot::RwLock;
    use tempfile::TempDir;

    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::page::{BasicPage, PAGE_TYPE_OFFSET, PageTrait, PageType};

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
        // Keep temp directory alive for the lifetime of the test context.
        // If dropped early, the database/log paths can disappear while the disk manager
        // is still using them.
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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
            Arc::clone(&self.bpm)
        }
    }

    #[tokio::test]
    async fn test_basic_page_operations() {
        let ctx = TestContext::new("basic_page_operations").await;
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

    #[tokio::test]
    async fn test_page_guard_type_safety() {
        let ctx = TestContext::new("page_guard_type_safety").await;
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

    #[tokio::test]
    async fn test_concurrent_access() {
        let ctx = TestContext::new("concurrent_access").await;
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

    #[tokio::test]
    async fn test_buffer_pool_interaction() {
        let ctx = TestContext::new("buffer_pool_interaction").await;
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

    #[tokio::test]
    async fn test_page_guard_drop_behavior() {
        let ctx = TestContext::new("page_guard_drop").await;
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

    #[tokio::test]
    async fn test_page_guard_concurrent_access_patterns() {
        let ctx = TestContext::new("page_guard_concurrent").await;
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

    #[tokio::test]
    async fn test_page_guard_dirty_flag() {
        let ctx = TestContext::new("page_guard_dirty").await;
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

    #[tokio::test]
    async fn test_page_guard_multiple_guards() {
        let ctx = TestContext::new("page_guard_multiple").await;
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
        tokio::time::sleep(Duration::from_millis(1)).await; // Small delay to ensure pin count update
        assert_eq!(
            guard1.read().get_pin_count(),
            2,
            "Pin count should be 2 after dropping first guard"
        );

        drop(guard2);
        tokio::time::sleep(Duration::from_millis(1)).await; // Small delay to ensure pin count update
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

    #[tokio::test]
    async fn test_page_guard_drop_writeback() {
        let ctx = TestContext::new("page_guard_writeback").await;
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
                // With guard semantics, obtaining a write lock marks the page dirty.
                assert!(data.is_dirty());

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
            assert!(page_guard.read().is_dirty());

            id
        }; // page_guard is dropped here - dirty state should be preserved

        // Fetch the page and verify state
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Page should remain dirty after being unpinned
        assert!(
            fetched_guard.read().is_dirty(),
            "Page should remain dirty when unpinned"
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
