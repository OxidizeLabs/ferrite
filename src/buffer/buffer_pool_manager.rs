use log::{debug, error, info, warn};
use spin::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use futures::AsyncWriteExt;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId, DB_PAGE_SIZE};
use crate::storage::disk::disk_manager::{DiskIO, FileDiskManager};
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::page::page::Page;
use crate::storage::page::page_guard::{BasicPageGuard, ReadPageGuard, WritePageGuard};

const INVALID_PAGE_ID: PageId = -1;

/// The `BufferPoolManager` is responsible for managing the buffer pool,
/// including fetching and unpinning pages, and handling page replacement.
pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicI32,
    pages: Arc<RwLock<Vec<Option<Arc<RwLock<Page>>>>>>,
    page_table: Arc<RwLock<HashMap<PageId, FrameId>>>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    free_list: Arc<RwLock<Vec<FrameId>>>,
    disk_scheduler: Arc<RwLock<DiskScheduler>>,
    disk_manager: Arc<FileDiskManager>,
}

impl BufferPoolManager {
    /// Creates a new `BufferPoolManager`.
    ///
    /// # Parameters
    /// - `pool_size`: The size of the buffer pool.
    /// - `disk_scheduler`: A reference to the disk scheduler.
    /// - `disk_manager`: A reference to the disk manager.
    /// - `replacer`: A reference to the page replacement policy.
    ///
    /// # Returns
    /// A new `BufferPoolManager` instance.
    pub fn new(
        pool_size: usize,
        disk_scheduler: Arc<RwLock<DiskScheduler>>,
        disk_manager: Arc<FileDiskManager>,
        replacer: Arc<RwLock<LRUKReplacer>>,
    ) -> Self {
        let free_list: Vec<FrameId> = (0..pool_size as FrameId).collect();
        info!("BufferPoolManager initialized with pool size: {}", pool_size);
        Self {
            pool_size,
            next_page_id: AtomicI32::new(0),
            pages: Arc::new(RwLock::new(vec![None; pool_size])),
            page_table: Arc::new(RwLock::new(HashMap::new())),
            replacer,
            free_list: Arc::new(RwLock::new(free_list)),
            disk_scheduler,
            disk_manager,
        }
    }

    /// Returns the size of the buffer pool.
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns the pages in the buffer pool.
    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Arc<RwLock<Page>>>>>> {
        self.pages.clone()
    }

    pub fn get_page_table(&self) -> Arc<RwLock<HashMap<PageId, FrameId>>> {
        self.page_table.clone()
    }

    /// Creates a new page and returns it.
    ///
    /// # Returns
    /// An optional new page.
    pub fn new_page(&self) -> Option<Arc<RwLock<Page>>> {
        debug!("Attempting to create a new page...");

        let mut replacer = self.replacer.write(); // Acquire lock for the replacer
        let mut page_table = self.page_table.write(); // Acquire lock for the page table
        let mut pages = self.pages.write(); // Acquire write lock for the pages
        let mut free_list = self.free_list.write(); // Acquire lock for the free list


        // Step 1: Acquire a frame ID from the free list or evict a page if necessary.
        let frame_id = {
            if let Some(id) = free_list.pop() {
                id
            } else {
                // No frames available in the free list; need to evict one.
                replacer.evict().unwrap_or(-1)
            }
        };

        // Check if we have a valid frame ID.
        if frame_id == -1 {
            warn!("Failed to create a new page: no available frame");
            return None;
        }

        debug!("Selected frame ID: {}", frame_id);

        // Step 2: If a page exists in the frame, evict it if necessary.
        if let Some(old_page_id) = {
            page_table.iter()
                .find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);
            if let Some(page_rwlock) = pages[frame_id as usize].as_mut() {
                let mut page = page_rwlock.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }
            page_table.remove(&old_page_id);
        }

        // Step 3: Create a new page wrapped in an `Arc`.
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let new_page = Arc::new(RwLock::new(Page::new(new_page_id)));
        debug!("Created new page with ID: {}", new_page_id);

        {
            pages[frame_id as usize] = Some(new_page.clone()); // Clone the `Arc`
        }

        page_table.insert(new_page_id as PageId, frame_id);
        debug!("Inserted new page with ID: {} into page table", new_page_id);

        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);

        Some(new_page)
    }

    /// Creates a new page guard and returns it.
    ///
    /// # Returns
    /// An optional new `BasicPageGuard`.
    pub fn new_page_guarded(self: &Arc<BufferPoolManager>) -> Option<BasicPageGuard> {
        debug!("Creating a new page guard...");

        let mut free_list = self.free_list.write();  // Use spin::RwLock for free_list
        let mut replacer = self.replacer.write();  // Use spin::RwLock for replacer
        let mut pages = self.pages.write();  // Use spin::RwLock for pages

        // Acquire frame ID from the free list or evict a page if necessary
        let frame_id = free_list.pop().unwrap_or_else(|| {
            replacer.evict().unwrap_or(-1)
        });

        if frame_id == -1 {
            warn!("Failed to create a new page guard: no available frame");
            return None;
        }

        debug!("Selected frame ID: {}", frame_id);

        // Acquire the page table lock to check and modify the page table
        let mut page_table = self.page_table.write();  // Use spin::RwLock for page_table

        // Evict the old page if necessary
        if let Some(old_page_id) = page_table
            .iter()
            .find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
        {
            debug!("Evicting old page with ID: {}", old_page_id);

            // Handle page eviction
            if let Some(page_rwlock) = pages[frame_id as usize].as_mut() {
                let mut page = page_rwlock.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }

            // Remove the old page from the page table
            page_table.remove(&old_page_id);
        }

        // Create a new page
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let new_page = Arc::new(RwLock::new(Page::new(new_page_id))); // Wrap the new page in an Arc
        debug!("Created new page with ID: {}", new_page_id);

        // Insert the new page into pages and page_table
        pages[frame_id as usize] = Some(Arc::clone(&new_page));
        page_table.insert(new_page_id as PageId, frame_id);
        debug!("Inserted new page with ID: {} into page table", new_page_id);

        // Update the replacer
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);

        Some(BasicPageGuard::new(
            Arc::clone(self),
            new_page,
        ))
    }

    /// Writes data to the page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to write to.
    /// - `data`: The data to write to the page.
    pub fn write_page(&self, page_id: PageId, data: [u8; DB_PAGE_SIZE]) {
        info!("Writing to page with ID: {}", page_id);

        // Acquire read lock on the page collection
        let pages = self.pages.read();

        // Find the target page
        let mut target_page = None;
        for (i, page_opt) in pages.iter().enumerate() {
            if let Some(page) = page_opt {
                let page_data = page.read(); // Acquire read lock on the page
                if page_data.get_page_id() == page_id {
                    target_page = Some((page.clone(), i));
                    break;
                }
            }
        }

        // Drop the read lock before acquiring the write lock on the target page
        drop(pages);

        if let Some((page, _index)) = target_page {
            // Acquire write lock on the target page
            let mut page_data = page.write();

            // Update the page data
            page_data.set_data(&data);
            page_data.set_dirty(true);

            debug!("Page {} written and marked dirty", page_id);
        } else {
            // Optionally log if the page was not found
            warn!("Page with ID {} not found for writing", page_id);
        }
    }

    /// Fetches a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<Page>>`.
    pub fn fetch_page(&self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        debug!("Fetching page with ID: {}", page_id);

        // Step 1: Check if the page is already in memory
        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            debug!("Page {} found in memory, frame ID: {}", page_id, frame_id);

            let pages = self.pages.read(); // Acquire read lock
            if let Some(page) = pages.get(frame_id as usize).and_then(Option::as_ref) {
                let page_clone = page.clone(); // Clone Arc<RwLock<Page>>

                // Step 2: Update page access in the replacer
                {
                    let mut replacer = self.replacer.write(); // Acquire write lock
                    replacer.record_access(frame_id, AccessType::Lookup);
                    replacer.set_evictable(frame_id, false);
                }

                return Some(page_clone); // Return the page wrapped in an Arc<RwLock<Page>>
            }
        }

        warn!("Page {} not found in memory, loading from disk", page_id);

        // Step 3: Get a frame ID for the page (either from free list or by evicting another page)
        let frame_id = {
            let mut free_list = self.free_list.write(); // Acquire write lock
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let mut replacer = self.replacer.write(); // Acquire write lock
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            error!("Failed to fetch page {}: no available frame", page_id);
            return None;
        }

        // Step 4: Evict the old page if needed
        if let Some(old_page_id) = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table
                .iter()
                .find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);

            let mut pages = self.pages.write(); // Acquire write lock
            if let Some(page_arc) = pages[frame_id as usize].as_mut() {
                let mut page = page_arc.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        // Step 5: Load the new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        let new_page_arc = Arc::new(RwLock::new(new_page)); // Wrap the page in RwLock and Arc

        // Step 6: Insert the new page into the buffer pool
        {
            let mut pages = self.pages.write(); // Acquire write lock
            pages[frame_id as usize] = Some(new_page_arc.clone()); // Clone the Arc
        }

        let mut page_table = self.page_table.write(); // Acquire write lock
        page_table.insert(page_id, frame_id);
        debug!("Loaded page {} from disk into frame ID: {}", page_id, frame_id);

        // Step 7: Update the replacer with the new page
        {
            let mut replacer = self.replacer.write(); // Acquire write lock
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(new_page_arc) // Return the page wrapped in an Arc<RwLock<Page>>
    }

    /// Fetches a basic page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<BasicPageGuard>>`.
    pub fn fetch_page_basic(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<Arc<BasicPageGuard>> {
        debug!("Fetching basic page guard for page ID: {}", page_id);

        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            debug!("Page {} found in memory, frame ID: {}", page_id, frame_id);

            let pages = self.pages.read(); // Acquire read lock
            let page_arc = pages[frame_id as usize].as_ref()?.clone(); // Clone the Arc<RwLock<Page>>

            // Update page access in replacer
            let mut replacer = self.replacer.write(); // Acquire write lock
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(BasicPageGuard::new(
                Arc::clone(&self),
                page_arc, // Use the Arc<RwLock<Page>> directly
            )));
        }

        warn!("Page {} not found in memory, loading from disk", page_id);

        let frame_id = {
            let mut free_list = self.free_list.write(); // Acquire write lock
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let mut replacer = self.replacer.write(); // Acquire write lock
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            error!("Failed to fetch basic page guard for page {}: no available frame", page_id);
            return None;
        }

        if let Some(old_page_id) = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table
                .iter()
                .find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);

            let mut pages = self.pages.write(); // Acquire write lock
            if let Some(page_rwlock) = pages[frame_id as usize].as_mut() {
                let mut page = page_rwlock.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        let new_page_arc = Arc::new(RwLock::new(new_page));

        {
            let mut pages = self.pages.write(); // Acquire write lock
            pages[frame_id as usize] = Some(new_page_arc.clone());
        }

        let mut page_table = self.page_table.write(); // Acquire write lock
        page_table.insert(page_id, frame_id);
        debug!("Loaded page {} from disk into frame ID: {}", page_id, frame_id);

        {
            let mut replacer = self.replacer.write(); // Acquire write lock
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(Arc::new(BasicPageGuard::new(
            Arc::clone(&self),
            new_page_arc, // Pass the Arc<RwLock<Page>> here
        )))
    }

    /// Fetches a read page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<ReadPageGuard>>`.
    pub fn fetch_page_read(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<Arc<RwLock<ReadPageGuard>>> {
        debug!("Fetching read page guard for page ID: {}", page_id);

        // Check if the page is already in memory
        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            debug!("Page {} found in memory, frame ID: {}", page_id, frame_id);

            let pages = self.pages.read(); // Acquire read lock
            let page = pages[frame_id as usize].as_ref()?.clone(); // Clone Arc<RwLock<Page>>

            // Update page access in replacer
            let mut replacer = self.replacer.write(); // Acquire write lock
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(ReadPageGuard::new(
                Arc::clone(self),
                page,
            ))));
        }

        warn!("Page {} not found in memory, loading from disk", page_id);

        // Get a frame ID for the page
        let frame_id = {
            let mut free_list = self.free_list.write(); // Acquire write lock
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let mut replacer = self.replacer.write(); // Acquire write lock
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            error!("Failed to fetch read page guard for page {}: no available frame", page_id);
            return None;
        }

        // Evict the old page if needed
        if let Some(old_page_id) = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table
                .iter()
                .find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);

            let mut pages = self.pages.write(); // Acquire write lock
            if let Some(page) = pages[frame_id as usize].as_mut() {
                let mut page = page.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        // Load the new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        let new_page_arc = Arc::new(RwLock::new(new_page)); // Wrap the page in RwLock and Arc

        // Insert the new page into the buffer pool
        {
            let mut pages = self.pages.write(); // Acquire write lock
            pages[frame_id as usize] = Some(new_page_arc.clone()); // Clone the Arc
        }

        let mut page_table = self.page_table.write(); // Acquire write lock
        page_table.insert(page_id, frame_id);
        debug!("Loaded page {} from disk into frame ID: {}", page_id, frame_id);

        let mut replacer = self.replacer.write(); // Acquire write lock
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);

        Some(Arc::new(RwLock::new(ReadPageGuard::new(
            Arc::clone(self),
            new_page_arc,
        ))))
    }

    /// Fetches a write page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<WritePageGuard>>`.
    pub fn fetch_page_write(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<Arc<RwLock<WritePageGuard>>> {
        debug!("Fetching write page guard for page ID: {}", page_id);

        // Check if the page is already in memory
        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            debug!("Page {} found in memory, frame ID: {}", page_id, frame_id);

            let pages = self.pages.read(); // Acquire read lock
            let page = pages[frame_id as usize].as_ref()?.clone(); // Clone Arc<RwLock<Page>>

            // Update page access in replacer
            let mut replacer = self.replacer.write(); // Acquire write lock
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(WritePageGuard::new(
                Arc::clone(self),
                page,
            ))));
        }

        warn!("Page {} not found in memory, loading from disk", page_id);

        // Get a frame ID for the page
        let frame_id = {
            let mut free_list = self.free_list.write(); // Acquire write lock
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let mut replacer = self.replacer.write(); // Acquire write lock
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            error!("Failed to fetch write page guard for page {}: no available frame", page_id);
            return None;
        }

        // Evict the old page if needed
        if let Some(old_page_id) = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table
                .iter()
                .find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);

            let mut pages = self.pages.write(); // Acquire write lock
            if let Some(page) = pages[frame_id as usize].as_mut() {
                let mut page = page.write(); // Acquire write lock on the page
                if page.is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.set_dirty(false);
                }

            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        // Load the new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        let new_page_arc = Arc::new(RwLock::new(new_page)); // Wrap the page in RwLock and Arc

        // Insert the new page into the buffer pool
        {
            let mut pages = self.pages.write(); // Acquire write lock
            pages[frame_id as usize] = Some(new_page_arc.clone()); // Clone the Arc
        }

        let mut page_table = self.page_table.write(); // Acquire write lock
        page_table.insert(page_id, frame_id);
        debug!("Loaded page {} from disk into frame ID: {}", page_id, frame_id);

        let mut replacer = self.replacer.write(); // Acquire write lock
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);

        Some(Arc::new(RwLock::new(WritePageGuard::new(
            Arc::clone(self),
            new_page_arc,
        ))))
    }

    /// Unpins a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to unpin.
    /// - `is_dirty`: Whether the page is dirty.
    /// - `access_type`: The access type.
    ///
    /// # Returns
    /// `true` if the page was successfully unpinned, `false` otherwise.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        debug!("Unpinning page with ID: {}", page_id);

        // Acquire a read lock on the page table to get the frame ID
        let frame_id = {
            let page_table = self.page_table.read();
            if let Some(&frame_id) = page_table.get(&page_id) {
                frame_id
            } else {
                info!("Page {} is not in the page table", page_id);
                return false;
            }
        };

        // Acquire a write lock on the pages array to access the page
        let mut pages = self.pages.write();
        if let Some(page_arc) = pages.get_mut(frame_id as usize).and_then(Option::as_mut) {
            // Lock the RwLock to access the inner Page object
            let mut page = page_arc.write();
            info!(
            "Unpinning page {} with current pin count {}",
            page_id,
            page.get_pin_count()
        );

            // Decrement the pin count and update the page state if necessary
            if page.get_pin_count() > 0 {
                page.decrement_pin_count();

                // Mark as dirty if needed
                if is_dirty {
                    page.set_dirty(true);
                }

                // If pin count is 0, make the page evictable
                if page.get_pin_count() == 0 {
                    // Release the page lock before updating the replacer
                    drop(page);

                    // Acquire the write lock on the replacer to update the replacer
                    let mut replacer = self.replacer.write();
                    replacer.set_evictable(frame_id, true);
                    replacer.record_access(frame_id, access_type);

                    info!("Page {} is now evictable", page_id);
                } else {
                    info!("Page {} unpinned but still in use", page_id);
                }

                return true;
            } else {
                info!("Page {} has pin count 0, cannot unpin further", page_id);
            }
        } else {
            info!("Page {} was not found in the pages array", page_id);
        }

        false
    }

    /// Flushes a page with the given page ID to disk.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to flush.
    ///
    /// # Returns
    /// An optional `bool` indicating whether the page was successfully flushed.
    pub fn flush_page(&self, page_id: PageId) -> Option<bool> {
        // Acquire read lock on the page table
        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            info!("Flushing page {} from frame {}", page_id, frame_id); // Debugging statement

            // Acquire write lock on the pages
            let mut pages = self.pages.write(); // Acquire write lock
            if let Some(page) = pages[frame_id as usize].as_mut() {
                let mut page = page.write(); // Acquire write lock on the page

                if page.is_dirty() {
                    let data = page.get_data();
                    info!("Page data before flushing: {:?}", &data[..64]); // Debugging statement

                    // Perform the asynchronous disk write
                    self.disk_manager.write_page(page_id, &data);
                    info!("Page data written to disk: {:?}", &data[..64]); // Debugging statement

                    page.set_dirty(false); // Reset dirty flag after flushing
                    info!("Page {} flushed successfully", page_id); // Debugging statement
                    Some(true)
                } else {
                    info!("Page {} is not dirty, no need to flush", page_id); // Debugging statement
                    Some(false)
                }
            } else {
                info!("Failed to find page in frame {}", frame_id); // Debugging statement
                None
            }
        } else {
            info!("Page ID {} not found in page table", page_id); // Debugging statement
            None
        }
    }

    /// Flushes all pages in the buffer pool to disk.
    pub fn flush_all_pages(&self) {
        info!("Flushing all pages in buffer pool...");

        // Acquire locks on page table and pages
        let page_table = self.page_table.read(); // Read lock on page table
        let pages = self.pages.read(); // Read lock on pages

        // Iterate over all pages in the page table
        for (&page_id, &frame_id) in page_table.iter() {
            if let Some(page) = &pages[frame_id as usize] {
                // Lock the page to check and modify its state
                let mut page = page.write(); // Write lock on the page

                if page.is_dirty() {
                    let data = page.get_data().clone(); // Clone the data for writing

                    // Write the page data to disk asynchronously
                    self.disk_manager.write_page(page_id, &data);

                    page.set_dirty(false); // Reset the dirty flag after successful write
                    debug!("Flushed page {} from frame {}", page_id, frame_id);
                } else {
                    debug!("Page {} is not dirty, no need to flush", page_id);
                }
            } else {
                debug!("Page with frame ID {} not found in pages", frame_id);
            }
        }
    }

    /// Deletes a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to delete.
    ///
    /// # Returns
    /// `true` if the page was successfully deleted, `false` otherwise.
    pub fn delete_page(&self, page_id: PageId) -> bool {
        debug!("Deleting page with ID: {}", page_id);

        // Acquire a write lock on the page table
        let mut page_table = self.page_table.write();

        // Check if the page ID is in the page table
        if let Some(&frame_id) = page_table.get(&page_id) {
            // Release the write lock on the page table
            drop(page_table);

            // Acquire a write lock on the pages array
            let mut pages = self.pages.write();

            // Check if the page is in the pages array
            if let Some(page_arc) = pages.get_mut(frame_id as usize).and_then(Option::as_mut) {
                // Lock the RwLock to access the inner Page object
                let mut page = page_arc.write();

                // Reset the page memory
                page.reset_memory();

                // Remove the frame ID from the replacer
                let mut replacer = self.replacer.write();
                replacer.remove(frame_id);

                // Add the frame ID to the free list
                self.free_list.write().push(frame_id);

                debug!("Page {} deleted from frame {}", page_id, frame_id);
                return true;
            }
        }

        false
    }

    /// Allocates a new page ID.
    ///
    /// # Returns
    /// A new page ID.
    pub fn allocate_page(&self) -> PageId {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst).into();
        debug!("Allocated new page with ID: {}", page_id);
        page_id
    }

    /// Deallocates a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to deallocate.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        debug!("Deallocating page with ID: {}", page_id);

        // Acquire a write lock on the page table
        let frame_id = {
            let mut page_table = self.page_table.write();

            // Remove the page ID from the page table and get the frame ID
            match page_table.remove(&page_id) {
                Some(frame_id) => frame_id,
                None => {
                    // Page ID not found in page table
                    info!("Page ID {} not found in page table", page_id);
                    return;
                }
            }
        };

        // Acquire a write lock on the pages array
        let mut pages = self.pages.write();

        // Access the specific page slot
        if let Some(page_slot) = pages.get_mut(frame_id as usize) {
            // Take the page_arc out of the slot
            let page_arc = page_slot.take(); // This will set page_slot to None and return the page_arc

            if let Some(page_arc) = page_arc {
                // Lock the RwLock to access the inner Page object
                let mut page = page_arc.write();

                // Reset the page memory
                page.reset_memory();
            }
        }

        // Remove the frame ID from the replacer
        {
            let mut replacer = self.replacer.write();
            replacer.remove(frame_id);
        }

        // Add the frame ID to the free list
        self.free_list.write().push(frame_id);

        debug!("Page ID {} deallocated", page_id);
    }
}
