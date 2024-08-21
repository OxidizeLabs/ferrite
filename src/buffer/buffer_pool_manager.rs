use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId, DB_PAGE_SIZE};
use crate::common::exception::DeletePageError;
use crate::storage::disk::disk_manager::{DiskIO, FileDiskManager};
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::page::page::{Page, PageTrait, PageType};
use futures::AsyncWriteExt;
use log::{debug, error, info, warn};
use spin::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;

const INVALID_PAGE_ID: PageId = -1;

// Define an enum to represent the type of page to create
#[derive(Debug)]
pub enum NewPageType {
    Basic,
    ExtendedHashTableDirectory,
    // Add other page types as needed
}

/// The `BufferPoolManager` is responsible for managing the buffer pool,
/// including fetching and unpinning pages, and handling page replacement.
pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicI32,
    pages: Arc<RwLock<Vec<Option<Arc<RwLock<PageType>>>>>>,
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
    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Arc<RwLock<PageType>>>>>> {
        self.pages.clone()
    }

    pub fn get_page_table(&self) -> Arc<RwLock<HashMap<PageId, FrameId>>> {
        self.page_table.clone()
    }

    /// Creates a new page and returns it.
    ///
    /// # Returns
    /// An optional new page.
    pub fn new_page(&self, new_page_type: NewPageType) -> Option<Arc<RwLock<PageType>>> {
        debug!("Attempting to create a new page of type: {:?}", new_page_type);

        // Step 1: Acquire a frame ID
        let frame_id = self.get_available_frame()?;
        debug!("Selected frame ID: {}", frame_id);

        // Step 2: Evict old page if necessary
        self.evict_page_if_necessary(frame_id);

        // Step 3: Create a new page
        let new_page_id = self.allocate_page_id();
        let new_page = self.create_page_of_type(new_page_type, new_page_id);
        debug!("Created new page with ID: {}", new_page_id);

        // Step 4: Update internal data structures
        self.update_page_metadata(frame_id, new_page_id, &new_page);

        Some(new_page)
    }

    /// Creates a new page guard and returns it.
    ///
    /// # Returns
    /// An optional new `BasicPageGuard`.
    pub fn new_page_guarded(self: &Arc<BufferPoolManager>) -> Option<PageGuard> {
        debug!("Creating a new page guard...");

        // Step 1: Acquire the frame ID without holding locks for too long
        let frame_id = {
            let mut free_list = self.free_list.write();  // Lock free_list
            if let Some(id) = free_list.pop() {
                id
            } else {
                // No frames available in the free list; need to evict one.
                let mut replacer = self.replacer.write();  // Lock replacer
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            warn!("Failed to create a new page guard: no available frame");
            return None;
        }

        debug!("Selected frame ID: {}", frame_id);

        // Step 2: Evict old page if necessary before locking `pages`
        if let Some(old_page_id) = {
            let page_table = self.page_table.read();  // Acquire page_table read lock
            page_table.iter().find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
        } {
            debug!("Evicting old page with ID: {}", old_page_id);

            // Step 2a: Evict the old page without holding the other locks
            self.evict_old_page(frame_id, old_page_id);
        }

        // Step 3: Create the new page
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let new_page = Arc::new(RwLock::new(PageType::Basic(Page::new(new_page_id))));
        debug!("Created new page with ID: {}", new_page_id);

        // Step 4: Insert the new page into `pages` and `page_table`
        {
            let mut pages = self.pages.write(); // Acquire lock on pages
            pages[frame_id as usize] = Some(Arc::clone(&new_page));
        }

        {
            let mut page_table = self.page_table.write(); // Acquire lock on page_table
            page_table.insert(new_page_id as PageId, frame_id);
        }

        // Step 5: Update the replacer
        {
            let mut replacer = self.replacer.write();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        // Step 6: Create and return the page guard without holding any locks
        Some(PageGuard::new(Arc::clone(self), new_page, new_page_id))
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
                if page_data.as_page_trait().get_page_id() == page_id {
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
            page_data.as_page_trait_mut().set_data(0, &data);
            page_data.as_page_trait_mut().set_dirty(true);

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
    pub fn fetch_page(&self, page_id: PageId) -> Option<Arc<RwLock<PageType>>> {
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
                if page.as_page_trait_mut().is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.as_page_trait().get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.as_page_trait_mut().set_dirty(false);
                }
            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        // Step 5: Load the new page from disk
        let mut new_page = PageType::Basic(Page::new(page_id));
        self.disk_manager.read_page(page_id, &mut new_page.as_page_trait_mut().get_data_mut()).expect("Failed to read page");

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
    pub fn fetch_page_guarded(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<Arc<PageGuard>> {
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

            return Some(Arc::new(PageGuard::new(Arc::clone(&self), page_arc, page_id)));
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
                if page.as_page_trait_mut().is_dirty() {
                    info!("Page {} is dirty, scheduling a write-back", old_page_id);

                    // Create a synchronous channel for communication
                    let (tx, rx) = mpsc::channel();

                    let mut ds = self.disk_scheduler.write(); // Acquire write lock for the disk scheduler

                    // Schedule the write-back operation synchronously
                    ds.schedule(
                        true, // Write operation
                        Arc::new(RwLock::new(page.as_page_trait().get_data().clone())), // Data to write
                        old_page_id, // Page ID
                        tx, // Sender for the completion signal
                    );

                    // Wait for the write-back operation to complete
                    rx.recv().expect("Failed to complete the write-back operation");

                    // Mark the page as not dirty
                    page.as_page_trait_mut().set_dirty(false);
                }
            }

            let mut page_table = self.page_table.write(); // Acquire write lock
            page_table.remove(&old_page_id);
        }

        let mut new_page = PageType::Basic(Page::new(page_id));
        self.disk_manager.read_page(page_id, &mut new_page.as_page_trait_mut().get_data_mut()).expect("Failed to read page");

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

        Some(Arc::new(PageGuard::new(
            Arc::clone(&self),
            new_page_arc,
            page_id
        )))
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

        // Step 1: Determine the frame ID without holding any lock too long
        let frame_id = {
            let page_table = self.page_table.read();
            if let Some(&frame_id) = page_table.get(&page_id) {
                frame_id
            } else {
                info!("Page {} is not in the page table", page_id);
                return false;
            }
        };

        debug!("Step 2: Update the page's pin count and state");
        // Step 2: Update the page's pin count and state
        let should_make_evictable = {
            debug!("Step 2a: Acquire pages list write lock");
            let mut pages = self.pages.write();

            debug!("Step 2c: Get mutable reference to page in pages list");
            if let Some(page_arc) = pages.get_mut(frame_id as usize).and_then(Option::as_mut) {
                // Temporarily release the `pages` lock after getting the page reference.
                let page_arc = Arc::clone(page_arc);
                drop(pages); // Drop the `pages` lock before acquiring the page lock

                debug!("Step 2ci: Acquire page write lock");
                let mut page = page_arc.write();

                info!(
            "Unpinning page {} with current pin count {}",
            page_id,
            page.as_page_trait().get_pin_count()
        );

                if page.as_page_trait().get_pin_count() > 0 {
                    page.as_page_trait_mut().decrement_pin_count();

                    // Mark as dirty if needed
                    if is_dirty {
                        page.as_page_trait_mut().set_dirty(true);
                    }

                    // If the pin count reaches 0, the page should become evictable
                    if page.as_page_trait().get_pin_count() == 0 {
                        info!("Page {} is now evictable", page_id);
                        true
                    } else {
                        info!("Page {} unpinned but still in use", page_id);
                        false
                    }
                } else {
                    info!("Page {} has pin count 0, cannot unpin further", page_id);
                    return false;
                }
            } else {
                info!("Page {} was not found in the pages array", page_id);
                return false;
            }
        };


        debug!("Step 3: Make the page evictable if needed, outside of the main locks");
        // Step 3: Make the page evictable if needed, outside of the main locks
        if should_make_evictable {
            let mut replacer = self.replacer.write();
            replacer.set_evictable(frame_id, true);
            replacer.record_access(frame_id, access_type);
        }

        true
    }

    /// Flushes a page with the given page ID to disk.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to flush.
    ///
    /// # Returns
    /// An optional `bool` indicating whether the page was successfully flushed.
    pub fn flush_page(&self, page_id: PageId) -> Option<bool> {
        // Step 1: Determine the frame ID associated with the page ID
        let frame_id = {
            let page_table = self.page_table.read(); // Acquire read lock on the page table
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            info!("Flushing page {} from frame {}", page_id, frame_id);

            // Step 2: Get a clone of the page Arc to release the pages lock early
            let page_to_flush = {
                let pages = self.pages.read(); // Acquire read lock on pages
                pages[frame_id as usize].as_ref().cloned() // Clone the Arc to avoid holding the pages lock during I/O
            };

            if let Some(page_arc) = page_to_flush {
                // Step 3: Perform the disk I/O operation outside of critical sections
                let mut flush_successful = false;
                {
                    let mut page = page_arc.write(); // Acquire write lock on the page

                    if page.as_page_trait_mut().is_dirty() {
                        let data = page.as_page_trait().get_data();
                        info!("Page data before flushing: {:?}", &data[..64]);

                        // Perform the disk write
                        self.disk_manager.write_page(page_id, &data).expect("Failed to write page");

                        info!("Page data written to disk: {:?}", &data[..64]);

                        page.as_page_trait_mut().set_dirty(false); // Reset dirty flag after flushing
                        flush_successful = true;
                    } else {
                        info!("Page {} is not dirty, no need to flush", page_id);
                    }
                } // Release the page lock after the I/O operation is complete

                if flush_successful {
                    info!("Page {} flushed successfully", page_id);
                    Some(true)
                } else {
                    Some(false)
                }
            } else {
                info!("Failed to find page in frame {}", frame_id);
                None
            }
        } else {
            info!("Page ID {} not found in page table", page_id);
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

                if page.as_page_trait_mut().is_dirty() {
                    let data = page.as_page_trait().get_data().clone(); // Clone the data for writing

                    // Write the page data to disk asynchronously
                    self.disk_manager.write_page(page_id, &data).expect("Failed to write page");

                    page.as_page_trait_mut().set_dirty(false); // Reset the dirty flag after successful write
                    debug!("Flushed page {} from frame {}", page_id, frame_id);
                } else {
                    debug!("Page {} is not dirty, no need to flush", page_id);
                }
            } else {
                debug!("Page with frame ID {} not found in pages", frame_id);
            }
        }
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
                page.as_page_trait_mut().reset_memory();
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

    /// Deletes a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to delete.
    ///
    /// # Returns
    /// `true` if the page was successfully deleted, `false` otherwise.
    pub fn delete_page(&self, page_id: PageId) -> Result<(), DeletePageError> {
        debug!("Attempting to delete page with ID: {}", page_id);

        let frame_id = self.get_frame_id(page_id)?;
        self.remove_page_from_table(page_id);
        self.reset_frame(frame_id)?;

        debug!("Successfully deleted page {} from frame {}", page_id, frame_id);
        Ok(())
    }

    fn get_frame_id(&self, page_id: PageId) -> Result<FrameId, DeletePageError> {
        let page_table_guard = self.page_table.read();
        page_table_guard
            .get(&page_id)
            .copied()
            .ok_or(DeletePageError::PageNotFound(page_id))
    }

    fn remove_page_from_table(&self, page_id: PageId) {
        let mut page_table = self.page_table.write();
        page_table.remove(&page_id);
    }

    fn reset_frame(&self, frame_id: FrameId) -> Result<(), DeletePageError> {
        let mut pages = self.pages.write();

        let page_slot = pages.get_mut(frame_id as usize)
            .ok_or(DeletePageError::FrameNotFound(frame_id))?;

        if let Some(page_arc) = page_slot.take() {
            // Reset the page memory
            let mut page = page_arc.write();
            page.as_page_trait_mut().reset_memory();
        }

        // Remove the frame ID from the replacer
        self.replacer.write().remove(frame_id);

        // Add the frame ID to the free list
        self.free_list.write().push(frame_id);

        Ok(())
    }

    fn evict_old_page(&self, frame_id: FrameId, old_page_id: PageId) {
        // Step 1: Acquire the necessary lock for the pages vector
        let page_to_evict = {
            let pages = self.pages.read(); // Lock `pages` just for reading
            pages[frame_id as usize].as_ref().cloned() // Clone the Arc<RwLock<Page>> so that we can drop the lock on `pages`
        };

        // Step 2: If there is a page to evict, handle it
        if let Some(page_rwlock) = page_to_evict {
            let mut page = page_rwlock.write(); // Acquire a write lock on the individual page
            if page.as_page_trait_mut().is_dirty() {
                info!("Page {} is dirty, scheduling a write-back", old_page_id);

                // Perform disk write operation outside the main locks
                self.flush_page(old_page_id); // Perform the flush without holding any locks

                // Re-acquire the write lock to modify the page's state after flushing
                let mut page = page_rwlock.write();
                page.as_page_trait_mut().set_dirty(false); // Mark the page as not dirty
            }
        }

        // Step 3: Remove the old page from `page_table`
        let mut page_table = self.page_table.write(); // Lock `page_table` only when modifying it
        page_table.remove(&old_page_id);
    }

    fn get_available_frame(&self) -> Option<FrameId> {
        let mut free_list = self.free_list.write();
        free_list.pop().or_else(|| {
            let mut replacer = self.replacer.write();
            replacer.evict()
        })
    }

    fn evict_page_if_necessary(&self, frame_id: FrameId) {
        if let Some(old_page_id) = self.get_page_id_for_frame(frame_id) {
            debug!("Evicting old page with ID: {}", old_page_id);
            self.evict_old_page(frame_id, old_page_id);
        }
    }

    fn get_page_id_for_frame(&self, frame_id: FrameId) -> Option<PageId> {
        let page_table = self.page_table.read();
        page_table.iter()
            .find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
    }

    fn allocate_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn create_page_of_type(&self, new_page_type: NewPageType, page_id: PageId) -> Arc<RwLock<PageType>> {
        match new_page_type {
            NewPageType::Basic => {
                Arc::new(RwLock::new(PageType::Basic(Page::new(page_id))))
            },
            NewPageType::ExtendedHashTableDirectory => {
                Arc::new(RwLock::new(PageType::ExtendedHashTableDirectory(ExtendableHTableDirectoryPage::new(page_id))))
            },
            // Add other page types as needed
        }
    }

    fn update_page_metadata(&self, frame_id: FrameId, page_id: PageId, new_page: &Arc<RwLock<PageType>>) {
        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
        }

        let mut replacer = self.replacer.write();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);
    }
}
