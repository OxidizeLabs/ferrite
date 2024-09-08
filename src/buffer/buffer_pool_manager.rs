use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId, DB_PAGE_SIZE};
use crate::common::exception::DeletePageError;
use crate::common::logger::initialize_logger;
use crate::storage::disk::disk_manager::{DiskIO, FileDiskManager};
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::index::b_plus_tree_index::KeyComparator;
use crate::storage::index::generic_key::{GenericKey, GenericKeyComparator};
use crate::storage::page::page::PageType::{ExtendedHashTableBucket, ExtendedHashTableDirectory, ExtendedHashTableHeader, Table};
use crate::storage::page::page::{Page, PageTrait, PageType};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::{ExtendableHTableBucketPage, TypeErasedBucketPage};
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::storage::page::page_types::table_page::TablePage;
use crate::types_db::integer_type::IntegerType;
use chrono::Utc;
use futures::AsyncWriteExt;
use log::{debug, error, info, warn};
use spin::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

const INVALID_PAGE_ID: PageId = -1;

// Define an enum to represent the type of page to create
#[derive(Debug)]
pub enum NewPageType {
    Basic,
    Table,
    ExtendedHashTableDirectory,
    ExtendedHashTableHeader,
    ExtendedHashTableBucket,
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
    pub fn new_page_guarded(self: &Arc<Self>, new_page_type: NewPageType) -> Option<PageGuard> {
        debug!("Creating new page of type: {:?}", new_page_type);

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
    ) -> Option<PageGuard> {
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

            return Some(PageGuard::new(Arc::clone(&self), page_arc, page_id));
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

        Some(PageGuard::new(
            Arc::clone(&self),
            new_page_arc,
            page_id,
        ))
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
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    fn create_page_of_type(&self, new_page_type: NewPageType, page_id: PageId) -> Arc<RwLock<PageType>> {
        let new_page = match new_page_type {
            NewPageType::Basic => PageType::Basic(Page::new(page_id)),
            NewPageType::ExtendedHashTableDirectory => ExtendedHashTableDirectory(ExtendableHTableDirectoryPage::new(page_id)),
            NewPageType::ExtendedHashTableHeader => ExtendedHashTableHeader(ExtendableHTableHeaderPage::new(page_id)),
            NewPageType::ExtendedHashTableBucket => {
                let bucket_page = ExtendableHTableBucketPage::<i32, 8>::new(page_id);
                ExtendedHashTableBucket(TypeErasedBucketPage::new(bucket_page))
            }
            NewPageType::Table => Table(TablePage::new(page_id))
        };

        Arc::new(RwLock::new(new_page))
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

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
    buffer_pool_size: usize,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 10;
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
            buffer_pool_size: BUFFER_POOL_SIZE,
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.db_file);
        let _ = std::fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log_file.clone();
        thread::spawn(move || {
            let _ = std::fs::remove_file(db_file);
            let _ = std::fs::remove_file(db_log);
        })
            .join()
            .unwrap();
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::logger::initialize_logger;
    use chrono::Utc;
    use log::{error, info};
    use rand::Rng;
    use spin::RwLock;
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn buffer_pool_manager_initialization() {
        let context = TestContext::new("test_buffer_pool_manager_initialization");

        // Check that the buffer pool size is correct
        assert_eq!(
            context.bpm.get_pool_size(),
            context.buffer_pool_size,
            "The buffer pool size should match the initialized value."
        );

        // Check that the buffer pool is empty initially
        assert_eq!(
            context.bpm.get_page_table().read().len(),
            0,
            "The page table should be empty on initialization."
        );
    }

    #[test]
    fn new_page_creation() {
        let context = TestContext::new("test_new_page_creation");

        // Create a new page

        let new_page = context.bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = new_page.read().as_page_trait().get_page_id();

        assert!(page_id >= 0, "The new page should have a valid page ID.");
        assert_eq!(
            new_page.read().as_page_trait().get_pin_count(),
            1,
            "The new page should have a pin count of 1."
        );

        // Ensure the page is added to the page table
        assert_eq!(
            context.bpm.get_page_table().read().len(),
            1,
            "The page table should contain one page."
        );
    }

    #[test]
    fn page_replacement_with_eviction() {
        let context = TestContext::new("test_page_replacement_with_eviction");

        // Fill the buffer pool to capacity
        for i in 0..context.buffer_pool_size {
            let page = context.bpm.new_page(NewPageType::Basic).unwrap();
            info!("Created page with ID: {}", page.read().as_page_trait().get_page_id());

            // Explicitly mark the newly created page as evictable
            let page_id = page.read().as_page_trait().get_page_id();
            context.bpm.unpin_page(page_id, false, AccessType::Unknown);
        }

        // The next new page should trigger an eviction
        info!("Creating an additional page to trigger eviction...");
        let extra_page = context.bpm.new_page(NewPageType::Basic);

        if extra_page.is_none() {
            error!("Failed to create a new page after buffer pool is full. Eviction did not work as expected.");
        }

        assert!(
            extra_page.is_some(),
            "There should be a page available after eviction."
        );

        // Verify that one page was evicted and replaced
        let page_count = context.bpm.get_page_table().read().len();
        assert_eq!(
            page_count,
            context.buffer_pool_size,
            "The buffer pool should still have the same number of pages after eviction."
        );
    }

    #[test]
    fn unpin_and_evict_page() {
        let context = TestContext::new("test_unpin_and_evict_page");

        // Create a new page
        let page = context.bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();

        // Unpin the page
        let success = context.bpm.unpin_page(page_id, false, AccessType::Lookup);
        assert!(success, "Unpinning should be successful.");

        // Now the page should be evictable
        let evicted = context.bpm.new_page(NewPageType::Basic).is_some();
        assert!(
            evicted,
            "The buffer pool manager should be able to evict an unpinned page."
        );
    }

    #[test]
    fn page_fetching_from_disk() {
        let context = TestContext::new("test_page_fetching_from_disk");

        // Create and write to a page
        let page = context.bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();
        let mut data = [0u8; DB_PAGE_SIZE];
        rand::thread_rng().fill(&mut data[..]);
        context.bpm.write_page(page_id, data);

        // Unpin and evict the page
        context.bpm.unpin_page(page_id, true, AccessType::Lookup);
        context.bpm.new_page(NewPageType::Basic); // Trigger eviction

        // Fetch the page back from disk
        let fetched_page = context.bpm.fetch_page(page_id).unwrap();
        assert_eq!(
            fetched_page.read().as_page_trait().get_page_id(),
            page_id,
            "The fetched page should have the same ID as the original page."
        );
        assert_eq!(
            &fetched_page.read().as_page_trait().get_data()[..],
            &data[..],
            "The fetched page data should match the written data."
        );
    }

    #[test]
    fn delete_page() {
        let context = TestContext::new("test_delete_page");

        // Create a new page
        let page = context.bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();

        // Delete the page
        match context.bpm.delete_page(page_id) {
            Ok(()) => println!("Page deleted successfully"),
            Err(e) => match e {
                DeletePageError::PageNotFound(pid) => println!("Page {} not found", pid),
                DeletePageError::FrameNotFound(fid) => println!("Frame {} not found", fid),
                _ => {}
            },
        }

        // Verify that the page is no longer in the page table
        let binding = context.bpm.get_page_table();
        let page_table = binding.read();
        assert!(
            !page_table.contains_key(&page_id),
            "The deleted page should not be in the page table."
        );
    }

    #[test]
    fn flush_page() {
        let context = TestContext::new("test_flush_page");

        // Create and write to a page
        let page = context.bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page.read().as_page_trait().get_page_id();
        let mut data = [0u8; DB_PAGE_SIZE];
        rand::thread_rng().fill(&mut data[..]);
        context.bpm.write_page(page_id, data);

        // Flush the page
        let flushed = context.bpm.flush_page(page_id);
        assert!(
            flushed.unwrap_or(false),
            "The page should be successfully flushed to disk."
        );

        // Unpin and evict the page
        context.bpm.unpin_page(page_id, true, AccessType::Lookup);
        context.bpm.new_page(NewPageType::Basic); // Trigger eviction

        // Fetch the page back from disk and verify the data
        let fetched_page = context.bpm.fetch_page(page_id).unwrap();
        assert_eq!(
            &fetched_page.read().as_page_trait().get_data()[..],
            &data[..],
            "The fetched page data should match the written and flushed data."
        );

        context.cleanup();
    }
}

#[cfg(test)]
mod basic_behaviour {}

#[cfg(test)]
mod concurrency {
    use super::*;
    use rand::Rng;

    #[test]
    fn concurrent_page_access() {
        let ctx = TestContext::new("concurrent_page_access_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));
        let page = bpm.write().new_page(NewPageType::Basic).unwrap();

        let page_id = page.read().as_page_trait().get_page_id();
        let bpm_clone = bpm.clone();
        let mut handles = vec![];

        // Spawn multiple threads to simulate concurrent access to the same page
        for i in 0..10 {
            let bpm = bpm_clone.clone();
            let page_id = page_id;

            let handle = thread::spawn(move || {
                {
                    // First, fetch the page with a short-lived lock on the BPM
                    let page = {
                        let bpm = bpm.read(); // Use a read lock on the BPM
                        bpm.fetch_page(page_id).unwrap() // Fetch the page
                    };

                    // Then, modify the page with a write lock on the page itself
                    {
                        let mut page_guard = page.write(); // Acquire write lock on the Page
                        let mut data = page_guard.as_page_trait_mut().get_data_mut();
                        data[0] = i as u8; // Each thread writes its index as the first byte
                    }

                    // Finally, unpin the page
                    let mut bpm = bpm.write();
                    bpm.unpin_page(page_id, true, AccessType::Lookup); // Mark the page as dirty
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread failed");
        }

        // Verify the final state of the page
        {
            let bpm = bpm.read();
            let final_page = bpm.fetch_page(page_id).unwrap();
            let binding = final_page.read();
            let final_data = binding.as_page_trait().get_data();
            assert!(
                final_data[0] < 10,
                "Final modification resulted in incorrect data: expected value < 10, got {}",
                final_data[0]
            );
        }

        // Clean up by unpinning the page
        bpm.write().unpin_page(page_id, false, AccessType::Lookup);
    }

    #[test]
    fn write_page_deadlock() {
        // Initialize the test context
        let ctx = TestContext::new("test_write_page_deadlock");
        let bpm = Arc::clone(&ctx.bpm);

        // Generate random data for writing
        let mut rng = rand::thread_rng();
        let data: [u8; DB_PAGE_SIZE] = rng.gen();

        // Mutex for synchronizing test completion
        let done = Arc::new(Mutex::new(false));
        let done_clone = Arc::clone(&done);

        // Create a list to hold thread handles
        let mut handles = vec![];

        // Spawn threads to perform concurrent write operations
        for i in 0..10 {
            let bpm_clone = Arc::clone(&bpm);
            let done_clone = Arc::clone(&done);

            let handle = thread::spawn(move || {
                // Create a new page
                let page_id = i as u32;
                bpm_clone.new_page(NewPageType::Basic).expect("Failed to create a new page");

                // Write data to the page
                bpm_clone.write_page(page_id as PageId, data);

                // Signal completion
                let mut done = done_clone.lock();
                *done = true;
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread failed");
        }

        // Check if the test finished without deadlocks
        let done = done.lock();
        assert!(*done, "Test did not complete successfully, possible deadlock detected");

        // Cleanup the test context
        ctx.cleanup();
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn eviction_policy() {
        let ctx = TestContext::new("eviction_policy_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Fill the buffer pool completely
        let mut last_page_id = 0;
        for i in 0..ctx.buffer_pool_size {
            // Create a new page within a narrow lock scope
            let page_id = {
                let mut bpm = bpm.write(); // Acquire write lock
                let page = bpm.new_page(NewPageType::Basic);

                let page_id = page.unwrap().read().as_page_trait().get_page_id();
                last_page_id = page_id;
                page_id
            }; // Release write lock immediately

            // Unpin the page in a separate lock scope
            bpm.write().unpin_page(page_id, false, AccessType::Lookup);
        }

        // Access the first page to make it recently used
        info!("Accessing page 0 to mark it as recently used");
        let page_0 = bpm.write().fetch_page(0);
        bpm.write().unpin_page(0, true, AccessType::Lookup);

        // Create a new page, forcing an eviction
        info!("Creating a new page to trigger eviction");
        let new_page = bpm.write().new_page(NewPageType::Basic);

        // Verify that the last page was evicted
        info!("Verify that the last page was evicted");
        {
            let bpm = bpm.write();
            let page_table = bpm.get_page_table();
            let page_table = page_table.read();
            assert!(
                !page_table.contains_key(&(1 as PageId)),
                "Last page should have been evicted: {}", &(1 as PageId)
            );
        }
    }

    #[test]
    fn repeated_fetch_and_modify() {
        let ctx = TestContext::new("repeated_fetch_and_modify_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Create and modify a page repeatedly
        let page = bpm.write().new_page(NewPageType::Basic).expect("Failed to create a new page");
        let page_id = page.read().as_page_trait().get_page_id(); // Store page_id for later use

        for i in 0..100 {
            {
                // Fetch the page with a short-lived lock on the BufferPoolManager
                let page = {
                    let bpm = bpm.read(); // Use a read lock on the BPM to fetch the page
                    bpm.fetch_page(page_id).expect("Failed to fetch page")
                };

                // Modify the page with a write lock on the page itself
                {
                    let mut page_guard = page.write(); // Acquire write lock on the page
                    let mut data = [0; DB_PAGE_SIZE];
                    data[0] = i as u8; // Modify the data
                    if let Err(e) = page_guard.as_page_trait_mut().set_data(0, &data) {
                        panic!("Error setting data: {:?}", e);
                    }
                }

                // Unpin the page
                let mut bpm = bpm.write(); // Acquire write lock to unpin the page
                bpm.unpin_page(page_id, true, AccessType::Lookup); // Mark the page as dirty
            } // The locks are dropped here to prevent deadlocks
        }

        // Verify the final modification
        {
            let bpm = bpm.read(); // Acquire read lock on BPM
            if let Some(page_guard) = bpm.fetch_page(page_id) {
                let page_guard = page_guard.read(); // Acquire read lock on the page
                let data = page_guard.as_page_trait().get_data(); // Get a reference to the data
                assert_eq!(data[0], 99, "Final modification did not persist");
            } else {
                panic!("Failed to fetch page");
            }

            bpm.unpin_page(page_id, false, AccessType::Lookup); // Clean up by unpinning the page
        }
    }

    #[test]
    fn boundary_conditions() {
        let ctx = TestContext::new("boundary_conditions_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Create maximum number of pages
        for _ in 0..ctx.buffer_pool_size {
            bpm.write().new_page(NewPageType::Basic).expect("Failed to create a new page");
        }

        // Attempt to create more pages than the buffer pool can handle
        for _ in 0..10 {
            let new_page_result = bpm.write().new_page(NewPageType::Basic);
            if let Some(ref page) = new_page_result {
                bpm.write().unpin_page(page.read().as_page_trait().get_page_id(), false, AccessType::Lookup);
                info!("Unexpectedly created page {}", page.read().as_page_trait().get_page_id());
            }
            assert!(new_page_result.is_none(), "Should not be able to create more pages than the buffer pool size");
        }
    }
}
