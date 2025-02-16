use crate::common::config::INVALID_PAGE_ID;
use crate::storage::page::page::{PageType, PAGE_TYPE_OFFSET};
use crate::storage::page::page::{Page, PageTrait};
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId, DB_PAGE_SIZE};
use crate::common::exception::DeletePageError;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::page::page_guard::{PageGuard};
use log::{error, info, trace, warn};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::fmt;
use std::time::Duration;
use std::error::Error;

/// The `BufferPoolManager` is responsible for managing the buffer pool,
/// including fetching and unpinning pages, and handling page replacement.
pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicU64,
    pages: Arc<RwLock<Vec<Option<Arc<RwLock<dyn PageTrait>>>>>>,
    page_table: Arc<RwLock<HashMap<PageId, FrameId>>>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    free_list: Arc<RwLock<Vec<FrameId>>>,
    disk_scheduler: Arc<RwLock<DiskScheduler>>,
    disk_manager: Arc<FileDiskManager>,
}

// Implement Debug manually
impl Debug for BufferPoolManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPoolManager")
            .field("pool_size", &self.pool_size)
            .field("next_page_id", &self.next_page_id)
            .field("page_table", &self.page_table)
            .field("replacer", &self.replacer)
            .field("free_list", &self.free_list)
            .field("disk_scheduler", &self.disk_scheduler)
            .field("disk_manager", &self.disk_manager)
            // Skip pages field as it contains the trait object
            .finish()
    }
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
        info!(
            "BufferPoolManager initialized with pool size: {}",
            pool_size
        );
        Self {
            pool_size,
            next_page_id: AtomicU64::new(0),
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
    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Arc<RwLock<dyn PageTrait>>>>>> {
        self.pages.clone()
    }

    pub fn get_page_table(&self) -> Arc<RwLock<HashMap<PageId, FrameId>>> {
        self.page_table.clone()
    }

    /// Creates a new page and returns it.
    ///
    /// # Returns
    /// An optional new page.
    pub fn new_page<T: Page>(&self) -> Option<PageGuard<T>> {
        trace!("Creating new page of type: {:?}", T::TYPE_ID);

        let frame_id = self.get_available_frame()?;
        trace!("Got available frame: {}", frame_id);
        
        self.evict_page_if_necessary(frame_id);

        let new_page_id = self.allocate_page_id();
        trace!("Allocated new page ID: {}", new_page_id);
        
        let new_page = self.create_typed_page::<T>(new_page_id);
        self.update_page_metadata(frame_id, new_page_id, &new_page);

        trace!("Created new page {} in frame {}", new_page_id, frame_id);
        Some(PageGuard::new_for_new_page(new_page, new_page_id))
    }

    /// Writes data to the page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to write to.
    /// - `data`: The data to write to the page.
    pub fn write_page(&self, page_id: PageId, data: [u8; DB_PAGE_SIZE as usize]) {
        trace!("Writing to page with ID: {}", page_id);

        // Acquire read lock on the page collection
        let pages = self.pages.read();

        // Find the target page
        let mut target_page = None;
        for (i, page_opt) in pages.iter().enumerate() {
            if let Some(page) = page_opt {
                let page_data = page.read(); // Acquire read lock on the page
                if page_data.get_page_id() == page_id {  // Access trait method directly
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

            // Update the page data - access trait methods directly
            let _ = page_data.set_data(0, &data);
            page_data.set_dirty(true);

            trace!("Page {} written and marked dirty", page_id);
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
    pub fn fetch_page<T: Page>(&self, page_id: PageId) -> Option<PageGuard<T>> {
        trace!("Fetching page {} of type {:?}", page_id, T::TYPE_ID);

        if page_id == INVALID_PAGE_ID || page_id >= self.next_page_id.load(Ordering::SeqCst) {
            trace!("Invalid page ID: {}", page_id);
            return None;
        }

        if let Some(page) = self.get_page_internal(page_id) {
            // Verify page type...
            let actual_type = {
                let page_read = page.read();
                let type_byte = page_read.get_data()[PAGE_TYPE_OFFSET];
                let page_type = PageType::from_u8(type_byte);
                trace!(
                    "Page {} in memory - Type byte: {}, Parsed type: {:?}, Expected: {:?}",
                    page_id,
                    type_byte,
                    page_type,
                    T::TYPE_ID
                );
                page_type
            };
            
            if actual_type != Some(T::TYPE_ID) {
                warn!("Page type mismatch: expected {:?}, found {:?}", T::TYPE_ID, actual_type);
                return None;
            }

            // Create new guard (which will increment pin count)
            let typed_page = unsafe {
                let raw = Arc::into_raw(page) as *const RwLock<T>;
                Arc::from_raw(raw)
            };

            trace!("Successfully fetched page {}", page_id);
            Some(PageGuard::new(typed_page, page_id))
        } else {
            trace!("Page {} not in buffer pool, reading from disk", page_id);
            // Page not in buffer pool, try to read from disk
            let frame_id = self.get_available_frame()?;
            self.evict_page_if_necessary(frame_id);

            // Create a new page first
            let mut page = T::new(page_id);
            page.get_data_mut()[PAGE_TYPE_OFFSET] = T::TYPE_ID.to_u8();

            // Try to read from disk
            let mut buffer = [0u8; DB_PAGE_SIZE as usize];
            match self.disk_manager.read_page(page_id, &mut buffer) {
                Ok(_) => {
                    // Preserve the page type but copy all other data
                    let page_type = T::TYPE_ID.to_u8();  // Use the expected type
                    page.get_data_mut().copy_from_slice(&buffer);
                    page.get_data_mut()[PAGE_TYPE_OFFSET] = page_type;
                }
                Err(e) => {
                    trace!("Failed to read page {} from disk: {}", page_id, e);
                    return None;
                }
            }

            let page = Arc::new(RwLock::new(page));
            self.update_page_metadata(frame_id, page_id, &page);

            trace!("Successfully fetched page {}", page_id);
            Some(PageGuard::new(page, page_id))
        }
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
        trace!("Unpinning page {} (dirty: {})", page_id, is_dirty);

        let frame_id = {
            let page_table = self.page_table.read();
            match page_table.get(&page_id) {
                Some(&frame_id) => frame_id,
                None => {
                    warn!("Page {} is not in the page table", page_id);
                    return false;
                }
            }
        };

        let should_make_evictable = {
            let pages = self.pages.read();
            if let Some(page) = &pages[frame_id as usize] {
                let mut page = page.write();
                if page.get_pin_count() > 0 {
                    page.decrement_pin_count();
                    if is_dirty {
                        page.set_dirty(true);
                    }
                    trace!("Page {} pin count now {}", page_id, page.get_pin_count());
                    page.get_pin_count() == 0
                } else {
                    false
                }
            } else {
                false
            }
        };

        if should_make_evictable {
            trace!("Making page {} evictable in frame {}", page_id, frame_id);
            let mut replacer = self.replacer.write();
            replacer.set_evictable(frame_id, true);
            replacer.record_access(frame_id, access_type);
        }

        true
    }

    /// Flushes a dirty page to disk
    pub fn flush_page(&self, page_id: PageId) -> Result<(), String> {
        trace!("Flushing dirty page {} to disk", page_id);
        
        if let Some(page) = self.get_page_internal(page_id) {
            let mut page_guard = page.write();
            if page_guard.is_dirty() {
                // Create a data buffer wrapped in Arc<RwLock>
                let data_buffer = {
                    let mut data = [0u8; DB_PAGE_SIZE as usize];
                    data.copy_from_slice(page_guard.get_data());
                    Arc::new(RwLock::new(data))
                };

                // Create a channel for synchronization
                let (tx, rx) = mpsc::channel();

                // Schedule the write operation
                self.disk_scheduler.write().schedule(
                    true,           // is_write
                    data_buffer,    // data
                    page_id,        // page_id
                    tx,             // tx
                );

                // Wait for the write to complete
                rx.recv().map_err(|e| format!("Failed to receive write completion: {}", e))?;

                // Clear dirty flag after successful write
                page_guard.set_dirty(false);
                trace!("Successfully flushed page {} to disk", page_id);
            }
            Ok(())
        } else {
            Err(format!("Page {} not found in buffer pool", page_id))
        }
    }

    /// Flushes all dirty pages to disk
    pub fn flush_all_pages(&self) {
        trace!("Flushing all dirty pages to disk");
        let pages = self.pages.read();
        
        for page_opt in pages.iter() {
            if let Some(page) = page_opt {
                let page_id = page.read().get_page_id();
                if page.read().is_dirty() {
                    trace!("Flushing dirty page {} to disk", page_id);
                    if let Err(e) = self.flush_page(page_id) {
                        error!("Failed to flush page {}: {}", page_id, e);
                    }
                }
            }
        }
        trace!("Completed flushing all dirty pages");
    }

    /// Allocates a new page ID.
    ///
    /// # Returns
    /// A new page ID.
    pub fn allocate_page(&self) -> PageId {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst).into();
        trace!("Allocated new page with ID: {}", page_id);
        page_id
    }

    /// Deallocates a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to deallocate.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        trace!("Deallocating page with ID: {}", page_id);

        // Acquire a write lock on the page table
        let frame_id = {
            let mut page_table = self.page_table.write();

            // Remove the page ID from the page table and get the frame ID
            match page_table.remove(&page_id) {
                Some(frame_id) => frame_id,
                None => {
                    // Page ID not found in page table
                    warn!("Page ID {} not found in page table", page_id);
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
            let replacer = self.replacer.write();
            replacer.remove(frame_id);
        }

        // Add the frame ID to the free list
        self.free_list.write().push(frame_id);

        trace!("Page ID {} deallocated", page_id);
    }

    /// Deletes a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to delete.
    ///
    /// # Returns
    /// `true` if the page was successfully deleted, `false` otherwise.
    pub fn delete_page(&self, page_id: PageId) -> Result<(), DeletePageError> {
        trace!("Attempting to delete page with ID: {}", page_id);

        let frame_id = self.get_frame_id(page_id)?;
        self.remove_page_from_table(page_id);
        self.reset_frame(frame_id)?;

        trace!(
            "Successfully deleted page {} from frame {}",
            page_id,
            frame_id
        );
        Ok(())
    }

    pub fn get_free_list_size(&self) -> usize {
        self.free_list.read().len()
    }

    pub fn get_replacer(&self) -> Option<RwLockReadGuard<LRUKReplacer>> {
        Some(self.replacer.read())
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

        let page_slot = pages
            .get_mut(frame_id as usize)
            .ok_or(DeletePageError::FrameNotFound(frame_id))?;

        if let Some(page_arc) = page_slot.take() {
            // Reset the page memory
            let mut page = page_arc.write();
            page.reset_memory();
        }

        // Remove the frame ID from the replacer
        self.replacer.write().remove(frame_id);

        // Add the frame ID to the free list
        self.free_list.write().push(frame_id);

        Ok(())
    }

    fn evict_page_if_necessary(&self, frame_id: FrameId) {
        if let Some(old_page_id) = self.get_page_id_for_frame(frame_id) {
            trace!("Evicting page {} from frame {}", old_page_id, frame_id);
            
            // Check if page can be evicted (pin count should be 0)
            let can_evict = {
                let pages = self.pages.read();
                if let Some(page) = &pages[frame_id as usize] {
                    let page_guard = page.read();
                    let pin_count = page_guard.get_pin_count();
                    trace!("Page {} has pin count {}", old_page_id, pin_count);
                    pin_count == 0
                } else {
                    true
                }
            };

            if !can_evict {
                warn!("Cannot evict page {} as it is still pinned", old_page_id);
                return;
            }

            // Make sure the page is marked as evictable in the replacer
            {
                let mut replacer = self.replacer.write();
                replacer.set_evictable(frame_id, true);
                trace!("Marked frame {} as evictable for page {}", frame_id, old_page_id);
            }

            self.evict_old_page(frame_id, old_page_id);
        }
    }

    fn evict_old_page(&self, frame_id: FrameId, old_page_id: PageId) {
        trace!("Starting eviction of page {} from frame {}", old_page_id, frame_id);
        
        // Get the page to evict
        let page_to_evict = {
            let mut pages = self.pages.write();
            trace!("Taking ownership of page {} from frame {}", old_page_id, frame_id);
            pages[frame_id as usize].take()
        };

        if let Some(page_arc) = page_to_evict {
            let (needs_writeback, pin_count) = {
                let page = page_arc.read();
                (page.is_dirty(), page.get_pin_count())
            };

            trace!("Page {} state before eviction: dirty={}, pin_count={}", 
                old_page_id, needs_writeback, pin_count);

            if needs_writeback {
                trace!("Writing dirty page {} to disk", old_page_id);
                if let Err(e) = self.write_page_to_disk(old_page_id, &page_arc) {
                    error!("Failed to write back dirty page {}: {}", old_page_id, e);
                }
            }
        }

        // Remove from page table
        {
            let mut page_table = self.page_table.write();
            trace!("Removing page {} from page table", old_page_id);
            page_table.remove(&old_page_id);
        }
        
        trace!("Completed eviction of page {} from frame {}", old_page_id, frame_id);
    }

    fn get_available_frame(&self) -> Option<FrameId> {
        // First try to get a frame from the free list
        if let Some(frame_id) = self.free_list.write().pop() {
            trace!("Got frame {} from free list", frame_id);
            return Some(frame_id);
        }

        trace!("No frames in free list, attempting eviction");
        let mut replacer = self.replacer.write();
        
        // Update evictable status for all frames
        {
            let pages = self.pages.read();
            for (frame_id, page_opt) in pages.iter().enumerate() {
                if let Some(page) = page_opt {
                    let pin_count = page.read().get_pin_count();
                    let page_id = page.read().get_page_id();
                    trace!("Frame {} (page {}) has pin count {}", frame_id, page_id, pin_count);
                    
                    let is_evictable = pin_count == 0;
                    replacer.set_evictable(frame_id as FrameId, is_evictable);
                    
                    if is_evictable {
                        trace!("Marked frame {} as evictable", frame_id);
                    }
                }
            }
        }
        
        // Try to evict a page
        while let Some(frame_id) = replacer.evict() {
            trace!("LRU replacer suggested frame {} for eviction", frame_id);
            
            // Double check if the frame can actually be evicted
            let can_evict = {
                let pages = self.pages.read();
                if let Some(page) = &pages[frame_id as usize] {
                    let pin_count = page.read().get_pin_count();
                    let page_id = page.read().get_page_id();
                    trace!("Verifying frame {} (page {}) pin count: {}", 
                        frame_id, page_id, pin_count);
                    pin_count == 0
                } else {
                    true // Empty frame can be evicted
                }
            };

            if can_evict {
                if let Some(page_id) = self.get_page_id_for_frame(frame_id) {
                    trace!("Evicting page {} from frame {}", page_id, frame_id);
                    self.evict_old_page(frame_id, page_id);
                }
                trace!("Frame {} is now available for reuse", frame_id);
                return Some(frame_id);
            } else {
                trace!("Cannot evict frame {} - page is still pinned", frame_id);
                replacer.set_evictable(frame_id, false);
            }
        }

        warn!("No evictable frames found in buffer pool");
        None
    }

    fn get_page_id_for_frame(&self, frame_id: FrameId) -> Option<PageId> {
        let page_table = self.page_table.read();
        page_table
            .iter()
            .find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
    }

    fn allocate_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    fn create_typed_page<T: Page>(&self, page_id: PageId) -> Arc<RwLock<T>> {
        let mut new_page = T::new(page_id);
        
        // Set the page type in the data
        new_page.get_data_mut()[PAGE_TYPE_OFFSET] = T::TYPE_ID.to_u8();
        
        // Verify the type byte was set correctly
        debug_assert_eq!(
            new_page.get_data()[PAGE_TYPE_OFFSET],
            T::TYPE_ID.to_u8(),
            "Page type not set correctly"
        );
        
        trace!("Creating new page {} - Initial type byte: {}, Type: {:?}", 
            page_id, T::TYPE_ID.to_u8(), T::TYPE_ID);
        
        Arc::new(RwLock::new(new_page))
    }

    fn read_typed_page_from_disk<T: Page>(&self, page_id: PageId) -> Option<Arc<RwLock<T>>> {
        let mut buffer = [0u8; DB_PAGE_SIZE as usize];

        trace!("Reading page {} from disk, expecting type {:?}", page_id, T::TYPE_ID);

        // Read the raw data from disk
        if let Err(e) = self.disk_manager.read_page(page_id, &mut buffer) {
            error!("Failed to read page {} from disk: {}", page_id, e);
            return None;
        }

        trace!(
            "Read data from disk - First bytes: {:?}, Type byte: {}",
            &buffer[..8],
            buffer[PAGE_TYPE_OFFSET]
        );

        // Create the page and copy the data
        let mut page = T::new(page_id);
        page.get_data_mut().copy_from_slice(&buffer);
        
        trace!(
            "After initial data copy - Type byte: {}, Expected: {}",
            page.get_data()[PAGE_TYPE_OFFSET],
            T::TYPE_ID.to_u8()
        );
        
        // Explicitly set the page type
        page.get_data_mut()[PAGE_TYPE_OFFSET] = T::TYPE_ID.to_u8();
        
        trace!(
            "After setting type - First bytes: {:?}, Type byte: {}",
            &page.get_data()[..8],
            page.get_data()[PAGE_TYPE_OFFSET]
        );
        
        // Verify the page type is set correctly
        debug_assert_eq!(
            PageType::from_u8(page.get_data()[PAGE_TYPE_OFFSET]),
            Some(T::TYPE_ID),
            "Page type not set correctly after creation"
        );
        
        Some(Arc::new(RwLock::new(page)))
    }

    fn update_page_metadata<P: PageTrait + 'static>(
        &self,
        frame_id: FrameId,
        page_id: PageId,
        new_page: &Arc<RwLock<P>>,
    ) {
        {
            let mut pages = self.pages.write();
            let page_trait: Arc<RwLock<dyn PageTrait>> = new_page.clone() as Arc<RwLock<dyn PageTrait>>;
            pages[frame_id as usize] = Some(page_trait);
        }

        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
        }

        let mut replacer = self.replacer.write();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, AccessType::Lookup);
    }

    /// Writes a page to disk
    fn write_page_to_disk(&self, page_id: PageId, page: &Arc<RwLock<dyn PageTrait>>) -> Result<(), String> {
        // Create a data buffer wrapped in Arc<RwLock>
        let data_buffer = {
            let page_guard = page.read();
            let mut data = [0u8; DB_PAGE_SIZE as usize];
            
            // Get the page type first
            let page_type = page_guard.get_page_type();
            let type_byte = page_type.to_u8();
            
            // Copy the data
            data.copy_from_slice(page_guard.get_data());
            
            // Ensure the type byte is preserved
            data[PAGE_TYPE_OFFSET] = type_byte;
            
            // Verify type byte
            debug_assert_eq!(data[PAGE_TYPE_OFFSET], type_byte,
                "Page type byte was corrupted during write");
            
            trace!("Writing page {} to disk. Type: {:?}, Type byte: {}, First bytes: {:?}", 
                page_id, page_type, data[PAGE_TYPE_OFFSET], &data[..8]);
            
            Arc::new(RwLock::new(data))
        };

        // Create a channel for synchronization
        let (tx, rx) = mpsc::channel();

        // Schedule the async write operation
        self.disk_scheduler.write().schedule(
            true,           // is_write
            data_buffer,    // data
            page_id,        // page_id
            tx,             // tx
        );

        // Wait for the write to complete
        rx.recv().map_err(|e| format!("Failed to receive write completion: {}", e))?;

        Ok(())
    }

    fn get_page_internal(&self, page_id: PageId) -> Option<Arc<RwLock<dyn PageTrait>>> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id)?;
        
        let pages = self.pages.read();
        pages[*frame_id as usize].clone()
    }
}

impl Clone for BufferPoolManager {
    fn clone(&self) -> Self {
        Self {
            pool_size: self.pool_size,
            next_page_id: AtomicU64::new(self.next_page_id.load(Ordering::SeqCst)),
            pages: Arc::clone(&self.pages),
            page_table: Arc::clone(&self.page_table),
            replacer: Arc::clone(&self.replacer),
            free_list: Arc::clone(&self.free_list),
            disk_scheduler: Arc::clone(&self.disk_scheduler),
            disk_manager: Arc::clone(&self.disk_manager),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::common::config::INVALID_PAGE_ID;
    use crate::storage::page::page::{BasicPage, PageTrait, PageType};
    use std::sync::Barrier;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 5;
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
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
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
    fn test_basic_page_operations() {
        let ctx = TestContext::new("basic_page_operations");
        let bpm = ctx.bpm;

        // Create a new page
        let page_guard = bpm.new_page::<BasicPage>()
            .expect("Failed to create new page");

        // Test read operations
        {
            let data = page_guard.read();
            assert_eq!(data.get_page_type(), PageType::Basic);
            assert_eq!(data.get_pin_count(), 1);
            assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        }

        // Test write operations (preserving page type)
        {
            let mut data = page_guard.write();
            let type_byte = data.get_data()[PAGE_TYPE_OFFSET];
            data.get_data_mut()[1] = 42; // Write to second byte, not the type byte
            data.get_data_mut()[PAGE_TYPE_OFFSET] = type_byte; // Ensure type is preserved
            assert_eq!(data.get_data()[1], 42);
            assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        }

        // Get page ID for later use
        let page_id = page_guard.read().get_page_id();

        // Verify type before dropping
        assert_eq!(page_guard.read().get_page_type(), PageType::Basic);
        assert_eq!(page_guard.read().get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());

        // Drop the guard to unpin the page
        drop(page_guard);

        // Fetch the page again
        let fetched_guard = bpm.fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify type and data after fetch
        let data = fetched_guard.read();
        assert_eq!(data.get_page_type(), PageType::Basic);
        assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        assert_eq!(data.get_data()[1], 42);
    }

    #[test]
    fn test_concurrent_access() {
        let ctx = TestContext::new("concurrent_access");
        let bpm = Arc::new(ctx.bpm);

        // Create a new page
        let page_guard = bpm.new_page::<BasicPage>()
            .expect("Failed to create new page");
        let page_id = page_guard.get_page_id();

        // Verify initial page type
        assert_eq!(page_guard.read().get_page_type(), PageType::Basic);
        
        let mut handles = vec![];
        let bpm_clone = Arc::clone(&bpm);

        // Use a barrier to ensure all threads start at the same time
        let thread_count = 3;
        let barrier = Arc::new(Barrier::new(thread_count));

        // Spawn multiple threads to access the same page
        for i in 0..thread_count {
            let bpm = Arc::clone(&bpm_clone);
            let barrier = Arc::clone(&barrier);
            
            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                let page = bpm.fetch_page::<BasicPage>(page_id)
                    .expect("Failed to fetch page");
                
                // Write to a unique offset for each thread (avoiding page type byte)
                let offset = i + 1; // Start at offset 1 to avoid page type byte
                {
                    let mut data = page.write();
                    data.get_data_mut()[offset] = (i + 1) as u8;
                    data.set_dirty(true);
                    
                    // Verify page type wasn't corrupted
                    assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8(),
                        "Page type was corrupted");
                }

                // Small delay to increase chance of concurrent access
                thread::sleep(Duration::from_millis(10));

                // Read back and verify our write
                {
                    let data = page.read();
                    assert_eq!(data.get_data()[offset], (i + 1) as u8,
                        "Data write was lost");
                    assert_eq!(data.get_page_type(), PageType::Basic,
                        "Page type was corrupted");
                }

                Ok::<(), String>(())
            });
            handles.push(handle);
        }

        // Drop the initial guard after spawning threads
        drop(page_guard);

        // Wait for all threads to complete and check results
        for handle in handles {
            handle.join().unwrap().expect("Thread operation failed");
        }

        // Verify final state
        let final_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        
        let data = final_guard.read();
        
        // Verify page type is preserved
        assert_eq!(data.get_page_type(), PageType::Basic, 
            "Final page type was corrupted");
        
        // Verify each thread's write
        for i in 0..thread_count {
            let offset = i + 1;
            assert_eq!(data.get_data()[offset], (i + 1) as u8,
                "Thread {} write was lost", i);
        }
    }

    #[test]
    fn test_page_eviction() {
        let ctx = TestContext::new("page_eviction");
        let bpm = ctx.bpm;

        // Fill buffer pool
        let mut pages = Vec::new();
        for _ in 0..bpm.get_pool_size() {
            let page_guard = bpm.new_page::<BasicPage>()
                .expect("Failed to create new page");
            pages.push(page_guard);
        }

        // Try to create one more page (should fail)
        assert!(bpm.new_page::<BasicPage>().is_none());

        // Drop half the pages
        for _ in 0..bpm.get_pool_size()/2 {
            pages.pop();
        }

        // Should now be able to create new pages
        assert!(bpm.new_page::<BasicPage>().is_some());
    }

    #[test]
    fn test_buffer_pool_concurrent_operations() -> Result<(), Box<dyn Error>> {
        let ctx = TestContext::new("concurrent_operations");
        let bpm = Arc::new(ctx.bpm());

        let num_threads = 5;
        let ops_per_thread = 20;
        let mut handles = vec![];

        // Create a barrier to synchronize thread starts
        let barrier = Arc::new(Barrier::new(num_threads));
        
        // Create initial pages to work with
        let mut initial_pages = Vec::new();
        for _ in 0..3 {
            if let Some(guard) = bpm.new_page::<BasicPage>() {
                initial_pages.push(guard.get_page_id());
            }
        }

        // Spawn multiple threads performing mixed operations
        for thread_id in 0..num_threads {
            let bpm_clone = Arc::clone(&bpm);
            let barrier_clone = Arc::clone(&barrier);
            let initial_pages = initial_pages.clone();
            
            let handle = thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
                // Wait for all threads to be ready
                barrier_clone.wait();
                
                for op_id in 0..ops_per_thread {
                    // Add small delay to reduce contention
                    thread::sleep(Duration::from_micros(1));
                    
                    match op_id % 3 {
                        0 => {
                            // Create new page
                            trace!("Thread {} creating new page", thread_id);
                            if let Some(guard) = bpm_clone.new_page::<BasicPage>() {
                                let mut data = guard.write();
                                data.get_data_mut()[1] = thread_id as u8; // Write to non-type byte
                                data.set_dirty(true);
                            }
                        }
                        1 => {
                            // Fetch existing page
                            let page_id = initial_pages[op_id % initial_pages.len()];
                            trace!("Thread {} fetching page {}", thread_id, page_id);
                            if let Some(guard) = bpm_clone.fetch_page::<BasicPage>(page_id) {
                                let data = guard.read();
                                trace!("Thread {} read data: {}", thread_id, data.get_data()[1]);
                            }
                        }
                        2 => {
                            // Delete page (only delete newly created pages)
                            if op_id > initial_pages.len() {
                                let page_id = (op_id - initial_pages.len()) as PageId;
                                trace!("Thread {} deleting page {}", thread_id, page_id);
                                let _ = bpm_clone.delete_page(page_id);
                            }
                        }
                        _ => unreachable!()
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }

        // Wait for all threads and collect results
        for handle in handles {
            handle.join().expect("Thread failed");
        }

        // Verify final state
        bpm.flush_all_pages();
        
        // Verify buffer pool state is consistent
        {
            let pages = bpm.pages.read();
            let page_table = bpm.page_table.read();
            
            // Verify consistency between pages and page table
            for (page_id, frame_id) in page_table.iter() {
                assert!(pages[*frame_id as usize].is_some(), 
                    "Page table entry {} points to empty frame {}", page_id, frame_id);
            }
        }
        
        Ok(())
    }

    #[test]
    fn test_buffer_pool_recovery() {
        let ctx = TestContext::new("recovery");
        let bpm = ctx.bpm();

        // Create and modify some pages
        let mut page_ids = Vec::new();
        for i in 0..3 {
            let guard = bpm
                .new_page::<BasicPage>()
                .expect("Failed to create new page");
            
            {
                let mut data = guard.write();
                data.get_data_mut()[1] = (i + 1) as u8;  // Write to offset 1 to preserve page type
                data.set_dirty(true);
                
                // Verify page type is preserved
                assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8(),
                    "Page type should be preserved");
            }
            
            page_ids.push(guard.get_page_id());
        }

        // Flush all pages to disk
        trace!("Flushing all pages to disk");
        bpm.flush_all_pages();

        // Clear buffer pool
        trace!("Clearing buffer pool");
        for page_id in &page_ids {
            bpm.delete_page(*page_id).expect("Failed to delete page");
        }

        // Verify pages can be recovered from disk
        for (i, page_id) in page_ids.iter().enumerate() {
            trace!("Recovering page {}", page_id);
            let guard = bpm
                .fetch_page::<BasicPage>(*page_id)
                .expect("Failed to recover page");
            
            let data = guard.read();
            
            // Verify page type is preserved
            assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8(),
                "Page type should be preserved for page {}", page_id);
            
            // Verify data is correct
            assert_eq!(
                data.get_data()[1], 
                (i + 1) as u8,
                "Recovered page {} has incorrect data", page_id
            );
            
            // Verify pin count is correct
            assert_eq!(data.get_pin_count(), 2,
                "Recovered page {} has incorrect pin count", page_id);
        }
    }

    #[test]
    fn test_buffer_pool_error_handling() {
        let ctx = TestContext::new("error_handling");
        let bpm = ctx.bpm();

        // Test invalid page fetch
        assert!(bpm.fetch_page::<BasicPage>(INVALID_PAGE_ID).is_none(),
            "Fetching invalid page should return None");

        // Test invalid page deletion
        assert!(matches!(
            bpm.delete_page(INVALID_PAGE_ID),
            Err(DeletePageError::PageNotFound(_))
        ), "Deleting invalid page should return error");

        // Test double deletion
        if let Some(guard) = bpm.new_page::<BasicPage>() {
            let page_id = guard.get_page_id();
            drop(guard);
            
            bpm.delete_page(page_id).expect("First deletion should succeed");
            assert!(matches!(
                bpm.delete_page(page_id),
                Err(DeletePageError::PageNotFound(_))
            ), "Second deletion should fail");
        }
    }

    #[test]
    fn test_dirty_page_writeback() {
        let ctx = TestContext::new("dirty_page_writeback");
        let bpm = ctx.bpm();

        // Create and modify a page
        let page_id = {
            let guard = bpm.new_page::<BasicPage>()
                .expect("Failed to create page");
            
            // Modify page and mark dirty (avoid writing to type byte)
            {
                let mut page = guard.write();
                page.get_data_mut()[1] = 42;  // Write to second byte, not type byte
                page.set_dirty(true);
                
                // Verify type byte is preserved
                assert_eq!(page.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8(),
                    "Type byte should be preserved");
            }
            
            let id = guard.get_page_id();
            drop(guard); // Should trigger write-back when unpinned
            id
        };

        // Force eviction of the page
        bpm.flush_all_pages();

        // Fetch page from disk and verify data persisted
        let guard = bpm.fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");
        
        let data = guard.read();
        assert_eq!(data.get_data()[1], 42, "Data should be preserved");
        assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8(),
            "Type byte should be preserved");
        assert!(!data.is_dirty(), "Page should not be dirty after load from disk");
    }
}
