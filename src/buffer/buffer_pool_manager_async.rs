use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::INVALID_PAGE_ID;
use crate::common::config::{FrameId, PageId, DB_PAGE_SIZE};
use crate::common::exception::DeletePageError;
use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};
use crate::storage::page::page::{Page, PageTrait};
use crate::storage::page::page::{PageType, PAGE_TYPE_OFFSET};
use crate::storage::page::page_guard::PageGuard;
use log::{error, info, trace, warn};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The `BufferPoolManager` is responsible for managing the buffer pool,
/// including fetching and unpinning pages, and handling page replacement.
/// This version integrates with the new AsyncAsyncDiskManager for better performance.
pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicU64,
    pages: Arc<RwLock<Vec<Option<Arc<RwLock<dyn PageTrait>>>>>>,
    page_table: Arc<RwLock<HashMap<PageId, FrameId>>>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    free_list: Arc<RwLock<Vec<FrameId>>>,
    disk_manager: Arc<AsyncDiskManager>,
    // No embedded runtime - we'll use the current runtime when available
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
    /// - `disk_manager`: A reference to the async disk manager.
    /// - `replacer`: A reference to the page replacement policy.
    ///
    /// # Returns
    /// A new `BufferPoolManager` instance.
    pub fn new(
        pool_size: usize,
        disk_manager: Arc<AsyncDiskManager>,
        replacer: Arc<RwLock<LRUKReplacer>>,
    ) -> Result<Self, String> {
        let free_list: Vec<FrameId> = (0..pool_size as FrameId).collect();
        
        // No embedded runtime needed - we'll use Handle::current() when available
        
        info!(
            "BufferPoolManager initialized with pool size: {}",
            pool_size
        );
        
        Ok(Self {
            pool_size,
            next_page_id: AtomicU64::new(0),
            pages: Arc::new(RwLock::new(vec![None; pool_size])),
            page_table: Arc::new(RwLock::new(HashMap::new())),
            replacer,
            free_list: Arc::new(RwLock::new(free_list)),
            disk_manager,
        })
    }

    /// Creates a new buffer pool manager with custom disk manager configuration
    pub async fn new_with_config(
        pool_size: usize,
        db_file_path: String,
        log_file_path: String,
        config: DiskManagerConfig,
    ) -> Result<Self, String> {
        // Create async disk manager
        let disk_manager = Arc::new(
            AsyncDiskManager::new(db_file_path, log_file_path, config)
                .await
                .map_err(|e| format!("Failed to create async disk manager: {}", e))?
        );
        
        // Create replacer (use default LRU-K with k=2)
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool_size, 2)));
        Self::new(pool_size, disk_manager, replacer)
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
                if page_data.get_page_id() == page_id {
                    // Access trait method directly
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
    /// An optional `PageGuard<T>`.
    pub fn fetch_page<T: Page + 'static>(&self, page_id: PageId) -> Option<PageGuard<T>> {
        trace!("Fetching page {} of type {:?}", page_id, T::TYPE_ID);

        if page_id == INVALID_PAGE_ID || page_id >= self.next_page_id.load(Ordering::SeqCst) {
            trace!("Invalid page ID: {}", page_id);
            return None;
        }

        // Check if page is already in memory
        if let Some(page) = self.get_page_internal(page_id) {
            // Verify page type
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
                warn!(
                    "Page type mismatch: expected {:?}, found {:?}",
                    T::TYPE_ID,
                    actual_type
                );
                return None;
            }

            // Create new guard (which will increment pin count)
            let typed_page = unsafe {
                let raw = Arc::into_raw(page) as *const RwLock<T>;
                Arc::from_raw(raw)
            };

            return Some(PageGuard::new(typed_page, page_id));
        }

        // Page not in memory, need to load from disk
        self.load_page_from_disk::<T>(page_id)
    }

    /// Loads a page from disk into memory
    fn load_page_from_disk<T: Page + 'static>(&self, page_id: PageId) -> Option<PageGuard<T>> {
        trace!("Loading page {} from disk", page_id);

        // Get available frame
        let frame_id = self.get_available_frame()?;
        trace!("Got available frame: {} for disk load", frame_id);

        // Evict if necessary
        self.evict_page_if_necessary(frame_id);

        // Read page data from disk using async disk manager
        let page_data = self.run_async_operation(async {
            self.disk_manager.read_page(page_id).await
        });

        let page_data = match page_data {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to read page {} from disk: {}", page_id, e);
                return None;
            }
        };

        // Verify page type
        if page_data.len() < PAGE_TYPE_OFFSET + 1 {
            error!("Page data too short for page {}", page_id);
            return None;
        }

        let type_byte = page_data[PAGE_TYPE_OFFSET];
        let page_type = PageType::from_u8(type_byte);
        
        trace!(
            "Loaded page {} from disk - Type byte: {}, Parsed type: {:?}, Expected: {:?}",
            page_id,
            type_byte,
            page_type,
            T::TYPE_ID
        );

        if page_type != Some(T::TYPE_ID) {
            warn!(
                "Page type mismatch for page {}: expected {:?}, found {:?}",
                page_id,
                T::TYPE_ID,
                page_type
            );
            return None;
        }

        // Create new page with loaded data
        let new_page = self.create_typed_page::<T>(page_id);
        
        // Set the data
        {
            let mut page_guard = new_page.write();
            if page_data.len() >= DB_PAGE_SIZE as usize {
                let _ = page_guard.set_data(0, &page_data[..DB_PAGE_SIZE as usize]);
            } else {
                error!("Page data size mismatch for page {}", page_id);
                return None;
            }
        }

        // Update metadata
        self.update_page_metadata(frame_id, page_id, &new_page);

        trace!("Successfully loaded page {} from disk in frame {}", page_id, frame_id);
        Some(PageGuard::new(new_page, page_id))
    }

    /// Unpins a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to unpin.
    /// - `is_dirty`: Whether the page is dirty.
    /// - `access_type`: The type of access for the replacer.
    ///
    /// # Returns
    /// `true` if the page was successfully unpinned, `false` otherwise.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        trace!("Unpinning page {} (dirty: {})", page_id, is_dirty);

        let page_table = self.page_table.read();
        let frame_id = match page_table.get(&page_id) {
            Some(&fid) => fid,
            None => {
                trace!("Page {} not found in page table", page_id);
                return false;
            }
        };
        drop(page_table);

        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let mut page_guard = page.write();

            // Update dirty flag
            if is_dirty {
                page_guard.set_dirty(true);
            }

            // Decrement pin count
            let pin_count = page_guard.get_pin_count();
            if pin_count > 0 {
                page_guard.set_pin_count(pin_count - 1);
                trace!(
                    "Page {} unpinned, new pin count: {}",
                    page_id,
                    pin_count - 1
                );

                // If pin count reaches 0, mark as evictable
                if pin_count == 1 {
                    let mut replacer = self.replacer.write();
                    replacer.record_access(frame_id, access_type);
                    replacer.set_evictable(frame_id, true);
                    trace!("Page {} marked as evictable", page_id);
                }

                true
            } else {
                warn!("Attempted to unpin page {} with pin count 0", page_id);
                false
            }
        } else {
            warn!("Page {} not found in pages array", page_id);
            false
        }
    }

    /// Flushes a page with the given page ID to disk.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to flush.
    ///
    /// # Returns
    /// `Ok(())` if the page was successfully flushed, `Err(String)` otherwise.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), String> {
        trace!("Flushing page {} to disk", page_id);

        let page_table = self.page_table.read();
        let frame_id = page_table
            .get(&page_id)
            .ok_or_else(|| format!("Page {} not found in page table", page_id))?;
        let frame_id = *frame_id;
        drop(page_table);

        let pages = self.pages.read();
        let page = pages
            .get(frame_id as usize)
            .and_then(|p| p.as_ref())
            .ok_or_else(|| format!("Page {} not found in pages array", page_id))?
            .clone();
        drop(pages);

        self.write_page_to_disk(page_id, &page)
    }

    /// Flushes all pages to disk.
    pub fn flush_all_pages(&self) {
        trace!("Flushing all pages to disk");

        let page_table = self.page_table.read();
        let page_ids: Vec<PageId> = page_table.keys().cloned().collect();
        drop(page_table);

        for page_id in page_ids {
            if let Err(e) = self.flush_page(page_id) {
                error!("Failed to flush page {}: {}", page_id, e);
            }
        }

        trace!("Finished flushing all pages");
    }

    /// Allocates a new page ID.
    ///
    /// # Returns
    /// A new page ID.
    pub fn allocate_page(&self) -> PageId {
        self.allocate_page_id()
    }

    /// Deallocates a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to deallocate.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        trace!("Deallocating page {}", page_id);
        
        // Remove from page table
        self.remove_page_from_table(page_id);
        
        // Note: In a full implementation, we might want to track deallocated page IDs
        // for reuse, but for now we'll just remove it from the buffer pool
    }

    /// Deletes a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to delete.
    ///
    /// # Returns
    /// `Ok(())` if the page was successfully deleted, `Err(DeletePageError)` otherwise.
    pub fn delete_page(&self, page_id: PageId) -> Result<(), DeletePageError> {
        trace!("Deleting page {}", page_id);

        let frame_id = self.get_frame_id(page_id)?;

        // Check if page is pinned
        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let page_guard = page.read();
            if page_guard.get_pin_count() > 0 {
                return Err(DeletePageError::PagePinned(page_id));
            }
        }
        drop(pages);

        // Reset the frame
        self.reset_frame(frame_id)?;

        // Remove from page table
        self.remove_page_from_table(page_id);

        // Add frame to free list
        {
            let mut free_list = self.free_list.write();
            free_list.push(frame_id);
        }

        // Remove from replacer
        {
            let replacer = self.replacer.write();
            replacer.remove(frame_id);
        }

        trace!("Successfully deleted page {}", page_id);
        Ok(())
    }

    /// Returns the size of the free list.
    pub fn get_free_list_size(&self) -> usize {
        self.free_list.read().len()
    }

    /// Returns a read guard to the replacer.
    pub fn get_replacer(&self) -> Option<RwLockReadGuard<LRUKReplacer>> {
        Some(self.replacer.read())
    }

    // Private helper methods

    fn get_frame_id(&self, page_id: PageId) -> Result<FrameId, DeletePageError> {
        let page_table = self.page_table.read();
        page_table
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
        if let Some(page_slot) = pages.get_mut(frame_id as usize) {
            *page_slot = None;
            Ok(())
        } else {
            Err(DeletePageError::InvalidFrame(frame_id))
        }
    }

    fn evict_page_if_necessary(&self, frame_id: FrameId) {
        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let page_guard = page.read();
            let old_page_id = page_guard.get_page_id();
            let pin_count = page_guard.get_pin_count();
            drop(page_guard);
            drop(pages);

            if pin_count == 0 {
                trace!("Evicting page {} from frame {}", old_page_id, frame_id);
                self.evict_old_page(frame_id, old_page_id);
            } else {
                warn!(
                    "Cannot evict pinned page {} (pin count: {}) from frame {}",
                    old_page_id, pin_count, frame_id
                );
            }
        }
    }

    fn evict_old_page(&self, frame_id: FrameId, old_page_id: PageId) {
        // Check if page is dirty and flush if necessary
        let page_to_flush = {
            let pages = self.pages.read();
            if let Some(Some(page)) = pages.get(frame_id as usize) {
                let is_dirty = page.read().is_dirty();
                if is_dirty {
                    Some(page.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(page) = page_to_flush {
            trace!("Flushing dirty page {} before eviction", old_page_id);
            if let Err(e) = self.write_page_to_disk(old_page_id, &page) {
                error!("Failed to flush page {} during eviction: {}", old_page_id, e);
            }
        }

        // Remove from page table
        self.remove_page_from_table(old_page_id);

        // Reset frame
        if let Err(e) = self.reset_frame(frame_id) {
            error!("Failed to reset frame {} during eviction: {:?}", frame_id, e);
        }

        trace!("Successfully evicted page {} from frame {}", old_page_id, frame_id);
    }

    fn get_available_frame(&self) -> Option<FrameId> {
        // Try to get a frame from the free list first
        {
            let mut free_list = self.free_list.write();
            if let Some(frame_id) = free_list.pop() {
                trace!("Got frame {} from free list", frame_id);
                return Some(frame_id);
            }
        }

        // No free frames, try to evict using LRU-K
        trace!("No free frames, attempting eviction");
        let mut replacer = self.replacer.write();
        if let Some(frame_id) = replacer.evict() {
            trace!("LRU-K replacer selected frame {} for eviction", frame_id);
            Some(frame_id)
        } else {
            warn!("No evictable frames found");
            None
        }
    }

    fn allocate_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    fn create_typed_page<T: Page>(&self, page_id: PageId) -> Arc<RwLock<T>> {
        let mut page = T::new(page_id);
        
        // Set initial page metadata
        page.set_pin_count(1); // Start with pin count 1
        page.set_dirty(false);

        // Ensure page type is set correctly
        let type_byte = T::TYPE_ID.to_u8();
        page.get_data_mut()[PAGE_TYPE_OFFSET] = type_byte;

        trace!(
            "Created new page {} of type {:?} (type byte: {})",
            page_id,
            T::TYPE_ID,
            type_byte
        );

        Arc::new(RwLock::new(page))
    }

    fn update_page_metadata<P: PageTrait + 'static>(
        &self,
        frame_id: FrameId,
        page_id: PageId,
        new_page: &Arc<RwLock<P>>,
    ) {
        // Update pages array
        {
            let mut pages = self.pages.write();
            let page_clone = new_page.clone();
            let trait_object: Arc<RwLock<dyn PageTrait>> = page_clone;
            pages[frame_id as usize] = Some(trait_object);
        }

        // Update page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
        }

        // Update replacer
        {
            let mut replacer = self.replacer.write();
            replacer.record_access(frame_id, AccessType::Unknown);
            replacer.set_evictable(frame_id, false); // New pages are not evictable
        }

        trace!(
            "Updated metadata for page {} in frame {}",
            page_id,
            frame_id
        );
    }

    fn write_page_to_disk(
        &self,
        page_id: PageId,
        page: &Arc<RwLock<dyn PageTrait>>,
    ) -> Result<(), String> {
        // Create a data buffer
        let data_buffer = {
            let page_guard = page.read();

            // Get the page type first
            let page_type = page_guard.get_page_type();
            let type_byte = page_type.to_u8();

            // Copy the data
            let mut data = Vec::with_capacity(DB_PAGE_SIZE as usize);
            data.extend_from_slice(page_guard.get_data());

            // Ensure the type byte is preserved
            data[PAGE_TYPE_OFFSET] = type_byte;

            // Verify type byte
            debug_assert_eq!(
                data[PAGE_TYPE_OFFSET], type_byte,
                "Page type byte was corrupted during write"
            );

            trace!(
                "Writing page {} to disk. Type: {:?}, Type byte: {}, First bytes: {:?}",
                page_id,
                page_type,
                data[PAGE_TYPE_OFFSET],
                &data[..8]
            );

            data
        };

        // Use async disk manager to write page
        self.run_async_operation(async {
            self.disk_manager.write_page(page_id, data_buffer).await
        })
        .map_err(|e| format!("Failed to write page to disk: {}", e))?;

        // Mark page as clean after successful write
        {
            let mut page_guard = page.write();
            page_guard.set_dirty(false);
        }

        trace!("Successfully wrote page {} to disk", page_id);
        Ok(())
    }

    fn get_page_internal(&self, page_id: PageId) -> Option<Arc<RwLock<dyn PageTrait>>> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id)?;

        let pages = self.pages.read();
        pages[*frame_id as usize].clone()
    }

    /// Flushes multiple dirty pages efficiently
    pub fn flush_dirty_pages_batch(&self, max_pages: usize) -> Result<usize, String> {
        trace!("Starting batch flush of up to {} dirty pages", max_pages);
        
        // Collect dirty page IDs in a single lock acquisition
        let dirty_pages: Vec<PageId> = {
            let pages = self.pages.read();
            pages
                .iter()
                .filter_map(|page_opt| {
                    if let Some(page) = page_opt {
                        let page_guard = page.read();
                        if page_guard.is_dirty() {
                            Some(page_guard.get_page_id())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .take(max_pages)
                .collect()
        };

        let mut flushed_count = 0;
        for page_id in dirty_pages {
            if let Ok(()) = self.flush_page(page_id) {
                flushed_count += 1;
            }
        }

        trace!("Successfully flushed {} dirty pages in batch", flushed_count);
        Ok(flushed_count)
    }

    /// Gets the async disk manager for direct access
    pub fn get_disk_manager(&self) -> Arc<AsyncDiskManager> {
        Arc::clone(&self.disk_manager)
    }

    /// Performs a health check on the buffer pool
    pub fn health_check(&self) -> bool {
        // Check if disk manager is healthy
        let disk_health = self.disk_manager.health_check();
        
        // Check buffer pool integrity
        let pages_count = self.pages.read().len();
        let free_list_size = self.free_list.read().len();
        let page_table_size = self.page_table.read().len();
        
        // Basic sanity checks
        let buffer_pool_healthy = pages_count == self.pool_size
            && free_list_size + page_table_size <= self.pool_size;
        
        disk_health && buffer_pool_healthy
    }

    /// Helper function to run async operations
    /// Uses futures executor to avoid runtime conflicts
    fn run_async_operation<F, T>(&self, future: F) -> Result<T, std::io::Error>
    where
        F: std::future::Future<Output = Result<T, std::io::Error>>,
    {
        // Use futures::executor::block_on which doesn't conflict with tokio
        futures::executor::block_on(future)
    }

    /// Gracefully shuts down the buffer pool manager
    pub async fn shutdown(&mut self) -> Result<(), String> {
        info!("Shutting down BufferPoolManager");
        
        // Flush all dirty pages
        self.flush_all_pages();
        
        // Shutdown the disk manager
        let disk_manager = Arc::clone(&self.disk_manager);
        if let Some(mut dm) = Arc::try_unwrap(disk_manager).ok() {
            dm.shutdown().await
                .map_err(|e| format!("Failed to shutdown disk manager: {}", e))?;
        }
        
        info!("BufferPoolManager shutdown complete");
        Ok(())
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
            disk_manager: Arc::clone(&self.disk_manager),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::page::{BasicPage, PageTrait, PageType};
    use std::sync::Arc;
    use tempfile::TempDir;

    struct AsyncTestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl AsyncTestContext {
        pub async fn new(name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 5;

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

            // Use default configuration for testing
            let config = DiskManagerConfig::default();
            
            let bpm = Arc::new(
                BufferPoolManager::new_with_config(
                    BUFFER_POOL_SIZE,
                    db_path,
                    log_path,
                    config,
                )
                .await
                .expect("Failed to create BufferPoolManager")
            );

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    #[tokio::test]
    async fn test_async_basic_page_operations() {
        let ctx = AsyncTestContext::new("async_basic_page_operations").await;
        let bpm = ctx.bpm();

        // Create a new page
        let page_guard = bpm
            .new_page::<BasicPage>()
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
        assert_eq!(
            page_guard.read().get_data()[PAGE_TYPE_OFFSET],
            PageType::Basic.to_u8()
        );

        // Drop the guard to unpin the page
        drop(page_guard);

        // Fetch the page again
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify type and data after fetch
        let data = fetched_guard.read();
        assert_eq!(data.get_page_type(), PageType::Basic);
        assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        assert_eq!(data.get_data()[1], 42);
    }

    #[tokio::test]
    async fn test_async_disk_integration() {
        let ctx = AsyncTestContext::new("async_disk_integration").await;
        let bpm = ctx.bpm();

        // Create and write a page
        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");
        
        let page_id = page_guard.read().get_page_id();
        
        // Write some test data
        {
            let mut data = page_guard.write();
            data.get_data_mut()[1] = 123;
            data.set_dirty(true);
        }

        // Flush to disk
        bpm.flush_page(page_id).expect("Failed to flush page");
        
        // Unpin the page
        drop(page_guard);
        
        // Delete from buffer pool to force disk read
        bpm.delete_page(page_id).expect("Failed to delete page");
        
        // Fetch again - should read from disk
        let reloaded_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page from disk");
        
        // Verify data was persisted
        let data = reloaded_guard.read();
        assert_eq!(data.get_data()[1], 123);
        assert_eq!(data.get_page_type(), PageType::Basic);
    }

    #[tokio::test]
    async fn test_async_health_check() {
        let ctx = AsyncTestContext::new("async_health_check").await;
        let bpm = ctx.bpm();

        // Fresh buffer pool should be healthy
        assert!(bpm.health_check());

        // After some operations, should still be healthy
        for i in 0..3 {
            let page_guard = bpm.new_page::<BasicPage>().expect("Failed to create page");
            let page_id = page_guard.read().get_page_id();
            
            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = i as u8;
            }
            
            drop(page_guard);
            bpm.flush_page(page_id).expect("Failed to flush page");
        }

        assert!(bpm.health_check());
    }

    #[tokio::test]
    async fn test_async_batch_flush() {
        let ctx = AsyncTestContext::new("async_batch_flush").await;
        let bpm = ctx.bpm();

        // Create multiple dirty pages
        let mut page_ids = Vec::new();
        for i in 0..4 {
            let page_guard = bpm.new_page::<BasicPage>().expect("Failed to create page");
            let page_id = page_guard.read().get_page_id();
            page_ids.push(page_id);
            
            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = i as u8;
                data.set_dirty(true);
            }
            
            drop(page_guard);
        }

        // Flush in batch
        let flushed_count = bpm.flush_dirty_pages_batch(3).expect("Failed to batch flush");
        assert!(flushed_count <= 3);
        assert!(flushed_count > 0);
    }
} 