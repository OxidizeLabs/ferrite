use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use spin::{Mutex, RwLock};
use tokio::sync::Mutex as TokioMutex;

use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{DB_PAGE_SIZE, FrameId, PageId};
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
    pages: Arc<RwLock<Vec<Option<Page>>>>,
    page_table: Arc<Mutex<HashMap<PageId, FrameId>>>,
    replacer: Arc<Mutex<LRUKReplacer>>,
    free_list: Arc<Mutex<Vec<FrameId>>>,
    disk_scheduler: Arc<TokioMutex<DiskScheduler>>,
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
        disk_scheduler: Arc<TokioMutex<DiskScheduler>>,
        disk_manager: Arc<FileDiskManager>,
        replacer: Arc<Mutex<LRUKReplacer>>,
    ) -> Self {
        let free_list: Vec<FrameId> = (0..pool_size as FrameId).collect();
        Self {
            pool_size,
            next_page_id: AtomicI32::new(0),
            pages: Arc::new(RwLock::new(vec![None; pool_size])),
            page_table: Arc::new(Mutex::new(HashMap::new())),
            replacer,
            free_list: Arc::new(Mutex::new(free_list)),
            disk_scheduler,
            disk_manager,
        }
    }

    /// Returns the size of the buffer pool.
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns the pages in the buffer pool.
    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Page>>>> {
        self.pages.clone()
    }

    /// Creates a new page and returns it.
    ///
    /// # Returns
    /// An optional new page.
    pub async fn new_page(&self) -> Option<Page> {
        let frame_id = {
            let mut free_list = self.free_list.lock();
            free_list.pop().unwrap_or_else(|| {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            })
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    // Schedule the write but don't match on the result since it's not a Result type
                    let ds = self.disk_scheduler.lock().await;
                    let _receiver = ds.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id);
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Create new page
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let new_page = Page::new(new_page_id.into());

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(new_page_id as PageId, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(new_page)
    }

    /// Creates a new page guard and returns it.
    ///
    /// # Returns
    /// An optional new `BasicPageGuard`.
    pub async fn new_page_guarded(self: &Arc<Self>) -> Option<BasicPageGuard> {
        let frame_id = {
            let mut free_list = self.free_list.lock();
            free_list.pop().unwrap_or_else(|| {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            })
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&page_id, &fid)| if fid == frame_id { Some(page_id) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    // Schedule the write but don't match on the result since it's not a Result type
                    let ds = self.disk_scheduler.lock().await;
                    let _receiver = ds.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id);
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Create new page
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let new_page = Page::new(new_page_id.into());

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(new_page_id as PageId, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(BasicPageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(new_page))))
    }

    /// Writes data to the page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to write to.
    /// - `data`: The data to write to the page.
    pub fn write_page(&self, page_id: PageId, data: [u8; DB_PAGE_SIZE]) {
        let mut pages = self.pages.write();
        for (frame_id, page_opt) in pages.iter_mut().enumerate() {
            if let Some(page) = page_opt {
                if page.get_page_id() == page_id {
                    let mut page_mut = page.clone();
                    page_mut.get_data_mut().copy_from_slice(&data);
                    page_mut.set_dirty(true);
                    *page_opt = Some(page_mut);
                    break;
                }
            }
        }
    }

    /// Fetches a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<Page>>`.
    pub async fn fetch_page(&self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        let frame_id = {
            let page_table = self.page_table.lock();
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            let pages = self.pages.read();
            let page = pages[frame_id as usize].as_ref();

            let replacer = self.replacer.lock();
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(page.unwrap().clone())));
        }

        // If page is not in memory, load it from disk
        let frame_id = {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.lock().await.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id).await;
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Load new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut()).await;

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(page_id, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(Arc::new(RwLock::new(new_page)))
    }

    /// Fetches a basic page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<BasicPageGuard>>`.
    pub async fn fetch_page_basic(self: &Arc<Self>, page_id: PageId) -> Option<Arc<RwLock<BasicPageGuard>>> {
        let frame_id = {
            let page_table = self.page_table.lock();
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            let pages = self.pages.read();
            let page = pages[frame_id as usize].as_ref();

            let replacer = self.replacer.lock();
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(BasicPageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(page.clone().unwrap().clone()))))));
        }

        // If page is not in memory, load it from disk
        let frame_id = {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.lock().await.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id).await;
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Load new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(page_id, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(Arc::new(RwLock::new(BasicPageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(new_page))))))
    }

    /// Fetches a read page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<ReadPageGuard>>`.
    pub async fn fetch_page_read(self: &Arc<Self>, page_id: PageId) -> Option<Arc<RwLock<ReadPageGuard>>> {
        let frame_id = {
            let page_table = self.page_table.lock();
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            let pages = self.pages.read();
            let page = pages[frame_id as usize].as_ref();

            let replacer = self.replacer.lock();
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(ReadPageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(page.clone().unwrap().clone()))))));
        }

        // If page is not in memory, load it from disk
        let frame_id = {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.lock().await.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id).await;
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Load new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(page_id, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(Arc::new(RwLock::new(ReadPageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(new_page))))))
    }

    /// Fetches a write page guard for the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `Arc<RwLock<WritePageGuard>>`.
    pub async fn fetch_page_write(self: &Arc<Self>, page_id: PageId) -> Option<Arc<RwLock<WritePageGuard>>> {
        let frame_id = {
            let page_table = self.page_table.lock();
            page_table.get(&page_id).copied()
        };

        if let Some(frame_id) = frame_id {
            let pages = self.pages.read();
            let page = pages[frame_id as usize].as_ref();

            let replacer = self.replacer.lock();
            replacer.record_access(frame_id, AccessType::Lookup);
            replacer.set_evictable(frame_id, false);

            return Some(Arc::new(RwLock::new(WritePageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(page.clone().unwrap().clone()))))));
        }

        // If page is not in memory, load it from disk
        let frame_id = {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                let replacer = self.replacer.lock();
                replacer.evict().unwrap_or(-1)
            }
        };

        if frame_id == -1 {
            return None;
        }

        // Handle eviction of old page if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock();
            page_table.iter().find_map(|(&pid, &fid)| if fid == frame_id { Some(pid) } else { None })
        } {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.lock().await.schedule(false, Arc::new(TokioMutex::new(page.get_data().clone())), old_page_id).await;
                    page.set_dirty(false);
                }
            }
            let mut page_table = self.page_table.lock();
            page_table.remove(&old_page_id);
        }

        // Load new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_manager.read_page(page_id, new_page.get_data_mut());

        {
            let mut pages = self.pages.write();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock();
        page_table.insert(page_id, frame_id);

        {
            let replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, AccessType::Lookup);
        }

        Some(Arc::new(RwLock::new(WritePageGuard::new(Arc::clone(&self), Arc::new(Mutex::new(new_page))))))
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
        let page_table = self.page_table.lock();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                info!("Unpinning page {} with current pin count {}", page_id, page.get_pin_count()); // Debug logging
                if page.get_pin_count() > 0 {
                    page.decrement_pin_count();
                    if is_dirty {
                        page.set_dirty(true);
                    }

                    if page.get_pin_count() == 0 {
                        let replacer = self.replacer.lock();
                        replacer.set_evictable(frame_id, true);
                        replacer.record_access(frame_id, access_type);
                    }
                    return true;
                } else {
                    info!("Page {} has pin count 0, cannot unpin", page_id); // Debug logging
                }
            } else {
                info!("Page {} is not in the pages array", page_id); // Debug logging
            }
        } else {
            info!("Page {} is not in the page table", page_id); // Debug logging
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
        let page_table = self.page_table.lock();
        if let Some(&frame_id) = page_table.get(&page_id) {
            info!("Flushing page {} from frame {}", page_id, frame_id); // Debugging statement
            let mut pages = self.pages.write();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    let data = page.get_data();
                    info!("Page data before flushing: {:?}", &data[..64]); // Debugging statement
                    self.disk_manager.write_page(page_id, &data);
                    info!("Page data written to disk: {:?}", &data[..64]); // Debugging statement
                    page.set_dirty(false); // Reset dirty flag after flushing
                    info!("Page {} flushed successfully", page_id); // Debugging statement
                } else {
                    info!("Page {} is not dirty, no need to flush", page_id); // Debugging statement
                }
                return Some(true);
            } else {
                info!("Failed to find page in frame {}", frame_id); // Debugging statement
                return None;
            }
        } else {
            info!("Page ID {} not found in page table", page_id); // Debugging statement
            return None;
        }
    }

    /// Flushes all pages in the buffer pool to disk.
    pub fn flush_all_pages(&self) {
        let page_table = self.page_table.lock();
        let pages = self.pages.read();

        for (&page_id, &frame_id) in page_table.iter() {
            if let Some(page) = &pages[frame_id as usize] {
                if page.is_dirty() {
                    self.disk_manager.write_page(page_id, &page.get_data());
                    let _ = &page.clone().set_dirty(false);
                }
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
        if let Some(frame_id) = self.page_table.lock().get(&page_id) {
            let mut pages = self.pages.write();
            if let Some(page) = pages.get_mut(*frame_id as usize).and_then(Option::as_mut) {
                page.reset_memory();
                let replacer = self.replacer.lock();
                replacer.remove(*frame_id);
                self.free_list.lock().push(*frame_id);
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
        self.next_page_id.fetch_add(1, Ordering::SeqCst).into()
    }

    /// Deallocates a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to deallocate.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        // Remove the page from the page table
        if let Some(frame_id) = self.page_table.lock().remove(&page_id) {
            // Remove the page from the pages array
            let mut pages = self.pages.write();
            if let Some(page) = pages.get_mut(frame_id as usize) {
                // Reset the page memory and mark it as free
                if let Some(page) = page {
                    page.reset_memory();
                }
                *page = None;
            }

            // Remove the frame from the replacer
            let replacer = self.replacer.lock();
            replacer.remove(frame_id);

            // Add the frame back to the free list
            self.free_list.lock().push(frame_id);
        }

        // Deallocate the page ID
        // (In this implementation, we simply do nothing. Depending on the use case,
        // you might want to track deallocated page IDs separately.)
    }

}
