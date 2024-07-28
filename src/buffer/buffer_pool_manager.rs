use crate::buffer::lru_k_replacer::AccessType::Lookup;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId};
use crate::page_db::page::Page;
use crate::disk::disk_manager::DiskManager;
use crate::disk::disk_scheduler::DiskScheduler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const INVALID_PAGE_ID: PageId = -1;

pub struct LogManager {}
pub struct WriteBackCache {}
pub struct BasicPageGuard {}
pub struct ReadPageGuard {}
pub struct WritePageGuard {}

/// BufferPoolManager reads disk pages to and from its internal buffer pool.
pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicI32,
    pages: Arc<RwLock<Vec<Option<Page>>>>,
    page_table: Arc<Mutex<HashMap<PageId, FrameId>>>,
    replacer: Arc<Mutex<LRUKReplacer>>,
    free_list: Arc<Mutex<Vec<FrameId>>>,
    disk_scheduler: Arc<DiskScheduler>,
    disk_manager: Arc<DiskManager>,
    write_back_cache: Option<Arc<WriteBackCache>>,
}

impl BufferPoolManager {
    ///  Creates a new BufferPoolManager.
    pub fn new(
        pool_size: usize,
        disk_scheduler: Arc<DiskScheduler>,
        disk_manager: Arc<DiskManager>,
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
            write_back_cache: None,
        }
    }

    /// returns: The size (number of frames) of the buffer pool.
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    /// returns: the pointer to all the pages in the buffer pool.
    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Page>>>> {
        self.pages.clone()
    }

    ///  Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all
    /// frames are currently in use and not evictable (in another word, pinned).
    pub fn new_page(&self) -> Option<Page> {
        // Attempt to find a free frame first
        let frame_id = {
            let mut free_list = self.free_list.lock().unwrap();
            if let Some(frame_id) = free_list.pop() {
                println!("Found free frame: {}", frame_id);  // Debugging statement
                frame_id
            } else {
                // Lock the replacer to evict a frame
                let mut replacer = self.replacer.lock().unwrap();
                if let Some(frame_id) = replacer.evict() {
                    println!("Evicted frame: {}", frame_id);  // Debugging statement
                    frame_id
                } else {
                    println!("No frames available for eviction.");  // Debugging statement
                    return None;
                }
            }
        };

        // Remove the old page from the page table if necessary
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock().unwrap();
            page_table.iter().find_map(
                |(&page_id, &fid)| {
                    if fid == frame_id {
                        Some(page_id)
                    } else {
                        None
                    }
                },
            )
        } {
            if let Some(page) = self.pages.read().unwrap()[frame_id as usize].as_ref() {
                if page.is_dirty() {
                    self.disk_scheduler.schedule(
                        false,
                        Arc::new(Mutex::new(page.get_data().clone())),
                        old_page_id,
                    );
                }
            }
            let mut page_table = self.page_table.lock().unwrap();
            page_table.remove(&old_page_id);
        }

        // Allocate a new page ID
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);

        // Create a new page and reset its metadata
        let mut new_page = Page::new(new_page_id);
        new_page.reset_memory();

        // Insert the new page into the pages map and page table
        {
            let mut pages = self.pages.write().unwrap();
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock().unwrap();
        page_table.insert(new_page_id, frame_id);

        // Lock the replacer to mark this frame as non-evictable
        {
            let mut replacer = self.replacer.lock().unwrap();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id, Lookup);
        }

        Some(new_page)
    }

    /// PageGuard wrapper for NewPage.
    pub fn new_page_guarded(&self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    /// Fetch the requested page from the buffer pool.
    pub fn fetch_page(&self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        // First, try to find the page in the page table
        {
            let page_table = self.page_table.lock().unwrap();
            if let Some(&frame_id) = page_table.get(&page_id) {
                let pages = self.pages.read().unwrap();
                let page = pages[frame_id as usize].as_ref().unwrap();

                // Record access and disable eviction
                let replacer = self.replacer.lock().unwrap();
                replacer.record_access(frame_id, Lookup);
                replacer.set_evictable(frame_id, false);

                return Some(Arc::new(RwLock::new(page.clone())));
            }
        }

        // Try to get a frame from the free list
        let frame_id = {
            let mut free_list = self.free_list.lock().unwrap();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                // If no free frame is available, evict a frame using the replacer
                let replacer = self.replacer.lock().unwrap();
                if let Some(evict_frame_id) = replacer.evict() {
                    evict_frame_id
                } else {
                    // All frames are pinned, cannot fetch the page
                    return None;
                }
            }
        };

        // If the frame is occupied, handle the dirty page and remove the old page from the page table
        if let Some(old_page_id) = {
            let page_table = self.page_table.lock().unwrap();
            page_table.iter().find_map(
                |(&pid, &fid)| {
                    if fid == frame_id {
                        Some(pid)
                    } else {
                        None
                    }
                },
            )
        } {
            let mut pages = self.pages.write().unwrap();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.schedule(
                        false,
                        Arc::clone(&Arc::new(Mutex::new(*page.get_data()))),
                        old_page_id
                    );
                }
            }
            let mut page_table = self.page_table.lock().unwrap();
            page_table.remove(&old_page_id);
        }

        // Read the new page from disk
        let new_page = Page::new(page_id);
        self.disk_scheduler.schedule(
            true,
            Arc::clone(&Arc::new(Mutex::new(*new_page.get_data()))),
            page_id
        );

        // Insert the new page into the pages map and page table
        let mut pages = self.pages.write().unwrap();
        pages[frame_id as usize] = Some(new_page.clone());

        let mut page_table = self.page_table.lock().unwrap();
        page_table.insert(page_id, frame_id);

        // Record access and disable eviction for the new page
        let replacer = self.replacer.lock().unwrap();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, Lookup);

        Some(Arc::new(RwLock::new(new_page)))
    }

    /// PageGuard wrappers for FetchPage
    pub fn fetch_page_basic(&self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    pub fn fetch_page_read(&self, page_id: PageId) -> Option<ReadPageGuard> {
        unimplemented!()
    }

    pub fn fetch_page_write(&self, page_id: PageId) -> Option<WritePageGuard> {
        unimplemented!()
    }

    /// Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its
    /// pin count is already 0, return false.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        let mut page_table = self.page_table.lock().unwrap();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut pages = self.pages.write().unwrap();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.get_pin_count() > 0 {
                    page.decrement_pin_count();
                    page.set_dirty(is_dirty);

                    if page.get_pin_count() == 0 {
                        let mut replacer = self.replacer.lock().unwrap();
                        replacer.set_evictable(frame_id, true);
                        replacer.record_access(frame_id, access_type);
                        println!("Frame {} marked as evictable", frame_id); // Debugging statement
                    } else {
                        println!("Frame {} pin count is not zero", frame_id); // Debugging statement
                    }
                    return true;
                } else {
                    println!("Frame {} pin count is already zero", frame_id); // Debugging statement
                }
            } else {
                println!("Page not found in pages for frame {}", frame_id); // Debugging statement
            }
        } else {
            println!("Page ID {} not found in page table", page_id); // Debugging statement
        }
        false
    }

    /// Flush the target page to disk.
    pub fn flush_page(&self, page_id: PageId) -> Option<bool> {
        // First, try to find the page in the page table
        let page_table = self.page_table.lock().unwrap();
        return if let Some(&frame_id) = page_table.get(&page_id) {
            let pages = self.pages.read().unwrap();
            let page = pages[frame_id as usize].clone().unwrap();
            self.disk_manager.write_page(page_id, *page.get_data());

            Some(true)
        } else {
            None
        }
    }

    /// Flush all the pages in the buffer pool to disk.
    pub fn flush_all_pages(&self) {
        let page_table = self.page_table.lock().unwrap();
        let pages = self.pages.read().unwrap();
        let pages_iter = pages.iter();

        for page in pages_iter {
            let page_id = page.clone().unwrap().get_page_id();
            self.flush_page(page_id);
        }
    }

    /// Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and
    /// return true. If the page is pinned and cannot be deleted, return false immediately.
    pub fn delete_page(&self, page_id: PageId) -> bool {
        if let Some(frame_id) = self.page_table.lock().unwrap().get(&page_id) {
            let mut pages = self.pages.write().unwrap();
            if let Some(page) = pages.get_mut(*frame_id as usize).and_then(Option::as_mut) {
                page.reset_memory();
                let replacer = self.replacer.lock().unwrap();
                replacer.remove(*frame_id);
                self.free_list.lock().unwrap().push(*frame_id);
                return true;
            }
        }
        false
    }

    /// Allocate a page on disk. Caller should acquire the latch before calling this function.
    pub fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Deallocate a page on disk. Caller should acquire the latch before calling this function.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        unimplemented!()
    }
}
