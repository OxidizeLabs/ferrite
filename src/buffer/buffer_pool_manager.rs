use crate::buffer::lru_k_replacer::AccessType::Lookup;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId};
use crate::disk::disk_manager::DiskManager;
use crate::page_db::page::Page;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use crate::disk::disk_scheduler::{DiskRequest, DiskScheduler};

const INVALID_PAGE_ID: PageId = -1;

struct LogManager {}
struct WriteBackCache {}
struct BasicPageGuard {}
struct ReadPageGuard {}
struct WritePageGuard {}

pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicI32,
    pages: Arc<RwLock<Vec<Option<Page>>>>,
    page_table: Arc<Mutex<HashMap<PageId, FrameId>>>,
    replacer: Arc<Mutex<LRUKReplacer>>,
    free_list: Arc<Mutex<Vec<FrameId>>>,
    disk_scheduler: Arc<DiskScheduler>,
    write_back_cache: Option<Arc<WriteBackCache>>,
}

impl BufferPoolManager {
    ///  Creates a new BufferPoolManager.
    ///
    /// # Arguments
    ///
    /// * `pool_size`: the size of the buffer pool
    /// * `disk_scheduler`: the disk scheduler
    /// * `replacer`: replacer_k the LookBack constant k for the LRU-K replacer
    ///
    /// returns: BufferPoolManager
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    pub fn new(
        pool_size: usize,
        disk_scheduler: Arc<DiskScheduler>,
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
            write_back_cache: None,
        }
    }

    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Page>>>> {
        self.pages.clone()
    }

    ///  Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
    ///  are currently in use and not evictable (in another word, pinned).
    ///
    ///  You should pick the replacement frame from either the free list or the replacer (always find from the free list
    ///  first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
    ///  you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
    ///
    ///  Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
    ///  so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
    ///  Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
    ///
    ///
    /// returns: nullptr if no new pages could be created, otherwise pointer to new page
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    pub fn new_page(&self) -> Option<Page> {
        // Attempt to find a free frame first
        let frame_id = {
            let mut free_list = self.free_list.lock().unwrap();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                // Lock the replacer to evict a frame
                let replacer = self.replacer.lock().unwrap();
                replacer.evict()?
            }
        };

        // Remove the old page from the page table if necessary
        let old_page_id = {
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
        };

        // If old page is dirty, write it back to disk before removing from the page table
        if let Some(old_page_id) = old_page_id {
            if let Some(page) = self.pages.read().unwrap()[frame_id as usize].as_ref() {
                if page.is_dirty() {
                    self.disk_scheduler.schedule(
                        false,
                        Arc::new(Mutex::new(page.get_data())),
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
            if frame_id >= pages.len() as i32 {
                pages.resize_with((frame_id + 1) as usize, || None);
            }
            pages[frame_id as usize] = Some(new_page.clone());
        }

        let mut page_table = self.page_table.lock().unwrap();
        page_table.insert(new_page_id, frame_id);

        // Lock the replacer to mark this frame as non-evictable
        let replacer = self.replacer.lock().unwrap();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, Lookup);

        Some(new_page)
    }

    pub fn new_page_guarded(&self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    /// Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
    /// but all frames are currently in use and not evictable (in another word, pinned).
    ///
    /// First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
    /// the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
    /// disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
    /// you need to write it back to disk and update the metadata of the new page.
    ///
    /// In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
    ///
    /// # Arguments
    ///
    /// * `page_id`: page_id id of page to be fetched
    /// * `access_type`: access_type type of access to the page, only needed for leaderboard tests.
    ///
    /// returns: Arc<RwLock<Page>, Global>
    /// nullptr if page_id cannot be fetched, otherwise pointer to the requested page
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    pub fn fetch_page(&self, page_id: PageId) -> Option<Arc<RwLock<Page>>> {
        // First, try to find the page in the page table
        {
            let page_table = self.page_table.lock().unwrap();
            if let Some(&frame_id) = page_table.get(&page_id) {
                let pages = self.pages.read().unwrap();
                let page = pages[frame_id as usize].as_ref().unwrap();

                // Record access and disable eviction
                let mut replacer = self.replacer.lock().unwrap();
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
                let mut replacer = self.replacer.lock().unwrap();
                if let Some(evict_frame_id) = replacer.evict() {
                    evict_frame_id
                } else {
                    // All frames are pinned, cannot fetch the page
                    return None;
                }
            }
        };

        // If the frame is occupied, handle the dirty page and remove the old page from the page table
        let old_page_id = {
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
        };

        if let Some(old_page_id) = old_page_id {
            let mut pages = self.pages.write().unwrap();
            if let Some(page) = pages[frame_id as usize].as_mut() {
                if page.is_dirty() {
                    self.disk_scheduler.schedule(
                        false,
                        Arc::clone(&Arc::new(Mutex::new(page.get_data()))),
                        old_page_id
                    );
                }
            }
            let mut page_table = self.page_table.lock().unwrap();
            page_table.remove(&old_page_id);
        }

        // Read the new page from disk
        let mut new_page = Page::new(page_id);
        self.disk_scheduler.schedule(
            true,
            Arc::clone(&Arc::new(Mutex::new(new_page.get_data()))),
            page_id
        );

        // Insert the new page into the pages map and page table
        let mut pages = self.pages.write().unwrap();
        pages[frame_id as usize] = Some(new_page.clone());

        let mut page_table = self.page_table.lock().unwrap();
        page_table.insert(page_id, frame_id);

        // Record access and disable eviction for the new page
        let mut replacer = self.replacer.lock().unwrap();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, Lookup);

        Some(Arc::new(RwLock::new(new_page)))
    }

    pub fn fetch_page_basic(&self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    pub fn fetch_page_read(&self, page_id: PageId) -> Option<ReadPageGuard> {
        unimplemented!()
    }

    pub fn fetch_page_write(&self, page_id: PageId) -> Option<WritePageGuard> {
        unimplemented!()
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        unimplemented!()
    }

    pub fn flush_page(&self, page_id: PageId) -> bool {
        unimplemented!()
    }

    pub fn flush_all_pages(&self) {
        unimplemented!()
    }

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

    pub fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn deallocate_page(&mut self, page_id: PageId) {
        unimplemented!()
    }
}

// fn main() {
//     // Example usage of BufferPoolManager
//     let disk_manager = Arc::new(disk_manager::DiskManager {});
//     let replacer = Arc::new(lru_k_replacer::LRUKReplacer {});
//     let mut buffer_pool_manager = BufferPoolManager::new(100, disk_manager, replacer);
//
//     // Create a new page
//     if let Some(new_page) = buffer_pool_manager.new_page() {
//         println!("Created new page: {:?}", new_page.lock().unwrap().get_page_id());
//     }
//
//     // Further usage...
// }
