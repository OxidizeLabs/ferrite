use crate::buffer::lru_k_replacer::AccessType::Lookup;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{FrameId, PageId};
use crate::disk::disk_manager::DiskManager;
use crate::page_db::page::Page;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

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
    disk_manager: Arc<DiskManager>,
    write_back_cache: Option<Arc<WriteBackCache>>,
}

impl BufferPoolManager {
    pub fn new(
        pool_size: usize,
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
            disk_manager,
            write_back_cache: None,
        }
    }

    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn get_pages(&self) -> Arc<RwLock<Vec<Option<Page>>>> {
        self.pages.clone()
    }

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

        if let Some(old_page_id) = old_page_id {
            if let Some(page) = self.pages.read().unwrap().iter().find(|p| {
                p.as_ref()
                    .map_or(false, |pg| pg.get_page_id() == old_page_id)
            }) {
                if page.as_ref().unwrap().is_dirty() {
                    self.disk_manager
                        .write_page(old_page_id, page.as_ref().unwrap().get_data());
                }
                let mut page_table = self.page_table.lock().unwrap();
                page_table.remove(&old_page_id);
            }
        }

        // Allocate a new page ID
        let new_page_id = self.allocate_page();

        // Create a new page and reset its metadata
        let mut new_page = Page::new(new_page_id);
        new_page.reset_memory();

        // Insert the new page into the pages map and page table
        let mut pages = self.pages.write().unwrap();
        if frame_id as usize >= pages.len() {
            pages.resize_with(frame_id as usize + 1, || None);
        }
        pages[frame_id as usize] = Some(new_page.clone());

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

    pub fn fetch_page(&self, page_id: PageId, access_type: AccessType) -> Arc<RwLock<Page>> {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
         * but all frames are currently in use and not evictable (in another word, pinned).
         *
         * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
         * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
         * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
         * you need to write it back to disk and update the metadata of the new page
         *
         * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
         *
         * @param page_id id of page to be fetched
         * @param access_type type of access to the page, only needed for leaderboard tests.
         * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
         */
        // let page_table = self.page_table.lock().unwrap();
        // if let Some(&frame_id) = page_table.get(&page_id) {
        //     let pages = self.pages.read().unwrap();
        //     Some(Arc::new(RwLock::new(pages[frame_id].clone().unwrap())))
        // } else if let frame_id = {
        //     let mut free_list = self.free_list.lock().unwrap();
        //     if let Some(frame_id) = free_list.pop() {
        //         self.d
        //     } else {
        //         // Lock the replacer to evict a frame
        //         let replacer = self.replacer.lock().unwrap();
        //         replacer.evict()?
        //     }
        // }
        unimplemented!()
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
