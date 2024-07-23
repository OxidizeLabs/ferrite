use config::{FrameId, PageId};
use disk_manager::DiskManager;
use lru_k_replacer::AccessType::Lookup;
use lru_k_replacer::{AccessType, LRUKReplacer};
use page::Page;
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
    pages: Arc<RwLock<Vec<Option<Page>>>>, // Use RwLock for concurrent reads/writes
    page_table: Arc<Mutex<HashMap<PageId, FrameId>>>,
    replacer: Arc<Mutex<LRUKReplacer>>, // Use Mutex for exclusive access
    free_list: Arc<Mutex<Vec<FrameId>>>,
    disk_manager: Arc<DiskManager>,
    write_back_cache: Option<Arc<WriteBackCache>>,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>, replacer: LRUKReplacer) -> Self {
        Self {
            pool_size,
            next_page_id: AtomicI32::new(0),
            pages: Arc::new(RwLock::new(vec![None; pool_size])),
            page_table: Arc::new(Mutex::new(HashMap::new())),
            replacer: Arc::new(Mutex::new(replacer)),
            free_list: Arc::new(Mutex::new(Vec::with_capacity(pool_size))),
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
                let mut replacer = self.replacer.lock().unwrap();
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
        new_page.set_dirty(false);
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
        let mut replacer = self.replacer.lock().unwrap();
        replacer.set_evictable(frame_id, false);
        replacer.record_access(frame_id, Lookup);

        Some(new_page)
    }

    pub fn new_page_guarded(&self, page_id: PageId) -> Option<BasicPageGuard> {
        unimplemented!()
    }

    pub fn fetch_page(&self, page_id: PageId, access_type: AccessType) -> Option<&Page> {
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
                let mut replacer = self.replacer.lock().unwrap();
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
