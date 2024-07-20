use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};
use config::LRUK_REPLACER_K;
use disk_manager::DiskManager;
use page::Page;

type PageId = i32;
type FrameId = usize;
const INVALID_PAGE_ID: PageId = -1;

struct LogManager {}
struct LRUKReplacer {}
struct WriteBackCache {}
struct BasicPageGuard {}
struct ReadPageGuard {}
struct WritePageGuard {}

#[derive(Clone, Copy)]
enum AccessType {
    Unknown,
}

pub struct BufferPoolManager {
    pool_size: usize,
    next_page_id: AtomicI32,
    pages: Vec<Page>,
    page_table: Arc<Mutex<HashMap<PageId, FrameId>>>,
    replacer: Arc<Mutex<LRUKReplacer>>,
    free_list: Arc<Mutex<VecDeque<FrameId>>>,
    latch: Arc<Mutex<()>>,
    disk_manager: Arc<DiskManager>,
    log_manager: Option<Arc<LogManager>>,
    write_back_cache: Option<Arc<WriteBackCache>>,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>, replacer_k: usize, log_manager: Option<Arc<LogManager>>) -> Self {
        Self {
            pool_size,
            next_page_id: AtomicI32::new(0),
            pages: vec![Page::new(0); pool_size],
            page_table: Arc::new(Mutex::new(HashMap::new())),
            replacer: Arc::new(Mutex::new(LRUKReplacer {})),
            free_list: Arc::new(Mutex::new(VecDeque::new())),
            latch: Arc::new(Mutex::new(())),
            disk_manager,
            log_manager,
            write_back_cache: None,
        }
    }

    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn get_pages(&self) -> &Vec<Page> {
        &self.pages
    }

    pub fn new_page(&mut self, page_id: PageId) -> &Page {

        // get replacement frame from free list then/or replacer


        // get page_id from calling allocate_page()
        unimplemented!()

    }

    pub fn new_page_guarded(&self, page_id: PageId) -> Option<BasicPageGuard> {
        // Implementation here
        None
    }

    pub fn fetch_page(&self, page_id: PageId, access_type: AccessType) -> Option<&Page> {
        // Implementation here
        None
    }

    pub fn fetch_page_basic(&self, page_id: PageId) -> Option<BasicPageGuard> {
        // Implementation here
        None
    }

    pub fn fetch_page_read(&self, page_id: PageId) -> Option<ReadPageGuard> {
        // Implementation here
        None
    }

    pub fn fetch_page_write(&self, page_id: PageId) -> Option<WritePageGuard> {
        // Implementation here
        None
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        // Implementation here
        false
    }

    pub fn flush_page(&self, page_id: PageId) -> bool {
        // Implementation here
        false
    }

    pub fn flush_all_pages(&self) {
        // Implementation here
    }

    pub fn delete_page(&self, page_id: PageId) -> bool {
        // Implementation here
        false
    }

    fn allocate_page(&self) -> PageId {
        // Implementation here
        INVALID_PAGE_ID
    }

    fn deallocate_page(&self, page_id: PageId) {
        // Implementation here
    }
}

// fn main() {
//     // Example usage of BufferPoolManager
//     let disk_manager = Arc::new(DiskManager {});
//     let log_manager = Arc::new(LogManager {});
//     let buffer_pool_manager = BufferPoolManager::new(100, disk_manager, 2, Some(log_manager));
//
//     // Further code...
// }
