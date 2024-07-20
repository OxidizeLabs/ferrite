use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};
use config::LRUK_REPLACER_K;
use disk_manager::DiskManager;
use lru_k_replacer::LRUKReplacer;
use page::Page;

type PageId = i32;
type FrameId = usize;
const INVALID_PAGE_ID: PageId = -1;

struct LogManager {}
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

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>, replacer_k: usize, log_manager: Option<Arc<LogManager>>) -> Self {
        /**
         * @brief Creates a new BufferPoolManager.
         * @param pool_size the size of the buffer pool
         * @param disk_manager the disk manager
         * @param replacer_k the LookBack constant k for the LRU-K replacer
         * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
         */

        let page_ = Page::new(0);
        let replacer_ = LRUKReplacer::new(0, replacer_k);
        Self {
            pool_size,
            next_page_id: AtomicI32::new(0),
            pages: Vec::with_capacity(pool_size),
            page_table: Arc::new(Mutex::new(HashMap::new())),
            replacer: Arc::new(Mutex::new(replacer_)),
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
        /**
         * TODO(P1): Add implementation
         *
         * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
         * are currently in use and not evictable (in another word, pinned).
         *
         * You should pick the replacement frame from either the free list or the replacer (always find from the free list
         * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
         * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
         *
         * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
         * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
         * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
         *
         * @param[out] page_id id of created page
         * @return nullptr if no new pages could be created, otherwise pointer to new page
         */

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
