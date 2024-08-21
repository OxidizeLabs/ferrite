use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::page::page::{Page, PageType};
use spin::RwLock;
use std::sync::Arc;
use log::{debug, error, info, warn};

pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<PageType>>,
    page_id: PageId,
}

impl PageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<PageType>>, page_id: PageId) -> Self {
        Self { bpm, page, page_id }
    }

    pub fn read(&self) -> spin::RwLockReadGuard<'_, PageType> {
        self.page.read()
    }

    pub fn write(&self) -> spin::RwLockWriteGuard<'_, PageType> {
        self.page.write()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    // Method to convert the guard into a specific page type
    pub fn into_specific_type<T, F>(self, converter: F) -> Option<T>
    where
        F: FnOnce(PageType) -> Option<T>,
    {
        let page_type = Arc::try_unwrap(self.page.clone())
            .ok()
            .and_then(|rwlock| Option::from(rwlock.into_inner()));

        page_type.and_then(converter)
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        // Unpin the page when the guard is dropped
        self.bpm.unpin_page(self.page_id, self.page.read().as_page_trait().is_dirty(), AccessType::Unknown);
    }
}