use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::page::page::Page;
use spin::RwLock;
use std::sync::Arc;
use log::{debug, error, info, warn};

/// BasicPageGuard is a structure that helps manage access to a page in the buffer pool.
pub struct BasicPageGuard {
    bpm: Option<Arc<BufferPoolManager>>,
    page: Option<Arc<RwLock<Page>>>,
    is_dirty: bool,
}

impl BasicPageGuard {
    /// Creates a new `BasicPageGuard`.
    ///
    /// # Parameters
    /// - `bpm`: A reference to the buffer pool manager.
    /// - `page`: A reference to the page.
    ///
    /// # Returns
    /// A new `BasicPageGuard` instance.
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<Page>>) -> Self {
        let page_id = {
            // Acquire a read lock on the page only long enough to get the page ID.
            let page_guard = page.read();
            page_guard.get_page_id()
        };

        info!("Creating new BasicPageGuard for page ID {}", page_id);

        Self {
            bpm: Some(bpm),
            page: Some(page),
            is_dirty: false,
        }
    }

    /// Move assignment for `BasicPageGuard`.
    ///
    /// Similar to a move constructor, except that the move
    /// assignment assumes that `BasicPageGuard` already has a page
    /// being guarded. Think carefully about what should happen when
    /// a guard replaces its held page with a different one, given
    /// the purpose of a page guard.
    pub fn assign(&mut self, mut other: BasicPageGuard) -> &Self {
        if let (Some(bpm), Some(page)) = (&self.bpm, &self.page) {
            let mut page_guard = page.write();
            let page_id = page_guard.get_page_id();
            bpm.unpin_page(page_id, self.is_dirty, AccessType::Unknown);
            page_guard.decrement_pin_count();
            info!("Reassigned BasicPageGuard from page ID {} to page ID {}", page_id, other.get_page_id());
        }

        self.bpm = other.bpm.take();
        self.page = other.page.take();
        self.is_dirty = other.is_dirty;

        self
    }

    /// Upgrades a `BasicPageGuard` to a `ReadPageGuard`.
    ///
    /// The protected page is not evicted from the buffer pool during the upgrade,
    /// and the basic page guard should be made invalid after calling this function.
    ///
    /// # Returns
    /// An upgraded `ReadPageGuard`.
    pub fn upgrade_read(self) -> ReadPageGuard {
        let page_id = self.get_page_id();

        let bpm = self.bpm.clone().expect("BPM should be present");
        let page = self.page.clone().expect("Page should be present");

        // Invalidate the original guard
        std::mem::forget(self);

        info!("Upgraded BasicPageGuard to ReadPageGuard for page ID {}", page_id);
        ReadPageGuard::new(bpm, page)
    }

    /// Upgrades a `BasicPageGuard` to a `WritePageGuard`.
    ///
    /// The protected page is not evicted from the buffer pool during the upgrade,
    /// and the basic page guard should be made invalid after calling this function.
    ///
    /// # Returns
    /// An upgraded `WritePageGuard`.
    pub fn upgrade_write(self) -> WritePageGuard {
        let page_id = self.get_page_id();

        let bpm = self.bpm.clone().expect("BPM should be present");
        let page = self.page.clone().expect("Page should be present");

        // Invalidate the original guard
        std::mem::forget(self);

        info!("Upgraded BasicPageGuard to WritePageGuard for page ID {}", page_id);
        WritePageGuard::new(bpm, page)
    }

    /// Returns the page ID if available, otherwise returns a placeholder or logs an error.
    pub fn get_page_id(&self) -> PageId {
        if let Some(ref page) = self.page {
            let page_guard = page.read();
            return page_guard.get_page_id();
        }
        error!("Attempted to get page ID, but page is None");
        -1
    }

    /// Returns an owned copy of the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        let page = self.page.as_ref().unwrap().read();
        debug!("Fetching data for page ID {}", page.get_page_id());
        Box::new(*page.get_data())
    }

    /// Returns an owned mutable copy of the data and marks the page as dirty.
    pub fn get_data_mut(&mut self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.is_dirty = true;
        let mut page = self.page.as_mut().unwrap().write();
        debug!("Fetching mutable data for page ID {}", page.get_page_id());
        Box::new(*page.get_data_mut())
    }

    /// Returns a reference to the data casted to a specific type.
    ///
    /// # Returns
    /// A reference to the data as a specific type.
    pub fn as_type<T>(&self) -> &T {
        let data = self.get_data();
        unsafe { &*(data.as_ptr() as *const T) }
    }

    /// Returns a mutable reference to the data casted to a specific type and marks the page as dirty.
    ///
    /// # Returns
    /// A mutable reference to the data as a specific type.
    pub fn as_type_mut<T>(&mut self) -> &mut T {
        self.is_dirty = true;
        let mut data = self.get_data_mut();
        unsafe { &mut *(data.as_mut_ptr() as *mut T) }
    }

    fn drop(&mut self) {
        // Skip if the page or bpm is already None (indicating the guard was moved or upgraded)
        if self.page.is_none() || self.bpm.is_none() {
            return;
        }

        let page_id = self.get_page_id();
        info!("Dropping BasicPageGuard for page ID: {}", page_id);

        // Perform unpinning and pin count management only if the page and bpm are valid
        if let (Some(bpm), Some(page)) = (self.bpm.take(), self.page.take()) {
            {
                let mut page_guard = page.write();
                if page_guard.get_pin_count() > 0 {
                    page_guard.decrement_pin_count();
                } else {
                    warn!("Attempted to decrement pin count below 0 for Page ID {}", page_id);
                }
            }

            // Unpin the page from the buffer pool manager
            bpm.unpin_page(page_id, self.is_dirty, AccessType::Unknown);
            info!("Successfully dropped BasicPageGuard for page ID {}", page_id);
        }
    }
}

impl Drop for BasicPageGuard {
    fn drop(&mut self) {
        self.drop()
    }
}

/// ReadPageGuard is a structure that helps manage read-only access to a page in the buffer pool.
pub struct ReadPageGuard {
    guard: BasicPageGuard,
}

impl ReadPageGuard {
    /// Creates a new `ReadPageGuard`.
    ///
    /// # Parameters
    /// - `bpm`: A reference to the buffer pool manager.
    /// - `page`: A reference to the page.
    ///
    /// # Returns
    /// A new `ReadPageGuard` instance.
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<Page>>) -> Self {
        let page_id = page.read().get_page_id();
        info!("Creating new ReadPageGuard for page ID {}", page_id);
        Self {
            guard: BasicPageGuard::new(bpm, page),
        }
    }

    /// Returns the page ID.
    pub fn get_page_id(&self) -> PageId {
        self.guard.get_page_id()
    }

    /// Returns an owned copy of the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data()
    }

    /// Returns a reference to the data casted to a specific type.
    ///
    /// # Returns
    /// A reference to the data as a specific type.
    pub fn as_type<T>(&self) -> &T {
        self.guard.as_type()
    }

    /// Returns a mutable reference to the data casted to a specific type and marks the page as dirty.
    ///
    /// # Returns
    /// A mutable reference to the data as a specific type.
    pub fn as_type_mut<T>(&mut self) -> &mut T {
        self.guard.as_type_mut()
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        self.guard.drop()
    }
}

/// WritePageGuard is a structure that helps manage read-write access to a page in the buffer pool.
pub struct WritePageGuard {
    guard: BasicPageGuard,
}

impl WritePageGuard {
    /// Creates a new `WritePageGuard`.
    ///
    /// # Parameters
    /// - `bpm`: A reference to the buffer pool manager.
    /// - `page`: A reference to the page.
    ///
    /// # Returns
    /// A new `WritePageGuard` instance.
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<Page>>) -> Self {
        let page_id = page.read().get_page_id();
        info!("Creating new WritePageGuard for page ID {}", page_id);
        Self {
            guard: BasicPageGuard::new(bpm, page),
        }
    }

    /// Returns the page ID.
    pub fn get_page_id(&self) -> PageId {
        self.guard.get_page_id()
    }

    /// Returns an owned copy of the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data()
    }

    /// Returns an owned mutable copy of the data.
    pub fn get_data_mut(&mut self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data_mut()
    }

    /// Returns a reference to the data casted to a specific type.
    ///
    /// # Returns
    /// A reference to the data as a specific type.
    pub fn as_type<T>(&self) -> &T {
        self.guard.as_type()
    }

    /// Returns a mutable reference to the data casted to a specific type and marks the page as dirty.
    ///
    /// # Returns
    /// A mutable reference to the data as a specific type.
    pub fn as_type_mut<T>(&mut self) -> &mut T {
        self.guard.is_dirty = true;
        self.guard.as_type_mut()
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        self.guard.drop()
    }
}
