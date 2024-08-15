use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::page::page::Page;
use spin::RwLock;
use std::sync::Arc;
use log::{debug, info};

/// BasicPageGuard is a structure that helps manage access to a page in the buffer pool.
#[derive(Clone)]
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
        let page_id = page.read().get_page_id();
        info!("Creating new BasicPageGuard for page ID {}", page_id);
        Self {
            bpm: Some(bpm),
            page: Some(page),
            is_dirty: false,
        }
    }

    /// Drops a `BasicPageGuard`.
    ///
    /// Dropping a page guard should clear all contents
    /// (so that the page guard is no longer useful), and
    /// it should tell the BPM that we are done using this page,
    /// per the specification in the writeup.
    fn drop_guard(&mut self) {
        if let (Some(bpm), Some(page)) = (self.bpm.take(), self.page.take()) {
            let mut page_guard = page.write();
            let page_id = page_guard.get_page_id();
            page_guard.decrement_pin_count();
            bpm.unpin_page(page_id, self.is_dirty, AccessType::Unknown);
            info!("Dropped BasicPageGuard for page ID {}", page_id);
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
            info!("Reassigned BasicPageGuard from page ID {} to page ID {}", page_id, other.page_id());
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
        let page_id = self.page_id();
        let bpm = self.bpm.clone().expect("BPM should be present");
        let page = self.page.clone().expect("Page should be present");

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
        let page_id = self.page_id();
        let bpm = self.bpm.clone().expect("BPM should be present");
        let page = self.page.clone().expect("Page should be present");

        info!("Upgraded BasicPageGuard to WritePageGuard for page ID {}", page_id);
        WritePageGuard::new(bpm, page)
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        let page = self.page.as_ref().unwrap().read();
        page.get_page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        let page = self.page.as_ref().unwrap().read();
        debug!("Fetching data for page ID {}", page.get_page_id());
        Box::new(*page.get_data())
    }

    /// Returns a mutable reference to the data and marks the page as dirty.
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
}

impl Drop for BasicPageGuard {
    fn drop(&mut self) {
        if let Some(bpm) = self.bpm.take() {
            if let Some(page) = self.page.take() {
                let mut page_guard = page.write();
                let page_id = page_guard.get_page_id();
                page_guard.decrement_pin_count();
                bpm.unpin_page(page_id, self.is_dirty, AccessType::Unknown);
                info!("Dropped BasicPageGuard for page ID {}", page_id);
            }
        }
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

    /// Move constructor for `ReadPageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. You want to create
    /// a `ReadPageGuard` using another `ReadPageGuard`. Think
    /// about if there's any way you can make this easier for yourself...
    pub fn from(other: ReadPageGuard) -> Self {
        info!("Moving ReadPageGuard for page ID {}", other.page_id());
        Self {
            guard: BasicPageGuard::from(other.guard.clone()),
        }
    }

    /// Move assignment for `ReadPageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. Given another `ReadPageGuard`,
    /// replace the contents of this one with that one.
    pub fn assign(&mut self, other: ReadPageGuard) -> &Self {
        info!("Assigning ReadPageGuard for page ID {}", other.page_id());
        self.guard.assign(other.guard.clone());
        self
    }

    /// Destructor for `ReadPageGuard`.
    ///
    /// Just like with `BasicPageGuard`, this should behave
    /// as if you were dropping the guard.
    pub fn drop(&mut self) {
        self.guard.drop_guard();
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.guard.page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data()
    }


    /// Returns a mutable reference to the data casted to a specific type and marks the page as dirty.
    ///
    /// # Returns
    /// A mutable reference to the data as a specific type.
    pub fn as_type_mut<T>(&mut self) -> &mut T {
        self.guard.is_dirty = true;
        let mut data = self.guard.get_data_mut();
        unsafe { &mut *(data.as_mut_ptr() as *mut T) }
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        self.guard.drop_guard();
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

    /// Move constructor for `WritePageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. You want to create
    /// a `WritePageGuard` using another `WritePageGuard`. Think
    /// about if there's any way you can make this easier for yourself...
    pub fn from(other: WritePageGuard) -> Self {
        info!("Moving WritePageGuard for page ID {}", other.page_id());
        Self {
            guard: BasicPageGuard::from(other.guard.clone()),
        }
    }

    /// Move assignment for `WritePageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. Given another `WritePageGuard`,
    /// replace the contents of this one with that one.
    pub fn assign(&mut self, other: WritePageGuard) -> &Self {
        info!("Assigning WritePageGuard for page ID {}", other.page_id());
        self.guard.assign(other.guard.clone());
        self
    }

    /// Destructor for `WritePageGuard`.
    ///
    /// Just like with `BasicPageGuard`, this should behave
    /// as if you were dropping the guard.
    pub fn drop(&mut self) {
        self.guard.drop_guard();
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.guard.page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data()
    }

    /// Returns a mutable reference to the data.
    pub fn get_data_mut(&mut self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data_mut()
    }

    /// Returns a mutable reference to the data casted to a specific type and marks the page as dirty.
    ///
    /// # Returns
    /// A mutable reference to the data as a specific type.
    pub fn as_type_mut<T>(&mut self) -> &mut T {
        self.guard.is_dirty = true;
        let mut data = self.get_data_mut();
        unsafe { &mut *(data.as_mut_ptr() as *mut T) }
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        self.guard.drop_guard();
    }
}