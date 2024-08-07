use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::page::page::Page;
use spin::Mutex;
use std::sync::Arc;

/// BasicPageGuard is a structure that helps manage access to a page in the buffer pool.
#[derive(Clone)]
pub struct BasicPageGuard {
    bpm: Option<Arc<BufferPoolManager>>,
    page: Option<Arc<Mutex<Page>>>,
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
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<Mutex<Page>>) -> Self {
        Self {
            bpm: Some(bpm),
            page: Some(page),
            is_dirty: false,
        }
    }

    /// Move constructor for `BasicPageGuard`.
    ///
    /// When you call `BasicPageGuard::new(std::move(other_guard))`, you
    /// expect that the new guard will behave exactly like the other
    /// one. In addition, the old page guard should not be usable.
    /// For example, it should not be possible to call `.drop()` on both page
    /// guards and have the pin count decrease by 2.
    pub fn from(other: BasicPageGuard) -> Self {
        other.clone()
    }

    /// Drops a `BasicPageGuard`.
    ///
    /// Dropping a page guard should clear all contents
    /// (so that the page guard is no longer useful), and
    /// it should tell the BPM that we are done using this page,
    /// per the specification in the writeup.
    pub fn drop(&mut self) {
        if let (Some(bpm), Some(page)) = (&self.bpm, &self.page) {
            let mut page_guard = page.lock();
            page_guard.decrement_pin_count();
            bpm.unpin_page(page_guard.get_page_id(), self.is_dirty, AccessType::Unknown);
        }
        self.bpm = None;
        self.page = None;
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
            let mut page_guard = page.lock();
            bpm.unpin_page(page_guard.get_page_id(), self.is_dirty, AccessType::Unknown);
            page_guard.decrement_pin_count();
        }

        self.bpm = other.bpm.take();
        self.page = other.page.take();
        self.is_dirty = other.is_dirty;

        self
    }

    /// Destructor for `BasicPageGuard`.
    ///
    /// When a page guard goes out of scope, it should behave as if
    /// the page guard was dropped.
    pub fn drop_guard(&mut self) {
        self.drop();
    }

    /// Upgrades a `BasicPageGuard` to a `ReadPageGuard`.
    ///
    /// The protected page is not evicted from the buffer pool during the upgrade,
    /// and the basic page guard should be made invalid after calling this function.
    ///
    /// # Returns
    /// An upgraded `ReadPageGuard`.
    pub fn upgrade_read(self) -> ReadPageGuard {
        let bpm = self.bpm.clone().unwrap();
        let page = self.page.clone().unwrap();

        // Invalidate the BasicPageGuard by setting its fields to None
        let mut invalidated_guard = self;
        invalidated_guard.bpm = None;
        invalidated_guard.page = None;

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
        let bpm = self.bpm.clone().unwrap();
        let page = self.page.clone().unwrap();

        // Invalidate the BasicPageGuard by setting its fields to None
        let mut invalidated_guard = self;
        invalidated_guard.bpm = None;
        invalidated_guard.page = None;

        WritePageGuard::new(bpm, page)
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.page.clone().unwrap().lock().get_page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        let page = self.page.as_ref().unwrap().lock();
        Box::new(*page.get_data())
    }

    /// Returns a mutable reference to the data and marks the page as dirty.
    pub fn get_data_mut(&mut self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.is_dirty = true;
        let mut page = self.page.as_mut().unwrap().lock();
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
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<Mutex<Page>>) -> Self {
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
        Self {
            guard: BasicPageGuard::from(other.guard),
        }
    }

    /// Move assignment for `ReadPageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. Given another `ReadPageGuard`,
    /// replace the contents of this one with that one.
    pub fn assign(&mut self, other: ReadPageGuard) -> &Self {
        self.guard.assign(other.guard);
        self
    }

    /// Drops a `ReadPageGuard`.
    ///
    /// `ReadPageGuard`'s Drop should behave similarly to `BasicPageGuard`,
    /// except that `ReadPageGuard` has an additional resource - the latch!
    /// However, you should think VERY carefully about in which order you
    /// want to release these resources.
    pub fn drop_guard(&mut self) {
        self.guard.drop();
    }

    /// Destructor for `ReadPageGuard`.
    ///
    /// Just like with `BasicPageGuard`, this should behave
    /// as if you were dropping the guard.
    pub fn drop(&mut self) {
        self.drop_guard();
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.guard.page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        self.guard.get_data()
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
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<Mutex<Page>>) -> Self {
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
        Self {
            guard: BasicPageGuard::from(other.guard),
        }
    }

    /// Move assignment for `WritePageGuard`.
    ///
    /// Very similar to `BasicPageGuard`. Given another `WritePageGuard`,
    /// replace the contents of this one with that one.
    pub fn assign(&mut self, other: WritePageGuard) -> &Self {
        self.guard.assign(other.guard);
        self
    }

    /// Drops a `WritePageGuard`.
    ///
    /// `WritePageGuard`'s Drop should behave similarly to `BasicPageGuard`,
    /// except that `WritePageGuard` has an additional resource - the latch!
    /// However, you should think VERY carefully about in which order you
    /// want to release these resources.
    pub fn drop_guard(&mut self) {
        self.guard.drop();
    }

    /// Destructor for `WritePageGuard`.
    ///
    /// Just like with `BasicPageGuard`, this should behave
    /// as if you were dropping the guard.
    pub fn drop(&mut self) {
        self.drop_guard();
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
}
