use std::sync::{Arc, Mutex};
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::PageId;
use crate::storage::page::page::Page;


/// BasicPageGuard is a structure that helps manage access to a page in the buffer pool.
pub struct BasicPageGuard {
    bpm: Option<Arc<Mutex<BufferPoolManager>>>,
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
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, page: Arc<Mutex<Page>>) -> Self {
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
        let bpm = other.bpm;
        let page = other.page;
        let is_dirty = other.is_dirty;

        // Invalidate the other guard
        std::mem::forget(other);

        Self {
            bpm,
            page,
            is_dirty,
        }
    }

    /// Drops a `BasicPageGuard`.
    ///
    /// Dropping a page guard should clear all contents
    /// (so that the page guard is no longer useful), and
    /// it should tell the BPM that we are done using this page,
    /// per the specification in the writeup.
    pub fn drop(&mut self) {
        if let Some(page) = &self.page {
            let page = page.lock().unwrap();
            if self.is_dirty {
                self.bpm.as_ref().unwrap().lock().unwrap().flush_page(page.get_page_id());
            }
            self.bpm.as_ref().unwrap().lock().unwrap().unpin_page(page.get_page_id(), self.is_dirty, AccessType::Unknown);
        }
        self.page = None;
        self.bpm = None;
        self.is_dirty = false;
    }

    /// Move assignment for `BasicPageGuard`.
    ///
    /// Similar to a move constructor, except that the move
    /// assignment assumes that `BasicPageGuard` already has a page
    /// being guarded. Think carefully about what should happen when
    /// a guard replaces its held page with a different one, given
    /// the purpose of a page guard.
    pub fn assign(&mut self, mut other: BasicPageGuard) -> &Self {
        if let Some(page) = &self.page {
            let page = page.lock().unwrap();
            self.bpm.as_ref().unwrap().lock().unwrap().unpin_page(page.get_page_id(), self.is_dirty, AccessType::Unknown);
        }
        self.bpm = other.bpm;
        self.page = other.page;
        self.is_dirty = other.is_dirty;

        // Invalidate the other guard
        other.page = None;
        other.bpm = None;
        other.is_dirty = false;

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
        let guard = ReadPageGuard::new(self.bpm.clone().unwrap(), self.page.clone().unwrap());
        std::mem::forget(self);
        guard
    }

    /// Upgrades a `BasicPageGuard` to a `WritePageGuard`.
    ///
    /// The protected page is not evicted from the buffer pool during the upgrade,
    /// and the basic page guard should be made invalid after calling this function.
    ///
    /// # Returns
    /// An upgraded `WritePageGuard`.
    pub fn upgrade_write(self) -> WritePageGuard {
        let guard = WritePageGuard::new(self.bpm.clone().unwrap(), self.page.clone().unwrap());
        std::mem::forget(self);
        guard
    }

    /// Returns the page ID.
    pub fn page_id(&self) -> PageId {
        self.page.as_ref().unwrap().lock().unwrap().get_page_id()
    }

    /// Returns a reference to the data.
    pub fn get_data(&self) -> &[u8] {
        self.page.as_ref().unwrap().lock().unwrap().get_data()
    }

    /// Returns a mutable reference to the data and marks the page as dirty.
    pub fn get_data_mut(&mut self) -> &mut [u8] {
        self.is_dirty = true;
        self.page.as_mut().unwrap().lock().unwrap().get_data_mut()
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
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, page: Arc<Mutex<Page>>) -> Self {
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
    pub fn get_data(&self) -> &[u8] {
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
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, page: Arc<Mutex<Page>>) -> Self {
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
    pub fn get_data(&self) -> &[u8] {
        self.guard.get_data()
    }

    /// Returns a mutable reference to the data.
    pub fn get_data_mut(&mut self) -> &mut [u8] {
        self.guard.get_data_mut()
    }
}
