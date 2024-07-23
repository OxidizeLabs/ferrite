use std::convert::TryInto;

use crate::config::*;

// Constants
const OFFSET_PAGE_START: usize = 0;
const OFFSET_LSN: usize = 4;

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */
#[derive(Debug, Clone)]
pub struct Page {
    /** The actual data that is stored within a page. */
    // Usually this should be stored as `char data_[DB_PAGE_SIZE]{};`. But to enable ASAN to detect page overflow,
    // we store it as a ptr.
    data: Box<[u8; DB_PAGE_SIZE]>,
    /** The ID of this page. */
    page_id: PageId,
    /** The pin count of this page. */
    pin_count: i32,
    /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
    is_dirty: bool,
}

impl Page {
    ///
    ///
    /// # Arguments
    ///
    /// * `page_id`:
    ///
    /// returns: Page
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    /** Constructor. Zeros out the page data. */
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: Box::new([0; DB_PAGE_SIZE]),
            page_id,
            pin_count: 0,
            is_dirty: false,
            //rwlatch: ReaderWriterLatch::new(),
        };
        page.reset_memory();
        page
    }

    /** @return the actual data contained within this page */
    pub fn get_data(&self) -> [u8; DB_PAGE_SIZE] {
        *self.data
    }

    /** @return the page id of this page */
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /** @return the pin count of this page */
    pub fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    /** @return true if the page in memory has been modified from the page on disk, false otherwise */
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    // /** Acquire the page write latch. */
    // pub fn w_latch(&self) -> RwLockWriteGuard<()> {
    //     self.rwlatch.w_lock()
    // }
    //
    // /** Release the page write latch. */
    // pub fn w_unlatch(&self, guard: RwLockWriteGuard<()>) {
    //     self.rwlatch.w_unlock(guard)
    // }
    //
    // /** Acquire the page read latch. */
    // pub fn r_latch(&self) -> RwLockReadGuard<()> {
    //     self.rwlatch.r_lock()
    // }
    //
    // /** Release the page read latch. */
    // pub fn r_unlatch(&self, guard: RwLockReadGuard<()>) {
    //     self.rwlatch.r_unlock(guard)
    // }

    /** @return the page LSN. */
    pub fn get_lsn(&self) -> Lsn {
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 4];
        i32::from_ne_bytes(bytes.try_into().unwrap())
    }

    /** Sets the page LSN. */
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn_bytes);
    }

    /** Zeroes out the data that is held within the page. */
    pub(crate) fn reset_memory(&mut self) {
        for byte in self.data.iter_mut() {
            *byte = OFFSET_PAGE_START as u8;
        }
    }
}
