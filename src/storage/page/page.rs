use std::convert::TryInto;
use crate::common::config::*;

// Constants
const OFFSET_PAGE_START: usize = 0;
const OFFSET_LSN: usize = 4;

/// Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
/// held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
/// pin count, dirty flag, page id, etc.
#[derive(Debug, Clone)]
pub struct Page {
    /// The actual data that is stored within a page.
    data: Box<[u8; DB_PAGE_SIZE]>,
    /// The ID of this page.
    page_id: PageId,
    /// The pin count of this page.
    pin_count: i32,
    /// True if the page is dirty, i.e. it is different from its corresponding page on disk.
    is_dirty: bool,
}

impl Page {
    /// Constructor. Zeros out the page data.
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: Box::new([0; DB_PAGE_SIZE]),
            page_id,
            pin_count: 1,
            is_dirty: false,
        };
        page.reset_memory();
        page
    }

    /// Returns the actual data contained within this page.
    pub fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        &self.data
    }

    /// Returns a mutable reference to the actual data contained within this page.
    pub fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        &mut self.data
    }

    /// Returns the page id of this page.
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns the pin count of this page.
    pub fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    /// Sets the pin count of this page.
    pub fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    /// Increments the pin count of this page.
    pub fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }

    /// Decrements the pin count of this page.
    pub fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    /// Returns true if the page is dirty.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Sets the dirty flag of this page.
    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    /// Returns the page LSN.
    pub fn get_lsn(&self) -> Lsn {
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 4];
        i32::from_ne_bytes(bytes.try_into().unwrap())
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn_bytes);
    }

    /// Zeroes out the data that is held within the page.
    pub fn reset_memory(&mut self) {
        self.data.fill(0);
    }
}
