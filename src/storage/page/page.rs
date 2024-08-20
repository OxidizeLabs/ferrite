use crate::common::config::*;
use log::{debug, error, info, warn};
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::convert::TryInto;

// Constants
const OFFSET_PAGE_START: usize = 0;
const OFFSET_LSN: usize = 4;

/// Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
/// held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
/// pin count, dirty flag, page id, etc.
#[derive(Debug)]
pub struct Page {
    /// The actual data that is stored within a page.
    data: Box<[u8; DB_PAGE_SIZE]>,
    /// The ID of this page.
    page_id: PageId,
    /// The pin count of this page.
    pin_count: i32,
    /// True if the page is dirty.
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
        info!("Created new Page with ID {}", page_id);
        page
    }

    /// Returns an immutable reference to the data. The data is protected by an internal read lock.
    pub fn get_data(&self) -> Box<[u8; DB_PAGE_SIZE]> {
        debug!("Fetching data for page ID {}", self.page_id);
        // Return a copy of the data.
        Box::new(*self.data)
    }

    /// Returns a mutable reference to the data, marks the page as dirty, and returns a copy.
    pub fn get_data_mut(&mut self) -> Box<[u8; DB_PAGE_SIZE]> {
        debug!("Fetching mutable data for page ID {}", self.page_id);

        // Return a copy of the mutable data.
        Box::new(*self.data)
    }

    /// Returns the page id of this page.
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns the pin count of this page.
    pub fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    /// Returns the page LSN.
    pub fn get_lsn(&self) -> Lsn {
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 4];
        let lsn = i32::from_ne_bytes(bytes.try_into().unwrap()).into();
        debug!("Fetching LSN for Page ID {}: {}", self.page_id, lsn);
        lsn
    }

    /// Returns true if the page is dirty.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Method to modify the data
    pub fn set_data(&mut self, new_data: &[u8; DB_PAGE_SIZE]) {
        debug!("Modifying data for Page ID {}", self.page_id);
        self.is_dirty = true;
        // Copy the contents of `new_data` into `self.data`
        self.data.copy_from_slice(new_data);
    }

    /// Sets the pin count of this page.
    pub fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
        debug!("Setting pin count for Page ID {}: {}", self.page_id, pin_count);
    }

    /// Sets the dirty flag of this page.
    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
        info!("Set dirty flag for Page ID {}: {}", self.page_id, is_dirty);
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn_bytes);
        info!("Set LSN for Page ID {}", self.page_id);
    }

    /// Increments the pin count of this page.
    pub fn increment_pin_count(&mut self) {
        self.pin_count += 1;
        debug!("Incremented pin count for Page ID {}: {}", self.page_id, self.pin_count);
    }

    /// Decrements the pin count of this page.
    pub fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
            debug!("Decremented pin count for Page ID {}: {}", self.page_id, self.pin_count);
        } else {
            error!("Attempted to decrement pin count below 0 for Page ID {}", self.page_id);
        }
    }

    /// Zeroes out the data that is held within the page.
    pub fn reset_memory(&mut self) {
        self.data.fill(0);
        debug!("Reset memory for Page ID {}", self.page_id);
    }
}
