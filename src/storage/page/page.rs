use spin::RwLock;
use std::convert::TryInto;
use log::{debug, info, warn};
use crate::common::config::*;

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
    /// True if the page is dirty, i.e. it is different from its corresponding page on disk.
    is_dirty: bool,
    /// Spin lock to make the Page struct thread-safe.
    latch: RwLock<()>,
}

impl Page {
    /// Constructor. Zeros out the page data.
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: Box::new([0; DB_PAGE_SIZE]),
            page_id,
            pin_count: 1,  // Start with a pin count of 1
            is_dirty: false,
            latch: RwLock::new(()),  // Initialize the spin lock
        };
        page.reset_memory();
        info!("Created new Page with ID {}", page_id);
        page
    }

    /// Returns the actual data contained within this page.
    pub fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        let _read_guard = self.latch.read();  // Acquire a read lock
        debug!("Accessing data for Page ID {} with read lock", self.page_id);
        &self.data
    }

    /// Returns a mutable reference to the actual data contained within this page.
    pub fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        let write_guard = self.latch.write();  // Acquire a write lock
        debug!("Accessing mutable data for Page ID {} with write lock", self.page_id);
        // Return the write guard to access the data
        &mut self.data
    }


    // Method to modify the data
    pub fn set_data(&mut self, new_data: &[u8; DB_PAGE_SIZE]) {
        let mut _write_guard = self.latch.write();  // Acquire a write lock
        self.data.copy_from_slice(new_data);  // Modify the data
    }

    /// Acquire a read lock on the page.
    pub fn read_lock(&self) -> spin::RwLockReadGuard<()> {
        self.latch.read()
    }

    /// Acquire a write lock on the page.
    pub fn write_lock(&self) -> spin::RwLockWriteGuard<()> {
        self.latch.write()
    }

    /// Returns the page id of this page.
    pub fn get_page_id(&self) -> PageId {
        let _read_guard = self.latch.read();  // Acquire a read lock
        self.page_id
    }

    /// Returns the pin count of this page.
    pub fn get_pin_count(&self) -> i32 {
        let _read_guard = self.latch.read();  // Acquire a read lock
        self.pin_count
    }

    /// Sets the pin count of this page.
    pub fn set_pin_count(&mut self, pin_count: i32) {
        let mut _write_guard = self.latch.write();  // Acquire a write lock
        self.pin_count = pin_count;
        debug!("Setting pin count for Page ID {}: {}", self.page_id, pin_count);
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
            warn!("Attempted to decrement pin count below 0 for Page ID {}", self.page_id);
        }
    }

    /// Returns true if the page is dirty.
    pub fn is_dirty(&self) -> bool {
        let _read_guard = self.latch.read();  // Acquire a read lock
        self.is_dirty
    }

    /// Sets the dirty flag of this page.
    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
        info!("Set dirty flag for Page ID {}: {}", self.page_id, is_dirty);
    }

    /// Returns the page LSN.
    pub fn get_lsn(&self) -> Lsn {
        let _read_guard = self.latch.read();  // Acquire a read lock
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 4];
        let lsn = i32::from_ne_bytes(bytes.try_into().unwrap()).into();
        debug!("Fetching LSN for Page ID {}: {}", self.page_id, lsn);
        lsn
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let mut _write_guard = self.latch.write();  // Acquire a write lock
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn_bytes);
        info!("Set LSN for Page ID {}", self.page_id);
    }

    /// Zeroes out the data that is held within the page.
    pub fn reset_memory(&mut self) {
        let mut _write_guard = self.latch.write();  // Acquire a write lock
        self.data.fill(0);
        debug!("Reset memory for Page ID {}", self.page_id);
    }

    /// Acquires a write lock on the page and provides a mutable reference to the page data.
    pub fn with_write_lock<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; DB_PAGE_SIZE]) -> R,
    {
        let mut _write_guard = self.latch.write();  // Acquire a write lock

        // Create a mutable reference to the internal data of the page
        let data: &mut [u8; DB_PAGE_SIZE] = &mut self.data; // Mutable reference to data
        f(data)  // Execute the function with a mutable reference to the page data
    }
}
