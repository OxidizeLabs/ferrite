use crate::common::config::*;
use crate::common::exception::PageError;
use crate::storage::page::page::PageType::{
    Basic, ExtendedHashTableBucket, ExtendedHashTableDirectory, ExtendedHashTableHeader, Table,
};
use crate::storage::page::page_types::extendable_hash_table_bucket_page::TypeErasedBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::storage::page::page_types::table_page::TablePage;
use log::{debug, error, info};
use std::any::Any;

// Constants
const OFFSET_LSN: usize = 4;

#[derive(Debug)]
pub enum PageType {
    Basic(Page),
    Table(TablePage),
    ExtendedHashTableDirectory(ExtendableHTableDirectoryPage),
    ExtendedHashTableHeader(ExtendableHTableHeaderPage),
    ExtendedHashTableBucket(TypeErasedBucketPage),
}

/// Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
/// held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
/// pin count, dirty flag, page id, etc.
#[derive(Debug, Clone)]
pub struct Page {
    /// The actual data that is stored within a page.
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    /// The ID of this page.
    page_id: PageId,
    /// The pin count of this page.
    pin_count: i32,
    /// True if the page is dirty.
    is_dirty: bool,
}

pub trait PageTrait {
    fn get_page_id(&self) -> PageId;
    fn is_dirty(&self) -> bool;
    fn set_dirty(&mut self, is_dirty: bool);
    fn get_pin_count(&self) -> i32;
    fn increment_pin_count(&mut self);
    fn decrement_pin_count(&mut self);
    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize];
    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize];
    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError>;
    fn set_pin_count(&mut self, pin_count: i32);
    fn reset_memory(&mut self);
}

pub trait AsAny {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl Page {
    /// Constructor. Zeros out the page data.
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
        };
        page.reset_memory();
        info!("Created new Page with ID {}", page_id);
        page
    }

    /// Returns the page LSN.
    pub fn get_lsn(&self) -> Lsn {
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 8];
        let lsn = u64::from_ne_bytes(bytes.try_into().unwrap()).into();
        debug!("Fetching LSN for Page ID {}: {}", self.page_id, lsn);
        lsn
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 8].copy_from_slice(&lsn_bytes);
        info!("Set LSN for Page ID {}", self.page_id);
    }
}

impl PageTrait for Page {
    /// Returns the page id of this page.
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns true if the page is dirty.
    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Sets the dirty flag of this page.
    fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
        info!("Set dirty flag for Page ID {}: {}", self.page_id, is_dirty);
    }

    /// Returns the pin count of this page.
    fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    /// Increments the pin count of this page.
    fn increment_pin_count(&mut self) {
        self.pin_count += 1;
        debug!(
            "Incremented pin count for Page ID {}: {}",
            self.page_id, self.pin_count
        );
    }

    /// Decrements the pin count of this page.
    fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
            debug!(
                "Decremented pin count for Page ID {}: {}",
                self.page_id, self.pin_count
            );
        } else {
            error!(
                "Attempted to decrement pin count below 0 for Page ID {}",
                self.page_id
            );
        }
    }

    /// Returns an immutable reference to the data. The data is protected by an internal read lock.
    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        debug!("Fetching data for page ID {}", self.page_id);
        // Return a copy of the data.
        &*self.data
    }

    /// Returns a mutable reference to the data, marks the page as dirty, and returns a copy.
    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        debug!("Fetching mutable data for page ID {}", self.page_id);

        // Return a copy of the mutable data.
        &mut *self.data
    }

    /// Method to modify the data
    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        debug!(
            "Attempting to modify data for Page ID {} at offset {}",
            self.page_id, offset
        );

        if offset >= self.data.len() {
            return Err(PageError::InvalidOffset {
                offset,
                page_size: self.data.len(),
            });
        }

        let remaining_space = self.data.len() - offset;
        if new_data.len() > remaining_space {
            return Err(PageError::DataTooLarge {
                data_size: new_data.len(),
                remaining_space,
            });
        }

        // Copy the contents of `new_data` into `self.data` starting at the specified offset
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        self.is_dirty = true;

        debug!(
            "Successfully modified data for Page ID {} at offset {}",
            self.page_id, offset
        );
        Ok(())
    }

    /// Sets the pin count of this page.
    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
        debug!(
            "Setting pin count for Page ID {}: {}",
            self.page_id, pin_count
        );
    }

    /// Zeroes out the data that is held within the page.
    fn reset_memory(&mut self) {
        self.data.fill(0);
        debug!("Reset memory for Page ID {}", self.page_id);
    }
}

impl PageType {
    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE as usize] {
        match self {
            Basic(page) => unimplemented!(),
            ExtendedHashTableDirectory(page) => page.serialize(),
            ExtendedHashTableHeader(page) => unimplemented!(),
            ExtendedHashTableBucket(page) => unimplemented!(),
            Table(page) => unimplemented!(),
        }
    }

    pub fn deserialize(&mut self, buffer: &[u8; DB_PAGE_SIZE as usize]) {
        match self {
            Basic(page) => unimplemented!(),
            ExtendedHashTableDirectory(page) => page.deserialize(buffer),
            ExtendedHashTableHeader(page) => unimplemented!(),
            ExtendedHashTableBucket(page) => unimplemented!(),
            Table(page) => unimplemented!(),
        }
    }

    pub fn as_page_trait(&self) -> &dyn PageTrait {
        match self {
            Basic(page) => page,
            ExtendedHashTableDirectory(page) => page,
            ExtendedHashTableHeader(page) => page,
            ExtendedHashTableBucket(page) => page,
            Table(page) => page,
        }
    }

    pub fn as_page_trait_mut(&mut self) -> &mut dyn PageTrait {
        match self {
            Basic(page) => page,
            ExtendedHashTableDirectory(page) => page,
            ExtendedHashTableHeader(page) => page,
            ExtendedHashTableBucket(page) => page,
            Table(page) => page,
        }
    }

    pub fn as_any(&self) -> &dyn Any {
        match self {
            Basic(page) => page as &dyn Any,
            ExtendedHashTableDirectory(page) => page as &dyn Any,
            ExtendedHashTableHeader(page) => page as &dyn Any,
            ExtendedHashTableBucket(page) => page as &dyn Any,
            Table(page) => page as &dyn Any,
        }
    }

    pub fn as_any_mut(&mut self) -> &mut dyn Any {
        match self {
            Basic(ref mut page) => page as &mut dyn Any,
            ExtendedHashTableDirectory(ref mut page) => page as &mut dyn Any,
            ExtendedHashTableHeader(ref mut page) => page as &mut dyn Any,
            ExtendedHashTableBucket(ref mut page) => page as &mut dyn Any,
            Table(ref mut page) => page as &mut dyn Any,
        }
    }
}

impl From<ExtendableHTableDirectoryPage> for PageType {
    fn from(page: ExtendableHTableDirectoryPage) -> Self {
        ExtendedHashTableDirectory(page)
    }
}

impl From<ExtendableHTableHeaderPage> for PageType {
    fn from(page: ExtendableHTableHeaderPage) -> Self {
        ExtendedHashTableHeader(page)
    }
}

impl From<TypeErasedBucketPage> for PageType {
    fn from(page: TypeErasedBucketPage) -> Self {
        ExtendedHashTableBucket(page)
    }
}

// Unit Tests
#[cfg(test)]
mod unit_tests {
    use crate::common::config::{Lsn, DB_PAGE_SIZE};
    use crate::storage::page::page::{Page, PageTrait};

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.get_pin_count(), 1);
        assert!(!page.is_dirty());
    }

    #[test]
    fn test_page_data_access() {
        let mut page = Page::new(2);
        let mut data = [0u8; DB_PAGE_SIZE as usize];
        data[0] = 42;
        // Use the set_data method with the offset to update the page data
        if let Err(e) = page.set_data(0, &data) {
            panic!("Error setting data: {:?}", e);
        }
        assert_eq!(page.get_data().get(0), Some(42).as_ref());
    }

    #[test]
    fn test_set_and_get_lsn() {
        let mut page = Page::new(3);
        let lsn: Lsn = 1234;
        page.set_lsn(lsn);
        assert_eq!(page.get_lsn(), lsn);
    }
}

// Basic Behavior
#[cfg(test)]
mod basic_behavior {
    use crate::common::config::DB_PAGE_SIZE;
    use crate::storage::page::page::{Page, PageTrait};

    #[test]
    fn test_increment_and_decrement_pin_count() {
        let mut page = Page::new(1);
        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 2);
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 1);
    }

    #[test]
    fn test_setting_dirty_flag() {
        let mut page = Page::new(1);
        assert!(!page.is_dirty());
        page.set_dirty(true);
        assert!(page.is_dirty());
    }

    #[test]
    fn test_reset_memory() {
        let mut page = Page::new(1);
        let mut data = [0u8; DB_PAGE_SIZE as usize];
        data[0] = 255;
        if let Err(e) = page.set_data(0, &data) {
            panic!("Error setting data: {:?}", e);
        }

        // Ensure data is modified before reset
        assert_eq!(page.get_data()[0], 255);

        // Reset the memory
        page.reset_memory();
        assert_eq!(page.get_data()[0], 0);
    }
}

// Concurrency
#[cfg(test)]
mod concurrency {
    use crate::common::config::DB_PAGE_SIZE;
    use crate::storage::page::page::{Page, PageTrait};
    use spin::{Barrier, RwLock};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn concurrent_read_and_write() {
        let page = Arc::new(RwLock::new(Page::new(1))); // Wrap Page in Arc<RwLock>.
        let start_barrier = Arc::new(Barrier::new(4)); // Used to start all threads simultaneously.
        let end_barrier = Arc::new(Barrier::new(4)); // Used to ensure writers finish before readers start.
        let mut handles = vec![];

        // Writer threads
        for i in 0..2 {
            let page = Arc::clone(&page);
            let start_barrier = Arc::clone(&start_barrier);
            let end_barrier = Arc::clone(&end_barrier);
            handles.push(thread::spawn(move || {
                start_barrier.wait(); // Wait until all threads are ready to start.

                // Perform the write operation.
                {
                    let mut page = page.write(); // Acquire write lock on the entire Page.
                    let mut data = [0u8; DB_PAGE_SIZE as usize];
                    data[0] = (i + 1) as u8;
                    if let Err(e) = page.set_data(0, &data) {
                        panic!("Error setting data: {:?}", e);
                    }
                }

                end_barrier.wait(); // Signal that the writer has finished.
            }));
        }

        // Reader threads
        for _ in 0..2 {
            let page = Arc::clone(&page);
            let start_barrier = Arc::clone(&start_barrier);
            let end_barrier = Arc::clone(&end_barrier);

            handles.push(thread::spawn(move || {
                start_barrier.wait(); // Wait until all threads are ready to start.

                // Ensure writers are finished before reading.
                end_barrier.wait();

                // Perform the read operation.
                let page = page.read(); // Acquire read lock on the entire Page.
                let data = page.get_data(); // Safely access immutable data.
                assert!(
                    data[0] == 1 || data[0] == 2,
                    "Unexpected values in the data"
                );
            }));
        }

        // Wait for all threads to finish.
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify the final content of the page after all operations.
        let page = page.read(); // Acquire read lock on the entire Page.
        let data = page.get_data();
        assert!(
            data[0] == 1 || data[0] == 2,
            "Unexpected values in the data after concurrent operations"
        );
    }

    #[test]
    fn multiple_concurrent_reads() {
        // Wrap the Page in Arc<RwLock> to manage thread-safe access.
        let page = Arc::new(RwLock::new(Page::new(1)));
        let barrier = Arc::new(Barrier::new(4)); // Barrier for synchronizing thread starts.
        let mut handles = vec![];

        // Launch multiple reader threads.
        for _ in 0..3 {
            let page = Arc::clone(&page);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait(); // Synchronize start.

                // Acquire read lock on the Page.
                let page = page.read();

                // Perform the read operation.
                let data = page.get_data();
                assert_eq!(data[0], 0); // Since no write happened, data should be zeroed.
            }));
        }

        barrier.wait(); // Trigger all threads to start.

        // Wait for all threads to finish.
        for handle in handles {
            handle.join().unwrap();
        }
    }
}

// Edge Cases
#[cfg(test)]
mod edge_cases {
    use crate::common::config::Lsn;
    use crate::storage::page::page::{Page, PageTrait};

    #[test]
    fn test_pin_count_underflow_protection() {
        let mut page = Page::new(1);
        page.decrement_pin_count(); // Pin count goes to 0
        assert_eq!(page.get_pin_count(), 0);

        // Attempting to decrement below 0 should be prevented
        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn test_page_with_large_lsn_value() {
        let mut page = Page::new(1);
        let large_lsn: Lsn = u64::MAX.into();
        page.set_lsn(large_lsn);
        assert_eq!(page.get_lsn(), large_lsn);
    }

    #[test]
    fn test_page_reset_memory_after_dirty() {
        let mut page = Page::new(1);
        let mut data = *page.get_data_mut();
        data[0] = 255;
        if let Err(e) = page.set_data(0, &data) {
            panic!("Error setting data: {:?}", e);
        }
        assert!(page.is_dirty());

        // Reset memory and verify it's no longer dirty
        page.reset_memory();
        page.set_dirty(false);
        assert!(!page.is_dirty());
        assert_eq!(page.get_data()[0], 0);
    }
}
