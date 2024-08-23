use std::any::Any;
use crate::common::config::*;
use log::{debug, error, info, warn};
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::convert::TryInto;
use crate::common::exception::PageError;
use crate::storage::page::page::PageType::{Basic, ExtendedHashTableBucket, ExtendedHashTableDirectory, ExtendedHashTableHeader};
use crate::storage::page::page_types::extendable_hash_table_bucket_page::TypeErasedBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;

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

    /// Returns the page LSN.
    pub fn get_lsn(&self) -> Lsn {
        let bytes = &self.data[OFFSET_LSN..OFFSET_LSN + 4];
        let lsn = i32::from_ne_bytes(bytes.try_into().unwrap()).into();
        debug!("Fetching LSN for Page ID {}: {}", self.page_id, lsn);
        lsn
    }

    /// Sets the page LSN.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        let lsn_bytes = lsn.to_ne_bytes();
        self.data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn_bytes);
        info!("Set LSN for Page ID {}", self.page_id);
    }

}

pub trait PageTrait {
    fn get_page_id(&self) -> PageId;
    fn is_dirty(&self) -> bool;
    fn set_dirty(&mut self, is_dirty: bool);
    fn get_pin_count(&self) -> i32;
    fn increment_pin_count(&mut self);
    fn decrement_pin_count(&mut self);
    fn get_data(&self) -> &[u8; DB_PAGE_SIZE];
    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE];
    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError>;
    fn set_pin_count(&mut self, pin_count: i32);
    fn reset_memory(&mut self);
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
        debug!("Incremented pin count for Page ID {}: {}", self.page_id, self.pin_count);
    }

    /// Decrements the pin count of this page.
    fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
            debug!("Decremented pin count for Page ID {}: {}", self.page_id, self.pin_count);
        } else {
            error!("Attempted to decrement pin count below 0 for Page ID {}", self.page_id);
        }
    }

    /// Returns an immutable reference to the data. The data is protected by an internal read lock.
    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        debug!("Fetching data for page ID {}", self.page_id);
        // Return a copy of the data.
        &*self.data
    }

    /// Returns a mutable reference to the data, marks the page as dirty, and returns a copy.
    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        debug!("Fetching mutable data for page ID {}", self.page_id);

        // Return a copy of the mutable data.
        &mut *self.data
    }

    /// Method to modify the data
    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        debug!("Attempting to modify data for Page ID {} at offset {}", self.page_id, offset);

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

        debug!("Successfully modified data for Page ID {} at offset {}", self.page_id, offset);
        Ok(())
    }

    /// Sets the pin count of this page.
    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
        debug!("Setting pin count for Page ID {}: {}", self.page_id, pin_count);
    }

    /// Zeroes out the data that is held within the page.
    fn reset_memory(&mut self) {
        self.data.fill(0);
        debug!("Reset memory for Page ID {}", self.page_id);
    }
}

#[derive(Debug)]
pub enum PageType {
    Basic(Page),
    ExtendedHashTableDirectory(ExtendableHTableDirectoryPage),
    ExtendedHashTableHeader(ExtendableHTableHeaderPage),
    ExtendedHashTableBucket(TypeErasedBucketPage),
}

impl PageType {

    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE] {
        unimplemented!()
    }

    pub fn deserialize(&mut self, buffer: &[u8; DB_PAGE_SIZE]) {
        unimplemented!()
    }

    pub fn as_page_trait(&self) -> &dyn PageTrait {
        match self {
            Basic(page) => page,
            ExtendedHashTableDirectory(page) => page,
            ExtendedHashTableHeader(page) => page,
            ExtendedHashTableBucket(page) => page
        }
    }

    pub fn as_page_trait_mut(&mut self) -> &mut dyn PageTrait {
        match self {
            Basic(page) => page,
            ExtendedHashTableDirectory(page) => page,
            ExtendedHashTableHeader(page) => page,
            ExtendedHashTableBucket(page) => page
        }
    }

    pub fn as_any(&self) -> &dyn Any {
        match self {
            Basic(page) => page as &dyn Any,
            ExtendedHashTableDirectory(page) => page as &dyn Any,
            ExtendedHashTableHeader(page) => page as &dyn Any,
            ExtendedHashTableBucket(page) => page as &dyn Any,
        }
    }

    pub fn as_any_mut(&mut self) -> &mut dyn Any {
        match self {
            PageType::Basic(ref mut page) => page as &mut dyn Any,
            PageType::ExtendedHashTableDirectory(ref mut page) => page as &mut dyn Any,
            PageType::ExtendedHashTableHeader(ref mut page) => page as &mut dyn Any,
            PageType::ExtendedHashTableBucket(ref mut page) => page as &mut dyn Any,
        }
    }
}

impl From<ExtendableHTableDirectoryPage> for PageType {
    fn from(page: ExtendableHTableDirectoryPage) -> Self {
        PageType::ExtendedHashTableDirectory(page)
    }
}

impl From<ExtendableHTableHeaderPage> for PageType {
    fn from(page: ExtendableHTableHeaderPage) -> Self {
        PageType::ExtendedHashTableHeader(page)
    }
}

impl From<TypeErasedBucketPage> for PageType {
    fn from(page: TypeErasedBucketPage) -> Self {
        ExtendedHashTableBucket(page)
    }
}

pub trait AsAny {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl AsAny for Page {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}