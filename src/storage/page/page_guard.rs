use std::marker::PhantomData;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::page::page::{Page, PageType};
use spin::RwLock;
use std::sync::Arc;
use log::{debug, error, info, warn};
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;

pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page: Arc<RwLock<PageType>>,
    page_id: PageId,
}

impl PageGuard {
    pub fn new(bpm: Arc<BufferPoolManager>, page: Arc<RwLock<PageType>>, page_id: PageId) -> Self {
        Self { bpm, page, page_id }
    }

    pub fn read(&self) -> spin::RwLockReadGuard<'_, PageType> {
        self.page.read()
    }

    pub fn write(&self) -> spin::RwLockWriteGuard<'_, PageType> {
        self.page.write()
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    // New method to get pin count without locking the entire page
    fn get_pin_count(&self) -> i32 {
        self.page.read().as_page_trait().get_pin_count()
    }

    // New method to set dirty flag without locking the entire page
    fn set_dirty(&self, is_dirty: bool) {
        self.page.write().as_page_trait_mut().set_dirty(is_dirty)
    }

    // Method to convert the guard into a specific page type
    pub fn into_specific_type<T>(self) -> Option<SpecificPageGuard<T>>
    where
        T: TryFrom<PageType>,
    {
        match T::try_from((*self.page.clone().read()).clone()) {
            Ok(_) => Some(SpecificPageGuard {
                inner: self,
                _phantom: PhantomData,
            }),
            Err(_) => None,
        }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let is_dirty = self.read().as_page_trait().is_dirty();
        self.set_dirty(is_dirty);
        self.bpm.unpin_page(self.page_id, is_dirty, AccessType::Unknown);
    }
}

pub struct SpecificPageGuard<T> {
    inner: PageGuard,
    _phantom: PhantomData<T>,
}

impl<T> SpecificPageGuard<T>
where
    T: TryFrom<PageType>,
{
    pub fn read(&self) -> SpecificPageReadGuard<T> {
        SpecificPageReadGuard(self.inner.read(), PhantomData)
    }

    pub fn write(&self) -> SpecificPageWriteGuard<T> {
        SpecificPageWriteGuard(self.inner.write(), PhantomData)
    }

    pub fn get_page_id(&self) -> PageId {
        self.inner.get_page_id()
    }
}

pub struct SpecificPageReadGuard<'a, T>(spin::RwLockReadGuard<'a, PageType>, PhantomData<T>);

impl<'a, T> SpecificPageReadGuard<'a, T>
where
    T: TryFrom<PageType>,
{
    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        T::try_from((*self.0).clone()).ok().map(|specific_page| f(&specific_page))
    }
}

pub struct SpecificPageWriteGuard<'a, T>(spin::RwLockWriteGuard<'a, PageType>, PhantomData<T>);

impl<'a, T> SpecificPageWriteGuard<'a, T>
where
    T: TryFrom<PageType>, PageType: From<T>
{
    pub fn access_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut page_type = (*self.0).clone();
        T::try_from(page_type)
            .ok()
            .map(|mut specific_page| {
                let result = f(&mut specific_page);
                *self.0 = specific_page.into();
                result
            })
    }
}

impl<T> Drop for SpecificPageGuard<T> {
    fn drop(&mut self) {
        // The inner PageGuard's drop will handle unpinning
    }
}

// Implement TryFrom for your specific page types
impl TryFrom<PageType> for ExtendableHTableDirectoryPage {
    type Error = ();

    fn try_from(page_type: PageType) -> Result<Self, Self::Error> {
        match page_type {
            PageType::ExtendedHashTableDirectory(page) => Ok(page),
            _ => Err(()),
        }
    }
}

impl TryFrom<PageType> for ExtendableHTableHeaderPage {
    type Error = ();

    fn try_from(page_type: PageType) -> Result<Self, Self::Error> {
        match page_type {
            PageType::ExtendedHashTableHeader(page) => Ok(page),
            _ => Err(()),
        }
    }
}

// Implement From for converting specific page types back to PageType
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
