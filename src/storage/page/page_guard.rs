use std::any::TypeId;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::AccessType;
use crate::common::config::PageId;
use crate::storage::page::page::{AsAny, Page};
use crate::storage::page::page::PageType;
use spin::RwLock;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::{BucketPageTrait, ExtendableHTableBucketPage};
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

    fn get_pin_count(&self) -> i32 {
        self.page.read().as_page_trait().get_pin_count()
    }

    fn set_dirty(&self, is_dirty: bool) {
        self.page.write().as_page_trait_mut().set_dirty(is_dirty)
    }

    pub fn get_page_type(&self) -> &'static str {
        let page = self.read();
        match *page {
            PageType::Basic(_) => "Basic",
            PageType::ExtendedHashTableDirectory(_) => "ExtendedHashTableDirectory",
            PageType::ExtendedHashTableHeader(_) => "ExtendedHashTableHeader",
            PageType::ExtendedHashTableBucket(_) => "ExtendedHashTableBucket",
        }
    }

    pub fn into_specific_type<T: 'static>(self) -> Option<SpecificPageGuard<T>> {
        let page_type = TypeId::of::<T>();
        let is_matching_type = match &*self.page.read() {
            PageType::Basic(_) =>
                page_type == TypeId::of::<Page>(),
            PageType::ExtendedHashTableBucket(bucket_page) =>
                page_type == TypeId::of::<ExtendableHTableBucketPage<8>>() && bucket_page.get_key_size() == 8,
            PageType::ExtendedHashTableDirectory(_) =>
                page_type == TypeId::of::<ExtendableHTableDirectoryPage>(),
            PageType::ExtendedHashTableHeader(_) =>
                page_type == TypeId::of::<ExtendableHTableHeaderPage>(),
        };

        if is_matching_type {
            Some(SpecificPageGuard {
                inner: self,
                _phantom: PhantomData,
            })
        } else {
            None
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

pub struct SpecificPageGuard<T: 'static> {
    inner: PageGuard,
    _phantom: PhantomData<T>,
}

impl<T: 'static> SpecificPageGuard<T> {
    pub fn read(&self) -> SpecificPageReadGuard<T> {
        SpecificPageReadGuard(self.inner.read(), PhantomData)
    }

    pub fn write(&self) -> SpecificPageWriteGuard<T> {
        SpecificPageWriteGuard(self.inner.write(), PhantomData)
    }

    pub fn get_page_id(&self) -> PageId {
        self.inner.get_page_id()
    }

    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        match &*self.inner.page.read() {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast::<8, _, _>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<8> in into_specific_type
                    let typed_page = unsafe { &*(page as *const _ as *const T) };
                    f(typed_page)
                })
            },
            PageType::Basic(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any().downcast_ref::<T>().map(f),
        }
    }

    pub fn access_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        match &mut *self.inner.page.write() {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast_mut::<8, _, _>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<8> in into_specific_type
                    let typed_page = unsafe { &mut *(page as *mut _ as *mut T) };
                    f(typed_page)
                })
            },
            PageType::Basic(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any_mut().downcast_mut::<T>().map(f),
        }
    }
    }

pub struct SpecificPageReadGuard<'a, T: 'static>(spin::RwLockReadGuard<'a, PageType>, PhantomData<T>);

impl<'a, T: 'static> SpecificPageReadGuard<'a, T> {
    pub fn access<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        match &*self.0 {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast::<8, _, _>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<8> in into_specific_type
                    let typed_page = unsafe { &*(page as *const _ as *const T) };
                    f(typed_page)
                })
            },
            PageType::Basic(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any().downcast_ref::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any().downcast_ref::<T>().map(f),
        }
    }
}

pub struct SpecificPageWriteGuard<'a, T: 'static>(spin::RwLockWriteGuard<'a, PageType>, PhantomData<T>);

impl<'a, T: 'static> SpecificPageWriteGuard<'a, T> {
    pub fn access_mut<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        match &mut *self.0 {
            PageType::ExtendedHashTableBucket(bucket_page) => {
                bucket_page.with_downcast_mut::<8, _, _>(|page| {
                    // SAFETY: We've already checked that T is ExtendableHTableBucketPage<8> in into_specific_type
                    let typed_page = unsafe { &mut *(page as *mut _ as *mut T) };
                    f(typed_page)
                })
            },
            PageType::Basic(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableDirectory(page) => page.as_any_mut().downcast_mut::<T>().map(f),
            PageType::ExtendedHashTableHeader(page) => page.as_any_mut().downcast_mut::<T>().map(f),
        }
    }
}

impl<T: 'static> Drop for SpecificPageGuard<T> {
    fn drop(&mut self) {
        // The inner PageGuard's drop will handle unpinning
    }
}