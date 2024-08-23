use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::storage::index::generic_key::{Comparator, GenericComparator, GenericKey};
use crate::storage::page::page::{AsAny, Page, PageTrait};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

pub const HTABLE_BUCKET_PAGE_METADATA_SIZE: usize = size_of::<u32>() * 2;
pub const BUCKET_HEADER_SIZE: usize = size_of::<PageId>() + size_of::<u16>() * 2;

#[derive(Clone)]
pub struct ExtendableHTableBucketPage<const KEY_SIZE: usize> {
    base: Page,
    size: u16,
    max_size: u16,
    local_depth: u8,
    array: Vec<Option<(GenericKey<KEY_SIZE>, PageId)>>,
}

pub struct TypeErasedBucketPage {
    inner: Arc<RwLock<dyn BucketPageTrait>>,
}

pub trait BucketPageTrait: PageTrait + AsAny + Send + Sync {
    fn get_key_size(&self) -> usize;
    fn lookup(&self, key: &[u8], comparator: &GenericComparator) -> Option<PageId>;
    fn insert(&mut self, key: &[u8], value: PageId, comparator: &GenericComparator) -> bool;
    fn remove(&mut self, key: &[u8], comparator: &GenericComparator) -> bool;
    fn is_full(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn get_size(&self) -> u16;
    fn get_local_depth(&self) -> u8;
    fn set_local_depth(&mut self, depth: u8);
}

impl<const KEY_SIZE: usize> ExtendableHTableBucketPage<KEY_SIZE> {
    pub fn new(page_id: PageId) -> Self {
        let entry_size = KEY_SIZE + std::mem::size_of::<PageId>();
        let max_entries = (DB_PAGE_SIZE - BUCKET_HEADER_SIZE) / entry_size;
        let mut instance = Self {
            base: Page::new(page_id),
            size: 0,
            max_size: max_entries as u16,
            local_depth: 0,
            array: Vec::with_capacity(max_entries),
        };

        instance
    }

    pub fn init(&mut self, size: u16) {
        self.size = size;
        self.local_depth = 0;
        self.array.clear();
        self.array.resize(self.max_size as usize, None);
    }

    pub fn lookup(&self, key: &GenericKey<KEY_SIZE>, comparator: &GenericComparator) -> Option<PageId> {
        for entry in self.array.iter().take(self.size as usize) {
            if let Some((k, v)) = entry {
                if comparator.compare(k, key) == Ordering::Equal {
                    return Some(*v);
                }
            }
        }
        None
    }

    pub fn insert(&mut self, key: GenericKey<KEY_SIZE>, value: PageId, comparator: &GenericComparator) -> bool {
        if self.is_full() {
            return false;
        }
        if self.lookup(&key, comparator).is_some() {
            return false;
        }
        self.array[self.size as usize] = Some((key, value));
        self.size += 1;
        true
    }

    pub fn remove(&mut self, key: &GenericKey<KEY_SIZE>, comparator: &GenericComparator) -> bool {
        if let Some(index) = (0..self.size as usize)
            .find(|&i| self.array[i].as_ref().map_or(false, |(k, _)| comparator.compare(k, key) == std::cmp::Ordering::Equal))
        {
            self.array[index] = None;
            self.array[index..self.size as usize].rotate_left(1);
            self.size -= 1;
            true
        } else {
            false
        }
    }

    pub fn is_full(&self) -> bool {
        self.size as usize == self.max_size as usize
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn get_size(&self) -> u16 {
        self.size
    }

    pub fn get_local_depth(&self) -> u8 {
        self.local_depth
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.local_depth = depth;
    }
}

impl TypeErasedBucketPage {
    pub fn new<T: BucketPageTrait + 'static>(page: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(page)),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<dyn BucketPageTrait> {
        self.inner.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<dyn BucketPageTrait> {
        self.inner.write()
    }

    pub fn with_downcast<const KEY_SIZE: usize, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&ExtendableHTableBucketPage<KEY_SIZE>) -> R,
    {
        let read_guard = self.inner.read();
        read_guard.as_any().downcast_ref::<ExtendableHTableBucketPage<KEY_SIZE>>().map(f)
    }

    pub fn with_downcast_mut<const KEY_SIZE: usize, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut ExtendableHTableBucketPage<KEY_SIZE>) -> R,
    {
        let mut write_guard = self.inner.write();
        write_guard.as_any_mut().downcast_mut::<ExtendableHTableBucketPage<KEY_SIZE>>().map(f)
    }

    pub fn get_key_size(&self) -> usize {
        self.inner.read().get_key_size()
    }

    pub fn with_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8; DB_PAGE_SIZE]) -> R,
    {
        let guard = self.inner.read();
        f(guard.get_data())
    }

    pub fn with_data_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8; DB_PAGE_SIZE]) -> R,
    {
        let mut guard = self.inner.write();
        f(guard.get_data_mut())
    }
}

// Implement Send and Sync for TypeErasedBucketPage
unsafe impl Send for TypeErasedBucketPage {}
unsafe impl Sync for TypeErasedBucketPage {}

// Update ExtendableHTableBucketPage to implement Send and Sync
unsafe impl<const KEY_SIZE: usize> Send for ExtendableHTableBucketPage<KEY_SIZE> {}
unsafe impl<const KEY_SIZE: usize> Sync for ExtendableHTableBucketPage<KEY_SIZE> {}

impl<const KEY_SIZE: usize> BucketPageTrait for ExtendableHTableBucketPage<KEY_SIZE> {
    fn get_key_size(&self) -> usize {
        KEY_SIZE
    }

    fn lookup(&self, key: &[u8], comparator: &GenericComparator) -> Option<PageId> {
        let generic_key = GenericKey::<KEY_SIZE>::from_bytes(key);
        self.lookup(&generic_key, comparator)
    }

    fn insert(&mut self, key: &[u8], value: PageId, comparator: &GenericComparator) -> bool {
        let generic_key = GenericKey::<KEY_SIZE>::from_bytes(key);
        self.insert(generic_key, value, comparator)
    }

    fn remove(&mut self, key: &[u8], comparator: &GenericComparator) -> bool {
        let generic_key = GenericKey::<KEY_SIZE>::from_bytes(key);
        self.remove(&generic_key, comparator)
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn get_size(&self) -> u16 {
        self.get_size()
    }

    fn get_local_depth(&self) -> u8 {
        self.get_local_depth()
    }

    fn set_local_depth(&mut self, depth: u8) {
        self.set_local_depth(depth)
    }
}

impl<const KEY_SIZE: usize> PageTrait for ExtendableHTableBucketPage<KEY_SIZE> {
    fn get_page_id(&self) -> PageId {
        self.base.get_page_id()
    }

    fn is_dirty(&self) -> bool {
        self.base.is_dirty()
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.base.set_dirty(is_dirty)
    }

    fn get_pin_count(&self) -> i32 {
        self.base.get_pin_count()
    }

    fn increment_pin_count(&mut self) {
        self.base.increment_pin_count()
    }

    fn decrement_pin_count(&mut self) {
        self.base.decrement_pin_count()
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        self.base.get_data()
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        self.base.get_data_mut()
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        self.base.set_data(offset, new_data)
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.base.set_pin_count(pin_count)
    }

    fn reset_memory(&mut self) {
        self.base.reset_memory()
    }
}

impl PageTrait for TypeErasedBucketPage {
    fn get_page_id(&self) -> PageId {
        self.inner.read().get_page_id()
    }

    fn is_dirty(&self) -> bool {
        self.inner.read().is_dirty()
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.inner.write().set_dirty(is_dirty);
    }

    fn get_pin_count(&self) -> i32 {
        self.inner.read().get_pin_count()
    }

    fn increment_pin_count(&mut self) {
        self.inner.write().increment_pin_count();
    }

    fn decrement_pin_count(&mut self) {
        self.inner.write().decrement_pin_count();
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        // This method can't be implemented safely for TypeErasedBucketPage
        // because we can't return a reference that outlives the RwLockReadGuard.
        // Instead, we'll panic with an explanation.
        panic!("get_data() is not supported for TypeErasedBucketPage. Use with_data() instead.")
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        // This method can't be implemented safely for TypeErasedBucketPage
        // because we can't return a reference that outlives the RwLockWriteGuard.
        // Instead, we'll panic with an explanation.
        panic!("get_data_mut() is not supported for TypeErasedBucketPage. Use with_data_mut() instead.")
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        self.inner.write().set_data(offset, new_data)
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.inner.write().set_pin_count(pin_count);
    }

    fn reset_memory(&mut self) {
        self.inner.write().reset_memory();
    }
}
impl<const KEY_SIZE: usize> AsAny for ExtendableHTableBucketPage<KEY_SIZE> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl AsAny for TypeErasedBucketPage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Debug for TypeErasedBucketPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Display for TypeErasedBucketPage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TypeErasedBucketPage")
    }
}
