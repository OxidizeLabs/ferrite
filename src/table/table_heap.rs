use std::option::Option;
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::rwlatch::ReaderWriterLatch;
use crate::page_db::table_page::TablePage;
use crate::table::tuple::{Tuple, TupleMeta};

type PageId = i32;
type TableOid = i32;
const INVALID_PAGE_ID: PageId = -1;



#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct RID {}

struct LockManager {}
struct Transaction {}
struct TableIterator {}
struct PageGuard {}
struct ReadPageGuard {}
struct WritePageGuard {}

pub struct TableHeap {
    bpm: Arc<Mutex<BufferPoolManager>>,
    first_page_id: PageId,
    last_page_id: Mutex<PageId>,
    reader_writer_latch: ReaderWriterLatch
}

impl TableHeap {
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, reader_writer_latch: ReaderWriterLatch) -> Self {
        Self {
            bpm,
            first_page_id: INVALID_PAGE_ID,
            last_page_id: Mutex::new(INVALID_PAGE_ID),
            reader_writer_latch,
        }
    }

    pub fn insert_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        lock_mgr: Option<&LockManager>,
        txn: Option<&Transaction>,
        oid: TableOid,
    ) -> Option<RID> {
        unimplemented!()
    }

    pub fn update_tuple_meta(&self, meta: &TupleMeta, rid: &RID) {
        unimplemented!()
    }

    pub fn get_tuple(&self, rid: &RID) -> Option<(TupleMeta, Tuple)> {
        unimplemented!()
    }

    pub fn get_tuple_meta(&self, rid: &RID) -> Option<TupleMeta> {
        unimplemented!()
    }

    pub fn make_iterator(&self) -> TableIterator {
        unimplemented!()
    }

    pub fn make_eager_iterator(&self) -> TableIterator {
        unimplemented!()
    }

    pub fn get_first_page_id(&self) -> PageId {
        self.first_page_id
    }

    pub fn update_tuple_in_place<F>(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: &RID,
        check: F,
    ) -> bool
    where
        F: Fn(&TupleMeta, &Tuple, &RID) -> bool,
    {
        // Update tuple in place logic here
        false
    }

    pub fn create_empty_heap(create_table_heap: bool) -> Option<Box<Self>> {
        assert!(
            !create_table_heap,
            "create_table_heap should be false to generate an empty heap"
        );
        // Some(Box::new(Self::new(Arc::new(Mutex::new(BufferPoolManager {})))))
        unimplemented!()
    }

    pub fn acquire_table_page_read_lock(&self, rid: &RID) -> ReadPageGuard {
        unimplemented!()
    }

    pub fn acquire_table_page_write_lock(&self, rid: &RID) -> WritePageGuard {
        unimplemented!()
    }

    pub fn update_tuple_in_place_with_lock_acquired(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: &RID,
        page: &mut TablePage,
    ) {
        unimplemented!()
    }

    pub fn get_tuple_with_lock_acquired(
        &self,
        rid: &RID,
        page: &TablePage,
    ) -> Option<(TupleMeta, Tuple)> {
        unimplemented!()
    }

    pub fn get_tuple_meta_with_lock_acquired(
        &self,
        rid: &RID,
        page: &TablePage,
    ) -> Option<TupleMeta> {
        unimplemented!()
    }
}

// fn main() {
//     // Example usage of TableHeap
//     let bpm = Arc::new(Mutex::new(BufferPoolManager {}));
//     let table_heap = TableHeap::new(bpm);
//
//     // Further code...
// }
