use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::{PageId, TableOidT, INVALID_PAGE_ID};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{error, info};
use spin::RwLock;
use std::sync::Arc;

/// TableHeap represents a physical table on disk.
/// This is just a doubly-linked list of pages.
pub struct TableHeap {
    bpm: Arc<BufferPoolManager>,
    first_page_id: PageId,
    latch: RwLock<()>,
    last_page_id: PageId,
}

impl TableHeap {
    /// Creates a new table heap without a transaction (open table).
    ///
    /// # Parameters
    ///
    /// - `bpm`: Buffer pool manager.
    ///
    /// # Returns
    ///
    /// A new `TableHeap` instance.
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        TableHeap {
            bpm,
            first_page_id: INVALID_PAGE_ID,
            latch: RwLock::new(()),
            last_page_id: INVALID_PAGE_ID,
        }
    }

    /// Inserts a tuple into the table. If the tuple is too large (>= page_size), returns `None`.
    ///
    /// # Parameters
    ///
    /// - `meta`: Tuple meta.
    /// - `tuple`: Tuple to insert.
    /// - `lock_mgr`: Optional lock manager.
    /// - `txn`: Optional transaction.
    /// - `oid`: Table OID.
    ///
    /// # Returns
    ///
    /// An `Option` containing the RID of the inserted tuple, or `None` if the tuple is too large
    pub fn insert_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        lock_mgr: Option<&LockManager>,
        txn: Option<&Transaction>,
        oid: TableOidT,
    ) -> Result<RID, String> {
        let _write_guard = self.latch.write(); // Use RAII for the write lock

        let page_guard = self.bpm.fetch_page_guarded(self.last_page_id)
            .ok_or_else(|| "Failed to fetch page".to_string())?;

        let page_id = page_guard.get_page_id();

        let mut table_page_guard = page_guard.into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        table_page_guard.access_mut(|table_page| {
            table_page.insert_tuple(meta, tuple)
                .map(|table_page_rid: RID| {
                    info!("Tuple inserted successfully");
                    assert_ne!(table_page.get_next_tuple_offset(meta, tuple), None);
                    assert_ne!(table_page.get_num_tuples(), 0);

                    tuple.get_rid()
                })
                .ok_or_else(|| "Failed to insert tuple: Not enough space".to_string())
        }).unwrap_or_else(|| Err("Failed to access table page".to_string()))
    }

    /// Updates the meta of a tuple.
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta.
    /// - `rid`: The RID of the inserted tuple.
    pub fn update_tuple_meta(&self, meta: &TupleMeta, rid: RID) {
        // Implementation of update tuple meta logic here
        unimplemented!()
    }

    pub fn get_bpm(&self) -> Arc<BufferPoolManager> {
        self.bpm.clone()
    }

    /// Reads a tuple from the table.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple to read.
    ///
    /// # Returns
    ///
    /// A pair containing the meta and tuple.
    pub fn get_tuple(&self, rid: RID) -> (TupleMeta, Tuple) {
        // Implementation of get tuple logic here
        unimplemented!()
    }

    /// Reads a tuple meta from the table. Note: if you want to get the tuple and meta together,
    /// use `get_tuple` instead to ensure atomicity.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple to read.
    ///
    /// # Returns
    ///
    /// The tuple meta.
    pub fn get_tuple_meta(&self, rid: RID) -> TupleMeta {
        // Implementation of get tuple meta logic here
        unimplemented!()
    }

    /// Returns an iterator of this table. When this iterator is created, it will record the
    /// current last tuple in the table heap, and the iterator will stop at that point,
    /// in order to avoid the Halloween problem.
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    pub fn make_iterator(&self) -> TableIterator {
        // Implementation of make iterator logic here
        unimplemented!()
    }

    /// Returns an eager iterator of this table. The iterator will stop at the last tuple
    /// at the time of iterating.
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    // pub fn make_eager_iterator(&self) -> TableIterator {
    //     // Implementation of make eager iterator logic here
    //     unimplemented!()
    // }

    /// Returns the ID of the first page of this table.
    ///
    /// # Returns
    ///
    /// The page ID of the first page.
    pub fn get_first_page_id(&self) -> PageId {
        self.first_page_id
    }

    /// Updates a tuple in place. Should NOT be used in project 3. Implement your project 3
    /// update executor as delete and insert. You will need to use this function in project 4.
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta.
    /// - `tuple`: New tuple.
    /// - `rid`: The RID of the tuple to be updated.
    /// - `check`: Optional check to run before actually updating.
    ///
    /// # Returns
    ///
    /// `true` if the tuple was updated successfully, `false` otherwise.
    pub fn update_tuple_in_place(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
        check: Option<Box<dyn Fn(&TupleMeta, &Tuple, RID) -> bool>>,
    ) -> bool {
        // Implementation of update tuple in place logic here
        unimplemented!()
    }

    /// Acquires a read lock on a table page.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the page to lock.
    ///
    /// # Returns
    ///
    /// A `ReadPageGuard`.
    pub fn acquire_table_page_read_lock(&self, rid: RID) -> PageGuard {
        // Implementation of acquire table page read lock logic here
        self.bpm.fetch_page_guarded(rid.get_page_id()).unwrap()
    }

    /// Acquires a write lock on a table page.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the page to lock.
    ///
    /// # Returns
    ///
    /// A `WritePageGuard`.
    pub fn acquire_table_page_write_lock(&self, rid: RID) -> PageGuard {
        // Implementation of acquire table page write lock logic here
        unimplemented!()
    }

    /// Updates a tuple in place with the lock acquired.
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta.
    /// - `tuple`: New tuple.
    /// - `rid`: RID of the tuple to be updated.
    /// - `page`: The table page.
    pub fn update_tuple_in_place_with_lock_acquired(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
        page: &TablePage,
    ) {
        // Implementation of update tuple in place with lock acquired logic here
        unimplemented!()
    }

    /// Gets a tuple with the lock acquired.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple.
    /// - `page`: The table page.
    ///
    /// # Returns
    ///
    /// A pair containing the meta and tuple.
    pub fn get_tuple_with_lock_acquired(&self, rid: RID, page: &TablePage) -> (TupleMeta, Tuple) {
        // Implementation of get tuple with lock acquired logic here
        unimplemented!()
    }

    /// Gets a tuple meta with the lock acquired.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple.
    /// - `page`: The table page.
    ///
    /// # Returns
    ///
    /// The tuple meta.
    pub fn get_tuple_meta_with_lock_acquired(&self, rid: RID, page: &TablePage) -> TupleMeta {
        // Implementation of get tuple meta with lock acquired logic here
        unimplemented!()
    }

    /// Creates a new `TableHeap` for binder tests.
    ///
    /// # Parameters
    ///
    /// - `create_table_heap`: Should be `false` to generate an empty heap.
    ///
    /// # Returns
    ///
    /// A new `TableHeap` instance.
    pub fn new_for_binder(create_table_heap: bool) -> Self {
        unimplemented!()
    }
}
