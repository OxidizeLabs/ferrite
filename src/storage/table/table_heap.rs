use crate::buffer::buffer_pool_manager::{BufferPoolManager, NewPageType};
use crate::common::config::{PageId, TableOidT, INVALID_PAGE_ID};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::storage::page::page::PageTrait;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use std::fmt::{Debug, Formatter};
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
        let table_page = bpm.new_page_guarded(NewPageType::Table);
        let page_guard = table_page.unwrap();
        let first_page_id = page_guard.get_page_id();
        if let Some(ext_guard) = page_guard.into_specific_type::<TablePage, 8>() {
            let read_guard = ext_guard.read();
            read_guard.access(|page| {
                info!("Create TablePage for TableHeap with PageId: {}", page.get_page_id())
            });
        }
        let last_page_id = first_page_id;
        TableHeap {
            bpm,
            first_page_id,
            latch: RwLock::new(()),
            last_page_id,
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

        let mut table_page_guard = page_guard.into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        table_page_guard.access_mut(|table_page| {
            table_page.insert_tuple(meta, tuple)
                .map(|table_page_rid: RID| {
                    debug!("Tuple inserted successfully");
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
        let _write_guard = self.latch.write(); // Use RAII for the write lock

        let page_guard = self.bpm.fetch_page_guarded(rid.get_page_id()).ok_or_else(|| "Failed to fetch page".to_string()).unwrap();

        let mut table_page_guard = page_guard.into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string()).unwrap();

        table_page_guard.access_mut(|table_page| {
            table_page.update_tuple_meta(meta, &rid)
        }).expect("Failed to update tuple metadata");
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
    pub fn get_tuple(&self, rid: RID) -> Result<(TupleMeta, Tuple), String> {
        let _read_guard = self.latch.read();

        let page_guard = self.bpm.fetch_page_guarded(rid.get_page_id()).ok_or_else(|| "Failed to fetch page".to_string())?;

        let table_page_guard = page_guard.into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        table_page_guard.access(|table_page| {
            table_page.get_tuple(&rid).unwrap()
        }).ok_or_else(|| "Failed to get tuple".to_string())

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
    pub fn get_tuple_meta(&self, rid: RID) -> Result<TupleMeta, String> {
        let _write_guard = self.latch.write();

        let page_guard = self.bpm.fetch_page_guarded(rid.get_page_id()).ok_or_else(|| "Failed to fetch page".to_string())?;

        let mut table_page_guard = page_guard.into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        table_page_guard.access_mut(|table_page| {
            table_page.get_tuple(&rid).unwrap().0
        }).ok_or_else(|| "Failed to get tuple metadata".to_string())
    }

    /// Returns an iterator of this table. When this iterator is created, it will record the
    /// current last tuple in the table heap, and the iterator will stop at that point,
    /// in order to avoid the Halloween problem.
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    pub fn make_iterator(&self) -> TableIterator {
        // Use a scoped lock to release it immediately after getting last_page_id
        let last_page_id = {
            let _read_guard = self.latch.read();
            self.last_page_id
        };

        let start_rid = RID::new(self.first_page_id, 0);

        let stop_at_rid = if last_page_id != INVALID_PAGE_ID {
            if let Some(page_guard) = self.bpm.fetch_page_guarded(last_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        RID::new(last_page_id, page.get_num_tuples())
                    })
                } else {
                    error!("Failed to convert to TablePage");
                    Option::from(RID::new(INVALID_PAGE_ID, 0))
                }
            } else {
                error!("Failed to fetch last page");
                Option::from(RID::new(INVALID_PAGE_ID, 0))
            }
        } else {
            Option::from(RID::new(INVALID_PAGE_ID, 0))
        };

        debug!("Creating iterator: start_rid = {:?}, stop_at_rid = {:?}", start_rid, stop_at_rid.unwrap());
        TableIterator::new(self, start_rid, stop_at_rid.unwrap())
    }

    /// Returns an eager iterator of this table. The iterator will stop at the last tuple
    /// at the time of iterating.
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    pub fn make_eager_iterator(&self) -> TableIterator {
        // Implementation of make eager iterator logic here
        unimplemented!()
    }

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
        self.bpm.fetch_page_guarded(rid.get_page_id()).unwrap()
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
        page.get_tuple(&rid).unwrap()
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
        page.get_tuple(&rid).unwrap().0
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

    /// Helper method to get the number of pages
    fn get_num_pages(&self) -> usize {
        let mut count = 0;
        let mut current_page_id = self.first_page_id;
        while current_page_id != INVALID_PAGE_ID {
            count += 1;
            if let Some(page_guard) = self.bpm.fetch_page_guarded(current_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        current_page_id = page.get_next_page_id();
                    });
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        count
    }

    /// Helper method to get the total number of tuples
    fn get_num_tuples(&self) -> usize {
        let mut count = 0;
        let mut current_page_id = self.first_page_id;
        while current_page_id != INVALID_PAGE_ID {
            if let Some(page_guard) = self.bpm.fetch_page_guarded(current_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        count += page.get_num_tuples() as usize;
                        current_page_id = page.get_next_page_id();
                    });
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        count
    }
}

impl Debug for TableHeap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHeap")
            .field("first_page_id", &self.first_page_id)
            .field("last_page_id", &self.last_page_id)
            .field("bpm", &"Arc<BufferPoolManager>") // We don't debug the BPM itself
            .field("num_pages", &self.get_num_pages())
            .field("num_tuples", &self.get_num_tuples())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use super::*;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            const K: usize = 2;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(
                &disk_manager,
            ))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));
            Self {
                bpm,
                db_file,
                db_log_file,
            }
        }

        fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup()
        }
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn test_table_heap_creation() {
        let ctx = TestContext::new("test_table_heap_creation");
        let bpm = ctx.bpm.clone();
        let table_heap = TableHeap::new(bpm);

        assert_ne!(table_heap.get_first_page_id(), INVALID_PAGE_ID);
        assert_eq!(table_heap.get_num_pages(), 1);
        assert_eq!(table_heap.get_num_tuples(), 0);
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let ctx = TestContext::new("test_insert_and_get_tuple");
        let bpm = ctx.bpm.clone();
        let table_heap = TableHeap::new(bpm);
        let schema = create_test_schema();
        let rid = RID::new(table_heap.get_first_page_id(), 0);

        let tuple_values = vec![
            Value::new(1),
            Value::new("Alice"),
            Value::new(30),
        ];
        let mut tuple = Tuple::new(tuple_values, schema.clone(), rid);
        let meta = TupleMeta::new(0, false);

        let rid = table_heap.insert_tuple(&meta, &mut tuple, None, None, 0)
            .expect("Failed to insert tuple");

        let (retrieved_meta, retrieved_tuple) = table_heap.get_tuple(rid)
            .expect("Failed to get tuple");

        assert_eq!(retrieved_meta, meta);
        assert_eq!(retrieved_tuple, tuple);
        assert_eq!(table_heap.get_num_tuples(), 1);
    }

    #[test]
    fn test_update_tuple_meta() {
        let ctx = TestContext::new("test_update_tuple_meta");
        let bpm = ctx.bpm.clone();
        let table_heap = TableHeap::new(bpm);
        let schema = create_test_schema();

        let tuple_values = vec![
            Value::new(1),
            Value::new("Bob"),
            Value::new(25),
        ];
        let mut tuple = Tuple::new(tuple_values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(0, false);

        let rid = table_heap.insert_tuple(&meta, &mut tuple, None, None, 0)
            .expect("Failed to insert tuple");

        let updated_meta = TupleMeta::new(1, true);
        table_heap.update_tuple_meta(&updated_meta, rid);

        let retrieved_meta = table_heap.get_tuple_meta(rid)
            .expect("Failed to get tuple meta");

        assert_eq!(retrieved_meta, updated_meta);
    }

    #[test]
    fn test_table_iterator() {
        let ctx = TestContext::new("test_table_iterator");
        let bpm = ctx.bpm.clone();        let table_heap = TableHeap::new(bpm);
        let schema = create_test_schema();

        // Insert multiple tuples
        for i in 0..5 {
            let tuple_values = vec![
                Value::new(i),
                Value::new(format!("Name{}", i)),
                Value::new(20 + i),
            ];
            let mut tuple = Tuple::new(tuple_values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(0, false);
            table_heap.insert_tuple(&meta, &mut tuple, None, None, 0)
                .expect("Failed to insert tuple");
        }

        let iterator = table_heap.make_iterator();
        let tuples: Vec<(TupleMeta, Tuple)> = iterator.collect();

        assert_eq!(tuples.len(), 5);
        for (i, (meta, tuple)) in tuples.iter().enumerate() {
            assert_eq!(tuple.get_value(0), &Value::new(i as i32));
            assert_eq!(tuple.get_value(1), &Value::new(format!("Name{}", i)));
            assert_eq!(tuple.get_value(2), &Value::new(20 + i as i32));
            assert_eq!(meta.get_timestamp(), 0);
            assert_eq!(meta.is_deleted(), false);
        }
    }

    #[test]
    fn test_table_heap_debug() {
        let ctx = TestContext::new("test_table_heap_debug");
        let bpm = ctx.bpm.clone();        let table_heap = TableHeap::new(bpm);

        let debug_output = format!("{:?}", table_heap);
        assert!(debug_output.contains("TableHeap"));
        assert!(debug_output.contains("first_page_id"));
        assert!(debug_output.contains("last_page_id"));
        assert!(debug_output.contains("num_pages"));
        assert!(debug_output.contains("num_tuples"));
    }
}