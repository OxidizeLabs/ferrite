use crate::common::config::INVALID_PAGE_ID;
use crate::common::rid::RID;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::Mutex;
use std::sync::Arc;


/// An iterator over the tuples in a table.
pub struct TableIterator {
    table_heap: Arc<Mutex<TableHeap>>,
    rid: RID,
    stop_at_rid: RID,
}

impl TableIterator {
    /// Creates a new `TableIterator`.
    ///
    /// # Arguments
    ///
    /// * `table_heap` - A reference to the `TableHeap` to iterate over.
    /// * `rid` - The starting `RID` for the iterator.
    /// * `stop_at_rid` - The `RID` at which to stop iteration.
    ///
    /// # Returns
    ///
    /// A new `TableIterator` instance.
    pub fn new(table_heap: Arc<Mutex<TableHeap>>, rid: RID, stop_at_rid: RID) -> Self {
        let mut iterator = Self {
            table_heap,
            rid,
            stop_at_rid,
        };

        iterator.initialize();
        iterator
    }

    /// Initializes the iterator, setting the initial RID.
    fn initialize(&mut self) {
        if self.rid.get_page_id() == INVALID_PAGE_ID {
            self.rid = RID::new(INVALID_PAGE_ID, 0);
        } else {
            let table_heap_lock = self.table_heap.lock();
            let bpm = table_heap_lock.get_bpm();
            if let Some(page_guard) = bpm.fetch_page_guarded(self.rid.get_page_id()) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        if self.rid.get_slot_num() >= page.get_num_tuples() {
                            self.rid = RID::new(INVALID_PAGE_ID, 0);
                        }
                    });
                } else {
                    // Log error instead of panicking
                    log::error!("Failed to convert to TablePage");
                    self.rid = RID::new(INVALID_PAGE_ID, 0);
                }
            } else {
                // Log error instead of panicking
                log::error!("Failed to fetch page");
                self.rid = RID::new(INVALID_PAGE_ID, 0);
            }
        }
    }

    /// Gets the current tuple.
    ///
    /// # Returns
    ///
    /// A tuple containing the `TupleMeta` and `Tuple` at the current position.
    pub fn get_tuple(&self) -> Option<(TupleMeta, Tuple)> {
        if self.is_end() {
            None
        } else {
            Some(self.table_heap.lock().get_tuple(self.rid))
        }
    }

    /// Gets the current RID.
    ///
    /// # Returns
    ///
    /// The current `RID`.
    pub fn get_rid(&self) -> RID {
        self.rid
    }

    /// Checks if the iterator has reached the end.
    ///
    /// # Returns
    ///
    /// `true` if the iterator has reached the end, `false` otherwise.
    pub fn is_end(&self) -> bool {
        self.rid.get_page_id() == INVALID_PAGE_ID
    }
}

impl Iterator for TableIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end() {
            return None;
        }

        let result = self.get_tuple();
        self.advance();
        result
    }
}

impl TableIterator {
    /// Advances the iterator to the next position.
    fn advance(&mut self) {
        let table_heap_lock = self.table_heap.lock();
        let bpm = table_heap_lock.get_bpm();
        if let Some(page_guard) = bpm.fetch_page_guarded(self.rid.get_page_id()) {
            if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                table_page.access(|page| {
                    let next_tuple_id = self.rid.get_slot_num() + 1;

                    if self.stop_at_rid.get_page_id() != INVALID_PAGE_ID {
                        assert!(
                            self.rid.get_page_id() < self.stop_at_rid.get_page_id() ||
                                (self.rid.get_page_id() == self.stop_at_rid.get_page_id() &&
                                    next_tuple_id <= self.stop_at_rid.get_slot_num()),
                            "iterator out of bound"
                        );
                    }

                    self.rid = RID::new(self.rid.get_page_id(), next_tuple_id);

                    if self.rid == self.stop_at_rid {
                        self.rid = RID::new(INVALID_PAGE_ID, 0);
                    } else if next_tuple_id >= page.get_num_tuples() {
                        let next_page_id = page.get_next_page_id();
                        self.rid = RID::new(next_page_id, 0);
                    }
                });
            } else {
                log::error!("Failed to convert to TablePage");
                self.rid = RID::new(INVALID_PAGE_ID, 0);
            }
        } else {
            log::error!("Failed to fetch page");
            self.rid = RID::new(INVALID_PAGE_ID, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::types_db::type_id::TypeId::Integer;
    use crate::types_db::value::Value;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
        buffer_pool_size: usize,
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
                buffer_pool_size,
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

    fn setup_test_table() -> Arc<Mutex<TableHeap>> {
        let ctx = TestContext::new("bucket_page_integrity");
        let bpm = &ctx.bpm;

        Arc::new(Mutex::new(TableHeap::new(bpm.clone())))
    }

    #[test]
    fn test_table_iterator_empty() {
        let table_heap = setup_test_table();
        let mut iterator = TableIterator::new(table_heap, RID::new(0, 0), RID::new(INVALID_PAGE_ID, 0));
        assert!(iterator.is_end());
        assert_eq!(None, iterator.next());
    }

    #[test]
    fn test_table_iterator_single_tuple() {
        let table_heap = setup_test_table();
        let meta = TupleMeta::new(123, false);
        let schema = Schema::new(vec![Column::new("col_1", Integer), Column::new("col_2", Integer), Column::new("col_3", Integer)]);
        let rid = RID::new(0, 0);
        let mut tuple = Tuple::new(vec![Value::from(1), Value::from(2), Value::from(3)], schema.clone(), rid);
        let meta = TupleMeta::new(123 ,false);

        table_heap.lock().insert_tuple(&meta, &mut tuple, None, None, 0).expect("failed to insert tuple");

        let mut iterator = TableIterator::new(table_heap, rid, RID::new(INVALID_PAGE_ID, 0));
        assert!(!iterator.is_end());

        let result = iterator.next();
        assert!(result.is_some());
        let (result_meta, result_tuple) = result.unwrap();
        assert_eq!(result_meta, meta);
        assert_eq!(result_tuple, tuple);

        assert!(iterator.is_end());
        assert_eq!(iterator.next(), None);
    }

    // #[tests]
    // fn test_table_iterator_multiple_tuples() {
    //     let table_heap = setup_test_table();
    //     let tuples = vec![
    //         Tuple::new(vec![1, 2, 3]),
    //         Tuple::new(vec![4, 5, 6]),
    //         Tuple::new(vec![7, 8, 9]),
    //     ];
    //
    //     let mut rids = Vec::new();
    //     for tuple in &tuples {
    //         let meta = TupleMeta::new();
    //         let rid = table_heap.lock().insert_tuple(&meta, &mut tuple.clone(), None, None, 0).unwrap();
    //         rids.push(rid);
    //     }
    //
    //     let iterator = TableIterator::new(table_heap, rids[0], RID::new(INVALID_PAGE_ID, 0));
    //
    //     for (i, tuple) in tuples.iter().enumerate() {
    //         let result = iterator.next();
    //         assert!(result.is_some());
    //         let (_, result_tuple) = result.unwrap();
    //         assert_eq!(&result_tuple, tuple);
    //         if i < tuples.len() - 1 {
    //             assert!(!iterator.is_end());
    //         }
    //     }
    //
    //     assert!(iterator.is_end());
    //     assert_eq!(iterator.next(), None);
    // }
}