use crate::common::config::INVALID_PAGE_ID;
use crate::common::rid::RID;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::Mutex;
use std::sync::Arc;
use log::{debug, error};

/// An iterator over the tuples in a table.
#[derive(Debug)]
pub struct TableIterator<'a>{
    table_heap: &'a TableHeap,
    rid: RID,
    stop_at_rid: RID,
}

impl<'a> TableIterator<'a> {
    pub fn new(table_heap: &'a TableHeap, rid: RID, stop_at_rid: RID) -> Self {
        let mut iterator = Self {
            table_heap,
            rid,
            stop_at_rid,
        };
        debug!("New iterator created: {:?}", iterator);
        iterator.initialize();
        iterator
    }

    fn initialize(&mut self) {
        if self.rid.get_page_id() == INVALID_PAGE_ID {
            self.rid = RID::new(INVALID_PAGE_ID, 0);
        } else {
            let bpm = self.table_heap.get_bpm();
            if let Some(page_guard) = bpm.fetch_page_guarded(self.rid.get_page_id()) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        if self.rid.get_slot_num() >= page.get_num_tuples() {
                            self.rid = RID::new(INVALID_PAGE_ID, 0);
                        }
                    });
                } else {
                    error!("Failed to convert to TablePage");
                    self.rid = RID::new(INVALID_PAGE_ID, 0);
                }
            } else {
                error!("Failed to fetch page");
                self.rid = RID::new(INVALID_PAGE_ID, 0);
            }
        }
        debug!("Iterator initialized: {:?}", self);
    }

    /// Advances the iterator to the next position.
    fn advance(&mut self) {
        let bpm = self.table_heap.get_bpm();
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

impl<'a> Iterator for TableIterator<'a> {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end() {
            debug!("Iterator has reached the end.");
            return None;
        }

        let result = self.table_heap.get_tuple(self.rid);
        self.advance();
        Some(result.unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId::Integer;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::sync::Arc;

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

    fn setup_test_table(test_name: &str) -> TableHeap {
        let ctx = TestContext::new(test_name);
        let bpm = ctx.bpm.clone();

        TableHeap::new(bpm)
    }

    #[test]
    fn test_table_iterator_create() {
        let table_heap = setup_test_table("test_table_iterator_create");
        let rid = RID::new(INVALID_PAGE_ID, 0);

        let iterator = TableIterator::new(&table_heap, rid, rid);
        assert_eq!(iterator.get_rid(), rid);
    }


    #[test]
    fn test_table_iterator_empty() {
        let table_heap = setup_test_table("test_table_iterator_empty");
        let rid = RID::new(0, 0);

        let mut iterator = TableIterator::new(&table_heap, rid, rid);
        assert!(iterator.is_end());
        assert_eq!(None, iterator.next(), "Testing TableIterator returns none on empty table");
    }

    #[test]
    fn test_table_iterator_single_tuple() {
        let mut table_heap = setup_test_table("test_table_iterator_single_tuple");
        let schema = Schema::new(vec![Column::new("col_1", Integer), Column::new("col_2", Integer), Column::new("col_3", Integer)]);
        let rid = RID::new(0, 0);
        let mut tuple = Tuple::new(vec![Value::from(1), Value::from(2), Value::from(3)], schema.clone(), rid);
        let meta = TupleMeta::new(123, false);

        table_heap.insert_tuple(&meta, &mut tuple, None, None, 0).expect("failed to insert tuple");

        let mut iterator = TableIterator::new(&table_heap, rid, RID::new(INVALID_PAGE_ID, 0));
        assert!(!iterator.is_end());

        let result = iterator.next();
        assert!(result.is_some());
        let (result_meta, result_tuple) = result.unwrap();
        assert_eq!(result_meta, meta);
        assert_eq!(result_tuple, tuple);

        assert!(iterator.is_end());
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_table_iterator_multiple_tuples() {
        let mut table_heap = setup_test_table("test_table_iterator_multiple_tuples");
        let schema = Schema::new(vec![Column::new("col_1", Integer), Column::new("col_2", Integer), Column::new("col_3", Integer)]);
        let tuples = vec![
            Tuple::new(vec![Value::from(1), Value::from(2), Value::from(3)], schema.clone(), RID::new(0, 0)),
            Tuple::new(vec![Value::from(4), Value::from(5), Value::from(6)], schema.clone(), RID::new(0, 0)),
            Tuple::new(vec![Value::from(7), Value::from(8), Value::from(9)], schema.clone(), RID::new(0, 0)),
        ];

        let mut rids = Vec::new();
        for mut tuple in tuples.clone() {
            let meta = TupleMeta::new(123, false);
            let rid = table_heap.insert_tuple(&meta, &mut tuple, None, None, 0).expect("failed to insert tuple");
            rids.push(rid);
        }

        let mut iterator = table_heap.make_iterator();

        for (i, expected_tuple) in tuples.iter().enumerate() {
            let result = iterator.next();
            assert!(result.is_some(), "Failed to get tuple at index {}", i);
            let (_, result_tuple) = result.unwrap();
            assert_eq!(&result_tuple, expected_tuple, "Mismatch at index {}", i);
            if i < tuples.len() - 1 {
                assert!(!iterator.is_end(), "Iterator ended prematurely at index {}", i);
            }
        }

        assert!(iterator.is_end(), "Iterator did not end after all tuples");
        assert_eq!(iterator.next(), None, "Iterator returned unexpected tuple");
    }
}