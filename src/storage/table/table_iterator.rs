use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::common::rid::RID;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error};
use std::sync::Arc;

/// An iterator over the tuples in a table.
#[derive(Debug)]
pub struct TableIterator {
    table_heap: Arc<TableHeap>,
    rid: RID,
    stop_at_rid: RID,
}

pub struct TableScanIterator {
    /// The underlying table iterator
    inner: TableIterator,
    /// Reference to table info
    table_info: Arc<TableInfo>,
}

impl TableIterator {
    pub fn new(table_heap: Arc<TableHeap>, rid: RID, stop_at_rid: RID) -> Self {
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
                        if self.rid.get_slot_num() >= page.get_num_tuples() as u32 {
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

    pub fn get_rid(&self) -> RID {
        self.rid
    }

    pub fn is_end(&self) -> bool {
        self.rid.get_page_id() == self.stop_at_rid.get_page_id()
            && self.rid.get_slot_num() >= self.stop_at_rid.get_slot_num()
            || self.rid.get_page_id() == INVALID_PAGE_ID
    }

    /// Checks if we need to move to the next page based on tuple position
    fn should_move_to_next_page(&self, page: &TablePage, next_tuple_id: u32) -> bool {
        next_tuple_id >= page.get_num_tuples() as u32
    }

    /// Gets the next RID when moving within the same page
    fn get_next_rid_same_page(&self, next_tuple_id: u32) -> RID {
        RID::new(self.rid.get_page_id(), next_tuple_id)
    }

    /// Gets the next RID when moving to a new page
    fn get_next_rid_new_page(&self, next_page_id: u32) -> RID {
        if next_page_id == INVALID_PAGE_ID as u32 {
            RID::new(INVALID_PAGE_ID, 0)
        } else {
            RID::new(next_page_id as PageId, 0)
        }
    }

    /// Checks if we've reached or passed the stop condition
    fn is_past_stop_point(&self, current_rid: RID) -> bool {
        if self.stop_at_rid.get_page_id() == INVALID_PAGE_ID {
            return false;
        }

        current_rid.get_page_id() > self.stop_at_rid.get_page_id()
            || (current_rid.get_page_id() == self.stop_at_rid.get_page_id()
            && current_rid.get_slot_num() >= self.stop_at_rid.get_slot_num())
    }

    /// Advances the iterator to the next position.
    fn advance(&mut self) {
        let bpm = self.table_heap.get_bpm();

        // Try to fetch the current page
        let page_guard = match bpm.fetch_page_guarded(self.rid.get_page_id()) {
            Some(guard) => guard,
            None => {
                error!("Failed to fetch page");
                self.rid = RID::new(INVALID_PAGE_ID, 0);
                return;
            }
        };

        // Try to convert to TablePage
        let table_page = match page_guard.into_specific_type::<TablePage, 8>() {
            Some(page) => page,
            None => {
                error!("Failed to convert to TablePage");
                self.rid = RID::new(INVALID_PAGE_ID, 0);
                return;
            }
        };

        // Process the page within the access closure
        table_page.access(|page| {
            let next_tuple_id = self.rid.get_slot_num() + 1;

            // Determine the next RID
            let next_rid = if self.should_move_to_next_page(page, next_tuple_id) {
                self.get_next_rid_new_page(page.get_next_page_id() as u32)
            } else {
                self.get_next_rid_same_page(next_tuple_id)
            };

            // Update the iterator's position
            self.rid = if self.is_past_stop_point(next_rid) {
                RID::new(INVALID_PAGE_ID, 0)
            } else {
                next_rid
            };
        });
    }
}

impl TableScanIterator {
    pub fn new(table_info: Arc<TableInfo>) -> Self {
        let table_heap = table_info.get_table_heap();
        let inner = TableIterator::new(table_heap, RID::new(0, 0), RID::new(INVALID_PAGE_ID, 0));

        Self { inner, table_info }
    }

    /// Check if scan has reached the end
    pub fn is_end(&self) -> bool {
        self.inner.is_end()
    }

    /// Get current RID
    pub fn get_rid(&self) -> RID {
        self.inner.get_rid()
    }

    /// Reset the iterator to start of table
    pub fn reset(&mut self) {
        let table_heap = self.table_info.get_table_heap();
        self.inner = TableIterator::new(table_heap, RID::new(0, 0), RID::new(INVALID_PAGE_ID, 0));
    }
}

impl Iterator for TableIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end() {
            debug!("Iterator has reached the end.");
            return None;
        }

        debug!("Attempting to get tuple with RID: {:?}", self.rid);
        let result = self.table_heap.get_tuple(self.rid);
        match &result {
            Ok(_) => debug!("Successfully retrieved tuple"),
            Err(e) => debug!("Failed to retrieve tuple: {:?}", e),
        }
        self.advance();
        Some(result.unwrap().clone())
    }
}

impl Iterator for TableScanIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
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
    use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanType};
    use crate::execution::plans::table_scan_plan::TableScanNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;

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
            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
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

    fn setup_test_table(test_name: &str) -> Arc<TableHeap> {
        let ctx = TestContext::new(test_name);
        let bpm = ctx.bpm.clone();
        Arc::new(TableHeap::new(bpm))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_table_info(
        name: &str,
        schema: Schema,
        table_heap: Arc<TableHeap>,
    ) -> TableInfo {
        TableInfo::new(
            schema,
            name.to_string(),
            table_heap,
            1, // table_oid
        )
    }
    #[test]
    fn test_table_iterator_create() {
        let table_heap = setup_test_table("test_table_iterator_create");
        let rid = RID::new(INVALID_PAGE_ID, 0);

        let iterator = TableIterator::new(table_heap, rid, rid);
        assert_eq!(iterator.get_rid(), rid);
    }

    #[test]
    fn test_table_iterator_empty() {
        let table_heap = setup_test_table("test_table_iterator_empty");
        let rid = RID::new(0, 0);

        let mut iterator = TableIterator::new(table_heap, rid, rid);
        assert!(iterator.is_end());
        assert_eq!(
            None,
            iterator.next(),
            "Testing TableIterator returns none on empty table"
        );
    }

    #[test]
    fn test_table_iterator_single_tuple() {
        let table_heap = setup_test_table("test_table_iterator_single_tuple");
        let schema = Schema::new(vec![
            Column::new("col_1", TypeId::Integer),
            Column::new("col_2", TypeId::Integer),
            Column::new("col_3", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);
        let mut tuple = Tuple::new(
            &*vec![Value::from(1), Value::from(2), Value::from(3)],
            schema.clone(),
            rid,
        );
        let meta = TupleMeta::new(123, false);

        table_heap
            .insert_tuple(&meta, &mut tuple)
            .expect("failed to insert tuple");

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

    #[test]
    fn test_table_iterator_multiple_tuples() {
        let table_heap = setup_test_table("test_table_iterator_single_tuple");
        let schema = Schema::new(vec![
            Column::new("col_1", TypeId::Integer),
            Column::new("col_2", TypeId::Integer),
            Column::new("col_3", TypeId::Integer),
        ]);
        let rid_1 = RID::new(0, 0);
        let rid_2 = RID::new(0, 1);

        let mut tuple_1 = Tuple::new(
            &*vec![Value::from(1), Value::from(2), Value::from(3)],
            schema.clone(),
            rid_1,
        );

        let mut tuple_2 = Tuple::new(
            &*vec![Value::from(4), Value::from(5), Value::from(6)],
            schema.clone(),
            rid_2,
        );

        let meta_1 = TupleMeta::new(123, false);
        let meta_2 = TupleMeta::new(124, false);

        table_heap
            .insert_tuple(&meta_1, &mut tuple_1)
            .expect("failed to insert tuple 1");

        // insert tuple twice
        table_heap
            .insert_tuple(&meta_2, &mut tuple_2)
            .expect("failed to insert tuple 2");

        let mut iterator = TableIterator::new(table_heap, rid_1, RID::new(INVALID_PAGE_ID, 1));
        assert!(!iterator.is_end());

        let result = iterator.next();
        assert!(result.is_some());
        let (result_meta, result_tuple) = result.unwrap();
        assert_eq!(result_meta, meta_1);
        assert_eq!(result_tuple, tuple_1);

        let result2 = iterator.next();
        assert!(result2.is_some());
        let (result_meta2, result_tuple2) = result2.unwrap();
        assert_eq!(result_meta2, meta_2);
        assert_eq!(result_tuple2, tuple_2);

        assert!(iterator.is_end());
        assert_eq!(iterator.next(), None);
    }

    #[test]
    fn test_table_scan_creation() {
        let schema = create_test_schema();
        let table_heap = setup_test_table("test_table_scan_creation");
        let table_info = create_test_table_info("users", schema.clone(), table_heap);

        let scan = TableScanNode::new(table_info, Arc::from(schema), Some("u".to_string()));

        assert_eq!(scan.get_type(), PlanType::TableScan);
        assert_eq!(scan.get_table_name(), "users");
        assert_eq!(scan.get_table_alias(), Some("u"));
    }

    #[test]
    fn test_table_scan_iterator() {
        let schema = create_test_schema();
        let table_heap = setup_test_table("test_table_scan_iterator");
        let table_info = create_test_table_info("users", schema.clone(), table_heap);

        let scan = TableScanNode::new(table_info, Arc::from(schema), None);
        let mut iterator = scan.scan();

        // Test empty table
        assert!(iterator.next().is_none());

        // Reset and test again
        iterator.reset();
        assert!(iterator.next().is_none());
    }
}
