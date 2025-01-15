use crate::catalog::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::storage::table::table_heap::TableInfo;
use crate::storage::table::table_iterator::TableScanIterator;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct TableScanNode {
    /// The table being scanned
    table_info: TableInfo,
    /// Output schema of the scan
    output_schema: Arc<Schema>,
    /// Table alias if any (used in query plans)
    table_alias: Option<String>,
}

impl TableScanNode {
    /// Create a new table scan node
    pub fn new(
        table_info: TableInfo,
        output_schema: Arc<Schema>,
        table_alias: Option<String>,
    ) -> Self {
        Self {
            table_info,
            output_schema,
            table_alias,
        }
    }

    /// Get the table metadata
    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    /// Get the table alias if any
    pub fn get_table_alias(&self) -> Option<&str> {
        self.table_alias.as_deref()
    }

    /// Get the actual table name
    pub fn get_table_name(&self) -> &str {
        self.table_info.get_table_name()
    }

    /// Create a scan iterator for this table
    pub fn scan(&self) -> TableScanIterator {
        TableScanIterator::new(Arc::from(self.table_info.clone()))
    }
}

impl AbstractPlanNode for TableScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static EMPTY: Vec<PlanNode> = Vec::new();
        &EMPTY
    }

    fn get_type(&self) -> PlanType {
        PlanType::TableScan
    }
}

impl Display for TableScanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ TableScan: {}", self.get_table_name())?;
        if let Some(alias) = &self.table_alias {
            write!(f, " as {}", alias)?;
        }

        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableHeap;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            // initialize_logger();
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

    // Add more tests for:
    // - Scanning tables with data
    // - Scanning across multiple pages
    // - Edge cases and error conditions
}
