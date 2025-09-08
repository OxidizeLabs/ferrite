use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::table::table_heap::TableHeap;
    use crate::types_db::type_id::TypeId;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    Arc::from(disk_manager.unwrap()),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager
            let transaction_manager = Arc::new(TransactionManager::new());

            Self {
                bpm,
                transaction_manager,
            }
        }
    }
    async fn setup_test_table(test_name: &str) -> (Arc<TableHeap>, Arc<TransactionManager>) {
        let ctx = TestContext::new(test_name).await;
        let bpm = ctx.bpm.clone();
        let txn_manager = ctx.transaction_manager.clone();
        let table_heap = Arc::new(TableHeap::new(bpm, 0));
        (table_heap, txn_manager)
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_table_info(name: &str, schema: Schema, table_heap: Arc<TableHeap>) -> TableInfo {
        TableInfo::new(
            schema,
            name.to_string(),
            table_heap,
            1, // table_oid
        )
    }

    #[tokio::test]
    async fn test_table_scan_creation() {
        let schema = create_test_schema();
        let (table_heap, _) = setup_test_table("test_table_scan_creation").await;
        let table_info = create_test_table_info("users", schema.clone(), table_heap);

        let scan = TableScanNode::new(table_info, Arc::from(schema), Some("u".to_string()));

        assert_eq!(scan.get_type(), PlanType::TableScan);
        assert_eq!(scan.get_table_name(), "users");
        assert_eq!(scan.get_table_alias(), Some("u"));
    }

    #[tokio::test]
    async fn test_table_scan_iterator() {
        let schema = create_test_schema();
        let (table_heap, _) = setup_test_table("test_table_scan_iterator").await;
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
