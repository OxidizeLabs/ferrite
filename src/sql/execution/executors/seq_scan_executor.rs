use crate::catalog::schema::Schema;
use crate::common::config::PageId;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::Tuple;
use log::{debug, error, trace};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;

pub struct SeqScanExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<SeqScanPlanNode>,
    table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    iterator: Option<TableIterator>,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl SeqScanExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<SeqScanPlanNode>) -> Self {
        let table_name = plan.get_table_name();
        trace!("Creating SeqScanExecutor for table '{}'", table_name);

        // Get the table heap using table OID
        let table_heap = {
            let context_guard = context.read();
            let catalog = context_guard.get_catalog();
            let catalog_guard = catalog.read();

            // Use table OID to get the table info
            let table_oid = plan.get_table_oid();
            debug!("Looking up table with OID: {}", table_oid);

            match catalog_guard.get_table_by_oid(table_oid) {
                Some(table_info) => {
                    trace!("Found table with OID {} in catalog", table_oid);
                    // Create TransactionalTableHeap with table_oid
                    Arc::new(TransactionalTableHeap::new(
                        table_info.get_table_heap(),
                        table_oid, // Pass table_oid instead of txn_ctx
                    ))
                }
                None => {
                    error!("Table with OID {} not found in catalog", table_oid);
                    panic!("Table not found");
                }
            }
        };

        Self {
            context,
            plan,
            table_heap,
            iterator: None,
            initialized: false,
            child_executor: None,
        }
    }
}

impl AbstractExecutor for SeqScanExecutor {
    fn init(&mut self) {
        self.initialized = false;

        trace!(
            "Initializing SeqScanExecutor for table '{}'",
            self.plan.get_table_name()
        );

        // Get the table's first page ID from the underlying table heap
        let first_page_id = self.table_heap.get_table_heap().get_first_page_id();
        trace!("Table '{}' first page ID: {}", self.plan.get_table_name(), first_page_id);

        // Create iterator from first page to end of table
        let start_rid = RID::new(first_page_id, 0);
        let stop_rid = RID::new(u32::MAX as PageId, u32::MAX);
        trace!(
            "Creating table iterator with range: {:?} to {:?}",
            start_rid, stop_rid
        );

        // Create new iterator with TransactionalTableHeap
        self.iterator = Some(TableIterator::new(
            self.table_heap.clone(),  // Pass the TransactionalTableHeap directly
            start_rid,
            stop_rid,
            None,
        ));
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            trace!("SeqScanExecutor not initialized, initializing now");
            self.init();
        }

        // Get iterator reference
        let iter = self.iterator.as_mut()?;

        // Keep trying until we find a valid tuple or reach the end
        while let Some((meta, tuple)) = iter.next() {
            let rid = tuple.get_rid();
            trace!("Found tuple with RID {:?}", rid);

            // Skip deleted tuples
            if meta.is_deleted() {
                trace!("Skipping deleted tuple with RID {:?}", rid);
                continue;
            }

            // Return valid tuple
            return Some((tuple, rid));
        }

        trace!("Reached end of table scan");
        None
    }

    fn get_output_schema(&self) -> &Schema {
        trace!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm(),
            0,              // next_index_oid
            0,              // next_table_oid
            HashMap::new(), // tables
            HashMap::new(), // indexes
            HashMap::new(), // table_names
            HashMap::new(), // index_names
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn create_test_tuple(schema: &Schema, id: i32, name: &str, age: i32) -> (TupleMeta, Tuple) {
        let values = vec![
            Value::new(id),
            Value::new(name.to_string()),
            Value::new(age),
        ];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(0);
        (meta, tuple)
    }

    #[test]
    fn test_seq_scan_executor_with_data() {
        let ctx = TestContext::new("test_seq_scan_executor_with_data");
        let bpm = ctx.bpm();
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create transaction and table
        let table_name = "test_table";
        let table_info = catalog.create_table(table_name.to_string(), schema.clone()).unwrap();
        let table_heap = table_info.get_table_heap();
        let table_heap_guard = table_heap;

        // Insert test data
        let test_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, mut tuple) = create_test_tuple(&schema, *id, name, *age);
            table_heap_guard
                .insert_tuple(&meta, &mut tuple)
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&bpm),
            Arc::new(RwLock::new(catalog)),
            transaction_context,
        )));

        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Test scanning
        let mut found_tuples = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            // Get values from tuple using proper indexing
            let id = match tuple.get_value(0).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for id"),
            };

            let name = match tuple.get_value(1).get_val() {
                Val::VarLen(s) => s.clone(),
                _ => panic!("Expected VarLen value for name"),
            };

            let age = match tuple.get_value(2).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for age"),
            };

            found_tuples.push((id, name, age));
        }

        assert_eq!(found_tuples.len(), 3, "Should have scanned 3 tuples");

        // Verify each tuple was found
        for (id, name, age) in test_data {
            assert!(
                found_tuples.iter().any(|(t_id, t_name, t_age)| {
                    *t_id == id && t_name == name && *t_age == age
                }),
                "Did not find tuple: ({}, {}, {})",
                id,
                name,
                age
            );
        }
    }

    #[test]
    fn test_seq_scan_executor_empty_table() {
        let ctx = TestContext::new("test_seq_scan_executor_empty");
        let bpm = ctx.bpm();
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create transaction and empty table
        let table_name = "empty_table";
        let table_info = catalog.create_table(table_name.to_string(), schema.clone()).unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&bpm),
            Arc::new(RwLock::new(catalog)),
            transaction_context,
        )));

        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Should return None for empty table
        assert!(
            executor.next().is_none(),
            "Empty table should return no tuples"
        );
    }

    #[test]
    fn test_seq_scan_executor_reinitialization() {
        let ctx = TestContext::new("test_seq_scan_reinit");
        let bpm = ctx.bpm();
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create transaction and table
        let table_name = "test_reinit_table";
        let table_info = catalog.create_table(table_name.to_string(), schema.clone()).unwrap();
        let table_heap = table_info.get_table_heap();
        let table_heap_guard = table_heap;

        // Insert test data
        let test_data = vec![(1, "Alice", 25)];

        for (id, name, age) in test_data.iter() {
            let (meta, mut tuple) = create_test_tuple(&schema, *id, name, *age);
            table_heap_guard
                .insert_tuple(&meta, &mut tuple)
                .expect("Failed to insert tuple");
        }

        // Create executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&bpm),
            Arc::new(RwLock::new(catalog)),
            transaction_context,
        )));

        let mut executor = SeqScanExecutor::new(context, plan);

        // First initialization
        executor.init();
        assert!(executor.initialized);
        assert!(executor.next().is_some());
        assert!(executor.next().is_none());

        // Second initialization should be safe
        executor.init();
        assert!(executor.initialized);
        assert!(executor.next().is_some());
        assert!(executor.next().is_none());
    }
}
