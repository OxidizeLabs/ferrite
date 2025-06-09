use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;
use log::{debug, error, trace};
use parking_lot::RwLock;
use std::sync::Arc;

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
        if self.initialized {
            trace!("SeqScanExecutor already initialized");
            return;
        }

        trace!(
            "Initializing SeqScanExecutor for table: {} (OID: {})",
            self.plan.get_table_name(),
            self.plan.get_table_oid()
        );

        // Get table info from catalog using OID to ensure we get the correct table
        let table_info = {
            let context = self.context.read();
            let catalog = context.get_catalog();
            let catalog_guard = catalog.read();
            catalog_guard
                .get_table_by_oid(self.plan.get_table_oid())
                .cloned()
        };

        if let Some(table_info) = table_info {
            // Verify we have the correct table
            assert_eq!(
                table_info.get_table_oidt(),
                self.plan.get_table_oid(),
                "Table OID mismatch in SeqScanExecutor"
            );

            // Create table heap and iterator (table_heap should already be correct from constructor,
            // but recreate it here to be safe and ensure consistency)
            let table_heap = Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ));

            self.table_heap = table_heap.clone();

            // Get transaction context for the iterator
            let txn_ctx = {
                let context = self.context.read();
                Some(context.get_transaction_context())
            };

            // Create iterator with start/end RIDs and transaction context
            let first_page_id = table_heap.get_table_heap().get_first_page_id();
            self.iterator = Some(TableIterator::new(
                table_heap,
                RID::new(first_page_id, 0), // start_rid from table's first page
                RID::new(u64::MAX, u32::MAX), // end_rid (scan entire table)
                txn_ctx,
            ));
            self.initialized = true;
            trace!(
                "SeqScanExecutor initialized successfully for table OID {}",
                self.plan.get_table_oid()
            );
        } else {
            error!("Table with OID {} not found", self.plan.get_table_oid());
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            trace!("SeqScanExecutor not initialized, initializing now");
            self.init();
        }

        // Get iterator reference
        let iter = match self.iterator.as_mut() {
            Some(iter) => iter,
            None => {
                return Err(DBError::Execution("Iterator not initialized".to_string()));
            }
        };

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
            return Ok(Some((tuple, rid)));
        }

        trace!("Reached end of table scan");
        Ok(None)
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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
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
            let disk_manager = AsyncDiskManager::new(db_path.clone(), log_path.clone(), DiskManagerConfig::default()).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_manager_arc.clone(),
                replacer.clone(),
            ).unwrap());

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
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn create_test_values(
        id: i32,
        name: &str,
        age: i32,
    ) -> (TupleMeta, Vec<Value>) {
        let values = vec![
            Value::new(id),
            Value::new(name.to_string()),
            Value::new(age),
        ];
        let meta = TupleMeta::new(0);
        (meta, values)
    }

    fn create_execution_context(
        ctx: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context.clone(),
        )))
    }

    #[tokio::test]
    async fn test_seq_scan_executor_with_data() {
        let ctx = TestContext::new("test_seq_scan_executor_with_data").await;

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create transaction and table
        let table_name = "test_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();
        let table_heap = table_info.get_table_heap();
        let table_heap_guard = table_heap;

        // Insert test data
        let test_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, values) = create_test_values(*id, name, *age);
            table_heap_guard
                .insert_tuple_from_values(values, &schema, Arc::from(meta))
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Test scanning
        let mut found_tuples = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
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

    #[tokio::test]
    async fn test_seq_scan_executor_empty_table() {
        let ctx = TestContext::new("test_seq_scan_executor_empty").await;
        let _transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create transaction and empty table
        let table_name = "empty_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seq_scan_executor_single_tuple() {
        let ctx = TestContext::new("test_seq_scan_executor_single").await;
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create table
        let table_name = "single_tuple_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Insert a single tuple
        let table_heap = table_info.get_table_heap();
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        let values = vec![Value::new(1), Value::new("Alice")];
        transactional_table_heap
            .insert_tuple_from_values(values, &schema, transaction_context.clone())
            .unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();

        // Should get one tuple
        let result = executor.next().unwrap();
        assert!(result.is_some());

        let (tuple, _rid) = result.unwrap();
        assert_eq!(tuple.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seq_scan_executor_multiple_tuples() {
        let ctx = TestContext::new("test_seq_scan_executor_multiple").await;
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create table
        let table_name = "multiple_tuples_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Insert multiple tuples
        let table_heap = table_info.get_table_heap();
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        let test_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(25)],
            vec![Value::new(2), Value::new("Bob"), Value::new(30)],
            vec![Value::new(3), Value::new("Charlie"), Value::new(35)],
        ];

        for values in test_data {
            transactional_table_heap
                .insert_tuple_from_values(values, &schema, transaction_context.clone())
                .unwrap();
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();

        // Count tuples
        let mut tuple_count = 0;
        loop {
            match executor.next().unwrap() {
                Some((tuple, _rid)) => {
                    tuple_count += 1;
                    assert_eq!(tuple.get_values().len(), 3);
                }
                None => break,
            }
        }

        assert_eq!(tuple_count, 3);
    }
}
