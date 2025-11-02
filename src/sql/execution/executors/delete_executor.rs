use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct DeleteExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<DeleteNode>,
    table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
    executed: bool,
    rows_deleted: usize,
}

impl DeleteExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<DeleteNode>) -> Self {
        debug!(
            "Creating DeleteExecutor for table '{}' with values plan",
            plan.get_table_name()
        );
        debug!("Target schema: {:?}", plan.get_output_schema());

        // First, make a brief read to get the catalog reference
        debug!("Acquiring context read lock");
        let catalog = {
            let context_guard = context.read();
            debug!("Context lock acquired, getting catalog reference");
            context_guard.get_catalog().clone()
        };
        debug!("Released context read lock");

        // Then briefly read from catalog to get the TableInfo
        debug!("Acquiring catalog read lock");
        let table_heap = {
            let catalog_guard = catalog.read();
            debug!("Catalog lock acquired, getting table info");
            let table_info = catalog_guard
                .get_table(plan.get_table_name())
                .unwrap_or_else(|| panic!("Table {} not found", plan.get_table_name()));
            debug!(
                "Found table '{}' with schema: {:?}",
                plan.get_table_name(),
                table_info.get_table_schema()
            );

            // Create TransactionalTableHeap
            Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ))
        };
        debug!("Released catalog read lock");

        debug!(
            "Successfully created DeleteExecutor for table '{}'",
            plan.get_table_name()
        );

        Self {
            context,
            plan,
            table_heap,
            initialized: false,
            child_executor: None,
            executed: false,
            rows_deleted: 0,
        }
    }
}

impl AbstractExecutor for DeleteExecutor {
    fn init(&mut self) {
        debug!("Initializing DeleteExecutor");

        // Create child executor from plan's children if not already created
        if self.child_executor.is_none() {
            let children_plans = self.plan.get_children();
            if !children_plans.is_empty() {
                // Get the first child plan (typically a Values or Scan node)
                match children_plans[0].create_executor(self.context.clone()) {
                    Ok(mut child_executor) => {
                        child_executor.init();
                        self.child_executor = Some(child_executor);
                        debug!("Child executor created and initialized");
                    }
                    Err(e) => {
                        error!("Failed to create child executor: {}", e);
                    }
                }
            }
        } else if let Some(ref mut child) = self.child_executor {
            child.init();
        }

        self.initialized = true;
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            warn!("DeleteExecutor not initialized, initializing now");
            self.init();
        }

        if self.executed {
            return Ok(None);
        }

        debug!("Executing delete operation");
        self.executed = true;

        // Get table information
        let table_id = self.plan.get_table_id();
        let table_info = {
            let binding = self.context.read();
            let catalog_guard = binding.get_catalog().read();
            catalog_guard
                .get_table_by_oid(table_id)
                .ok_or_else(|| {
                    DBError::Execution(format!("Table with OID {} not found", table_id))
                })?
                .clone()
        };

        let transactional_table_heap = self.table_heap.clone();
        let _schema = table_info.get_table_schema();

        // Get transaction context
        let txn_context = self.context.read().get_transaction_context();

        let mut delete_count = 0;

        // Process all tuples from child executor (scan/filter)
        if let Some(ref mut child) = self.child_executor {
            while let Some((_tuple, rid)) = child.next()? {
                debug!("Processing tuple for deletion with RID {:?}", rid);

                // Delete the tuple from the table
                match transactional_table_heap.delete_tuple(rid, txn_context.clone()) {
                    Ok(_) => {
                        delete_count += 1;
                        trace!(
                            "Successfully deleted tuple #{} with RID {:?}",
                            delete_count, rid
                        );
                    }
                    Err(e) => {
                        error!("Failed to delete tuple with RID {:?}: {}", rid, e);
                        return Err(DBError::Execution(format!("Delete failed: {}", e)));
                    }
                }
            }
        } else {
            return Err(DBError::Execution(
                "No child executor found for DELETE".to_string(),
            ));
        }

        info!(
            "Delete operation completed successfully, {} rows deleted",
            delete_count
        );
        self.rows_deleted = delete_count;

        // Return a result tuple if any rows were deleted
        if delete_count > 0 {
            // Create a simple result tuple indicating success with the number of rows deleted
            let result_values = vec![Value::new(delete_count as i32)];
            // Create a simple schema for the result tuple with just one column for the count
            let result_schema = Schema::new(vec![Column::new("rows_deleted", TypeId::Integer)]);
            let result_tuple = Arc::new(Tuple::new(&result_values, &result_schema, RID::default()));
            Ok(Some((result_tuple, RID::default())))
        } else {
            // No rows were deleted
            Ok(None)
        }
    }

    fn get_output_schema(&self) -> &Schema {
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
    use crate::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
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
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
            // Set transaction state to Running
            transaction.set_state(TransactionState::Running);

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            // Create catalog with transaction manager
            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(), // Pass transaction manager
            )));
            Self {
                bpm,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
                transaction_manager,
            }
        }

        // Add helper method to create new transactions
        fn create_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
            // Use a simple atomic counter for test transaction IDs
            static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

            let txn_id = NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst);
            let txn = Arc::new(Transaction::new(txn_id, isolation_level));
            // Set transaction state to Running
            txn.set_state(TransactionState::Running);

            Arc::new(TransactionContext::new(
                txn,
                Arc::new(LockManager::new()),
                self.transaction_manager.clone(),
            ))
        }
    }

    #[tokio::test]
    async fn test_delete_executor() {
        let ctx = TestContext::new("test_delete_executor").await;

        // Create execution context with a running transaction
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create a simple table schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        // Create table in catalog
        let table_name = "test_table";
        execution_context
            .write()
            .get_catalog()
            .write()
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Get table info and create TransactionalTableHeap
        let (table_oid, txn_table_heap) = {
            // Hold the execution context lock
            let exec_guard = execution_context.read();
            // Hold the catalog lock
            let catalog_guard = exec_guard.get_catalog().read();
            let table_info = catalog_guard
                .get_table(table_name)
                .expect("Table not found");

            // Create values that will live beyond the lock scope
            let oid = table_info.get_table_oidt();
            let txn_table_heap = Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ));

            (oid, txn_table_heap)
        };

        // Insert test data
        let test_data = vec![
            (1, "one"),
            (2, "two"),
            (3, "three"),
            (4, "four"),
            (5, "five"),
        ];

        for (id, value) in test_data {
            let values = vec![Value::new(id), Value::new(value)];

            txn_table_heap
                .insert_tuple_from_values(values, &schema, ctx.transaction_context.clone())
                .expect("Failed to insert tuple");
        }

        // Verify initial table state
        let mut initial_count = 0;
        let mut table_iter = txn_table_heap.make_iterator(Some(ctx.transaction_context.clone()));
        for (meta, tuple) in &mut table_iter {
            if !meta.is_deleted() {
                initial_count += 1;
                let id = tuple.get_value(0);
                let value = tuple.get_value(1);
                debug!("Found tuple with id: {:?}, value: {}", id, value);
            }
        }
        assert_eq!(initial_count, 5, "Should have 5 tuples initially");

        // Create a table scan node to scan the table for tuples to delete
        let table_scan_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            table_oid,
            table_name.to_string(),
        ));

        // Create delete plan with table scan as child
        let delete_plan = Arc::new(DeleteNode::new(
            schema.clone(),
            table_name.to_string(),
            table_oid,
            vec![table_scan_plan],
        ));

        // Create and execute delete executor
        let mut delete_executor = DeleteExecutor::new(execution_context.clone(), delete_plan);

        // Execute the delete operation (this will delete ALL tuples since we don't have a filter)
        delete_executor.init();
        let result = delete_executor.next();
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Delete returns Some with row count when rows are deleted

        // Commit the transaction after deletes
        let txn_ctx = execution_context.read().get_transaction_context();
        ctx.transaction_manager
            .commit(txn_ctx.get_transaction(), ctx.bpm.clone())
            .await;

        // Create new transaction for verification
        let verify_txn_ctx = ctx.create_transaction(IsolationLevel::ReadCommitted);

        // Verify all tuples are deleted
        let mut remaining_count = 0;
        let mut table_iter = txn_table_heap.make_iterator(Some(verify_txn_ctx.clone()));

        for (meta, tuple) in &mut table_iter {
            // Only count non-deleted tuples
            if !meta.is_deleted() {
                remaining_count += 1;
                debug!("Found remaining tuple with id: {:?}", tuple.get_value(0));
            } else {
                debug!("Skipping deleted tuple with id: {:?}", tuple.get_value(0));
            }
        }

        assert_eq!(
            remaining_count, 0,
            "Should have 0 tuples remaining (all deleted)"
        );

        // Clean up - commit verification transaction
        ctx.transaction_manager
            .commit(verify_txn_ctx.get_transaction(), ctx.bpm.clone())
            .await;
    }
}
