use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::values_executor::ValuesExecutor;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;
use log::{debug, error, warn};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::concurrency::transaction::{Transaction, TransactionState};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::concurrency::lock_manager::LockManager;
use crate::sql::execution::transaction_context::TransactionContext;
use std::sync::atomic::{AtomicU64, Ordering};


pub struct DeleteExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<DeleteNode>,
    table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
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
        }
    }
}

impl AbstractExecutor for DeleteExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug!(
            "Initializing DeleteExecutor for table '{}'",
            self.plan.get_table_name()
        );

        // Create the child executor from the values plan
        self.plan
            .get_children()
            .iter()
            .for_each(|child| match child {
                PlanNode::Values(values_plan) => {
                    debug!(
                        "Creating ValuesExecutor for delete with {} rows",
                        values_plan.get_rows().len()
                    );
                    debug!("Values schema: {:?}", values_plan.get_output_schema());

                    self.child_executor = Some(Box::new(ValuesExecutor::new(
                        self.context.clone(),
                        Arc::new(values_plan.clone()),
                    )));

                    debug!("Initializing child ValuesExecutor");
                    if let Some(child) = self.child_executor.as_mut() {
                        child.init();
                        debug!("Child ValuesExecutor initialized successfully");
                    }
                }
                _ => {
                    warn!("Unexpected child plan type for DeleteExecutor");
                }
            });
        self.initialized = true;
        debug!("DeleteExecutor initialization completed");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        if let Some(child) = &mut self.child_executor {
            // Get the next tuple to delete from the values executor
            if let Some((tuple_to_delete, _)) = child.next() {
                debug!("Processing delete for tuple: {:?}", tuple_to_delete);

                let txn_ctx = {
                    let context = self.context.read();
                    context.get_transaction_context().clone()
                };

                // Get the ID we want to delete
                let id_to_delete = tuple_to_delete.get_value(0).get_val();
                debug!("Looking for tuple with ID: {:?}", id_to_delete);

                // Scan the table to find the matching tuple
                let mut table_iter = self.table_heap.make_iterator(Some(txn_ctx.clone()));

                while let Some((_tuple_meta, tuple)) = table_iter.next() {
                    // Store the current ID and RID before any mutable borrows
                    let current_id = tuple.get_value(0).get_val().clone();
                    let rid = tuple.get_rid();

                    if current_id == *id_to_delete {
                        debug!("Found matching tuple with ID: {:?}", current_id);

                        // Delete the tuple
                        match self.table_heap.delete_tuple(rid, txn_ctx.clone()) {
                            Ok(_) => {
                                debug!("Successfully deleted tuple with ID: {:?}", &current_id);
                                return Some((tuple, rid));
                            }
                            Err(e) => {
                                error!("Failed to delete tuple: {}", e);
                                return None;
                            }
                        }
                    }
                }

                warn!("No matching tuple found for ID: {:?}", id_to_delete);
                self.next() // Try next value from values executor
            } else {
                debug!("No more tuples to delete from values executor");
                None
            }
        } else {
            error!("No child executor available for delete operation");
            None
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
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::sql::execution::expressions::abstract_expression::Expression::Constant;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
        transaction_manager: Arc<TransactionManager>,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
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
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
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
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
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

    #[test]
    fn test_delete_executor() {
        let ctx = TestContext::new("test_delete_executor");

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
        let (table_oid, table_heap) = {
            // Hold the execution context lock
            let exec_guard = execution_context.read();
            // Hold the catalog lock
            let catalog_guard = exec_guard.get_catalog().read();
            let table_info = catalog_guard.get_table(table_name).expect("Table not found");
            
            // Create values that will live beyond the lock scope
            let oid = table_info.get_table_oidt();
            let heap = Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ));
            
            (oid, heap)
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
            let mut tuple = Tuple::new(
                &vec![Value::new(id), Value::new(value.to_string())],
                schema.clone(),
                Default::default(),
            );

            let tuple_meta = TupleMeta::new(ctx.transaction_context.get_transaction_id());

            table_heap
                .insert_tuple(
                    &tuple_meta,
                    &mut tuple,
                    ctx.transaction_context.clone(),
                )
                .expect("Failed to insert tuple");
        }

        // Verify initial table state
        let mut initial_count = 0;
        let mut table_iter = table_heap.make_iterator(Some(ctx.transaction_context.clone()));
        while let Some((meta, tuple)) = table_iter.next() {
            if !meta.is_deleted() {
                initial_count += 1;
                let id = tuple.get_value(0);
                let value = tuple.get_value(1);
                debug!("Found tuple with id: {:?}, value: {}", id, value);
            }
        }
        assert_eq!(initial_count, 5, "Should have 5 tuples initially");

        // Convert values to expressions for delete operation (delete where id > 3)
        let values_to_delete = vec![
            vec![
                Arc::new(Constant(ConstantExpression::new(
                    Value::new(4),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Constant(ConstantExpression::new(
                    Value::new("four".to_string()),
                    Column::new("value", TypeId::VarChar),
                    vec![],
                ))),
            ],
            vec![
                Arc::new(Constant(ConstantExpression::new(
                    Value::new(5),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Constant(ConstantExpression::new(
                    Value::new("five".to_string()),
                    Column::new("value", TypeId::VarChar),
                    vec![],
                ))),
            ],
        ];

        let values_plan = PlanNode::Values(ValuesNode::new(
            schema.clone(),
            values_to_delete,
            vec![], // Empty children vector
        ));

        // Create delete plan
        let delete_plan = Arc::new(DeleteNode::new(
            schema.clone(),
            table_name.to_string(),
            table_oid,
            vec![values_plan],
        ));

        // Create and execute delete executor
        let mut delete_executor = DeleteExecutor::new(execution_context.clone(), delete_plan);

        // Count deleted tuples
        let mut delete_count = 0;
        while let Some((tuple, _)) = delete_executor.next() {
            delete_count += 1;
            // Verify the deleted tuple has id > 3
            let id = tuple.get_value(0).get_val();
            assert!(id > &Val::from(3), "Should only delete tuples with id > 3");
        }

        assert_eq!(delete_count, 2, "Should have deleted 2 tuples");

        // Commit the transaction after deletes
        let txn_ctx = execution_context.read().get_transaction_context();
        ctx.transaction_manager.commit(
            txn_ctx.get_transaction(),
            ctx.bpm.clone()
        );

        // Create new transaction for verification
        let verify_txn_ctx = ctx.create_transaction(IsolationLevel::ReadCommitted);
        
        // Verify remaining tuples
        let mut remaining_count = 0;
        let mut remaining_ids = Vec::new();
        let mut table_iter = table_heap.make_iterator(Some(verify_txn_ctx.clone()));

        while let Some((meta, tuple)) = table_iter.next() {
            // Only count and collect non-deleted tuples
            if !meta.is_deleted() {
                remaining_count += 1;
                remaining_ids.push(tuple.get_value(0).get_val().clone());
                debug!("Found remaining tuple with id: {:?}", tuple.get_value(0));
            } else {
                debug!("Skipping deleted tuple with id: {:?}", tuple.get_value(0));
            }
        }

        assert_eq!(remaining_count, 3, "Should have 3 tuples remaining");
        
        // Verify specific IDs remain
        assert!(remaining_ids.contains(&Val::from(1)), "ID 1 should still exist");
        assert!(remaining_ids.contains(&Val::from(2)), "ID 2 should still exist");
        assert!(remaining_ids.contains(&Val::from(3)), "ID 3 should still exist");

        // Clean up - commit verification transaction
        ctx.transaction_manager.commit(
            verify_txn_ctx.get_transaction(),
            ctx.bpm.clone()
        );
    }
}
