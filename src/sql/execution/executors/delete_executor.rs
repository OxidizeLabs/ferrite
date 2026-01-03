//! # Delete Executor Module
//!
//! This module implements the executor for SQL `DELETE` statements, which
//! remove tuples from tables based on selection criteria.
//!
//! ## SQL Syntax
//!
//! ```sql
//! DELETE FROM <table_name> [WHERE <condition>]
//! ```
//!
//! ## Execution Model
//!
//! The delete executor follows a **pull-based model** with a child executor
//! that identifies tuples to delete:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                   DeleteExecutor                        │
//! │                                                         │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │ Child Executor (SeqScan/IndexScan + Filter)     │    │
//! │  │  └─ Produces RIDs of tuples to delete           │    │
//! │  └─────────────────────────────────────────────────┘    │
//! │                        │                                │
//! │                        ▼                                │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │ TransactionalTableHeap                          │    │
//! │  │  └─ Mark tuples as deleted (MVCC)               │    │
//! │  └─────────────────────────────────────────────────┘    │
//! │                        │                                │
//! │                        ▼                                │
//! │        Result: rows_deleted count                       │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## MVCC Semantics
//!
//! Deletes use soft-delete via the `TransactionalTableHeap`:
//! - Tuples are marked as deleted, not physically removed
//! - Deleted tuples remain visible to earlier transactions
//! - Physical cleanup happens during garbage collection
//!
//! ## Output
//!
//! Returns a single tuple with the count of deleted rows:
//! - Schema: `(rows_deleted INTEGER)`
//! - Returns `None` if zero rows were deleted

use std::sync::Arc;

use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;

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

/// Executor for SQL `DELETE` statements.
///
/// `DeleteExecutor` removes tuples from a table by marking them as deleted
/// in the transactional table heap. It works with a child executor that
/// identifies which tuples to delete (typically a scan with optional filter).
///
/// # Transaction Safety
///
/// All deletes are performed through `TransactionalTableHeap`, which:
/// - Records delete operations in the transaction's write set
/// - Supports rollback if the transaction aborts
/// - Maintains MVCC visibility for concurrent readers
///
/// # Output Schema
///
/// On successful deletion, returns a single tuple:
/// ```text
/// (rows_deleted: INTEGER)
/// ```
///
/// Returns `None` if no rows matched the delete criteria.
///
/// # Example
///
/// ```ignore
/// // DELETE FROM users WHERE age < 18
/// let scan_plan = SeqScanPlanNode::new(schema, table_oid, "users");
/// let filter_plan = FilterPlanNode::new(schema, predicate, scan_plan);
/// let delete_plan = DeleteNode::new(schema, "users", table_oid, vec![filter_plan]);
/// let executor = DeleteExecutor::new(context, Arc::new(delete_plan));
/// ```
pub struct DeleteExecutor {
    /// Shared execution context for catalog and transaction access.
    context: Arc<RwLock<ExecutionContext>>,
    /// Delete plan node containing table information.
    plan: Arc<DeleteNode>,
    /// Transactional table heap for MVCC-safe deletions.
    table_heap: Arc<TransactionalTableHeap>,
    /// Flag indicating whether `init()` has been called.
    initialized: bool,
    /// Child executor that identifies tuples to delete.
    child_executor: Option<Box<dyn AbstractExecutor>>,
    /// Flag indicating whether the delete has been executed.
    executed: bool,
    /// Count of rows deleted (for result tuple).
    rows_deleted: usize,
}

impl DeleteExecutor {
    /// Creates a new `DeleteExecutor` for the given delete plan.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context for catalog and transaction access.
    /// * `plan` - Delete plan node containing the target table and child plans.
    ///
    /// # Returns
    ///
    /// A new executor ready for initialization.
    ///
    /// # Panics
    ///
    /// Panics if the target table does not exist in the catalog. This is a
    /// programming error since the planner should validate table existence.
    ///
    /// # Lock Acquisition
    ///
    /// Briefly acquires read locks on:
    /// 1. Execution context (to get catalog reference)
    /// 2. Catalog (to get table information)
    ///
    /// Locks are released before returning to avoid holding them during execution.
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
    /// Initializes the delete executor and its child.
    ///
    /// Creates the child executor from the plan's children if not already
    /// present. The child executor (typically a scan + filter combination)
    /// identifies which tuples to delete.
    ///
    /// # Child Executor
    ///
    /// The first child plan is used to create the executor that produces
    /// tuples to delete. Common patterns:
    /// - `SeqScanPlanNode`: Delete all rows
    /// - `FilterPlanNode` → `SeqScanPlanNode`: Delete matching rows
    /// - `IndexScanPlanNode`: Delete by indexed lookup
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
                    },
                    Err(e) => {
                        error!("Failed to create child executor: {}", e);
                    },
                }
            }
        } else if let Some(ref mut child) = self.child_executor {
            child.init();
        }

        self.initialized = true;
    }

    /// Executes the delete operation and returns a row count.
    ///
    /// Pulls all tuples from the child executor and marks each one as
    /// deleted in the transactional table heap. This is a **pipeline breaker**
    /// that processes all deletions before returning.
    ///
    /// # Execution Flow
    ///
    /// 1. Retrieve table info from catalog
    /// 2. Pull tuples from child executor
    /// 3. For each tuple, call `delete_tuple` on the table heap
    /// 4. Return count of deleted rows
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Tuple with `rows_deleted` count (if > 0)
    /// * `Ok(None)` - No rows were deleted, or already executed
    /// * `Err(DBError::Execution)` - Delete operation failed
    ///
    /// # Errors
    ///
    /// | Condition                | Error                              |
    /// |--------------------------|------------------------------------|
    /// | Table not found          | `DBError::Execution`               |
    /// | No child executor        | `DBError::Execution`               |
    /// | Delete tuple failed      | `DBError::Execution`               |
    ///
    /// # Transaction Behavior
    ///
    /// All deletions are recorded in the current transaction's write set.
    /// If the transaction aborts, deletions are rolled back automatically.
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
                    },
                    Err(e) => {
                        error!("Failed to delete tuple with RID {:?}: {}", rid, e);
                        return Err(DBError::Execution(format!("Delete failed: {}", e)));
                    },
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

    /// Returns the output schema for delete results.
    ///
    /// The actual result tuple uses a simplified schema with just
    /// `rows_deleted INTEGER`. This returns the plan's schema for
    /// compatibility with the executor interface.
    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to the catalog and transaction context for
    /// delete operations.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tempfile::TempDir;

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
