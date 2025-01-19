use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::values_executor::ValuesExecutor;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct InsertExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<InsertNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl InsertExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<InsertNode>) -> Self {
        debug!(
            "Creating InsertExecutor for table '{}' with values plan",
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
            table_info.get_table_heap()
        };
        debug!("Released catalog read lock");

        debug!(
            "Successfully created InsertExecutor for table '{}'",
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

impl AbstractExecutor for InsertExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("InsertExecutor already initialized");
            return;
        }

        debug!(
            "Initializing InsertExecutor for table '{}'",
            self.plan.get_table_name()
        );

        self.plan
            .get_children()
            .iter()
            .for_each(|child| match child {
                PlanNode::Values(values_plan) => {
                    debug!(
                        "Creating ValuesExecutor for insert with {} rows",
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
                    warn!("Unexpected child plan type for InsertExecutor");
                }
            });
        self.initialized = true;
        debug!("InsertExecutor initialization completed");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("InsertExecutor not initialized, initializing now");
            self.init();
        }

        // Get tuple from child executor (VALUES or SELECT)
        if let Some(child_executor) = self.child_executor.as_mut() {
            if let Some((mut tuple, _)) = child_executor.next() {
                debug!("Got tuple from child executor: {:?}", tuple.get_values());

                // Get transaction context
                let txn_ctx = self.context.read().get_transaction_context();
                
                // Create tuple metadata
                let tuple_meta = TupleMeta::new(txn_ctx.get_transaction_id());

                debug!(
                    "Inserting tuple with transaction ID {} into table '{}'",
                    tuple_meta.get_timestamp(),
                    self.plan.get_table_name()
                );

                // Insert tuple into table heap with transaction context
                match self.table_heap.insert_tuple(&tuple_meta, &mut tuple, Some(txn_ctx)) {
                    Ok(rid) => {
                        info!(
                            "Successfully inserted tuple into '{}' at RID {:?}: {:?}",
                            self.plan.get_table_name(),
                            rid,
                            tuple.get_values()
                        );

                        Some((tuple, rid))
                    }
                    Err(e) => {
                        error!(
                            "Failed to insert tuple into table '{}': {}",
                            self.plan.get_table_name(),
                            e
                        );
                        None
                    }
                }
            } else {
                debug!("No more tuples available from child executor");
                None
            }
        } else {
            error!("No child executor available for insert operation");
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
    use crate::common::result_writer::CliResultWriter;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
    use crate::recovery::log_manager::LogManager;
    use crate::sql::execution::execution_engine::ExecutionEngine;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        transaction_factory: Arc<TransactionManagerFactory>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            // Create disk manager and scheduler
            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));

            // Create buffer pool manager
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                Arc::clone(&buffer_pool),
                0,
                0,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )));

            let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                Arc::clone(&catalog),
                log_manager.clone(),
            )));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager.clone())));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            let transaction_factory = Arc::new(TransactionManagerFactory::new(
                Arc::clone(&catalog),
                log_manager,
            ));

            Self {
                catalog,
                buffer_pool,
                transaction_context,
                transaction_manager,
                transaction_factory,
                lock_manager,
                db_file,
                log_file,
            }
        }

        fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.log_file);
        }

        fn create_executor_context(
            &self,
            isolation_level: IsolationLevel,
        ) -> Arc<RwLock<ExecutionContext>> {
            let transaction = Arc::new(Transaction::new(0, isolation_level));
            Arc::new(RwLock::new(ExecutionContext::new(
                Arc::clone(&self.buffer_pool),
                Arc::clone(&self.catalog),
                Arc::clone(&self.transaction_context),
            )))
        }

        fn create_execution_engine(&self) -> Arc<RwLock<ExecutionEngine>> {
            Arc::new(RwLock::new(ExecutionEngine::new(
                Arc::clone(&self.catalog),
                Arc::clone(&self.buffer_pool),
                Arc::clone(&self.transaction_factory),
            )))
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_insert_single_row() {
        let test_ctx = TestContext::new("insert_single_row");

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_name = "test_table".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values to insert
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("test"),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
        ]];

        // Create values plan node
        let values_node = Arc::new(ValuesNode::new(
            schema.clone(),
            expressions,
            vec![PlanNode::Empty],
        ));

        // Get table info for insert plan
        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name.as_str())
                .expect("Table not found")
                .get_table_oidt()
        };

        // Create insert plan
        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        // Create executor context and insert executor
        let exec_ctx = test_ctx.create_executor_context(IsolationLevel::ReadUncommitted);
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();

        // Verify inserted values
        assert_eq!(*tuple.get_value(0), Value::from(1));
        assert_eq!(*tuple.get_value(1), Value::from("test"));

        // Verify no more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_multiple_rows() {
        let test_ctx = TestContext::new("insert_multiple_rows");

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_name = "test_table_multi".to_string();

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create multiple rows
        let expressions = vec![
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            )))],
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2),
                Column::new("id", TypeId::Integer),
                vec![],
            )))],
        ];

        let values_node = Arc::new(ValuesNode::new(
            schema.clone(),
            expressions,
            vec![PlanNode::Empty],
        ));

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name.as_str())
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context(IsolationLevel::ReadUncommitted);
        let mut executor = InsertExecutor::new(exec_ctx.clone(), insert_plan);

        // Execute inserts
        executor.init();

        // First row
        let (tuple1, _) = executor.next().expect("Expected first tuple");
        assert_eq!(*tuple1.get_value(0), Value::from(1));

        // Second row
        let (tuple2, _) = executor.next().expect("Expected second tuple");
        assert_eq!(*tuple2.get_value(0), Value::from(2));

        // No more rows
        assert!(executor.next().is_none());

        // Commit the transaction
        {
            let exec_ctx_guard = exec_ctx.read();
            let mut txn_manager = test_ctx.transaction_manager.write();
            assert!(txn_manager.commit(
                exec_ctx_guard
                    .get_transaction_context()
                    .get_transaction()
                    .clone()
            ));
        }
    }

    #[test]
    fn test_insert_transaction_rollback() {
        let test_ctx = TestContext::new("insert_rollback");
        let table_name = "test_rollback_table".to_string();

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        let expressions = vec![vec![Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(1), Column::new("id", TypeId::Integer), vec![]),
        ))]];

        let values_node = Arc::new(ValuesNode::new(
            schema.clone(),
            expressions,
            vec![PlanNode::Empty],
        ));

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name.as_str())
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        // Create a new transaction context
        let exec_ctx = test_ctx.create_executor_context(IsolationLevel::ReadUncommitted);
        let mut executor = InsertExecutor::new(exec_ctx.clone(), insert_plan);

        // Execute insert
        executor.init();
        let (tuple, _) = executor.next().expect("Expected tuple");
        assert_eq!(*tuple.get_value(0), Value::from(1));

        // Rollback the transaction
        {
            let exec_ctx_guard = exec_ctx.read();
            let txn_ctx = exec_ctx_guard.get_transaction_context();
            let mut txn_manager = test_ctx.transaction_manager.write();
            txn_manager.abort(txn_ctx.get_transaction());
        }

        // Verify the rollback by checking the table is empty
        let verify_exec_ctx = test_ctx.create_executor_context(IsolationLevel::ReadUncommitted);
        let mut writer = CliResultWriter::new();
        let select_sql = "SELECT COUNT(*) FROM test_rollback_table;";

        let execution_engine = test_ctx.create_execution_engine();

        let result = execution_engine
            .write()
            .execute_sql(select_sql, verify_exec_ctx, &mut writer);

        assert!(result.is_ok(), "Failed to execute verification query");
    }
}
