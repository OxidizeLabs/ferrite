use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::executors::values_executor::ValuesExecutor;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::insert_plan::InsertNode;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct InsertExecutor {
    context: Arc<ExecutorContext>,
    plan: Arc<InsertNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl InsertExecutor {
    pub fn new(context: Arc<ExecutorContext>, plan: Arc<InsertNode>) -> Self {
        let table_name = plan.get_table_name();
        debug!("Creating insert executor for table: {}", table_name);

        let table_heap = {
            let catalog = context.get_catalog();
            let catalog_guard = catalog.read();
            let table_info = catalog_guard
                .get_table(table_name)
                .expect("Table not found");
            table_info.get_table_heap()
        };

        Self {
            context,
            plan,
            table_heap,
            initialized: false,
            child_executor: None,
        }
    }

    fn insert_tuple(
        &self,
        tuple_meta: &TupleMeta,
        tuple: &mut Tuple,
    ) -> Result<RID, String> {
        debug!("Inserting tuple into table: {}", self.plan.get_table_name());
        self.table_heap.insert_tuple(
            tuple_meta,
            tuple
        )
    }
}

impl AbstractExecutor for InsertExecutor {
    fn init(&mut self) {
        if !self.initialized {
            debug!("Initializing insert executor");
            match self.plan.get_child() {
                PlanNode::Values(values_plan) => {
                    // Initialize child executor
                    debug!("Creating values executor for insert");
                    self.child_executor = Some(Box::new(ValuesExecutor::new(
                        self.context.clone(),
                        Arc::new(values_plan.clone()),
                    )));
                    self.child_executor.as_mut().unwrap().init();
                }
                _ => {
                    warn!("Unexpected child plan type for insert executor");
                }
            }
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Get tuple from child executor (VALUES or SELECT)
        if let Some(child_executor) = self.child_executor.as_mut() {
            if let Some((mut tuple, _)) = child_executor.next() {
                debug!("Got tuple from child executor: {:?}", tuple);

                // Create tuple metadata
                let time_stamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_nanos() as u64;
                let tuple_meta = TupleMeta::new(time_stamp, false);

                // Insert tuple into table heap
                match self.insert_tuple(&tuple_meta, &mut tuple) {
                    Ok(rid) => {
                        info!("Successfully inserted tuple at RID: {:?}", rid);
                        Some((tuple, rid))
                    }
                    Err(e) => {
                        error!("Failed to insert tuple: {}", e);
                        None
                    }
                }
            } else {
                debug!("No more tuples from child executor");
                None
            }
        } else {
            error!("No child executor available");
            None
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::execution::plans::values_plan::ValuesNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::{Mutex, RwLock};

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool: Arc<BufferPoolManager>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
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

            // Create transaction manager
            let transaction_manager =
                Arc::new(Mutex::new(TransactionManager::new(Arc::clone(&catalog))));
            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));

            Self {
                catalog,
                buffer_pool,
                transaction_manager,
                lock_manager,
                db_file,
                log_file,
            }
        }

        fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.log_file);
        }

        fn create_executor_context(&self, isolation_level: IsolationLevel) -> Arc<ExecutorContext> {
            let transaction = Arc::new(Mutex::new(Transaction::new(0, isolation_level)));
            Arc::new(ExecutorContext::new(
                transaction,
                Arc::clone(&self.transaction_manager),
                Arc::clone(&self.catalog),
                Arc::clone(&self.buffer_pool),
                Arc::clone(&self.lock_manager),
            ))
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

        let table_name = "test_table";
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name, schema.clone())
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
        let values_node = Arc::new(
            ValuesNode::new(schema.clone(), expressions, PlanNode::Empty)
                .expect("Failed to create ValuesNode"),
        );

        // Get table info for insert plan
        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        // Create insert plan
        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            PlanNode::Values(values_node.as_ref().clone()),
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
        let table_name = "test_table_multi";

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name, schema.clone())
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

        let values_node = Arc::new(
            ValuesNode::new(schema.clone(), expressions, PlanNode::Empty)
                .expect("Failed to create ValuesNode"),
        );

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            PlanNode::Values(values_node.as_ref().clone()),
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
            let mut txn_manager = test_ctx.transaction_manager.lock();
            assert!(txn_manager.commit(exec_ctx.get_transaction().clone()));
        }
    }

    #[test]
    fn test_insert_transaction_rollback() {
        let test_ctx = TestContext::new("insert_rollback");
        let table_name = "test_rollback_table";

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name, schema.clone())
                .expect("Failed to create table");
        }

        let expressions = vec![vec![Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(1), Column::new("id", TypeId::Integer), vec![]),
        ))]];

        let values_node = Arc::new(
            ValuesNode::new(schema.clone(), expressions, PlanNode::Empty)
                .expect("Failed to create ValuesNode"),
        );

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            PlanNode::Values(values_node.as_ref().clone()),
        ));

        let exec_ctx = test_ctx.create_executor_context(IsolationLevel::ReadUncommitted);
        let mut executor = InsertExecutor::new(exec_ctx.clone(), insert_plan);

        // Execute insert
        executor.init();
        let (tuple, _) = executor.next().expect("Expected tuple");
        assert_eq!(*tuple.get_value(0), Value::from(1));

        // Rollback the transaction
        {
            let mut txn_manager = test_ctx.transaction_manager.lock();
            txn_manager.abort(exec_ctx.get_transaction().clone());
        }
    }
}
