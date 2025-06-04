use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::common::exception::DBError;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::planner::schema_manager::SchemaManager;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct InsertExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<InsertNode>,
    txn_table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
    schema_manager: SchemaManager,
    executed: bool,
    rows_inserted: usize,
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
        let txn_table_heap = {
            let catalog_guard = catalog.read();
            debug!("Catalog lock acquired, getting table info");
            let table_info = catalog_guard
                .get_table(&plan.get_table_name())
                .expect(&format!("Table '{}' not found", plan.get_table_name()));
            debug!(
                "Found table '{}' with schema: {:?}",
                plan.get_table_name(),
                table_info.get_table_schema()
            );
            Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ))
        };
        debug!("Released catalog read lock");

        debug!(
            "Successfully created InsertExecutor for table '{}'",
            plan.get_table_name()
        );

        Self {
            context,
            plan,
            txn_table_heap,
            initialized: false,
            child_executor: None,
            schema_manager: SchemaManager::new(),
            executed: false,
            rows_inserted: 0,
        }
    }
}

impl AbstractExecutor for InsertExecutor {
    fn init(&mut self) {
        debug!("Initializing InsertExecutor");
        
        // Create child executor from plan's children if not already created
        if self.child_executor.is_none() {
            let children_plans = self.plan.get_children();
            if !children_plans.is_empty() {
                // Get the first child plan (typically a Values node)
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
            warn!("InsertExecutor not initialized, initializing now");
            self.init();
        }

        if self.executed {
            return Ok(None);
        }

        debug!("Executing insert operation");
        self.executed = true;

        // Get catalog and table information
        let (schema, table_heap, table_oidt, txn_context) = {
            let binding = self.context.read();
            let catalog_guard = binding.get_catalog().read();
            let table_info = catalog_guard
                .get_table(&self.plan.get_table_name())
                .ok_or_else(|| DBError::TableNotFound(self.plan.get_table_name().to_string()))?;

            let schema = table_info.get_table_schema().clone();
            let table_heap = table_info.get_table_heap().clone();
            let table_oidt = table_info.get_table_oidt();
            let txn_context = binding.get_transaction_context().clone();
            
            (schema, table_heap, table_oidt, txn_context)
        };

        // Create transactional table heap
        let transactional_table_heap = TransactionalTableHeap::new(table_heap, table_oidt);

        let mut insert_count = 0;

        // Check if we have direct values to insert
        let values_to_insert = self.plan.get_input_values();
        if !values_to_insert.is_empty() {
            debug!("Inserting {} direct value sets", values_to_insert.len());
            
            for values in values_to_insert {
                match transactional_table_heap.insert_tuple_from_values(
                    values.clone(),
                    &schema,
                    txn_context.clone(),
                ) {
                    Ok(_rid) => {
                        insert_count += 1;
                        trace!("Successfully inserted tuple #{}", insert_count);
                    }
                    Err(e) => {
                        error!("Failed to insert tuple: {}", e);
                        return Err(DBError::Execution(format!("Insert failed: {}", e)));
                    }
                }
            }
        } else if let Some(ref mut child) = self.child_executor {
            debug!("Inserting from child executor (SELECT query)");
            
            loop {
                match child.next()? {
                    Some((tuple, _)) => {
                        let values = tuple.get_values().clone();
                        match transactional_table_heap.insert_tuple_from_values(
                            values,
                            &schema,
                            txn_context.clone(),
                        ) {
                            Ok(_rid) => {
                                insert_count += 1;
                                trace!("Successfully inserted tuple #{} from SELECT", insert_count);
                            }
                            Err(e) => {
                                error!("Failed to insert tuple from SELECT: {}", e);
                                return Err(DBError::Execution(format!("Insert from SELECT failed: {}", e)));
                            }
                        }
                    }
                    None => break,
                }
            }
        } else {
            return Err(DBError::Execution("No values or child executor found for INSERT".to_string()));
        }

        info!("Insert operation completed successfully, {} rows inserted", insert_count);
        self.rows_inserted = insert_count;

        // Insert operations don't return tuples to the caller
        Ok(None)
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
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));

            // Create buffer pool manager
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create catalog with transaction manager
            let catalog = Arc::new(RwLock::new(Catalog::new(
                Arc::clone(&buffer_pool),
                transaction_manager.clone(), // Pass transaction manager
            )));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                catalog,
                buffer_pool,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        fn create_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
            Arc::new(RwLock::new(ExecutionContext::new(
                Arc::clone(&self.buffer_pool),
                Arc::clone(&self.catalog),
                Arc::clone(&self.transaction_context),
            )))
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

        // Create insert plan with empty values vector (values come from child)
        let insert_plan = Arc::new(InsertNode::new(
            schema,
            table_oid,
            table_name.to_string(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        // Create executor context and insert executor
        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.unwrap().is_none(), "Insert should return None");

        // Verify no more tuples
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_multiple_rows() {
        let test_ctx = TestContext::new("insert_multiple_rows");

        // Create executor context with the new transaction context
        let exec_ctx = test_ctx.create_executor_context();

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

        let mut executor = InsertExecutor::new(exec_ctx.clone(), insert_plan);

        // Execute inserts
        executor.init();
        
        // Insert operations complete in one call and return None
        assert!(executor.next().unwrap().is_none());

        // No more calls needed
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_empty_values() {
        let test_ctx = TestContext::new("insert_empty_values");

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_name = "test_table_empty".to_string();

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create empty values
        let expressions: Vec<Vec<Arc<Expression>>> = vec![];

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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();

        // Should return None immediately since there are no values
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_different_data_types() {
        let test_ctx = TestContext::new("insert_different_types");

        // Create schema with various data types
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
            Column::new("score", TypeId::Decimal),
        ]);

        let table_name = "test_table_types".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with different types
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(42),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Alice"),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("active", TypeId::Boolean),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(95.5),
                Column::new("score", TypeId::Decimal),
                vec![],
            ))),
        ]];

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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.unwrap().is_none(), "Insert should return None");

        // Verify no more tuples
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_large_batch() {
        let test_ctx = TestContext::new("insert_large_batch");

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        let table_name = "test_table_large".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create a large batch of rows (100 rows)
        let mut expressions = Vec::new();
        for i in 0..100 {
            expressions.push(vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(i),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(format!("value_{}", i)),
                    Column::new("value", TypeId::VarChar),
                    vec![],
                ))),
            ]);
        }

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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute inserts
        executor.init();
        
        // Insert operations complete in one call and return None
        assert!(executor.next().unwrap().is_none());

        // No more calls needed
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_with_null_values() {
        let test_ctx = TestContext::new("insert_null_values");

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("optional_name", TypeId::VarChar),
        ]);

        let table_name = "test_table_nulls".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with null
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new_with_type(Val::Null, TypeId::VarChar),
                Column::new("optional_name", TypeId::VarChar),
                vec![],
            ))),
        ]];

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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.unwrap().is_none(), "Insert should return None");

        // Verify no more tuples
        assert!(executor.next().unwrap().is_none());
    }
}
