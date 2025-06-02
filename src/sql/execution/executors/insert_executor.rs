use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::planner::schema_manager::SchemaManager;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::types_db::value::Value;
use log::{debug, error, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct InsertExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<InsertNode>,
    txn_table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
    schema_manager: SchemaManager,
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
                .get_table(plan.get_table_name())
                .unwrap_or_else(|| panic!("Table {} not found", plan.get_table_name()));
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
        }
    }
}

impl AbstractExecutor for InsertExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug!(
            "Initializing InsertExecutor for table '{}'",
            self.plan.get_table_name()
        );

        // Create the child executor from any child plan
        if let Some(child_plan) = self.plan.get_children().first() {
            debug!("Creating child executor for insert operation");
            debug!("Child plan type: {:?}", child_plan.get_type());
            debug!("Child schema: {:?}", child_plan.get_output_schema());

            match child_plan.create_executor(self.context.clone()) {
                Ok(mut child_executor) => {
                    debug!("Initializing child executor");
                    child_executor.init();
                    self.child_executor = Some(child_executor);
                    debug!("Child executor initialized successfully");
                }
                Err(e) => {
                    error!("Failed to create child executor: {}", e);
                }
            }
        } else {
            warn!("No child plan found for InsertExecutor");
        }
        self.initialized = true;
        debug!("InsertExecutor initialization completed");
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if !self.initialized {
            self.init();
        }

        if let Some(child) = &mut self.child_executor {
            if let Some((tuple, _)) = child.next() {
                debug!(
                    "Inserting tuple into table '{}'",
                    self.plan.get_table_name()
                );

                let txn_ctx = {
                    let context = self.context.read();
                    context.get_transaction_context().clone()
                };

                let _tuple_meta = Arc::new(TupleMeta::new(txn_ctx.get_transaction_id()));
                
                // Get the full table schema (target schema)
                let table_schema = self.plan.get_output_schema();
                
                // Get the child schema (VALUES schema - may be partial)
                let child_schema = child.get_output_schema();

                // Extract values from the child tuple
                let child_values: Vec<_> = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();

                // Create full tuple values by mapping child values to table schema
                let full_values = self.schema_manager.map_values_to_schema(
                    &child_values, 
                    child_schema, 
                    table_schema
                );

                // Use insert_tuple_from_values with the full table schema
                match self
                    .txn_table_heap
                    .insert_tuple_from_values(full_values.clone(), table_schema, txn_ctx)
                {
                    Ok(rid) => {
                        debug!("Successfully inserted tuple with RID {:?}", rid);
                        
                        // Create a new tuple with the full table schema values to return
                        let full_tuple = Arc::new(Tuple::new(&full_values, table_schema, rid));
                        Some((full_tuple, rid))
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
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();

        // Verify inserted values
        assert_eq!(tuple.get_value(0), Value::from(1));
        assert_eq!(tuple.get_value(1), Value::from("test"));

        // Verify no more tuples
        assert!(executor.next().is_none());
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

        // First row
        let (tuple1, _) = executor.next().expect("Expected first tuple");
        assert_eq!(tuple1.get_value(0), Value::from(1));

        // Second row
        let (tuple2, _) = executor.next().expect("Expected second tuple");
        assert_eq!(tuple2.get_value(0), Value::from(2));

        // No more rows
        assert!(executor.next().is_none());
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
        assert!(executor.next().is_none());
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
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();

        // Verify inserted values
        assert_eq!(tuple.get_value(0), Value::from(42));
        assert_eq!(tuple.get_value(1), Value::from("Alice"));
        assert_eq!(tuple.get_value(2), Value::from(true));
        assert_eq!(tuple.get_value(3), Value::from(95.5));

        // Verify no more tuples
        assert!(executor.next().is_none());
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

        // Verify all 100 rows are inserted
        for i in 0..100 {
            let result = executor.next();
            assert!(result.is_some(), "Expected tuple {}", i);
            
            let (tuple, _) = result.unwrap();
            assert_eq!(tuple.get_value(0), Value::from(i));
            assert_eq!(tuple.get_value(1), Value::from(format!("value_{}", i)));
        }

        // Verify no more tuples
        assert!(executor.next().is_none());
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
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();

        // Verify inserted values
        assert_eq!(tuple.get_value(0), Value::from(1));
        assert!(tuple.get_value(1).is_null());

        // Verify no more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_single_column_table() {
        let test_ctx = TestContext::new("insert_single_column");

        // Create schema with single column
        let schema = Schema::new(vec![Column::new("value", TypeId::VarChar)]);

        let table_name = "test_table_single_col".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values for single column
        let expressions = vec![vec![Arc::new(Expression::Constant(
            ConstantExpression::new(
                Value::new("single_value"),
                Column::new("value", TypeId::VarChar),
                vec![],
            ),
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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();

        // Verify inserted value
        assert_eq!(tuple.get_value(0), Value::from("single_value"));

        // Verify no more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_executor_reinitialization() {
        let test_ctx = TestContext::new("insert_reinit");

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_name = "test_table_reinit".to_string();

        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values
        let expressions = vec![vec![Arc::new(Expression::Constant(
            ConstantExpression::new(
                Value::new(123),
                Column::new("id", TypeId::Integer),
                vec![],
            ),
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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Initialize multiple times - should be safe
        executor.init();
        executor.init();
        executor.init();

        // Should still work correctly
        let result = executor.next();
        assert!(result.is_some(), "Insert should return a tuple");

        let (tuple, _) = result.unwrap();
        assert_eq!(tuple.get_value(0), Value::from(123));

        // Verify no more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_executor_output_schema() {
        let test_ctx = TestContext::new("insert_output_schema");

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_name = "test_table_schema".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create empty values (we're just testing schema)
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
            schema.clone(),
            table_oid,
            table_name.to_string(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context();
        let executor = InsertExecutor::new(exec_ctx.clone(), insert_plan);

        // Test output schema
        let output_schema = executor.get_output_schema();
        assert_eq!(output_schema.get_column_count(), 2);
        assert_eq!(output_schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(output_schema.get_column(1).unwrap().get_name(), "name");

        // Test executor context
        let returned_context = executor.get_executor_context();
        assert!(Arc::ptr_eq(&exec_ctx, &returned_context));
    }

    #[test]
    fn test_insert_mixed_value_types() {
        let test_ctx = TestContext::new("insert_mixed_types");

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("int_col", TypeId::Integer),
            Column::new("str_col", TypeId::VarChar),
        ]);

        let table_name = "test_table_mixed".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create multiple rows with different value combinations
        let expressions = vec![
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(0),
                    Column::new("int_col", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(""),
                    Column::new("str_col", TypeId::VarChar),
                    vec![],
                ))),
            ],
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(-1),
                    Column::new("int_col", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("negative"),
                    Column::new("str_col", TypeId::VarChar),
                    vec![],
                ))),
            ],
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2147483647), // Max i32
                    Column::new("int_col", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("max_int"),
                    Column::new("str_col", TypeId::VarChar),
                    vec![],
                ))),
            ],
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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute inserts
        executor.init();

        // First row: 0, ""
        let (tuple1, _) = executor.next().expect("Expected first tuple");
        assert_eq!(tuple1.get_value(0), Value::from(0));
        assert_eq!(tuple1.get_value(1), Value::from(""));

        // Second row: -1, "negative"
        let (tuple2, _) = executor.next().expect("Expected second tuple");
        assert_eq!(tuple2.get_value(0), Value::from(-1));
        assert_eq!(tuple2.get_value(1), Value::from("negative"));

        // Third row: max_int, "max_int"
        let (tuple3, _) = executor.next().expect("Expected third tuple");
        assert_eq!(tuple3.get_value(0), Value::from(2147483647));
        assert_eq!(tuple3.get_value(1), Value::from("max_int"));

        // No more rows
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_select_with_explicit_columns() {
        let test_ctx = TestContext::new("insert_select_explicit_columns");

        // Create source table (users)
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        let users_table = "users".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(users_table.clone(), users_schema.clone())
                .expect("Failed to create users table");
        }

        // Create target table (products) with different column order
        let products_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("price", TypeId::Decimal),
            Column::new("quantity", TypeId::Integer),
            Column::new("weight", TypeId::Decimal),
            Column::new("category", TypeId::VarChar),
        ]);

        let products_table = "products".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(products_table.clone(), products_schema.clone())
                .expect("Failed to create products table");
        }

        // Insert test data into users table
        let users_data = vec![
            vec![
                Value::new(1),
                Value::new("Alice"),
                Value::new(25),
                Value::new("alice@example.com"),
            ],
            vec![
                Value::new(2),
                Value::new("Bob"),
                Value::new(30),
                Value::new("bob@example.com"),
            ],
            vec![
                Value::new(3),
                Value::new("Charlie"),
                Value::new(35),
                Value::new("charlie@example.com"),
            ],
        ];

        // Insert users data manually using VALUES expressions
        for user_data in users_data {
            let expressions = vec![user_data.into_iter().map(|val| {
                Arc::new(Expression::Constant(ConstantExpression::new(
                    val.clone(),
                    Column::new("temp", val.get_type_id()),
                    vec![],
                )))
            }).collect()];

            let values_node = Arc::new(ValuesNode::new(
                users_schema.clone(),
                expressions,
                vec![PlanNode::Empty],
            ));

            let table_oid = {
                let catalog = test_ctx.catalog.read();
                catalog
                    .get_table(&users_table)
                    .expect("Users table not found")
                    .get_table_oidt()
            };

            let insert_plan = Arc::new(InsertNode::new(
                users_schema.clone(),
                table_oid,
                users_table.clone(),
                vec![],
                vec![PlanNode::Values(values_node.as_ref().clone())],
            ));

            let exec_ctx = test_ctx.create_executor_context();
            let mut executor = InsertExecutor::new(exec_ctx, insert_plan);
            executor.init();
            executor.next(); // Insert the row
        }

        // Now test INSERT...SELECT with explicit columns
        // This should map: [id, name, 10.00, age, 1.00, email] 
        // to columns:     [id, name, price, quantity, weight, category]
        // positionally, not by name
        
        // Create the SELECT part manually since we need to simulate the problematic scenario
        // The SELECT produces: id, name, 10.00, age, 1.00, email
        // These should map positionally to: id, name, price, quantity, weight, category
        
        let select_expressions = vec![
            // id -> id
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            // name -> name
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Alice"),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
            // 10.00 -> price (literal value, not a column name)
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(10.00),
                Column::new("10.00", TypeId::Decimal), // Note: column name is the literal
                vec![],
            ))),
            // age -> quantity
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(25),
                Column::new("age", TypeId::Integer),
                vec![],
            ))),
            // 1.00 -> weight (literal value, not a column name)
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1.00),
                Column::new("1.00", TypeId::Decimal), // Note: column name is the literal
                vec![],
            ))),
            // email -> category
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("alice@example.com"),
                Column::new("email", TypeId::VarChar),
                vec![],
            ))),
        ];

        // Create a schema that represents the SELECT output
        // This simulates what happens when SELECT produces literal values
        let select_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("10.00", TypeId::Decimal),  // Literal column name
            Column::new("age", TypeId::Integer),
            Column::new("1.00", TypeId::Decimal),   // Literal column name
            Column::new("email", TypeId::VarChar),
        ]);

        let select_values_node = Arc::new(ValuesNode::new(
            select_schema.clone(),
            vec![select_expressions],
            vec![PlanNode::Empty],
        ));

        // Create INSERT plan for products table with explicit columns
        let products_table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(&products_table)
                .expect("Products table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            products_schema.clone(), // Full target schema
            products_table_oid,
            products_table.clone(),
            vec![],
            vec![PlanNode::Values(select_values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.is_some(), "INSERT...SELECT should return a tuple");

        let (tuple, _) = result.unwrap();

        // The first 3 values should be mapped positionally, the rest should be NULL
        assert_eq!(tuple.get_value(0), Value::from(1), "ID should be mapped");
        assert_eq!(tuple.get_value(1), Value::from("Alice"), "Name should be mapped");
        assert_eq!(tuple.get_value(2), Value::from(10.00), "Price should be 10.00 (positional mapping)");
        assert_eq!(tuple.get_value(3), Value::from(25), "Quantity should be 25 (age value)");
        assert_eq!(tuple.get_value(4), Value::from(1.00), "Weight should be 1.00 (positional mapping)");
        assert_eq!(tuple.get_value(5), Value::from("alice@example.com"), "Category should be email value");

        // Verify no more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_insert_select_vs_insert_values_comparison() {
        let test_ctx = TestContext::new("insert_select_vs_values");

        // Create a test table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("score", TypeId::Decimal),
        ]);

        let table_name = "test_scores".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(&table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        // Test 1: INSERT...VALUES (should work correctly)
        let values_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Alice"),
                Column::new("name", TypeId::VarChar),
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
            values_expressions,
            vec![PlanNode::Empty],
        ));

        let values_insert_plan = Arc::new(InsertNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context();
        let mut values_executor = InsertExecutor::new(exec_ctx, values_insert_plan);
        values_executor.init();
        let values_result = values_executor.next().expect("VALUES insert should work");

        println!("INSERT...VALUES result:");
        for i in 0..values_result.0.get_column_count() {
            println!("  Column {}: {:?}", i, values_result.0.get_value(i));
        }

        // Test 2: INSERT...SELECT simulation (problematic case)
        // Simulate SELECT that produces literal values with non-matching column names
        let select_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2),
                Column::new("user_id", TypeId::Integer), // Different column name
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Bob"),
                Column::new("user_name", TypeId::VarChar), // Different column name
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(87.3),
                Column::new("calculated_score", TypeId::Decimal), // Different column name
                vec![],
            ))),
        ]];

        // Schema that doesn't match target table column names (simulates SELECT output)
        let select_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
            Column::new("calculated_score", TypeId::Decimal),
        ]);

        let select_values_node = Arc::new(ValuesNode::new(
            select_schema,
            select_expressions,
            vec![PlanNode::Empty],
        ));

        let select_insert_plan = Arc::new(InsertNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(select_values_node.as_ref().clone())],
        ));

        let exec_ctx2 = test_ctx.create_executor_context();
        let mut select_executor = InsertExecutor::new(exec_ctx2, select_insert_plan);
        select_executor.init();
        let select_result = select_executor.next().expect("SELECT-style insert should work");

        println!("INSERT...SELECT simulation result:");
        for i in 0..select_result.0.get_column_count() {
            println!("  Column {}: {:?}", i, select_result.0.get_value(i));
        }

        // With the current bug, this will fail because column names don't match
        // and the mapping function uses name-based matching instead of positional
        assert_eq!(select_result.0.get_value(0), Value::from(2), "ID should be mapped positionally");
        assert_eq!(select_result.0.get_value(1), Value::from("Bob"), "Name should be mapped positionally");
        assert_eq!(select_result.0.get_value(2), Value::from(87.3), "Score should be mapped positionally");
    }

    #[test]
    fn test_insert_select_partial_columns() {
        let test_ctx = TestContext::new("insert_select_partial");

        // Create target table with many columns
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("email", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::BigInt),
            Column::new("active", TypeId::Boolean),
        ]);

        let table_name = "employees".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Test INSERT with only some columns specified
        // This simulates: INSERT INTO employees (id, name, salary) SELECT user_id, user_name, calculated_pay FROM ...
        
        // Create schema that represents SELECT output with different column names
        let select_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
            Column::new("calculated_pay", TypeId::BigInt),
        ]);

        let select_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("user_id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Alice"),
                Column::new("user_name", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(50000i64),
                Column::new("calculated_pay", TypeId::BigInt),
                vec![],
            ))),
        ]];

        let select_values_node = Arc::new(ValuesNode::new(
            select_schema,
            select_expressions,
            vec![PlanNode::Empty],
        ));

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(&table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema.clone(), // Full target schema
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(select_values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        executor.init();
        let result = executor.next().expect("Partial column insert should work");

        println!("Partial column INSERT result:");
        for i in 0..result.0.get_column_count() {
            println!("  Column {}: {:?}", i, result.0.get_value(i));
        }

        // The first 3 values should be mapped positionally, the rest should be NULL
        assert_eq!(result.0.get_value(0), Value::from(1), "ID should be mapped");
        assert_eq!(result.0.get_value(1), Value::from("Alice"), "Name should be mapped");
        assert_eq!(result.0.get_value(2), Value::from("50000"), "Email should get calculated_pay cast to VarChar"); // BigInt 50000 cast to VarChar becomes "50000"
        assert!(result.0.get_value(3).is_null(), "Age should be NULL");
        assert!(result.0.get_value(4).is_null(), "Salary should be NULL"); // No 4th source value, so NULL
        assert!(result.0.get_value(5).is_null(), "Active should be NULL");
    }

    #[test]
    fn test_insert_select_with_literals_and_expressions() {
        let test_ctx = TestContext::new("insert_select_literals");

        // Create target table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("fixed_value", TypeId::Decimal),
            Column::new("computed_value", TypeId::Integer),
            Column::new("constant_text", TypeId::VarChar),
        ]);

        let table_name = "test_table".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Simulate SELECT with literals and computed expressions:
        // SELECT id, name, 99.99, age * 2, 'ACTIVE' FROM users
        // The schema from this SELECT would have column names like the expressions
        let select_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("99.99", TypeId::Decimal),        // Literal
            Column::new("age * 2", TypeId::Integer),      // Expression
            Column::new("'ACTIVE'", TypeId::VarChar),     // String literal
        ]);

        let select_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Alice"),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(99.99),
                Column::new("99.99", TypeId::Decimal),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(50), // age * 2 where age = 25
                Column::new("age * 2", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("ACTIVE"),
                Column::new("'ACTIVE'", TypeId::VarChar),
                vec![],
            ))),
        ]];

        let select_values_node = Arc::new(ValuesNode::new(
            select_schema,
            select_expressions,
            vec![PlanNode::Empty],
        ));

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(&table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        let insert_plan = Arc::new(InsertNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(select_values_node.as_ref().clone())],
        ));

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        executor.init();
        let result = executor.next().expect("Literal/expression insert should work");

        println!("Literal/expression INSERT result:");
        for i in 0..result.0.get_column_count() {
            println!("  Column {}: {:?}", i, result.0.get_value(i));
        }

        // These should be mapped positionally
        assert_eq!(result.0.get_value(0), Value::from(1), "ID should be mapped");
        assert_eq!(result.0.get_value(1), Value::from("Alice"), "Name should be mapped");
        assert_eq!(result.0.get_value(2), Value::from(99.99), "Fixed value should be mapped");
        assert_eq!(result.0.get_value(3), Value::from(50), "Computed value should be mapped");
        assert_eq!(result.0.get_value(4), Value::from("ACTIVE"), "Constant text should be mapped");
    }

    #[test]
    fn test_name_based_vs_positional_mapping_detection() {
        let test_ctx = TestContext::new("mapping_detection");

        // Create target table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("score", TypeId::Decimal),
        ]);

        let table_name = "test_table".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        let table_oid = {
            let catalog = test_ctx.catalog.read();
            catalog
                .get_table(&table_name)
                .expect("Table not found")
                .get_table_oidt()
        };

        // Test Case 1: Column names match exactly (should use name-based mapping)
        let matching_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("score", TypeId::Decimal),
        ]);

        let matching_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Test1"),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(100.0),
                Column::new("score", TypeId::Decimal),
                vec![],
            ))),
        ]];

        let matching_node = Arc::new(ValuesNode::new(
            matching_schema,
            matching_expressions,
            vec![PlanNode::Empty],
        ));

        let matching_plan = Arc::new(InsertNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(matching_node.as_ref().clone())],
        ));

        let exec_ctx1 = test_ctx.create_executor_context();
        let mut matching_executor = InsertExecutor::new(exec_ctx1, matching_plan);
        matching_executor.init();
        let matching_result = matching_executor.next().expect("Matching names should work");

        // Test Case 2: Column names don't match (should use positional mapping)
        let non_matching_schema = Schema::new(vec![
            Column::new("user_id", TypeId::Integer),
            Column::new("user_name", TypeId::VarChar),
            Column::new("calculated_score", TypeId::Decimal),
        ]);

        let non_matching_expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2),
                Column::new("user_id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("Test2"),
                Column::new("user_name", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(200.0),
                Column::new("calculated_score", TypeId::Decimal),
                vec![],
            ))),
        ]];

        let non_matching_node = Arc::new(ValuesNode::new(
            non_matching_schema,
            non_matching_expressions,
            vec![PlanNode::Empty],
        ));

        let non_matching_plan = Arc::new(InsertNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            vec![],
            vec![PlanNode::Values(non_matching_node.as_ref().clone())],
        ));

        let exec_ctx2 = test_ctx.create_executor_context();
        let mut non_matching_executor = InsertExecutor::new(exec_ctx2, non_matching_plan);
        non_matching_executor.init();
        let non_matching_result = non_matching_executor.next().expect("Non-matching names should use positional mapping");

        println!("Matching names result:");
        for i in 0..matching_result.0.get_column_count() {
            println!("  Column {}: {:?}", i, matching_result.0.get_value(i));
        }

        println!("Non-matching names result:");
        for i in 0..non_matching_result.0.get_column_count() {
            println!("  Column {}: {:?}", i, non_matching_result.0.get_value(i));
        }

        // Both should produce the same correct results
        assert_eq!(matching_result.0.get_value(0), Value::from(1));
        assert_eq!(matching_result.0.get_value(1), Value::from("Test1"));
        assert_eq!(matching_result.0.get_value(2), Value::from(100.0));

        assert_eq!(non_matching_result.0.get_value(0), Value::from(2));
        assert_eq!(non_matching_result.0.get_value(1), Value::from("Test2"));
        assert_eq!(non_matching_result.0.get_value(2), Value::from(200.0));
    }
}
