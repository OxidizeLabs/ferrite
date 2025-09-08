use crate::catalog::schema::Schema;
use crate::common::config::BATCH_INSERT_THRESHOLD;
use crate::common::exception::DBError;
use crate::common::performance_monitor::{record_insert_performance, record_query_execution};
use crate::common::rid::RID;
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

        debug!(
            "Successfully created InsertExecutor for table '{}'",
            plan.get_table_name()
        );

        Self {
            context,
            plan,
            initialized: false,
            child_executor: None,
            schema_manager: SchemaManager::new(),
            executed: false,
            rows_inserted: 0,
        }
    }

    /// PERFORMANCE OPTIMIZATION: True bulk insert processing
    fn bulk_insert_values(
        &self,
        values_batch: &[Vec<crate::types_db::value::Value>],
        schema: &Schema,
        transactional_table_heap: &TransactionalTableHeap,
        txn_context: Arc<crate::sql::execution::transaction_context::TransactionContext>,
    ) -> Result<usize, DBError> {
        debug!("Processing TRUE bulk insert of {} rows", values_batch.len());

        // Use the new TRUE bulk insert method with batched operations
        match transactional_table_heap.bulk_insert_tuples_from_values(
            values_batch,
            schema,
            txn_context,
        ) {
            Ok(rids) => {
                info!(
                    "✓ TRUE bulk insert completed successfully: {} rows inserted",
                    rids
                );
                Ok(rids)
            }
            Err(e) => {
                error!("✗ TRUE bulk insert failed: {}", e);
                Err(DBError::Execution(format!("Bulk insert failed: {}", e)))
            }
        }
    }

    /// Validates foreign key constraints for the given values
    fn validate_foreign_key_constraints_with_context(
        context: &Arc<RwLock<ExecutionContext>>,
        values: &[crate::types_db::value::Value],
        schema: &Schema,
    ) -> Result<(), String> {
        for (column_index, column) in schema.get_columns().iter().enumerate() {
            if let Some(foreign_key_constraint) = column.get_foreign_key() {
                if column_index >= values.len() {
                    return Err(format!(
                        "Value index {} out of bounds for foreign key validation",
                        column_index
                    ));
                }

                let value = &values[column_index];

                // NULL values are allowed for foreign keys (unless column is NOT NULL)
                if value.is_null() {
                    continue;
                }

                // Validate that the foreign key value exists in the referenced table
                if !Self::validate_foreign_key_reference_with_context(
                    context,
                    value,
                    &foreign_key_constraint.referenced_table,
                    &foreign_key_constraint.referenced_column,
                )? {
                    return Err(format!(
                        "Foreign key constraint violation for column '{}': value '{}' does not exist in table '{}' column '{}'",
                        column.get_name(),
                        value,
                        foreign_key_constraint.referenced_table,
                        foreign_key_constraint.referenced_column
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validate that a foreign key value exists in the referenced table
    fn validate_foreign_key_reference_with_context(
        context: &Arc<RwLock<ExecutionContext>>,
        value: &crate::types_db::value::Value,
        referenced_table: &str,
        referenced_column: &str,
    ) -> Result<bool, String> {
        let context_guard = context.read();
        let catalog_guard = context_guard.get_catalog().read();

        // Get the referenced table info
        let table_info = catalog_guard
            .get_table(referenced_table)
            .ok_or_else(|| format!("Referenced table '{}' not found", referenced_table))?;

        let referenced_schema = table_info.get_table_schema();

        // Find the referenced column index
        let column_index = referenced_schema
            .get_columns()
            .iter()
            .position(|col| col.get_name() == referenced_column)
            .ok_or_else(|| {
                format!(
                    "Referenced column '{}' not found in table '{}'",
                    referenced_column, referenced_table
                )
            })?;

        // Get transaction context
        let txn_context = context_guard.get_transaction_context().clone();

        // Create transactional table heap for the referenced table
        let referenced_table_heap =
            TransactionalTableHeap::new(table_info.get_table_heap(), table_info.get_table_oidt());

        // Check if the value exists in the referenced table
        let iterator = referenced_table_heap.make_iterator(Some(txn_context));

        for item in iterator {
            let (_, tuple) = item;
            let existing_value = tuple.get_value(column_index);
            if existing_value == *value {
                return Ok(true); // Found the referenced value
            }
        }

        Ok(false) // Referenced value not found
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
                .get_table(self.plan.get_table_name())
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

            // PERFORMANCE OPTIMIZATION: Process inserts in batches
            if values_to_insert.len() >= BATCH_INSERT_THRESHOLD {
                debug!(
                    "Using TRUE bulk insert optimization for {} rows",
                    values_to_insert.len()
                );

                // OPTIMIZATION: Process in optimal batch sizes for memory efficiency
                // Use larger batches for better performance while respecting memory limits
                let optimal_batch_size = std::cmp::min(BATCH_INSERT_THRESHOLD * 2, 1000);

                for batch in values_to_insert.chunks(optimal_batch_size) {
                    let batch_result = self.bulk_insert_values(
                        batch,
                        &schema,
                        &transactional_table_heap,
                        txn_context.clone(),
                    )?;
                    insert_count += batch_result;
                    trace!(
                        "✓ Successfully completed TRUE bulk insert batch of {} tuples",
                        batch_result
                    );
                }
            } else {
                // OPTIMIZATION: Even for small batches, use bulk insert if we have multiple rows
                if values_to_insert.len() > 1 {
                    debug!(
                        "Using TRUE bulk insert for small batch of {} rows",
                        values_to_insert.len()
                    );
                    let batch_result = self.bulk_insert_values(
                        values_to_insert,
                        &schema,
                        &transactional_table_heap,
                        txn_context.clone(),
                    )?;
                    insert_count += batch_result;
                } else {
                    // Single row - fall back to individual insert
                    for values in values_to_insert {
                        match transactional_table_heap.insert_tuple_from_values(
                            values.clone(),
                            &schema,
                            txn_context.clone(),
                        ) {
                            Ok(_rid) => {
                                insert_count += 1;
                                trace!("Successfully inserted single tuple");
                            }
                            Err(e) => {
                                error!("Failed to insert single tuple: {}", e);
                                return Err(DBError::Execution(format!("Insert failed: {}", e)));
                            }
                        }
                    }
                }
            }
        } else {
            // Get the child executor reference separately to avoid borrowing conflicts
            if let Some(ref mut child) = self.child_executor {
                debug!("Inserting from child executor (VALUES/SELECT query)");

                // Get the child executor's output schema first, before any mutable borrows
                let child_schema = child.get_output_schema().clone();
                debug!("Child schema: {:?}", child_schema);
                debug!("Target table schema: {:?}", schema);

                // Collect all values first to avoid borrowing conflicts
                let mut all_values = Vec::new();
                loop {
                    match child.next()? {
                        Some((tuple, _)) => {
                            let values = tuple.get_values().clone();
                            all_values.push(values);
                        }
                        None => break,
                    }
                }

                // Now validate and insert each set of values
                for values in all_values {
                    // Map values from child schema to table schema
                    let mapped_values =
                        self.schema_manager
                            .map_values_to_schema(&values, &child_schema, &schema);

                    debug!("Original values: {:?}", values);
                    debug!("Mapped values: {:?}", mapped_values);

                    // Validate foreign key constraints before inserting
                    if let Err(e) = Self::validate_foreign_key_constraints_with_context(
                        &self.context,
                        &mapped_values,
                        &schema,
                    ) {
                        error!("Foreign key constraint violation: {}", e);
                        return Err(DBError::Execution(format!(
                            "Insert from VALUES/SELECT failed: {}",
                            e
                        )));
                    }

                    match transactional_table_heap.insert_tuple_from_values(
                        mapped_values,
                        &schema,
                        txn_context.clone(),
                    ) {
                        Ok(_rid) => {
                            insert_count += 1;
                            trace!(
                                "Successfully inserted tuple #{} from VALUES/SELECT",
                                insert_count
                            );
                        }
                        Err(e) => {
                            error!("Failed to insert tuple from VALUES/SELECT: {}", e);
                            return Err(DBError::Execution(format!(
                                "Insert from VALUES/SELECT failed: {}",
                                e
                            )));
                        }
                    }
                }
            } else {
                return Err(DBError::Execution(
                    "No values or child executor found for INSERT".to_string(),
                ));
            }
        }

        info!(
            "Insert operation completed successfully, {} rows inserted",
            insert_count
        );
        self.rows_inserted = insert_count;

        // PERFORMANCE MONITORING: Record insert performance
        let operation_duration = std::time::Instant::now().elapsed();
        let is_bulk_operation = insert_count >= BATCH_INSERT_THRESHOLD;
        record_insert_performance(insert_count, operation_duration, is_bulk_operation);
        record_query_execution("insert", insert_count, insert_count, operation_duration);

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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
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

            // Create catalog with transaction manager
            let catalog = Arc::new(RwLock::new(Catalog::new(
                Arc::clone(&bpm),
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
                buffer_pool: bpm,
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

    #[tokio::test]
    async fn test_insert_single_row() {
        let test_ctx = TestContext::new("insert_single_row").await;

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

    #[tokio::test]
    async fn test_insert_multiple_rows() {
        let test_ctx = TestContext::new("insert_multiple_rows").await;

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

    #[tokio::test]
    async fn test_insert_empty_values() {
        let test_ctx = TestContext::new("insert_empty_values").await;

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

    #[tokio::test]
    async fn test_insert_different_data_types() {
        let test_ctx = TestContext::new("insert_different_types").await;

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

    #[tokio::test]
    async fn test_insert_large_batch() {
        let test_ctx = TestContext::new("insert_large_batch").await;

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

    #[tokio::test]
    async fn test_insert_with_null_values() {
        let test_ctx = TestContext::new("insert_null_values").await;

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

    #[tokio::test]
    async fn test_insert_with_special_characters() {
        let test_ctx = TestContext::new("insert_special_chars").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("text", TypeId::VarChar),
        ]);

        let table_name = "test_table_special".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with special characters
        let special_text = "Hello\nWorld\t!@#$%^&*()[]{}\"'\\";
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(special_text),
                Column::new("text", TypeId::VarChar),
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
    }

    #[tokio::test]
    async fn test_insert_boundary_values() {
        let test_ctx = TestContext::new("insert_boundary_values").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("max_int", TypeId::Integer),
            Column::new("min_int", TypeId::Integer),
            Column::new("zero", TypeId::Integer),
            Column::new("empty_string", TypeId::VarChar),
        ]);

        let table_name = "test_table_boundary".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with boundary cases
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(i32::MAX),
                Column::new("max_int", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(i32::MIN),
                Column::new("min_int", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(0),
                Column::new("zero", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(""),
                Column::new("empty_string", TypeId::VarChar),
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
    }

    #[tokio::test]
    async fn test_insert_very_large_string() {
        let test_ctx = TestContext::new("insert_large_string").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("large_text", TypeId::VarChar),
        ]);

        let table_name = "test_table_large_text".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create a very large string (10KB) - this should fail due to page size constraints
        let large_string = "A".repeat(10240);
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(large_string),
                Column::new("large_text", TypeId::VarChar),
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

        // Execute insert - this should fail because the string is too large for a page
        executor.init();
        let result = executor.next();

        // Expect the insert to fail with an error about the tuple being too large
        assert!(result.is_err(), "Insert should fail for oversized tuple");
        if let Err(error) = result {
            let error_msg = error.to_string();
            assert!(
                error_msg.contains("too large") || error_msg.contains("Tuple is too large"),
                "Error should mention tuple size issue, got: {}",
                error_msg
            );
        }
    }

    #[tokio::test]
    async fn test_insert_mixed_null_and_values() {
        let test_ctx = TestContext::new("insert_mixed_nulls").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("score", TypeId::Decimal),
            Column::new("active", TypeId::Boolean),
        ]);

        let table_name = "test_table_mixed".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create multiple rows with mixed null and non-null values
        let expressions = vec![
            // Row 1: All non-null
            vec![
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
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("active", TypeId::Boolean),
                    vec![],
                ))),
            ],
            // Row 2: Some nulls
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_with_type(Val::Null, TypeId::VarChar),
                    Column::new("name", TypeId::VarChar),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(87.2),
                    Column::new("score", TypeId::Decimal),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_with_type(Val::Null, TypeId::Boolean),
                    Column::new("active", TypeId::Boolean),
                    vec![],
                ))),
            ],
            // Row 3: All nulls except id
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(3),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_with_type(Val::Null, TypeId::VarChar),
                    Column::new("name", TypeId::VarChar),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_with_type(Val::Null, TypeId::Decimal),
                    Column::new("score", TypeId::Decimal),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_with_type(Val::Null, TypeId::Boolean),
                    Column::new("active", TypeId::Boolean),
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

        // Execute insert
        executor.init();
        let result = executor.next();
        assert!(result.unwrap().is_none(), "Insert should return None");
    }

    #[tokio::test]
    async fn test_insert_decimal_precision() {
        let test_ctx = TestContext::new("insert_decimal_precision").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("precise_value", TypeId::Decimal),
            Column::new("small_decimal", TypeId::Decimal),
        ]);

        let table_name = "test_table_decimals".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with various decimal precisions
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(123456.789012345),
                Column::new("precise_value", TypeId::Decimal),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(0.001),
                Column::new("small_decimal", TypeId::Decimal),
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
    }

    #[tokio::test]
    async fn test_insert_reasonably_large_string() {
        let test_ctx = TestContext::new("insert_reasonable_large_string").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("large_text", TypeId::VarChar),
        ]);

        let table_name = "test_table_reasonable_large_text".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create a reasonably large string (1KB) that should fit in a page
        let large_string = "A".repeat(1024);
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(large_string),
                Column::new("large_text", TypeId::VarChar),
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

        // Execute insert - this should succeed
        executor.init();
        let result = executor.next();
        assert!(
            result.unwrap().is_none(),
            "Insert should return None for success"
        );
    }

    #[tokio::test]
    async fn test_insert_unicode_characters() {
        let test_ctx = TestContext::new("insert_unicode").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("unicode_text", TypeId::VarChar),
        ]);

        let table_name = "test_table_unicode".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create values with Unicode characters
        let unicode_text = "Hello, 世界! Здравствуй мир! 🌍🚀✨";
        let expressions = vec![vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(unicode_text),
                Column::new("unicode_text", TypeId::VarChar),
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
    }

    #[tokio::test]
    async fn test_insert_extremely_large_batch() {
        let test_ctx = TestContext::new("insert_extremely_large_batch").await;

        // Create schema and table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        let table_name = "test_table_xl_batch".to_string();
        {
            let mut catalog = test_ctx.catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Create a large batch (50 rows) that should fit within buffer pool constraints
        // Note: Using 1000 rows would exhaust the small test buffer pool (10 pages)
        let mut expressions = Vec::new();
        for i in 0..50 {
            expressions.push(vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(i),
                    Column::new("id", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(format!("batch_value_{}", i)),
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

    #[tokio::test]
    async fn test_insert_multiple_calls() {
        let test_ctx = TestContext::new("insert_multiple_calls").await;

        // Create schema and table
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_name = "test_table_multiple_calls".to_string();

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

        let exec_ctx = test_ctx.create_executor_context();
        let mut executor = InsertExecutor::new(exec_ctx, insert_plan);

        // Execute insert
        executor.init();

        // First call should insert and return None
        assert!(executor.next().unwrap().is_none());

        // Subsequent calls should return None (no more work to do)
        assert!(executor.next().unwrap().is_none());
        assert!(executor.next().unwrap().is_none());
        assert!(executor.next().unwrap().is_none());
    }
}
