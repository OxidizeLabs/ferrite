use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct ValuesExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<ValuesNode>,
    current_row: usize,
    initialized: bool,
}

impl ValuesExecutor {
    pub fn new(context: Arc<RwLock<ExecutorContext>>, plan: Arc<ValuesNode>) -> Self {
        Self {
            context,
            plan,
            current_row: 0,
            initialized: false,
        }
    }
}

impl AbstractExecutor for ValuesExecutor {
    fn init(&mut self) {
        if !self.initialized {
            debug!("Initializing values executor");
            self.current_row = 0;
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Check if we've processed all rows
        if self.current_row >= self.plan.get_row_count() {
            debug!("No more rows to process");
            return None;
        }

        // Get the current row
        let row = self.plan.get_rows().get(self.current_row)?;
        let mut row_clone = row.clone();
        let schema = self.plan.get_output_schema();

        // Evaluate the expressions in the current row
        match row_clone.evaluate(schema) {
            Ok(values) => {
                // Create tuple with evaluated values
                let tuple = Tuple::new(values, schema.clone(), RID::default());

                // Advance to next row
                self.current_row += 1;

                debug!(
                    "Successfully evaluated row {}, advancing to next row",
                    self.current_row - 1
                );
                Some((tuple, RID::default()))
            }
            Err(e) => {
                error!("Failed to evaluate values: {}", e);
                None
            }
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    mod helpers {
        use super::*;
        use crate::catalog::catalog::Catalog;
        use crate::execution::plans::abstract_plan::PlanNode;
        use crate::recovery::log_manager::LogManager;

        pub fn create_test_schema() -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
            ])
        }

        pub fn create_constant_expression(value: Value, column: Column) -> Arc<Expression> {
            Arc::new(Expression::Constant(ConstantExpression::new(
                value,
                column,
                vec![],
            )))
        }

        pub fn create_test_values_node(schema: Schema, row_count: usize) -> Arc<ValuesNode> {
            let mut rows = Vec::new();
            for i in 0..row_count {
                let row_expressions = vec![
                    create_constant_expression(
                        Value::new(i as i32),
                        schema.get_column(0).unwrap().clone(),
                    ),
                    create_constant_expression(
                        Value::new(format!("name_{}", i)),
                        schema.get_column(1).unwrap().clone(),
                    ),
                    create_constant_expression(
                        Value::new(i % 2 == 0),
                        schema.get_column(2).unwrap().clone(),
                    ),
                ];
                rows.push(row_expressions);
            }

            Arc::new(ValuesNode::new(schema, rows, vec![PlanNode::Empty]))
        }

        pub struct TestContext {
            pub catalog: Arc<RwLock<Catalog>>,
            pub transaction_manager: Arc<RwLock<TransactionManager>>,
            pub lock_manager: Arc<LockManager>,
            pub buffer_pool_manager: Arc<BufferPoolManager>,
            pub transaction: Arc<Transaction>,
        }

        impl TestContext {
            pub fn new() -> Self {
                let temp_dir = tempdir().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let log_path = temp_dir.path().join("test.log");

                let disk_manager = Arc::new(FileDiskManager::new(
                    db_path.to_str().unwrap().to_string(),
                    log_path.to_str().unwrap().to_string(),
                    100,
                ));
                let disk_scheduler =
                    Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

                const BUFFER_POOL_SIZE: usize = 10;
                const K: usize = 2;
                let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));

                let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_scheduler,
                    disk_manager.clone(),
                    replacer,
                ));

                let catalog = Arc::new(RwLock::new(Catalog::new(
                    buffer_pool_manager.clone(),
                    0,
                    0,
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                )));

                let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
                let transaction_manager =
                    Arc::new(RwLock::new(TransactionManager::new(catalog.clone(), log_manager)));
                let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

                let transaction = transaction_manager
                    .write()
                    .begin(IsolationLevel::Serializable);

                Self {
                    catalog,
                    transaction_manager,
                    lock_manager,
                    buffer_pool_manager,
                    transaction,
                }
            }

            pub fn create_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
                Arc::new(RwLock::new(ExecutorContext::new(
                    self.transaction.clone(),
                    self.transaction_manager.clone(),
                    self.catalog.clone(),
                    self.buffer_pool_manager.clone(),
                    self.lock_manager.clone(),
                )))
            }
        }
    }

    mod initialization {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_initial_state() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 1);
            let context = test_ctx.create_executor_context();

            let executor = ValuesExecutor::new(context, plan);
            assert!(!executor.initialized);
            assert_eq!(executor.current_row, 0);
        }

        #[test]
        fn test_init_method() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 1);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);
            executor.init();

            assert!(executor.initialized);
            assert_eq!(executor.current_row, 0);
        }
    }

    mod row_processing {
        use super::helpers::*;
        use super::*;
        use crate::concurrency::transaction::TransactionState;

        #[test]
        fn test_empty_values() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 0);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);
            assert!(executor.next().is_none());
        }

        #[test]
        fn test_single_row() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 1);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);
            let result = executor.next();

            assert!(result.is_some());
            let (tuple, rid) = result.unwrap();
            assert_eq!(tuple.get_value(0), &Value::new(0));
            assert_eq!(tuple.get_value(1), &Value::new("name_0".to_string()));
            assert_eq!(tuple.get_value(2), &Value::new(true));
            assert_eq!(rid, RID::default());
        }

        #[test]
        fn test_multiple_rows_with_transaction() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 3);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);

            {
                let txn = test_ctx.transaction.clone();
                assert_eq!(txn.get_state(), TransactionState::Running);
            }

            for i in 0..3 {
                let result = executor.next();
                assert!(result.is_some());
                let (tuple, _) = result.unwrap();
                assert_eq!(tuple.get_value(0), &Value::new(i));
            }

            test_ctx
                .transaction_manager
                .write()
                .commit(test_ctx.transaction.clone());

            {
                let txn = test_ctx.transaction;
                assert_eq!(txn.get_state(), TransactionState::Committed);
            }
        }
    }

    mod schema_handling {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_output_schema() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema.clone(), 1);
            let context = test_ctx.create_executor_context();

            let executor = ValuesExecutor::new(context, plan);
            let output_schema = executor.get_output_schema();

            assert_eq!(output_schema.get_column_count(), 3);
            assert_eq!(
                output_schema.get_column(0).unwrap().get_type(),
                TypeId::Integer
            );
            assert_eq!(
                output_schema.get_column(1).unwrap().get_type(),
                TypeId::VarChar
            );
            assert_eq!(
                output_schema.get_column(2).unwrap().get_type(),
                TypeId::Boolean
            );
        }
    }

    mod transaction_handling {
        use super::helpers::*;
        use super::*;
        use crate::concurrency::transaction::TransactionState;

        #[test]
        fn test_transaction_commit() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 1);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);

            // Process row within transaction
            assert!(executor.next().is_some());

            // Commit transaction
            test_ctx
                .transaction_manager
                .write()
                .commit(test_ctx.transaction.clone());

            {
                let txn = test_ctx.transaction;
                assert_eq!(txn.get_state(), TransactionState::Committed);
            }
        }

        #[test]
        fn test_transaction_abort() {
            let test_ctx = TestContext::new();
            let schema = create_test_schema();
            let plan = create_test_values_node(schema, 1);
            let context = test_ctx.create_executor_context();

            let mut executor = ValuesExecutor::new(context, plan);

            // Process row within transaction
            assert!(executor.next().is_some());

            // Abort transaction
            test_ctx
                .transaction_manager
                .write()
                .abort(test_ctx.transaction.clone());

            {
                let txn = test_ctx.transaction;
                assert_eq!(txn.get_state(), TransactionState::Aborted);
            }
        }
    }
}
