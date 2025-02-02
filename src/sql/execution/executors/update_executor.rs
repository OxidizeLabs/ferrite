use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::filter_executor::FilterExecutor;
use crate::sql::execution::executors::table_scan_executor::TableScanExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::table_scan_plan::TableScanNode;
use crate::sql::execution::plans::update_plan::UpdateNode;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::warn;
use log::{debug, error};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

pub struct UpdateExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<UpdateNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
    updated_rids: HashSet<RID>,
}

impl UpdateExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<UpdateNode>) -> Self {
        debug!(
            "Creating UpdateExecutor for table '{}'",
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
            "Successfully created UpdateExecutor for table '{}'",
            plan.get_table_name()
        );

        Self {
            context,
            plan,
            table_heap,
            initialized: false,
            child_executor: None,
            updated_rids: HashSet::new(),
        }
    }
}

impl AbstractExecutor for UpdateExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug!(
            "Initializing UpdateExecutor for table '{}'",
            self.plan.get_table_name()
        );

        // Initialize child executor if present
        if let Some(child) = self.plan.get_children().first() {
            match child {
                PlanNode::Filter(filter_plan) => {
                    // Create table scan executor as the child for filter executor
                    let catalog = {
                        let context_guard = self.context.read();
                        context_guard.get_catalog().clone()
                    };

                    let table_info = {
                        let catalog_guard = catalog.read();
                        catalog_guard
                            .get_table(self.plan.get_table_name())
                            .unwrap_or_else(|| panic!("Table {} not found", self.plan.get_table_name()))
                            .clone()
                    };

                    let table_scan_plan = Arc::new(TableScanNode::new(
                        table_info,
                        Arc::new(self.plan.get_output_schema().clone()),
                        None,
                    ));

                    let table_scan_executor = Box::new(TableScanExecutor::new(
                        self.context.clone(),
                        table_scan_plan,
                    ));

                    // Create filter executor with table scan as child
                    self.child_executor = Some(Box::new(FilterExecutor::new(
                        table_scan_executor,
                        self.context.clone(),
                        filter_plan.clone().into(),
                    )));
                }
                _ => warn!("Unexpected child plan type for UpdateExecutor"),
            }
        }

        if let Some(child) = self.child_executor.as_mut() {
            child.init();
        }

        self.initialized = true;
        debug!("UpdateExecutor initialization completed");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Get the next tuple to update from child executor
        let child = self.child_executor.as_mut()?;

        while let Some((mut tuple, rid)) = child.next() {
            // Skip if we've already updated this tuple
            if self.updated_rids.contains(&rid) {
                continue;
            }

            debug!("Updating tuple in table '{}'", self.plan.get_table_name());

            let txn_ctx = {
                let context = self.context.read();
                context.get_transaction_context()
            };

            // Get the target expressions before applying updates
            let target_expressions = self.plan.get_target_expressions();
            let output_schema = self.plan.get_output_schema();

            // Apply the updates to the tuple
            for (i, expr) in target_expressions.iter().step_by(2).enumerate() {
                let col_idx = match expr.as_ref() {
                    Expression::ColumnRef(col_ref) => col_ref.get_column_index(),
                    _ => panic!("Expected ColumnRef as target expression"),
                };

                let value_expr = &target_expressions[i * 2 + 1];
                let new_value = value_expr.evaluate(&tuple, output_schema)
                    .expect("Failed to evaluate expression");

                let values = tuple.get_values_mut();
                values[col_idx] = new_value;
            }

            // Create tuple meta
            let tuple_meta = TupleMeta::new(txn_ctx.get_transaction_id());

            // Update the tuple in the table - no need to explicitly acquire the latch
            // as table_heap.update_tuple handles locking internally
            match self.table_heap.update_tuple(&tuple_meta, &mut tuple, rid, Some(txn_ctx)) {
                Ok(new_rid) => {
                    debug!("Successfully updated tuple: {:?}", new_rid);
                    // Add the RID to our set of updated tuples
                    self.updated_rids.insert(rid);
                    return Some((tuple, new_rid));
                }
                Err(e) => {
                    error!(
                        "Failed to update tuple in table '{}': {}",
                        self.plan.get_table_name(),
                        e
                    );
                }
            }
        }
        None
    }

    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
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
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::filter_plan::FilterNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableHeap;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
    use crate::types_db::value::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 5;
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

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
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
            0,                               // next_index_oid
            0,                               // next_table_oid
            HashMap::new(),                  // tables
            HashMap::new(),                  // indexes
            HashMap::new(),                  // table_names
            HashMap::new(),                  // index_names
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn create_test_executor_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        // Create a new transaction

        Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog,
            test_context.transaction_context.clone(),
        )))
    }

    fn create_age_filter(age: i32, comparison_type: ComparisonType, schema: &Schema) -> FilterNode {
        // Create column reference for age
        let col_idx = schema.get_column_index("age").unwrap();
        let age_col = schema.get_column(col_idx).unwrap().clone();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            col_idx,
            age_col.clone(),
            vec![],
        )));

        // Create constant expression for comparison
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(age),
            age_col,
            vec![],
        )));

        // Create predicate
        let predicate = Expression::Comparison(ComparisonExpression::new(
            col_expr,
            const_expr,
            comparison_type,
            vec![],
        ));

        FilterNode::new(
            schema.clone(),
            0,
            "test_table".to_string(),
            Arc::from(predicate),
            vec![PlanNode::Empty],
        )
    }

    fn create_invalid_age_filter(
        age: i32,
        comparison_type: ComparisonType,
        schema: &Schema,
    ) -> Arc<FilterNode> {
        // Create column reference for age
        let age_col = Column::new("age", TypeId::Integer);
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            age_col.clone(),
            vec![],
        )));

        // Create constant expression for comparison
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(age),
            age_col,
            vec![],
        )));

        // Create predicate
        let predicate = Expression::Comparison(ComparisonExpression::new(
            col_expr,
            const_expr,
            comparison_type,
            vec![],
        ));

        Arc::new(FilterNode::new(
            schema.clone(),
            0,
            "test_table".to_string(),
            Arc::from(predicate),
            vec![PlanNode::Empty],
        ))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn setup_test_table(
        table_heap: &TableHeap,
        schema: &Schema,
        transaction_context: &Arc<TransactionContext>,
    ) {
        let test_data = vec![
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
            (4, "David", 28),
            (5, "Eve", 32),
        ];

        for (id, name, age) in test_data {
            let values = vec![
                Value::new(id),
                Value::new(name.to_string()),
                Value::new(age),
            ];
            let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(0);
            table_heap
                .insert_tuple(&meta, &mut tuple, Some(transaction_context.clone()))
                .unwrap();
        }
    }

    #[test]
    fn test_update_executor() {
        let ctx = TestContext::new("test_update_executor");
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));
        let exec_ctx = create_test_executor_context(&ctx, catalog.clone());

        // Create test table
        let schema = create_test_schema();
        let table_name = "test_table".to_string();
        {
            let mut catalog = catalog.write();
            catalog
                .create_table(table_name.clone(), schema.clone())
                .expect("Failed to create table");
        }

        // Get table info and heap
        let (table_oid, table_heap) = {
            let catalog = catalog.read();
            let table_info = catalog.get_table(&table_name).expect("Table not found");
            (table_info.get_table_oidt(), table_info.get_table_heap())
        };

        // Insert test data
        setup_test_table(&table_heap, &schema, &ctx.transaction_context);

        // Create filter for age > 30
        let filter_plan = create_age_filter(30, ComparisonType::GreaterThan, &schema);

        // Create update expression to increment age by 1
        let age_col = schema.get_column(schema.get_column_index("age").unwrap()).unwrap().clone();
        let col_idx = schema.get_column_index("age").unwrap();

        // Create the column reference for the target column (this identifies which column to update)
        let target_col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            col_idx,
            age_col.clone(),
            vec![],
        )));

        // Create the arithmetic expression for computing age + 1
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            col_idx,
            age_col.clone(),
            vec![],
        )));

        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            age_col.clone(),
            vec![],
        )));

        let update_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            col_expr,
            const_expr,
            ArithmeticOp::Add,
            vec![],
        )));

        // Create update plan with both the target column and the update expression
        let update_plan = Arc::new(UpdateNode::new(
            schema.clone(),
            table_name,
            table_oid,
            vec![target_col_expr, update_expr],  // Include both expressions
            vec![PlanNode::Filter(filter_plan)],
        ));

        // Execute update
        let mut executor = UpdateExecutor::new(exec_ctx, update_plan);
        executor.init();

        // Count and verify updates
        let mut update_count = 0;
        while let Some(_) = executor.next() {
            update_count += 1;
        }

        assert_eq!(update_count, 2); // Should update 2 records (Charlie and Eve)

        // Verify the updates using TableHeap's iterator
        let mut found_updates = 0;
        let iterator = table_heap.make_iterator(Some(ctx.transaction_context.clone()));

        for (_meta, tuple) in iterator {
            let name = ToString::to_string(&tuple.get_value(1));
            let age = tuple.get_value(2).as_integer();
            match name.as_str() {
                "Charlie" => {
                    assert_eq!(age, Ok(36));
                    found_updates += 1;
                }
                "Eve" => {
                    assert_eq!(age, Ok(33));
                    found_updates += 1;
                }
                _ => {}
            }
        }

        assert_eq!(found_updates, 2, "Not all updates were found");
    }
}
