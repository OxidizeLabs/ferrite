use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::container::disk_extendable_hash_table::DiskExtendableHashTable;
use crate::container::hash_function::HashFunction;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::hash_join_plan::HashJoinNode;
use crate::storage::table::tuple::Tuple;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct HashJoinExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<HashJoinNode>,
    left_child: Box<dyn AbstractExecutor>,
    right_child: Box<dyn AbstractExecutor>,
    hash_table: Option<DiskExtendableHashTable>,
    right_tuples: Vec<(RID, Arc<Tuple>)>,
    current_left_tuple: Option<(Arc<Tuple>, RID)>,
    initialized: bool,
}

impl HashJoinExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<HashJoinNode>,
        left_child: Box<dyn AbstractExecutor>,
        right_child: Box<dyn AbstractExecutor>,
    ) -> Self {
        debug!("Creating HashJoinExecutor");

        Self {
            context,
            plan,
            left_child,
            right_child,
            hash_table: None,
            right_tuples: Vec::new(),
            current_left_tuple: None,
            initialized: false,
        }
    }

    fn build_hash_table(&mut self) -> bool {
        debug!("Building hash table from right child");

        let hash_table = match DiskExtendableHashTable::new(
            "hash_join_table".to_string(),
            self.get_executor_context().read().get_buffer_pool_manager(),
            HashFunction::new(),
            4,
            4,
            100,
        ) {
            Ok(ht) => ht,
            Err(_) => return false,
        };

        self.hash_table = Some(hash_table);

        self.right_child.init();

        loop {
            match self.right_child.next() {
                Ok(Some((tuple, rid))) => {
                    let key = match self.plan.get_right_key_expressions()[0]
                        .evaluate(&tuple, self.plan.get_right_schema())
                    {
                        Ok(value) => value,
                        Err(_) => {
                            debug!("Failed to evaluate right key expression");
                            return false;
                        }
                    };

                    if let Some(ref mut ht) = self.hash_table
                        && !ht.insert(key, rid) {
                            debug!("Failed to insert into hash table");
                            return false;
                        }

                    self.right_tuples.push((rid, tuple));
                }
                Ok(None) => break,
                Err(e) => {
                    debug!("Error from right child executor: {}", e);
                    return false;
                }
            }
        }

        true
    }
}

impl AbstractExecutor for HashJoinExecutor {
    fn init(&mut self) {
        if !self.initialized {
            debug!("Initializing HashJoinExecutor");

            if !self.build_hash_table() {
                debug!("Failed to build hash table");
                return;
            }

            self.left_child.init();

            self.initialized = true;
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            return Ok(None);
        }

        loop {
            if self.current_left_tuple.is_none() {
                match self.left_child.next()? {
                    Some((tuple, rid)) => {
                        self.current_left_tuple = Some((tuple, rid));
                    }
                    None => return Ok(None),
                }
            }

            // Clone the values we need and drop the borrow immediately
            let (left_tuple, left_rid) = {
                let (tuple, rid) = self.current_left_tuple.as_ref().unwrap();
                (tuple.clone(), *rid)
            };

            let key = match self.plan.get_left_key_expressions()[0]
                .evaluate(&left_tuple, self.plan.get_left_schema())
            {
                Ok(value) => value,
                Err(_) => {
                    debug!("Failed to evaluate left key expression");
                    self.current_left_tuple = None;
                    continue;
                }
            };

            if let Some(ref ht) = self.hash_table
                && let Some(right_rid) = ht.get_value(&key)
                && let Some((_, right_tuple)) = self.right_tuples.iter().find(|(rid, _)| *rid == right_rid) {
                        let combined_tuple = Arc::new(left_tuple.combine(right_tuple));
                        self.current_left_tuple = None;
                        return Ok(Some((combined_tuple, left_rid)));
                    }

            self.current_left_tuple = None;
        }
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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
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
            let disk_manager = AsyncDiskManager::new(db_path.clone(), log_path.clone(), DiskManagerConfig::default()).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_manager_arc.clone(),
                replacer.clone(),
            ).unwrap());

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());
            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                transaction_manager.clone(),
            )));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                catalog,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }
    }

    // Mock executor that returns predefined tuples
    struct MockExecutor {
        tuples: Vec<(Arc<Tuple>, RID)>,
        index: usize,
        schema: Schema,
    }

    impl MockExecutor {
        fn new(tuples: Vec<(Arc<Tuple>, RID)>, schema: Schema) -> Self {
            Self {
                tuples,
                index: 0,
                schema,
            }
        }
    }

    impl AbstractExecutor for MockExecutor {
        fn init(&mut self) {
            self.index = 0;
        }

        fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
            if self.index < self.tuples.len() {
                let result = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(result))
            } else {
                Ok(None)
            }
        }

        fn get_output_schema(&self) -> &Schema {
            &self.schema
        }

        fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
            panic!("Mock executor should not be asked for context")
        }
    }

    #[tokio::test]
    async fn test_hash_join_executor() {
        let ctx = TestContext::new("test_hash_join_executor").await;

        let execution_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create schemas
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        // Create test data for left child
        let left_tuples = vec![
            (
                Arc::new(Tuple::new(
                    &[Value::new(1), Value::new("Alice")],
                    &left_schema,
                    RID::new(1, 0),
                )),
                RID::new(1, 0),
            ),
            (
                Arc::new(Tuple::new(
                    &[Value::new(2), Value::new("Bob")],
                    &left_schema,
                    RID::new(1, 1),
                )),
                RID::new(1, 1),
            ),
            (
                Arc::new(Tuple::new(
                    &[Value::new(3), Value::new("Charlie")],
                    &left_schema,
                    RID::new(1, 2),
                )),
                RID::new(1, 2),
            ),
        ];

        // Create test data for right child
        let right_tuples = vec![
            (
                Arc::new(Tuple::new(
                    &[Value::new(1), Value::new(25)],
                    &right_schema,
                    RID::new(2, 0),
                )),
                RID::new(2, 0),
            ),
            (
                Arc::new(Tuple::new(
                    &[Value::new(2), Value::new(30)],
                    &right_schema.clone(),
                    RID::new(2, 1),
                )),
                RID::new(2, 1),
            ),
            (
                Arc::new(Tuple::new(
                    &[Value::new(4), Value::new(35)],
                    &right_schema,
                    RID::new(2, 2),
                )),
                RID::new(2, 2),
            ),
        ];

        // Create mock executors
        let left_executor = Box::new(MockExecutor::new(left_tuples, left_schema.clone()));
        let right_executor = Box::new(MockExecutor::new(right_tuples, right_schema.clone()));

        // Create join key expressions
        let left_key_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index for left
            0, // column index for id
            Column::new("id", TypeId::Integer),
            vec![], // no children
        )));

        let right_key_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple index for right
            0, // column index for id
            Column::new("id", TypeId::Integer),
            vec![], // no children
        )));

        // Create predicate expression (always true)
        let predicate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index doesn't matter for constant
            0, // column index doesn't matter for constant
            Column::new("const", TypeId::Boolean),
            vec![], // no children
        )));

        // Create hash join plan
        let plan = Arc::new(HashJoinNode::new(
            left_schema.clone(),
            right_schema.clone(),
            predicate,
            JoinOperator::Inner(JoinConstraint::None),
            vec![left_key_expr],
            vec![right_key_expr],
            vec![], // Children are passed to executor directly
        ));

        // Create hash join executor
        let mut join_executor =
            HashJoinExecutor::new(execution_ctx, plan, left_executor, right_executor);

        // Initialize the executor
        join_executor.init();

        // Collect and verify results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = join_executor.next() {
            results.push(tuple);
        }

        // Should have 2 matches: (1, Alice) ⋈ (1, 25) and (2, Bob) ⋈ (2, 30)
        assert_eq!(results.len(), 2);

        // Verify first match
        assert_eq!(results[0].get_value(0), Value::new(1)); // left id
        assert_eq!(results[0].get_value(1), Value::new("Alice")); // name
        assert_eq!(results[0].get_value(2), Value::new(1)); // right id
        assert_eq!(results[0].get_value(3), Value::new(25)); // age

        // Verify second match
        assert_eq!(results[1].get_value(0), Value::new(2)); // left id
        assert_eq!(results[1].get_value(1), Value::new("Bob")); // name
        assert_eq!(results[1].get_value(2), Value::new(2)); // right id
        assert_eq!(results[1].get_value(3), Value::new(30)); // age
    }
}
