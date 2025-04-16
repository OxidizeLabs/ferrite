use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val;
use log::{debug, trace};
use parking_lot::RwLock;
use sqlparser::ast::{JoinConstraint, JoinOperator as JoinType};
use std::collections::HashMap;
use std::sync::Arc;

pub struct NestedLoopJoinExecutor {
    children_executors: Option<Vec<Box<dyn AbstractExecutor>>>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<NestedLoopJoinNode>,
    initialized: bool,
    current_left_tuple: Option<(Tuple, RID)>, // Current tuple from outer (left) relation
    right_exhausted: bool, // Whether we've exhausted the inner (right) relation for current left tuple
}

impl NestedLoopJoinExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedLoopJoinNode>) -> Self {
        Self {
            children_executors: None,
            context,
            plan,
            initialized: false,
            current_left_tuple: None,
            right_exhausted: false,
        }
    }

    // Helper function to evaluate join predicate
    fn evaluate_predicate(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> bool {
        let predicate = self.plan.get_predicate();
        let left_schema = self.plan.get_left_schema();
        let right_schema = self.plan.get_right_schema();

        trace!(
            "Evaluating predicate between left tuple: {:?} and right tuple: {:?}",
            left_tuple.get_values(),
            right_tuple.get_values()
        );
        trace!("Left schema: {:?}", left_schema);
        trace!("Right schema: {:?}", right_schema);
        trace!("Predicate: {:?}", predicate);

        match predicate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema) {
            Ok(v) => match v.get_val() {
                Val::Boolean(b) => {
                    trace!("Predicate evaluation returned boolean: {}", b);
                    *b
                }
                other => {
                    trace!("Predicate evaluation returned non-boolean: {:?}", other);
                    false
                }
            },
            Err(e) => {
                trace!("Predicate evaluation error: {:?}", e);
                false
            }
        }
    }

    fn construct_output_tuple(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Tuple {
        let mut joined_values = left_tuple.get_values().clone();
        joined_values.extend(right_tuple.get_values().clone());

        Tuple::new(
            &joined_values,
            self.get_output_schema().clone(),
            RID::new(0, 0), // Use a placeholder RID for joined tuples
        )
    }

    fn reset_right_executor(&mut self) -> bool {
        if let Some(children) = &mut self.children_executors {
            let right_plan = self.plan.get_right_child();
            if let Ok(mut right_executor) = right_plan.create_executor(self.context.clone()) {
                right_executor.init();
                children[1] = right_executor;
                self.right_exhausted = false;
                return true;
            }
        }
        false
    }
}

impl AbstractExecutor for NestedLoopJoinExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("NestedLoopJoinExecutor already initialized");
            return;
        }

        debug!("Initializing NestedLoopJoinExecutor");

        // Initialize child executors vector
        let mut children = Vec::with_capacity(2);

        // Initialize left executor
        let left_plan = self.plan.get_left_child();
        debug!("Creating left executor with plan: {:?}", left_plan);
        if let Ok(mut left_executor) = left_plan.create_executor(self.context.clone()) {
            left_executor.init();
            debug!(
                "Left executor initialized with schema: {:?}",
                left_executor.get_output_schema()
            );
            children.push(left_executor);
        } else {
            debug!("Failed to create left executor");
            return;
        }

        // Initialize right executor
        let right_plan = self.plan.get_right_child();
        debug!("Creating right executor with plan: {:?}", right_plan);
        if let Ok(mut right_executor) = right_plan.create_executor(self.context.clone()) {
            right_executor.init();
            debug!(
                "Right executor initialized with schema: {:?}",
                right_executor.get_output_schema()
            );
            children.push(right_executor);
        } else {
            debug!("Failed to create right executor");
            return;
        }

        self.children_executors = Some(children);
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("NestedLoopJoinExecutor not initialized");
            return None;
        }

        loop {
            // If we don't have a current left tuple, get the next one
            if self.current_left_tuple.is_none() {
                let left_tuple = if let Some(children) = &mut self.children_executors {
                    children[0].next()
                } else {
                    None
                };

                match left_tuple {
                    Some(tuple) => {
                        debug!("Got new left tuple: {:?}", tuple.0.get_values());
                        self.current_left_tuple = Some(tuple);
                        // Reset right executor for the new left tuple
                        if !self.reset_right_executor() {
                            debug!("Failed to reset right executor");
                            return None;
                        }
                    }
                    None => return None,
                }
            }

            // Get the next right tuple
            if let Some(children) = &mut self.children_executors {
                if let Some(right_tuple) = children[1].next() {
                    let left_tuple = self.current_left_tuple.as_ref().unwrap();
                    debug!("Checking right tuple: {:?}", right_tuple.0.get_values());

                    // For cross joins or when predicate evaluates to true
                    if matches!(self.plan.get_join_type(), JoinType::CrossJoin)
                        || self.evaluate_predicate(&left_tuple.0, &right_tuple.0)
                    {
                        debug!(
                            "Found matching tuples - left: {:?}, right: {:?}",
                            left_tuple.0.get_values(),
                            right_tuple.0.get_values()
                        );

                        return Some((
                            self.construct_output_tuple(&left_tuple.0, &right_tuple.0),
                            RID::new(0, 0),
                        ));
                    }
                } else {
                    // Right relation is exhausted for current left tuple
                    debug!("Right relation exhausted for current left tuple");
                    self.current_left_tuple = None; // Clear the current left tuple to get a new one
                    continue;
                }
            }
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
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::value::Value;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, BUFFER_POOL_SIZE));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create transaction manager and lock manager
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create transaction with ID 0 and ReadUncommitted isolation level
            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            // Create catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                transaction_manager.clone(),
            )));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
            }
        }
    }

    #[test]
    fn test_cross_join() {
        let ctx = TestContext::new("cross_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data for users table
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
        ];

        // Create mock data for posts table
        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Post 3")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let mut tuple = Tuple::new(&row, users_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                users_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }

            for row in posts_data {
                let mut tuple = Tuple::new(&row, posts_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                posts_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        // We expect 6 rows (2 users × 3 posts)
        assert_eq!(result_count, 6);
    }

    #[test]
    fn test_empty_join() {
        let ctx = TestContext::new("empty_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog (but don't insert any data)
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        // We expect 0 rows since both tables are empty
        assert_eq!(result_count, 0);
    }

    #[test]
    fn test_inner_join_with_predicate() {
        let ctx = TestContext::new("inner_join_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Create mock data for users table
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice")],
            vec![Value::new(2), Value::new("Bob")],
            vec![Value::new(3), Value::new("Charlie")],
        ];

        // Create mock data for posts table
        let posts_data = vec![
            vec![Value::new(1), Value::new(1), Value::new("Post 1")],
            vec![Value::new(2), Value::new(1), Value::new("Post 2")],
            vec![Value::new(3), Value::new(2), Value::new("Post 3")],
        ];

        // Insert data into tables
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            for row in users_data {
                let mut tuple = Tuple::new(&row, users_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                users_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }

            for row in posts_data {
                let mut tuple = Tuple::new(&row, posts_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                posts_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            0, // column index
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple index
            1, // column index
            Column::new("user_id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results
        let mut result_count = 0;
        let mut alice_posts = 0;
        let mut bob_posts = 0;
        let mut charlie_posts = 0;

        while let Some((tuple, _)) = join_executor.next() {
            result_count += 1;
            let values = tuple.get_values();
            let user_name = values[1].to_string();
            match user_name.as_str() {
                "Alice" => alice_posts += 1,
                "Bob" => bob_posts += 1,
                "Charlie" => charlie_posts += 1,
                _ => panic!("Unexpected user: {}", user_name),
            }
        }

        // We expect 3 rows total:
        // - Alice has 2 posts
        // - Bob has 1 post
        // - Charlie has 0 posts
        assert_eq!(result_count, 3);
        assert_eq!(alice_posts, 2);
        assert_eq!(bob_posts, 1);
        assert_eq!(charlie_posts, 0);
    }

    #[test]
    fn test_join_one_empty() {
        let ctx = TestContext::new("join_one_empty_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Only insert data into users table
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();

            let users_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
            ];

            for row in users_data {
                let mut tuple = Tuple::new(&row, users_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                users_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join plan with constant TRUE predicate for cross join
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("", TypeId::Boolean),
                vec![],
            ))),
            JoinType::CrossJoin,
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results - should be empty since one table is empty
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        assert_eq!(result_count, 0);
    }

    #[test]
    fn test_join_no_matches() {
        let ctx = TestContext::new("join_no_matches_test");

        // Create schemas for both tables
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let posts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("user_id", TypeId::Integer),
            Column::new("title", TypeId::VarChar),
        ]);

        // Create tables in catalog
        let users_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("users".to_string(), users_schema.clone())
                .unwrap()
        };

        let posts_table_info = {
            let mut catalog = ctx.catalog.write();
            catalog
                .create_table("posts".to_string(), posts_schema.clone())
                .unwrap()
        };

        // Insert data with no matching user_ids
        {
            let catalog = ctx.catalog.read();
            let users_table = catalog
                .get_table_by_oid(users_table_info.get_table_oidt())
                .unwrap();
            let posts_table = catalog
                .get_table_by_oid(posts_table_info.get_table_oidt())
                .unwrap();

            // Users with IDs 1 and 2
            let users_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
            ];

            // Posts with user_ids 3 and 4 (no matching users)
            let posts_data = vec![
                vec![Value::new(1), Value::new(3), Value::new("Post 1")],
                vec![Value::new(2), Value::new(4), Value::new("Post 2")],
            ];

            for row in users_data {
                let mut tuple = Tuple::new(&row, users_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                users_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }

            for row in posts_data {
                let mut tuple = Tuple::new(&row, posts_schema.clone(), RID::new(0, 0));
                let meta = TupleMeta::new(0);
                posts_table
                    .get_table_heap()
                    .insert_tuple(&meta, &mut tuple)
                    .unwrap();
            }
        }

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            ctx.catalog.clone(),
            ctx.transaction_context.clone(),
        )));

        // Create left and right child plans
        let left_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            users_schema.clone(),
            users_table_info.get_table_oidt(),
            "users".to_string(),
        ));

        let right_plan = PlanNode::SeqScan(SeqScanPlanNode::new(
            posts_schema.clone(),
            posts_table_info.get_table_oidt(),
            "posts".to_string(),
        ));

        // Create join predicate (users.id = posts.user_id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            0, // column index
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple index
            1, // column index
            Column::new("user_id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col, right_col],
        )));

        // Create join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            users_schema.clone(),
            posts_schema.clone(),
            predicate,
            JoinType::Inner(JoinConstraint::None),
            vec![],
            vec![],
            vec![left_plan, right_plan],
        ));

        // Create join executor
        let mut join_executor = NestedLoopJoinExecutor::new(execution_context.clone(), join_plan);

        // Initialize executor
        join_executor.init();

        // Verify results - should be empty since there are no matching user_ids
        let mut result_count = 0;
        while let Some(_) = join_executor.next() {
            result_count += 1;
        }

        assert_eq!(result_count, 0);
    }
}
