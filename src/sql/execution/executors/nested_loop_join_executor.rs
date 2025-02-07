use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Val;
use log::{debug, trace};
use parking_lot::RwLock;
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
            let right_plan = self.plan.get_right_child().clone();
            debug!("Resetting right executor with plan: {:?}", right_plan);

            // Create a fresh executor for the right side
            match right_plan.create_executor(self.context.clone()) {
                Ok(mut new_executor) => {
                    new_executor.init();
                    debug!(
                        "Right executor reset and initialized with schema: {:?}",
                        new_executor.get_output_schema()
                    );

                    // Verify the first tuple from the right executor
                    if let Some((first_tuple, _)) = new_executor.next() {
                        debug!(
                            "First tuple from reset right executor: {:?}",
                            first_tuple.get_values()
                        );
                    }

                    // Reset the executor
                    new_executor.init();

                    // Replace the existing right executor
                    children[1] = new_executor;
                    self.right_exhausted = false;
                    true
                }
                Err(e) => {
                    debug!("Failed to create right executor: {:?}", e);
                    false
                }
            }
        } else {
            false
        }
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

                    // Check if the tuples satisfy the join condition
                    if self.evaluate_predicate(&left_tuple.0, &right_tuple.0) {
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
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use crate::types_db::value::{Val, Value};
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    use std::collections::HashMap;

    use crate::catalog::column::Column;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                HashMap::default(),
                HashMap::default(),
                HashMap::default(),
                HashMap::default(),
                transaction_manager.clone(),
            )));

            Self {
                bpm,
                transaction_context,
                catalog,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }

        pub fn transaction_context(&self) -> Arc<TransactionContext> {
            self.transaction_context.clone()
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
        }
    }

    #[test]
    fn test_nested_loop_join_executor() {
        let ctx = TestContext::new("test_nested_loop_join_executor");

        // Create schemas for both tables
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create tables with different OIDs
        let left_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("left_table".to_string(), left_schema.clone())
                .unwrap()
        };
        let right_table_info = {
            let mut catalog_guard = ctx.catalog.write();
            catalog_guard
                .create_table("right_table".to_string(), right_schema.clone())
                .unwrap()
        };

        // Verify table OIDs are different
        assert_ne!(
            left_table_info.get_table_oidt(),
            right_table_info.get_table_oidt(),
            "Left and right tables must have different OIDs"
        );

        let left_table = left_table_info.get_table_heap();
        let right_table = right_table_info.get_table_heap();

        debug!("Left table OID: {}", left_table_info.get_table_oidt());
        debug!("Right table OID: {}", right_table_info.get_table_oidt());

        // Insert data into left table
        let left_data = vec![(1, "A"), (2, "B"), (3, "C")];
        let left_tuple_meta = TupleMeta::new(0);
        for (id, value) in left_data {
            let mut tuple = Tuple::new(
                &vec![Value::new(id), Value::new(value.to_string())],
                left_schema.clone(),
                RID::new(0, 0),
            );
            let rid = left_table.insert_tuple(
                &left_tuple_meta,
                &mut tuple,
                Some(ctx.transaction_context()),
            ).expect("Failed to insert into left table");
            debug!("Inserted into left table: id={}, value={} at RID={:?}", id, value, rid);
        }

        // Verify left table's first page ID
        debug!("Left table first page ID: {}", left_table.get_first_page_id());

        // Insert data into right table
        let right_data = vec![(1, "X"), (2, "Y"), (4, "Z")];
        let right_tuple_meta = TupleMeta::new(0);
        for (id, data) in right_data {
            let mut tuple = Tuple::new(
                &vec![Value::new(id), Value::new(data.to_string())],
                right_schema.clone(),
                RID::new(0, 0),
            );
            let rid = right_table.insert_tuple(
                &right_tuple_meta,
                &mut tuple,
                Some(ctx.transaction_context()),
            ).expect("Failed to insert into right table");
            debug!("Inserted into right table: id={}, data={} at RID={:?}", id, data, rid);
        }

        // Verify right table's first page ID
        debug!("Right table first page ID: {}", right_table.get_first_page_id());

        // Verify each table's contents separately
        {
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx.bpm(),
                ctx.catalog(),
                ctx.transaction_context(),
            )));

            // Verify left table
            let left_scan = SeqScanPlanNode::new(
                left_schema.clone(),
                left_table_info.get_table_oidt(),
                "left_table".to_string(),
            );
            let mut left_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(left_scan));
            left_executor.init();

            debug!("Starting left table scan");
            let mut left_tuples = Vec::new();
            while let Some((tuple, rid)) = left_executor.next() {
                debug!("Left table scan tuple at RID {:?}: {:?}", rid, tuple.get_values());
                left_tuples.push((tuple, rid));
            }
            assert_eq!(left_tuples.len(), 3, "Left table should have 3 tuples");

            // Verify the values match what we inserted
            for (tuple, _) in left_tuples {
                let values = tuple.get_values();
                let id = values[0].get_val();
                let value = values[1].get_val();
                match id {
                    Val::Integer(1) => assert_eq!(*value, Val::VarLen("A".to_string())),
                    Val::Integer(2) => assert_eq!(*value, Val::VarLen("B".to_string())),
                    Val::Integer(3) => assert_eq!(*value, Val::VarLen("C".to_string())),
                    _ => panic!("Unexpected ID in left table: {:?}", id),
                }
            }

            // Verify right table
            let right_scan = SeqScanPlanNode::new(
                right_schema.clone(),
                right_table_info.get_table_oidt(),
                "right_table".to_string(),
            );
            let mut right_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(right_scan));
            right_executor.init();

            debug!("Starting right table scan");
            let mut right_tuples = Vec::new();
            while let Some((tuple, rid)) = right_executor.next() {
                debug!("Right table scan tuple at RID {:?}: {:?}", rid, tuple.get_values());
                right_tuples.push((tuple, rid));
            }
            assert_eq!(right_tuples.len(), 3, "Right table should have 3 tuples");

            // Verify the values match what we inserted
            for (tuple, _) in right_tuples {
                let values = tuple.get_values();
                let id = values[0].get_val();
                let data = values[1].get_val();
                match id {
                    Val::Integer(1) => assert_eq!(*data, Val::VarLen("X".to_string())),
                    Val::Integer(2) => assert_eq!(*data, Val::VarLen("Y".to_string())),
                    Val::Integer(4) => assert_eq!(*data, Val::VarLen("Z".to_string())),
                    _ => panic!("Unexpected ID in right table: {:?}", id),
                }
            }
        }

        // Create sequential scan plans with correct table OIDs
        let left_scan = SeqScanPlanNode::new(
            left_schema.clone(),
            left_table_info.get_table_oidt(),
            "left_table".to_string(),
        );
        debug!("Created left scan plan: {:?}", left_scan);

        let right_scan = SeqScanPlanNode::new(
            right_schema.clone(),
            right_table_info.get_table_oidt(),
            "right_table".to_string(),
        );
        debug!("Created right scan plan: {:?}", right_scan);

        // Create join predicate (left.id = right.id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index = 0 for left table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple_index = 1 for right table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![], // Don't need to add children here since they're already in left_col and right_col
        )));

        // Create nested loop join plan
        let join_plan = Arc::new(NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinOperator::Inner(JoinConstraint::None),
            vec![left_col],
            vec![right_col],
            vec![PlanNode::SeqScan(left_scan), PlanNode::SeqScan(right_scan)], // Convert directly to PlanNode
        ));

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog(),
            ctx.transaction_context(),
        )));

        // Create and initialize join executor
        let mut join_executor = NestedLoopJoinExecutor::new(exec_ctx, join_plan);
        join_executor.init();

        // Collect and verify results
        let mut results = Vec::new();
        while let Some((tuple, _)) = join_executor.next() {
            results.push(tuple);
        }

        // Should have 2 matching rows (id=1 and id=2)
        assert_eq!(results.len(), 2);

        // Verify the joined results
        for tuple in results {
            let values = tuple.get_values();
            assert_eq!(values.len(), 4); // 2 columns from each table

            let left_id = values.get(0).unwrap();
            let left_value = values.get(1).unwrap();
            let right_id = values.get(2).unwrap();
            let right_data = values.get(3).unwrap();

            // IDs should match
            assert_eq!(left_id, right_id);

            // Verify the correct pairs are joined
            match left_id.get_val() {
                Val::Integer(1) => {
                    assert_eq!(
                        left_value.compare_equals(&Value::new("A")),
                        CmpBool::CmpTrue
                    );
                    assert_eq!(
                        right_data.compare_equals(&Value::new("X")),
                        CmpBool::CmpTrue
                    );
                }
                Val::Integer(2) => {
                    assert_eq!(
                        left_value.compare_equals(&Value::new("B")),
                        CmpBool::CmpTrue
                    );
                    assert_eq!(
                        right_data.compare_equals(&Value::new("Y")),
                        CmpBool::CmpTrue
                    );
                }
                _ => panic!("Unexpected join result"),
            }
        }
    }

    #[test]
    fn test_join_predicate() {
        initialize_logger();
        // Create simple schemas
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create join predicate (left.id = right.id)
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index = 0 for left table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple_index = 1 for right table
            0, // column_index for id
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        // Verify column references are set up correctly
        if let Expression::ColumnRef(left_expr) = left_col.as_ref() {
            debug!(
                "Left column reference - tuple_index: {}, column_index: {}",
                left_expr.get_tuple_index(),
                left_expr.get_column_index()
            );
        }
        if let Expression::ColumnRef(right_expr) = right_col.as_ref() {
            debug!(
                "Right column reference - tuple_index: {}, column_index: {}",
                right_expr.get_tuple_index(),
                right_expr.get_column_index()
            );
        }

        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![],
        )));

        // Verify the predicate's column references
        if let Expression::Comparison(comp) = predicate.as_ref() {
            if let Expression::ColumnRef(left_expr) = comp.get_left().as_ref() {
                debug!(
                    "Predicate left column reference - tuple_index: {}, column_index: {}",
                    left_expr.get_tuple_index(),
                    left_expr.get_column_index()
                );
            }
            if let Expression::ColumnRef(right_expr) = comp.get_right().as_ref() {
                debug!(
                    "Predicate right column reference - tuple_index: {}, column_index: {}",
                    right_expr.get_tuple_index(),
                    right_expr.get_column_index()
                );
            }
        }

        // Create test tuples
        let left_tuple = Tuple::new(
            &vec![Value::new(1), Value::new("A")],
            left_schema.clone(),
            RID::new(0, 0),
        );
        let right_tuple = Tuple::new(
            &vec![Value::new(1), Value::new("X")],
            right_schema.clone(),
            RID::new(0, 0),
        );

        debug!("Left tuple values: {:?}", left_tuple.get_values());
        debug!("Right tuple values: {:?}", right_tuple.get_values());
        debug!("Left schema: {:?}", left_schema);
        debug!("Right schema: {:?}", right_schema);
        debug!("Predicate: {:?}", predicate);

        // Test predicate evaluation
        let result = predicate
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();

        debug!("Predicate evaluation result: {:?}", result);

        match result.get_val() {
            Val::Boolean(b) => {
                debug!("Boolean result: {}", b);
                assert!(b, "Predicate should evaluate to true for matching IDs");
            }
            other => {
                panic!(
                    "Expected boolean result from predicate evaluation, got: {:?}",
                    other
                );
            }
        }
    }
}
