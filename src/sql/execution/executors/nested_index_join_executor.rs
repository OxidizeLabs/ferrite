use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::executors::index_scan_executor::IndexScanExecutor;
use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val;
use log::{debug, trace};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct NestedIndexJoinExecutor {
    children_executors: Option<Vec<Box<dyn AbstractExecutor>>>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<NestedIndexJoinNode>,
    initialized: bool,
    current_left_tuple: Option<(Arc<Tuple>, RID)>, // Current tuple from outer (left) relation
    right_exhausted: bool, // Whether we've exhausted the inner (right) relation for current left tuple
    current_right_executor: Option<Box<dyn AbstractExecutor>>, // Track current right relation executor (either index scan or seq scan)
    processed_right_rids: Vec<RID>, // Keep track of right RIDs we've already processed for current left tuple
}

impl NestedIndexJoinExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<NestedIndexJoinNode>) -> Self {
        debug!("Creating NestedIndexJoinExecutor");
        Self {
            children_executors: None,
            context,
            plan,
            initialized: false,
            current_left_tuple: None,
            right_exhausted: false,
            current_right_executor: None,
            processed_right_rids: Vec::new(),
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

    fn construct_output_tuple(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Arc<Tuple> {
        let mut joined_values = left_tuple.get_values().clone();
        joined_values.extend(right_tuple.get_values().clone());

        // Create tuple with combined values and a placeholder RID
        Arc::new(Tuple::new(
            &joined_values,
            &self.get_output_schema(),
            RID::new(0, 0), // Use a placeholder RID for joined tuples
        ))
    }

    fn get_next_inner_tuple(&mut self) -> Option<(Arc<Tuple>, RID)> {
        // If we have a current right executor, try to get next tuple from it
        if let Some(executor) = &mut self.current_right_executor {
            loop {
                if let Some(tuple_and_rid) = executor.next() {
                    let right_rid = tuple_and_rid.1;
                    // Skip if we've already processed this right tuple for the current left tuple
                    if self.processed_right_rids.contains(&right_rid) {
                        debug!("Skipping already processed RID: {:?}", right_rid);
                        continue; // Skip to next tuple
                    }

                    // Add the RID to processed list
                    debug!("Adding RID to processed list: {:?}", right_rid);
                    self.processed_right_rids.push(right_rid);

                    return Some(tuple_and_rid);
                } else {
                    // No more tuples from this executor
                    break;
                }
            }
            // If we get here, exhausted the executor
            self.current_right_executor = None;
            return None;
        }

        // Create executor for the current left tuple
        self.create_right_executor()?;

        // Now try to get the first result with the newly created executor
        if let Some(executor) = &mut self.current_right_executor {
            if let Some(tuple_and_rid) = executor.next() {
                let right_rid = tuple_and_rid.1;

                // Check if we've already processed this RID
                if self.processed_right_rids.contains(&right_rid) {
                    debug!(
                        "Skipping already processed RID (first result): {:?}",
                        right_rid
                    );
                    return self.get_next_inner_tuple(); // Recursively try next
                }

                // Add to processed list
                debug!(
                    "Adding RID to processed list (first result): {:?}",
                    right_rid
                );
                self.processed_right_rids.push(right_rid);

                return Some(tuple_and_rid);
            }
        }

        // No results found
        None
    }

    // New helper method to create the right executor
    fn create_right_executor(&mut self) -> Option<()> {
        // Get the current left tuple
        let (left_tuple, _) = self.current_left_tuple.as_ref()?;

        // Create new index scan for the current left tuple
        let right_schema = self.plan.get_right_schema().clone();
        let right_key_exprs = self.plan.get_right_key_expressions();
        let left_key_exprs = self.plan.get_left_key_expressions();

        // Evaluate left key expressions to get values for index lookup
        let mut predicate_keys = Vec::new();
        for (left_expr, right_expr) in left_key_exprs.iter().zip(right_key_exprs.iter()) {
            if let Ok(value) = left_expr.evaluate(left_tuple, self.plan.get_left_schema()) {
                debug!("Evaluated left key expression to: {:?}", value);

                // Create comparison expression for index scan
                let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
                    right_expr.clone(),
                    Arc::new(Expression::Constant(ConstantExpression::new(
                        value.clone(),
                        Column::new("const", TypeId::Integer),
                        vec![],
                    ))),
                    ComparisonType::Equal,
                    vec![],
                )));
                predicate_keys.push(predicate);
            } else {
                debug!("Failed to evaluate left key expression");
            }
        }

        // Get catalog info for right table
        let binding = self.context.read();
        let catalog = binding.get_catalog();
        let catalog_guard = catalog.read();

        // Extract table name from right child plan node
        let table_name = match self.plan.get_right_child() {
            PlanNode::SeqScan(scan) => scan.get_table_name(),
            PlanNode::IndexScan(scan) => scan.get_table_name(),
            // Add other cases as needed
            _ => return None, // Return None if we can't get table name
        };

        let table_info = catalog_guard.get_table(&table_name).unwrap();
        let table_indexes = catalog_guard.get_table_indexes(&table_name);

        // Find suitable index
        let index_info = match table_indexes.iter().find(|idx| {
            let key_schema = idx.get_key_schema();
            key_schema.get_column_count() as usize == right_key_exprs.len()
        }) {
            Some(idx) => idx,
            None => {
                // No suitable index found - create a sequential scan instead
                debug!("No suitable index found, falling back to sequential scan");
                let seq_scan_plan = Arc::new(SeqScanPlanNode::new(
                    right_schema.clone(),
                    table_info.get_table_oidt(),
                    table_name.to_string(),
                ));

                let mut seq_scan_executor =
                    SeqScanExecutor::new(self.context.clone(), seq_scan_plan);
                seq_scan_executor.init();

                // Store the executor for retrieving right tuples
                self.current_right_executor = Some(Box::new(seq_scan_executor));
                return Some(());
            }
        };

        // Create a dummy executor for the index scan
        let dummy_executor = Box::new(DummyExecutor {
            initialized: false,
            schema: right_schema.clone(),
            context: self.context.clone(),
        });

        // Create and execute index scan
        let index_scan_plan = Arc::new(IndexScanNode::new(
            right_schema,
            table_name.to_string(),
            table_info.get_table_oidt(),
            index_info.get_index_name().to_string(),
            index_info.get_index_oid(),
            predicate_keys,
        ));

        // Create new executor with dummy child
        let mut index_scan_executor =
            IndexScanExecutor::new(dummy_executor, self.context.clone(), index_scan_plan);

        index_scan_executor.init();

        // Store for future use
        self.current_right_executor = Some(Box::new(index_scan_executor));
        Some(())
    }
}

// Add DummyExecutor struct for index scan
struct DummyExecutor {
    initialized: bool,
    schema: Schema,
    context: Arc<RwLock<ExecutionContext>>,
}

impl AbstractExecutor for DummyExecutor {
    fn init(&mut self) {
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        None
    }

    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

impl AbstractExecutor for NestedIndexJoinExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("NestedIndexJoinExecutor already initialized");
            return;
        }

        debug!("Initializing NestedIndexJoinExecutor");

        // Initialize child executors vector
        let mut children = Vec::with_capacity(1); // Only need left executor

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

        self.children_executors = Some(children);
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if !self.initialized {
            debug!("NestedIndexJoinExecutor not initialized");
            return None;
        }

        loop {
            // If we have a current left tuple and haven't exhausted right relation
            if !self.right_exhausted {
                if let Some((left_tuple, left_rid)) = &self.current_left_tuple {
                    let left_tuple_clone = left_tuple.clone();
                    let left_rid_clone = *left_rid;

                    // Try to get next matching tuple from right relation using index
                    if let Some((right_tuple, _)) = self.get_next_inner_tuple() {
                        // Double-check the predicate since get_next_inner_tuple already filters by key equality
                        if self.evaluate_predicate(&left_tuple_clone, &right_tuple) {
                            debug!("Found matching tuple pair, constructing join result");
                            return Some((
                                self.construct_output_tuple(&left_tuple_clone, &right_tuple),
                                left_rid_clone,
                            ));
                        }
                        continue;
                    } else {
                        // No more matching tuples for this left tuple
                        debug!("No more right tuples for current left tuple");
                        self.right_exhausted = true;
                        self.current_right_executor = None;
                    }
                }
            }

            // Get next tuple from left relation
            if let Some(children) = &mut self.children_executors {
                if let Some(left_tuple) = children[0].next() {
                    debug!(
                        "Got new left tuple: {:?}, clearing processed RIDs list",
                        left_tuple.0.get_values()
                    );
                    self.current_left_tuple = Some(left_tuple);
                    self.right_exhausted = false;
                    // Clear the list of processed right RIDs for the new left tuple
                    self.processed_right_rids.clear();
                    continue;
                }
            }

            // No more tuples from left relation
            debug!("No more left tuples, ending join");
            return None;
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
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::executors::create_index_executor::CreateIndexExecutor;
    use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use crate::types_db::value::{Val, Value};
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    use std::collections::HashMap;
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
    fn test_nested_index_join_executor() {
        let ctx = TestContext::new("test_nested_index_join_executor");

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
        let left_tuple_meta = Arc::new(TupleMeta::new(0));

        // Insert each tuple separately with proper Value objects
        for (id, value) in vec![(1, "A"), (2, "B"), (3, "C")] {
            let values = vec![Value::new(id), Value::new(value)];
            left_table
                .insert_tuple_from_values(values, &left_schema, left_tuple_meta.clone())
                .expect("Failed to insert into left table");
        }

        // Verify left table's first page ID
        debug!(
            "Left table first page ID: {}",
            left_table.get_first_page_id()
        );

        // Insert data into right table
        let right_tuple_meta = Arc::new(TupleMeta::new(0));

        // Insert each tuple separately with proper Value objects
        for (id, data) in vec![(1, "X"), (2, "Y"), (4, "Z")] {
            let values = vec![Value::new(id), Value::new(data)];
            right_table
                .insert_tuple_from_values(values, &right_schema, right_tuple_meta.clone())
                .expect("Failed to insert into right table");
        }

        // Verify right table's first page ID
        debug!(
            "Right table first page ID: {}",
            right_table.get_first_page_id()
        );

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
                debug!(
                    "Left table scan tuple at RID {:?}: {:?}",
                    rid,
                    tuple.get_values()
                );
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
                debug!(
                    "Right table scan tuple at RID {:?}: {:?}",
                    rid,
                    tuple.get_values()
                );
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

        // Create Index for right table
        let index_columns = vec![Column::new("id", TypeId::Integer)];
        let index_schema = Schema::new(index_columns);

        let index_plan = Arc::new(CreateIndexPlanNode::new(
            index_schema,
            "right_table".to_string(),
            "right_table_index".to_string(),
            vec![0],
            false, // not unique
        ));

        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            ctx.catalog(),
            ctx.transaction_context(),
        )));

        let mut index_executor = CreateIndexExecutor::new(exec_ctx.clone(), index_plan, false);
        index_executor.init();
        index_executor.next(); // Execute index creation

        // Populate the index with data from right table
        {
            let catalog = ctx.catalog.read();
            let table_info = catalog.get_table("right_table").unwrap();
            let index_info = catalog.get_table_indexes("right_table")[0].clone();
            let table_heap = table_info.get_table_heap();

            // Create a sequential scan to iterate through all tuples
            let right_scan = SeqScanPlanNode::new(
                right_schema.clone(),
                right_table_info.get_table_oidt(),
                "right_table".to_string(),
            );
            let mut right_executor = SeqScanExecutor::new(exec_ctx.clone(), Arc::new(right_scan));
            right_executor.init();

            // Manually insert entries into the index using the B+ tree directly
            let index_oid = index_info.get_index_oid();
            if let Some((_, btree)) = catalog.get_index_by_index_oid(index_oid) {
                let dummy_transaction = ctx.transaction_context.get_transaction();

                // Insert each tuple's keys into the index
                while let Some((tuple, rid)) = right_executor.next() {
                    // Extract the key value (id) from the tuple
                    let key_columns = vec![0]; // column index for "id"
                    let mut key_values = Vec::new();
                    for &col_idx in &key_columns {
                        key_values.push(tuple.get_values()[col_idx].clone());
                    }

                    // Create key tuple with just the indexed column
                    let key_schema = index_info.get_key_schema();
                    let key_tuple = Tuple::new(&key_values, key_schema, rid);

                    // Extract the key value for the B+ tree
                    let key_value = key_values[0].clone();

                    // Insert directly into B+ tree
                    let mut btree_write = btree.write();
                    btree_write.insert(key_value, rid);

                    debug!(
                        "Inserted index entry for key: {:?}, RID: {:?}",
                        key_values, rid
                    );
                }
            }
        }

        // Verify index was created
        {
            let catalog = ctx.catalog.read();
            let indexes = catalog.get_table_indexes("right_table");
            assert_eq!(indexes.len(), 1, "Index should be created");
            assert_eq!(indexes[0].get_index_name(), "right_table_index");
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
        let join_plan = Arc::new(NestedIndexJoinNode::new(
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
        let mut join_executor = NestedIndexJoinExecutor::new(exec_ctx, join_plan);
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
            &left_schema,
            RID::new(0, 0),
        );
        let right_tuple = Tuple::new(
            &vec![Value::new(1), Value::new("X")],
            &right_schema,
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
