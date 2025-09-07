use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::comparison_expression::ComparisonType;
use crate::sql::execution::expressions::logic_expression::LogicType;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::storage::index::index::IndexInfo;
use crate::storage::index::index_iterator_mem::IndexIterator;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use log::{debug, error, info};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct IndexScanExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<IndexScanNode>,
    table_heap: Arc<TransactionalTableHeap>,
    initialized: bool,
    iterator: Option<IndexIterator>,
}

impl IndexScanExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<IndexScanNode>,
    ) -> Self {
        let table_name = plan.get_table_name();
        debug!(
            "Creating IndexScanExecutor for table '{}' using index '{}'",
            table_name,
            plan.get_index_name()
        );

        // Clone Arc before getting read lock
        let context_clone = context.clone();

        // Get the context lock
        debug!("Attempting to acquire context read lock");
        let context_guard = context_clone.read();

        // Get catalog within its own scope
        debug!("Attempting to acquire catalog read lock");
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();

        // Get table info
        let table_info = match catalog_guard.get_table(table_name) {
            Some(info) => {
                debug!("Found table '{}' in catalog", table_name);
                info
            }
            None => {
                error!("Table '{}' not found in catalog", table_name);
                panic!("Table not found");
            }
        };

        // Create TransactionalTableHeap
        let table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Verify index exists
        if catalog_guard
            .get_index_by_index_oid(plan.get_index_id())
            .is_none()
        {
            error!("Index '{}' not found in catalog", plan.get_index_name());
            panic!("Index not found");
        }

        Self {
            context,
            plan,
            table_heap,
            iterator: None,
            initialized: false,
        }
    }

    fn analyze_predicate_ranges(
        expr: &Expression,
    ) -> Vec<(Option<Value>, bool, Option<Value>, bool)> {
        match expr {
            Expression::Logic(logic_expr) => {
                let mut ranges = Vec::new();

                match logic_expr.get_logic_type() {
                    LogicType::And => {
                        // For AND, find the most restrictive range
                        let mut start_value = None;
                        let mut end_value = None;
                        let mut include_start = false;
                        let mut include_end = false;

                        // Process each child of AND
                        for child in logic_expr.get_children() {
                            let child_ranges = Self::analyze_predicate_ranges(child);
                            for (child_start, child_start_incl, child_end, child_end_incl) in
                                child_ranges
                            {
                                // Update start bound if more restrictive
                                if let Some(value) = child_start {
                                    match &start_value {
                                        None => {
                                            start_value = Some(value);
                                            include_start = child_start_incl;
                                        }
                                        Some(current) if value > *current => {
                                            start_value = Some(value);
                                            include_start = child_start_incl;
                                        }
                                        _ => {}
                                    }
                                }

                                // Update end bound if more restrictive
                                if let Some(value) = child_end {
                                    match &end_value {
                                        None => {
                                            end_value = Some(value);
                                            include_end = child_end_incl;
                                        }
                                        Some(current) if value < *current => {
                                            end_value = Some(value);
                                            include_end = child_end_incl;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        ranges.push((start_value, include_start, end_value, include_end));
                    }
                    LogicType::Or => {
                        // For OR, collect all ranges and union them
                        for child in logic_expr.get_children() {
                            ranges.extend(Self::analyze_predicate_ranges(child));
                        }
                    }
                    LogicType::Not => {
                        // For NOT operations on comparisons, we need to invert the bounds
                        if let Some(child) = logic_expr.get_children().first() {
                            match child.as_ref() {
                                Expression::Comparison(comp_expr) => {
                                    if let Ok(value) = comp_expr.get_right().evaluate(
                                        &Tuple::new(&[], &Schema::new(vec![]), RID::new(0, 0)),
                                        &Schema::new(vec![]),
                                    ) {
                                        match comp_expr.get_comp_type() {
                                            ComparisonType::Equal => {
                                                // NOT (x = value) -> x < value OR x > value
                                                // This requires full scan as it creates a disjoint range
                                                Vec::new()
                                            }
                                            ComparisonType::GreaterThan => {
                                                // NOT (x > value) -> x <= value
                                                vec![(None, false, Some(value), true)]
                                            }
                                            ComparisonType::GreaterThanOrEqual => {
                                                // NOT (x >= value) -> x < value
                                                vec![(None, false, Some(value), false)]
                                            }
                                            ComparisonType::LessThan => {
                                                // NOT (x < value) -> x >= value
                                                vec![(Some(value), true, None, false)]
                                            }
                                            ComparisonType::LessThanOrEqual => {
                                                // NOT (x <= value) -> x > value
                                                vec![(Some(value), false, None, false)]
                                            }
                                            ComparisonType::NotEqual => {
                                                // NOT (x != value) -> x = value
                                                vec![(Some(value.clone()), true, Some(value), true)]
                                            }
                                            ComparisonType::IsNotNull => {
                                                // NOT (IS NOT NULL) -> IS NULL
                                                // This requires special handling at runtime
                                                Vec::new()
                                            }
                                        }
                                    } else {
                                        Vec::new()
                                    }
                                }
                                _ => Vec::new(), // NOT on non-comparison expressions
                            }
                        } else {
                            Vec::new()
                        };
                    }
                }
                ranges
            }
            Expression::Comparison(comp_expr) => {
                if let Ok(value) = comp_expr.get_right().evaluate(
                    &Tuple::new(&[], &Schema::new(vec![]), RID::new(0, 0)),
                    &Schema::new(vec![]),
                ) {
                    match comp_expr.get_comp_type() {
                        ComparisonType::Equal => {
                            // For equality, use same value for both bounds, inclusive
                            vec![(Some(value.clone()), true, Some(value), true)]
                        }
                        ComparisonType::GreaterThan => {
                            // x > value
                            vec![(Some(value), false, None, false)]
                        }
                        ComparisonType::GreaterThanOrEqual => {
                            // x >= value
                            vec![(Some(value), true, None, false)]
                        }
                        ComparisonType::LessThan => {
                            // x < value
                            vec![(None, false, Some(value), false)]
                        }
                        ComparisonType::LessThanOrEqual => {
                            // x <= value
                            vec![(None, false, Some(value), true)]
                        }
                        ComparisonType::NotEqual => {
                            // Full scan for not equal
                            Vec::new()
                        }
                        ComparisonType::IsNotNull => {
                            // For IS NOT NULL, scan all non-null values
                            // This effectively means no bounds, as we'll filter nulls later
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    fn analyze_bounds(&self, index_info: &IndexInfo) -> (Option<Arc<Tuple>>, Option<Arc<Tuple>>) {
        let predicate_keys = self.plan.get_predicate_keys();
        let mut all_ranges = Vec::new();

        for pred in predicate_keys {
            all_ranges.extend(Self::analyze_predicate_ranges(pred.as_ref()));
        }

        // If we have no ranges to analyze, return None for full scan
        if all_ranges.is_empty() {
            return (None, None);
        }

        // Find the widest range that covers all predicates
        let mut final_start = None;
        let mut final_end = None;
        let mut final_start_incl = false;
        let mut final_end_incl = false;

        for (start, start_incl, end, end_incl) in all_ranges {
            // Take minimum start value
            match (&final_start, start) {
                (None, Some(value)) => {
                    final_start = Some(value);
                    final_start_incl = start_incl;
                }
                (Some(current), Some(value)) if value < *current => {
                    final_start = Some(value);
                    final_start_incl = start_incl;
                }
                _ => {}
            }

            // Take maximum end value
            match (&final_end, end) {
                (None, Some(value)) => {
                    final_end = Some(value);
                    final_end_incl = end_incl;
                }
                (Some(current), Some(value)) if value > *current => {
                    final_end = Some(value);
                    final_end_incl = end_incl;
                }
                _ => {}
            }
        }

        // Create tuples using the index's key schema
        let key_schema = index_info.get_key_schema().clone();

        let start_tuple = final_start
            .clone()
            .map(|v| Arc::new(Tuple::new(&[v], &key_schema, RID::new(0, 0))));

        let end_tuple = final_end
            .clone()
            .map(|v| Arc::new(Tuple::new(&[v], &key_schema, RID::new(0, 0))));

        debug!(
            "Scan bounds: start={:?} (incl={}), end={:?} (incl={})",
            final_start, final_start_incl, final_end, final_end_incl
        );

        (start_tuple, end_tuple)
    }
}

impl AbstractExecutor for IndexScanExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        let context_guard = self.context.read();
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();

        if let Some((index_info, btree)) =
            catalog_guard.get_index_by_index_oid(self.plan.get_index_id())
        {
            // Analyze predicates and create bounds...
            let (start_key, end_key) = self.analyze_bounds(&index_info);

            // Create iterator with Arc<Tuple> arguments
            self.iterator = Some(IndexIterator::new(btree.clone(), start_key, end_key));
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            debug!("IndexScanExecutor not initialized, initializing now");
            self.init();
        }

        // Proceed directly with index scan

        // Get schema upfront
        let output_schema = self.get_output_schema().clone();
        let predicate_keys = self.plan.get_predicate_keys();

        // Get transaction context from execution context
        let txn_ctx = {
            let context = self.context.read();
            context.get_transaction_context().clone()
        };

        // Get iterator reference
        let iter = match self.iterator.as_mut() {
            Some(i) => i,
            None => return Ok(None),
        };

        // Keep trying until we find a valid tuple or reach the end
        while let Some(rid) = iter.next() {
            debug!("Found RID {:?} in index", rid);

            // Use RID to fetch tuple from table heap with transaction context
            match self.table_heap.get_tuple(rid, txn_ctx.clone()) {
                Ok((meta, tuple)) => {
                    // Skip deleted tuples
                    if meta.is_deleted() {
                        debug!("Skipping deleted tuple with RID {:?}", rid);
                        continue;
                    }

                    // Additional check against predicates
                    let mut predicates_match = true;
                    for predicate in predicate_keys {
                        if let Ok(result) = predicate.evaluate(&tuple, &output_schema) {
                            match result.get_val() {
                                Val::Boolean(b) => {
                                    if !b {
                                        predicates_match = false;
                                        break;
                                    }
                                }
                                _ => {
                                    predicates_match = false;
                                    break;
                                }
                            }
                        }
                    }

                    if !predicates_match {
                        debug!("Tuple does not match all predicates, skipping");
                        continue;
                    }

                    debug!("Successfully fetched tuple for RID {:?}", rid);
                    return Ok(Some((tuple, rid)));
                }
                Err(e) => {
                    debug!("Failed to fetch tuple for RID {:?}, skipping: {}", rid, e);
                    continue;
                }
            }
        }

        info!("Reached end of index scan");
        Ok(None)
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
mod index_scan_executor_tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::config::{IndexOidT, TableOidT};
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::ComparisonExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::index::index::IndexType;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        execution_context: Arc<RwLock<ExecutionContext>>,
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
                lock_manager,
                transaction_manager.clone(),
            ));

            let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
                bpm,
                catalog,
                transaction_context,
            )));

            Self {
                execution_context,
                _temp_dir: temp_dir,
            }
        }

        fn transaction_context(&self) -> Arc<TransactionContext> {
            self.execution_context
                .read()
                .get_transaction_context()
                .clone()
        }

        pub fn execution_context(&self) -> Arc<RwLock<ExecutionContext>> {
            self.execution_context.clone()
        }
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        let exec_ctx = ctx.execution_context.read();
        let bpm = Arc::clone(&ctx.execution_context.read().get_buffer_pool_manager()); // Access the Arc directly
        let txn_mgr = exec_ctx.get_transaction_context().get_transaction_manager();

        Catalog::new(
            bpm,             // Already an Arc<BufferPoolManager>
            txn_mgr.clone(), // Clone the Arc<TransactionManager>
        )
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ])
    }

    fn setup_test_table(
        schema: &Schema,
        context: &Arc<RwLock<ExecutionContext>>,
    ) -> (
        String,
        String,
        TableOidT,
        IndexOidT,
        Arc<TransactionalTableHeap>,
    ) {
        let context_guard = context.read();
        let catalog = context_guard.get_catalog();
        let mut catalog_guard = catalog.write();
        let transaction_context = context_guard.get_transaction_context();

        // Create table
        let table_name = "test_table".to_string();
        let table_info = catalog_guard
            .create_table(table_name.clone(), schema.clone())
            .unwrap();
        let table_id = table_info.get_table_oidt();

        // Create transactional table heap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_info.get_table_heap(),
            table_info.get_table_oidt(),
        ));

        // Create index
        let index_name = "test_index".to_string();
        let key_schema = Schema::new(vec![schema.get_column(0).unwrap().clone()]);

        let index_info = catalog_guard
            .create_index(
                &index_name,
                &table_name,
                key_schema,
                vec![0],
                4,
                false,
                IndexType::BPlusTreeIndex,
            )
            .expect("Failed to create index");

        let index_id = index_info.0.get_index_oid();

        // Get index info
        let (_, btree) = catalog_guard.get_index_by_index_oid(index_id).unwrap();

        // Insert test data
        {
            let mut btree_guard = btree.write();

            // Insert test data
            for i in 1..=10 {
                let values = vec![Value::new(i), Value::new(i * 10)];

                // Insert using transactional table heap
                let rid = txn_table_heap
                    .insert_tuple_from_values(values, &schema, transaction_context.clone())
                    .unwrap();

                // Insert into index
                btree_guard.insert(Value::new(i), rid);
            }
        }

        (table_name, index_name, table_id, index_id, txn_table_heap)
    }

    fn create_predicate_expression(op: ComparisonType, value: i32) -> Arc<Expression> {
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        Arc::new(Expression::Comparison(ComparisonExpression::new(
            left,
            right,
            op,
            vec![],
        )))
    }

    fn create_test_values(id: i32) -> Vec<Value> {
        let values = vec![
            Value::new(id),      // id column
            Value::new(id * 10), // value column
        ];
        values
    }



    #[tokio::test]
    async fn test_index_scan_full() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_full").await;
        let context = ctx.execution_context();

        let (table_name, index_name, table_id, index_id, _) = setup_test_table(&schema, &context);

        let plan = Arc::new(IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![], // no predicates
        ));

        let mut executor = IndexScanExecutor::new(context.clone(), plan);

        let mut count = 0;
        while let Ok(Some((tuple, _))) = executor.next() {
            count += 1;
            println!("Got tuple: {:?}", tuple);
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_index_scan_equality() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_equality").await;
        let context = ctx.execution_context();

        let (table_name, index_name, table_id, index_id, _) = setup_test_table(&schema, &context);

        // id = 5
        let predicate = create_predicate_expression(ComparisonType::Equal, 5);

        let plan = Arc::new(IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![predicate],
        ));

        let mut executor = IndexScanExecutor::new(context.clone(), plan);

        let mut count = 0;
        while let Ok(Some((tuple, _))) = executor.next() {
            count += 1;
            let val0 = tuple.get_value(0);
            let val1 = tuple.get_value(1);
            assert_eq!(val0.compare_equals(&Value::new(5)), CmpBool::CmpTrue);
            assert_eq!(val1.compare_equals(&Value::new(50)), CmpBool::CmpTrue);
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_index_scan_range() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_range").await;
        let context = ctx.execution_context();

        let (table_name, index_name, table_id, index_id, _) = setup_test_table(&schema, &context);

        // 3 <= id <= 7
        let pred_low = create_predicate_expression(ComparisonType::GreaterThanOrEqual, 3);
        let pred_high = create_predicate_expression(ComparisonType::LessThanOrEqual, 7);

        let plan = Arc::new(IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![pred_low, pred_high],
        ));

        let mut executor = IndexScanExecutor::new(context.clone(), plan);

        let mut count = 0;
        while let Ok(Some((tuple, _))) = executor.next() {
            count += 1;
            let id = tuple.get_value(0);
            assert_eq!(
                id.compare_greater_than_equals(&Value::new(3)),
                CmpBool::CmpTrue
            );
            assert_eq!(
                id.compare_less_than_equals(&Value::new(7)),
                CmpBool::CmpTrue
            );
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_index_scan_with_deletion() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_with_deletion").await;
        let context = ctx.execution_context();
        let (table_name, index_name, table_id, index_id, txn_table_heap) =
            setup_test_table(&schema, &context);

        let txn_ctx = context.read().get_transaction_context();

        // Mark tuples as deleted using delete_tuple
        for i in &[3, 7] {
            let mut iterator = txn_table_heap.make_iterator(Some(txn_ctx.clone()));

            while let Some((_, tuple)) = iterator.next() {
                let id = tuple.get_value(0).compare_equals(&Value::new(*i));
                let rid = tuple.get_rid();

                if id == CmpBool::CmpTrue {
                    // Use delete_tuple instead of manually setting deleted flag
                    let _ = txn_table_heap.delete_tuple(rid, txn_ctx.clone());
                    break;
                }
            }
        }

        // Commit the transaction that performed the deletes
        let txn_manager = txn_ctx.get_transaction_manager();
        txn_manager.commit(
            txn_ctx.get_transaction(),
            context.read().get_buffer_pool_manager(),
        ).await;

        // Create new transaction for scanning with new execution context
        let scan_txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        let scan_txn_ctx = Arc::new(TransactionContext::new(
            scan_txn,
            Arc::new(LockManager::new()),
            txn_manager.clone(),
        ));

        // Create new execution context with the new transaction
        let scan_context = Arc::new(RwLock::new(ExecutionContext::new(
            context.read().get_buffer_pool_manager(),
            context.read().get_catalog().clone(),
            scan_txn_ctx.clone(),
        )));

        // Create and execute scan with new context
        let plan = Arc::new(IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![], // no predicates
        ));

        let mut executor = IndexScanExecutor::new(scan_context.clone(), plan);

        let mut count = 0;
        let mut seen_ids = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            count += 1;
            let id = tuple.get_value(0);
            seen_ids.push(id.clone());

            assert_eq!(
                id.compare_not_equals(&Value::new(3)),
                CmpBool::CmpTrue,
                "Found deleted tuple with ID 3"
            );
            assert_eq!(
                id.compare_not_equals(&Value::new(7)),
                CmpBool::CmpTrue,
                "Found deleted tuple with ID 7"
            );
        }
        assert_eq!(count, 8, "Should see 8 non-deleted tuples");

        // Verify we see the expected IDs
        for i in 1..=10 {
            if i != 3 && i != 7 {
                assert!(
                    seen_ids
                        .iter()
                        .any(|id| id.compare_equals(&Value::new(i)) == CmpBool::CmpTrue),
                    "Missing ID {}",
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_index_scan_with_deleted_tuples() {
        let test_context = TestContext::new("index_scan_deleted").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let transaction_context = test_context.transaction_context();

        // Create table and insert data
        {
            let mut catalog_guard = catalog.write();
            let table_info = catalog_guard
                .create_table("test_table".to_string(), schema.clone())
                .unwrap();
            let table_heap = Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ));

            // Insert test data
            for i in 1..=10 {
                let values = create_test_values(i);
                table_heap
                    .insert_tuple_from_values(values, &schema, transaction_context.clone())
                    .unwrap();
            }

            // Delete tuples with IDs 3 and 7
            let mut iterator = table_heap.make_iterator(Some(transaction_context.clone()));
            while let Some((meta, mut tuple)) = iterator.next() {
                // Store both id and rid before any mutable borrows
                let id = tuple.get_value(0).as_integer().unwrap();
                let rid = tuple.get_rid();

                if id == 3 || id == 7 {
                    // Create a new TupleMeta with deleted flag set to true
                    let new_meta =
                        Arc::new(TupleMeta::new_with_delete(meta.get_creator_txn_id(), true));

                    let _ = table_heap.update_tuple(
                        &new_meta,
                        &mut tuple,
                        rid, // Use the stored rid
                        transaction_context.clone(),
                    );
                }
            }
        }
    }
}
