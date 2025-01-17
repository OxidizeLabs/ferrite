use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::execution_context::ExecutionContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::comparison_expression::ComparisonType;
use crate::execution::expressions::logic_expression::LogicType;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::index_scan_plan::IndexScanNode;
use crate::storage::index::index::IndexInfo;
use crate::storage::index::index_iterator::IndexIterator;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use log::{debug, error, info};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct IndexScanExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<IndexScanNode>,
    table_heap: Arc<TableHeap>,
    initialized: bool,
    iterator: Option<IndexIterator>,
}

impl IndexScanExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<IndexScanNode>) -> Self {
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

        // Clone the table heap while we have the lock
        let table_heap = table_info.get_table_heap().clone();

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
                }
                ranges
            }
            Expression::Comparison(comp_expr) => {
                if let Ok(value) = comp_expr.get_right().evaluate(
                    &Tuple::new(&vec![], Schema::new(vec![]), RID::new(0, 0)),
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
                    }
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    fn analyze_bounds(&self, index_info: &IndexInfo) -> (Option<Tuple>, Option<Tuple>) {
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
            .map(|v| Tuple::new(&vec![v], key_schema.clone(), RID::new(0, 0)));

        let end_tuple = final_end
            .clone()
            .map(|v| Tuple::new(&vec![v], key_schema.clone(), RID::new(0, 0)));

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

            // Create iterator
            self.iterator = Some(IndexIterator::new(btree.clone(), start_key, end_key));
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("IndexScanExecutor not initialized, initializing now");
            self.init();
        }

        // Get schema upfront
        let output_schema = self.get_output_schema().clone();
        let predicate_keys = self.plan.get_predicate_keys();

        // Get iterator reference
        let iter = self.iterator.as_mut()?;

        // Keep trying until we find a valid tuple or reach the end
        loop {
            match iter.next() {
                Some(rid) => {
                    debug!("Found RID {:?} in index", rid);

                    // Use RID to fetch tuple from table heap
                    match self.table_heap.get_tuple(rid) {
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
                                    match result.get_value() {
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
                            return Some((tuple, rid));
                        }
                        Err(e) => {
                            error!("Failed to fetch tuple for RID {:?}: {:?}", rid, e);
                            continue;
                        }
                    }
                }
                None => {
                    info!("Reached end of index scan");
                    return None;
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
mod index_scan_executor_tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::config::{IndexOidT, TableOidT};
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::expressions::comparison_expression::ComparisonExpression;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::index::index::IndexType;
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::{CmpBool, Type};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use crate::execution::transaction_context::TransactionContext;

    struct TestContext {
        execution_context: Arc<RwLock<ExecutionContext>>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            // Initialize logger first using the common module
            // initialize_logger();
            let timestamp = chrono::Utc::now().timestamp();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));
            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                10,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool_manager.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                catalog.clone(),
                log_manager,
            )));

            let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager.clone())));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
                buffer_pool_manager,
                catalog,
                transaction_context
            )));

            Self {
                execution_context,
                db_file,
                log_file,
            }
        }

        pub fn execution_context(&self) -> Arc<RwLock<ExecutionContext>> {
            self.execution_context.clone()
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
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
    ) -> (String, String, TableOidT, IndexOidT) {
        let context_guard = context.read();
        let catalog = context_guard.get_catalog();
        let mut catalog_guard = catalog.write();

        // Create table
        let table_name = "test_table".to_string();
        let table_info = catalog_guard
            .create_table(&table_name, schema.clone())
            .unwrap();
        let table_id = table_info.get_table_oidt();

        // Get table info to confirm index
        let table_info = catalog_guard.get_table(&table_name).unwrap();
        let table_heap = table_info.get_table_heap();

        // Insert test data
        for i in 1..=10 {
            let mut tuple = Tuple::new(
                &vec![Value::new(i as i32), Value::new(i as i32 * 10)],
                schema.clone(),
                RID::new(0, i),
            );
            let tuple_meta = TupleMeta::new(i as u64, false);
            table_heap.insert_tuple(&tuple_meta, &mut tuple).unwrap();
        }

        // Create index on id column
        let index_name = "test_index".to_string();
        let key_schema = Schema::new(vec![schema.get_column(0).unwrap().clone()]);

        catalog_guard.create_index(
            &index_name,
            &table_name,
            key_schema,
            vec![0],
            4, // key size
            false,
            IndexType::BPlusTreeIndex,
        );
        let index_id = catalog_guard.get_table_indexes(&table_name)[0].get_index_oid();

        (table_name, index_name, table_id, index_id)
    }

    fn insert_test_data(context: &Arc<RwLock<ExecutionContext>>, table_name: &str) {
        let context_guard = context.read();
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();

        let table_info = catalog_guard.get_table(table_name).unwrap();
        let table_heap = table_info.get_table_heap();
        let schema = table_info.get_table_schema();

        // Insert test data
        for i in 1..=10 {
            let mut tuple = Tuple::new(
                &vec![Value::new(i), Value::new(i * 10)],
                schema.clone(),
                RID::new(0, 0),
            );
            let tuple_meta = TupleMeta::new(i, false);
            table_heap.insert_tuple(&tuple_meta, &mut tuple).unwrap();
        }
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

    #[test]
    fn test_index_scan_full() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_full");
        let context = ctx.execution_context();

        let (table_name, index_name, table_id, index_id) = setup_test_table(&schema, &context);
        insert_test_data(&context, &table_name);

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
        while let Some((tuple, _)) = executor.next() {
            count += 1;
            println!("Got tuple: {:?}", tuple);
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_index_scan_equality() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_equality");
        let context = ctx.execution_context();
        let (table_name, index_name, table_id, index_id) = setup_test_table(&schema, &context);
        insert_test_data(&context, &table_name);

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
        while let Some((tuple, _)) = executor.next() {
            count += 1;
            let val0 = tuple.get_value(0);
            let val1 = tuple.get_value(1);
            let count_value = Value::new(5);
            let count_value_10x = Value::new(50);
            assert_eq!(val0.compare_equals(&count_value), CmpBool::CmpTrue);
            assert_eq!(val1.compare_equals(&count_value_10x), CmpBool::CmpTrue);
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn test_index_scan_range() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_range");
        let context = ctx.execution_context();

        let (table_name, index_name, table_id, index_id) = setup_test_table(&schema, &context);
        insert_test_data(&context, &table_name);

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
        while let Some((tuple, _)) = executor.next() {
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
            // assert_eq!(val.compare_equals(&Value::new(100)), CmpBool::CmpTrue);
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_index_scan_with_deletion() {
        let schema = create_test_schema();
        let ctx = TestContext::new("test_index_scan_with_deletion");
        let context = ctx.execution_context();
        let (table_name, index_name, table_id, index_id) = setup_test_table(&schema, &context);
        insert_test_data(&context, &table_name);

        // Delete tuples with id = 3 and id = 7
        let context_guard = context.read();
        let catalog = context_guard.get_catalog();
        let catalog_guard = catalog.read();
        let table_info = catalog_guard.get_table(&table_name).unwrap();
        let table_heap = table_info.get_table_heap();

        // Mark tuples as deleted
        for i in &[3, 7] {
            let mut iterator = table_heap.make_iterator();
            while let Some((mut meta, tuple)) = iterator.next() {
                if tuple.get_value(0).compare_equals(&Value::new(*i)) == CmpBool::CmpTrue {
                    meta.mark_as_deleted();
                    table_heap.update_tuple_meta(&meta, tuple.get_rid());
                    break;
                }
            }
        }

        // Scan all tuples
        let plan = Arc::new(IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![], // no predicates
        ));

        let mut executor = IndexScanExecutor::new(context.clone(), plan);

        // Should only see non-deleted tuples
        let mut count = 0;
        let mut seen_ids = Vec::new();
        while let Some((tuple, _)) = executor.next() {
            count += 1;
            let id = tuple.get_value(0);
            seen_ids.push(id.clone());
            assert_eq!(id.compare_not_equals(&Value::new(3)), CmpBool::CmpTrue);
            assert_eq!(id.compare_not_equals(&Value::new(7)), CmpBool::CmpTrue);
        }
        assert_eq!(count, 8, "Should see 8 non-deleted tuples");
        assert!(
            !seen_ids.contains(&Value::new(3)),
            "Should not contain deleted ID 3"
        );
        assert!(
            !seen_ids.contains(&Value::new(7)),
            "Should not contain deleted ID 7"
        );
    }
}
