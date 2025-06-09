use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use log::{debug, trace};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

/// Wrapper type for tuple and sort keys to implement custom ordering
#[derive(Eq, Clone)]
struct TupleWithKeys {
    sort_keys: Vec<Value>,
    tuple: Arc<Tuple>,
    rid: RID,
}

impl PartialEq for TupleWithKeys {
    fn eq(&self, other: &Self) -> bool {
        self.sort_keys.len() == other.sort_keys.len()
            && self
                .sort_keys
                .iter()
                .zip(other.sort_keys.iter())
                .all(|(a, b)| a.compare_not_equals(b) == CmpBool::CmpTrue)
    }
}

impl PartialOrd for TupleWithKeys {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TupleWithKeys {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare sort keys in ascending order for min-heap
        for (a, b) in self.sort_keys.iter().zip(other.sort_keys.iter()) {
            match a.compare_less_than(b) {
                CmpBool::CmpTrue => return Ordering::Less,
                CmpBool::CmpFalse => {
                    match a.compare_greater_than(b) {
                        CmpBool::CmpTrue => return Ordering::Greater,
                        CmpBool::CmpFalse => continue, // Equal, check next key
                        CmpBool::CmpNull => return Ordering::Equal, // Treat NULL as equal
                    }
                }
                CmpBool::CmpNull => return Ordering::Equal, // Treat NULL as equal
            }
        }
        Ordering::Equal
    }
}

pub struct TopNPerGroupExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<TopNPerGroupNode>,
    child: Box<dyn AbstractExecutor>,
    // Map from group key to min-heap of top K elements for that group
    groups: HashMap<Vec<Value>, BinaryHeap<Reverse<TupleWithKeys>>>,
    // Track which group and index we're currently returning from
    current_group: Option<Vec<Value>>,
    group_keys: Vec<Vec<Value>>,
    group_index: usize,
    // Store sorted tuples for current group
    current_group_tuples: Vec<TupleWithKeys>,
    current_tuple_index: usize,
    initialized: bool,
}

impl TopNPerGroupExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<TopNPerGroupNode>) -> Self {
        debug!("Creating TopNPerGroupExecutor");

        let child_plan = plan.get_child().expect("TopNPerGroup should have a child");
        let child = child_plan
            .create_executor(Arc::clone(&context))
            .expect("Failed to create child executor");

        Self {
            context,
            plan,
            child,
            groups: HashMap::new(),
            current_group: None,
            group_keys: Vec::new(),
            group_index: 0,
            current_group_tuples: Vec::new(),
            current_tuple_index: 0,
            initialized: false,
        }
    }

    /// Evaluates the sort keys for a tuple
    fn evaluate_sort_keys(&self, tuple: &Tuple) -> Vec<Value> {
        let schema = self.plan.get_output_schema();
        let keys: Vec<Value> = self
            .plan
            .get_sort_expressions()
            .iter()
            .map(|expr| expr.evaluate(tuple, schema).unwrap())
            .collect();
        trace!("Evaluated sort keys for tuple: {:?}", keys);
        keys
    }

    /// Evaluates the group keys for a tuple
    fn evaluate_group_keys(&self, tuple: &Tuple) -> Vec<Value> {
        let schema = self.plan.get_output_schema();
        let keys: Vec<Value> = self
            .plan
            .get_group_expressions()
            .iter()
            .map(|expr| expr.evaluate(tuple, schema).unwrap())
            .collect();
        trace!("Evaluated group keys for tuple: {:?}", keys);
        keys
    }

    /// Compare two group keys using the Value comparison methods
    fn compare_group_keys(a: &[Value], b: &[Value]) -> Ordering {
        for (a_val, b_val) in a.iter().zip(b.iter()) {
            match a_val.compare_less_than(b_val) {
                CmpBool::CmpTrue => return Ordering::Less,
                CmpBool::CmpFalse => {
                    match a_val.compare_greater_than(b_val) {
                        CmpBool::CmpTrue => return Ordering::Greater,
                        CmpBool::CmpFalse => continue, // Equal, check next key
                        CmpBool::CmpNull => return Ordering::Equal, // Treat NULL as equal
                    }
                }
                CmpBool::CmpNull => return Ordering::Equal, // Treat NULL as equal
            }
        }
        // If all values are equal or we run out of values, compare lengths
        a.len().cmp(&b.len())
    }
}

impl AbstractExecutor for TopNPerGroupExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug!("Initializing TopNPerGroupExecutor");
        self.child.init();

        let n = self.plan.get_n();
        debug!("TopNPerGroup n value: {}", n);

        // Process all input tuples and maintain top N per group
        loop {
            match self.child.next() {
                Ok(Some((tuple, rid))) => {
                    let sort_keys = self.evaluate_sort_keys(&tuple);
                    let group_keys = self.evaluate_group_keys(&tuple);

                    debug!(
                        "Processing tuple with sort keys: {:?}, group keys: {:?}",
                        sort_keys, group_keys
                    );

                    let tuple_with_keys = TupleWithKeys {
                        sort_keys,
                        tuple,
                        rid,
                    };

                    // Get or create min-heap for this group
                    let group_heap = self
                        .groups
                        .entry(group_keys)
                        .or_insert_with(BinaryHeap::new);

                    if group_heap.len() < n {
                        // Heap not full yet, add element
                        debug!(
                            "Group heap not full (size {}), adding tuple",
                            group_heap.len()
                        );
                        group_heap.push(Reverse(tuple_with_keys));
                    } else if let Some(Reverse(current_min)) = group_heap.peek() {
                        // Compare with smallest element in heap
                        if tuple_with_keys > *current_min {
                            // Changed comparison
                            // Remove smallest and add new larger element
                            debug!("Replacing smallest element in group with larger tuple");
                            group_heap.pop();
                            group_heap.push(Reverse(tuple_with_keys));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    debug!("Error from child executor: {}", e);
                    return; // Initialize failed, but we don't propagate errors from init
                }
            }
        }

        // Sort group keys for deterministic iteration order
        self.group_keys = self.groups.keys().cloned().collect();
        self.group_keys
            .sort_by(|a, b| Self::compare_group_keys(a.as_slice(), b.as_slice()));

        debug!(
            "TopNPerGroupExecutor initialization complete. Found {} groups",
            self.groups.len()
        );
        self.initialized = true;
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        loop {
            // If we have tuples from the current group, return them
            if self.current_tuple_index < self.current_group_tuples.len() {
                let tuple_with_keys = &self.current_group_tuples[self.current_tuple_index];
                self.current_tuple_index += 1;
                return Ok(Some((tuple_with_keys.tuple.clone(), tuple_with_keys.rid)));
            }

            // Move to next group
            if self.group_index >= self.group_keys.len() {
                debug!("No more groups to process");
                return Ok(None);
            }

            let group_key = &self.group_keys[self.group_index];
            self.group_index += 1;

            if let Some(group_heap) = self.groups.get(group_key) {
                // Convert heap to sorted vector (largest first)
                let mut tuples: Vec<TupleWithKeys> = group_heap
                    .iter()
                    .map(|Reverse(tuple_with_keys)| (*tuple_with_keys).clone())
                    .collect();

                // Sort in descending order (reverse of natural order)
                tuples.sort_by(|a, b| b.cmp(a));

                debug!("Processing group with {} tuples", tuples.len());

                self.current_group_tuples = tuples;
                self.current_tuple_index = 0;
                self.current_group = Some(group_key.clone());

                // Continue loop to return first tuple from this group
            }
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
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::types_db::type_id::TypeId;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
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

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
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
    }

    #[tokio::test]
    async fn test_topn_per_group_executor() {
        let ctx = TestContext::new("test_topn_per_group_executor").await;

        // Create schema
        let schema = Schema::new(vec![
            Column::new("department", TypeId::Integer),
            Column::new("salary", TypeId::Integer),
        ]);

        // Create test tuples - two departments (1,2) with varying salaries
        let test_tuples = vec![
            // Department 1
            Tuple::new(
                &[Value::new(1), Value::new(100)],
                &schema.clone(),
                RID::new(0, 0),
            ),
            Tuple::new(
                &[Value::new(1), Value::new(200)],
                &schema.clone(),
                RID::new(0, 1),
            ),
            Tuple::new(&[Value::new(1), Value::new(300)], &schema, RID::new(0, 2)),
            // Department 2
            Tuple::new(&[Value::new(2), Value::new(150)], &schema, RID::new(0, 3)),
            Tuple::new(&[Value::new(2), Value::new(250)], &schema, RID::new(0, 4)),
            Tuple::new(&[Value::new(2), Value::new(350)], &schema, RID::new(0, 5)),
        ];

        // Create catalog
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm.clone(),
            ctx.transaction_manager.clone(),
        )));

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            catalog,
            ctx.transaction_context.clone(),
        )));

        // Convert tuples for mock scan
        let test_data: Vec<(Vec<Value>, RID)> = test_tuples
            .into_iter()
            .map(|tuple| (tuple.get_values().to_vec(), tuple.get_rid()))
            .collect();

        // Create mock scan
        let mock_scan = Arc::new(
            MockScanNode::new(schema.clone(), "test_table".to_string(), vec![])
                .with_tuples(test_data),
        );

        // Create group by expression (department)
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0, // department column
            Column::new("department", TypeId::Integer),
            vec![],
        )));

        // Create sort expression (salary)
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1, // salary column
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        // Create TopNPerGroup plan
        let topn_plan = Arc::new(TopNPerGroupNode::new(
            2, // top 2 per group
            vec![sort_expr],
            vec![group_expr],
            schema,
            vec![PlanNode::MockScan((*mock_scan).clone())],
        ));

        // Create and initialize executor
        let mut executor = TopNPerGroupExecutor::new(execution_context, topn_plan);
        executor.init();

        // Verify results - should get top 2 salaries from each department
        // Department 1
        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.0.get_value(0).as_integer().unwrap(), 1); // dept
        assert_eq!(result.0.get_value(1).as_integer().unwrap(), 300); // salary

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.0.get_value(0).as_integer().unwrap(), 1);
        assert_eq!(result.0.get_value(1).as_integer().unwrap(), 200);

        // Department 2
        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.0.get_value(0).as_integer().unwrap(), 2);
        assert_eq!(result.0.get_value(1).as_integer().unwrap(), 350);

        let result = executor.next().unwrap().unwrap();
        assert_eq!(result.0.get_value(0).as_integer().unwrap(), 2);
        assert_eq!(result.0.get_value(1).as_integer().unwrap(), 250);

        // No more results
        let result = executor.next().unwrap();
        assert!(result.is_none());
    }
}
