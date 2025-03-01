use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::topn_plan::TopNNode;
use crate::storage::table::tuple::Tuple;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::types_db::types::{CmpBool, Type};
use log::{debug, trace};
use parking_lot::RwLock;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::cmp::Ordering;
use crate::types_db::value::Value;

/// Wrapper type for tuple and sort keys to implement custom ordering
#[derive(Eq)]
struct TupleWithKeys {
    sort_keys: Vec<Value>,
    tuple: Tuple,
    rid: RID,
}

impl PartialEq for TupleWithKeys {
    fn eq(&self, other: &Self) -> bool {
        self.sort_keys.len() == other.sort_keys.len() && 
        self.sort_keys.iter().zip(other.sort_keys.iter())
            .all(|(a, b)| a.compare_equals(b) == CmpBool::CmpTrue)
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
                        CmpBool::CmpFalse => continue,  // Equal, check next key
                        CmpBool::CmpNull => return Ordering::Equal,  // Treat NULL as equal
                    }
                }
                CmpBool::CmpNull => return Ordering::Equal,  // Treat NULL as equal
            }
        }
        Ordering::Equal
    }
}

/// TopNExecutor maintains a min-heap of size K to efficiently track the top K elements
pub struct TopNExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<TopNNode>,
    child: Box<dyn AbstractExecutor>,
    // Store sorted tuples for output
    sorted_tuples: Vec<TupleWithKeys>,
    current_index: usize,
    initialized: bool,
}

impl TopNExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<TopNNode>) -> Self {
        debug!("Creating TopNExecutor");
        
        let child_plan = plan.get_children().first().expect("TopN should have a child");
        let child = child_plan.create_executor(Arc::clone(&context))
            .expect("Failed to create child executor");

        Self {
            context,
            plan,
            child,
            sorted_tuples: Vec::new(),
            current_index: 0,
            initialized: false,
        }
    }

    /// Evaluates the sort keys for a tuple based on the order by expressions
    fn evaluate_sort_keys(&self, tuple: &Tuple) -> Vec<Value> {
        let schema = self.plan.get_output_schema();
        let keys = self.plan
            .get_sort_order_by()
            .iter()
            .map(|expr| expr.evaluate(tuple, schema).unwrap())
            .collect();
        trace!("Evaluated sort keys for tuple: {:?}", keys);
        keys
    }
}

impl AbstractExecutor for TopNExecutor {
    fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug!("Initializing TopNExecutor");
        self.child.init();
        
        let k = self.plan.get_k();
        debug!("TopN k value: {}", k);
        
        // Use min-heap to maintain top K elements
        let mut heap = BinaryHeap::new();
        
        // Process tuples one at a time
        while let Some((tuple, rid)) = self.child.next() {
            let sort_keys = self.evaluate_sort_keys(&tuple);
            debug!("Processing tuple with sort keys: {:?}", sort_keys);
            
            let tuple_with_keys = TupleWithKeys {
                sort_keys,
                tuple,
                rid,
            };
            
            if heap.len() < k {
                // Heap not full yet, add element
                debug!("Heap not full (size {}), adding tuple", heap.len());
                heap.push(Reverse(tuple_with_keys));
            } else if let Some(Reverse(current_min)) = heap.peek() {
                // Compare with smallest element in heap
                if current_min < &tuple_with_keys {
                    // Remove smallest and add new larger element
                    debug!("Replacing smallest element with larger tuple");
                    heap.pop();
                    heap.push(Reverse(tuple_with_keys));
                }
            }
        }

        // Convert heap to sorted vector (will be in ascending order)
        self.sorted_tuples = heap.into_iter().map(|Reverse(t)| t).collect();
        // Reverse to get descending order
        self.sorted_tuples.reverse();

        debug!("TopNExecutor initialization complete. Number of tuples: {}", self.sorted_tuples.len());
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        if self.current_index < self.sorted_tuples.len() {
            let result = &self.sorted_tuples[self.current_index];
            self.current_index += 1;
            debug!("Returning tuple {} with sort keys: {:?}", self.current_index - 1, result.sort_keys);
            Some((result.tuple.clone(), result.rid))
        } else {
            debug!("No more tuples to return");
            None
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
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use tempfile::TempDir;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use std::collections::HashMap;
    use crate::catalog::catalog::Catalog;
    use crate::sql::execution::expressions::abstract_expression::Expression;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 100;
            const K: usize = 2;

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

            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

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

    #[test]
    fn test_topn_executor() {
        let ctx = TestContext::new("test_topn_executor");

        // Create schema with a single integer column
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);

        // Create test tuples
        let test_tuples = vec![
            Tuple::new(&[Value::new(1), Value::new(10)], schema.clone(), RID::new(0, 0)),
            Tuple::new(&[Value::new(2), Value::new(20)], schema.clone(), RID::new(0, 1)),
            Tuple::new(&[Value::new(3), Value::new(30)], schema.clone(), RID::new(0, 2)),
            Tuple::new(&[Value::new(4), Value::new(40)], schema.clone(), RID::new(0, 3)),
            Tuple::new(&[Value::new(5), Value::new(50)], schema.clone(), RID::new(0, 4)),
        ];

        // Create catalog
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm.clone(),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            ctx.transaction_manager.clone(),
        )));

        // Create execution context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            catalog,
            ctx.transaction_context.clone(),
        )));

        // Create mock scan plan - modify this section
        let test_data: Vec<(Vec<Value>, RID)> = test_tuples.into_iter()
            .map(|tuple| (tuple.get_values().to_vec(), tuple.get_rid()))
            .collect();

        let mock_scan = Arc::new(
            MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],  // no children for mock scan
            )
            .with_tuples(test_data)
        );
        
        // Create order by expression (sort by value column)
        let order_by= Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,  // tuple index
            1,  // value column index
            Column::new("value", TypeId::Integer),  // return type
            vec![],  // children
        )));

        // Create TopN plan
        let topn_plan = Arc::new(TopNNode::new(
            schema,
            vec![order_by],
            3, // Get top 3
            vec![PlanNode::MockScan((*mock_scan).clone())],
        ));

        // Create and initialize TopN executor
        let mut executor = TopNExecutor::new(execution_context, topn_plan);
        executor.init();

        // Verify results (should get top 3 values in descending order)
        let result1 = executor.next().unwrap();
        let result2 = executor.next().unwrap();
        let result3 = executor.next().unwrap();
        let result4 = executor.next();

        assert_eq!(result1.0.get_value(1).as_integer().unwrap(), 50);
        assert_eq!(result2.0.get_value(1).as_integer().unwrap(), 40);
        assert_eq!(result3.0.get_value(1).as_integer().unwrap(), 30);
        assert!(result4.is_none());
    }
}