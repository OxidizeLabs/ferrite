use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::window_plan::{WindowFunctionType, WindowNode};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use log::debug;
use parking_lot::RwLock;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

/// Wrapper type for tuple and sort keys to implement custom ordering
#[derive(Eq)]
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
        // Compare sort keys in ascending order
        for (a, b) in self.sort_keys.iter().zip(other.sort_keys.iter()) {
            match a.compare_less_than(b) {
                CmpBool::CmpTrue => return Ordering::Less,
                CmpBool::CmpFalse => {
                    match a.compare_greater_than(b) {
                        CmpBool::CmpTrue => return Ordering::Greater,
                        CmpBool::CmpFalse => continue, // Equal, check next key
                        CmpBool::CmpNull => return Ordering::Equal,
                    }
                }
                CmpBool::CmpNull => return Ordering::Equal,
            }
        }
        Ordering::Equal
    }
}

pub struct WindowExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<WindowNode>,
    child_executor: Box<dyn AbstractExecutor>,
    // Store all tuples and their partitions
    partitions: HashMap<Vec<Value>, Vec<(Arc<Tuple>, RID)>>,
    // Track which partition and index we're currently returning from
    current_partition_keys: Vec<Vec<Value>>,
    partition_index: usize,
    tuple_index: usize,
    initialized: bool,
}

impl WindowExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<WindowNode>) -> Self {
        debug!("Creating WindowExecutor");
        let child_executor = plan.get_children()[0]
            .create_executor(context.clone())
            .expect("Failed to create child executor");

        Self {
            context,
            plan,
            child_executor,
            partitions: HashMap::new(),
            current_partition_keys: Vec::new(),
            partition_index: 0,
            tuple_index: 0,
            initialized: false,
        }
    }

    fn evaluate_sort_keys(&self, tuple: &Tuple) -> Vec<Value> {
        let schema = self.child_executor.get_output_schema();
        let window_fn = &self.plan.get_window_functions()[0];

        window_fn
            .get_order_by()
            .iter()
            .map(|expr| expr.evaluate(tuple, schema).unwrap())
            .collect()
    }

    fn evaluate_partition_keys(&self, tuple: &Tuple) -> Vec<Value> {
        let window_fns = self.plan.get_window_functions();
        if window_fns.is_empty() {
            return vec![];
        }

        let partition_exprs = window_fns[0].get_partition_by();
        let schema = self.child_executor.get_output_schema();

        partition_exprs
            .iter()
            .map(|expr| expr.evaluate(tuple, schema).unwrap())
            .collect()
    }
}

impl AbstractExecutor for WindowExecutor {
    fn init(&mut self) {
        if !self.initialized {
            self.child_executor.init();

            // Collect all tuples and their sort keys
            let mut all_tuples: Vec<(Vec<Value>, Vec<Value>, Arc<Tuple>, RID)> = Vec::new();
            loop {
                match self.child_executor.next() {
                    Ok(Some((tuple, rid))) => {
                        let partition_keys = self.evaluate_partition_keys(&tuple);
                        let sort_keys = self.evaluate_sort_keys(&tuple);
                        all_tuples.push((partition_keys, sort_keys, tuple, rid));
                    }
                    Ok(None) => break,
                    Err(e) => {
                        debug!("Error from child executor: {}", e);
                        return; // Initialize failed, but we don't propagate errors from init
                    }
                }
            }

            // Group tuples by partition
            let mut partitions_map: HashMap<Vec<Value>, Vec<(Vec<Value>, Arc<Tuple>, RID)>> =
                HashMap::new();
            for (partition_keys, sort_keys, tuple, rid) in all_tuples {
                partitions_map
                    .entry(partition_keys)
                    .or_insert_with(Vec::new)
                    .push((sort_keys, tuple, rid));
            }

            // Sort partition keys for deterministic order
            self.current_partition_keys = partitions_map.keys().cloned().collect();
            self.current_partition_keys.sort_by(|a, b| {
                for (a_val, b_val) in a.iter().zip(b.iter()) {
                    match a_val.compare_less_than(b_val) {
                        CmpBool::CmpTrue => return Ordering::Less,
                        CmpBool::CmpFalse => match a_val.compare_greater_than(b_val) {
                            CmpBool::CmpTrue => return Ordering::Greater,
                            CmpBool::CmpFalse => continue,
                            CmpBool::CmpNull => return Ordering::Equal,
                        },
                        CmpBool::CmpNull => return Ordering::Equal,
                    }
                }
                a.len().cmp(&b.len())
            });

            // Sort each partition using binary heap
            for (partition_key, tuples) in partitions_map {
                let mut heap = BinaryHeap::new();

                // Add tuples to heap
                for (sort_keys, tuple, rid) in tuples {
                    heap.push(Reverse(TupleWithKeys {
                        sort_keys,
                        tuple,
                        rid,
                    }));
                }

                // Extract sorted tuples
                let mut sorted_tuples = Vec::new();
                while let Some(Reverse(tuple_with_keys)) = heap.pop() {
                    sorted_tuples.push((tuple_with_keys.tuple, tuple_with_keys.rid));
                }

                self.partitions.insert(partition_key, sorted_tuples);
            }

            self.initialized = true;
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        // Check if we have more partitions to process
        if self.partition_index >= self.current_partition_keys.len() {
            return Ok(None);
        }

        // Get current partition
        let partition_key = &self.current_partition_keys[self.partition_index];
        let partition = self.partitions.get(partition_key).unwrap();

        // Check if we have more tuples in current partition
        if self.tuple_index >= partition.len() {
            // Move to next partition
            self.partition_index += 1;
            self.tuple_index = 0;
            return self.next(); // Recursively call to get next partition
        }

        // Get current tuple and compute window function results
        let (original_tuple, rid) = partition[self.tuple_index].clone();

        // Compute window function results for this tuple
        let window_functions = self.plan.get_window_functions();
        let mut window_results = Vec::new();

        for window_fn in window_functions {
            match window_fn.get_function_type() {
                WindowFunctionType::RowNumber => {
                    // Row number is 1-based within partition
                    let row_number = (self.tuple_index + 1) as i32;
                    window_results.push(Value::new(row_number));
                }
                _ => {
                    // For now, only support ROW_NUMBER
                    return Err(DBError::Execution(
                        "Unsupported window function".to_string(),
                    ));
                }
            }
        }

        // Create new tuple with original values plus window function results
        let mut output_values = original_tuple.get_values().clone();
        output_values.extend(window_results);

        let output_tuple = Arc::new(Tuple::new(
            &output_values,
            self.plan.get_output_schema(),
            rid,
        ));

        self.tuple_index += 1;

        Ok(Some((output_tuple, rid)))
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
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::sql::execution::plans::window_plan::{WindowFunction, WindowFunctionType};
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
    use crate::types_db::value::Val;
    use crate::types_db::value::Value;
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

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }
    }

    // Helper function to create test tuples
    fn create_test_tuples() -> Vec<Vec<Value>> {
        vec![
            // (dept, salary)
            vec![Value::new("IT"), Value::new(60000)],
            vec![Value::new("IT"), Value::new(70000)],
            vec![Value::new("HR"), Value::new(50000)],
            vec![Value::new("HR"), Value::new(55000)],
            vec![Value::new("IT"), Value::new(65000)],
        ]
    }

    // Helper function to convert Values to Expressions
    fn values_to_expressions(values: Vec<Vec<Value>>) -> Vec<Vec<Arc<Expression>>> {
        values
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| {
                        // Create a Column with the appropriate type from the value
                        let ret_type = Column::new(
                            "", // Empty name since this is a constant
                            value.get_type_id(),
                        );
                        Arc::new(Expression::Constant(ConstantExpression::new(
                            value,
                            ret_type,
                            vec![], // No children for constant expressions
                        )))
                    })
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_window_row_number() {
        // Create test context
        let ctx = TestContext::new("test_window_row_number").await;
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm(),                       // index_names
            ctx.transaction_manager.clone(), // txn_manager
        )));
        let execution_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context.clone(),
        )));

        // Create schema
        let input_schema = Schema::new(vec![
            Column::new("dept", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        let output_schema = Schema::new(vec![
            Column::new("dept", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
            Column::new("row_number", TypeId::Integer),
        ]);

        // Create test data and convert to expressions
        let tuples = create_test_tuples();
        let tuple_expressions = values_to_expressions(tuples);

        // Create values plan node as child
        let values_plan = Arc::new(PlanNode::Values(ValuesNode::new(
            input_schema.clone(),
            tuple_expressions,
            vec![], // No children for ValuesNode
        )));

        // Create window function
        let salary_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_fn = WindowFunction::new(
            WindowFunctionType::RowNumber,
            salary_expr.clone(),
            vec![],            // No PARTITION BY
            vec![salary_expr], // ORDER BY salary
        );

        // Create window plan node
        let window_plan = Arc::new(WindowNode::new(
            output_schema,
            vec![window_fn],
            vec![(*values_plan).clone()], // Dereference and clone the PlanNode
        ));

        // Create and initialize window executor
        let mut executor = WindowExecutor::new(execution_ctx, window_plan);
        executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Verify results
        assert_eq!(results.len(), 5);

        // Results should be ordered by salary with row numbers 1..5
        let row_numbers: Vec<i32> = results
            .iter()
            .map(|t| t.get_value(2).as_integer().unwrap())
            .collect();
        assert_eq!(row_numbers, vec![1, 2, 3, 4, 5]);

        // Verify salaries are in ascending order
        let salaries: Vec<i32> = results
            .iter()
            .map(|t| t.get_value(1).as_integer().unwrap())
            .collect();
        let mut sorted_salaries = salaries.clone();
        sorted_salaries.sort();
        assert_eq!(salaries, sorted_salaries);
    }

    #[tokio::test]
    async fn test_window_partitioned_row_number() {
        let ctx = TestContext::new("test_window_partitioned").await;
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm(),
            ctx.transaction_manager.clone(), // txn_manager
        )));
        let execution_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context.clone(),
        )));

        // Create schema
        let input_schema = Schema::new(vec![
            Column::new("dept", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        let output_schema = Schema::new(vec![
            Column::new("dept", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
            Column::new("dept_row_number", TypeId::Integer),
        ]);

        // Create test data and convert to expressions
        let tuples = create_test_tuples();
        let tuple_expressions = values_to_expressions(tuples);

        // Create values plan node as child
        let values_plan = Arc::new(PlanNode::Values(ValuesNode::new(
            input_schema.clone(),
            tuple_expressions,
            vec![], // No children for ValuesNode
        )));

        // Create window function with PARTITION BY dept
        let dept_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("dept", TypeId::VarChar),
            vec![],
        )));

        let salary_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_fn = WindowFunction::new(
            WindowFunctionType::RowNumber,
            salary_expr.clone(),
            vec![dept_expr],   // PARTITION BY dept
            vec![salary_expr], // ORDER BY salary
        );

        // Create window plan node
        let window_plan = Arc::new(WindowNode::new(
            output_schema,
            vec![window_fn],
            vec![(*values_plan).clone()], // Dereference and clone the PlanNode
        ));

        // Create and initialize window executor
        let mut executor = WindowExecutor::new(execution_ctx, window_plan);
        executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            results.push(tuple);
        }

        // Verify results
        assert_eq!(results.len(), 5);

        // Group results by department
        let mut hr_rows = Vec::new();
        let mut it_rows = Vec::new();

        for tuple in results {
            let dept = tuple.get_value(0);
            let dept_val = dept.get_val();
            let row_num = tuple.get_value(2).as_integer().unwrap();

            match dept_val {
                Val::VarLen(s) | Val::ConstLen(s) => match s.as_str() {
                    "HR" => hr_rows.push(row_num),
                    "IT" => it_rows.push(row_num),
                    _ => panic!("Unexpected department"),
                },
                _ => panic!("Expected string value for department"),
            }
        }

        // Verify row numbers within each partition
        assert_eq!(hr_rows, vec![1, 2]); // 2 HR employees
        assert_eq!(it_rows, vec![1, 2, 3]); // 3 IT employees
    }
}
