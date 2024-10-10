use std::sync::Arc;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::common::rid::RID;
use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::AbstractPlanNode;

pub struct SeqScanExecutor {
    context: ExecutorContext,
    plan: SeqScanPlanNode,
    table_heap: Arc<TableHeap>,
    initialized: bool,
}

impl SeqScanExecutor {
    pub fn new(context: ExecutorContext, plan: SeqScanPlanNode) -> Self {
        let table_oid = plan.get_table_oid();
        let catalog = context.get_catalogue();
        let table_info = catalog.get_table(table_oid.to_string().as_str()).expect("Table not found");
        let table_heap = table_info.get_table_heap();

        Self {
            context,
            plan,
            table_heap,
            initialized: false,
        }
    }

    fn apply_predicate(&self, tuple: &Tuple) -> bool {
        if let Some(predicate) = self.plan.get_filter_predicate() {
            // Evaluate the predicate on the tuple
            // This is a placeholder and should be replaced with actual predicate evaluation
            true
        } else {
            true
        }
    }
}

impl AbstractExecutor for SeqScanExecutor {
    fn init(&mut self) {
        self.initialized = true;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        let mut iter = self.table_heap.make_iterator();

        while let Some((tuple_meta, tuple)) = iter.next() {
            if self.apply_predicate(&tuple) {
                return Some((tuple.clone(), tuple.get_rid()));
            }
        }

        None
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::types_db::type_id::TypeId;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::types_db::value::Value;
    use crate::concurrency::transaction::{Transaction, IsolationLevel};
    use crate::catalogue::catalogue::Catalog;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::recovery::log_manager::LogManager;
    use std::sync::Arc;
    use parking_lot::{RwLock, Mutex};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn test_seq_scan_executor() {
        let schema = create_test_schema();
        let table_oid = 1;
        let table_name = "test_table".to_string();
        let filter_predicate = Some(Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("test_column", TypeId::Integer), vec![]))));

        let plan = SeqScanPlanNode::new(
            schema,
            table_oid,
            table_name,
            filter_predicate,
        );

        // Create mock objects for BufferPoolManager
        let disk_manager = Arc::new(FileDiskManager::new("test_db.db".to_string(), "test_log.log".to_string(), 100));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));

        // Create BufferPoolManager
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(
            10,  // pool_size
            disk_scheduler,
            disk_manager.clone(),
            replacer,
        ));

        // Create TransactionManager and LockManager
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new()));
        let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

        // Create mock objects for ExecutorContext
        let transaction = Transaction::new(0, IsolationLevel::Serializable);
        let log_manager = Arc::new(parking_lot::Mutex::new(LogManager::new(disk_manager)));

        let catalog = Catalog::new(
            buffer_pool_manager.clone(),
            lock_manager.clone(),
            log_manager,
            0, 0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default()
        );

        // Create ExecutorContext with mock objects
        let context = ExecutorContext::new(
            transaction,
            catalog,
            buffer_pool_manager,
            lock_manager,
        );

        let mut executor = SeqScanExecutor::new(context, plan);

        executor.init();

        // Test the executor
        let mut result_count = 0;
        while let Some((tuple, _)) = executor.next() {
            // Add assertions to check the tuple contents
            result_count += 1;
        }

        // Add assertions to check the result_count or other expected behaviors
        assert!(result_count > 0, "Expected at least one tuple from the scan");
    }
}