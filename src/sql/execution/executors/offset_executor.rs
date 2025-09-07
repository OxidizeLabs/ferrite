use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::offset_plan::OffsetNode;
use crate::storage::table::tuple::Tuple;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct OffsetExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<OffsetNode>,
    current_index: usize,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl OffsetExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<OffsetNode>,
    ) -> Self {
        debug!("Creating OffsetExecutor");

        Self {
            context,
            plan,
            current_index: 0,
            initialized: false,
            child_executor: Some(child_executor),
        }
    }
}

impl AbstractExecutor for OffsetExecutor {
    fn init(&mut self) {
        if !self.initialized {
            // Initialize child executor
            if let Some(child) = &mut self.child_executor {
                child.init();
            }
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        // Skip the first `offset` tuples
        while self.current_index < self.plan.get_offset() {
            if let Some(child) = &mut self.child_executor {
                match child.next()? {
                    Some(_) => {
                        self.current_index += 1;
                    }
                    None => return Ok(None), // No more tuples to skip
                }
            } else {
                return Ok(None);
            }
        }

        // After skipping, return all remaining tuples
        if let Some(child) = &mut self.child_executor {
            child.next()
        } else {
            Ok(None)
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
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::execution_context::ExecutionContext;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use crate::common::rid::RID;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::catalog::catalog::Catalog;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::concurrency::transaction::Transaction;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::concurrency::transaction::IsolationLevel;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::buffer::lru_k_replacer::LRUKReplacer;

    async fn create_test_context() -> (
        Arc<BufferPoolManager>,
        Arc<RwLock<Catalog>>,
        Arc<TransactionContext>,
        TempDir,
    ) {
        const BUFFER_POOL_SIZE: usize = 10;
        const K: usize = 2;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("test.db")
            .to_str()
            .unwrap()
            .to_string();
        let log_path = temp_dir
            .path()
            .join("test.log")
            .to_str()
            .unwrap()
            .to_string();

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(
            BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc, replacer).unwrap(),
        );

        let transaction_manager = Arc::new(TransactionManager::new());
        let lock_manager = Arc::new(LockManager::new());
        let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
        let transaction_context = Arc::new(TransactionContext::new(
            transaction,
            lock_manager,
            transaction_manager.clone(),
        ));

        let catalog = Arc::new(RwLock::new(Catalog::new(
            bpm.clone(),
            transaction_manager.clone(),
        )));

        (bpm, catalog, transaction_context, temp_dir)
    }

    #[tokio::test]
    async fn test_offset_executor() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
            Arc::new(Tuple::new(
                &[Value::new(2), Value::new("Bob")],
                &schema,
                RID::new(0, 1),
            )),
            Arc::new(Tuple::new(
                &[Value::new(3), Value::new("Charlie")],
                &schema,
                RID::new(0, 2),
            )),
            Arc::new(Tuple::new(
                &[Value::new(4), Value::new("David")],
                &schema,
                RID::new(0, 3),
            )),
            Arc::new(Tuple::new(
                &[Value::new(5), Value::new("Eve")],
                &schema,
                RID::new(0, 4),
            )),
        ];

        // Convert Vec<Arc<Tuple>> to Vec<(Vec<Value>, RID)> for MockExecutor
        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(2, schema.clone(), vec![]));
        
        // Create mock executor as child
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),  // Provide table name instead of tuples
                vec![],
            )),
            0,
            mock_tuples,  // Use converted tuples
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should skip first 2 tuples and return the remaining 3
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 3);
        // The returned tuples should be Charlie, David, and Eve (ids 3, 4, 5)
        assert_eq!(results[0].get_value(0).to_string(), "3");
        assert_eq!(results[1].get_value(0).to_string(), "4");
        assert_eq!(results[2].get_value(0).to_string(), "5");
    }

    #[tokio::test]
    async fn test_offset_executor_zero_offset() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
            Arc::new(Tuple::new(
                &[Value::new(2), Value::new("Bob")],
                &schema,
                RID::new(0, 1),
            )),
            Arc::new(Tuple::new(
                &[Value::new(3), Value::new("Charlie")],
                &schema,
                RID::new(0, 2),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(0, schema.clone(), vec![]));
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return all tuples (no offset)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get_value(0).to_string(), "1");
        assert_eq!(results[1].get_value(0).to_string(), "2");
        assert_eq!(results[2].get_value(0).to_string(), "3");
    }

    #[tokio::test]
    async fn test_offset_executor_offset_greater_than_tuples() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = vec![
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
            Arc::new(Tuple::new(
                &[Value::new(2), Value::new("Bob")],
                &schema,
                RID::new(0, 1),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(5, schema.clone(), vec![])); // Offset > tuple count
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return no tuples (offset > available tuples)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_offset_executor_offset_equals_tuple_count() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
            Arc::new(Tuple::new(
                &[Value::new(2), Value::new("Bob")],
                &schema,
                RID::new(0, 1),
            )),
            Arc::new(Tuple::new(
                &[Value::new(3), Value::new("Charlie")],
                &schema,
                RID::new(0, 2),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(3, schema.clone(), vec![])); // Offset = tuple count
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return no tuples (offset = available tuples)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_offset_executor_empty_input() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let mock_tuples: Vec<(Vec<Value>, RID)> = vec![]; // Empty input

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(2, schema.clone(), vec![]));
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return no tuples (empty input)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_offset_executor_offset_one() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
            Arc::new(Tuple::new(
                &[Value::new(2), Value::new("Bob")],
                &schema,
                RID::new(0, 1),
            )),
            Arc::new(Tuple::new(
                &[Value::new(3), Value::new("Charlie")],
                &schema,
                RID::new(0, 2),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(1, schema.clone(), vec![]));
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should skip first tuple and return the remaining 2
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_value(0).to_string(), "2");
        assert_eq!(results[1].get_value(0).to_string(), "3");
    }

    #[tokio::test]
    async fn test_offset_executor_single_tuple_large_offset() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(1), Value::new("Alice")],
                &schema,
                RID::new(0, 0),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(100, schema.clone(), vec![])); // Large offset
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return no tuples (large offset with single tuple)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_offset_executor_single_tuple_no_offset() {
        let (bpm, catalog, transaction_context, _temp_dir) = create_test_context().await;

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let tuples = [
            Arc::new(Tuple::new(
                &[Value::new(42), Value::new("OnlyOne")],
                &schema,
                RID::new(0, 0),
            )),
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = tuples
            .iter()
            .map(|tuple| {
                let values = (0..tuple.get_column_count())
                    .map(|i| tuple.get_value(i).clone())
                    .collect();
                (values, tuple.get_rid())
            })
            .collect();

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));
        let offset_node = Arc::new(OffsetNode::new(0, schema.clone(), vec![]));
        
        let mock_executor = MockExecutor::new(
            context.clone(),
            Arc::new(crate::sql::execution::plans::mock_scan_plan::MockScanNode::new(
                schema.clone(),
                "test_table".to_string(),
                vec![],
            )),
            0,
            mock_tuples,
            schema.clone(),
        );

        let mut offset_executor = OffsetExecutor::new(
            Box::new(mock_executor),
            context,
            offset_node,
        );

        offset_executor.init();

        // Should return the single tuple (no offset)
        let mut results = Vec::new();
        while let Ok(Some((tuple, _))) = offset_executor.next() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_value(0).to_string(), "42");
        assert_eq!(results[0].get_value(1).to_string(), "OnlyOne");
    }
} 