use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::table_scan_plan::TableScanNode;
use crate::storage::table::table_iterator::TableScanIterator;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

/// TableScanExecutor implements a simple sequential scan over a table
/// using the Volcano-style iterator model.
pub struct TableScanExecutor {
    /// The executor context
    context: Arc<RwLock<ExecutorContext>>,
    /// The table scan plan node
    plan: Arc<TableScanNode>,
    /// Flag indicating if the executor has been initialized
    initialized: bool,
    /// The iterator over the table's tuples
    iterator: Option<TableScanIterator>,
}

impl TableScanExecutor {
    /// Create a new table scan executor
    pub fn new(context: Arc<RwLock<ExecutorContext>>, plan: Arc<TableScanNode>) -> Self {
        Self {
            context,
            plan,
            iterator: None,
            initialized: false,
        }
    }
}

impl AbstractExecutor for TableScanExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing TableScanExecutor for table: {}",
            self.plan.get_table_name()
        );

        // Create a new table scan iterator from the plan
        self.iterator = Some(self.plan.scan());
        self.initialized = true;

        debug!("TableScanExecutor initialized successfully");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        // Initialize if not already done
        if !self.initialized {
            self.init();
        }

        // Get the next tuple from the iterator
        match &mut self.iterator {
            Some(iter) => {
                match iter.next() {
                    Some((meta, tuple)) => {
                        // Skip deleted tuples
                        if meta.is_deleted() {
                            return self.next();
                        }
                        debug!("Found tuple with RID: {:?}", tuple.get_rid());
                        Some((tuple.clone(), tuple.get_rid()))
                    }
                    None => {
                        debug!("No more tuples to scan");
                        None
                    }
                }
            }
            None => {
                error!("Iterator not initialized");
                None
            }
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::catalogue::catalogue::{Catalog, TableInfo};
//     use crate::catalogue::column::Column;
//     use crate::concurrency::transaction::{IsolationLevel, Transaction};
//     use crate::storage::table::tuple::{Tuple, TupleMeta};
//     use crate::types_db::type_id::TypeId;
//     use crate::types_db::value::Value;
//     use std::collections::HashMap;
//     use std::fs;
//     use chrono::Utc;
//     use parking_lot::{Mutex, RwLock};
//     use crate::buffer::buffer_pool_manager::BufferPoolManager;
//     use crate::buffer::lru_k_replacer::LRUKReplacer;
//     use crate::common::logger::initialize_logger;
//     use crate::concurrency::lock_manager::LockManager;
//     use crate::concurrency::transaction_manager::TransactionManager;
//     use crate::storage::disk::disk_manager::FileDiskManager;
//     use crate::storage::disk::disk_scheduler::DiskScheduler;
//
//     struct TestContext {
//         bpm: Arc<BufferPoolManager>,
//         transaction_manager: Arc<Mutex<TransactionManager>>,
//         lock_manager: Arc<LockManager>,
//         db_file: String,
//         db_log_file: String,
//     }
//
//     impl TestContext {
//         fn new(test_name: &str) -> Self {
//             initialize_logger();
//             const BUFFER_POOL_SIZE: usize = 5;
//             const K: usize = 2;
//
//             let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
//             let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
//             let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
//
//             let disk_manager = Arc::new(FileDiskManager::new(
//                 db_file.clone(),
//                 db_log_file.clone(),
//                 100,
//             ));
//             let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
//             let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
//             let bpm = Arc::new(BufferPoolManager::new(
//                 BUFFER_POOL_SIZE,
//                 disk_scheduler,
//                 disk_manager,
//                 replacer,
//             ));
//
//             let catalog = Arc::new(RwLock::new(Catalog::new(
//                 bpm.clone(),
//                 0,
//                 0,
//                 Default::default(),
//                 Default::default(),
//                 Default::default(),
//                 Default::default(),
//             )));
//
//             let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(catalog)));
//             let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));
//
//             Self {
//                 bpm,
//                 transaction_manager,
//                 lock_manager,
//                 db_file,
//                 db_log_file,
//             }
//         }
//
//         fn cleanup(&self) {
//             let _ = fs::remove_file(&self.db_file);
//             let _ = fs::remove_file(&self.db_log_file);
//         }
//     }
//
//     impl Drop for TestContext {
//         fn drop(&mut self) {
//             self.cleanup();
//         }
//     }
//
//     fn create_test_tuple(schema: &Schema, id: i32, name: &str, age: i32) -> (TupleMeta, Tuple) {
//         let values = vec![
//             Value::new(id),
//             Value::new(name.to_string()),
//             Value::new(age),
//         ];
//         let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
//         let meta = TupleMeta::new(0, false);
//         (meta, tuple)
//     }
//
//     #[test]
//     fn test_table_scan_executor() {
//         let ctx = TestContext::new("test_table_scan_executor");
//         let bpm = ctx.bpm.clone();
//
//         // Create schema
//         let schema = Arc::new(Schema::new(vec![
//             Column::new("id", TypeId::Integer),
//             Column::new("name", TypeId::VarChar),
//             Column::new("age", TypeId::Integer),
//         ]));
//
//         // Create catalog and table
//         let mut catalog = Catalog::new(
//             bpm.clone(),
//             0,
//             0,
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//         );
//
//         let txn = Arc::new(Transaction::new(0, IsolationLevel::Serializable));
//         let table_name = "test_table";
//         let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();
//
//         // Insert test data
//         let table_heap = table_info.get_table_heap();
//         let test_data = vec![
//             (1, "Alice", 25),
//             (2, "Bob", 30),
//             (3, "Charlie", 35),
//         ];
//
//         for (id, name, age) in test_data.iter() {
//             let (meta, mut tuple) = create_test_tuple(&schema, *id, name, *age);
//             table_heap.insert_tuple(&meta, &mut tuple).expect("Failed to insert tuple");
//         }
//
//         // Create scan plan and executor
//         let plan = Arc::new(TableScanNode::new(
//             Arc::new(table_info),  // Wrap in Arc
//             schema.clone(),
//             None,
//         ));
//
//
//         let context = Arc::new(ExecutorContext::new(
//             txn,
//             ctx.transaction_manager,
//             Arc::new(RwLock::new(catalog)),
//             bpm,
//             ctx.lock_manager,
//         ));
//
//         let mut executor = TableScanExecutor::new(context, plan);
//
//         // Test scanning
//         executor.init();
//         let mut count = 0;
//         while let Some(_) = executor.next() {
//             count += 1;
//         }
//
//         assert_eq!(count, 3, "Should have scanned exactly 3 tuples");
//     }
//
//     #[test]
//     fn test_table_scan_executor_empty() {
//         let ctx = TestContext::new("test_table_scan_executor_empty");
//         let bpm = ctx.bpm.clone();
//
//         // Create schema and empty table
//         let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
//         let mut catalog = Catalog::new(
//             bpm.clone(),
//             0,
//             0,
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//         );
//
//         let txn = Arc::new(Transaction::new(0, IsolationLevel::Serializable));
//         let table_name = "empty_table";
//         let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();
//
//         // Create executor
//         let plan = Arc::new(TableScanNode::new(
//             table_info,  // table_info is already an Arc<TableInfo>
//             schema.clone(),
//             None,
//         ));
//
//         let context = Arc::new(ExecutorContext::new(
//             txn,
//             ctx.transaction_manager,
//             Arc::new(RwLock::new(catalog)),
//             bpm,
//             ctx.lock_manager,
//         ));
//
//         let mut executor = TableScanExecutor::new(context, plan);
//         executor.init();
//
//         assert!(executor.next().is_none(), "Empty table should return no tuples");
//     }
// }
