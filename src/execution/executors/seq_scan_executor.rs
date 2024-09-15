// use crate::catalogue::schema::Schema;
// use crate::common::rid::RID;
// use crate::execution::executor_context::ExecutorContext;
// use crate::execution::executors::abstract_exector::{AbstractExecutor, BaseExecutor};
// use crate::execution::plans::abstract_plan::AbstractPlanNode;
// use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
// use crate::storage::table::table_heap::TableHeap;
// use crate::storage::table::table_iterator::TableIterator;
// use crate::storage::table::tuple::Tuple;
// use std::sync::Arc;
// use parking_lot::Mutex;
//
// /// The SeqScanExecutor executor executes a sequential table scan.
// pub struct SeqScanExecutor {
//     base: BaseExecutor,
//     plan: Arc<SeqScanPlanNode>,
//     table_heap: Option<TableHeap>,
//     iter: Option<TableIterator>,
// }
//
// impl SeqScanExecutor {
//     /// Construct a new SeqScanExecutor instance.
//     ///
//     /// # Arguments
//     ///
//     /// * `exec_ctx` - The executor context
//     /// * `plan` - The sequential scan plan to be executed
//     pub fn new(exec_ctx: Arc<ExecutorContext>, plan: Arc<SeqScanPlanNode>) -> Self {
//         Self {
//             base: BaseExecutor::new(exec_ctx),
//             plan,
//             table_heap: None,
//             iter: None,
//         }
//     }
// }
//
// impl AbstractExecutor for SeqScanExecutor {
//     fn init(&mut self) {
//         let table_oid = self.plan.get_table_oid().to_string();
//         let catalog = self.base.get_executor_context().get_catalogue();
//         let table_info = catalog.get_table(table_oid.as_str()).expect("Table not found");
//         self.table_heap = Some(*table_info.get_table_heap());
//         self.iter = None
//     }
//
//     fn next(&mut self) -> Option<(Tuple, RID)> {
//         unimplemented!()
//         // self.iter.as_mut().and_then(|iter| iter.next())
//     }
//
//     fn get_output_schema(&self) -> &Schema {
//         self.plan.get_output_schema()
//     }
//
//     fn get_executor_context(&self) -> &ExecutorContext {
//         self.base.get_executor_context()
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::buffer::buffer_pool_manager::BufferPoolManager;
//     use crate::buffer::lru_k_replacer::LRUKReplacer;
//     use crate::catalogue::catalogue::Catalog;
//     use crate::catalogue::column::Column;
//     use crate::common::config::TableOidT;
//     use crate::concurrency::lock_manager::LockManager;
//     use crate::concurrency::transaction::{IsolationLevel, Transaction};
//     use crate::concurrency::transaction_manager::TransactionManager;
//     use crate::recovery::log_manager::LogManager;
//     use crate::storage::disk::disk_manager::FileDiskManager;
//     use crate::storage::disk::disk_scheduler::DiskScheduler;
//     use crate::types_db::type_id::TypeId;
//     use crate::types_db::value::Value;
//     use parking_lot::{Mutex, RwLock};
//     use crate::storage::table::tuple::TupleMeta;
//
//     fn create_test_table(catalog: &mut Catalog) -> (TableOidT, TableHeap) {
//         let columns = vec![
//             Column::new("id", TypeId::Integer),
//             Column::new("name", TypeId::VarChar),
//         ];
//         let schema = Schema::new(columns);
//         let transaction = Transaction::new(0, IsolationLevel::Serializable);
//         let table_info = catalog.create_table(&transaction, "test_table", schema, true).unwrap();
//         let table_oid = table_info.get_table_oidt();
//         let table_heap = table_info.get_table_heap();
//
//         let rid1 = RID::new(1, 1);
//         let rid2 = RID::new(1, 2);
//         let rid3 = RID::new(1, 3);
//
//         // Insert some test data
//         let tuples = vec![
//             Tuple::new(vec![Value::from(1), Value::from("Alice")], schema, rid1),
//             Tuple::new(vec![Value::from(2), Value::from("Bob")], schema.clone(), rid2),
//             Tuple::new(vec![Value::from(3), Value::from("Charlie")], schema, rid3),
//         ];
//         let tuple_meta = TupleMeta::new(0, false);
//
//         for mut tuple in tuples {
//             table_heap.insert_tuple(&tuple_meta, &mut tuple, None, None, 0).expect("Failed to insert tuple");
//         }
//         (table_oid, *table_heap)
//     }
//
//     #[test]
//     fn test_seq_scan_executor() {
//         let transaction = Arc::new(Mutex::new(Transaction::new(0, IsolationLevel::Serializable)));
//         let transaction_manager = TransactionManager::new();
//         let disk_manager = Arc::new(FileDiskManager::new("seq_scan_executor.db".to_string(), "seq_scan_executor.log".to_string(), 100));
//         let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
//         let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, 2)));
//         let log_manager = Arc::new(Mutex::new(LogManager::new(disk_manager.clone())));
//         let lock_manager = Arc::new(Mutex::new(LockManager::new(Arc::new(Mutex::new(transaction_manager)))));
//         let bpm = Arc::new(BufferPoolManager::new(4, disk_scheduler, disk_manager, replacer));
//         let mut catalog = Catalog::new(bpm.clone(), lock_manager.clone(), log_manager, 0, 0, Default::default(), Default::default(), Default::default(), Default::default());
//
//         let (table_oid, table_heap) = create_test_table(&mut catalog);
//
//         let exec_ctx = Arc::new(ExecutorContext::new(
//             transaction,
//             Arc::new(Mutex::new(catalog)),
//             bpm,
//             lock_manager,
//         ));
//
//         let plan = Arc::new(SeqScanPlanNode::new(
//             0,
//             table_oid,
//             None,
//             table_heap.get_schema().clone(),
//             None,
//         ));
//
//         let mut executor = SeqScanExecutor::new(exec_ctx, plan);
//
//         executor.init();
//
//         let mut result_count = 0;
//         while let Some((tuple, _)) = executor.next() {
//             result_count += 1;
//             assert!(tuple.get_value(executor.get_output_schema(), 0).is_integer());
//             assert!(tuple.get_value(executor.get_output_schema(), 1).is_varchar());
//         }
//
//         assert_eq!(result_count, 3);
//     }
// }