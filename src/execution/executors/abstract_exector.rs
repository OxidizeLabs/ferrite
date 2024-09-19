// use std::sync::Arc;
// use crate::catalogue::schema::Schema;
// use crate::common::rid::RID;
// use crate::execution::executor_context::ExecutorContext;
// use crate::storage::table::tuple::Tuple;
//
// /// The AbstractExecutor implements the Volcano tuple-at-a-time iterator model.
// /// This is the base trait from which all executors in the BusTub execution
// /// engine inherit, and defines the minimal interface that all executors support.
// pub trait AbstractExecutor {
//     /// Initialize the executor.
//     ///
//     /// # Warning
//     ///
//     /// This function must be called before `next()` is called!
//     fn init(&mut self);
//
//     /// Yield the next tuple from this executor.
//     ///
//     /// # Returns
//     ///
//     /// Returns `Some((Tuple, RID))` if a tuple was produced, `None` if there are no more tuples.
//     fn next(&mut self) -> Option<(Tuple, RID)>;
//
//     /// Get the schema of the tuples that this executor produces.
//     fn get_output_schema(&self) -> &Schema;
//
//     /// Get the executor context in which this executor runs.
//     fn get_executor_context(&self) -> &ExecutorContext;
// }
//
// /// A base struct for concrete executors to inherit from.
// pub struct BaseExecutor {
//     exec_ctx: Arc<ExecutorContext>,
// }
//
// impl BaseExecutor {
//     /// Construct a new BaseExecutor instance.
//     ///
//     /// # Arguments
//     ///
//     /// * `exec_ctx` - The executor context that the executor runs with.
//     pub fn new(exec_ctx: Arc<ExecutorContext>) -> Self {
//         Self { exec_ctx }
//     }
//
//     /// Get the executor context in which this executor runs.
//     pub fn get_executor_context(&self) -> &ExecutorContext {
//         &self.exec_ctx
//     }
// }
//
// #[cfg(tests)]
// mod tests {
//     use super::*;
//
//     struct MockExecutor {
//         base: BaseExecutor,
//         output_schema: Schema,
//         data: Vec<(Tuple, RID)>,
//         index: usize,
//     }
//
//     impl MockExecutor {
//         fn new(exec_ctx: Arc<ExecutorContext>) -> Self {
//             Self {
//                 base: BaseExecutor::new(exec_ctx),
//                 output_schema: Schema::new(vec![]),
//                 data: vec![],
//                 index: 0,
//             }
//         }
//     }
//
//     impl AbstractExecutor for MockExecutor {
//         fn init(&mut self) {
//             self.index = 0;
//         }
//
//         fn next(&mut self) -> Option<(Tuple, RID)> {
//             if self.index < self.data.len() {
//                 let result = self.data[self.index].clone();
//                 self.index += 1;
//                 Some(result)
//             } else {
//                 None
//             }
//         }
//
//         fn get_output_schema(&self) -> &Schema {
//             &self.output_schema
//         }
//
//         fn get_executor_context(&self) -> &ExecutorContext {
//             self.base.get_executor_context()
//         }
//     }
//
//     // #[tests]
//     // fn test_mock_executor() {
//     //     let transaction = Arc::new(Transaction::default());
//     //     let transaction_manager = Arc::new(TransactionManager::default());
//     //     let catalog = Arc::new(Catalog::default());
//     //     let buffer_pool_manager = Arc::new(BufferPoolManager::default());
//     //     let lock_manager = Arc::new(LockManager::default());
//     //
//     //     let exec_ctx = Arc::new(ExecutorContext::new(
//     //         transaction,
//     //         transaction_manager,
//     //         catalog,
//     //         buffer_pool_manager,
//     //         lock_manager,
//     //     ));
//     //
//     //     let mut executor = MockExecutor::new(exec_ctx.clone());
//     //
//     //     executor.init();
//     //     assert_eq!(executor.next(), None);
//     //
//     //     executor.data.push((Tuple::default(), RID::default()));
//     //     executor.data.push((Tuple::default(), RID::default()));
//     //
//     //     executor.init();
//     //     assert!(executor.next().is_some());
//     //     assert!(executor.next().is_some());
//     //     assert_eq!(executor.next(), None);
//     //
//     //     assert_eq!(executor.get_output_schema().column_count(), 0);
//     //     assert!(Arc::ptr_eq(&executor.get_executor_context(), &exec_ctx));
//     // }
// }