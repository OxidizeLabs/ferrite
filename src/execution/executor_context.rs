// use crate::buffer::buffer_pool_manager::BufferPoolManager;
// use crate::catalogue::catalogue::Catalog;
// use crate::concurrency::lock_manager::LockManager;
// use crate::concurrency::transaction::Transaction;
// use crate::execution::check_option::CheckOptions;
// use std::fmt::Pointer;
// use std::sync::Arc;
//
// pub struct ExecutorContext {
//     transaction: Transaction,
//     catalog: Catalog,
//     buffer_pool_manager: Arc<BufferPoolManager>,
//     // transaction_manager: Arc<TransactionManager>,
//     lock_manager: LockManager,
//     // _nlj_check_exec_set: VecDeque<(dyn AbstractExecutor, dyn AbstractExecutor)>,
//     _check_options: Arc<CheckOptions>,
//     _is_delete: bool,
// }
//
// impl ExecutorContext {
//     pub fn new(transaction: Transaction,
//                // transaction_manager: Arc<TransactionManager>,
//                catalog: Catalog,
//                buffer_pool_manager: Arc<BufferPoolManager>,
//                lock_manager: LockManager,
//     ) -> Self {
//         Self {
//             transaction,
//             catalog,
//             buffer_pool_manager,
//             // transaction_manager,
//             lock_manager,
//             // _nlj_check_exec_set: Default::default(),
//             _check_options: Arc::new(CheckOptions::default()),
//             _is_delete: false,
//         }
//     }
//
//     pub fn get_transaction(&self) -> &Transaction {
//         &self.transaction
//     }
//
//     pub fn get_catalogue(&self) -> &Catalog {
//         &self.catalog
//     }
//
//     pub fn get_buffer_pool_manager(&self) -> &BufferPoolManager {
//         &self.buffer_pool_manager
//     }
//
//     // pub fn get_transaction_manager(&self) -> &TransactionManager {
//     //     &self.transaction_manager
//     // }
//
//     pub fn get_lock_manager(&self) -> &LockManager {
//         &self.lock_manager
//     }
//
//     // pub fn get_nlj_check_exec_set(&self) -> &VecDeque<(dyn AbstractExecutor, dyn AbstractExecutor)> {
//     //     &self._nlj_check_exec_set
//     // }
//
//     pub fn get_check_options(&self) -> Arc<CheckOptions> {
//         Arc::clone(&self._check_options)
//     }
//
//     // pub fn add_check_option(&mut self, _left_exec: Box<dyn AbstractExecutor>, _right_exec: Box<dyn AbstractExecutor>) {
//     //     self._nlj_check_exec_set.push_back((_left_exec, _right_exec))
//     // }
//
//     pub fn init_check_options(&self) {
//         todo!()
//     }
//
//     pub fn is_delete(&self) -> bool {
//         self._is_delete
//     }
// }
//
