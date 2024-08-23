use std::sync::Arc;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::common::rid::RID;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::execution_common::AbstractExecutor;
use crate::storage::table::tuple::Tuple;

pub struct ExecutorEngine {
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    transaction_manager: Arc<TransactionManager>
}

impl ExecutorEngine {
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>, catalog: Arc<Catalog>, transaction_manager: Arc<TransactionManager>) -> Self {
        Self {
            buffer_pool_manager,
            catalog,
            transaction_manager,
        }
    }

    pub fn execute(&self) -> bool {
        todo!()
    }

    pub fn perform_checks(&self) {
        todo!()
    }

    // fn poll_executor(&self, executor: AbstractExecutor, plan: AbstractPlanNodeRef, mut result_set: Vec<Tuple>) {
    //     let _rid: RID = Default::default();
    //     let _tuple: Tuple = ();
    //
    //     while executor.next(_tuple, _rid) {
    //         if !result_set.is_empty() {
    //             result_set.push(_tuple)
    //         }
    //     }
    // }
}
