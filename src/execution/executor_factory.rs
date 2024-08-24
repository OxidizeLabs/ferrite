use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;

pub struct ExecutorFactory {}

impl ExecutorFactory {
    pub fn create_executor(_exec_ctx: ExecutorContext) -> AbstractExecutor {
        todo!()
    }
}
