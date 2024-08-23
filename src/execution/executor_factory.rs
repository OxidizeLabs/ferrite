use std::sync::Arc;
use crate::execution::execution_common::AbstractExecutor;
use crate::execution::executor_context::ExecutorContext;

pub struct ExecutorFactory {}

impl ExecutorFactory {
    pub fn create_executor(_exec_ctx: ExecutorContext) -> Arc<dyn AbstractExecutor> {
        todo!()
    }
}
