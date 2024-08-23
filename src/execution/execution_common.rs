use std::sync::Arc;
use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::storage::table::tuple::Tuple;


pub trait AbstractExecutor {
    fn new() -> Box<dyn AbstractExecutor> {
        todo!()
    }

    fn init() {
        todo!()
    }

    fn next(&self, tuple: Tuple, rid: RID) -> bool {
        todo!()
    }

    fn get_output_schema() -> Schema {
        todo!()
    }

    fn get_executor_context(&self) -> Arc<ExecutorContext> {
        Arc::clone(&self._exec_ctx)
    }
}