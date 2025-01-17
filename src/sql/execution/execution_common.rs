// use crate::catalog::schema::Schema;
// use crate::common::rid::RID;
// use crate::sql::execution::executor_context::ExecutorContext;
// use crate::storage::table::tuple::Tuple;
// use std::sync::Arc;
//
// pub trait AbstractExecutor {
//     fn new(&self) -> Box<Self> {
//         todo!()
//     }
//
//     fn init(&self) {
//         todo!()
//     }
//
//     fn next(&self, tuple: Tuple, rid: RID) -> bool {
//         todo!()
//     }
//
//     fn get_output_schema(&self) -> Schema {
//         todo!()
//     }
//
//     fn get_executor_context(&self) -> Arc<ExecutionContext> {
//         todo!()
//     }
// }
