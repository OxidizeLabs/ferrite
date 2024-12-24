use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct MockExecutor {
    tuples: Vec<(Vec<Value>, RID)>,
    current_index: usize,
    schema: Schema,
    context: Arc<RwLock<ExecutorContext>>,
}

impl MockExecutor {
    pub fn new(
        tuples: Vec<(Vec<Value>, RID)>,
        schema: Schema,
        context: Arc<RwLock<ExecutorContext>>,
    ) -> Self {
        Self {
            tuples,
            current_index: 0,
            schema,
            context,
        }
    }
}

impl AbstractExecutor for MockExecutor {
    fn init(&mut self) {
        self.current_index = 0;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.current_index < self.tuples.len() {
            let (values, rid) = &self.tuples[self.current_index];
            self.current_index += 1;
            let tuple = Tuple::new(&*values.to_vec(), self.schema.clone(), *rid);
            Some((tuple, *rid))
        } else {
            None
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.schema.clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}
