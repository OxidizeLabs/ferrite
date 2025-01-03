use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use parking_lot::RwLock;
use std::sync::Arc;
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::execution::plans::table_scan_plan::TableScanNode;

pub struct MockExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<MockScanNode>,
    current_index: usize,
    tuples: Vec<(Vec<Value>, RID)>,
    schema: Schema,
}

impl MockExecutor {
    pub fn new(context: Arc<RwLock<ExecutorContext>>, plan: Arc<MockScanNode>,     current_index: usize, tuples: Vec<(Vec<Value>, RID)>, schema: Schema) -> Self {
        Self {
            context,
            plan,
            current_index,
            tuples,
            schema,
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
