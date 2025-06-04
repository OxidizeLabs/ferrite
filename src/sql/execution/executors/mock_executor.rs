use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::common::exception::DBError;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct MockExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<MockScanNode>,
    current_index: usize,
    tuples: Vec<(Vec<Value>, RID)>,
    schema: Schema,
    initialized: bool,
    current_tuple_idx: usize,
}

impl MockExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<MockScanNode>,
        current_index: usize,
        tuples: Vec<(Vec<Value>, RID)>,
        schema: Schema,
    ) -> Self {
        Self {
            context,
            plan,
            current_index,
            tuples,
            schema,
            initialized: false,
            current_tuple_idx: 0,
        }
    }
}

impl AbstractExecutor for MockExecutor {
    fn init(&mut self) {
        self.current_index = 0;
        self.initialized = true;
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            self.init();
        }

        if self.current_tuple_idx >= self.tuples.len() {
            return Ok(None);
        }

        let (values, rid) = &self.tuples[self.current_tuple_idx];
        self.current_tuple_idx += 1;

        let tuple = Arc::new(Tuple::new(values, &self.schema, *rid));

        Ok(Some((tuple, *rid)))
    }

    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}
