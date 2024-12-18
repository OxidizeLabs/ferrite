use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::storage::table::tuple::Tuple;
use std::sync::Arc;

pub struct CreateTableExecutor {
    context: Arc<ExecutorContext>,
    plan: CreateTablePlanNode,
    executed: bool,
}

impl CreateTableExecutor {
    pub fn new(context: Arc<ExecutorContext>, plan: CreateTablePlanNode, executed: bool) -> Self {
        Self {
            context,
            plan,
            executed,
        }
    }
}

impl AbstractExecutor for CreateTableExecutor {
    fn init(&mut self) {
        self.executed = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.executed {
            return None;
        }

        let catalog = self.context.get_catalog();
        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema();

        // Since Catalog isn't wrapped in RwLock, we need to be careful with mutation
        // Check if table exists when if_not_exists is true
        let mut catalog_guard = catalog.write();
        if self.plan.if_not_exists() && catalog_guard.get_table(table_name).is_some() {
            self.executed = true;
            return None;
        }

        // Create new table in catalog
        catalog_guard.create_table(
            table_name,
            schema.clone(),
        );

        self.executed = true;

        // CREATE TABLE doesn't produce any tuples
        None
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        &self.context
    }
}
