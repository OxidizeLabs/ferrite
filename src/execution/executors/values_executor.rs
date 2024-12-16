use std::sync::{Arc};
use parking_lot::RwLock;
use log::{debug, error, trace};
use crate::common::rid::RID;
use crate::catalogue::schema::Schema;
use crate::execution::plans::values_plan::{ValueRow, ValuesNode};
use crate::storage::table::tuple::Tuple;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};

pub struct ValuesExecutor {
    context: Arc<ExecutorContext>,
    plan: Arc<ValuesNode>,
    current_row: usize,
    initialized: bool,
}

impl ValuesExecutor {
    pub fn new(context: Arc<ExecutorContext>, plan: Arc<ValuesNode>) -> Self {
        Self {
            context,
            plan,
            current_row: 0,
            initialized: false,
        }
    }
}

impl AbstractExecutor for ValuesExecutor {
    fn init(&mut self) {
        if !self.initialized {
            debug!("Initializing values executor");
            self.current_row = 0;
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            self.init();
        }

        // Check if we've processed all rows
        if self.current_row >= self.plan.get_row_count() {
            debug!("No more rows to process");
            return None;
        }

        // Get the current row
        let row = self.plan.get_rows().get(self.current_row)?;
        let mut row_clone = row.clone();
        let schema = self.plan.get_output_schema();

        // Evaluate the expressions in the current row
        match row_clone.evaluate(schema) {
            Ok(values) => {
                // Create tuple with evaluated values
                let tuple = Tuple::new(values, schema.clone(), RID::default());

                // Advance to next row
                self.current_row += 1;

                debug!("Successfully evaluated row {}, advancing to next row", self.current_row - 1);
                Some((tuple, RID::default()))
            }
            Err(e) => {
                error!("Failed to evaluate values: {}", e);
                None
            }
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> &ExecutorContext {
        self.context.as_ref()
    }
}