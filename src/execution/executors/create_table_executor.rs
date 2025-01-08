use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CreateTableExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<CreateTablePlanNode>,
    executed: bool,
}

impl CreateTableExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<CreateTablePlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateTableExecutor for table '{}', if_not_exists={}",
            plan.get_table_name(),
            plan.if_not_exists()
        );
        debug!("Output schema: {:?}", plan.get_output_schema());

        Self {
            context,
            plan,
            executed,
        }
    }
}

impl AbstractExecutor for CreateTableExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
        );
        self.executed = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.executed {
            debug!("CreateTableExecutor already executed, returning None");
            return None;
        }

        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema().clone();

        debug!("Acquiring executor context lock for table '{}'", table_name);
        let catalog = {
            let context_guard = match self.context.try_read() {
                Some(guard) => {
                    debug!("Successfully acquired context read lock");
                    guard
                }
                None => {
                    warn!("Failed to acquire context read lock - lock contention detected");
                    return None;
                }
            };
            context_guard.get_catalog().clone()
        };
        debug!("Released executor context lock");

        debug!("Acquiring catalog write lock for table '{}'", table_name);
        {
            let mut catalog_guard = match catalog.try_write() {
                Some(guard) => {
                    debug!("Successfully acquired catalog write lock");
                    guard
                }
                None => {
                    warn!("Failed to acquire catalog write lock - lock contention detected");
                    return None;
                }
            };

            // Check existence first
            if self.plan.if_not_exists() && catalog_guard.get_table(table_name).is_some() {
                info!(
                    "Table '{}' already exists, skipping creation (IF NOT EXISTS)",
                    table_name
                );
                self.executed = true;
                return None;
            }

            // Create the table
            debug!("Creating new table '{}' in catalog", table_name);
            match catalog_guard.create_table(table_name, schema) {
                Some(table_info) => {
                    info!(
                        "Successfully created table '{}' with OID {}",
                        table_name,
                        table_info.get_table_oidt()
                    );
                }
                None => {
                    if !self.plan.if_not_exists() {
                        warn!(
                            "Failed to create table '{}' - table may already exist",
                            table_name
                        );
                    }
                }
            }
        }
        debug!("Released catalog write lock");

        self.executed = true;
        debug!("CreateTableExecutor execution completed");
        None
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}
impl Drop for CreateTableExecutor {
    fn drop(&mut self) {
        debug!(
            "Dropping CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
        );
    }
}
