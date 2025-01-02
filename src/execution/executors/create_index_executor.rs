use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::storage::index::index::IndexType;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CreateIndexExecutor {
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<CreateIndexPlanNode>,
    executed: bool,
}

impl CreateIndexExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<CreateIndexPlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateIndexExecutor for index '{}', if_not_exists={}",
            plan.get_index_name(),
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

impl AbstractExecutor for CreateIndexExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
        self.executed = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.executed {
            debug!("CreateIndexExecutor already executed, returning None");
            return None;
        }

        let index_name = self.plan.get_index_name();
        let table_name = self.plan.get_table_name();

        debug!("Acquiring executor context lock for index '{}'", index_name);
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

        debug!("Acquiring catalog write lock for index '{}'", index_name);
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
            if !self.plan.if_not_exists() && catalog_guard.get_table_indexes(table_name).is_empty() {
                info!(
                    "Index '{}' already exists, skipping creation (IF NOT EXISTS)",
                    index_name
                );
                self.executed = true;
                return None;
            }

            // Create the index
            debug!("Creating new index '{}' in catalog", index_name);
            match catalog_guard.create_index(
                index_name,
                table_name,
                Default::default(),
                vec![],
                0,
                false,
                IndexType::BPlusTreeIndex,
            ) {
                Some(index_info) => {
                    info!(
                        "Successfully created index '{}' with OID {}",
                        index_name,
                        index_info.get_index_oid()
                    );
                }
                None => {
                    if !self.plan.if_not_exists() {
                        warn!(
                            "Failed to create index '{}' - index may already exist",
                            index_name
                        );
                    }
                }
            }
        }
        debug!("Released catalog write lock");

        self.executed = true;
        debug!("CreateIndexExecutor execution completed");
        None
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}
impl Drop for CreateIndexExecutor {
    fn drop(&mut self) {
        debug!(
            "Dropping CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
    }
}
