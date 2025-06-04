use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::storage::table::tuple::Tuple;
use parking_lot::RwLock;
use std::sync::Arc;

/// The AbstractExecutor implements the Volcano tuple-at-a-time iterator model.
/// This is the base trait from which all executors in the BusTub execution
/// engine inherit, and defines the minimal interface that all executors support.
pub trait AbstractExecutor: Send + Sync {
    /// Initialize the executor.
    ///
    /// # Warning
    ///
    /// This function must be called before `next()` is called!
    fn init(&mut self);

    /// Yield the next tuple from this executor.
    ///
    /// # Returns
    ///
    /// Returns `Some((Tuple, RID))` if a tuple was produced, `None` if there are no more tuples.
    fn next(&mut self) -> Option<(Arc<Tuple>, RID)>;

    /// Get the schema of the tuples that this executor produces.
    fn get_output_schema(&self) -> &Schema;

    /// Get the executor context in which this executor runs.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>>;
}

/// A base struct for concrete executors to inherit from.
pub struct BaseExecutor {
    exec_ctx: Arc<ExecutionContext>,
}

impl BaseExecutor {
    /// Construct a new BaseExecutor instance.
    ///
    /// # Arguments
    ///
    /// * `exec_ctx` - The executor context that the executor runs with.
    pub fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self { exec_ctx }
    }

    /// Get the executor context in which this executor runs.
    pub fn get_executor_context(&self) -> &ExecutionContext {
        &self.exec_ctx
    }
}
