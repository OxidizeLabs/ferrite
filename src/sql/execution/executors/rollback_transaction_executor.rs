use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::rollback_transaction_plan::RollbackTransactionPlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;

/// Executor for rolling back transactions
pub struct RollbackTransactionExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: RollbackTransactionPlanNode,
    executed: bool,
}

impl RollbackTransactionExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: RollbackTransactionPlanNode) -> Self {
        debug!("Creating RollbackTransactionExecutor");
        Self {
            context,
            plan,
            executed: false,
        }
    }
}

impl AbstractExecutor for RollbackTransactionExecutor {
    fn init(&mut self) {
        debug!("Initializing RollbackTransactionExecutor");
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if self.executed {
            return None;
        }

        debug!("Executing transaction rollback");
        self.executed = true;

        // Get transaction information from the context
        let txn_context = self.context.read().get_transaction_context();
        let transaction_id = txn_context.get_transaction_id();
        
        // Log whether this is a chained rollback
        if self.plan.is_chain() {
            debug!("Transaction {} rollback with chain option", transaction_id);
        }
        
        // Log whether this is a partial rollback to a savepoint
        if let Some(savepoint) = self.plan.get_savepoint() {
            debug!("Transaction {} rollback to savepoint '{}'", transaction_id, savepoint);
            
            // TODO: Implement partial rollback to savepoint
            // This would require savepoint functionality in the transaction system
            warn!("Partial rollback to savepoint not yet implemented");
        } else {
            debug!("Transaction {} full rollback", transaction_id);
        }

        info!("Transaction {} rollback operation prepared", transaction_id);
        
        // The actual abort will be performed by the execution engine
        // We just need to prepare for potential chaining after rollback
        if self.plan.is_chain() {
            // Store the information needed to chain transactions in the context
            debug!("Preparing for transaction chaining after rollback");
            let mut context_write = self.context.write();
            context_write.set_chain_after_transaction(true);
        }
        
        // Return None as transaction operations don't produce tuples
        None
    }

    fn get_output_schema(&self) -> &Schema {
        // Transaction operations don't have output schemas
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
} 