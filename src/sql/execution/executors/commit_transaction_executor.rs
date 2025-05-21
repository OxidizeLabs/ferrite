use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::commit_transaction_plan::CommitTransactionPlanNode;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, error};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for committing transactions
pub struct CommitTransactionExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: CommitTransactionPlanNode,
    executed: bool,
}

impl CommitTransactionExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: CommitTransactionPlanNode) -> Self {
        debug!("Creating CommitTransactionExecutor");
        Self {
            context,
            plan,
            executed: false,
        }
    }
}

impl AbstractExecutor for CommitTransactionExecutor {
    fn init(&mut self) {
        debug!("Initializing CommitTransactionExecutor");
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        if self.executed {
            return None;
        }

        debug!("Executing transaction commit");
        self.executed = true;

        // Get transaction information from the context
        let txn_context = self.context.read().get_transaction_context();
        let transaction_id = txn_context.get_transaction_id();
        
        // Log whether this is a chained commit
        if self.plan.is_chain() {
            debug!("Transaction {} commit with chain option", transaction_id);
        }
        
        // Log whether this is a commit and release
        if self.plan.is_end() {
            debug!("Transaction {} commit with end option", transaction_id);
        }

        info!("Transaction {} commit operation prepared", transaction_id);
        
        // The actual commit will be performed by the execution engine
        // We just need to prepare for potential chaining after commit
        if self.plan.is_chain() {
            // Store the information needed to chain transactions in the context
            debug!("Preparing for transaction chaining after commit");
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