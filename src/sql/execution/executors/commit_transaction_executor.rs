//! # Commit Transaction Executor Module
//!
//! This module implements the executor for `COMMIT` statements, which finalize
//! transactions and make their changes permanent in the database.
//!
//! ## SQL Syntax
//!
//! ```sql
//! COMMIT [AND [NO] CHAIN] [RELEASE]
//! ```
//!
//! ## Commit Options
//!
//! | Option       | Description                                              |
//! |--------------|----------------------------------------------------------|
//! | (default)    | Commits transaction and ends it                          |
//! | `AND CHAIN`  | Commits and immediately starts a new transaction         |
//! | `RELEASE`    | Commits and disconnects the session (end option)         |
//!
//! ## Execution Model
//!
//! The `CommitTransactionExecutor` prepares the commit operation but delegates
//! the actual commit to the execution engine. This separation allows proper
//! coordination with the transaction manager and recovery system.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │              CommitTransactionExecutor                  │
//! │                                                         │
//! │  1. Validate transaction state                          │
//! │  2. Set chain flag if AND CHAIN specified               │
//! │  3. Signal execution engine to perform commit           │
//! │                                                         │
//! │              ┌─────────────────────┐                    │
//! │              │  Execution Engine   │                    │
//! │              │  - Flush WAL        │                    │
//! │              │  - Release locks    │                    │
//! │              │  - Update txn state │                    │
//! │              └─────────────────────┘                    │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Output
//!
//! Commit operations produce no output tuples. The executor returns `None`
//! immediately, and the execution engine handles the actual commit.

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::commit_transaction_plan::CommitTransactionPlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for `COMMIT` transaction statements.
///
/// `CommitTransactionExecutor` handles the preparation phase of transaction
/// commits. It validates the transaction state and sets up any chaining
/// behavior before the execution engine performs the actual commit.
///
/// # ACID Guarantees
///
/// Upon successful commit:
/// - **Durability**: All changes are written to the WAL and flushed to disk
/// - **Atomicity**: The entire transaction's changes become visible atomically
/// - **Isolation**: Locks are released, allowing other transactions to proceed
///
/// # Transaction Chaining
///
/// When `AND CHAIN` is specified, the execution context is configured to
/// automatically begin a new transaction after the current one commits.
/// This is useful for batch processing that needs transaction boundaries.
///
/// # Example
///
/// ```ignore
/// // COMMIT AND CHAIN - commit and start new transaction
/// let executor = CommitTransactionExecutor::new(context, plan);
/// executor.init();
/// executor.next()?; // Prepares commit, returns None
/// // Execution engine completes the commit
/// ```
pub struct CommitTransactionExecutor {
    /// Shared execution context containing transaction state.
    context: Arc<RwLock<ExecutionContext>>,
    /// Plan node with commit options (chain, end/release).
    plan: CommitTransactionPlanNode,
    /// Flag indicating whether the commit has been processed.
    executed: bool,
}

impl CommitTransactionExecutor {
    /// Creates a new `CommitTransactionExecutor` from a plan node.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context with current transaction state.
    /// * `plan` - Commit plan node containing options (chain, end/release).
    ///
    /// # Returns
    ///
    /// A new executor ready for initialization.
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
    /// Initializes the commit executor.
    ///
    /// Performs logging for debugging purposes. No state setup is required
    /// since commit preparation happens in `next()`.
    fn init(&mut self) {
        debug!("Initializing CommitTransactionExecutor");
    }

    /// Prepares the transaction commit operation.
    ///
    /// This method:
    /// 1. Retrieves the current transaction ID from the context
    /// 2. Logs the commit operation with any options (chain, end)
    /// 3. Sets the chain flag in the context if `AND CHAIN` was specified
    ///
    /// The actual commit (WAL flush, lock release, state update) is performed
    /// by the execution engine after this executor returns.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` - Always returns `None` since commits produce no tuples.
    ///   The execution engine interprets this as a signal to perform the
    ///   actual commit.
    ///
    /// # Transaction State
    ///
    /// After `next()` returns:
    /// - If `AND CHAIN`: `context.chain_after_transaction` is set to `true`
    /// - The transaction remains active until the execution engine commits
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            return Ok(None);
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

        // Return Ok(None) as transaction operations don't produce tuples
        Ok(None)
    }

    /// Returns the output schema for commit operations.
    ///
    /// Commit statements produce no output tuples, so this returns an
    /// empty schema from the plan node.
    fn get_output_schema(&self) -> &Schema {
        // Transaction operations don't have output schemas
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to the transaction context for commit coordination.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}
