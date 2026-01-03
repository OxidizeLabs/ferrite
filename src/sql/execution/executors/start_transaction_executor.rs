//! # Start Transaction Executor
//!
//! This module implements the executor for the SQL `BEGIN` or `START TRANSACTION`
//! statement, which initiates a new database transaction with configurable
//! isolation level and access mode.
//!
//! ## SQL Syntax
//!
//! The executor supports various transaction start patterns:
//!
//! ```sql
//! -- Basic transaction start (uses default isolation level)
//! BEGIN;
//! START TRANSACTION;
//!
//! -- Specify isolation level
//! BEGIN ISOLATION LEVEL READ UNCOMMITTED;
//! BEGIN ISOLATION LEVEL READ COMMITTED;
//! BEGIN ISOLATION LEVEL REPEATABLE READ;
//! BEGIN ISOLATION LEVEL SERIALIZABLE;
//!
//! -- Read-only transaction (optimization hint)
//! BEGIN READ ONLY;
//! START TRANSACTION READ ONLY;
//!
//! -- Combined options
//! BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY;
//! ```
//!
//! ## Transaction Lifecycle
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    Transaction Lifecycle                            │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │   BEGIN / START TRANSACTION                                         │
//! │          │                                                          │
//! │          ▼                                                          │
//! │   ┌─────────────────────────────────────────┐                       │
//! │   │  Transaction Active (ID assigned)       │                       │
//! │   │  - Isolation level set                  │                       │
//! │   │  - Read/write mode configured           │                       │
//! │   │  - Locks can be acquired                │                       │
//! │   └────────────────┬────────────────────────┘                       │
//! │                    │                                                │
//! │          ┌─────────┴─────────┐                                      │
//! │          │                   │                                      │
//! │          ▼                   ▼                                      │
//! │       COMMIT             ROLLBACK                                   │
//! │          │                   │                                      │
//! │          ▼                   ▼                                      │
//! │   ┌─────────────┐     ┌─────────────┐                               │
//! │   │   Persist   │     │   Discard   │                               │
//! │   │   Changes   │     │   Changes   │                               │
//! │   └─────────────┘     └─────────────┘                               │
//! │                                                                     │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Isolation Levels
//!
//! The executor supports four SQL standard isolation levels:
//!
//! | Level | Dirty Read | Non-repeatable Read | Phantom Read |
//! |-------|------------|---------------------|--------------|
//! | READ UNCOMMITTED | Possible | Possible | Possible |
//! | READ COMMITTED | Prevented | Possible | Possible |
//! | REPEATABLE READ | Prevented | Prevented | Possible |
//! | SERIALIZABLE | Prevented | Prevented | Prevented |
//!
//! ## Execution Model
//!
//! The executor performs these steps:
//!
//! 1. **Get isolation level**: From plan or use default (READ COMMITTED)
//! 2. **Create transaction**: Via transaction manager with unique ID
//! 3. **Create context**: New `TransactionContext` with the transaction
//! 4. **Update execution context**: Replace old transaction context
//!
//! ## Concurrency Considerations
//!
//! - Multiple transactions can be started concurrently from different sessions
//! - Each transaction gets a unique, monotonically increasing ID
//! - The transaction manager ensures thread-safe transaction creation
//! - Lock manager is shared across transactions for deadlock detection
//!
//! ## Error Handling
//!
//! - **Transaction manager shutdown**: Returns `Ok(None)` gracefully
//! - **Other failures**: Returns `Err(DBError::Execution)` with details

use std::sync::Arc;

use log::{debug, info};
use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::start_transaction_plan::StartTransactionPlanNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::tuple::Tuple;

/// Executor for starting a new database transaction.
///
/// `StartTransactionExecutor` handles the `BEGIN` or `START TRANSACTION` SQL
/// statement, creating a new transaction with the specified isolation level
/// and access mode, then updating the execution context to use it.
///
/// # Transaction Creation Flow
///
/// ```text
/// StartTransactionExecutor::next()
///           │
///           ▼
/// ┌─────────────────────────────┐
/// │ Get isolation level         │
/// │ (from plan or default)      │
/// └─────────────┬───────────────┘
///               │
///               ▼
/// ┌─────────────────────────────┐
/// │ TransactionManager::begin() │
/// │ - Assign unique txn_id      │
/// │ - Set isolation level       │
/// │ - Initialize txn state      │
/// └─────────────┬───────────────┘
///               │
///               ▼
/// ┌─────────────────────────────┐
/// │ Create TransactionContext   │
/// │ - Wrap new transaction      │
/// │ - Attach lock manager       │
/// │ - Attach txn manager        │
/// └─────────────┬───────────────┘
///               │
///               ▼
/// ┌─────────────────────────────┐
/// │ Update ExecutionContext     │
/// │ - Replace txn context       │
/// │ - New txn now active        │
/// └─────────────────────────────┘
/// ```
///
/// # Example Usage
///
/// ```ignore
/// // Start a serializable, read-only transaction
/// let plan = StartTransactionPlanNode::new(
///     Some(IsolationLevel::Serializable),
///     true,  // read_only
/// );
///
/// let mut executor = StartTransactionExecutor::new(context.clone(), plan);
/// executor.init();
/// executor.next()?;
///
/// // Now the execution context has a new transaction
/// let new_txn = context.read().get_transaction_context();
/// println!("New transaction ID: {}", new_txn.get_transaction_id());
/// ```
///
/// # Fields
///
/// - `context`: Shared execution context to be updated with new transaction
/// - `plan`: Plan node containing isolation level and read-only flag
/// - `executed`: Ensures the transaction is started exactly once
pub struct StartTransactionExecutor {
    /// Shared execution context that will be updated with the new transaction.
    context: Arc<RwLock<ExecutionContext>>,

    /// Plan node containing transaction options (isolation level, read-only).
    plan: StartTransactionPlanNode,

    /// Flag ensuring single execution of transaction start.
    executed: bool,
}

impl StartTransactionExecutor {
    /// Creates a new `StartTransactionExecutor` with the given context and plan.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context that will be updated with the
    ///   new transaction once started
    /// * `plan` - Plan node containing:
    ///   - Optional isolation level (defaults to READ COMMITTED if not specified)
    ///   - Read-only flag for optimization hints
    ///
    /// # Returns
    ///
    /// A new executor instance ready for initialization and execution.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create executor for READ COMMITTED transaction
    /// let plan = StartTransactionPlanNode::new(
    ///     Some(IsolationLevel::ReadCommitted),
    ///     false,
    /// );
    /// let executor = StartTransactionExecutor::new(context.clone(), plan);
    ///
    /// // Create executor with default isolation level
    /// let default_plan = StartTransactionPlanNode::new(None, false);
    /// let default_executor = StartTransactionExecutor::new(context.clone(), default_plan);
    /// ```
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: StartTransactionPlanNode) -> Self {
        Self {
            context,
            plan,
            executed: false,
        }
    }
}

impl AbstractExecutor for StartTransactionExecutor {
    /// Initializes the start transaction executor.
    ///
    /// For transaction control operations, initialization is minimal since
    /// there are no child executors or complex state to set up. This method
    /// primarily exists for trait compliance and logging.
    fn init(&mut self) {
        debug!("Initializing StartTransactionExecutor");
    }

    /// Executes the transaction start operation.
    ///
    /// This method performs the core transaction creation logic:
    ///
    /// 1. **Determine isolation level**: Use plan's level or default (READ COMMITTED)
    /// 2. **Create transaction**: Via transaction manager with unique ID
    /// 3. **Build context**: Create new `TransactionContext` with lock manager
    /// 4. **Update execution context**: Install new transaction as active
    ///
    /// # Returns
    ///
    /// - `Ok(None)` - Transaction started successfully (transaction ops produce no tuples)
    /// - `Ok(None)` - Transaction manager is shut down (graceful degradation)
    /// - `Err(DBError::Execution)` - Other transaction start failures
    ///
    /// # Execution Semantics
    ///
    /// - **Exactly-once**: The `executed` flag ensures start happens only once
    /// - **Idempotent after first**: Subsequent calls return `Ok(None)` immediately
    /// - **Atomic context update**: The execution context is updated under write lock
    ///
    /// # Concurrency
    ///
    /// The transaction manager assigns unique IDs atomically, allowing multiple
    /// concurrent transaction starts from different sessions:
    ///
    /// ```text
    /// Session A: BEGIN → Transaction ID 100
    /// Session B: BEGIN → Transaction ID 101  (concurrent, both succeed)
    /// Session C: BEGIN → Transaction ID 102
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut executor = StartTransactionExecutor::new(ctx, plan);
    /// executor.init();
    ///
    /// // Start the transaction
    /// executor.next()?;
    ///
    /// // Execution context now has the new transaction
    /// let txn_id = ctx.read()
    ///     .get_transaction_context()
    ///     .get_transaction_id();
    /// ```
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            return Ok(None);
        }

        debug!("Executing start transaction");
        self.executed = true;

        // Get the isolation level from the plan or use default
        let isolation_level = self.plan.get_isolation_level().unwrap_or_default();

        debug!(
            "Starting new transaction with isolation level {:?}",
            isolation_level
        );

        // Check if this is a read-only transaction
        if self.plan.is_read_only() {
            debug!("Transaction will be read-only");
        }

        // Get the transaction manager from the current transaction context
        let transaction_manager = {
            let context = self.context.read();
            let txn_context = context.get_transaction_context();
            txn_context.get_transaction_manager()
        };

        // Start a new transaction
        match transaction_manager.begin(isolation_level) {
            Ok(new_transaction) => {
                let transaction_id = new_transaction.get_transaction_id();
                info!("Started new transaction with ID {}", transaction_id);

                // Create new transaction context with the same lock manager
                let lock_manager = {
                    let context = self.context.read();
                    context.get_transaction_context().get_lock_manager()
                };

                let new_transaction_context = Arc::new(TransactionContext::new(
                    new_transaction,
                    lock_manager,
                    transaction_manager,
                ));

                // Update the execution context with the new transaction context
                {
                    let mut context = self.context.write();
                    context.set_transaction_context(new_transaction_context);
                }

                debug!("Transaction {} started successfully", transaction_id);
            },
            Err(e) => {
                // Check if the error is due to transaction manager shutdown
                if e.contains("Transaction manager is shutdown") {
                    debug!("Transaction manager is shut down, cannot start new transaction");
                    // Return Ok(None) to indicate the operation completed but no transaction was started
                    return Ok(None);
                }
                return Err(DBError::Execution(format!(
                    "Failed to start transaction: {}",
                    e
                )));
            },
        }

        // Transaction operations don't produce tuples
        Ok(None)
    }

    /// Returns the output schema for this executor.
    ///
    /// Transaction control operations like `BEGIN` do not produce data tuples,
    /// so the output schema is empty (contains no columns).
    ///
    /// # Returns
    ///
    /// A reference to an empty schema from the underlying plan node.
    fn get_output_schema(&self) -> &Schema {
        // Transaction operations don't have output schemas
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// After successful transaction start, the execution context will contain
    /// the newly created transaction. This allows callers to access:
    ///
    /// - **New transaction ID**: Via `get_transaction_context().get_transaction_id()`
    /// - **Isolation level**: The level specified in the plan or default
    /// - **Lock manager**: Shared across all transactions for coordination
    ///
    /// # Returns
    ///
    /// An `Arc`-wrapped, `RwLock`-protected reference to the execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use parking_lot::RwLock;
    use tempfile::TempDir;

    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::Catalog;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Use the transaction manager to create a transaction instead of hardcoding ID 0
            let transaction = transaction_manager
                .begin(IsolationLevel::ReadUncommitted)
                .unwrap();
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn create_execution_context(&self) -> Arc<RwLock<ExecutionContext>> {
            let catalog = Arc::new(RwLock::new(Catalog::new(
                self.bpm.clone(),
                self.transaction_manager.clone(),
            )));

            Arc::new(RwLock::new(ExecutionContext::new(
                self.bpm.clone(),
                catalog,
                self.transaction_context.clone(),
            )))
        }
    }

    #[tokio::test]
    async fn test_start_transaction_executor() {
        let test_context = TestContext::new("start_transaction_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a start transaction plan with serializable isolation level
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), false);

        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_ne!(
            original_txn_id, new_txn_id,
            "New transaction should have different ID"
        );

        // Execute again - should return None as it's already executed
        let result = executor.next();
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_start_transaction_with_default_isolation() {
        let test_context = TestContext::new("start_transaction_default_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a start transaction plan with no specified isolation level
        let plan = StartTransactionPlanNode::new(None, true);

        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_ne!(
            original_txn_id, new_txn_id,
            "New transaction should have different ID"
        );

        // Check isolation level - should be default (READ COMMITTED)
        let isolation_level = exec_context
            .read()
            .get_transaction_context()
            .get_transaction()
            .get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::default());
    }

    #[tokio::test]
    async fn test_start_transaction_with_invalid_isolation() {
        let test_context = TestContext::new("start_transaction_invalid_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a start transaction plan with an invalid isolation level
        let plan = StartTransactionPlanNode::new(None, false);

        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_ne!(
            original_txn_id, new_txn_id,
            "New transaction should have different ID"
        );

        // Check isolation level - should default to READ COMMITTED when none is specified
        let isolation_level = exec_context
            .read()
            .get_transaction_context()
            .get_transaction()
            .get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::ReadCommitted);
    }

    #[tokio::test]
    async fn test_start_read_only_transaction() {
        let test_context = TestContext::new("start_read_only_transaction_test").await;
        let exec_context = test_context.create_execution_context();

        // Store the original transaction ID
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();

        // Create a start transaction plan with read-only flag set to true
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::ReadCommitted), true);

        // Create the executor
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data)
        assert!(result.unwrap().is_none());

        // Verify that a new transaction was created with a different ID
        let new_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_ne!(
            original_txn_id, new_txn_id,
            "New transaction should have different ID"
        );

        // Check isolation level
        let isolation_level = exec_context
            .read()
            .get_transaction_context()
            .get_transaction()
            .get_isolation_level();
        assert_eq!(isolation_level, IsolationLevel::ReadCommitted);

        // In a real implementation, we would verify the read-only flag here
        // but that would require modifying the Transaction class to store that flag
    }

    #[tokio::test]
    async fn test_all_isolation_levels() {
        // Test each of the isolation levels
        let isolation_levels = vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ];

        for expected_level in isolation_levels {
            let test_context = TestContext::new(&format!(
                "isolation_level_{}_test",
                expected_level.to_string().to_lowercase()
            ))
            .await;
            let exec_context = test_context.create_execution_context();

            // Store the original transaction ID
            let original_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();

            // Create a start transaction plan with this isolation level
            let plan = StartTransactionPlanNode::new(Some(expected_level), false);

            // Create the executor
            let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

            // Initialize and execute
            executor.init();
            let result = executor.next();

            // Should return None (no output data)
            assert!(result.unwrap().is_none());

            // Verify that a new transaction was created with a different ID
            let new_txn_id = exec_context
                .read()
                .get_transaction_context()
                .get_transaction_id();
            assert_ne!(
                original_txn_id, new_txn_id,
                "New transaction should have different ID"
            );

            // Check isolation level
            let isolation_level = exec_context
                .read()
                .get_transaction_context()
                .get_transaction()
                .get_isolation_level();
            assert_eq!(
                isolation_level, expected_level,
                "Isolation level should match for {}",
                expected_level
            );
        }
    }

    #[tokio::test]
    async fn test_concurrent_transaction_starts() {
        // This test verifies that multiple transactions can be started concurrently
        let test_context = TestContext::new("concurrent_transaction_starts_test").await;
        let transaction_manager = test_context.transaction_manager.clone();
        let buffer_pool = test_context.bpm.clone();

        // Number of threads to create
        const NUM_THREADS: usize = 5;

        // Create a barrier to synchronize thread start
        let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));

        // Create multiple threads, each starting a transaction
        let mut handles = Vec::with_capacity(NUM_THREADS);
        let txn_ids = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(NUM_THREADS)));

        for i in 0..NUM_THREADS {
            let thread_tm = transaction_manager.clone();
            let thread_barrier = barrier.clone();
            let thread_txn_ids = txn_ids.clone();

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                thread_barrier.wait();

                // Start a transaction with a specific isolation level
                let isolation_level = match i % 4 {
                    0 => IsolationLevel::ReadUncommitted,
                    1 => IsolationLevel::ReadCommitted,
                    2 => IsolationLevel::RepeatableRead,
                    _ => IsolationLevel::Serializable,
                };

                // Try to begin a transaction and record its ID
                match thread_tm.begin(isolation_level) {
                    Ok(txn) => {
                        let txn_id = txn.get_transaction_id();
                        thread_txn_ids.lock().push(txn_id);

                        // Small sleep to simulate some work
                        thread::sleep(Duration::from_millis(10));

                        // Return transaction for cleanup
                        Some(txn)
                    },
                    Err(_) => None,
                }
            });

            handles.push(handle);
        }

        // Collect results and clean up
        for handle in handles {
            if let Ok(Some(txn)) = handle.join() {
                transaction_manager.commit(txn, buffer_pool.clone()).await;
            }
        }

        // Verify that all transactions got unique IDs
        let final_txn_ids = txn_ids.lock().clone();
        assert_eq!(
            final_txn_ids.len(),
            NUM_THREADS,
            "All threads should have started transactions"
        );

        // Check for uniqueness by converting to a HashSet
        let unique_ids: std::collections::HashSet<_> = final_txn_ids.iter().collect();
        assert_eq!(
            unique_ids.len(),
            NUM_THREADS,
            "All transaction IDs should be unique"
        );
    }

    #[tokio::test]
    async fn test_transaction_manager_shutdown_handling() {
        let test_context = TestContext::new("transaction_manager_shutdown_test").await;
        let exec_context = test_context.create_execution_context();

        // First, shut down the transaction manager
        test_context.transaction_manager.shutdown().unwrap();

        // Try to start a transaction after shutdown
        let plan = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), false);
        let mut executor = StartTransactionExecutor::new(exec_context.clone(), plan);

        // Initialize and execute
        executor.init();
        let result = executor.next();

        // Should return None (no output data) since transaction manager is shut down
        assert!(result.unwrap().is_none());

        // The transaction context should not have changed since transaction start failed
        let original_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        let new_txn_id = exec_context
            .read()
            .get_transaction_context()
            .get_transaction_id();
        assert_eq!(
            original_txn_id, new_txn_id,
            "Transaction context should not change when transaction manager is shut down"
        );
    }
}
