//! # Database Instance
//!
//! This module provides `DBInstance`, the central orchestrator for the Ferrite database.
//! It initializes and coordinates all core subsystems: storage, buffer management, catalog,
//! transactions, recovery, and query execution.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────────────┐
//!   │                              DBInstance                                          │
//!   │                                                                                  │
//!   │   ┌────────────────────────────────────────────────────────────────────────────┐ │
//!   │   │  Storage Layer                                                             │ │
//!   │   │                                                                            │ │
//!   │   │   AsyncDiskManager ──► BufferPoolManager ──► LRUKReplacer                  │ │
//!   │   │        │                      │                                            │ │
//!   │   │        │                      │                                            │ │
//!   │   │   (disk I/O)            (page cache)                                       │ │
//!   │   └────────────────────────────────────────────────────────────────────────────┘ │
//!   │                                      │                                           │
//!   │   ┌────────────────────────────────────────────────────────────────────────────┐ │
//!   │   │  Metadata Layer                  │                                         │ │
//!   │   │                                  ▼                                         │ │
//!   │   │   Catalog ◄──────────────────────┘                                         │ │
//!   │   │      │                                                                     │ │
//!   │   │      ├── Tables (TableInfo, TableHeap)                                     │ │
//!   │   │      ├── Indexes (IndexInfo)                                               │ │
//!   │   │      └── Schemas                                                           │ │
//!   │   └────────────────────────────────────────────────────────────────────────────┘ │
//!   │                                      │                                           │
//!   │   ┌────────────────────────────────────────────────────────────────────────────┐ │
//!   │   │  Transaction & Recovery Layer    │                                         │ │
//!   │   │                                  ▼                                         │ │
//!   │   │   TransactionManagerFactory ──► TransactionManager                         │ │
//!   │   │            │                          │                                    │ │
//!   │   │            ▼                          ▼                                    │ │
//!   │   │   WALManager ──────────────► LogManager                                    │ │
//!   │   │            │                          │                                    │ │
//!   │   │            ▼                          ▼                                    │ │
//!   │   │   LogRecoveryManager ◄──────── (log file)                                  │ │
//!   │   └────────────────────────────────────────────────────────────────────────────┘ │
//!   │                                      │                                           │
//!   │   ┌────────────────────────────────────────────────────────────────────────────┐ │
//!   │   │  Execution Layer                 │                                         │ │
//!   │   │                                  ▼                                         │ │
//!   │   │   ExecutionEngine                                                          │ │
//!   │   │        │                                                                   │ │
//!   │   │        ├── SQL Parser                                                      │ │
//!   │   │        ├── Query Planner                                                   │ │
//!   │   │        ├── Optimizer                                                       │ │
//!   │   │        └── Executor                                                        │ │
//!   │   └────────────────────────────────────────────────────────────────────────────┘ │
//!   │                                      │                                           │
//!   │   ┌────────────────────────────────────────────────────────────────────────────┐ │
//!   │   │  Client Layer                    │                                         │ │
//!   │   │                                  ▼                                         │ │
//!   │   │   client_sessions: HashMap<u64, ClientSession>                             │ │
//!   │   │   prepared_statements: HashMap<u64, PreparedStatement>                     │ │
//!   │   │   writer: dyn ResultWriter                                                 │ │
//!   │   └────────────────────────────────────────────────────────────────────────────┘ │
//!   └──────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Initialization Flow
//!
//! ```text
//!   DBInstance::new(config)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  1. Check if database files exist                                      │
//!   │     db_file_exists = Path::exists(db_filename)                         │
//!   │     log_file_exists = Path::exists(db_log_filename)                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  2. Initialize storage components                                      │
//!   │     AsyncDiskManager → BufferPoolManager → LRUKReplacer                │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  3. Initialize recovery components                                     │
//!   │     LogManager (start flush thread)                                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  4. Initialize transaction components                                  │
//!   │     TransactionManager → Catalog → WALManager → TransactionFactory     │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  5. Initialize execution engine                                        │
//!   │     ExecutionEngine(catalog, buffer_pool, transaction_factory, wal)    │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  6. Run recovery if needed                                             │
//!   │     if (db_file_exists && log_file_exists && enable_logging)           │
//!   │         LogRecoveryManager::start_recovery()                           │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  7. Rebuild catalog from system tables                                 │
//!   │     catalog.rebuild_from_system_catalog()                              │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Query Execution Flow
//!
//! ```text
//!   execute_sql("SELECT * FROM users", IsolationLevel::ReadCommitted, writer)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  1. Begin transaction                                                  │
//!   │     txn_ctx = transaction_factory.begin_transaction(isolation_level)   │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  2. Create execution context                                           │
//!   │     exec_ctx = ExecutionContext(buffer_pool, catalog, txn_ctx)         │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  3. Execute query                                                      │
//!   │     engine.execute_sql(sql, exec_ctx, writer)                          │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ├──── Success ────►  transaction_factory.commit_transaction(txn_ctx)
//!        │
//!        └──── Error ──────►  transaction_factory.abort_transaction(txn_ctx)
//! ```
//!
//! ## Key Components
//!
//! | Component               | Type                            | Purpose                      |
//! |-------------------------|---------------------------------|------------------------------|
//! | `buffer_pool_manager`   | `Arc<BufferPoolManager>`        | Page caching and I/O         |
//! | `catalog`               | `Arc<RwLock<Catalog>>`          | Metadata (tables, indexes)   |
//! | `transaction_factory`   | `Arc<TransactionManagerFactory>`| Transaction lifecycle        |
//! | `execution_engine`      | `Arc<Mutex<ExecutionEngine>>`   | SQL processing               |
//! | `log_manager`           | `Arc<RwLock<LogManager>>`       | WAL persistence              |
//! | `wal_manager`           | `Arc<WALManager>`               | Transaction logging          |
//! | `recovery_manager`      | `Option<Arc<LogRecoveryManager>>`| Crash recovery              |
//! | `client_sessions`       | `HashMap<u64, ClientSession>`   | Per-client state             |
//! | `prepared_statements`   | `HashMap<u64, PreparedStatement>`| Prepared statement cache   |
//!
//! ## DBConfig Fields
//!
//! | Field                        | Type     | Default        | Description                  |
//! |------------------------------|----------|----------------|------------------------------|
//! | `db_filename`                | `String` | "test.db"      | Database file path           |
//! | `db_log_filename`            | `String` | "test.log"     | WAL file path                |
//! | `buffer_pool_size`           | `usize`  | 1024           | Buffer pool capacity (pages) |
//! | `enable_logging`             | `bool`   | true           | Enable WAL                   |
//! | `enable_managed_transactions`| `bool`   | true           | Auto-manage transactions     |
//! | `lru_k`                      | `usize`  | 10             | LRU-K parameter              |
//! | `lru_sample_size`            | `usize`  | 7              | LRU-K sample size            |
//! | `server_enabled`             | `bool`   | false          | Enable TCP server            |
//! | `server_host`                | `String` | "127.0.0.1"    | Server bind address          |
//! | `server_port`                | `u16`    | 5432           | Server port                  |
//! | `max_connections`            | `u32`    | 100            | Max concurrent connections   |
//! | `connection_timeout`         | `u64`    | 30             | Connection timeout (seconds) |
//!
//! ## Core API
//!
//! | Method                     | Description                                    |
//! |----------------------------|------------------------------------------------|
//! | `new(config)`              | Create and initialize database instance        |
//! | `execute_sql(sql, iso, w)` | Execute SQL with auto-commit                   |
//! | `execute_transaction()`    | Execute within existing transaction            |
//! | `begin_transaction(iso)`   | Start new transaction                          |
//! | `commit_transaction(id)`   | Commit transaction by ID                       |
//! | `abort_transaction(id)`    | Abort transaction by ID                        |
//! | `handle_network_query()`   | Process client request (server mode)           |
//! | `display_tables(writer)`   | List all tables                                |
//! | `get_table_info(name)`     | Get table schema details                       |
//!
//! ## Session Management
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │  Client Session Lifecycle                                                │
//!   │                                                                          │
//!   │   1. create_client_session(client_id)                                    │
//!   │      └── Creates ClientSession { id, transaction: None, isolation }      │
//!   │                                                                          │
//!   │   2. handle_network_query(request, client_id)                            │
//!   │      ├── Query(sql)        → execute with session's transaction          │
//!   │      ├── BeginTransaction  → start explicit transaction                  │
//!   │      ├── Commit            → commit session's transaction                │
//!   │      ├── Rollback          → abort session's transaction                 │
//!   │      ├── Prepare(sql)      → create prepared statement                   │
//!   │      ├── Execute(id, args) → run prepared statement                      │
//!   │      └── Close(id)         → deallocate prepared statement               │
//!   │                                                                          │
//!   │   3. remove_client_session(client_id)                                    │
//!   │      └── Aborts any active transaction, removes session                  │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::common::db_instance::{DBInstance, DBConfig};
//! use crate::common::result_writer::CliResultWriter;
//! use crate::concurrency::transaction::IsolationLevel;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure and create instance
//!     let config = DBConfig {
//!         db_filename: "my_database.db".to_string(),
//!         db_log_filename: "my_database.log".to_string(),
//!         buffer_pool_size: 2048,
//!         ..Default::default()
//!     };
//!
//!     let db = DBInstance::new(config).await?;
//!
//!     // Execute SQL with auto-commit
//!     let mut writer = CliResultWriter::new();
//!     db.execute_sql(
//!         "CREATE TABLE users (id INT, name VARCHAR(100))",
//!         IsolationLevel::ReadCommitted,
//!         &mut writer,
//!     ).await?;
//!
//!     db.execute_sql(
//!         "INSERT INTO users VALUES (1, 'Alice')",
//!         IsolationLevel::ReadCommitted,
//!         &mut writer,
//!     ).await?;
//!
//!     db.execute_sql(
//!         "SELECT * FROM users",
//!         IsolationLevel::ReadCommitted,
//!         &mut writer,
//!     ).await?;
//!
//!     // Explicit transaction
//!     let txn_ctx = db.begin_transaction(IsolationLevel::Serializable);
//!     db.execute_transaction("INSERT INTO users VALUES (2, 'Bob')", txn_ctx.clone(), &mut writer).await?;
//!     db.execute_transaction("INSERT INTO users VALUES (3, 'Charlie')", txn_ctx.clone(), &mut writer).await?;
//!     // Commit handled by transaction_factory
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Thread Safety
//!
//! - `DBInstance` is `Clone` (all fields are `Arc`-wrapped)
//! - Safe to share across threads/tasks
//! - Internal components use appropriate synchronization:
//!   - `Mutex` for exclusive access (execution_engine, client_sessions)
//!   - `RwLock` for read-heavy access (catalog, log_manager)
//!
//! ## Recovery Behavior
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Recovery Decision Matrix                                               │
//!   │                                                                         │
//!   │   db_file_exists │ log_file_exists │ enable_logging │ Recovery?         │
//!   │   ───────────────┼─────────────────┼────────────────┼─────────────────  │
//!   │   false          │ false           │ *              │ No (fresh start)  │
//!   │   true           │ false           │ *              │ No (no log)       │
//!   │   false          │ true            │ *              │ No (no db)        │
//!   │   true           │ true            │ false          │ No (disabled)     │
//!   │   true           │ true            │ true           │ YES               │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Implementation Notes
//!
//! - **Shared TransactionManager**: Single instance shared between Catalog and Factory
//! - **Async Initialization**: `new()` is async for disk I/O and recovery
//! - **Auto-Commit**: `execute_sql()` auto-commits unless ROLLBACK is executed
//! - **Prepared Statements**: Server-side caching with parameter type validation
//! - **Debug Mode**: Optional verbose logging for client operations

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalog::Catalog;
use crate::client::ClientSession;
use crate::common::exception::DBError;
use crate::common::result_writer::{CliResultWriter, NetworkResultWriter, ResultWriter};
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_recovery::LogRecoveryManager;
use crate::recovery::wal_manager::WALManager;
use crate::server::{DatabaseRequest, DatabaseResponse, QueryResults};
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::execution_engine::ExecutionEngine;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::error;
use log::{debug, info, warn};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Configuration options for initializing a database instance.
///
/// All fields have sensible defaults via [`Default`]. Customize as needed
/// for your deployment (e.g., larger buffer pool, different file paths).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DBConfig {
    /// Path to the database file (default: `"test.db"`).
    pub db_filename: String,
    /// Path to the write-ahead log file (default: `"test.log"`).
    pub db_log_filename: String,
    /// Number of pages in the buffer pool (default: `1024`).
    pub buffer_pool_size: usize,
    /// Whether to enable write-ahead logging (default: `true`).
    pub enable_logging: bool,
    /// Whether to auto-manage transaction lifecycle (default: `true`).
    pub enable_managed_transactions: bool,
    /// The K parameter for LRU-K replacement policy (default: `10`).
    pub lru_k: usize,
    /// Sample size for LRU-K eviction decisions (default: `7`).
    pub lru_sample_size: usize,
    /// Whether to start the TCP server (default: `false`).
    pub server_enabled: bool,
    /// Host address for the TCP server (default: `"127.0.0.1"`).
    pub server_host: String,
    /// Port for the TCP server (default: `5432`).
    pub server_port: u16,
    /// Maximum concurrent client connections (default: `100`).
    pub max_connections: u32,
    /// Connection timeout in seconds (default: `30`).
    pub connection_timeout: u64,
}

/// The central database instance that orchestrates all subsystems.
///
/// `DBInstance` is the main entry point for interacting with the database.
/// It initializes and coordinates storage, buffer management, catalog,
/// transactions, recovery, and query execution components.
///
/// # Thread Safety
/// This struct is `Clone` (all fields are `Arc`-wrapped) and safe to share
/// across threads/tasks.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DBInstance {
    /// Manages page caching and disk I/O.
    buffer_pool_manager: Arc<BufferPoolManager>,
    /// Stores metadata about tables, indexes, and schemas.
    catalog: Arc<RwLock<Catalog>>,
    /// Creates and manages transaction lifecycles.
    transaction_factory: Arc<TransactionManagerFactory>,
    /// Parses, plans, optimizes, and executes SQL queries.
    execution_engine: Arc<Mutex<ExecutionEngine>>,
    /// Manages write-ahead log persistence.
    log_manager: Arc<RwLock<LogManager>>,
    /// High-level WAL operations for transaction logging.
    wal_manager: Arc<WALManager>,
    /// Handles crash recovery (present only if recovery was performed).
    recovery_manager: Option<Arc<LogRecoveryManager>>,
    /// Configuration used to initialize this instance.
    config: DBConfig,
    /// Default result writer for output formatting.
    writer: Arc<Mutex<dyn ResultWriter>>,
    /// Per-client session state for server mode.
    client_sessions: Arc<Mutex<HashMap<u64, ClientSession>>>,
    /// Whether verbose debug logging is enabled.
    debug_mode: bool,
    /// Cache of prepared statements by statement ID.
    prepared_statements: Arc<Mutex<HashMap<u64, PreparedStatement>>>,
    /// Counter for generating unique prepared statement IDs.
    next_statement_id: Arc<Mutex<u64>>,
}

/// Server-side representation of a prepared SQL statement.
///
/// Stores the original SQL and expected parameter types for validation
/// when the statement is executed with bound parameters.
#[derive(Clone)]
struct PreparedStatement {
    /// The original SQL query with `?` placeholders.
    sql: String,
    /// Expected types for each parameter placeholder.
    parameter_types: Vec<TypeId>,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            db_filename: "test.db".to_string(),
            db_log_filename: "test.log".to_string(),
            buffer_pool_size: 1024,
            enable_logging: true,
            enable_managed_transactions: true,
            lru_k: 10,
            lru_sample_size: 7,
            server_enabled: false,
            server_host: "127.0.0.1".to_string(),
            server_port: 5432,
            max_connections: 100,
            connection_timeout: 30,
        }
    }
}

impl DBInstance {
    /// Creates and initializes a new database instance.
    ///
    /// This async constructor performs the full initialization sequence:
    /// 1. Checks for existing database/log files
    /// 2. Initializes storage (disk manager, buffer pool, LRU-K replacer)
    /// 3. Initializes recovery components (log manager)
    /// 4. Initializes transaction components (transaction manager, WAL)
    /// 5. Initializes execution engine
    /// 6. Runs crash recovery if applicable
    /// 7. Rebuilds catalog from system tables
    ///
    /// # Parameters
    /// - `config`: Configuration options for the database instance.
    ///
    /// # Returns
    /// A fully initialized `DBInstance` on success, or a [`DBError`] if
    /// initialization or recovery fails.
    ///
    /// # Recovery Behavior
    /// Recovery is triggered only when both database and log files exist
    /// and `enable_logging` is true in the configuration.
    pub async fn new(config: DBConfig) -> Result<Self, DBError> {
        // Check if database and log files already exist
        let db_file_exists = Path::new(&config.db_filename).exists();
        let log_file_exists = Path::new(&config.db_log_filename).exists();

        // Initialize disk components
        let disk_manager = AsyncDiskManager::new(
            config.db_filename.clone(),
            config.db_log_filename.clone(),
            DiskManagerConfig::default(),
        )
        .await?;

        let disk_manager_arc = Arc::new(disk_manager);

        // Initialize buffer pool
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(
            config.buffer_pool_size,
            disk_manager_arc.clone(),
            Arc::new(RwLock::new(LRUKReplacer::new(
                config.lru_sample_size,
                config.lru_k,
            ))),
        )?);

        // Initialize recovery components
        let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager_arc.clone())));
        log_manager.write().run_flush_thread();

        // Single shared transaction manager for catalog + factory
        let transaction_manager = Arc::new(TransactionManager::new());

        // Initialize catalog with default values
        let catalog = Arc::new(RwLock::new(Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
        )));

        // Initialize WAL manager
        let wal_manager = Arc::new(WALManager::new(log_manager.clone()));

        // Initialize transaction components reusing the same transaction manager
        let transaction_factory = Arc::new(TransactionManagerFactory::with_wal_manager_and_txn(
            buffer_pool_manager.clone(),
            wal_manager.clone(),
            transaction_manager.clone(),
        ));

        // Initialize execution engine
        let execution_engine = Arc::new(Mutex::new(ExecutionEngine::new(
            catalog.clone(),
            buffer_pool_manager.clone(),
            transaction_factory.clone(),
            wal_manager.clone(),
        )));

        // Create recovery manager and run recovery if needed
        let recovery_manager = if db_file_exists && log_file_exists && config.enable_logging {
            info!("Existing database and log files found, running recovery...");
            let recovery_manager = Arc::new(LogRecoveryManager::new(
                disk_manager_arc.clone(),
                log_manager.clone(),
                buffer_pool_manager.clone(),
            ));

            // Start recovery process
            if let Err(e) = recovery_manager.start_recovery().await {
                error!("Failed to recover database: {}", e);
                return Err(DBError::Recovery(format!("Recovery failed: {}", e)));
            }

            info!("Database recovery completed successfully");
            Some(recovery_manager)
        } else {
            info!("No existing database files or logging disabled, skipping recovery");
            None
        };

        // Rebuild in-memory catalog caches from persisted system catalog tables
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.rebuild_from_system_catalog();
        }

        Ok(Self {
            buffer_pool_manager,
            catalog,
            transaction_factory,
            execution_engine,
            log_manager,
            wal_manager,
            recovery_manager,
            config,
            writer: Arc::new(Mutex::new(CliResultWriter::new())),
            client_sessions: Arc::new(Mutex::new(HashMap::new())),
            debug_mode: false,
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            next_statement_id: Arc::new(Mutex::new(0)),
        })
    }

    /// Creates an execution context for query execution.
    ///
    /// The execution context bundles the buffer pool, catalog, and transaction
    /// context needed by executors during query processing.
    ///
    /// # Parameters
    /// - `txn`: The transaction under which queries will execute.
    ///
    /// # Returns
    /// An `Arc<ExecutionContext>` ready for use by the execution engine.
    pub fn make_executor_context(
        &self,
        txn: Arc<Transaction>,
    ) -> Result<Arc<ExecutionContext>, DBError> {
        Ok(Arc::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            Arc::new(TransactionContext::new(
                txn,
                self.transaction_factory.get_lock_manager(),
                self.transaction_factory.get_transaction_manager(),
            )),
        )))
    }

    /// Executes a SQL statement with auto-commit semantics.
    ///
    /// A new transaction is started, the SQL is executed, and the transaction
    /// is automatically committed on success or aborted on failure.
    ///
    /// # Parameters
    /// - `sql`: The SQL statement to execute.
    /// - `isolation_level`: Transaction isolation level for this statement.
    /// - `writer`: Output destination for query results.
    ///
    /// # Returns
    /// `Ok(true)` if execution and commit succeed, `Ok(false)` if execution
    /// succeeds but commit fails, or `Err(DBError)` on execution failure.
    ///
    /// # Note
    /// For explicit transaction control, use [`begin_transaction`](Self::begin_transaction)
    /// and [`execute_transaction`](Self::execute_transaction) instead.
    pub async fn execute_sql(
        &self,
        sql: &str,
        isolation_level: IsolationLevel,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        debug!(
            "Executing SQL with isolation level {:?}: {}",
            isolation_level, sql
        );
        let normalized_sql = sql.trim().to_lowercase();

        // Begin transaction through factory
        let txn_ctx = self.transaction_factory.begin_transaction(isolation_level);

        // Create executor context with transaction
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            txn_ctx.clone(),
        )));

        // Execute query
        let result = {
            let mut engine = self.execution_engine.lock();
            engine.execute_sql(sql, exec_ctx, writer).await
        };

        // Handle transaction completion
        match result {
            Ok(success) => {
                // Transaction control statements like ROLLBACK are already handled inside the
                // execution engine and should not be auto-committed/aborted here to avoid
                // double-finalizing the transaction.
                if normalized_sql.starts_with("rollback") {
                    return Ok(success);
                }

                if success {
                    if self
                        .transaction_factory
                        .commit_transaction(txn_ctx.clone())
                        .await
                    {
                        Ok(true)
                    } else {
                        self.transaction_factory.abort_transaction(txn_ctx);
                        Ok(false)
                    }
                } else {
                    self.transaction_factory.abort_transaction(txn_ctx);
                    Ok(false)
                }
            },
            Err(e) => {
                self.transaction_factory.abort_transaction(txn_ctx);
                Err(e)
            },
        }
    }

    /// Executes SQL within an existing transaction context.
    ///
    /// Unlike [`execute_sql`](Self::execute_sql), this method does not
    /// auto-commit. The caller is responsible for committing or aborting
    /// the transaction.
    ///
    /// # Parameters
    /// - `sql`: The SQL statement to execute.
    /// - `txn_ctx`: The transaction context to execute within.
    /// - `writer`: Output destination for query results.
    ///
    /// # Returns
    /// `Ok(true)` on success, `Ok(false)` on logical failure, or
    /// `Err(DBError)` on execution error.
    pub async fn execute_transaction(
        &self,
        sql: &str,
        txn_ctx: Arc<TransactionContext>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            txn_ctx.clone(),
        )));

        let mut engine = self.execution_engine.lock();
        engine.execute_sql(sql, exec_ctx, writer).await
    }

    /// Displays all tables in the current database.
    ///
    /// Outputs a table with columns: Table ID, Table Name, Schema, and Rows.
    ///
    /// # Parameters
    /// - `writer`: Output destination for the table listing.
    pub fn display_tables(&self, writer: &mut dyn ResultWriter) -> Result<(), DBError> {
        let catalog = self.catalog.read();
        let table_names = catalog.get_table_names();

        if table_names.is_empty() {
            println!("No tables found");
            return Ok(());
        }

        writer.write_schema_header(vec![
            "Table ID".to_string(),
            "Table Name".to_string(),
            "Schema".to_string(),
            "Rows".to_string(),
        ]);

        for name in table_names {
            if let Some(table_info) = catalog.get_table(&name) {
                let table_heap = table_info.get_table_heap();
                let _table_heap_guard = table_heap.latch.read();
                let schema = table_info.get_table_schema();
                let schema_str = schema
                    .get_columns()
                    .iter()
                    .map(|col| format!("{}({:?})", col.get_name(), col.get_type()))
                    .collect::<Vec<_>>()
                    .join(", ");

                writer.write_row(vec![
                    Value::new(table_info.get_table_oidt() as i32),
                    Value::new(table_info.get_table_name()),
                    Value::new(format!("({})", schema_str)),
                    Value::new(table_heap.get_num_tuples() as i32),
                ]);
            }
        }

        Ok(())
    }

    /// Returns schema information about a specific table.
    ///
    /// # Parameters
    /// - `table_name`: Name of the table to describe.
    ///
    /// # Returns
    /// A confirmation string after displaying the table schema.
    ///
    /// # Panics
    /// Panics if the table does not exist.
    pub fn get_table_info(&self, table_name: &str) -> Result<String, DBError> {
        let catalog = self.catalog.read();
        let table_info = catalog.get_table(table_name).unwrap();

        let mut writer = CliResultWriter::new();

        writer.write_schema_header(vec![
            "Column".to_string(),
            "Type".to_string(),
            // "Nullable".to_string()
        ]);

        for column in table_info.get_table_schema().get_columns() {
            writer.write_row(vec![
                Value::new(column.get_name()),
                Value::new(column.get_type()),
                // Value::new(column.is_nullable()),
            ]);
        }

        Ok("Table info displayed".to_string())
    }

    /// Returns a reference to the database configuration.
    pub fn get_config(&self) -> &DBConfig {
        &self.config
    }

    /// Returns a reference to the buffer pool manager.
    pub fn get_buffer_pool_manager(&self) -> &Arc<BufferPoolManager> {
        &self.buffer_pool_manager
    }

    /// Returns a reference to the catalog.
    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
    }

    /// Returns a reference to the transaction factory.
    pub fn get_transaction_factory(&self) -> &Arc<TransactionManagerFactory> {
        &self.transaction_factory
    }

    /// Begins a new transaction with the specified isolation level.
    ///
    /// # Parameters
    /// - `isolation_level`: The isolation level for the new transaction.
    ///
    /// # Returns
    /// A transaction context for use with [`execute_transaction`](Self::execute_transaction).
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
        self.transaction_factory.begin_transaction(isolation_level)
    }

    /// Commits the transaction with the specified ID.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID to commit.
    ///
    /// # Returns
    /// `Ok(())` on successful commit, or `Err(DBError)` if the transaction
    /// is not found or commit fails.
    pub async fn commit_transaction(&mut self, txn_id: u64) -> Result<(), DBError> {
        let txn_manager = self.transaction_factory.get_transaction_manager();

        let txn = txn_manager
            .get_transaction(&txn_id)
            .ok_or_else(|| DBError::Transaction(format!("Transaction {} not found", txn_id)))?;

        if !txn_manager
            .commit(txn, self.buffer_pool_manager.clone())
            .await
        {
            warn!("Transaction commit failed");
            return Err(DBError::Transaction(
                "Failed to commit transaction".to_string(),
            ));
        }

        Ok(())
    }

    /// Aborts the transaction with the specified ID.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID to abort.
    ///
    /// # Returns
    /// `Ok(())` on successful abort, or `Err(DBError)` if the transaction
    /// is not found.
    pub fn abort_transaction(&mut self, txn_id: u64) -> Result<(), DBError> {
        let txn_manager = self.transaction_factory.get_transaction_manager();

        let txn = txn_manager
            .get_transaction(&txn_id)
            .ok_or_else(|| DBError::Transaction(format!("Transaction {} not found", txn_id)))?;

        txn_manager.abort(txn);
        Ok(())
    }

    /// Handles a client request in server mode.
    ///
    /// Routes the request to the appropriate handler based on request type:
    /// - `Query`: Execute SQL with auto-commit or within active transaction
    /// - `BeginTransaction`: Start explicit transaction
    /// - `Commit`: Commit active transaction
    /// - `Rollback`: Abort active transaction
    /// - `Prepare`: Create prepared statement
    /// - `Execute`: Execute prepared statement with parameters
    /// - `Close`: Deallocate prepared statement
    ///
    /// # Parameters
    /// - `query`: The client request to process.
    /// - `client_id`: The client's session identifier.
    ///
    /// # Returns
    /// A `DatabaseResponse` containing results or error information.
    pub async fn handle_network_query(
        &self,
        query: DatabaseRequest,
        client_id: u64,
    ) -> Result<DatabaseResponse, DBError> {
        match query {
            DatabaseRequest::Query(sql) => {
                // Extract session data first
                let (txn_ctx, has_current_transaction) = {
                    let mut sessions = self.client_sessions.lock();
                    let session = sessions.get_mut(&client_id).ok_or_else(|| {
                        DBError::Client(format!("No session found for client {}", client_id))
                    })?;

                    let txn_ctx = if let Some(txn) = &session.current_transaction {
                        txn.clone()
                    } else {
                        self.transaction_factory
                            .begin_transaction(session.isolation_level)
                    };
                    let has_current_transaction = session.current_transaction.is_some();
                    (txn_ctx, has_current_transaction)
                };

                let mut writer = NetworkResultWriter::new();

                // Create execution context
                let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                    self.buffer_pool_manager.clone(),
                    self.catalog.clone(),
                    txn_ctx.clone(),
                )));

                // Execute query
                let execution_result = {
                    let mut engine = self.execution_engine.lock();
                    engine.execute_sql(&sql, exec_ctx, &mut writer).await
                };

                match execution_result {
                    Ok(_) => {
                        if !has_current_transaction {
                            // Auto-commit if not in transaction
                            if !self
                                .transaction_factory
                                .commit_transaction(txn_ctx.clone())
                                .await
                            {
                                self.transaction_factory.abort_transaction(txn_ctx);
                                return Err(DBError::Execution(
                                    "Failed to commit transaction".to_string(),
                                ));
                            }
                        }
                        debug!("Query executed successfully");
                        Ok(DatabaseResponse::Results(writer.into_results()))
                    },
                    Err(e) => {
                        if !has_current_transaction {
                            self.transaction_factory.abort_transaction(txn_ctx);
                        }
                        Err(e)
                    },
                }
            },
            DatabaseRequest::BeginTransaction { isolation_level } => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_begin_transaction(session, isolation_level)
            },
            #[allow(clippy::await_holding_lock)]
            DatabaseRequest::Commit => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_commit(session).await
            },
            DatabaseRequest::Rollback => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_rollback(session)
            },
            #[allow(clippy::await_holding_lock)]
            DatabaseRequest::Prepare(sql) => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_sql_query(sql, session).await
            },
            #[allow(clippy::await_holding_lock)]
            DatabaseRequest::Execute { stmt_id, params } => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_execute_statement(stmt_id, params, session)
                    .await
            },
            DatabaseRequest::Close(stmt_id) => {
                let mut sessions = self.client_sessions.lock();
                let session = sessions.get_mut(&client_id).ok_or_else(|| {
                    DBError::Client(format!("No session found for client {}", client_id))
                })?;
                self.handle_close_statement(stmt_id, session)
            },
        }
    }

    /// Executes a SQL query for a client session.
    ///
    /// Uses the session's active transaction if present, otherwise auto-commits.
    async fn handle_sql_query(
        &self,
        sql: String,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        let mut writer = NetworkResultWriter::new();

        let result = if let Some(txn_ctx) = &session.current_transaction {
            // Execute within existing transaction
            self.execute_transaction(&sql, txn_ctx.clone(), &mut writer)
                .await
        } else {
            // Auto-commit transaction
            self.execute_sql(&sql, session.isolation_level, &mut writer)
                .await
        };

        match result {
            Ok(success) => {
                if success {
                    Ok(DatabaseResponse::Results(writer.into_results()))
                } else {
                    let error = "Query execution failed".to_string();
                    if self.debug_mode {
                        error!("Client {}: {}", session.id, error);
                    }
                    Ok(DatabaseResponse::Error(error))
                }
            },
            Err(e) => {
                let error = format!("Query error: {}", e);
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            },
        }
    }

    /// Starts an explicit transaction for a client session.
    fn handle_begin_transaction(
        &self,
        session: &mut ClientSession,
        isolation_level: IsolationLevel,
    ) -> Result<DatabaseResponse, DBError> {
        if session.current_transaction.is_some() {
            return Ok(DatabaseResponse::Error(
                "Transaction already in progress".to_string(),
            ));
        }

        session.current_transaction = Some(self.begin_transaction(isolation_level));
        session.isolation_level = isolation_level;

        if self.debug_mode {
            info!(
                "Client {} started transaction with isolation level {:?}",
                session.id, isolation_level
            );
        }

        Ok(DatabaseResponse::Results(QueryResults::empty()))
    }

    /// Commits the active transaction for a client session.
    async fn handle_commit(
        &self,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        if let Some(txn_ctx) = session.current_transaction.take() {
            if self.transaction_factory.commit_transaction(txn_ctx).await {
                if self.debug_mode {
                    info!("Client {} committed transaction", session.id);
                }
                Ok(DatabaseResponse::Results(QueryResults::empty()))
            } else {
                let error = "Transaction commit failed".to_string();
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            }
        } else {
            Ok(DatabaseResponse::Error(
                "No transaction in progress".to_string(),
            ))
        }
    }

    /// Aborts the active transaction for a client session.
    fn handle_rollback(&self, session: &mut ClientSession) -> Result<DatabaseResponse, DBError> {
        if let Some(txn_ctx) = session.current_transaction.take() {
            self.transaction_factory.abort_transaction(txn_ctx);
            if self.debug_mode {
                info!("Client {} rolled back transaction", session.id);
            }
            Ok(DatabaseResponse::Results(QueryResults::empty()))
        } else {
            Ok(DatabaseResponse::Error(
                "No transaction in progress".to_string(),
            ))
        }
    }

    /// Enables or disables verbose debug logging for client operations.
    ///
    /// # Parameters
    /// - `enabled`: Whether to enable debug mode.
    pub fn set_debug_mode(&mut self, enabled: bool) {
        self.debug_mode = enabled;
    }

    /// Creates a new client session for server mode.
    ///
    /// # Parameters
    /// - `client_id`: Unique identifier for the client connection.
    pub fn create_client_session(&self, client_id: u64) {
        let session = ClientSession {
            id: client_id,
            current_transaction: None,
            isolation_level: IsolationLevel::ReadCommitted,
        };

        self.client_sessions.lock().insert(client_id, session);
        if self.debug_mode {
            info!("Created new session for client {}", client_id);
        }
    }

    /// Removes a client session and cleans up resources.
    ///
    /// Aborts any active transaction before removing the session.
    ///
    /// # Parameters
    /// - `client_id`: The client session to remove.
    pub fn remove_client_session(&self, client_id: u64) {
        let mut sessions = self.client_sessions.lock();
        if let Some(session) = sessions.remove(&client_id) {
            // Cleanup any active transaction
            if let Some(txn) = session.current_transaction {
                self.transaction_factory.abort_transaction(txn);
            }
        }
        if self.debug_mode {
            info!("Removed session for client {}", client_id);
        }
    }

    /// Executes a prepared statement with bound parameters.
    ///
    /// Validates parameter count and types before execution.
    async fn handle_execute_statement(
        &self,
        stmt_id: u64,
        params: Vec<Value>,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        // Get prepared statement
        let stmt = {
            let stmts = self.prepared_statements.lock();
            match stmts.get(&stmt_id) {
                Some(stmt) => stmt.clone(),
                None => {
                    return Ok(DatabaseResponse::Error(format!(
                        "Prepared statement {} not found",
                        stmt_id
                    )));
                },
            }
        };

        // Validate parameters
        if params.len() != stmt.parameter_types.len() {
            return Ok(DatabaseResponse::Error(format!(
                "Expected {} parameters, got {}",
                stmt.parameter_types.len(),
                params.len()
            )));
        }

        // Validate parameter types
        for (i, (param, expected_type)) in
            params.iter().zip(stmt.parameter_types.iter()).enumerate()
        {
            if param.get_type_id() != *expected_type {
                return Ok(DatabaseResponse::Error(format!(
                    "Parameter {} has wrong type: expected {:?}, got {:?}",
                    i + 1,
                    expected_type,
                    param.get_type_id()
                )));
            }
        }

        // Execute the statement with parameters
        let mut writer = NetworkResultWriter::new();
        let result = if let Some(txn_ctx) = &session.current_transaction {
            self.execute_prepared_statement(&stmt.sql, params, txn_ctx.clone(), &mut writer)
                .await
        } else {
            self.execute_prepared_statement_autocommit(
                &stmt.sql,
                params,
                session.isolation_level,
                &mut writer,
            )
            .await
        };

        match result {
            Ok(success) => {
                if success {
                    Ok(DatabaseResponse::Results(writer.into_results()))
                } else {
                    let error = "Statement execution failed".to_string();
                    if self.debug_mode {
                        error!("Client {}: {}", session.id, error);
                    }
                    Ok(DatabaseResponse::Error(error))
                }
            },
            Err(e) => {
                let error = format!("Statement execution error: {}", e);
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            },
        }
    }

    /// Deallocates a prepared statement.
    fn handle_close_statement(
        &self,
        stmt_id: u64,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        let removed = self.prepared_statements.lock().remove(&stmt_id).is_some();

        if self.debug_mode {
            if removed {
                info!("Client {} closed statement {}", session.id, stmt_id);
            } else {
                warn!(
                    "Client {} tried to close non-existent statement {}",
                    session.id, stmt_id
                );
            }
        }

        Ok(DatabaseResponse::Results(QueryResults::empty()))
    }

    /// Executes a prepared statement within an existing transaction.
    async fn execute_prepared_statement(
        &self,
        sql: &str,
        params: Vec<Value>,
        txn_ctx: Arc<TransactionContext>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            txn_ctx.clone(),
        )));

        let mut engine = self.execution_engine.lock();
        engine
            .execute_prepared_statement(sql, params, exec_ctx, writer)
            .await
    }

    /// Executes a prepared statement with auto-commit semantics.
    async fn execute_prepared_statement_autocommit(
        &self,
        sql: &str,
        params: Vec<Value>,
        isolation_level: IsolationLevel,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        let txn_ctx = self.transaction_factory.begin_transaction(isolation_level);

        let result = self
            .execute_prepared_statement(sql, params, txn_ctx.clone(), writer)
            .await;

        match result {
            Ok(success) => {
                if success {
                    if self.transaction_factory.commit_transaction(txn_ctx).await {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    self.transaction_factory.abort_transaction(txn_ctx);
                    Ok(false)
                }
            },
            Err(e) => {
                self.transaction_factory.abort_transaction(txn_ctx);
                Err(e)
            },
        }
    }

    /// Checks if database and log files exist at the configured paths.
    ///
    /// # Returns
    /// A tuple `(db_exists, log_exists)` indicating file presence.
    pub fn files_exist(&self) -> (bool, bool) {
        let db_file_exists = Path::new(&self.config.db_filename).exists();
        let log_file_exists = Path::new(&self.config.db_log_filename).exists();
        (db_file_exists, log_file_exists)
    }

    /// Returns the recovery manager if crash recovery was performed.
    ///
    /// Returns `None` for fresh databases or when logging is disabled.
    pub fn get_recovery_manager(&self) -> Option<&Arc<LogRecoveryManager>> {
        self.recovery_manager.as_ref()
    }

    /// Returns a reference to the log manager.
    pub fn get_log_manager(&self) -> &Arc<RwLock<LogManager>> {
        &self.log_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn _get_unique_path() -> String {
        let start = SystemTime::now();
        let since_epoch = start.duration_since(UNIX_EPOCH).unwrap();
        let timestamp =
            since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000;
        format!("test_{}", timestamp)
    }

    /// Helper to clean up test files
    fn _cleanup_files(config: &DBConfig) {
        let _ = fs::remove_file(&config.db_filename);
        let _ = fs::remove_file(&config.db_log_filename);
    }
}
