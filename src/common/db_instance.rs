use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::{CliResultWriter, ResultWriter};
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::recovery::checkpoint_manager::CheckpointManager;
use crate::recovery::log_manager::LogManager;
use crate::server::ServerHandle;
use crate::server::{DatabaseRequest, DatabaseResponse, QueryResults};
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::execution_engine::ExecutionEngine;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;
use log::error;
use log::{debug, info, warn};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

/// Represents a client's session with the database
#[derive(Debug)]
struct ClientSession {
    id: u64,
    current_transaction: Option<Arc<TransactionContext>>,
    isolation_level: IsolationLevel,
}

/// Configuration options for DB instance
#[derive(Debug, Clone)]
pub struct DBConfig {
    pub db_filename: String,
    pub db_log_filename: String,
    pub buffer_pool_size: usize,
    pub enable_logging: bool,
    pub enable_managed_transactions: bool,
    pub lru_k: usize,
    pub lru_sample_size: usize,
    pub server_enabled: bool,
    pub server_host: String,
    pub server_port: u16,
    pub max_connections: u32,
    pub connection_timeout: u64,
}

/// Main struct representing the DB database instance with generic disk manager type
#[derive(Clone)]
pub struct DBInstance {
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    transaction_factory: Arc<TransactionManagerFactory>,
    execution_engine: Arc<Mutex<ExecutionEngine>>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    config: DBConfig,
    writer: Arc<Mutex<dyn ResultWriter>>,
    client_sessions: Arc<Mutex<HashMap<u64, ClientSession>>>,
    debug_mode: bool,
    prepared_statements: Arc<Mutex<HashMap<u64, PreparedStatement>>>,
    next_statement_id: Arc<Mutex<u64>>,
}

/// Add new struct for prepared statements
#[derive(Clone)]
struct PreparedStatement {
    sql: String,
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
    /// Creates a new DB instance with the given configuration
    pub fn new(config: DBConfig) -> Result<Self, DBError> {
        // Initialize disk components
        let disk_manager = Arc::new(FileDiskManager::new(
            config.db_filename.clone(),
            config.db_log_filename.clone(),
            config.buffer_pool_size,
        ));

        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

        // Initialize buffer pool
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(
            config.buffer_pool_size,
            disk_scheduler,
            disk_manager.clone(),
            Arc::new(RwLock::new(LRUKReplacer::new(
                config.lru_sample_size,
                config.lru_k,
            ))),
        ));

        // Initialize catalog with default values
        let catalog = Arc::new(RwLock::new(Catalog::new(
            buffer_pool_manager.clone(), // Buffer pool manager
            0,                           // next_index_oid
            0,                           // next_table_oid
            HashMap::new(),              // tables
            HashMap::new(),              // indexes
            HashMap::new(),              // table_names
            HashMap::new(),              // index_names
        )));

        // Initialize recovery components
        let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));

        // Initialize transaction components
        let transaction_factory =
            Arc::new(TransactionManagerFactory::new(catalog.clone(), log_manager));

        // Initialize execution engine
        let execution_engine = Arc::new(Mutex::new(ExecutionEngine::new(
            catalog.clone(),
            buffer_pool_manager.clone(),
            transaction_factory.clone(),
        )));

        Ok(Self {
            buffer_pool_manager,
            catalog,
            transaction_factory,
            execution_engine,
            checkpoint_manager: None,
            config,
            writer: Arc::new(Mutex::new(CliResultWriter::new())),
            client_sessions: Arc::new(Mutex::new(HashMap::new())),
            debug_mode: false,
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            next_statement_id: Arc::new(Mutex::new(0)),
        })
    }

    /// Creates an executor context for query execution
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

    pub fn execute_sql(
        &self,
        sql: &str,
        isolation_level: IsolationLevel,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        debug!(
            "Executing SQL with isolation level {:?}: {}",
            isolation_level, sql
        );

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
            engine.execute_sql(sql, exec_ctx, writer)
        };

        // Handle transaction completion
        match result {
            Ok(success) => {
                if success {
                    if self.transaction_factory.commit_transaction(txn_ctx.clone()) {
                        Ok(true)
                    } else {
                        self.transaction_factory.abort_transaction(txn_ctx);
                        Ok(false)
                    }
                } else {
                    self.transaction_factory.abort_transaction(txn_ctx);
                    Ok(false)
                }
            }
            Err(e) => {
                self.transaction_factory.abort_transaction(txn_ctx);
                Err(e)
            }
        }
    }

    pub fn execute_transaction(
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
        engine.execute_sql(sql, exec_ctx, writer)
    }

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
                    Value::new(table_info.get_table_heap().get_num_tuples() as i32),
                ]);
            }
        }

        Ok(())
    }

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

    // Add back the getter methods
    pub fn get_config(&self) -> &DBConfig {
        &self.config
    }

    pub fn get_buffer_pool_manager(&self) -> &Arc<BufferPoolManager> {
        &self.buffer_pool_manager
    }

    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
    }

    pub fn get_transaction_factory(&self) -> &Arc<TransactionManagerFactory> {
        &self.transaction_factory
    }

    pub fn get_checkpoint_manager(&self) -> Option<&Arc<CheckpointManager>> {
        self.checkpoint_manager.as_ref()
    }

    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
        self.transaction_factory.begin_transaction(isolation_level)
    }

    pub fn commit_transaction(&mut self, txn_id: u64) -> Result<(), DBError> {
        let txn_manager = self.transaction_factory.get_transaction_manager();
        let mut txn_manager_guard = txn_manager.write();

        let txn = txn_manager_guard
            .get_transaction(&txn_id)
            .ok_or_else(|| DBError::Transaction(format!("Transaction {} not found", txn_id)))?;

        if !txn_manager_guard.commit(txn) {
            warn!("Transaction commit failed");
            return Err(DBError::Transaction(
                "Failed to commit transaction".to_string(),
            ));
        }

        Ok(())
    }

    pub fn abort_transaction(&mut self, txn_id: u64) -> Result<(), DBError> {
        let txn_manager = self.transaction_factory.get_transaction_manager();
        let mut txn_manager_guard = txn_manager.write();

        let txn = txn_manager_guard
            .get_transaction(&txn_id)
            .ok_or_else(|| DBError::Transaction(format!("Transaction {} not found", txn_id)))?;

        txn_manager_guard.abort(txn);
        Ok(())
    }

    /// Handles network queries
    pub async fn handle_network_query(
        &self,
        query: DatabaseRequest,
        client_id: u64,
    ) -> Result<DatabaseResponse, DBError> {
        let mut sessions = self.client_sessions.lock();
        let session = sessions
            .get_mut(&client_id)
            .ok_or_else(|| DBError::Other(format!("No session found for client {}", client_id)))?;

        match query {
            DatabaseRequest::Query(sql) => self.handle_sql_query(sql, session).await,
            DatabaseRequest::BeginTransaction { isolation_level } => {
                self.handle_begin_transaction(session, isolation_level)
            }
            DatabaseRequest::Commit => self.handle_commit(session),
            DatabaseRequest::Rollback => self.handle_rollback(session),
            DatabaseRequest::Prepare(sql) => self.handle_sql_query(sql, session).await,
            DatabaseRequest::Execute { stmt_id, params } => {
                self.handle_execute_statement(stmt_id, params, session)
                    .await
            }
            DatabaseRequest::Close(stmt_id) => self.handle_close_statement(stmt_id, session),
        }
    }

    async fn handle_sql_query(
        &self,
        sql: String,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        let mut writer = NetworkResultWriter::new();

        let result = if let Some(txn_ctx) = &session.current_transaction {
            // Execute within existing transaction
            self.execute_transaction(&sql, txn_ctx.clone(), &mut writer)
        } else {
            // Auto-commit transaction
            self.execute_sql(&sql, session.isolation_level, &mut writer)
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
            }
            Err(e) => {
                let error = format!("Query error: {}", e);
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            }
        }
    }

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

    fn handle_commit(&self, session: &mut ClientSession) -> Result<DatabaseResponse, DBError> {
        if let Some(txn_ctx) = session.current_transaction.take() {
            if self.transaction_factory.commit_transaction(txn_ctx) {
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

    // Private helper methods
    fn create_disk_manager(config: &DBConfig) -> Result<Arc<FileDiskManager>, DBError> {
        Ok(Arc::new(FileDiskManager::new(
            config.db_filename.clone(),
            config.db_log_filename.clone(),
            config.buffer_pool_size,
        )))
    }

    fn create_log_manager(
        disk_manager: &Arc<FileDiskManager>,
    ) -> Result<Option<Arc<RwLock<LogManager>>>, DBError> {
        Ok(if cfg!(not(feature = "disable-checkpoint-manager")) {
            Some(Arc::new(RwLock::new(LogManager::new(disk_manager.clone()))))
        } else {
            None
        })
    }

    fn create_buffer_pool_manager(
        config: &DBConfig,
        disk_manager: &Arc<FileDiskManager>,
    ) -> Result<Option<Arc<BufferPoolManager>>, DBError> {
        let replacer = LRUKReplacer::new(config.lru_sample_size, config.lru_k);
        let scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

        let bpm = BufferPoolManager::new(
            config.buffer_pool_size,
            scheduler,
            disk_manager.clone(),
            Arc::new(RwLock::new(replacer)),
        );

        Ok(Some(Arc::new(bpm)))
    }

    fn create_lock_manager(
        transaction_manager: &Arc<RwLock<TransactionManager>>,
    ) -> Result<Option<Arc<LockManager>>, DBError> {
        Ok(if cfg!(not(feature = "disable-lock-manager")) {
            let lock_manager = Arc::new(LockManager::new(Arc::clone(transaction_manager)));
            Some(lock_manager)
        } else {
            None
        })
    }

    fn create_transaction_manager(
        catalog: Arc<RwLock<Catalog>>,
        log_manager: Arc<RwLock<LogManager>>,
    ) -> Result<Option<Arc<RwLock<TransactionManager>>>, DBError> {
        Ok(if cfg!(not(feature = "disable-transaction-manager")) {
            let transaction_manager =
                Arc::new(RwLock::new(TransactionManager::new(catalog, log_manager)));
            Some(transaction_manager)
        } else {
            None
        })
    }

    /// Add method to enable/disable debug output
    pub fn set_debug_mode(&mut self, enabled: bool) {
        self.debug_mode = enabled;
    }

    /// Add session management methods
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

    async fn handle_prepare_statement(
        &self,
        sql: String,
        session: &mut ClientSession,
    ) -> Result<DatabaseResponse, DBError> {
        // Parse SQL to validate and get parameter types
        match self.execution_engine.lock().prepare_statement(&sql) {
            Ok(param_types) => {
                let stmt = PreparedStatement {
                    sql,
                    parameter_types: param_types,
                };

                let stmt_id = {
                    let mut id = self.next_statement_id.lock();
                    *id += 1;
                    *id
                };

                self.prepared_statements.lock().insert(stmt_id, stmt);

                if self.debug_mode {
                    info!("Client {} prepared statement {}", session.id, stmt_id);
                }

                Ok(DatabaseResponse::Results(QueryResults {
                    column_names: vec!["statement_id".to_string()],
                    rows: vec![vec![Value::new(stmt_id as i32)]],
                }))
            }
            Err(e) => {
                let error = format!("Failed to prepare statement: {}", e);
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            }
        }
    }

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
                    )))
                }
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
        } else {
            self.execute_prepared_statement_autocommit(
                &stmt.sql,
                params,
                session.isolation_level,
                &mut writer,
            )
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
            }
            Err(e) => {
                let error = format!("Statement execution error: {}", e);
                if self.debug_mode {
                    error!("Client {}: {}", session.id, error);
                }
                Ok(DatabaseResponse::Error(error))
            }
        }
    }

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

    fn execute_prepared_statement(
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
        engine.execute_prepared_statement(sql, params, exec_ctx, writer)
    }

    fn execute_prepared_statement_autocommit(
        &self,
        sql: &str,
        params: Vec<Value>,
        isolation_level: IsolationLevel,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        let txn_ctx = self.transaction_factory.begin_transaction(isolation_level);

        let result = self.execute_prepared_statement(sql, params, txn_ctx.clone(), writer);

        match result {
            Ok(success) => {
                if success {
                    if self.transaction_factory.commit_transaction(txn_ctx) {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    self.transaction_factory.abort_transaction(txn_ctx);
                    Ok(false)
                }
            }
            Err(e) => {
                self.transaction_factory.abort_transaction(txn_ctx);
                Err(e)
            }
        }
    }
}

// New result writer for network responses
struct NetworkResultWriter {
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl NetworkResultWriter {
    fn new() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn into_results(self) -> QueryResults {
        QueryResults {
            column_names: self.column_names,
            rows: self.rows,
        }
    }
}

impl ResultWriter for NetworkResultWriter {
    fn write_schema_header(&mut self, column_names: Vec<String>) {
        self.column_names = column_names;
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_message(&mut self, message: &str) {
        todo!()
    }
}

impl From<Box<dyn std::error::Error>> for DBError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        DBError::Other(error.to_string())
    }
}
