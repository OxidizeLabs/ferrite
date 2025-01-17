use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::{CliResultWriter, ResultWriter};
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_engine::ExecutionEngine;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::recovery::checkpoint_manager::CheckpointManager;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use log::{debug, info, warn};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use crate::types_db::value::Value;
use std::collections::HashMap;
use crate::sql::execution::execution_context::ExecutionContext;

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
}

/// Main struct representing the DB database instance with generic disk manager type
pub struct DBInstance {
    pub buffer_pool_manager: Arc<BufferPoolManager>,
    pub catalog: Arc<RwLock<Catalog>>,
    pub transaction_factory: Arc<TransactionManagerFactory>,
    execution_engine: Arc<Mutex<ExecutionEngine>>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    config: DBConfig,
    writer: Box<dyn ResultWriter>,
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
            buffer_pool_manager.clone(),  // Buffer pool manager
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
        let transaction_factory = Arc::new(TransactionManagerFactory::new(
            catalog.clone(),
            log_manager,
        ));

        // Initialize execution engine
        let execution_engine = Arc::new(Mutex::new(ExecutionEngine::new(
            catalog.clone(),
            buffer_pool_manager.clone(),
            transaction_factory.clone()
        )));

        Ok(Self {
            buffer_pool_manager,
            catalog,
            transaction_factory,
            execution_engine,
            checkpoint_manager: None,
            config,
            writer: Box::new(CliResultWriter::new()),
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
        debug!("Executing SQL with isolation level {:?}: {}", isolation_level, sql);

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
                let schema_str = schema.get_columns()
                    .iter()
                    .map(|col| format!("{}({:?})",
                        col.get_name(), 
                        col.get_type()
                    ))
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
        
        let txn = txn_manager_guard.get_transaction(&txn_id).ok_or_else(|| {
            DBError::Transaction(format!("Transaction {} not found", txn_id))
        })?;

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
        
        let txn = txn_manager_guard.get_transaction(&txn_id).ok_or_else(|| {
            DBError::Transaction(format!("Transaction {} not found", txn_id))
        })?;

        txn_manager_guard.abort(txn);
        Ok(())
    }
}
