use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalogue::catalogue::Catalog;
use crate::common::exception::DBError;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::CheckOptions;
use crate::execution::execution_engine::ExecutorEngine;
use crate::execution::executor_context::ExecutorContext;
use crate::recovery::checkpoint_manager::CheckpointManager;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_manager_memory::DiskManagerMemory;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Trait for writing query results in a tabular format
pub trait ResultWriter {
    fn begin_table(&mut self, bordered: bool);
    fn end_table(&mut self);
    fn begin_header(&mut self);
    fn end_header(&mut self);
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn write_cell(&mut self, content: &str);
    fn write_header_cell(&mut self, content: &str);
    fn one_cell(&mut self, content: &str);
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
}

/// Main struct representing the DB database instance with generic disk manager type
pub struct DBInstance {
    disk_manager: Arc<FileDiskManager>,
    buffer_pool_manager: Option<Arc<BufferPoolManager>>,
    log_manager: Option<Arc<LogManager>>,
    transaction_manager: Arc<TransactionManager>,
    lock_manager: Option<Arc<LockManager>>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    catalog: Arc<Catalog>,
    execution_engine: Arc<ExecutorEngine>,
    config: DBConfig,
    current_txn: Option<Arc<Mutex<Transaction>>>,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            db_filename: "test.db".to_string(),
            db_log_filename: "test.log".to_string(),
            buffer_pool_size: 1024,
            enable_logging: false,
            enable_managed_transactions: false,
            lru_k: 10,
            lru_sample_size: 7,
        }
    }
}

impl DBInstance {
    /// Creates a new DB instance with the given configuration
    pub fn new(config: DBConfig) -> Result<Self, DBError> {
        let disk_manager = Self::create_disk_manager(&config)?;
        let log_manager = Self::create_log_manager(&disk_manager)?;
        let buffer_pool_manager = Self::create_buffer_pool_manager(&config, &disk_manager)?;

        let catalog = Arc::new(Catalog::new(
            buffer_pool_manager
                .as_ref()
                .map(Arc::clone)
                .expect("REASON"),
            log_manager.clone().unwrap(),
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        ));

        let transaction_manager = Arc::new(TransactionManager::new(catalog.clone()));

        let execution_engine = Arc::new(ExecutorEngine::new(
            buffer_pool_manager.clone().unwrap(),
            Arc::clone(&catalog),
        ));

        Ok(Self {
            disk_manager,
            buffer_pool_manager,
            log_manager,
            transaction_manager,
            lock_manager: None,
            checkpoint_manager: None,
            catalog,
            execution_engine,
            config,
            current_txn: None,
        })
    }

    /// Creates an executor context for query execution
    pub fn make_executor_context(&self, txn: Transaction) -> Result<ExecutorContext, DBError> {
        let buffer_pool = self.buffer_pool_manager.as_ref().ok_or_else(|| {
            DBError::NotImplemented("Buffer pool manager not available".to_string())
        })?;

        let lock_manager = self
            .lock_manager
            .as_ref()
            .ok_or_else(|| DBError::NotImplemented("Lock manager not available".to_string()))?;

        let catalogue = self.catalog.clone();

        Ok(ExecutorContext::new(
            txn,
            self.transaction_manager.clone(),
            catalogue,
            Arc::clone(buffer_pool),
            Arc::clone(lock_manager),
        ))
    }

    pub fn execute_sql_txn(
        &self,
        sql: &str,
        writer: &mut impl ResultWriter,
        txn: Arc<Mutex<Transaction>>,
        check_options: Option<CheckOptions>,
    ) -> Result<bool, DBError> {
        // Ensure required components are available
        let buffer_pool = self.buffer_pool_manager.as_ref().ok_or_else(|| {
            DBError::NotImplemented("Buffer pool manager not available".to_string())
        })?;

        let lock_manager = self
            .lock_manager
            .as_ref()
            .ok_or_else(|| DBError::NotImplemented("Lock manager not available".to_string()))?;

        // Create execution context
        let txn_guard = txn.lock();
        let mut executor_context = ExecutorContext::new(
            Transaction::new(txn_guard.txn_id(), txn_guard.isolation_level()),
            self.transaction_manager.clone(),
            self.catalog.clone(),
            buffer_pool.clone(),
            lock_manager.clone(),
        );

        // Set and initialize check options if provided
        if let Some(opts) = check_options {
            executor_context.set_check_options(opts);
            executor_context.init_check_options();
        }

        drop(txn_guard);

        let check_options = executor_context.get_check_options();
        // Parse and execute the SQL statement
        let plan = match self.execution_engine.prepare_statement(sql, check_options) {
            Ok(p) => p,
            Err(e) => {
                txn.lock().set_tainted();
                return Err(e);
            }
        };

        // Execute the plan
        let result = self
            .execution_engine
            .execute_statement(&plan, executor_context, writer);

        // Handle execution result
        match result {
            Ok(has_results) => {
                let txn_guard = txn.lock();
                match txn_guard.get_state() {
                    TransactionState::Running => Ok(has_results),
                    TransactionState::Tainted => {
                        Err(DBError::Transaction("Transaction is tainted".to_string()))
                    }
                    TransactionState::Aborted => {
                        Err(DBError::Transaction("Transaction was aborted".to_string()))
                    }
                    TransactionState::Committed => Err(DBError::Transaction(
                        "Transaction already committed".to_string(),
                    )),
                }
            }
            Err(e) => {
                txn.lock().set_tainted();
                Err(e)
            }
        }
    }

    pub fn execute_sql(
        &mut self,
        sql: &str,
        writer: &mut impl ResultWriter,
        check_options: Option<CheckOptions>,
    ) -> Result<bool, DBError> {
        let is_local_txn = self.current_txn.is_some();
        let txn = match is_local_txn {
            true => Arc::clone(self.current_txn.as_ref().unwrap()),
            false => {
                // Get mutable reference to transaction manager for begin
                let transaction_manager =
                    Arc::get_mut(&mut self.transaction_manager).ok_or_else(|| {
                        DBError::Transaction(
                            "Failed to get mutable reference to transaction manager".to_string(),
                        )
                    })?;
                transaction_manager.begin(IsolationLevel::ReadUncommitted)
            }
        };

        let result = self.execute_sql_txn(sql, writer, txn.clone(), check_options);

        match result {
            Ok(success) => {
                if !is_local_txn {
                    // Get mutable reference for commit
                    let transaction_manager = Arc::get_mut(&mut self.transaction_manager)
                        .ok_or_else(|| {
                            DBError::Transaction(
                                "Failed to get mutable reference to transaction manager"
                                    .to_string(),
                            )
                        })?;
                    if !transaction_manager.commit(txn) {
                        return Err(DBError::Transaction(
                            "Failed to commit transaction".to_string(),
                        ));
                    }
                }
                Ok(success)
            }
            Err(e) => {
                // Get mutable reference for abort
                if let Some(transaction_manager) = Arc::get_mut(&mut self.transaction_manager) {
                    transaction_manager.abort(txn);
                }
                self.current_txn = None;
                Err(e)
            }
        }
    }

    // Command handlers
    pub fn handle_cmd_display_tables(&self, writer: &mut impl ResultWriter) -> Result<(), DBError> {
        let catalog = self.catalog.clone();
        let table_names = catalog.get_table_names();

        writer.begin_table(false);
        writer.begin_header();
        writer.write_header_cell("oid");
        writer.write_header_cell("name");
        writer.write_header_cell("cols");
        writer.end_header();

        for name in table_names {
            writer.begin_row();
            if let Some(table_info) = catalog.get_table(&name) {
                writer.write_cell(&table_info.get_table_oidt().to_string());
                writer.write_cell(&table_info.get_table_name());
                writer.write_cell(&table_info.get_table_schema().to_string(false));
            }
            writer.end_row();
        }
        writer.end_table();
        Ok(())
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
    ) -> Result<Option<Arc<LogManager>>, DBError> {
        Ok(if cfg!(not(feature = "disable-checkpoint-manager")) {
            Some(Arc::new(LogManager::new(disk_manager.clone())))
        } else {
            None
        })
    }

    fn create_buffer_pool_manager(
        config: &DBConfig,
        disk_manager: &Arc<FileDiskManager>,
    ) -> Result<Option<Arc<BufferPoolManager>>, DBError> {
        let replacer = LRUKReplacer::new(config.lru_k, config.lru_sample_size);
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
        transaction_manager: &Arc<TransactionManager>,
    ) -> Result<Option<Arc<LockManager>>, DBError> {
        Ok(if cfg!(not(feature = "disable-lock-manager")) {
            let lock_manager = Arc::new(LockManager::new(Arc::clone(transaction_manager)));
            Some(lock_manager)
        } else {
            None
        })
    }

    fn create_checkpoint_manager(
        transaction_manager: Arc<TransactionManager>,
        log_manager: Arc<LogManager>,
        buffer_pool_manager: Arc<BufferPoolManager>,
    ) -> Result<Option<Arc<CheckpointManager>>, DBError> {
        Ok(if cfg!(not(feature = "disable-checkpoint-manager")) {
            let checkpoint_manager = Arc::new(CheckpointManager::new(
                transaction_manager,
                log_manager,
                buffer_pool_manager,
            ));
            Some(checkpoint_manager)
        } else {
            None
        })
    }
}
