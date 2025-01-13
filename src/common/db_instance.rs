use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::CheckOptions;
use crate::execution::execution_engine::ExecutorEngine;
use crate::execution::executor_context::ExecutorContext;
use crate::recovery::checkpoint_manager::CheckpointManager;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use log::{debug, info, warn};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

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
    buffer_pool_manager: Option<Arc<BufferPoolManager>>,
    log_manager: Option<Arc<RwLock<LogManager>>>,
    transaction_manager: Option<Arc<RwLock<TransactionManager>>>,
    lock_manager: Option<Arc<LockManager>>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    catalog: Arc<RwLock<Catalog>>,
    execution_engine: Arc<Mutex<ExecutorEngine>>,
    config: DBConfig,
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
        let disk_manager = Self::create_disk_manager(&config)?;
        let log_manager = Self::create_log_manager(&disk_manager)?;

        let buffer_pool_manager = Self::create_buffer_pool_manager(&config, &disk_manager)?;

        let catalog = Arc::new(RwLock::new(Catalog::new(
            buffer_pool_manager
                .as_ref()
                .map(Arc::clone)
                .expect("Cannot create catalog without buffer pool manager"),
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )));

        let transaction_manager =
            Self::create_transaction_manager(catalog.clone(), log_manager.clone().unwrap())?;
        let lock_manager = Self::create_lock_manager(&transaction_manager.clone().unwrap())?;

        let execution_engine = Arc::new(Mutex::new(ExecutorEngine::new(
            Arc::clone(&catalog),
            buffer_pool_manager.clone().unwrap(),
            transaction_manager.clone().unwrap(),
            lock_manager.clone().unwrap()
        )));

        Ok(Self {
            buffer_pool_manager,
            log_manager,
            transaction_manager,
            lock_manager,
            checkpoint_manager: None,
            catalog,
            execution_engine,
            config,
        })
    }

    /// Creates an executor context for query execution
    pub fn make_executor_context(
        &self,
        txn: Arc<Transaction>,
    ) -> Result<Arc<ExecutorContext>, DBError> {
        let buffer_pool = self.buffer_pool_manager.as_ref().ok_or_else(|| {
            DBError::NotImplemented("Buffer pool manager not available".to_string())
        })?;

        let lock_manager = self
            .lock_manager
            .as_ref()
            .ok_or_else(|| DBError::NotImplemented("Lock manager not available".to_string()))?;

        Ok(Arc::new(ExecutorContext::new(
            txn,
            self.transaction_manager.clone().unwrap(),
            self.catalog.clone(),
            Arc::clone(buffer_pool),
            Arc::clone(lock_manager),
        )))
    }

    pub fn execute_sql_txn(
        &mut self,
        sql: &str,
        writer: &mut impl ResultWriter,
        txn: Arc<Transaction>,
        check_options: Option<CheckOptions>,
    ) -> Result<bool, DBError> {
        debug!(
            "Starting SQL transaction execution for transaction {}",
            txn.get_transaction_id()
        );

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutorContext::new(
            txn.clone(),
            self.transaction_manager.clone().unwrap(),
            self.catalog.clone(),
            self.buffer_pool_manager.clone().unwrap(),
            self.lock_manager.clone().unwrap(),
        )));

        debug!("Created execution context");

        // Configure check options if needed
        if let Some(opts) = check_options {
            debug!("Configuring check options");
            let mut ctx_guard = exec_ctx.write();
            ctx_guard.set_check_options(opts);
            ctx_guard.init_check_options();
            debug!("Check options configured");
        }

        // Execute the SQL statement
        let result = {
            let mut engine = self.execution_engine.lock();
            engine.execute_sql(sql, exec_ctx, writer)
        };

        match &result {
            Ok(_) => {
                debug!("SQL execution completed successfully");
            }
            Err(e) => {
                warn!("SQL execution failed: {:?}", e);
                txn.set_tainted();
            }
        }

        result
    }

    pub fn execute_sql(
        &mut self,
        sql: &str,
        writer: &mut impl ResultWriter,
        check_options: Option<CheckOptions>,
    ) -> Result<bool, DBError> {
        info!("Starting SQL execution: {}", sql);

        // Create new transaction
        let txn = {
            let txn_manager = self.transaction_manager.as_ref().unwrap();
            let mut txn_manager_guard = txn_manager.write();
            debug!("Creating new transaction");
            txn_manager_guard.begin(IsolationLevel::ReadUncommitted)
        };
        debug!("Created transaction {}", txn.get_transaction_id());

        // Execute in transaction context
        let result = self.execute_sql_txn(sql, writer, txn.clone(), check_options);

        match &result {
            Ok(_) => {
                debug!("Committing transaction {}", txn.get_transaction_id());
                let txn_manager = self.transaction_manager.as_ref().unwrap();
                let mut txn_manager_guard = txn_manager.write();
                if !txn_manager_guard.commit(txn.clone()) {
                    warn!("Transaction commit failed");
                    return Err(DBError::Transaction(
                        "Failed to commit transaction".to_string(),
                    ));
                }
                info!("Transaction committed successfully");
            }
            Err(e) => {
                warn!(
                    "Rolling back transaction {} due to error: {:?}",
                    txn.get_transaction_id(),
                    e
                );
                let txn_manager = self.transaction_manager.as_ref().unwrap();
                let mut txn_manager_guard = txn_manager.write();
                txn_manager_guard.abort(txn.clone());
                info!("Transaction rolled back");
            }
        }

        result
    }

    pub fn handle_cmd_display_tables(&self, writer: &mut impl ResultWriter) -> Result<(), DBError> {
        let catalog = self.catalog.read();
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

    pub fn get_config(&self) -> &DBConfig {
        &self.config
    }

    pub fn get_buffer_pool_manager(&self) -> Option<&Arc<BufferPoolManager>> {
        self.buffer_pool_manager.as_ref()
    }

    pub fn get_log_manager(&self) -> Option<&Arc<RwLock<LogManager>>> {
        self.log_manager.as_ref()
    }

    pub fn get_checkpoint_manager(&self) -> Option<&Arc<CheckpointManager>> {
        self.checkpoint_manager.as_ref()
    }

    pub fn get_lock_manager(&self) -> Option<&Arc<LockManager>> {
        self.lock_manager.as_ref()
    }

    pub fn get_transaction_manager(&self) -> Option<&Arc<RwLock<TransactionManager>>> {
        self.transaction_manager.as_ref()
    }

    pub fn get_catalog(&self) -> &Arc<RwLock<Catalog>> {
        &self.catalog
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
}
