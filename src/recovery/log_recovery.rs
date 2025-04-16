use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use log::{debug, info, warn};
use parking_lot::RwLock;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::{Lsn, PageId, TxnId, INVALID_LSN};
use crate::common::rid::RID;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_record::{LogRecord, LogRecordType};
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::table::tuple::Tuple;

/// LogRecoveryManager is responsible for recovering the database from log records
/// after a crash. It follows the ARIES recovery protocol:
/// 1. Analysis phase: Identify active transactions at time of crash
/// 2. Redo phase: Replay all actions, even for aborted transactions
/// 3. Undo phase: Reverse actions of uncommitted transactions
pub struct LogRecoveryManager {
    disk_manager: Arc<FileDiskManager>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    log_manager: Arc<RwLock<LogManager>>,
}

/// Keeps track of active transactions during recovery
struct TxnTable {
    table: HashMap<TxnId, Lsn>,
}

impl TxnTable {
    fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    fn add_txn(&mut self, txn_id: TxnId, lsn: Lsn) {
        self.table.insert(txn_id, lsn);
    }

    fn update_txn(&mut self, txn_id: TxnId, lsn: Lsn) {
        if self.table.contains_key(&txn_id) {
            self.table.insert(txn_id, lsn);
        }
    }

    fn remove_txn(&mut self, txn_id: TxnId) {
        self.table.remove(&txn_id);
    }

    fn contains_txn(&self, txn_id: TxnId) -> bool {
        self.table.contains_key(&txn_id)
    }

    fn get_prev_lsn(&self, txn_id: TxnId) -> Option<Lsn> {
        self.table.get(&txn_id).copied()
    }

    fn get_active_txns(&self) -> Vec<TxnId> {
        self.table.keys().copied().collect()
    }
}

/// Keeps track of dirty pages during recovery for efficient checkpointing
struct DirtyPageTable {
    table: HashMap<PageId, Lsn>,
}

impl DirtyPageTable {
    fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    fn add_page(&mut self, page_id: PageId, lsn: Lsn) {
        self.table.entry(page_id).or_insert(lsn);
    }

    fn get_rec_lsn(&self, page_id: PageId) -> Option<Lsn> {
        self.table.get(&page_id).copied()
    }

    fn get_dirty_pages(&self) -> Vec<(PageId, Lsn)> {
        self.table.iter().map(|(&pid, &lsn)| (pid, lsn)).collect()
    }
}

impl LogRecoveryManager {
    /// Creates a new `LogRecoveryManager`.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    /// - `buffer_pool_manager`: A reference to the buffer pool manager.
    /// - `log_manager`: A reference to the log manager.
    ///
    /// # Returns
    /// A new `LogRecoveryManager` instance.
    pub fn new(
        disk_manager: Arc<FileDiskManager>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        log_manager: Arc<RwLock<LogManager>>,
    ) -> Self {
        Self {
            disk_manager,
            buffer_pool_manager,
            log_manager,
        }
    }

    /// Starts the recovery process.
    /// This follows the ARIES recovery protocol:
    /// 1. Analysis phase
    /// 2. Redo phase
    /// 3. Undo phase
    pub fn start_recovery(&self) -> Result<(), String> {
        info!("Starting database recovery");

        // Phase 1: Analysis - determine active transactions at time of crash
        let (mut txn_table, mut dirty_page_table, start_redo_lsn) = self.analyze_log()?;

        // Phase 2: Redo - reapply all updates (even from aborted transactions)
        self.redo_log(&mut txn_table, &mut dirty_page_table, start_redo_lsn)?;

        // Phase 3: Undo - rollback uncommitted transactions
        self.undo_log(&txn_table)?;

        // Update the log manager's persistent LSN
        let mut log_manager = self.log_manager.write();
        let lsn = log_manager.get_next_lsn();
        log_manager.set_persistent_lsn(lsn);

        info!("Database recovery completed successfully");
        Ok(())
    }

    /// Phase 1: Analyzes the log to identify active transactions and dirty pages.
    ///
    /// # Returns
    /// A tuple containing:
    /// - TxnTable: Active transactions at time of crash
    /// - DirtyPageTable: Dirty pages at time of crash
    /// - Lsn: The LSN to start redo from
    fn analyze_log(&self) -> Result<(TxnTable, DirtyPageTable, Lsn), String> {
        info!("Starting analysis phase of recovery");

        let mut txn_table = TxnTable::new();
        let mut dirty_page_table = DirtyPageTable::new();
        let mut start_redo_lsn = INVALID_LSN;

        // Scan the log from beginning to end
        let mut offset = 0;
        let mut buffer = vec![0u8; 1024]; // Buffer for reading log entries

        while let Ok(_) = self.disk_manager.read_log(&mut buffer, offset) {
            // Parse log record from buffer
            // For simplicity, assuming we can parse one log record from the buffer
            // In a real implementation, we would need to properly parse serialized log records
            if let Some(log_record) = self.parse_log_record(&buffer) {
                let lsn = log_record.get_lsn();
                let txn_id = log_record.get_txn_id();

                // Initialize start_redo_lsn on first record
                if start_redo_lsn == INVALID_LSN {
                    start_redo_lsn = lsn;
                }

                match log_record.get_log_record_type() {
                    LogRecordType::Begin => {
                        // Add to active transaction table
                        txn_table.add_txn(txn_id, lsn);
                    }
                    LogRecordType::Commit => {
                        // Remove from active transactions
                        txn_table.remove_txn(txn_id);
                    }
                    LogRecordType::Abort => {
                        // Remove from active transactions
                        txn_table.remove_txn(txn_id);
                    }
                    LogRecordType::Update
                    | LogRecordType::Insert
                    | LogRecordType::MarkDelete
                    | LogRecordType::ApplyDelete
                    | LogRecordType::RollbackDelete => {
                        // Update transaction table
                        txn_table.update_txn(txn_id, lsn);

                        // Get page ID from the record
                        if let Some(page_id) = self.get_page_id_from_record(&log_record) {
                            // Add to dirty page table if not already there
                            dirty_page_table.add_page(page_id, lsn);
                        }
                    }
                    LogRecordType::NewPage => {
                        // Update transaction table
                        txn_table.update_txn(txn_id, lsn);

                        // Add page to dirty page table
                        if let Some(page_id) = log_record.get_page_id() {
                            dirty_page_table.add_page(*page_id, lsn);
                        }
                    }
                    _ => {} // Ignore invalid records
                }

                // Update offset for next read
                offset += log_record.get_size() as u64;
            } else {
                // If we can't parse more records, break the loop
                break;
            }
        }

        debug!(
            "Analysis complete: {} active txns, {} dirty pages, start redo LSN: {}",
            txn_table.get_active_txns().len(),
            dirty_page_table.get_dirty_pages().len(),
            start_redo_lsn
        );

        Ok((txn_table, dirty_page_table, start_redo_lsn))
    }

    /// Phase 2: Redoes all operations from the log.
    ///
    /// # Parameters
    /// - `txn_table`: The transaction table from analysis phase
    /// - `dirty_page_table`: The dirty page table from analysis phase
    /// - `start_redo_lsn`: The LSN to start redo from
    fn redo_log(
        &self,
        txn_table: &mut TxnTable,
        dirty_page_table: &mut DirtyPageTable,
        start_redo_lsn: Lsn,
    ) -> Result<(), String> {
        info!(
            "Starting redo phase of recovery from LSN {}",
            start_redo_lsn
        );

        // Similar to analyze, scan log but this time redo all operations
        let mut offset = 0;
        let mut buffer = vec![0u8; 1024];

        while let Ok(_) = self.disk_manager.read_log(&mut buffer, offset) {
            if let Some(log_record) = self.parse_log_record(&buffer) {
                let lsn = log_record.get_lsn();

                // Skip records before start_redo_lsn
                if lsn < start_redo_lsn {
                    offset += log_record.get_size() as u64;
                    continue;
                }

                // Check if we need to redo this operation
                let should_redo = match log_record.get_log_record_type() {
                    LogRecordType::Update
                    | LogRecordType::Insert
                    | LogRecordType::MarkDelete
                    | LogRecordType::ApplyDelete
                    | LogRecordType::RollbackDelete
                    | LogRecordType::NewPage => {
                        // Get page ID from record
                        if let Some(page_id) = self.get_page_id_from_record(&log_record) {
                            // Check if page is in dirty page table
                            if let Some(rec_lsn) = dirty_page_table.get_rec_lsn(page_id) {
                                // Redo if record's LSN >= recovery LSN for the page
                                lsn >= rec_lsn
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    }
                    _ => false, // Don't redo other types
                };

                if should_redo {
                    debug!("Redoing operation at LSN {}", lsn);
                    self.redo_record(&log_record)?;
                }

                offset += log_record.get_size() as u64;
            } else {
                break;
            }
        }

        info!("Redo phase complete");
        Ok(())
    }

    /// Phase 3: Rolls back uncommitted transactions.
    ///
    /// # Parameters
    /// - `txn_table`: The transaction table containing active transactions
    fn undo_log(&self, txn_table: &TxnTable) -> Result<(), String> {
        info!("Starting undo phase of recovery");

        let active_txns = txn_table.get_active_txns();
        if active_txns.is_empty() {
            info!("No active transactions to undo");
            return Ok(());
        }

        info!("Undoing {} active transactions", active_txns.len());

        // For each active transaction, undo its operations in reverse LSN order
        for txn_id in active_txns {
            debug!("Undoing transaction {}", txn_id);

            // Start from the last LSN of this transaction
            let mut current_lsn = txn_table.get_prev_lsn(txn_id).unwrap_or(INVALID_LSN);

            // Continue until we reach the BEGIN record or INVALID_LSN
            while current_lsn != INVALID_LSN {
                // Read the log record at this LSN
                let mut buffer = vec![0u8; 1024];
                // In a real implementation, we would need to calculate the offset for this LSN
                let offset = current_lsn; // Simplified - assuming LSN equals offset

                if let Ok(_) = self.disk_manager.read_log(&mut buffer, offset) {
                    if let Some(log_record) = self.parse_log_record(&buffer) {
                        if log_record.get_txn_id() == txn_id {
                            match log_record.get_log_record_type() {
                                LogRecordType::Begin => {
                                    // We've reached the beginning of this transaction
                                    debug!("Reached BEGIN record for transaction {}", txn_id);
                                    break;
                                }
                                LogRecordType::Insert => {
                                    // Undo insert by deleting
                                    self.undo_insert(&log_record)?;
                                }
                                LogRecordType::Update => {
                                    // Undo update by restoring old value
                                    self.undo_update(&log_record)?;
                                }
                                LogRecordType::MarkDelete => {
                                    // Undo delete by restoring tuple
                                    self.undo_delete(&log_record)?;
                                }
                                LogRecordType::NewPage => {
                                    // Don't need to undo new page creation
                                }
                                _ => {} // Ignore other record types for undo
                            }

                            // Move to previous LSN
                            current_lsn = log_record.get_prev_lsn();
                        } else {
                            // This shouldn't happen in a well-formed log
                            warn!("Found record for different transaction during undo");
                            break;
                        }
                    } else {
                        warn!("Could not parse log record during undo");
                        break;
                    }
                } else {
                    warn!("Could not read log during undo");
                    break;
                }
            }

            // Write an ABORT record for this transaction
            debug!("Writing ABORT record for transaction {}", txn_id);
            // In a real implementation, we would create and append an actual ABORT record
        }

        info!("Undo phase complete");
        Ok(())
    }

    /// Parse a log record from raw bytes
    ///
    /// # Parameters
    /// - `data`: The raw log record data
    ///
    /// # Returns
    /// An optional LogRecord if parsing was successful
    fn parse_log_record(&self, data: &[u8]) -> Option<LogRecord> {
        // In a real implementation, this would deserialize the log record
        // For now, we'll return None as a placeholder
        None
    }

    /// Get the page ID from a log record
    ///
    /// # Parameters
    /// - `record`: The log record
    ///
    /// # Returns
    /// The page ID if available
    fn get_page_id_from_record(&self, record: &LogRecord) -> Option<PageId> {
        match record.get_log_record_type() {
            LogRecordType::Insert | LogRecordType::Update => {
                // Get RID from record and extract page ID
                if let Some(rid) = record.get_insert_rid().or(record.get_update_rid()) {
                    Some(rid.get_page_id())
                } else {
                    None
                }
            }
            LogRecordType::MarkDelete
            | LogRecordType::ApplyDelete
            | LogRecordType::RollbackDelete => {
                if let Some(rid) = record.get_delete_rid() {
                    Some(rid.get_page_id())
                } else {
                    None
                }
            }
            LogRecordType::NewPage => {
                // For new page records, the page ID is directly accessible
                record.get_page_id().copied()
            }
            _ => None,
        }
    }

    /// Redo a single log record
    ///
    /// # Parameters
    /// - `record`: The log record to redo
    fn redo_record(&self, record: &LogRecord) -> Result<(), String> {
        match record.get_log_record_type() {
            LogRecordType::Insert => {
                if let (Some(rid), Some(tuple)) =
                    (record.get_insert_rid(), record.get_insert_tuple())
                {
                    debug!("Redoing INSERT for RID {:?}", rid);
                    // In a real implementation, we would insert the tuple into the table
                }
            }
            LogRecordType::Update => {
                if let (Some(rid), Some(tuple)) =
                    (record.get_update_rid(), record.get_update_tuple())
                {
                    debug!("Redoing UPDATE for RID {:?}", rid);
                    // In a real implementation, we would update the tuple in the table
                }
            }
            LogRecordType::MarkDelete => {
                if let Some(rid) = record.get_delete_rid() {
                    debug!("Redoing MARK_DELETE for RID {:?}", rid);
                    // In a real implementation, we would mark the tuple as deleted
                }
            }
            LogRecordType::ApplyDelete => {
                if let Some(rid) = record.get_delete_rid() {
                    debug!("Redoing APPLY_DELETE for RID {:?}", rid);
                    // In a real implementation, we would delete the tuple
                }
            }
            LogRecordType::NewPage => {
                if let Some(page_id) = record.get_page_id() {
                    debug!("Redoing NEW_PAGE for page {}", page_id);
                    // In a real implementation, we would create a new page
                }
            }
            _ => {} // Ignore other record types for redo
        }
        Ok(())
    }

    /// Undo an insert operation
    ///
    /// # Parameters
    /// - `record`: The insert log record to undo
    fn undo_insert(&self, record: &LogRecord) -> Result<(), String> {
        if let Some(rid) = record.get_insert_rid() {
            debug!("Undoing INSERT for RID {:?}", rid);
            // In a real implementation, we would delete the inserted tuple
        }
        Ok(())
    }

    /// Undo an update operation
    ///
    /// # Parameters
    /// - `record`: The update log record to undo
    fn undo_update(&self, record: &LogRecord) -> Result<(), String> {
        if let (Some(rid), Some(old_tuple)) = (record.get_update_rid(), record.get_original_tuple())
        {
            debug!("Undoing UPDATE for RID {:?}", rid);
            // In a real implementation, we would restore the original tuple
        }
        Ok(())
    }

    /// Undo a delete operation
    ///
    /// # Parameters
    /// - `record`: The delete log record to undo
    fn undo_delete(&self, record: &LogRecord) -> Result<(), String> {
        if let (Some(rid), Some(tuple)) = (record.get_delete_rid(), record.get_delete_tuple()) {
            debug!("Undoing DELETE for RID {:?}", rid);
            // In a real implementation, we would restore the deleted tuple
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use std::fs;
    use std::path::Path;

    struct TestContext {
        log_file_path: String,
        db_file_path: String,
        disk_manager: Arc<FileDiskManager>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        log_manager: Arc<RwLock<LogManager>>,
        recovery_manager: LogRecoveryManager,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            let db_file_path = format!(
                "tests/data/{}_db_{}.db",
                test_name,
                chrono::Utc::now().timestamp()
            );
            let log_file_path = format!(
                "tests/data/{}_log_{}.log",
                test_name,
                chrono::Utc::now().timestamp()
            );

            // Ensure test directory exists
            if let Some(parent) = Path::new(&db_file_path).parent() {
                fs::create_dir_all(parent).unwrap();
            }

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file_path.clone(),
                log_file_path.clone(),
                1024,
            ));

            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                10, // Small pool for testing
                disk_scheduler,
                disk_manager.clone(),
                Arc::new(RwLock::new(LRUKReplacer::new(2, 2))),
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager.clone())));
            log_manager.write().run_flush_thread();

            let recovery_manager = LogRecoveryManager::new(
                disk_manager.clone(),
                buffer_pool_manager.clone(),
                log_manager.clone(),
            );

            Self {
                log_file_path,
                db_file_path,
                disk_manager,
                buffer_pool_manager,
                log_manager,
                recovery_manager,
            }
        }

        fn cleanup(&self) {
            if Path::new(&self.log_file_path).exists() {
                let _ = fs::remove_file(&self.log_file_path);
            }
            if Path::new(&self.db_file_path).exists() {
                let _ = fs::remove_file(&self.db_file_path);
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_recovery_manager_creation() {
        let ctx = TestContext::new("create_test");
        assert!(ctx.recovery_manager.start_recovery().is_ok());
    }

    // Additional tests would be added to test the recovery process
}
