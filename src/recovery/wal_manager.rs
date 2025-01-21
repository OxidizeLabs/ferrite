use crate::common::config::INVALID_LSN;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_record::{LogRecord, LogRecordType};
use crate::storage::table::tuple::Tuple;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct WALManager {
    log_manager: Arc<RwLock<LogManager>>,
}

impl WALManager {
    pub fn new(log_manager: Arc<RwLock<LogManager>>) -> Self {
        Self { log_manager }
    }

    pub fn write_commit_record(&self, txn: &Transaction) -> u64 {
        let commit_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Commit,
        );
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(&commit_record)
    }

    pub fn write_abort_record(&self, txn: &Transaction) -> u64 {
        let abort_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Abort,
        );
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(&abort_record)
    }

    pub fn write_begin_record(&self, txn: &Transaction) -> u64 {
        let begin_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            INVALID_LSN,
            LogRecordType::Begin,
        );
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(&begin_record)
    }

    pub fn write_update_record(
        &self,
        txn: &Transaction,
        rid: RID,
        old_tuple: Tuple,
        new_tuple: Tuple,
    ) -> u64 {
        let update_record = LogRecord::new_update_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Update,
            rid,
            old_tuple,
            new_tuple,
        );
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(&update_record)
    }
}
