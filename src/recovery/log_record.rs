use std::fmt::Debug;
use std::mem::size_of;

use crate::common::config::{INVALID_LSN, Lsn, PageId, TxnId};
use crate::common::rid::RID;
use crate::storage::table::tuple::Tuple;

/// The type of the log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    Invalid = 0,
    Insert,
    MarkDelete,
    ApplyDelete,
    RollbackDelete,
    Update,
    Begin,
    Commit,
    Abort,
    NewPage,
}

/// For every write operation on the table page, you should write ahead a corresponding log record.
///
/// For EACH log record, HEADER is like (5 fields in common, 20 bytes in total).
///---------------------------------------------
/// | size | LSN | transID | prevLSN | LogType |
///---------------------------------------------
/// For insert type log record
///---------------------------------------------------------------
/// | HEADER | tuple_rid | tuple_size | tuple_data (byte array) |
///---------------------------------------------------------------
/// For delete type (including markdelete, rollbackdelete, applydelete)
///----------------------------------------------------------------
/// | HEADER | tuple_rid | tuple_size | tuple_data (byte array) |
///---------------------------------------------------------------
/// For update type log record
///-----------------------------------------------------------------------------------
/// | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
///-----------------------------------------------------------------------------------
/// For new page type log record
///--------------------------
/// | HEADER | prev_page_id | page_id |
///--------------------------
pub struct LogRecord {
    size: i32,
    lsn: Lsn,
    txn_id: TxnId,
    prev_lsn: Lsn,
    log_record_type: LogRecordType,

    // Fields for different types of log records
    delete_rid: Option<RID>,
    delete_tuple: Option<Tuple>,
    insert_rid: Option<RID>,
    insert_tuple: Option<Tuple>,
    update_rid: Option<RID>,
    old_tuple: Option<Tuple>,
    new_tuple: Option<Tuple>,
    prev_page_id: Option<PageId>,
    page_id: Option<PageId>,
}

impl LogRecord {
    const HEADER_SIZE: usize = 20;

    /// Creates a new `LogRecord` for transaction type (BEGIN/COMMIT/ABORT).
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous log sequence number.
    /// - `log_record_type`: The type of the log record.
    ///
    /// # Returns
    /// A new `LogRecord` instance.
    pub fn new_transaction_record(txn_id: TxnId, prev_lsn: Lsn, log_record_type: LogRecordType) -> Self {
        Self {
            size: Self::HEADER_SIZE as i32,
            lsn: INVALID_LSN,
            txn_id,
            prev_lsn,
            log_record_type,
            delete_rid: None,
            delete_tuple: None,
            insert_rid: None,
            insert_tuple: None,
            update_rid: None,
            old_tuple: None,
            new_tuple: None,
            prev_page_id: None,
            page_id: None,
        }
    }

    /// Creates a new `LogRecord` for INSERT/DELETE type.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous log sequence number.
    /// - `log_record_type`: The type of the log record.
    /// - `rid`: The row ID.
    /// - `tuple`: The tuple.
    ///
    /// # Returns
    /// A new `LogRecord` instance.
    pub fn new_insert_delete_record(txn_id: TxnId, prev_lsn: Lsn, log_record_type: LogRecordType, rid: RID, tuple: Tuple) -> Self {
        let size = Self::HEADER_SIZE as i32 + size_of::<RID>() as i32 + size_of::<i32>() as i32 + tuple.get_length() as i32;
        if log_record_type == LogRecordType::Insert {
            Self {
                size,
                lsn: INVALID_LSN,
                txn_id,
                prev_lsn,
                log_record_type,
                delete_rid: None,
                delete_tuple: None,
                insert_rid: Some(rid),
                insert_tuple: Some(tuple),
                update_rid: None,
                old_tuple: None,
                new_tuple: None,
                prev_page_id: None,
                page_id: None,
            }
        } else {
            assert!(
                log_record_type == LogRecordType::ApplyDelete
                    || log_record_type == LogRecordType::MarkDelete
                    || log_record_type == LogRecordType::RollbackDelete
            );
            Self {
                size,
                lsn: INVALID_LSN,
                txn_id,
                prev_lsn,
                log_record_type,
                delete_rid: Some(rid),
                delete_tuple: Some(tuple),
                insert_rid: None,
                insert_tuple: None,
                update_rid: None,
                old_tuple: None,
                new_tuple: None,
                prev_page_id: None,
                page_id: None,
            }
        }
    }

    /// Creates a new `LogRecord` for UPDATE type.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous log sequence number.
    /// - `log_record_type`: The type of the log record.
    /// - `update_rid`: The row ID of the updated row.
    /// - `old_tuple`: The old tuple.
    /// - `new_tuple`: The new tuple.
    ///
    /// # Returns
    /// A new `LogRecord` instance.
    pub fn new_update_record(txn_id: TxnId, prev_lsn: Lsn, log_record_type: LogRecordType, update_rid: RID, old_tuple: Tuple, new_tuple: Tuple) -> Self {
        let size = Self::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + old_tuple.get_length() as i32
            + new_tuple.get_length() as i32
            + 2 * size_of::<i32>() as i32;
        Self {
            size,
            lsn: INVALID_LSN,
            txn_id,
            prev_lsn,
            log_record_type,
            delete_rid: None,
            delete_tuple: None,
            insert_rid: None,
            insert_tuple: None,
            update_rid: Some(update_rid),
            old_tuple: Some(old_tuple),
            new_tuple: Some(new_tuple),
            prev_page_id: None,
            page_id: None,
        }
    }

    /// Creates a new `LogRecord` for NEWPAGE type.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous log sequence number.
    /// - `log_record_type`: The type of the log record.
    /// - `prev_page_id`: The previous page ID.
    /// - `page_id`: The new page ID.
    ///
    /// # Returns
    /// A new `LogRecord` instance.
    pub fn new_newpage_record(txn_id: TxnId, prev_lsn: Lsn, log_record_type: LogRecordType, prev_page_id: PageId, page_id: PageId) -> Self {
        let size = Self::HEADER_SIZE as i32 + 2 * size_of::<PageId>() as i32;
        Self {
            size,
            lsn: INVALID_LSN,
            txn_id,
            prev_lsn,
            log_record_type,
            delete_rid: None,
            delete_tuple: None,
            insert_rid: None,
            insert_tuple: None,
            update_rid: None,
            old_tuple: None,
            new_tuple: None,
            prev_page_id: Some(prev_page_id),
            page_id: Some(page_id),
        }
    }

    /// Returns the delete tuple.
    pub fn get_delete_tuple(&self) -> Option<&Tuple> {
        self.delete_tuple.as_ref()
    }

    /// Returns the delete RID.
    pub fn get_delete_rid(&self) -> Option<&RID> {
        self.delete_rid.as_ref()
    }

    /// Returns the insert tuple.
    pub fn get_insert_tuple(&self) -> Option<&Tuple> {
        self.insert_tuple.as_ref()
    }

    /// Returns the insert RID.
    pub fn get_insert_rid(&self) -> Option<&RID> {
        self.insert_rid.as_ref()
    }

    /// Returns the original tuple.
    pub fn get_original_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    /// Returns the update tuple.
    pub fn get_update_tuple(&self) -> Option<&Tuple> {
        self.new_tuple.as_ref()
    }

    /// Returns the update RID.
    pub fn get_update_rid(&self) -> Option<&RID> {
        self.update_rid.as_ref()
    }

    /// Returns the new page record's previous page ID.
    pub fn get_new_page_record(&self) -> Option<PageId> {
        self.prev_page_id
    }

    /// Returns the size of the log record.
    pub fn get_size(&self) -> i32 {
        self.size
    }

    /// Returns the log sequence number (LSN).
    pub fn get_lsn(&self) -> Lsn {
        self.lsn
    }

    /// Returns the transaction ID.
    pub fn get_txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Returns the previous log sequence number (LSN).
    pub fn get_prev_lsn(&self) -> Lsn {
        self.prev_lsn
    }

    /// Returns the log record type.
    pub fn get_log_record_type(&self) -> LogRecordType {
        self.log_record_type
    }

    /// Returns a string representation of the log record for debugging purposes.
    pub fn to_string(&self) -> String {
        format!(
            "Log[size:{}, LSN:{}, transID:{}, prevLSN:{}, LogType:{}]",
            self.size,
            self.lsn,
            self.txn_id,
            self.prev_lsn,
            self.log_record_type as i32,
        )
    }
}
