//! # Log Record
//!
//! This module defines the `LogRecord` structure used for Write-Ahead Logging (WAL).
//! Log records capture database operations for crash recovery, enabling the system
//! to redo committed transactions and undo uncommitted ones after a failure.
//!
//! ## Architecture
//!
//! ```text
//!                        ┌──────────────────────────────────────────────────────┐
//!                        │                    LogRecord                         │
//!                        │                                                      │
//!                        │  ┌────────────────────────────────────────────────┐  │
//!                        │  │                    Header                      │  │
//!                        │  │  ┌──────────┬──────────┬──────────┬─────────┐  │  │
//!                        │  │  │  size    │   lsn    │  txn_id  │prev_lsn │  │  │
//!                        │  │  │  (i32)   │(AtomicU64│  (u64)   │  (u64)  │  │  │
//!                        │  │  └──────────┴──────────┴──────────┴─────────┘  │  │
//!                        │  │  ┌────────────────────────────────────────┐    │  │
//!                        │  │  │           log_record_type              │    │  │
//!                        │  │  │  (Begin|Commit|Abort|Insert|Update|...)│    │  │
//!                        │  │  └────────────────────────────────────────┘    │  │
//!                        │  └────────────────────────────────────────────────┘  │
//!                        │                                                      │
//!                        │  ┌────────────────────────────────────────────────┐  │
//!                        │  │              Payload (type-dependent)          │  │
//!                        │  │                                                │  │
//!                        │  │  Insert/Delete:  rid + tuple                   │  │
//!                        │  │  Update:         rid + old_tuple + new_tuple   │  │
//!                        │  │  NewPage:        prev_page_id + page_id        │  │
//!                        │  │  Transaction:    (no additional payload)       │  │
//!                        │  └────────────────────────────────────────────────┘  │
//!                        └──────────────────────────────────────────────────────┘
//! ```
//!
//! ## Log Record Types
//!
//! | Type | Category | Description |
//! |------|----------|-------------|
//! | `Begin` | Transaction | Transaction started |
//! | `Commit` | Transaction | Transaction committed (durability point) |
//! | `Abort` | Transaction | Transaction aborted |
//! | `Insert` | Data | Tuple inserted |
//! | `MarkDelete` | Data | Tuple marked for deletion |
//! | `ApplyDelete` | Data | Deletion finalized |
//! | `RollbackDelete` | Data | Deletion rolled back |
//! | `Update` | Data | Tuple updated (old → new) |
//! | `NewPage` | Structure | New page allocated |
//!
//! ## LSN Chain (Per-Transaction)
//!
//! ```text
//!   Transaction T1 Log Chain
//!   ═════════════════════════════════════════════════════════════════▶
//!
//!   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//!   │   BEGIN     │     │   INSERT    │     │   UPDATE    │     │   COMMIT    │
//!   │   lsn=10    │◀────│   lsn=25    │◀────│   lsn=40    │◀────│   lsn=55    │
//!   │   prev=∅    │     │   prev=10   │     │   prev=25   │     │   prev=40   │
//!   └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
//!
//!   Recovery: Follow prev_lsn chain backward to undo uncommitted operations
//! ```
//!
//! ## Record Layout by Type
//!
//! ```text
//!   Transaction Record (Begin/Commit/Abort):
//!   ┌────────────────────────────────────────┐
//!   │ Header: size | lsn | txn_id | prev_lsn │
//!   │ Type: Begin/Commit/Abort               │
//!   └────────────────────────────────────────┘
//!
//!   Insert/Delete Record:
//!   ┌────────────────────────────────────────┐
//!   │ Header: size | lsn | txn_id | prev_lsn │
//!   │ Type: Insert/MarkDelete/ApplyDelete/...│
//!   │ Payload: rid + tuple                   │
//!   └────────────────────────────────────────┘
//!
//!   Update Record:
//!   ┌────────────────────────────────────────┐
//!   │ Header: size | lsn | txn_id | prev_lsn │
//!   │ Type: Update                           │
//!   │ Payload: rid + old_tuple + new_tuple   │
//!   └────────────────────────────────────────┘
//!
//!   NewPage Record:
//!   ┌────────────────────────────────────────┐
//!   │ Header: size | lsn | txn_id | prev_lsn │
//!   │ Type: NewPage                          │
//!   │ Payload: prev_page_id + page_id        │
//!   └────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | `LogRecordType` | Enum identifying the operation type |
//! | `LogRecord` | Complete log record with header and payload |
//! | `lsn` | Log Sequence Number (unique, monotonically increasing) |
//! | `prev_lsn` | Links to previous record in same transaction |
//! | `size` | Total serialized size in bytes |
//!
//! ## Constructors
//!
//! | Constructor | For Types | Description |
//! |-------------|-----------|-------------|
//! | `new_transaction_record()` | Begin, Commit, Abort | Transaction control records |
//! | `new_insert_delete_record()` | Insert, *Delete | Tuple insert/delete operations |
//! | `new_update_record()` | Update | Tuple update with before/after images |
//! | `new_page_record()` | NewPage | Page allocation tracking |
//!
//! ## Serialization
//!
//! Log records use `bincode` 2.0 for compact binary serialization:
//!
//! ```rust,ignore
//! // Serialize to WAL
//! let bytes = log_record.to_bytes()?;
//!
//! // Deserialize during recovery
//! let record = LogRecord::from_bytes(&bytes)?;
//! ```
//!
//! ## Recovery Usage
//!
//! During crash recovery, the recovery manager:
//! 1. **Analysis Phase**: Scan log to identify active transactions
//! 2. **Redo Phase**: Replay committed operations (use new_tuple for updates)
//! 3. **Undo Phase**: Rollback uncommitted operations (use old_tuple for updates)
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::recovery::log_record::{LogRecord, LogRecordType};
//!
//! // Create a begin record
//! let begin = LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);
//!
//! // Create an insert record
//! let insert = LogRecord::new_insert_delete_record(
//!     txn_id,
//!     begin.get_lsn(),
//!     LogRecordType::Insert,
//!     rid,
//!     Arc::new(tuple),
//! );
//!
//! // Create a commit record
//! let commit = LogRecord::new_transaction_record(
//!     txn_id,
//!     insert.get_lsn(),
//!     LogRecordType::Commit,
//! );
//!
//! // Serialize for WAL
//! let bytes = commit.to_bytes()?;
//! ```
//!
//! ## Thread Safety
//!
//! - `lsn` uses `AtomicU64` for lock-free updates
//! - Log records are immutable after creation (except LSN assignment)
//! - Implements `Encode`/`Decode` for bincode serialization

use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use log::error;

use crate::common::config::{storage_bincode_config, INVALID_LSN, Lsn, PageId, TxnId};
use crate::common::rid::RID;
use crate::storage::table::tuple::Tuple;
use bincode::{Decode, Encode};

/// The type of the log record.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
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

/// A log record is the unit of logging. It's used to log operations that need to be persisted.
///
/// Each log record has a header that consists of:
/// - size: The size of the log record in bytes, including the header.
/// - LSN: Log Sequence Number.
/// - txn_id: Transaction ID.
/// - prev_lsn: Previous LSN of the transaction.
/// - log_record_type: The type of the log record.
///
/// Based on the record type, different additional information is stored.
#[derive(Debug, Encode, Decode)]
pub struct LogRecord {
    size: i32,
    lsn: AtomicU64, // AtomicU64 is supported by bincode 2.0 with atomic feature
    txn_id: TxnId,
    prev_lsn: Lsn,
    log_record_type: LogRecordType,

    // Fields for different types of log records
    delete_rid: Option<RID>,
    delete_tuple: Option<Tuple>, // Store as Tuple for serialization
    insert_rid: Option<RID>,
    insert_tuple: Option<Tuple>, // Store as Tuple for serialization
    update_rid: Option<RID>,
    old_tuple: Option<Tuple>, // Store as Tuple for serialization
    new_tuple: Option<Tuple>, // Store as Tuple for serialization
    prev_page_id: Option<PageId>,
    page_id: Option<PageId>,
}

impl LogRecord {
    // Header fields: size (i32) + lsn (AtomicU64) + txn_id (TxnId) + prev_lsn (Lsn) + log_record_type (LogRecordType as i32)
    const HEADER_SIZE: usize = size_of::<i32>()
        + size_of::<AtomicU64>()
        + size_of::<TxnId>()
        + size_of::<Lsn>()
        + size_of::<LogRecordType>();

    fn refresh_size_from_encoding(&mut self) {
        match bincode::encode_to_vec(&*self, storage_bincode_config()) {
            Ok(bytes) => self.size = bytes.len() as i32,
            Err(e) => {
                // Encoding should be infallible for supported types; log and leave current size on error.
                error!("Failed to encode log record for size calculation: {}", e);
            }
        }
    }

    /// Creates a new transaction log record.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous LSN of the transaction.
    /// - `log_record_type`: The type of the transaction log record.
    ///
    /// # Returns
    /// A new transaction log record.
    pub fn new_transaction_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
    ) -> Self {
        let mut record = Self {
            size: 0,
            lsn: AtomicU64::new(INVALID_LSN),
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
        };

        record.refresh_size_from_encoding();
        record
    }

    /// Creates a new insert or delete log record.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous LSN of the transaction.
    /// - `log_record_type`: The type of the log record.
    /// - `rid`: The RID of the tuple being inserted or deleted.
    /// - `tuple`: The tuple being inserted or deleted.
    ///
    /// # Returns
    /// A new insert or delete log record.
    pub fn new_insert_delete_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        rid: RID,
        tuple_arc: Arc<Tuple>,
    ) -> Self {
        let tuple = (*tuple_arc).clone();

        let mut record = match log_record_type {
            LogRecordType::Insert => Self {
                size: 0,
                lsn: AtomicU64::new(INVALID_LSN),
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
            },
            LogRecordType::MarkDelete | LogRecordType::ApplyDelete | LogRecordType::RollbackDelete => Self {
                size: 0,
                lsn: AtomicU64::new(INVALID_LSN),
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
            },
            _ => Self {
                size: 0,
                lsn: AtomicU64::new(INVALID_LSN),
                txn_id,
                prev_lsn,
                log_record_type: LogRecordType::Invalid,
                delete_rid: None,
                delete_tuple: None,
                insert_rid: None,
                insert_tuple: None,
                update_rid: None,
                old_tuple: None,
                new_tuple: None,
                prev_page_id: None,
                page_id: None,
            },
        };

        record.refresh_size_from_encoding();
        record
    }

    /// Creates a new update log record.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous LSN of the transaction.
    /// - `log_record_type`: The type of the log record.
    /// - `update_rid`: The RID of the tuple being updated.
    /// - `old_tuple`: The old tuple.
    /// - `new_tuple`: The new tuple.
    ///
    /// # Returns
    /// A new update log record.
    pub fn new_update_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        update_rid: RID,
        old_tuple_arc: Arc<Tuple>,
        new_tuple_arc: Arc<Tuple>,
    ) -> Self {
        let old_tuple = (*old_tuple_arc).clone();
        let new_tuple = (*new_tuple_arc).clone();

        let mut record = Self {
            size: 0,
            lsn: AtomicU64::new(INVALID_LSN),
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
        };

        record.refresh_size_from_encoding();
        record
    }

    /// Creates a new page log record.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `prev_lsn`: The previous LSN of the transaction.
    /// - `log_record_type`: The type of the log record.
    /// - `prev_page_id`: The previous page ID.
    /// - `page_id`: The page ID.
    ///
    /// # Returns
    /// A new page log record.
    pub fn new_page_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        prev_page_id: PageId,
        page_id: PageId,
    ) -> Self {
        let mut record = Self {
            size: 0,
            lsn: AtomicU64::new(INVALID_LSN),
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
        };

        record.refresh_size_from_encoding();
        record
    }

    /// Gets the page ID.
    pub fn get_page_id(&self) -> Option<&PageId> {
        self.page_id.as_ref()
    }

    /// Gets the delete tuple.
    pub fn get_delete_tuple(&self) -> Option<&Tuple> {
        self.delete_tuple.as_ref()
    }

    /// Gets the delete RID.
    pub fn get_delete_rid(&self) -> Option<&RID> {
        self.delete_rid.as_ref()
    }

    /// Gets the insert tuple.
    pub fn get_insert_tuple(&self) -> Option<&Tuple> {
        self.insert_tuple.as_ref()
    }

    /// Gets the insert RID.
    pub fn get_insert_rid(&self) -> Option<&RID> {
        self.insert_rid.as_ref()
    }

    /// Gets the original tuple in an update record.
    pub fn get_original_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    /// Gets the new tuple in an update record.
    pub fn get_update_tuple(&self) -> Option<&Tuple> {
        self.new_tuple.as_ref()
    }

    /// Gets the update RID.
    pub fn get_update_rid(&self) -> Option<&RID> {
        self.update_rid.as_ref()
    }

    /// Gets the previous page ID in a new page record.
    pub fn get_new_page_record(&self) -> Option<PageId> {
        self.prev_page_id
    }

    /// Gets the size of the log record.
    pub fn get_size(&self) -> i32 {
        self.size
    }

    /// Gets the LSN of the log record.
    pub fn get_lsn(&self) -> Lsn {
        self.lsn.load(Ordering::SeqCst)
    }

    /// Sets the LSN of the log record.
    pub fn set_lsn(&self, lsn: Lsn) {
        self.lsn.store(lsn, Ordering::SeqCst);
    }

    /// Gets the transaction ID.
    pub fn get_txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Gets the previous LSN of the transaction.
    pub fn get_prev_lsn(&self) -> Lsn {
        self.prev_lsn
    }

    /// Gets the type of the log record.
    pub fn get_log_record_type(&self) -> LogRecordType {
        self.log_record_type
    }

    /// Gets a string representation of the log record.
    pub fn to_string(&self) -> String {
        format!(
            "Log[size:{}, LSN:{}, transID:{}, prevLSN:{}, LogType:{}]",
            self.size,
            self.lsn.load(Ordering::SeqCst),
            self.txn_id,
            self.prev_lsn,
            self.log_record_type as i32
        )
    }

    /// Checks if the log record is a commit record.
    pub fn is_commit(&self) -> bool {
        self.log_record_type == LogRecordType::Commit
    }

    /// Serializes the log record to bytes using bincode 2.0.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized log record, or an error if serialization fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::encode_to_vec(self, storage_bincode_config())
    }

    /// Deserializes a log record from bytes using bincode 2.0.
    ///
    /// # Parameters
    /// - `bytes`: The bytes to deserialize.
    ///
    /// # Returns
    /// A deserialized log record, or an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (record, _) = bincode::decode_from_slice(bytes, storage_bincode_config())?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    const DUMMY_TXN_ID: TxnId = 1;
    const DUMMY_PREV_LSN: Lsn = 0;
    const DUMMY_PAGE_ID: PageId = 1;
    const DUMMY_PREV_PAGE_ID: PageId = 0;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ])
    }

    fn create_test_tuple() -> Tuple {
        let schema = create_test_schema();
        let values = vec![Value::new(1), Value::new("test")];
        let rid = RID::new(1, 1);
        Tuple::new(&values, &schema, rid)
    }

    fn create_test_tuple_updated() -> Tuple {
        let schema = create_test_schema();
        let values = vec![Value::new(1), Value::new("updated")];
        let rid = RID::new(1, 1);
        Tuple::new(&values, &schema, rid)
    }

    #[test]
    fn test_transaction_record() {
        let record =
            LogRecord::new_transaction_record(DUMMY_TXN_ID, DUMMY_PREV_LSN, LogRecordType::Begin);

        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
        assert_eq!(record.get_txn_id(), DUMMY_TXN_ID);
        assert_eq!(record.get_prev_lsn(), DUMMY_PREV_LSN);
        assert_eq!(record.get_log_record_type(), LogRecordType::Begin);
        assert_eq!(record.get_lsn(), INVALID_LSN);
    }

    #[test]
    fn test_insert_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            Arc::new(tuple),
        );

        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
        assert_eq!(record.get_log_record_type(), LogRecordType::Insert);
        assert_eq!(record.get_insert_rid(), Some(&rid));
        assert!(record.get_insert_tuple().is_some());
        assert!(record.get_delete_rid().is_none());
        assert!(record.get_delete_tuple().is_none());
    }

    #[test]
    fn test_delete_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::MarkDelete,
            rid,
            Arc::new(tuple),
        );

        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
        assert_eq!(record.get_log_record_type(), LogRecordType::MarkDelete);
        assert_eq!(record.get_delete_rid(), Some(&rid));
        assert!(record.get_delete_tuple().is_some());
        assert!(record.get_insert_rid().is_none());
        assert!(record.get_insert_tuple().is_none());
    }

    #[test]
    fn test_update_record() {
        let old_tuple = create_test_tuple();
        let new_tuple = create_test_tuple_updated();
        let rid = old_tuple.get_rid();

        let record = LogRecord::new_update_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Update,
            rid,
            Arc::new(old_tuple),
            Arc::new(new_tuple),
        );

        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
        assert_eq!(record.get_log_record_type(), LogRecordType::Update);
        assert_eq!(record.get_update_rid(), Some(&rid));
        assert!(record.get_original_tuple().is_some());
        assert!(record.get_update_tuple().is_some());
    }

    #[test]
    fn test_new_page_record() {
        let record = LogRecord::new_page_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::NewPage,
            DUMMY_PREV_PAGE_ID,
            DUMMY_PAGE_ID,
        );

        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
        assert_eq!(record.get_log_record_type(), LogRecordType::NewPage);
        assert_eq!(record.get_new_page_record(), Some(DUMMY_PREV_PAGE_ID));
        assert_eq!(record.get_page_id(), Some(&DUMMY_PAGE_ID));
    }

    #[test]
    fn test_to_string() {
        let record =
            LogRecord::new_transaction_record(DUMMY_TXN_ID, DUMMY_PREV_LSN, LogRecordType::Begin);

        let expected_string = format!(
            "Log[size:{}, LSN:{}, transID:{}, prevLSN:{}, LogType:{}]",
            record.get_size(),
            INVALID_LSN,
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Begin as i32
        );

        assert_eq!(record.to_string(), expected_string);
    }

    #[test]
    fn test_invalid_record_type() {
        let record =
            LogRecord::new_transaction_record(DUMMY_TXN_ID, DUMMY_PREV_LSN, LogRecordType::Invalid);

        assert_eq!(record.get_log_record_type(), LogRecordType::Invalid);
        let encoded_len = record.to_bytes().unwrap().len() as i32;
        assert_eq!(record.get_size(), encoded_len);
    }

    #[test]
    fn test_rollback_delete_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::RollbackDelete,
            rid,
            Arc::new(tuple),
        );

        assert_eq!(record.get_log_record_type(), LogRecordType::RollbackDelete);
        assert_eq!(record.get_delete_rid(), Some(&rid));
        assert!(record.get_delete_tuple().is_some());
    }

    #[test]
    fn test_apply_delete_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::ApplyDelete,
            rid,
            Arc::new(tuple),
        );

        assert_eq!(record.get_log_record_type(), LogRecordType::ApplyDelete);
        assert_eq!(record.get_delete_rid(), Some(&rid));
        assert!(record.get_delete_tuple().is_some());
    }

    // Add to existing test setup functions
    fn create_large_test_tuple() -> Tuple {
        let schema = create_test_schema();
        let large_string = "a".repeat(1000);
        let values = vec![Value::new(1), Value::new(large_string)];
        let rid = RID::new(1, 1);
        Tuple::new(&values, &schema, rid)
    }

    #[test]
    fn test_size_calculations_with_large_tuples() {
        let large_tuple = create_large_test_tuple();
        let rid = large_tuple.get_rid();

        // Test insert record size
        let insert_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            Arc::new(large_tuple),
        );

        let encoded_len = insert_record.to_bytes().unwrap().len() as i32;
        assert_eq!(insert_record.get_size(), encoded_len);
    }

    #[test]
    fn test_sequential_update_records() {
        let tuple1 = Arc::new(create_test_tuple());
        let tuple2 = Arc::new(create_test_tuple_updated());
        let tuple3 = Arc::new(create_large_test_tuple());
        let rid = tuple1.get_rid();

        // Create chain of update records
        let record1 = LogRecord::new_update_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Update,
            rid,
            tuple1,
            tuple2.clone(),
        );

        let record2 = LogRecord::new_update_record(
            DUMMY_TXN_ID,
            record1.get_lsn(),
            LogRecordType::Update,
            rid,
            tuple2,
            tuple3,
        );

        assert_eq!(record2.get_prev_lsn(), record1.get_lsn());
        assert!(record2.get_original_tuple().is_some());
        assert!(record2.get_update_tuple().is_some());
    }

    #[test]
    fn test_commit_after_operations() {
        // Test insert followed by commit
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let insert_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            Arc::new(tuple),
        );

        let commit_record = LogRecord::new_transaction_record(
            DUMMY_TXN_ID,
            insert_record.get_lsn(),
            LogRecordType::Commit,
        );

        assert_eq!(commit_record.get_prev_lsn(), insert_record.get_lsn());
        assert_eq!(commit_record.get_txn_id(), insert_record.get_txn_id());
    }

    #[test]
    fn test_multiple_page_records() {
        let page_id1 = DUMMY_PAGE_ID;
        let page_id2 = DUMMY_PAGE_ID + 1;
        let page_id3 = DUMMY_PAGE_ID + 2;

        // Create chain of new page records
        let record1 = LogRecord::new_page_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::NewPage,
            page_id1,
            page_id2,
        );

        let record2 = LogRecord::new_page_record(
            DUMMY_TXN_ID,
            record1.get_lsn(),
            LogRecordType::NewPage,
            page_id2,
            page_id3,
        );

        assert_eq!(record1.get_page_id(), Some(&page_id2));
        assert_eq!(record2.get_page_id(), Some(&page_id3));
        assert_eq!(record2.get_new_page_record(), Some(page_id2));
    }

    #[test]
    fn test_transaction_abort_after_operations() {
        let tuple = Arc::new(create_test_tuple());
        let rid = tuple.get_rid();

        // Create sequence: Insert -> MarkDelete -> Abort
        let insert_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            tuple.clone(),
        );

        let delete_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            insert_record.get_lsn(),
            LogRecordType::MarkDelete,
            rid,
            tuple,
        );

        let abort_record = LogRecord::new_transaction_record(
            DUMMY_TXN_ID,
            delete_record.get_lsn(),
            LogRecordType::Abort,
        );

        assert_eq!(abort_record.get_prev_lsn(), delete_record.get_lsn());
        assert_eq!(abort_record.get_txn_id(), delete_record.get_txn_id());
    }

    #[test]
    fn test_record_type_transitions() {
        let tuple = Arc::new(create_test_tuple());
        let rid = tuple.get_rid();

        // Test valid sequence: Begin -> Insert -> MarkDelete -> ApplyDelete -> Commit
        let begin_record =
            LogRecord::new_transaction_record(DUMMY_TXN_ID, DUMMY_PREV_LSN, LogRecordType::Begin);

        let insert_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            begin_record.get_lsn(),
            LogRecordType::Insert,
            rid,
            tuple.clone(),
        );

        let mark_delete_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            insert_record.get_lsn(),
            LogRecordType::MarkDelete,
            rid,
            tuple.clone(),
        );

        let apply_delete_record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            mark_delete_record.get_lsn(),
            LogRecordType::ApplyDelete,
            rid,
            tuple,
        );

        let commit_record = LogRecord::new_transaction_record(
            DUMMY_TXN_ID,
            apply_delete_record.get_lsn(),
            LogRecordType::Commit,
        );

        assert_eq!(begin_record.get_log_record_type(), LogRecordType::Begin);
        assert_eq!(insert_record.get_log_record_type(), LogRecordType::Insert);
        assert_eq!(
            mark_delete_record.get_log_record_type(),
            LogRecordType::MarkDelete
        );
        assert_eq!(
            apply_delete_record.get_log_record_type(),
            LogRecordType::ApplyDelete
        );
        assert_eq!(commit_record.get_log_record_type(), LogRecordType::Commit);
    }

    #[test]
    fn test_arc_clone_consistency() {
        let tuple = create_test_tuple();
        let arc_tuple = Arc::new(tuple);
        let rid = arc_tuple.get_rid();

        // Create two log records sharing the same tuple
        let record1 = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            arc_tuple.clone(),
        );

        let record2 = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID + 1,
            DUMMY_PREV_LSN,
            LogRecordType::MarkDelete,
            rid,
            arc_tuple,
        );

        // Both records should preserve identical tuple contents
        let insert_tuple = record1.get_insert_tuple().unwrap();
        let delete_tuple = record2.get_delete_tuple().unwrap();
        assert_eq!(insert_tuple.get_rid(), delete_tuple.get_rid());
        assert_eq!(insert_tuple.get_values(), delete_tuple.get_values());
    }

    #[test]
    fn test_bincode_serialization_transaction_record() {
        let record =
            LogRecord::new_transaction_record(DUMMY_TXN_ID, DUMMY_PREV_LSN, LogRecordType::Begin);

        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();

        // Deserialize back to a record
        let deserialized_record = LogRecord::from_bytes(&bytes).unwrap();

        // Check that the deserialized record matches the original
        assert_eq!(deserialized_record.get_txn_id(), record.get_txn_id());
        assert_eq!(deserialized_record.get_prev_lsn(), record.get_prev_lsn());
        assert_eq!(
            deserialized_record.get_log_record_type(),
            record.get_log_record_type()
        );
        assert_eq!(deserialized_record.get_size(), record.get_size());
    }

    #[test]
    fn test_bincode_serialization_insert_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            Arc::new(tuple),
        );

        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();

        // Deserialize back to a record
        let deserialized_record = LogRecord::from_bytes(&bytes).unwrap();

        // Check that the deserialized record matches the original
        assert_eq!(deserialized_record.get_txn_id(), record.get_txn_id());
        assert_eq!(deserialized_record.get_prev_lsn(), record.get_prev_lsn());
        assert_eq!(
            deserialized_record.get_log_record_type(),
            record.get_log_record_type()
        );
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(
            deserialized_record.get_insert_rid(),
            record.get_insert_rid()
        );

        // Compare tuple data
        let original_tuple = record.get_insert_tuple().unwrap();
        let deserialized_tuple = deserialized_record.get_insert_tuple().unwrap();

        assert_eq!(original_tuple.get_values(), deserialized_tuple.get_values());
    }

    #[test]
    fn test_bincode_serialization_update_record() {
        let old_tuple = create_test_tuple();
        let new_tuple = create_test_tuple_updated();
        let rid = old_tuple.get_rid();

        let record = LogRecord::new_update_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Update,
            rid,
            Arc::new(old_tuple),
            Arc::new(new_tuple),
        );

        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();

        // Deserialize back to a record
        let deserialized_record = LogRecord::from_bytes(&bytes).unwrap();

        // Check that the deserialized record matches the original
        assert_eq!(deserialized_record.get_txn_id(), record.get_txn_id());
        assert_eq!(deserialized_record.get_prev_lsn(), record.get_prev_lsn());
        assert_eq!(
            deserialized_record.get_log_record_type(),
            record.get_log_record_type()
        );
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(
            deserialized_record.get_update_rid(),
            record.get_update_rid()
        );

        // Compare old tuple data
        let original_old_tuple = record.get_original_tuple().unwrap();
        let deserialized_old_tuple = deserialized_record.get_original_tuple().unwrap();

        assert_eq!(
            original_old_tuple.get_values(),
            deserialized_old_tuple.get_values()
        );

        // Compare new tuple data
        let original_new_tuple = record.get_update_tuple().unwrap();
        let deserialized_new_tuple = deserialized_record.get_update_tuple().unwrap();

        assert_eq!(
            original_new_tuple.get_values(),
            deserialized_new_tuple.get_values()
        );
    }

    #[test]
    fn test_bincode_serialization_new_page_record() {
        let record = LogRecord::new_page_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::NewPage,
            DUMMY_PREV_PAGE_ID,
            DUMMY_PAGE_ID,
        );

        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();

        // Deserialize back to a record
        let deserialized_record = LogRecord::from_bytes(&bytes).unwrap();

        // Check that the deserialized record matches the original
        assert_eq!(deserialized_record.get_txn_id(), record.get_txn_id());
        assert_eq!(deserialized_record.get_prev_lsn(), record.get_prev_lsn());
        assert_eq!(
            deserialized_record.get_log_record_type(),
            record.get_log_record_type()
        );
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(
            deserialized_record.get_new_page_record(),
            record.get_new_page_record()
        );
        assert_eq!(deserialized_record.get_page_id(), record.get_page_id());
    }
}
