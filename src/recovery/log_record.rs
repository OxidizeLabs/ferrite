use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;

use crate::common::config::{Lsn, PageId, TxnId, INVALID_LSN};
use crate::common::rid::RID;
use crate::storage::table::tuple::Tuple;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

/// The type of the log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug)]
pub struct LogRecord {
    size: i32,
    lsn: Lsn,
    txn_id: TxnId,
    prev_lsn: Lsn,
    log_record_type: LogRecordType,

    // Fields for different types of log records
    delete_rid: Option<RID>,
    delete_tuple: Option<Arc<Tuple>>,
    insert_rid: Option<RID>,
    insert_tuple: Option<Arc<Tuple>>,
    update_rid: Option<RID>,
    old_tuple: Option<Arc<Tuple>>,
    new_tuple: Option<Arc<Tuple>>,
    prev_page_id: Option<PageId>,
    page_id: Option<PageId>,
}

// Helper function to serialize an Option<Arc<Tuple>>
fn serialize_arc_tuple<S>(
    tuple: &Option<Arc<Tuple>>, 
    serializer: S
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match tuple {
        Some(arc_tuple) => {
            // Get a reference to the tuple and serialize it directly
            let tuple_ref = arc_tuple.as_ref();
            // Serialize as Some(tuple)
            Some(tuple_ref).serialize(serializer)
        }
        None => {
            // Serialize None
            Option::<Tuple>::None.serialize(serializer)
        }
    }
}

// Helper function to deserialize into Option<Arc<Tuple>>
fn deserialize_arc_tuple<'de, D>(
    deserializer: D
) -> Result<Option<Arc<Tuple>>, D::Error>
where
    D: Deserializer<'de>,
{
    // First deserialize to Option<Tuple>
    let option_tuple: Option<Tuple> = Deserialize::deserialize(deserializer)?;
    
    // Convert Option<Tuple> to Option<Arc<Tuple>>
    let result = option_tuple.map(Arc::new);
    Ok(result)
}

// Custom serialization implementation for LogRecord
impl Serialize for LogRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        // Create a serializable struct that uses our helper functions
        #[derive(Serialize)]
        struct SerializableLogRecord<'a> {
            size: i32,
            lsn: Lsn,
            txn_id: TxnId,
            prev_lsn: Lsn,
            log_record_type: LogRecordType,
            delete_rid: &'a Option<RID>,
            #[serde(serialize_with = "serialize_arc_tuple")]
            delete_tuple: &'a Option<Arc<Tuple>>,
            insert_rid: &'a Option<RID>,
            #[serde(serialize_with = "serialize_arc_tuple")]
            insert_tuple: &'a Option<Arc<Tuple>>,
            update_rid: &'a Option<RID>,
            #[serde(serialize_with = "serialize_arc_tuple")]
            old_tuple: &'a Option<Arc<Tuple>>,
            #[serde(serialize_with = "serialize_arc_tuple")]
            new_tuple: &'a Option<Arc<Tuple>>,
            prev_page_id: &'a Option<PageId>,
            page_id: &'a Option<PageId>,
        }

        let serializable = SerializableLogRecord {
            size: self.size,
            lsn: self.lsn,
            txn_id: self.txn_id,
            prev_lsn: self.prev_lsn,
            log_record_type: self.log_record_type,
            delete_rid: &self.delete_rid,
            delete_tuple: &self.delete_tuple,
            insert_rid: &self.insert_rid,
            insert_tuple: &self.insert_tuple,
            update_rid: &self.update_rid,
            old_tuple: &self.old_tuple,
            new_tuple: &self.new_tuple,
            prev_page_id: &self.prev_page_id,
            page_id: &self.page_id,
        };

        serializable.serialize(serializer)
    }
}

// Custom deserialization implementation for LogRecord
impl<'de> Deserialize<'de> for LogRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Create a deserializable struct that uses our helper functions
        #[derive(Deserialize)]
        struct DeserializableLogRecord {
            size: i32,
            lsn: Lsn,
            txn_id: TxnId,
            prev_lsn: Lsn,
            log_record_type: LogRecordType,
            delete_rid: Option<RID>,
            #[serde(deserialize_with = "deserialize_arc_tuple")]
            delete_tuple: Option<Arc<Tuple>>,
            insert_rid: Option<RID>,
            #[serde(deserialize_with = "deserialize_arc_tuple")]
            insert_tuple: Option<Arc<Tuple>>,
            update_rid: Option<RID>,
            #[serde(deserialize_with = "deserialize_arc_tuple")]
            old_tuple: Option<Arc<Tuple>>,
            #[serde(deserialize_with = "deserialize_arc_tuple")]
            new_tuple: Option<Arc<Tuple>>,
            prev_page_id: Option<PageId>,
            page_id: Option<PageId>,
        }

        let d = DeserializableLogRecord::deserialize(deserializer)?;

        Ok(LogRecord {
            size: d.size,
            lsn: d.lsn,
            txn_id: d.txn_id,
            prev_lsn: d.prev_lsn,
            log_record_type: d.log_record_type,
            delete_rid: d.delete_rid,
            delete_tuple: d.delete_tuple,
            insert_rid: d.insert_rid,
            insert_tuple: d.insert_tuple,
            update_rid: d.update_rid,
            old_tuple: d.old_tuple,
            new_tuple: d.new_tuple,
            prev_page_id: d.prev_page_id,
            page_id: d.page_id,
        })
    }
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
    pub fn new_transaction_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
    ) -> Self {
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
    pub fn new_insert_delete_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        rid: RID,
        tuple: Arc<Tuple>,
    ) -> Self {
        let size = Self::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + size_of::<i32>() as i32
            + tuple.get_length().unwrap() as i32;
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
    pub fn new_update_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        update_rid: RID,
        old_tuple: Arc<Tuple>,
        new_tuple: Arc<Tuple>,
    ) -> Self {
        let size = Self::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + old_tuple.get_length().unwrap() as i32
            + new_tuple.get_length().unwrap() as i32
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
    pub fn new_page_record(
        txn_id: TxnId,
        prev_lsn: Lsn,
        log_record_type: LogRecordType,
        prev_page_id: PageId,
        page_id: PageId,
    ) -> Self {
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

    /// Returns the page id
    pub fn get_page_id(&self) -> Option<&PageId> {
        self.page_id.as_ref()
    }

    /// Returns the delete tuple.
    pub fn get_delete_tuple(&self) -> Option<&Tuple> {
        self.delete_tuple.as_ref().map(|arc| arc.as_ref())
    }

    /// Returns the delete RID.
    pub fn get_delete_rid(&self) -> Option<&RID> {
        self.delete_rid.as_ref()
    }

    /// Returns the insert tuple.
    pub fn get_insert_tuple(&self) -> Option<&Tuple> {
        self.insert_tuple.as_ref().map(|arc| arc.as_ref())
    }

    /// Returns the insert RID.
    pub fn get_insert_rid(&self) -> Option<&RID> {
        self.insert_rid.as_ref()
    }

    /// Returns the original tuple.
    pub fn get_original_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref().map(|arc| arc.as_ref())
    }

    /// Returns the update tuple.
    pub fn get_update_tuple(&self) -> Option<&Tuple> {
        self.new_tuple.as_ref().map(|arc| arc.as_ref())
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
            self.size, self.lsn, self.txn_id, self.prev_lsn, self.log_record_type as i32,
        )
    }

    pub fn is_commit(&self) -> bool {
        matches!(self.log_record_type, LogRecordType::Commit)
    }

    /// Serializes the log record to bytes using bincode.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized log record, or an error if serialization fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserializes a log record from bytes using bincode.
    ///
    /// # Parameters
    /// - `bytes`: The bytes to deserialize.
    ///
    /// # Returns
    /// A deserialized log record, or an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::mem::size_of;

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

        assert_eq!(record.get_size(), LogRecord::HEADER_SIZE as i32);
        assert_eq!(record.get_txn_id(), DUMMY_TXN_ID);
        assert_eq!(record.get_prev_lsn(), DUMMY_PREV_LSN);
        assert_eq!(record.get_log_record_type(), LogRecordType::Begin);
        assert_eq!(record.get_lsn(), INVALID_LSN);
    }

    #[test]
    fn test_insert_record() {
        let tuple = create_test_tuple();
        let rid = tuple.get_rid();
        let expected_size = LogRecord::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + size_of::<i32>() as i32
            + tuple.get_length().unwrap() as i32;

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Insert,
            rid,
            Arc::new(tuple),
        );

        assert_eq!(record.get_size(), expected_size);
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
        let expected_size = LogRecord::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + size_of::<i32>() as i32
            + tuple.get_length().unwrap() as i32;

        let record = LogRecord::new_insert_delete_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::MarkDelete,
            rid,
            Arc::new(tuple),
        );

        assert_eq!(record.get_size(), expected_size);
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
        let expected_size = LogRecord::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + old_tuple.get_length().unwrap() as i32
            + new_tuple.get_length().unwrap() as i32
            + 2 * size_of::<i32>() as i32;

        let record = LogRecord::new_update_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::Update,
            rid,
            Arc::new(old_tuple),
            Arc::new(new_tuple),
        );

        assert_eq!(record.get_size(), expected_size);
        assert_eq!(record.get_log_record_type(), LogRecordType::Update);
        assert_eq!(record.get_update_rid(), Some(&rid));
        assert!(record.get_original_tuple().is_some());
        assert!(record.get_update_tuple().is_some());
    }

    #[test]
    fn test_new_page_record() {
        let expected_size = LogRecord::HEADER_SIZE as i32 + 2 * size_of::<PageId>() as i32;

        let record = LogRecord::new_page_record(
            DUMMY_TXN_ID,
            DUMMY_PREV_LSN,
            LogRecordType::NewPage,
            DUMMY_PREV_PAGE_ID,
            DUMMY_PAGE_ID,
        );

        assert_eq!(record.get_size(), expected_size);
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
            LogRecord::HEADER_SIZE,
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
        assert_eq!(record.get_size(), LogRecord::HEADER_SIZE as i32);
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

        let expected_size = LogRecord::HEADER_SIZE as i32
            + size_of::<RID>() as i32
            + size_of::<i32>() as i32
            + insert_record
                .get_insert_tuple()
                .unwrap()
                .get_length()
                .unwrap() as i32;

        assert_eq!(insert_record.get_size(), expected_size);
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

        // Both records should point to the same tuple
        assert!(std::ptr::eq(
            record1.get_insert_tuple().unwrap(),
            record2.get_delete_tuple().unwrap()
        ));
    }

    #[test]
    fn test_bincode_serialization_transaction_record() {
        let record = LogRecord::new_transaction_record(
            DUMMY_TXN_ID, 
            DUMMY_PREV_LSN, 
            LogRecordType::Begin
        );
        
        // Serialize to bytes
        let bytes = record.to_bytes().unwrap();
        
        // Deserialize back to a record
        let deserialized_record = LogRecord::from_bytes(&bytes).unwrap();
        
        // Check that the deserialized record matches the original
        assert_eq!(deserialized_record.get_txn_id(), record.get_txn_id());
        assert_eq!(deserialized_record.get_prev_lsn(), record.get_prev_lsn());
        assert_eq!(deserialized_record.get_log_record_type(), record.get_log_record_type());
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
        assert_eq!(deserialized_record.get_log_record_type(), record.get_log_record_type());
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(deserialized_record.get_insert_rid(), record.get_insert_rid());
        
        // Compare tuple data
        let original_tuple = record.get_insert_tuple().unwrap();
        let deserialized_tuple = deserialized_record.get_insert_tuple().unwrap();
        
        assert_eq!(original_tuple.get_rid(), deserialized_tuple.get_rid());
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
        assert_eq!(deserialized_record.get_log_record_type(), record.get_log_record_type());
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(deserialized_record.get_update_rid(), record.get_update_rid());
        
        // Compare old tuple data
        let original_old_tuple = record.get_original_tuple().unwrap();
        let deserialized_old_tuple = deserialized_record.get_original_tuple().unwrap();
        
        assert_eq!(original_old_tuple.get_rid(), deserialized_old_tuple.get_rid());
        assert_eq!(original_old_tuple.get_values(), deserialized_old_tuple.get_values());
        
        // Compare new tuple data
        let original_new_tuple = record.get_update_tuple().unwrap();
        let deserialized_new_tuple = deserialized_record.get_update_tuple().unwrap();
        
        assert_eq!(original_new_tuple.get_rid(), deserialized_new_tuple.get_rid());
        assert_eq!(original_new_tuple.get_values(), deserialized_new_tuple.get_values());
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
        assert_eq!(deserialized_record.get_log_record_type(), record.get_log_record_type());
        assert_eq!(deserialized_record.get_size(), record.get_size());
        assert_eq!(deserialized_record.get_new_page_record(), record.get_new_page_record());
        assert_eq!(deserialized_record.get_page_id(), record.get_page_id());
    }
}
