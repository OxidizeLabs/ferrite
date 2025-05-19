use std::sync::atomic::AtomicBool;
use std::time::Duration;

/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
pub static CYCLE_DETECTION_INTERVAL: Duration = Duration::from_millis(100);

/** True if logging should be enabled, false otherwise. */
pub static ENABLE_LOGGING: AtomicBool = AtomicBool::new(false);

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
pub static LOG_TIMEOUT: Duration = Duration::from_secs(1);

pub const INVALID_PAGE_ID: PageId = u64::MAX; // invalid page id
pub const INVALID_TXN_ID: TxnId = u64::MAX; // invalid transaction id
pub const INVALID_LSN: Lsn = u64::MAX; // invalid log sequence number
pub const INVALID_TS: Timestamp = Timestamp::MAX;
pub const HEADER_PAGE_ID: PageId = u64::MAX; // the header page id
pub const DB_PAGE_SIZE: u64 = 4096; // size of a data page in bytes
pub const BUFFER_POOL_SIZE: u64 = 10; // size of buffer pool
pub const LOG_BUFFER_SIZE: u64 = (BUFFER_POOL_SIZE + 1) * DB_PAGE_SIZE; // size of a log buffer in bytes
pub const BUCKET_SIZE: u64 = 50; // size of extendable hash bucket
pub const LRUK_REPLACER_K: u64 = 10; // lookback window for lru-k replacer

pub type FrameId = u64; // frame id type
pub type PageId = u64; // page id type
pub type TxnId = u64; // transaction id type
pub type Lsn = u64; // log sequence number type
pub type SlotOffset = u64; // slot offset type
pub type Oid = u64; // object id type
pub type DataBaseOid = u64;
pub type TableOidT = u64;
pub type ColumnOidT = u64;
pub type IndexOidT = u64;
pub type TimeStampOidT = u64;
pub type TransactionId = u64;
pub type Timestamp = u64;

pub const TXN_START_ID: TxnId = 1 << 63; // first txn id

pub const VARCHAR_DEFAULT_LENGTH: usize = 64; // default length for varchar when constructing the column
