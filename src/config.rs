use std::sync::atomic::AtomicBool;
use std::time::Duration;

const DISABLE_LOCK_MANAGER: bool = true;
const DISABLE_CHECKPOINT_MANAGER: bool = true;


/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
pub static CYCLE_DETECTION_INTERVAL: Duration = Duration::from_millis(100);

/** True if logging should be enabled, false otherwise. */
pub static ENABLE_LOGGING: AtomicBool = AtomicBool::new(false);

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
pub static LOG_TIMEOUT: Duration = Duration::from_secs(1);

pub const INVALID_PAGE_ID: i32 = -1;          // invalid page id
pub const INVALID_TXN_ID: i32 = -1;           // invalid transaction id
pub const INVALID_LSN: i32 = -1;              // invalid log sequence number
pub const HEADER_PAGE_ID: i32 = 0;            // the header page id
pub const BUSTUB_PAGE_SIZE: usize = 4096;     // size of a data page in bytes
pub const BUFFER_POOL_SIZE: usize = 10;       // size of buffer pool
pub const LOG_BUFFER_SIZE: usize = (BUFFER_POOL_SIZE + 1) * BUSTUB_PAGE_SIZE; // size of a log buffer in bytes
pub const BUCKET_SIZE: usize = 50;            // size of extendible hash bucket
pub const LRUK_REPLACER_K: usize = 10;        // lookback window for lru-k replacer

pub type FrameId = i32;          // frame id type
pub type PageId = i32;           // page id type
pub type TxnId = i64;            // transaction id type
pub type Lsn = i32;              // log sequence number type
pub type SlotOffset = usize;     // slot offset type
pub type Oid = u16;              // object id type

pub const TXN_START_ID: TxnId = 1 << 62;  // first txn id

pub const VARCHAR_DEFAULT_LENGTH: usize = 128;  // default length for varchar when constructing the column
