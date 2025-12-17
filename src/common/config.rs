use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bincode::config as bincode_config;

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

// PERFORMANCE OPTIMIZATION: Dramatically increased buffer pool size
pub const BUFFER_POOL_SIZE: u64 = 8192; // 32MB buffer pool (8192 * 4KB pages)
pub const LOG_BUFFER_SIZE: u64 = (BUFFER_POOL_SIZE + 1) * DB_PAGE_SIZE; // size of a log buffer in bytes
pub const BUCKET_SIZE: u64 = 50; // size of extendable hash bucket
/// Maximum serialized tuple body size we will accept (bytes).
/// This should be comfortably below `DB_PAGE_SIZE` to leave room for metadata.
pub const TUPLE_MAX_SERIALIZED_SIZE: usize = 3500;

// PERFORMANCE OPTIMIZATION: Reduced LRU-K window for faster eviction decisions
pub const LRUK_REPLACER_K: u64 = 2; // lookback window for lru-k replacer (reduced from 10)

// NEW PERFORMANCE PARAMETERS
pub const BATCH_INSERT_THRESHOLD: usize = 100; // Process inserts in batches of 100
pub const DISK_IO_BATCH_SIZE: usize = 16; // Batch disk operations
pub const PAGE_PREFETCH_COUNT: usize = 4; // Number of pages to prefetch
pub const FLUSH_THRESHOLD: usize = 1000; // Flush after 1000 dirty pages

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

/// Bincode configuration for persisted (on-disk) encodings.
///
/// Keep this centralized so tuple/page/index encodings don’t accidentally diverge across call sites.
/// NOTE: Changing this is an on-disk format change.
#[inline]
pub(crate) fn storage_bincode_config() -> impl bincode_config::Config {
    // Pin the on-disk encoding policy explicitly.
    //
    // Using `standard()` alone is a policy choice that may change across bincode versions.
    // We explicitly choose:
    // - little-endian encoding
    // - fixed-width integer encoding
    //
    // NOTE: Changing any of these is an on-disk format change.
    // NOTE: We intentionally return an opaque config (`impl Config`) from call sites by letting
    // bincode infer the concrete type; the concrete type changes when we change knobs (e.g.
    // varint vs fixint), but call sites should not need to care.
    //
    // Unfortunately `bincode::config::Configuration` is an alias with a default integer encoding,
    // so returning it directly would force that default. Keep the signature flexible by
    // returning the inferred concrete type via `impl Config`.
    bincode_config::standard()
        .with_little_endian()
        .with_fixed_int_encoding()
}
