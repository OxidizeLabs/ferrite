//! # I/O Operations Definitions
//!
//! This module defines the core data structures that represent I/O requests within the system.
//! It decouples the *intent* of an operation from its *execution*, allowing flexible scheduling
//! and prioritization.
//!
//! ## Architecture
//!
//! ```text
//!   Caller (AsyncDiskManager, BufferPool, etc.)
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ Creates IOOperationType + priority
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        IOOperation                                      │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Scheduling Metadata                                            │   │
//!   │   │                                                                 │   │
//!   │   │  priority: u8          ← Determines queue position              │   │
//!   │   │  id: u64               ← Unique tracking identifier             │   │
//!   │   │  submitted_at: Instant ← For FIFO tie-breaking & latency        │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Operation Payload                                              │   │
//!   │   │                                                                 │   │
//!   │   │  operation_type: IOOperationType                                │   │
//!   │   │                                                                 │   │
//!   │   │     ┌─────────────────────────────────────────────────────────┐ │   │
//!   │   │     │ ReadPage  { page_id }                                   │ │   │
//!   │   │     │ WritePage { page_id, data }                             │ │   │
//!   │   │     │ ReadLog   { offset, size }                              │ │   │
//!   │   │     │ WriteLog  { data, offset }                              │ │   │
//!   │   │     │ AppendLog { data }                                      │ │   │
//!   │   │     │ Sync                                                    │ │   │
//!   │   │     │ SyncLog                                                 │ │   │
//!   │   │     └─────────────────────────────────────────────────────────┘ │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ Enqueued to IOQueueManager
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  IOQueueManager (priority-ordered BinaryHeap)                           │
//!   │                                                                         │
//!   │  Ord: priority DESC, submitted_at ASC                                   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ Dequeued by IOWorkerManager
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  IOOperationExecutor (performs actual I/O)                              │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## IOOperationType Variants
//!
//! | Variant       | Data                        | Purpose                          |
//! |---------------|-----------------------------|----------------------------------|
//! | `ReadPage`    | `page_id: PageId`           | Read 4KB page from database file |
//! | `WritePage`   | `page_id, data: Vec<u8>`    | Write 4KB page to database file  |
//! | `ReadLog`     | `offset: u64, size: usize`  | Read from WAL at specific offset |
//! | `WriteLog`    | `data: Vec<u8>, offset: u64`| Write to WAL at specific offset  |
//! | `AppendLog`   | `data: Vec<u8>`             | Append to WAL (returns offset)   |
//! | `Sync`        | (none)                      | fsync database file              |
//! | `SyncLog`     | (none)                      | fsync WAL file                   |
//!
//! ## IOOperation Fields
//!
//! | Field           | Type              | Description                          |
//! |-----------------|-------------------|--------------------------------------|
//! | `priority`      | `u8`              | Higher = dequeued first (0-255)      |
//! | `id`            | `u64`             | Unique ID for tracking & completion  |
//! | `operation_type`| `IOOperationType` | The actual I/O action to perform     |
//! | `submitted_at`  | `Instant`         | Timestamp for FIFO and latency       |
//!
//! ## Priority Levels (`priorities` module)
//!
//! ```text
//!   Priority Scale (higher = more urgent)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   10 ─┬─ SYNC         ← Durability operations (fsync)
//!       │
//!    9 ─┼─ LOG_APPEND   ← WAL append (transaction commit)
//!       │
//!    8 ─┼─ LOG_WRITE    ← WAL write at offset
//!       │
//!    7 ─┼─ PAGE_WRITE   ← Dirty page flush
//!       │
//!    6 ─┼─ LOG_READ     ← WAL read (recovery)
//!       │
//!    5 ─┼─ PAGE_READ    ← Page fetch (user query)
//!       │
//!    1 ─┴─ BACKGROUND   ← Prefetch, maintenance
//! ```
//!
//! | Constant      | Value | Use Case                                   |
//! |---------------|-------|--------------------------------------------|
//! | `SYNC`        | 10    | fsync operations for durability            |
//! | `LOG_APPEND`  | 9     | WAL append during transaction commit       |
//! | `LOG_WRITE`   | 8     | WAL write at specific offset               |
//! | `PAGE_WRITE`  | 7     | Flushing dirty pages to disk               |
//! | `LOG_READ`    | 6     | Reading from WAL (recovery, replay)        |
//! | `PAGE_READ`   | 5     | Reading pages for user queries             |
//! | `BACKGROUND`  | 1     | Prefetching, speculative reads             |
//!
//! ## Ordering Implementation
//!
//! ```text
//!   IOOperation implements Ord for BinaryHeap (max-heap) ordering:
//!
//!   fn cmp(&self, other: &Self) -> Ordering {
//!       self.priority.cmp(&other.priority)                  // Higher priority first
//!           .then_with(|| other.submitted_at.cmp(&self.submitted_at))  // Earlier wins ties
//!   }
//!
//!   Example:
//!   ┌────────────┬──────────┬─────────────────────┬────────────────┐
//!   │ Operation  │ Priority │ Submitted At        │ Queue Position │
//!   ├────────────┼──────────┼─────────────────────┼────────────────┤
//!   │ Sync       │ 10       │ 10:00:00.200        │ 1st (highest)  │
//!   │ WriteLog   │ 8        │ 10:00:00.100        │ 2nd            │
//!   │ WriteLog   │ 8        │ 10:00:00.150        │ 3rd (same pri) │
//!   │ ReadPage   │ 5        │ 10:00:00.050        │ 4th            │
//!   └────────────┴──────────┴─────────────────────┴────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::io::operations::{
//!     IOOperation, IOOperationType, priorities,
//! };
//! use std::time::Instant;
//!
//! // Create a page read operation
//! let read_op = IOOperation {
//!     priority: priorities::PAGE_READ,
//!     id: 1,
//!     operation_type: IOOperationType::ReadPage { page_id: 42 },
//!     submitted_at: Instant::now(),
//! };
//!
//! // Create a higher-priority sync operation
//! let sync_op = IOOperation {
//!     priority: priorities::SYNC,
//!     id: 2,
//!     operation_type: IOOperationType::Sync,
//!     submitted_at: Instant::now(),
//! };
//!
//! // sync_op > read_op in BinaryHeap ordering
//! assert!(sync_op > read_op);
//!
//! // Create a page write with data
//! let write_op = IOOperation {
//!     priority: priorities::PAGE_WRITE,
//!     id: 3,
//!     operation_type: IOOperationType::WritePage {
//!         page_id: 42,
//!         data: vec![0u8; 4096],
//!     },
//!     submitted_at: Instant::now(),
//! };
//!
//! // Create a log append operation
//! let log_op = IOOperation {
//!     priority: priorities::LOG_APPEND,
//!     id: 4,
//!     operation_type: IOOperationType::AppendLog {
//!         data: b"log record data".to_vec(),
//!     },
//!     submitted_at: Instant::now(),
//! };
//! ```
//!
//! ## Trait Implementations
//!
//! | Trait       | Implementation                                          |
//! |-------------|---------------------------------------------------------|
//! | `Ord`       | Compare by (priority DESC, submitted_at ASC)            |
//! | `PartialOrd`| Delegates to `Ord::cmp`                                 |
//! | `Eq`        | Marker trait for total equality                         |
//! | `PartialEq` | Compares by `id` only (unique operations)               |
//! | `Debug`     | Derived for debugging                                   |
//! | `Clone`     | `IOOperationType` is cloneable                          |
//!
//! ## Thread Safety
//!
//! - `IOOperationType` is `Clone` and can be sent across threads
//! - `IOOperation` contains `Instant` which is not `Sync` but is `Send`
//! - Operations are typically created on one thread and processed on another
//! - All data is owned (no references), enabling safe queue transfers

use std::time::Instant;

use crate::common::config::PageId;

/// Types of I/O operations supported by the async I/O engine
#[derive(Debug, Clone)]
pub enum IOOperationType {
    /// Read a page from the database file
    ReadPage { page_id: PageId },

    /// Write a page to the database file
    WritePage { page_id: PageId, data: Vec<u8> },

    /// Read data from the log file at a specific offset
    ReadLog { offset: u64, size: usize },

    /// Write data to the log file at a specific offset
    WriteLog { data: Vec<u8>, offset: u64 },

    /// Append data to the log file
    AppendLog { data: Vec<u8> },

    /// Sync the database file to disk
    Sync,

    /// Sync the log file to disk
    SyncLog,
}

/// I/O operation with priority and tracking metadata
///
/// This structure represents a single I/O operation that will be processed
/// by the worker threads. It includes priority information for queue ordering
/// and metadata for tracking and observability.
#[derive(Debug)]
pub struct IOOperation {
    /// Priority level (higher number = higher priority)
    pub priority: u8,

    /// Unique operation identifier
    pub id: u64,

    /// The specific type of I/O operation to perform
    pub operation_type: IOOperationType,

    /// Timestamp when the operation was submitted
    pub submitted_at: Instant,
}

impl Ord for IOOperation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority operations should come first in the priority queue
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.submitted_at.cmp(&self.submitted_at)) // Earlier submissions have higher priority for same priority level
    }
}

impl PartialOrd for IOOperation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for IOOperation {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for IOOperation {}

/// Default priority levels for different operation types
pub mod priorities {
    /// Sync operations - highest priority to ensure durability
    pub const SYNC: u8 = 10;

    /// Log append operations - very high priority for WAL consistency
    pub const LOG_APPEND: u8 = 9;

    /// Log write operations - high priority for transaction logging
    pub const LOG_WRITE: u8 = 8;

    /// Page write operations - higher than reads for data consistency
    pub const PAGE_WRITE: u8 = 7;

    /// Log read operations - medium priority
    pub const LOG_READ: u8 = 6;

    /// Page read operations - default priority
    pub const PAGE_READ: u8 = 5;

    /// Background operations - lowest priority
    pub const BACKGROUND: u8 = 1;
}
