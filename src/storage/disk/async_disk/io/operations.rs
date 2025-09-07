// I/O Operations Module
//
// This module defines the types and structures for I/O operations,
// including operation types, operation metadata, and priority handling.

use crate::common::config::PageId;
use std::time::Instant;
use tokio::sync::oneshot;
use std::io::Result as IoResult;

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

/// I/O operation with priority and completion signaling
/// 
/// This structure represents a single I/O operation that will be processed
/// by the worker threads. It includes priority information for queue ordering
/// and a completion channel to signal when the operation is done.
#[derive(Debug)]
pub struct IOOperation {
    /// Priority level (higher number = higher priority)
    pub priority: u8,
    
    /// Unique operation identifier
    pub id: u64,
    
    /// The specific type of I/O operation to perform
    pub operation_type: IOOperationType,
    
    /// Channel for sending the operation result back to the caller
    pub completion_sender: oneshot::Sender<IoResult<Vec<u8>>>,
    
    /// Timestamp when the operation was submitted
    pub submitted_at: Instant,
}

impl Ord for IOOperation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority operations should come first in the priority queue
        self.priority.cmp(&other.priority)
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