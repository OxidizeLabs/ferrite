//! # Async Disk I/O Subsystem
//!
//! This module implements a high-performance, asynchronous I/O layer for the database storage engine.
//! It is designed to handle heavy concurrent I/O loads with priority scheduling, direct I/O support,
//! and detailed observability.
//!
//! ## Modules
//!
//! - **`io_impl`**: The main `AsyncIOEngine` entry point.
//! - **`executor`**: Low-level `tokio::fs` wrapper for executing file operations.
//! - **`worker`**: Thread pool management for processing operations.
//! - **`queue`**: Priority queue for scheduling I/O requests.
//! - **`completion`**: Lifecycle tracking and notification for async operations.
//! - **`metrics`**: Performance monitoring and statistics.
//! - **`operations`**: Data structures defining I/O request types.

mod completion;
mod io_impl;
mod metrics;
mod operation_status;

// New modular components
pub mod executor;
pub mod operations;
pub mod queue;
pub mod worker;

pub use io_impl::*;
