//! Async Disk Manager Module
//!
//! This module implements a high-performance, asynchronous disk management system designed for
//! database workloads. It provides a non-blocking I/O interface that leverages `tokio` for
//! efficient concurrency handling.
//!
//! # Core Responsibilities
//!
//! - **Asynchronous I/O**: Efficiently handles disk reads and writes without blocking execution threads.
//! - **Resource Management**: Manages I/O resources including threads, file descriptors, and memory buffers.
//! - **Optimization**: Implements strategies like write buffering, prefetching, and I/O scheduling.
//! - **Observability**: Exposes detailed metrics for monitoring system health and performance.
//!
//! # Submodules
//!
//! - [`config`]: Configuration structures for tuning disk manager behavior (threads, buffer sizes, etc.).
//! - [`manager`]: The main [`AsyncDiskManager`] implementation, serving as the primary entry point.
//! - [`scheduler`]: Intelligent I/O task scheduling with work-stealing and priority management.
//! - [`cache`]: Caching mechanisms for hot pages to reduce physical disk access.
//! - [`memory`]: Memory management components including write buffers and durability providers.
//! - [`io`]: Low-level asynchronous I/O engine implementations.
//! - [`metrics`]: Comprehensive statistics collection and monitoring dashboard.
//! - [`compression`]: Data compression utilities for efficient storage.

// Core components
pub mod config;
pub mod manager;

// Optimization components
pub mod compression;
pub mod scheduler;
// Cache implementations
pub mod cache;
pub mod memory;

// Metrics and monitoring
pub mod io;
pub mod metrics;

pub use config::DiskManagerConfig;
// Re-export the main AsyncDiskManager for backward compatibility
pub use manager::AsyncDiskManager;
