//! # Memory Management Subsystem
//!
//! This module implements the memory-side components of the Async Disk Manager, focusing on write buffering,
//! caching, and durability coordination. It provides a robust, layered architecture for managing dirty
//! pages before they are persisted to disk.
//!
//! ## Modules
//!
//! - **`write_manager`**: The central orchestrator for all write operations. It coordinates between buffering,
//!   coalescing, and flushing components.
//! - **`write_staging_buffer`**: Handles the actual in-memory storage of dirty pages, supporting optional compression.
//! - **`flush_coordinator`**: Manages the timing and triggers for flushing dirty pages to disk (e.g., memory thresholds, time intervals).
//! - **`durability_manager`**: Enforces persistence guarantees (fsync policies, WAL integration).
//! - **`coalescing`**: A subsystem for merging adjacent writes into larger, contiguous I/O operations to improve throughput.

pub mod coalescing;
pub mod durability_manager;
pub mod flush_coordinator;
pub mod write_manager;
pub mod write_staging_buffer;

pub use coalescing::{
    CleanupReason, CoalesceResult, CoalescedSizeInfo, CoalescingEngine, PageRange, SizeAnalyzer,
};
pub use durability_manager::{DurabilityManager, DurabilityProvider, DurabilityResult};
pub use flush_coordinator::{FlushCoordinator, FlushDecision};
pub use write_manager::WriteManager;
pub use write_staging_buffer::{WriteBufferStats, WriteStagingBuffer};
