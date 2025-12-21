//! # Write Coalescing Subsystem
//!
//! This module implements logic to combine multiple small write operations into larger, contiguous I/O requests.
//! This significantly reduces the overhead of disk seeks and syscalls, especially for spinning disks or cloud storage.
//!
//! ## Components
//!
//! - **`engine`**: The `CoalescingEngine` that actively manages pending writes and merges overlapping/adjacent requests.
//! - **`size_analyzer`**: Provides heuristic analysis to determine if coalescing specific ranges is actually efficient (considering gaps, alignment, etc.).

pub mod engine;
pub mod size_analyzer;

pub use engine::{CleanupReason, CoalesceResult, CoalescingEngine};
pub use size_analyzer::{CoalescedSizeInfo, PageRange, SizeAnalyzer};
