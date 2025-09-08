//! Write coalescing functionality
//! 
//! This module provides write coalescing capabilities to improve I/O efficiency
//! by combining adjacent writes into larger, more efficient operations.

pub mod engine;
pub mod size_analyzer;

pub use engine::{CleanupReason, CoalesceResult, CoalescingEngine};
pub use size_analyzer::{CoalescedSizeInfo, PageRange, SizeAnalyzer};
