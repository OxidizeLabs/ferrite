// Async Disk Manager Module
// Refactored according to Single Responsibility Principle

// Core components
pub mod manager;
pub mod config;

// Optimization components
pub mod compression;
pub mod simd;
pub mod prefetching;
pub mod scheduler;

// Cache implementations
pub mod cache;
pub mod memory;

// Metrics and monitoring
pub mod metrics;
pub mod io;

pub use config::DiskManagerConfig;
// Re-export the main AsyncDiskManager for backward compatibility
pub use manager::AsyncDiskManager;
