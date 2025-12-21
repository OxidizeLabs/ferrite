// Async Disk Manager Module
// Refactored according to Single Responsibility Principle

// Core components
pub mod config;
pub mod manager;

// Optimization components
pub mod compression;
pub mod prefetching;
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
