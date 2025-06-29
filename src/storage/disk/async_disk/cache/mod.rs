pub mod cache_trait;
pub mod lru;
pub mod lfu;
pub mod fifo;
pub mod lru_k;
pub mod cache_manager;

// Re-export key components for backward compatibility
pub use cache_manager::CacheManager;
