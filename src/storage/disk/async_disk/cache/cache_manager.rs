//! Advanced multi-level cache management system for the Async Disk Manager
//! 
//! This module contains the CacheManager and related components for efficient page caching,
//! prefetching, and data temperature management.

use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::disk::async_disk::config::DiskManagerConfig;
use crate::storage::disk::async_disk::prefetching::MLPrefetcher;
use crate::storage::disk::async_disk::metrics::collector::MetricsCollector;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::storage::disk::async_disk::cache::cache_trait::Cache;
use super::lru_k::LRUKCache;
use super::lfu::LFUCache;
use super::fifo::FIFOCache;

/// Page data with metadata
#[derive(Debug)]
pub struct PageData {
    pub data: Vec<u8>,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub temperature: DataTemperature,
}

impl Clone for PageData {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            last_accessed: self.last_accessed,
            access_count: self.access_count,
            temperature: self.temperature.clone(),
        }
    }
}

/// Data temperature classification
#[derive(Debug, Clone, PartialEq)]
pub enum DataTemperature {
    Hot,
    Warm,
    Cold,
    Frozen,
}

/// Access pattern classification
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPattern {
    Sequential,
    Random,
    Temporal,
    Spatial,
}

/// Metadata for hot/cold data separation
#[derive(Debug)]
pub struct HotColdMetadata {
    pub access_count: u64,
    pub last_accessed: Instant,
    pub access_pattern: AccessPattern,
    pub temperature: DataTemperature,
    pub predicted_reuse: bool,
    pub size_bytes: usize,
}

/// Prefetch engine for predicting page access patterns
#[derive(Debug)]
pub struct PrefetchEngine {
    pub access_patterns: HashMap<PageId, Vec<PageId>>,
    pub sequential_threshold: usize,
    pub prefetch_distance: usize,
    pub accuracy_tracker: AtomicU64,
}

/// Admission controller for cache management
#[derive(Debug)]
pub struct AdmissionController {
    pub admission_rate: AtomicU64,
    pub memory_pressure: AtomicU64,
    pub max_memory: usize,
}

/// Deduplication engine for identifying duplicate pages
#[derive(Debug)]
pub struct DeduplicationEngine {
    pub page_hashes: HashMap<u64, PageId>,
    pub dedup_savings_bytes: AtomicUsize,
    pub total_pages_processed: AtomicUsize,
}

impl DeduplicationEngine {
    pub fn new() -> Self {
        Self {
            page_hashes: HashMap::new(),
            dedup_savings_bytes: AtomicUsize::new(0),
            total_pages_processed: AtomicUsize::new(0),
        }
    }

    pub fn check_duplicate(&mut self, page_id: PageId, data: &[u8]) -> Option<PageId> {
        // In a real implementation, this would use a more sophisticated hashing algorithm
        // For this example, we'll use a simple hash
        let hash = Self::simple_hash(data);

        self.total_pages_processed.fetch_add(1, Ordering::Relaxed);

        if let Some(&existing_page_id) = self.page_hashes.get(&hash) {
            if existing_page_id != page_id {
                // Found a duplicate page
                self.dedup_savings_bytes.fetch_add(data.len(), Ordering::Relaxed);
                return Some(existing_page_id);
            }
        }

        // No duplicate found, add to hash map
        self.page_hashes.insert(hash, page_id);
        None
    }

    fn simple_hash(data: &[u8]) -> u64 {
        let mut hash: u64 = 0;
        for &byte in data {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }

    pub fn get_stats(&self) -> (usize, usize, f64) {
        let savings = self.dedup_savings_bytes.load(Ordering::Relaxed);
        let total = self.total_pages_processed.load(Ordering::Relaxed);
        let ratio = if total > 0 { savings as f64 / (total * DB_PAGE_SIZE as usize) as f64 } else { 0.0 };
        (savings, total, ratio)
    }
}

/// Cache statistics
#[derive(Debug)]
pub struct CacheStatistics {
    pub hot_cache_size: usize,
    pub warm_cache_size: usize,
    pub cold_cache_size: usize,
    pub hot_cache_used: usize,
    pub warm_cache_used: usize,
    pub cold_cache_used: usize,
    pub hot_cache_hit_ratio: f64,
    pub warm_cache_hit_ratio: f64,
    pub cold_cache_hit_ratio: f64,
    pub overall_hit_ratio: f64,
    pub promotion_count: u64,
    pub demotion_count: u64,
    pub prefetch_accuracy: f64,
}

/// Advanced multi-level cache manager with ML-based prefetching
#[derive(Debug)]
pub struct CacheManager {
    // L1: Hot page cache (fastest access) - LRU-K based
    hot_cache: Arc<RwLock<LRUKCache<PageId, Vec<u8>>>>,

    // L2: Warm page cache (medium access) - LFU based
    warm_cache: Arc<RwLock<LFUCache<PageId, PageData>>>,

    // L3: Cold page cache (background prefetch) - FIFO based
    cold_cache: Arc<RwLock<FIFOCache<PageId, PageData>>>,

    // Cache sizes
    hot_cache_size: usize,
    warm_cache_size: usize,
    cold_cache_size: usize,

    // Predictive prefetching
    prefetch_engine: Arc<RwLock<PrefetchEngine>>,

    // ML-based prefetching
    ml_prefetcher: Arc<RwLock<MLPrefetcher>>,

    // Cache admission control
    admission_controller: Arc<AdmissionController>,

    // Hot/cold data separation
    hot_data_tracker: Arc<RwLock<HashMap<PageId, HotColdMetadata>>>,

    // Deduplication engine
    dedup_engine: Arc<RwLock<DeduplicationEngine>>,

    // Cache statistics
    promotion_count: AtomicU64,
    demotion_count: AtomicU64,

    // Advanced statistics
    prefetch_accuracy: AtomicU64,
    dedup_savings: AtomicU64,
}

impl CacheManager {
    /// Creates a new cache manager
    pub fn new(config: &DiskManagerConfig) -> Self {
        let total_cache_mb = config.cache_size_mb;
        let hot_cache_size = (total_cache_mb as f64 * config.hot_cache_ratio) as usize;
        let warm_cache_size = (total_cache_mb as f64 * config.warm_cache_ratio) as usize;
        let cold_cache_size = total_cache_mb - hot_cache_size - warm_cache_size;

        // Use LRU-K cache for hot cache (L1)
        let hot_cache = Arc::new(RwLock::new(LRUKCache::with_k(hot_cache_size, 2)));
        let warm_cache = Arc::new(RwLock::new(LFUCache::new(warm_cache_size)));
        let cold_cache = Arc::new(RwLock::new(FIFOCache::new(cold_cache_size)));

        let prefetch_engine = Arc::new(RwLock::new(PrefetchEngine {
            access_patterns: HashMap::new(),
            sequential_threshold: 3,
            prefetch_distance: config.prefetch_distance,
            accuracy_tracker: AtomicU64::new(0),
        }));

        let admission_controller = Arc::new(AdmissionController {
            admission_rate: AtomicU64::new(100),
            memory_pressure: AtomicU64::new(0),
            max_memory: total_cache_mb * 1024 * 1024, // Convert to bytes
        });

        // Initialize ML prefetcher
        let ml_prefetcher = Arc::new(RwLock::new(MLPrefetcher::new()));

        // Initialize hot/cold data tracker
        let hot_data_tracker = Arc::new(RwLock::new(HashMap::new()));

        // Initialize deduplication engine
        let dedup_engine = Arc::new(RwLock::new(DeduplicationEngine::new()));

        Self {
            hot_cache,
            warm_cache,
            cold_cache,
            hot_cache_size,
            warm_cache_size,
            cold_cache_size,
            prefetch_engine,
            ml_prefetcher,
            admission_controller,
            hot_data_tracker,
            dedup_engine,
            promotion_count: AtomicU64::new(0),
            demotion_count: AtomicU64::new(0),
            prefetch_accuracy: AtomicU64::new(0),
            dedup_savings: AtomicU64::new(0),
        }
    }

    /// Attempts to get a page from any cache level
    pub fn get_page(&self, page_id: PageId) -> Option<Vec<u8>> {
        // Check L1 hot cache first (LRU-K)
        if let Some(data) = self.hot_cache.try_write().ok()?.get(&page_id) {
            return Some(data.clone());
        }

        // Check L2 warm cache (LFU)
        if let Some(page_data) = self.warm_cache.try_write().ok()?.get(&page_id) {
            // Promote to hot cache on hit
            self.promotion_count.fetch_add(1, Ordering::Relaxed);
            let data_copy = page_data.data.clone();
            if let Ok(mut hot_cache) = self.hot_cache.try_write() {
                hot_cache.insert(page_id, data_copy.clone());
            }
            return Some(data_copy);
        }

        // Check L3 cold cache (FIFO)
        if let Some(page_data) = self.cold_cache.try_write().ok()?.get(&page_id) {
            // Promote to warm cache on hit
            self.promotion_count.fetch_add(1, Ordering::Relaxed);
            let data_copy = page_data.data.clone();
            if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                warm_cache.insert(page_id, page_data.clone());
            }
            return Some(data_copy);
        }

        None
    }

    /// Attempts to get a page from any cache level with enhanced monitoring
    pub fn get_page_with_metrics(&self, page_id: PageId, metrics_collector: &MetricsCollector) -> Option<Vec<u8>> {
        // Check L1 hot cache first (LRU-K)
        if let Ok(mut hot_cache) = self.hot_cache.try_write() {
            if let Some(data) = hot_cache.get(&page_id) {
                metrics_collector.record_cache_operation("hot", true);
                return Some(data.clone());
            }
        }

        // Check L2 warm cache (LFU)
        if let Ok(mut warm_cache) = self.warm_cache.try_write() {
            if let Some(page_data) = warm_cache.get(&page_id) {
                // Promote to hot cache on hit
                self.promotion_count.fetch_add(1, Ordering::Relaxed);
                metrics_collector.record_cache_migration(true);
                metrics_collector.record_cache_operation("warm", true);

                let data_copy = page_data.data.clone();
                if let Ok(mut hot_cache) = self.hot_cache.try_write() {
                    hot_cache.insert(page_id, data_copy.clone());
                }
                return Some(data_copy);
            }
        }

        // Check L3 cold cache (FIFO)
        if let Ok(mut cold_cache) = self.cold_cache.try_write() {
            if let Some(page_data) = cold_cache.get(&page_id) {
                // Promote to warm cache on hit
                self.promotion_count.fetch_add(1, Ordering::Relaxed);
                metrics_collector.record_cache_migration(true);
                metrics_collector.record_cache_operation("cold", true);

                let data_copy = page_data.data.clone();
                if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                    warm_cache.insert(page_id, page_data.clone());
                }
                return Some(data_copy);
            }
        }

        // Cache miss
        metrics_collector.record_cache_operation("all", false);
        None
    }

    /// Stores a page in the appropriate cache level
    pub fn store_page(&self, page_id: PageId, data: Vec<u8>) {
        // Check for duplicate pages
        let mut should_store = true;
        if let Ok(mut dedup) = self.dedup_engine.try_write() {
            if let Some(_existing_page) = dedup.check_duplicate(page_id, &data) {
                // This is a duplicate page, don't store it
                should_store = false;
                self.dedup_savings.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !should_store {
            return;
        }

        // Determine the appropriate cache level based on access patterns
        let temperature = self.determine_data_temperature(page_id);

        let page_data = PageData {
            data: data.clone(),
            last_accessed: Instant::now(),
            access_count: 1,
            temperature: temperature.clone(),
        };

        match temperature {
            DataTemperature::Hot => {
                if let Ok(mut cache) = self.hot_cache.try_write() {
                    cache.insert(page_id, data);
                }
            },
            DataTemperature::Warm => {
                if let Ok(mut cache) = self.warm_cache.try_write() {
                    cache.insert(page_id, page_data);
                }
            },
            _ => {
                if let Ok(mut cache) = self.cold_cache.try_write() {
                    cache.insert(page_id, page_data);
                }
            }
        }
    }

    /// Determines the temperature of a page based on access patterns
    fn determine_data_temperature(&self, page_id: PageId) -> DataTemperature {
        if let Ok(tracker) = self.hot_data_tracker.try_read() {
            if let Some(metadata) = tracker.get(&page_id) {
                return metadata.temperature.clone();
            }
        }

        // Default to cold for new pages
        DataTemperature::Cold
    }

    /// Gets cache statistics
    pub fn get_cache_statistics(&self) -> CacheStatistics {
        let hot_used = if let Ok(cache) = self.hot_cache.try_read() {
            cache.len()
        } else {
            0
        };

        let warm_used = if let Ok(cache) = self.warm_cache.try_read() {
            cache.len()
        } else {
            0
        };

        let cold_used = if let Ok(cache) = self.cold_cache.try_read() {
            cache.len()
        } else {
            0
        };

        // Simplified statistics - the cache implementations don't have hit_ratio() and stats() methods
        // For now, we'll provide placeholder values
        let hot_hit_ratio = 0.0;
        let warm_hit_ratio = 0.0;
        let cold_hit_ratio = 0.0;
        let total_hits = 0.0;

        let prefetch_accuracy = self.prefetch_accuracy.load(Ordering::Relaxed) as f64 / 100.0;

        CacheStatistics {
            hot_cache_size: self.hot_cache_size,
            warm_cache_size: self.warm_cache_size,
            cold_cache_size: self.cold_cache_size,
            hot_cache_used: hot_used,
            warm_cache_used: warm_used,
            cold_cache_used: cold_used,
            hot_cache_hit_ratio: hot_hit_ratio,
            warm_cache_hit_ratio: warm_hit_ratio,
            cold_cache_hit_ratio: cold_hit_ratio,
            overall_hit_ratio: total_hits,
            promotion_count: self.promotion_count.load(Ordering::Relaxed),
            demotion_count: self.demotion_count.load(Ordering::Relaxed),
            prefetch_accuracy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;

    #[test]
    fn test_cache_manager_basic() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test store_page and get_page
        let page_id = 1;
        let data = vec![1, 2, 3, 4];
        cache_manager.store_page(page_id, data.clone());

        let retrieved = cache_manager.get_page(page_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        // Test cache statistics
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }
}
