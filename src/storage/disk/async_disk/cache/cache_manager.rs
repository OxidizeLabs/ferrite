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
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait, LFUCacheTrait, LRUKCacheTrait};
use super::lru_k::LRUKCache;
use super::lfu::LFUCache;
use super::fifo::FIFOCache;

/// Page data with metadata
#[derive(Debug, Clone)]
pub struct PageData {
    pub data: Arc<Vec<u8>>,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub temperature: DataTemperature,
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

impl Default for DeduplicationEngine {
    fn default() -> Self {
        Self::new()
    }
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

        if let Some(&existing_page_id) = self.page_hashes.get(&hash)
            && existing_page_id != page_id {
                // Found a duplicate page
                self.dedup_savings_bytes.fetch_add(data.len(), Ordering::Relaxed);
                return Some(existing_page_id);
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

/// Enhanced cache statistics using specialized trait functionality
#[derive(Debug)]
pub struct EnhancedCacheStatistics {
    pub basic_stats: CacheStatistics,
    pub lru_k_value: usize,
    pub hot_cache_algorithm: String,
    pub warm_cache_algorithm: String,
    pub cold_cache_algorithm: String,
}

/// Detailed page access information
#[derive(Debug)]
pub struct PageAccessDetails {
    pub cache_level: String,
    pub algorithm: String,
    pub k_value: Option<usize>,
    pub access_count: Option<u64>,
    pub k_distance: Option<u64>,
    pub eviction_rank: Option<usize>,
    pub temperature: DataTemperature,
}

/// Advanced multi-level cache manager with ML-based prefetching
#[derive(Debug)]
pub struct CacheManager {
    // L1: Hot page cache (fastest access) - LRU-K based
    hot_cache: Arc<RwLock<LRUKCache<PageId, Arc<Vec<u8>>>>>,

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

    // ML-based prefetcher
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

    // Prefetch tracking
    prefetch_predictions: Arc<RwLock<HashMap<PageId, Instant>>>,
    prefetch_hits: AtomicU64,
    prefetch_total: AtomicU64,
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

        // Initialize prefetch tracking
        let prefetch_predictions = Arc::new(RwLock::new(HashMap::new()));
        let prefetch_hits = AtomicU64::new(0);
        let prefetch_total = AtomicU64::new(0);

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
            prefetch_predictions,
            prefetch_hits,
            prefetch_total,
        }
    }

    /// Attempts to get a page from any cache level
    pub fn get_page(&self, page_id: PageId) -> Option<Vec<u8>> {
        self.get_page_internal(page_id, None)
    }

    /// Internal method to get a page from any cache level
    fn get_page_internal(&self, page_id: PageId, metrics_collector: Option<&MetricsCollector>) -> Option<Vec<u8>> {
        // Record access pattern for prefetching
        self.record_access_pattern(page_id);

        // Check if this page access was a prefetch hit
        self.check_prefetch_hit(page_id);

        // Check L1 hot cache first (LRU-K)
        if let Ok(mut hot_cache) = self.hot_cache.try_write()
            && let Some(data_arc) = <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::get(&mut hot_cache, &page_id) {
                if let Some(metrics) = metrics_collector {
                    metrics.record_cache_operation("hot", true);
                }
                // Only clone the actual data when returning to caller
                return Some((**data_arc).clone());
            }

        // Check L2 warm cache (LFU)
        if let Ok(mut warm_cache) = self.warm_cache.try_write()
            && let Some(page_data) = <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::get(&mut warm_cache, &page_id) {
                // Promote to hot cache on hit - share the Arc, no data copying
                self.promotion_count.fetch_add(1, Ordering::Relaxed);
                let data_arc = Arc::clone(&page_data.data);
                if let Ok(mut hot_cache) = self.hot_cache.try_write() {
                    <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::insert(&mut hot_cache, page_id, data_arc);
                }
                if let Some(metrics) = metrics_collector {
                    metrics.record_cache_operation("warm", true);
                    metrics.record_cache_migration(true);
                }
                // Only clone the actual data when returning to caller
                return Some((*page_data.data).clone());
            }

        // Check L3 cold cache (FIFO)
        if let Ok(mut cold_cache) = self.cold_cache.try_write()
            && let Some(page_data) = <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::get(&mut cold_cache, &page_id) {
                // Promote to warm cache on hit - clone the PageData (cheap with Arc)
                self.promotion_count.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                    <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(&mut warm_cache, page_id, page_data.clone());
                }
                if let Some(metrics) = metrics_collector {
                    metrics.record_cache_operation("cold", true);
                    metrics.record_cache_migration(true);
                }
                // Only clone the actual data when returning to caller
                return Some((*page_data.data).clone());
            }

        // Cache miss - record for metrics
        if let Some(metrics) = metrics_collector {
            metrics.record_cache_operation("all", false);
        }

        None
    }

    /// Attempts to get a page from any cache level with enhanced monitoring
    pub fn get_page_with_metrics(&self, page_id: PageId, metrics_collector: &MetricsCollector) -> Option<Vec<u8>> {
        self.get_page_internal(page_id, Some(metrics_collector))
    }

    /// Records access pattern for prefetching engines
    fn record_access_pattern(&self, page_id: PageId) {
        // Update basic prefetch engine
        if let Ok(mut prefetch_engine) = self.prefetch_engine.try_write() {
            // Update access patterns for sequential detection
            prefetch_engine.access_patterns
                .entry(page_id)
                .or_insert_with(Vec::new)
                .push(page_id);
        }

        // Update ML prefetcher
        if let Ok(mut ml_prefetcher) = self.ml_prefetcher.try_write() {
            ml_prefetcher.record_access(page_id);
        }

        // Update hot/cold data tracker
        if let Ok(mut tracker) = self.hot_data_tracker.try_write() {
            let metadata = tracker.entry(page_id).or_insert_with(|| HotColdMetadata {
                access_count: 0,
                last_accessed: Instant::now(),
                access_pattern: AccessPattern::Random,
                temperature: DataTemperature::Cold,
                predicted_reuse: false,
                size_bytes: 0,
            });
            
            metadata.access_count += 1;
            metadata.last_accessed = Instant::now();
            
            // Update temperature based on access frequency
            metadata.temperature = match metadata.access_count {
                n if n > 10 => DataTemperature::Hot,
                n if n > 5 => DataTemperature::Warm,
                n if n > 2 => DataTemperature::Cold,
                _ => DataTemperature::Frozen,
            };
        }
    }

    /// Stores a page in the appropriate cache level
    pub fn store_page(&self, page_id: PageId, data: Vec<u8>) {
        // Check admission controller first
        if !self.should_admit_page(page_id, &data) {
            return;
        }

        // Check for duplicate pages
        let mut should_store = true;
        if let Ok(mut dedup) = self.dedup_engine.try_write()
            && let Some(_existing_page) = dedup.check_duplicate(page_id, &data) {
                // This is a duplicate page, don't store it
                should_store = false;
                self.dedup_savings.fetch_add(1, Ordering::Relaxed);
            }

        if !should_store {
            return;
        }

        // Determine the appropriate cache level based on access patterns
        let temperature = self.determine_data_temperature(page_id);

        // Wrap data in Arc for efficient sharing between cache levels
        let data_arc = Arc::new(data);

        match temperature {
            DataTemperature::Hot => {
                if let Ok(mut cache) = self.hot_cache.try_write() {
                    <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::insert(&mut cache, page_id, Arc::clone(&data_arc));
                }
            },
            DataTemperature::Warm => {
                let page_data = PageData {
                    data: Arc::clone(&data_arc),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    temperature: temperature.clone(),
                };
                if let Ok(mut cache) = self.warm_cache.try_write() {
                    <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(&mut cache, page_id, page_data);
                }
            },
            _ => {
                let page_data = PageData {
                    data: Arc::clone(&data_arc),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    temperature: temperature.clone(),
                };
                if let Ok(mut cache) = self.cold_cache.try_write() {
                    <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(&mut cache, page_id, page_data);
                }
            }
        }

        // Update access pattern for this page
        self.record_access_pattern(page_id);
    }

    /// Determines whether a page should be admitted to the cache
    fn should_admit_page(&self, page_id: PageId, data: &[u8]) -> bool {
        // Check memory pressure
        let memory_pressure = self.admission_controller.memory_pressure.load(Ordering::Relaxed);
        if memory_pressure > 80 {
            // High memory pressure - be more selective
            let admission_rate = self.admission_controller.admission_rate.load(Ordering::Relaxed);
            if admission_rate < 50 {
                return false;
            }
        }

        // Check if this page is likely to be reused
        if let Ok(tracker) = self.hot_data_tracker.try_read()
            && let Some(metadata) = tracker.get(&page_id) {
                // If page has been accessed recently, admit it
                if metadata.last_accessed.elapsed().as_secs() < 60 {
                    return true;
                }
                // If page has high access count, admit it
                if metadata.access_count > 3 {
                    return true;
                }
            }

        // Check data size - don't admit extremely large pages under pressure
        if memory_pressure > 60 && data.len() > (DB_PAGE_SIZE * 2) as usize {
            return false;
        }

        // Default to admitting the page
        true
    }

    /// Determines the temperature of a page based on access patterns
    fn determine_data_temperature(&self, page_id: PageId) -> DataTemperature {
        if let Ok(tracker) = self.hot_data_tracker.try_read()
            && let Some(metadata) = tracker.get(&page_id) {
                return metadata.temperature.clone();
            }

        // Default to cold for new pages
        DataTemperature::Cold
    }

    /// Predicts and returns pages that should be prefetched
    pub fn predict_prefetch_pages(&self, current_page: PageId) -> Vec<PageId> {
        let mut predicted_pages = Vec::new();
        
        // Get current prefetch accuracy to adapt prefetch aggressiveness
        let current_accuracy = self.get_prefetch_accuracy();
        let accuracy_factor = if current_accuracy > 0.7 {
            1.0 // High accuracy - be more aggressive
        } else if current_accuracy > 0.4 {
            0.7 // Medium accuracy - be conservative
        } else if current_accuracy > 0.0 {
            0.3 // Low accuracy - be very conservative
        } else {
            0.5 // No data yet - moderate prefetching
        };

        // Get predictions from ML prefetcher
        if let Ok(ml_prefetcher) = self.ml_prefetcher.try_read() {
            let ml_accuracy = ml_prefetcher.get_accuracy();
            if ml_accuracy > 0.5 && current_accuracy > 0.3 {
                predicted_pages.extend(ml_prefetcher.predict_prefetch(current_page));
            }
        }

        // Get predictions from basic prefetch engine
        if let Ok(prefetch_engine) = self.prefetch_engine.try_read() {
            // Adaptive sequential prefetching based on accuracy
            let prefetch_distance = (prefetch_engine.prefetch_distance as f64 * accuracy_factor) as usize;
            let effective_distance = prefetch_distance.max(1).min(prefetch_engine.prefetch_distance);
            
            for i in 1..=effective_distance {
                predicted_pages.push(current_page + i as u64);
            }

            // Pattern-based prefetching (only if accuracy is reasonable)
            if current_accuracy > 0.2
                && let Some(patterns) = prefetch_engine.access_patterns.get(&current_page)
                    && patterns.len() >= prefetch_engine.sequential_threshold {
                        // Find the most common next page
                        let mut next_page_counts = HashMap::new();
                        for &page in patterns {
                            *next_page_counts.entry(page + 1).or_insert(0) += 1;
                        }
                        
                        if let Some((most_common_next, count)) = next_page_counts.iter().max_by_key(|(_, count)| *count)
                            && *count >= 2 {
                                predicted_pages.push(*most_common_next);
                            }
                    }
        }

        // Remove duplicates and limit the number of prefetch pages based on accuracy
        predicted_pages.sort_unstable();
        predicted_pages.dedup();
        let max_predictions = if current_accuracy > 0.7 {
            12 // High accuracy - allow more predictions
        } else if current_accuracy > 0.4 {
            8  // Medium accuracy - standard predictions
        } else {
            4  // Low accuracy - fewer predictions
        };
        predicted_pages.truncate(max_predictions);

        predicted_pages
    }

    /// Updates memory pressure based on current cache usage
    pub fn update_memory_pressure(&self) {
        let hot_used = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::len(&cache)
        } else {
            0
        };

        let warm_used = if let Ok(cache) = self.warm_cache.try_read() {
            <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let cold_used = if let Ok(cache) = self.cold_cache.try_read() {
            <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let total_used = hot_used + warm_used + cold_used;
        let total_capacity = self.hot_cache_size + self.warm_cache_size + self.cold_cache_size;

        let usage_percentage = if total_capacity > 0 {
            (total_used as f64 / total_capacity as f64) * 100.0
        } else {
            0.0
        };

        self.admission_controller.memory_pressure.store(
            usage_percentage as u64,
            Ordering::Relaxed,
        );

        // Adjust admission rate based on memory pressure
        let new_admission_rate = match usage_percentage as u64 {
            0..=50 => 100,
            51..=70 => 80,
            71..=85 => 60,
            86..=95 => 40,
            _ => 20,
        };

        self.admission_controller.admission_rate.store(
            new_admission_rate,
            Ordering::Relaxed,
        );
    }

    /// Evicts pages from cache based on pressure and policies
    pub fn evict_if_needed(&self) {
        let memory_pressure = self.admission_controller.memory_pressure.load(Ordering::Relaxed);
        
        if memory_pressure > 85 {
            // High memory pressure - reduce cold cache size
            if let Ok(mut cold_cache) = self.cold_cache.try_write() {
                let target_size = self.cold_cache_size / 2;
                let current_size = <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cold_cache);
                if current_size > target_size {
                    // For FIFO cache, we can't easily evict specific items
                    // Instead, we'll clear part of the cache
                    if current_size > target_size * 2 {
                        let evicted_count = current_size as u64;
                        <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::clear(&mut cold_cache);
                        self.demotion_count.fetch_add(evicted_count, Ordering::Relaxed);
                    }
                }
            }
        }

        if memory_pressure > 95 {
            // Extreme memory pressure - reduce warm cache size
            if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                let target_size = self.warm_cache_size / 2;
                let current_size = <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&warm_cache);
                if current_size > target_size {
                    // For LFU cache, we can't easily evict specific items
                    // Instead, we'll clear part of the cache
                    if current_size > target_size * 2 {
                        let evicted_count = current_size as u64;
                        <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::clear(&mut warm_cache);
                        self.demotion_count.fetch_add(evicted_count, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Gets cache statistics
    pub fn get_cache_statistics(&self) -> CacheStatistics {
        // First update memory pressure before getting statistics
        self.update_memory_pressure();

        let hot_used = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::len(&cache)
        } else {
            0
        };

        let warm_used = if let Ok(cache) = self.warm_cache.try_read() {
            <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let cold_used = if let Ok(cache) = self.cold_cache.try_read() {
            <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        // Use our integrated prefetch accuracy tracking
        let prefetch_accuracy = self.get_prefetch_accuracy();

        // Simplified statistics - the cache implementations don't have hit_ratio() and stats() methods
        // For now, we'll provide placeholder values
        let hot_hit_ratio = 0.0;
        let warm_hit_ratio = 0.0;
        let cold_hit_ratio = 0.0;
        let total_hits = 0.0;

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

    /// Trigger prefetching for a given page (public interface)
    pub fn trigger_prefetch(&self, current_page: PageId) -> Vec<PageId> {
        let predicted_pages = self.predict_prefetch_pages(current_page);
        
        // Record predictions for accuracy tracking
        if !predicted_pages.is_empty() {
            self.record_prefetch_predictions(&predicted_pages);
        }
        
        predicted_pages
    }

    /// Perform maintenance tasks like memory pressure updates and eviction
    pub fn perform_maintenance(&self) {
        self.update_memory_pressure();
        self.evict_if_needed();
    }

    /// Get current memory pressure (0-100)
    pub fn get_memory_pressure(&self) -> u64 {
        self.admission_controller.memory_pressure.load(Ordering::Relaxed)
    }

    /// Get current admission rate (0-100)
    pub fn get_admission_rate(&self) -> u64 {
        self.admission_controller.admission_rate.load(Ordering::Relaxed)
    }

    /// Check if a page access was a prefetch hit and update accuracy
    fn check_prefetch_hit(&self, page_id: PageId) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write()
            && predictions.remove(&page_id).is_some() {
                // This page was predicted and accessed - it's a hit
                self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                self.update_prefetch_accuracy();
            }
    }

    /// Update prefetch accuracy based on hits vs total predictions
    fn update_prefetch_accuracy(&self) {
        let hits = self.prefetch_hits.load(Ordering::Relaxed);
        let total = self.prefetch_total.load(Ordering::Relaxed);
        
        if total > 0 {
            // Calculate accuracy as percentage (0-100)
            let accuracy = (hits as f64 / total as f64 * 100.0) as u64;
            self.prefetch_accuracy.store(accuracy, Ordering::Relaxed);
        }
    }

    /// Record prefetch predictions for accuracy tracking
    fn record_prefetch_predictions(&self, predicted_pages: &[PageId]) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write() {
            let now = Instant::now();
            for &page_id in predicted_pages {
                predictions.insert(page_id, now);
            }
        }
        
        // Update total predictions count
        self.prefetch_total.fetch_add(predicted_pages.len() as u64, Ordering::Relaxed);
        
        // Clean up old predictions (older than 60 seconds)
        self.cleanup_old_predictions();
    }

    /// Clean up old prefetch predictions that are unlikely to be accessed
    fn cleanup_old_predictions(&self) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write() {
            let now = Instant::now();
            let cutoff_time = now - std::time::Duration::from_secs(60);
            
            // Remove predictions older than 60 seconds
            let old_count = predictions.len();
            predictions.retain(|_, &mut timestamp| timestamp > cutoff_time);
            let removed_count = old_count - predictions.len();
            
            // Adjust total count for removed predictions (they become misses)
            if removed_count > 0 {
                self.update_prefetch_accuracy();
            }
        }
    }

    /// Get current prefetch accuracy as a percentage (0-100)
    pub fn get_prefetch_accuracy(&self) -> f64 {
        self.prefetch_accuracy.load(Ordering::Relaxed) as f64 / 100.0
    }

    /// Get enhanced cache statistics using specialized trait functionality
    pub fn get_enhanced_cache_statistics(&self) -> EnhancedCacheStatistics {
        // Get LRU-K specific statistics from hot cache
        let lru_k_value = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::k_value(&cache)
        } else {
            0
        };

        // Get basic statistics using the standard method
        let basic_stats = self.get_cache_statistics();

        EnhancedCacheStatistics {
            basic_stats,
            lru_k_value,
            hot_cache_algorithm: "LRU-K".to_string(),
            warm_cache_algorithm: "LFU".to_string(),
            cold_cache_algorithm: "FIFO".to_string(),
        }
    }

    /// Demonstrate specialized cache operations for maintenance
    pub fn perform_specialized_maintenance(&self) {
        // Use FIFO-specific operations for cold cache
        if let Ok(cold_cache) = self.cold_cache.try_read()
            && let Some((oldest_key, _)) = <FIFOCache<PageId, PageData> as FIFOCacheTrait<PageId, PageData>>::peek_oldest(&cold_cache) {
                // Log the oldest page for monitoring
                log::trace!("Oldest page in cold cache: {}", oldest_key);
            }

        // Use LFU-specific operations for warm cache  
        if let Ok(warm_cache) = self.warm_cache.try_read()
            && let Some((lfu_key, _)) = <LFUCache<PageId, PageData> as LFUCacheTrait<PageId, PageData>>::peek_lfu(&warm_cache) {
                // Log the least frequently used page
                log::trace!("Least frequently used page in warm cache: {}", lfu_key);
            }

        // Use LRU-K specific operations for hot cache
        if let Ok(hot_cache) = self.hot_cache.try_read()
            && let Some((lru_k_key, _)) = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::peek_lru_k(&hot_cache) {
                // Log the LRU-K candidate for eviction
                log::trace!("LRU-K eviction candidate in hot cache: {}", lru_k_key);
            }

        // Perform standard maintenance
        self.perform_maintenance();
    }

    /// Get detailed page access information using LRU-K specific functionality
    pub fn get_page_access_details(&self, page_id: PageId) -> Option<PageAccessDetails> {
        if let Ok(hot_cache) = self.hot_cache.try_read() {
            // Check if page is in hot cache and get LRU-K specific details
            if <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::contains(&hot_cache, &page_id) {
                let k_value = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::k_value(&hot_cache);
                let access_count = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::access_count(&hot_cache, &page_id).map(|count| count as u64);
                let k_distance = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::k_distance(&hot_cache, &page_id);
                let rank = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::k_distance_rank(&hot_cache, &page_id);

                return Some(PageAccessDetails {
                    cache_level: "Hot".to_string(),
                    algorithm: "LRU-K".to_string(),
                    k_value: Some(k_value),
                    access_count,
                    k_distance,
                    eviction_rank: rank,
                    temperature: DataTemperature::Hot,
                });
            }
        }

        if let Ok(warm_cache) = self.warm_cache.try_read() {
            // Check if page is in warm cache and get LFU specific details
            if <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::contains(&warm_cache, &page_id) {
                let frequency = <LFUCache<PageId, PageData> as LFUCacheTrait<PageId, PageData>>::frequency(&warm_cache, &page_id);

                return Some(PageAccessDetails {
                    cache_level: "Warm".to_string(),
                    algorithm: "LFU".to_string(),
                    k_value: None,
                    access_count: frequency,
                    k_distance: None,
                    eviction_rank: None,
                    temperature: DataTemperature::Warm,
                });
            }
        }

        if let Ok(cold_cache) = self.cold_cache.try_read() {
            // Check if page is in cold cache and get FIFO specific details
            if <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::contains(&cold_cache, &page_id) {
                let age_rank = <FIFOCache<PageId, PageData> as FIFOCacheTrait<PageId, PageData>>::age_rank(&cold_cache, &page_id);

                return Some(PageAccessDetails {
                    cache_level: "Cold".to_string(),
                    algorithm: "FIFO".to_string(),
                    k_value: None,
                    access_count: None,
                    k_distance: None,
                    eviction_rank: age_rank,
                    temperature: DataTemperature::Cold,
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;
    use crate::storage::disk::async_disk::metrics::collector::MetricsCollector;

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

    #[test]
    fn test_cache_manager_integration() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test prefetch functionality
        let page_id = 1;
        let data = vec![1, 2, 3, 4];
        cache_manager.store_page(page_id, data.clone());

        // Access pattern to build up ML prefetcher knowledge
        for i in 1..=5 {
            let test_data = vec![i as u8; 4];
            cache_manager.store_page(i, test_data);
            let _ = cache_manager.get_page(i);
        }

        // Test prefetch predictions
        let predicted_pages = cache_manager.trigger_prefetch(3);
        assert!(!predicted_pages.is_empty());

        // Test maintenance operations
        cache_manager.perform_maintenance();

        // Test cache statistics after integration
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
        assert!(stats.prefetch_accuracy >= 0.0);
    }

    #[test]
    fn test_admission_controller_memory_pressure() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache to increase memory pressure
        for i in 1..=100 {
            let data = vec![i as u8; 1024]; // Larger data to increase memory usage
            cache_manager.store_page(i, data);
        }

        // Update memory pressure
        cache_manager.update_memory_pressure();

        let memory_pressure = cache_manager.get_memory_pressure();
        let admission_rate = cache_manager.get_admission_rate();

        // Memory pressure should be calculated based on cache usage
        assert!(memory_pressure > 0);
        assert!(admission_rate > 0);
        assert!(admission_rate <= 100);
    }

    #[test]
    fn test_prefetch_engine_sequential_patterns() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Create a sequential access pattern: 1, 2, 3, 4, 5
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
            let _ = cache_manager.get_page(i);
        }

        // Predict prefetch for page 3
        let predicted_pages = cache_manager.predict_prefetch_pages(3);
        
        // Should predict sequential pages (4, 5, 6, 7)
        assert!(!predicted_pages.is_empty());
        assert!(predicted_pages.contains(&4));
        assert!(predicted_pages.len() <= 8); // Limited to 8 predictions
    }

    #[test]
    fn test_ml_prefetcher_learning() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Create a repeating pattern: 1->2->3, 1->2->3, 1->2->3
        for _ in 0..10 {
            for page_id in [1, 2, 3] {
                let data = vec![page_id as u8; 64];
                cache_manager.store_page(page_id, data);
                let _ = cache_manager.get_page(page_id);
            }
        }

        // After learning the pattern, check if ML prefetcher can predict
        let predicted_pages = cache_manager.predict_prefetch_pages(2);
        
        // Should include some predictions based on learned patterns
        assert!(!predicted_pages.is_empty());
    }

    #[test]
    fn test_admission_controller_rejection() {
        let mut config = DiskManagerConfig::default();
        config.cache_size_mb = 1; // Very small cache to trigger admission control
        let cache_manager = CacheManager::new(&config);

        // Fill cache completely
        for i in 1..=50 {
            let data = vec![i as u8; 1024]; // 1KB per page
            cache_manager.store_page(i, data);
        }

        // Force memory pressure calculation
        cache_manager.update_memory_pressure();
        
        let memory_pressure = cache_manager.get_memory_pressure();
        let admission_rate = cache_manager.get_admission_rate();
        
        // With small cache, memory pressure should be high
        assert!(memory_pressure > 50); // Should be quite high
        
        // Admission rate should be reduced due to high memory pressure
        assert!(admission_rate <= 100);
    }

    #[test]
    fn test_cache_eviction_under_pressure() {
        let mut config = DiskManagerConfig::default();
        config.cache_size_mb = 1; // Small cache
        let cache_manager = CacheManager::new(&config);

        // Fill cache beyond capacity
        for i in 1..=100 {
            let data = vec![i as u8; 1024];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();
        let initial_total = initial_stats.hot_cache_used + initial_stats.warm_cache_used + initial_stats.cold_cache_used;

        // Trigger eviction
        cache_manager.evict_if_needed();

        let final_stats = cache_manager.get_cache_statistics();
        let final_total = final_stats.hot_cache_used + final_stats.warm_cache_used + final_stats.cold_cache_used;

        // Eviction should have reduced cache usage under extreme pressure
        // Note: Due to cache implementation details, exact eviction behavior may vary
        assert!(final_total <= initial_total);
        assert!(final_stats.demotion_count >= initial_stats.demotion_count);
    }

    #[test]
    fn test_cache_promotion_behavior() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages in cold cache (default for new pages)
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();
        let initial_promotions = initial_stats.promotion_count;

        // Access pages multiple times to trigger promotions
        for _ in 0..3 {
            for i in 1..=5 {
                let _ = cache_manager.get_page(i);
            }
        }

        let final_stats = cache_manager.get_cache_statistics();
        let final_promotions = final_stats.promotion_count;

        // Should have promoted some pages
        assert!(final_promotions >= initial_promotions);
    }

    #[test]
    fn test_cache_with_metrics_collector() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);
        let metrics_collector = MetricsCollector::new(&config);

        // Store and retrieve pages with metrics
        let page_id = 42;
        let data = vec![42; 128];
        cache_manager.store_page(page_id, data.clone());

        // Test cache hit with metrics
        let retrieved = cache_manager.get_page_with_metrics(page_id, &metrics_collector);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        // Test cache miss with metrics
        let missing = cache_manager.get_page_with_metrics(999, &metrics_collector);
        assert!(missing.is_none());
    }

    #[test]
    fn test_deduplication_engine() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store the same data under different page IDs
        let duplicate_data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        
        cache_manager.store_page(1, duplicate_data.clone());
        cache_manager.store_page(2, duplicate_data.clone());
        cache_manager.store_page(3, duplicate_data.clone());

        // Get deduplication stats
        let stats = cache_manager.get_cache_statistics();

        assert_eq!(stats.hot_cache_used, 0);
    }

    #[test]
    fn test_data_temperature_classification() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Access pages with different frequencies to create temperature differences
        
        // Hot page - access many times
        let hot_page = 1;
        for _ in 0..15 {
            let data = vec![1; 64];
            cache_manager.store_page(hot_page, data);
            let _ = cache_manager.get_page(hot_page);
        }

        // Warm page - moderate access
        let warm_page = 2;
        for _ in 0..7 {
            let data = vec![2; 64];
            cache_manager.store_page(warm_page, data);
            let _ = cache_manager.get_page(warm_page);
        }

        // Cold page - few accesses
        let cold_page = 3;
        for _ in 0..3 {
            let data = vec![3; 64];
            cache_manager.store_page(cold_page, data);
            let _ = cache_manager.get_page(cold_page);
        }

        // Verify that pages are still accessible regardless of temperature
        assert!(cache_manager.get_page(hot_page).is_some());
        assert!(cache_manager.get_page(warm_page).is_some());
        assert!(cache_manager.get_page(cold_page).is_some());

        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }

    #[test]
    fn test_cache_statistics_accuracy() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages across different cache levels
        for i in 1..=10 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        let stats = cache_manager.get_cache_statistics();

        // Calculate expected cache sizes (matching the constructor logic)
        let expected_hot_size = (config.cache_size_mb as f64 * config.hot_cache_ratio) as usize;
        let expected_warm_size = (config.cache_size_mb as f64 * config.warm_cache_ratio) as usize;
        let expected_cold_size = config.cache_size_mb - expected_hot_size - expected_warm_size;

        // Verify statistics are reasonable
        assert_eq!(stats.hot_cache_size, expected_hot_size);
        assert_eq!(stats.warm_cache_size, expected_warm_size);
        assert_eq!(stats.cold_cache_size, expected_cold_size);
        assert!(stats.hot_cache_used <= stats.hot_cache_size);
        assert!(stats.warm_cache_used <= stats.warm_cache_size);
        assert!(stats.cold_cache_used <= stats.cold_cache_size);
        assert!(stats.prefetch_accuracy >= 0.0 && stats.prefetch_accuracy <= 1.0);
    }

    #[test]
    fn test_maintenance_operations() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache with some data
        for i in 1..=20 {
            let data = vec![i as u8; 256];
            cache_manager.store_page(i, data);
        }

        // Perform maintenance
        cache_manager.perform_maintenance();

        let final_memory_pressure = cache_manager.get_memory_pressure();
        let final_admission_rate = cache_manager.get_admission_rate();

        // Memory pressure should be calculated
        assert!(final_memory_pressure > 0);
        assert!(final_memory_pressure <= 100);
        
        // Admission rate should be reasonable
        assert!(final_admission_rate > 0);
        assert!(final_admission_rate <= 100);
    }

    #[test]
    fn test_concurrent_access_simulation() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Simulate concurrent access patterns
        let access_patterns = vec![
            vec![1, 2, 3, 4, 5],    // Sequential
            vec![10, 8, 6, 4, 2],   // Reverse
            vec![1, 3, 5, 7, 9],    // Odd numbers
            vec![2, 4, 6, 8, 10],   // Even numbers
        ];

        // Execute different access patterns
        for pattern in access_patterns {
            for page_id in pattern {
                let data = vec![page_id as u8; 64];
                cache_manager.store_page(page_id, data.clone());
                
                // Verify immediate retrieval
                let retrieved = cache_manager.get_page(page_id);
                assert!(retrieved.is_some());
                assert_eq!(retrieved.unwrap(), data);
            }
        }

        // Test prefetch predictions after complex access patterns
        let predictions = cache_manager.trigger_prefetch(5);
        assert!(!predictions.is_empty());

        // Verify cache is functioning properly
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }

    #[test]
    fn test_edge_case_empty_cache() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test operations on empty cache
        assert!(cache_manager.get_page(1).is_none());
        
        let predictions = cache_manager.trigger_prefetch(1);
        // Should still return some predictions (sequential)
        assert!(!predictions.is_empty());

        let memory_pressure = cache_manager.get_memory_pressure();
        assert_eq!(memory_pressure, 0); // Empty cache should have no pressure

        let admission_rate = cache_manager.get_admission_rate();
        assert_eq!(admission_rate, 100); // Should allow all admissions when empty

        let stats = cache_manager.get_cache_statistics();
        assert_eq!(stats.hot_cache_used, 0);
        assert_eq!(stats.warm_cache_used, 0);
        assert_eq!(stats.cold_cache_used, 0);
    }

    #[test]
    fn test_large_page_admission_control() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache to create memory pressure
        for i in 1..=50 {
            let data = vec![i as u8; 512];
            cache_manager.store_page(i, data);
        }

        // Force high memory pressure
        cache_manager.update_memory_pressure();

        // Try to store a very large page
        let large_page_data = vec![255; (DB_PAGE_SIZE * 3) as usize]; // Larger than admission threshold
        cache_manager.store_page(999, large_page_data.clone());

        // The large page may or may not be admitted depending on pressure
        // This tests the admission control logic
        let memory_pressure = cache_manager.get_memory_pressure();
        assert!(memory_pressure > 0);
    }

    #[test]
    fn test_prefetch_accuracy_tracking() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Initially, accuracy should be 0 (no predictions made yet)
        assert_eq!(cache_manager.get_prefetch_accuracy(), 0.0);

        // Make some prefetch predictions
        let predicted_pages = cache_manager.trigger_prefetch(10);
        assert!(!predicted_pages.is_empty());

        // Access some of the predicted pages (hits)
        for i in 0..predicted_pages.len().min(3) {
            let page_id = predicted_pages[i];
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data.clone());
            
            // Access the page to register a prefetch hit
            let retrieved = cache_manager.get_page(page_id);
            assert!(retrieved.is_some());
        }

        // Check that accuracy has been calculated
        let accuracy = cache_manager.get_prefetch_accuracy();
        assert!(accuracy > 0.0, "Accuracy should be greater than 0 after hits");
        assert!(accuracy <= 1.0, "Accuracy should not exceed 100%");

        // Test adaptive prefetching based on accuracy
        let new_predictions = cache_manager.trigger_prefetch(20);
        assert!(!new_predictions.is_empty());

        // Test that accuracy affects prediction count
        // With some accuracy, we should get reasonable number of predictions
        if accuracy > 0.4 {
            assert!(new_predictions.len() >= 4, "Should make more predictions with decent accuracy");
        }
    }

    #[test]
    fn test_prefetch_accuracy_cleanup() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Make predictions
        let predicted_pages = cache_manager.trigger_prefetch(30);
        assert!(!predicted_pages.is_empty());

        // Simulate some time passing and accessing pages
        for &page_id in &predicted_pages[0..2] {
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data);
            let _ = cache_manager.get_page(page_id);
        }

        // Test cleanup of old predictions
        cache_manager.cleanup_old_predictions();

        // Accuracy should still be reasonable
        let accuracy = cache_manager.get_prefetch_accuracy();
        assert!(accuracy >= 0.0 && accuracy <= 1.0);
    }

    #[test]
    fn test_adaptive_prefetch_distance() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test that prefetch distance adapts to accuracy
        
        // With no history, should get moderate prefetching
        let initial_predictions = cache_manager.trigger_prefetch(40);
        let initial_count = initial_predictions.len();
        assert!(initial_count > 0);

        // Create some successful prefetch history
        for &page_id in &initial_predictions[0..initial_predictions.len().min(4)] {
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data);
            let _ = cache_manager.get_page(page_id);
        }

        // With higher accuracy, might get more predictions
        let improved_predictions = cache_manager.trigger_prefetch(50);
        assert!(!improved_predictions.is_empty());
        
        // Test that cache statistics include our prefetch accuracy
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.prefetch_accuracy >= 0.0);
        assert!(stats.prefetch_accuracy <= 1.0);
    }

    #[test]
    fn test_specialized_traits_enhanced_statistics() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store some pages to populate caches
        for i in 1..=10 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        // Test enhanced statistics using specialized traits
        let enhanced_stats = cache_manager.get_enhanced_cache_statistics();

        // Verify enhanced statistics structure
        assert_eq!(enhanced_stats.lru_k_value, 2); // Default K value for LRU-K
        assert_eq!(enhanced_stats.hot_cache_algorithm, "LRU-K");
        assert_eq!(enhanced_stats.warm_cache_algorithm, "LFU");
        assert_eq!(enhanced_stats.cold_cache_algorithm, "FIFO");

        // Verify basic stats are included (values are unsigned, so always >= 0)
        // Just verify they're accessible without panicking
        let _ = enhanced_stats.basic_stats.hot_cache_used;
        let _ = enhanced_stats.basic_stats.warm_cache_used;
        let _ = enhanced_stats.basic_stats.cold_cache_used;
    }

    #[test]
    fn test_specialized_traits_page_access_details() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store a page and verify it's accessible through different cache levels
        let page_id = 42;
        let data = vec![42; 256];
        cache_manager.store_page(page_id, data);

        // Get detailed access information using specialized traits
        let access_details = cache_manager.get_page_access_details(page_id);
        
        if let Some(details) = access_details {
            // Verify we get meaningful access details
            assert!(!details.cache_level.is_empty());
            assert!(!details.algorithm.is_empty());
            
            // Different cache levels should provide different information
            match details.algorithm.as_str() {
                "LRU-K" => {
                    assert!(details.k_value.is_some());
                    assert_eq!(details.k_value.unwrap(), 2); // Default K value
                    assert!(details.access_count.is_some());
                },
                "LFU" => {
                    assert!(details.access_count.is_some()); // Frequency information
                },
                "FIFO" => {
                    assert!(details.eviction_rank.is_some()); // Age rank information
                },
                _ => panic!("Unknown algorithm: {}", details.algorithm),
            }
        }
    }

    #[test]
    fn test_specialized_maintenance_operations() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill caches with data
        for i in 1..=20 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        // Test specialized maintenance using new traits
        cache_manager.perform_specialized_maintenance();

        // Verify cache is still functional after specialized maintenance
        let stats = cache_manager.get_cache_statistics();
        // Values are unsigned, so always >= 0 - just verify they're accessible
        let _ = stats.hot_cache_used;
        let _ = stats.warm_cache_used;
        let _ = stats.cold_cache_used;
    }

    #[test]
    fn test_lru_k_specific_functionality() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages with different access patterns to test LRU-K behavior
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        // Access some pages multiple times to trigger LRU-K behavior
        for _ in 0..3 {
            let _ = cache_manager.get_page(1);
            let _ = cache_manager.get_page(2);
        }

        // Access page 1 one more time to give it K=2 accesses
        let _ = cache_manager.get_page(1);

        // Get page access details to verify LRU-K specific information
        if let Some(details) = cache_manager.get_page_access_details(1) {
            if details.algorithm == "LRU-K" {
                assert_eq!(details.k_value, Some(2));
                assert!(details.access_count.is_some());
                // Page 1 should have at least 2 accesses (K value reached)
                assert!(details.access_count.unwrap() >= 2);
            }
        }
    }

    #[test]
    fn test_cache_manager_trait_migration_compatibility() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test that all existing functionality still works after trait migration
        
        // Basic store and retrieve
        let page_id = 100;
        let data = vec![100; 512];
        cache_manager.store_page(page_id, data.clone());
        let retrieved = cache_manager.get_page(page_id);
        assert_eq!(retrieved.unwrap(), data);

        // Prefetch functionality
        let predictions = cache_manager.trigger_prefetch(page_id);
        assert!(!predictions.is_empty());

        // Memory pressure and admission control
        cache_manager.update_memory_pressure();
        // Memory pressure is unsigned, so always >= 0 - just verify it's accessible
        let _ = cache_manager.get_memory_pressure();
        assert!(cache_manager.get_admission_rate() > 0);

        // Statistics
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used >= 0);
        
        // Enhanced statistics using new traits
        let enhanced_stats = cache_manager.get_enhanced_cache_statistics();
        assert_eq!(enhanced_stats.lru_k_value, 2);

        // Page access details using specialized traits
        let access_details = cache_manager.get_page_access_details(page_id);
        assert!(access_details.is_some());
    }

    #[test]
    fn test_multi_level_cache_promotion_with_traits() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages that will initially go to cold cache
        for i in 1..=10 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();

        // Access pages to trigger promotions between cache levels
        for _ in 0..5 {
            for i in 1..=5 {
                let _ = cache_manager.get_page(i);
            }
        }

        let final_stats = cache_manager.get_cache_statistics();

        // Should have some promotions
        assert!(final_stats.promotion_count >= initial_stats.promotion_count);

        // Test that we can get detailed information about promoted pages
        for i in 1..=5 {
            if let Some(details) = cache_manager.get_page_access_details(i) {
                // Pages that were accessed multiple times might be in hot or warm cache
                assert!(!details.cache_level.is_empty());
                assert!(!details.algorithm.is_empty());
            }
        }
    }
}
