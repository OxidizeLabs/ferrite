use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};

/// FIFO (First In, First Out) Cache implementation.
#[derive(Debug)]
pub struct FIFOCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    cache: HashMap<K, V>,
    insertion_order: VecDeque<K>, // Tracks the order of insertion
}

impl<K, V> FIFOCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new FIFO cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        FIFOCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
        }
    }

    /// Evicts the oldest valid entry from the cache.
    /// Skips over any stale entries (keys that were lazily deleted).
    fn evict_oldest(&mut self) {
        // Keep popping from front until we find a valid key or the queue is empty
        while let Some(oldest_key) = self.insertion_order.pop_front() {
            if self.cache.contains_key(&oldest_key) {
                // Found a valid key, remove it and stop
                self.cache.remove(&oldest_key);
                break;
            }
            // Skip stale entries (keys that were already removed from cache)
        }
    }
}

// ========================================
// CACHE TRAITS IMPLEMENTATION
// Phase 1: Implement new traits alongside existing Cache trait
// ========================================

impl<K, V> CoreCache<K, V> for FIFOCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        // If capacity is 0, cannot store anything
        if self.capacity == 0 {
            return None;
        }

        // If key already exists, just update the value
        if self.cache.contains_key(&key) {
            return self.cache.insert(key, value);
        }

        // If cache is at capacity, remove the oldest valid item (FIFO)
        if self.cache.len() >= self.capacity {
            self.evict_oldest();
        }

        // Add the new key to the insertion order and cache
        self.insertion_order.push_back(key.clone());
        self.cache.insert(key, value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        // In FIFO, getting an item doesn't change its position
        self.cache.get(key)
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.insertion_order.clear();
    }
}

impl<K, V> FIFOCacheTrait<K, V> for FIFOCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn pop_oldest(&mut self) -> Option<(K, V)> {
        // Use the existing evict_oldest logic but return the key-value pair
        while let Some(oldest_key) = self.insertion_order.pop_front() {
            if let Some(value) = self.cache.remove(&oldest_key) {
                return Some((oldest_key, value));
            }
            // Skip stale entries (keys that were already removed from cache)
        }
        None
    }

    fn peek_oldest(&self) -> Option<(&K, &V)> {
        // Find the first valid entry in insertion order
        for key in &self.insertion_order {
            if let Some(value) = self.cache.get(key) {
                return Some((key, value));
            }
        }
        None
    }

    fn age_rank(&self, key: &K) -> Option<usize> {
        // Find position in insertion order, accounting for stale entries
        let mut rank = 0;
        for insertion_key in &self.insertion_order {
            if self.cache.contains_key(insertion_key) {
                if insertion_key == key {
                    return Some(rank);
                }
                rank += 1;
            }
        }
        None
    }

    fn pop_oldest_batch(&mut self, count: usize) -> Vec<(K, V)> {
        let mut result = Vec::with_capacity(count.min(<Self as CoreCache<K, V>>::len(self)));
        for _ in 0..count {
            if let Some(entry) = self.pop_oldest() {
                result.push(entry);
            } else {
                break;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};

    // ==============================================
    // CORRECTNESS TESTS MODULE
    // ==============================================
    mod correctness {
        use super::*;

        // Basic FIFO Behavior Tests
        mod basic_behavior {
            use super::*;

            #[test]
            fn test_basic_fifo_insertion_and_retrieval() {
                let mut cache = FIFOCache::new(3);
                
                // Test basic insertion and retrieval
                assert_eq!(cache.insert("key1", "value1"), None);
                assert_eq!(cache.insert("key2", "value2"), None);
                assert_eq!(cache.insert("key3", "value3"), None);
                
                assert_eq!(cache.get(&"key1"), Some(&"value1"));
                assert_eq!(cache.get(&"key2"), Some(&"value2"));
                assert_eq!(cache.get(&"key3"), Some(&"value3"));
                assert_eq!(cache.len(), 3);
            }

            #[test]
            fn test_fifo_eviction_order() {
                let mut cache = FIFOCache::new(2);
                
                // Fill cache to capacity
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                assert_eq!(cache.len(), 2);
                
                // Insert third item - should evict "first" (oldest)
                cache.insert("third", "value3");
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"first"));  // First item should be evicted
                assert!(cache.contains(&"second"));
                assert!(cache.contains(&"third"));
            }

            #[test]
            fn test_capacity_enforcement() {
                let mut cache = FIFOCache::new(3);
                
                // Fill beyond capacity
                for i in 1..=5 {
                    cache.insert(format!("key{}", i), format!("value{}", i));
                }
                
                // Should only contain last 3 items due to FIFO eviction
                assert_eq!(cache.len(), 3);
                assert!(!cache.contains(&"key1".to_string()));
                assert!(!cache.contains(&"key2".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                assert!(cache.contains(&"key4".to_string()));
                assert!(cache.contains(&"key5".to_string()));
            }

            #[test]
            fn test_update_existing_key() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "original");
                cache.insert("key2", "value2");
                
                // Update existing key - should return old value
                let old_value = cache.insert("key1", "updated");
                assert_eq!(old_value, Some("original"));
                assert_eq!(cache.get(&"key1"), Some(&"updated"));
                assert_eq!(cache.len(), 2); // Length shouldn't change
            }

            #[test]
            fn test_insertion_order_preservation() {
                // TODO: Test that items maintain correct insertion order across operations
                todo!("Implement insertion order preservation test")
            }

            #[test]
            fn test_key_operations_consistency() {
                // TODO: Test consistency of insert, get, contains, len operations
                todo!("Implement key operations consistency test")
            }
        }

        // Edge Cases Tests
        mod edge_cases {
            use super::*;

            #[test]
            fn test_empty_cache_operations() {
                let mut cache: FIFOCache<String, String> = FIFOCache::new(5);
                
                assert_eq!(cache.get(&"nonexistent".to_string()), None);
                assert!(!cache.contains(&"nonexistent".to_string()));
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 5);
                
                // Clear empty cache should work
                cache.clear();
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_single_item_cache() {
                let mut cache = FIFOCache::new(1);
                
                cache.insert("only", "value1");
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.get(&"only"), Some(&"value1"));
                
                // Insert second item - should evict first
                cache.insert("second", "value2");
                assert_eq!(cache.len(), 1);
                assert!(!cache.contains(&"only"));
                assert!(cache.contains(&"second"));
            }

            #[test]
            fn test_zero_capacity_cache() {
                let mut cache = FIFOCache::new(0);
                
                // Should not be able to store anything
                cache.insert("key", "value");
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key"));
                assert_eq!(cache.get(&"key"), None);
            }

            #[test]
            fn test_clear_operation() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                assert_eq!(cache.len(), 2);
                
                cache.clear();
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key1"));
                assert!(!cache.contains(&"key2"));
                assert_eq!(cache.get(&"key1"), None);
            }

            #[test]
            fn test_duplicate_key_handling() {
                // TODO: Test various scenarios with duplicate key insertions
                todo!("Implement duplicate key handling test")
            }

            #[test]
            fn test_boundary_conditions() {
                // TODO: Test operations at exactly capacity limit
                todo!("Implement boundary conditions test")
            }

            #[test]
            fn test_empty_to_full_transition() {
                // TODO: Test cache behavior during empty->full transition
                todo!("Implement empty to full transition test")
            }

            #[test]
            fn test_full_to_empty_transition() {
                // TODO: Test cache behavior during full->empty transition
                todo!("Implement full to empty transition test")
            }
        }

        // FIFO Trait Methods Tests
        mod trait_methods {
            use super::*;

            #[test]
            fn test_pop_oldest() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                cache.insert("third", "value3");
                
                // Pop oldest should return first inserted
                assert_eq!(cache.pop_oldest(), Some(("first", "value1")));
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"first"));
                
                // Next pop should return second oldest
                assert_eq!(cache.pop_oldest(), Some(("second", "value2")));
                assert_eq!(cache.len(), 1);
            }

            #[test]
            fn test_peek_oldest() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                
                // Peek should return oldest without removing
                assert_eq!(cache.peek_oldest(), Some((&"first", &"value1")));
                assert_eq!(cache.len(), 2); // Should not change length
                assert!(cache.contains(&"first")); // Should still be present
                
                // Multiple peeks should return same result
                assert_eq!(cache.peek_oldest(), Some((&"first", &"value1")));
            }

            #[test]
            fn test_age_rank() {
                let mut cache = FIFOCache::new(4);
                
                cache.insert("first", "value1");   // rank 0 (oldest)
                cache.insert("second", "value2");  // rank 1
                cache.insert("third", "value3");   // rank 2
                cache.insert("fourth", "value4");  // rank 3 (newest)
                
                assert_eq!(cache.age_rank(&"first"), Some(0));
                assert_eq!(cache.age_rank(&"second"), Some(1));
                assert_eq!(cache.age_rank(&"third"), Some(2));
                assert_eq!(cache.age_rank(&"fourth"), Some(3));
                assert_eq!(cache.age_rank(&"nonexistent"), None);
            }

            #[test]
            fn test_pop_oldest_batch() {
                let mut cache = FIFOCache::new(5);
                
                for i in 1..=5 {
                    cache.insert(format!("key{}", i), format!("value{}", i));
                }
                
                // Pop batch of 3 items
                let batch = cache.pop_oldest_batch(3);
                assert_eq!(batch.len(), 3);
                assert_eq!(batch[0], ("key1".to_string(), "value1".to_string()));
                assert_eq!(batch[1], ("key2".to_string(), "value2".to_string()));
                assert_eq!(batch[2], ("key3".to_string(), "value3".to_string()));
                
                // Cache should have 2 items left
                assert_eq!(cache.len(), 2);
                assert!(cache.contains(&"key4".to_string()));
                assert!(cache.contains(&"key5".to_string()));
            }

            #[test]
            fn test_pop_oldest_batch_more_than_available() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                
                // Request more than available
                let batch = cache.pop_oldest_batch(5);
                assert_eq!(batch.len(), 2); // Should only return what's available
                assert_eq!(cache.len(), 0); // Cache should be empty
            }

            #[test]
            fn test_pop_oldest_empty_cache() {
                // TODO: Test pop_oldest on empty cache
                todo!("Implement pop_oldest empty cache test")
            }

            #[test]
            fn test_peek_oldest_empty_cache() {
                // TODO: Test peek_oldest on empty cache
                todo!("Implement peek_oldest empty cache test")
            }

            #[test]
            fn test_age_rank_after_eviction() {
                // TODO: Test age_rank correctness after evictions
                todo!("Implement age_rank after eviction test")
            }

            #[test]
            fn test_batch_operations_edge_cases() {
                // TODO: Test pop_oldest_batch with count=0, negative, etc.
                todo!("Implement batch operations edge cases test")
            }
        }

        // Stale Entry Handling Tests
        mod stale_entries {
            use super::*;

            #[test]
            fn test_stale_entry_skipping_during_eviction() {
                // TODO: Test that eviction skips over stale entries correctly
                todo!("Implement stale entry skipping during eviction test")
            }

            #[test]
            fn test_insertion_order_consistency_with_stale_entries() {
                // TODO: Test insertion order queue consistency when stale entries exist
                todo!("Implement insertion order consistency with stale entries test")
            }

            #[test]
            fn test_lazy_deletion_behavior() {
                // TODO: Test lazy deletion patterns and cleanup
                todo!("Implement lazy deletion behavior test")
            }

            #[test]
            fn test_stale_entry_cleanup_during_operations() {
                // TODO: Test that stale entries are cleaned up during various operations
                todo!("Implement stale entry cleanup during operations test")
            }
        }
    }

    // ==============================================
    // ALGORITHMIC COMPLEXITY TESTS MODULE
    // ==============================================
    mod complexity {
        use super::*;
        use std::time::Instant;

        // Time Complexity Tests
        mod time_complexity {
            use super::*;

            #[test]
            fn test_insert_time_complexity() {
                // TODO: Verify O(1) insert time complexity
                todo!("Implement insert time complexity test")
            }

            #[test]
            fn test_get_time_complexity() {
                // TODO: Verify O(1) get time complexity
                todo!("Implement get time complexity test")
            }

            #[test]
            fn test_eviction_time_complexity() {
                // TODO: Verify O(n) worst case eviction time (due to stale entries)
                todo!("Implement eviction time complexity test")
            }

            #[test]
            fn test_age_rank_time_complexity() {
                // TODO: Verify O(n) age_rank time complexity
                todo!("Implement age_rank time complexity test")
            }

            #[test]
            fn test_contains_time_complexity() {
                // TODO: Verify O(1) contains time complexity
                todo!("Implement contains time complexity test")
            }

            #[test]
            fn test_clear_time_complexity() {
                // TODO: Verify O(1) clear time complexity
                todo!("Implement clear time complexity test")
            }
        }

        // Space Complexity Tests
        mod space_complexity {
            use super::*;

            #[test]
            fn test_memory_usage_patterns() {
                // TODO: Verify O(capacity) space usage
                todo!("Implement memory usage patterns test")
            }

            #[test]
            fn test_overhead_analysis() {
                // TODO: Measure HashMap + VecDeque overhead
                todo!("Implement overhead analysis test")
            }

            #[test]
            fn test_memory_leak_detection() {
                // TODO: Ensure proper cleanup and no memory leaks
                todo!("Implement memory leak detection test")
            }

            #[test]
            fn test_growth_patterns() {
                // TODO: Test memory growth as cache size increases
                todo!("Implement growth patterns test")
            }
        }

        // Performance Benchmark Tests
        mod performance {
            use super::*;

            #[test]
            fn test_small_cache_performance() {
                // TODO: Benchmark with cache size 100
                todo!("Implement small cache performance test")
            }

            #[test]
            fn test_medium_cache_performance() {
                // TODO: Benchmark with cache size 1K
                todo!("Implement medium cache performance test")
            }

            #[test]
            fn test_large_cache_performance() {
                // TODO: Benchmark with cache size 10K
                todo!("Implement large cache performance test")
            }

            #[test]
            fn test_very_large_cache_performance() {
                // TODO: Benchmark with cache size 100K
                todo!("Implement very large cache performance test")
            }

            #[test]
            fn test_sequential_access_pattern() {
                // TODO: Benchmark sequential access patterns
                todo!("Implement sequential access pattern test")
            }

            #[test]
            fn test_random_access_pattern() {
                // TODO: Benchmark random access patterns
                todo!("Implement random access pattern test")
            }

            #[test]
            fn test_mixed_workload_performance() {
                // TODO: Benchmark mixed read/write workloads
                todo!("Implement mixed workload performance test")
            }

            #[test]
            fn test_heavy_eviction_performance() {
                // TODO: Benchmark performance under heavy eviction scenarios
                todo!("Implement heavy eviction performance test")
            }

            #[test]
            fn test_light_eviction_performance() {
                // TODO: Benchmark performance with minimal evictions
                todo!("Implement light eviction performance test")
            }
        }
    }

    // ==============================================
    // CONCURRENCY TESTS MODULE
    // ==============================================
    mod concurrency {
        use super::*;
        use std::sync::{Arc, Mutex};
        use std::thread;

        // Thread Safety Analysis Tests
        mod thread_safety {
            use super::*;

            #[test]
            fn test_identify_race_conditions() {
                // TODO: Demonstrate that current impl is NOT thread-safe
                todo!("Implement race condition identification test")
            }

            #[test]
            fn test_concurrent_hashmap_access() {
                // TODO: Test concurrent HashMap access issues
                todo!("Implement concurrent hashmap access test")
            }

            #[test]
            fn test_concurrent_vecdeque_access() {
                // TODO: Test concurrent VecDeque access issues
                todo!("Implement concurrent vecdeque access test")
            }

            #[test]
            fn test_data_corruption_scenarios() {
                // TODO: Test scenarios that could lead to data corruption
                todo!("Implement data corruption scenarios test")
            }

            #[test]
            fn test_inconsistent_state_detection() {
                // TODO: Test detection of inconsistent internal state
                todo!("Implement inconsistent state detection test")
            }
        }

        // Thread-Safe Wrapper Tests
        mod thread_safe_wrapper {
            use super::*;

            // Helper type for thread-safe testing
            type ThreadSafeFIFOCache<K, V> = Arc<Mutex<FIFOCache<K, V>>>;

            #[test]
            fn test_basic_thread_safe_operations() {
                // TODO: Test basic operations with mutex wrapper
                todo!("Implement basic thread safe operations test")
            }

            #[test]
            fn test_read_heavy_workload() {
                // TODO: Test multiple readers with occasional writers
                todo!("Implement read heavy workload test")
            }

            #[test]
            fn test_write_heavy_workload() {
                // TODO: Test concurrent insertions and evictions
                todo!("Implement write heavy workload test")
            }

            #[test]
            fn test_mixed_operations_concurrency() {
                // TODO: Test random mix of all operations across threads
                todo!("Implement mixed operations concurrency test")
            }

            #[test]
            fn test_deadlock_prevention() {
                // TODO: Test that operations don't cause deadlocks
                todo!("Implement deadlock prevention test")
            }

            #[test]
            fn test_fairness_across_threads() {
                // TODO: Test that all threads get fair access
                todo!("Implement fairness across threads test")
            }
        }

        // Stress Testing
        mod stress_testing {
            use super::*;

            #[test]
            fn test_high_contention_scenario() {
                // TODO: Many threads accessing same keys
                todo!("Implement high contention scenario test")
            }

            #[test]
            fn test_cache_thrashing_scenario() {
                // TODO: Rapid insertions causing frequent evictions
                todo!("Implement cache thrashing scenario test")
            }

            #[test]
            fn test_long_running_stability() {
                // TODO: Verify stability over extended periods
                todo!("Implement long running stability test")
            }

            #[test]
            fn test_memory_pressure_scenario() {
                // TODO: Test behavior under memory pressure
                todo!("Implement memory pressure scenario test")
            }

            #[test]
            fn test_rapid_thread_creation_destruction() {
                // TODO: Test with threads being created/destroyed rapidly
                todo!("Implement rapid thread creation destruction test")
            }

            #[test]
            fn test_burst_load_handling() {
                // TODO: Test handling of sudden burst loads
                todo!("Implement burst load handling test")
            }

            #[test]
            fn test_gradual_load_increase() {
                // TODO: Test behavior as load gradually increases
                todo!("Implement gradual load increase test")
            }
        }
    }
}
