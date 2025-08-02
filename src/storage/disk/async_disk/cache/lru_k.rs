use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait, MutableCache};

/// LRU-K Cache implementation.
///
/// This cache evicts the item whose K-th most recent access is furthest in the past.
#[derive(Debug)]
pub struct LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    k: usize,
    cache: HashMap<K, (V, VecDeque<u64>)>, // (value, access history)
}

impl<K, V> LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new LRU-K cache with default K=2.
    pub fn new(capacity: usize) -> Self {
        Self::with_k(capacity, 2)
    }

    /// Creates a new LRU-K cache with the specified capacity and K value.
    pub fn with_k(capacity: usize, k: usize) -> Self {
        LRUKCache {
            capacity,
            k,
            cache: HashMap::with_capacity(capacity),
        }
    }

    /// Gets the current time in microseconds.
    fn current_time_in_micros() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}



// Implementation of the new specialized traits
impl<K, V> CoreCache<K, V> for LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let now = Self::current_time_in_micros();

        // If key already exists, update its value and access history
        if let Some((old_value, history)) = self.cache.get_mut(&key) {
            let old_value_clone = old_value.clone();
            *old_value = value;

            // Update access history
            history.push_back(now);
            if history.len() > self.k {
                history.pop_front();
            }

            return Some(old_value_clone);
        }

        // If cache is at capacity, evict based on LRU-K policy
        if self.cache.len() >= self.capacity && !self.cache.is_empty() {
            let mut victim_key = None;
            let mut min_k_accesses = usize::MAX;
            let mut oldest_k_access = u64::MAX;

            for (k, (_, history)) in &self.cache {
                let num_accesses = history.len();

                // First prioritize items with fewer than k accesses
                if num_accesses < self.k {
                    if victim_key.is_none() || num_accesses < min_k_accesses {
                        min_k_accesses = num_accesses;
                        oldest_k_access = history.front().copied().unwrap_or(u64::MAX);
                        victim_key = Some(k.clone());
                    } else if num_accesses == min_k_accesses {
                        // If same number of accesses, choose the one with earlier first access
                        let first_access = history.front().copied().unwrap_or(u64::MAX);
                        if first_access < oldest_k_access {
                            oldest_k_access = first_access;
                            victim_key = Some(k.clone());
                        }
                    }
                } else if min_k_accesses >= self.k {
                    // For items with k or more accesses, compare their k-th most recent access
                    let k_access = history
                        .get(num_accesses - self.k)
                        .copied()
                        .unwrap_or(u64::MAX);

                    if victim_key.is_none() || k_access < oldest_k_access {
                        min_k_accesses = self.k;
                        oldest_k_access = k_access;
                        victim_key = Some(k.clone());
                    }
                }
            }

            if let Some(key_to_remove) = victim_key {
                self.cache.remove(&key_to_remove);
            }
        }

        // Initialize access history with current time
        let mut history = VecDeque::with_capacity(self.k);
        history.push_back(now);

        // Insert the new item
        self.cache.insert(key, (value, history)).map(|(v, _)| v)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        let now = Self::current_time_in_micros();

        if let Some((_, history)) = self.cache.get_mut(key) {
            // Update access history
            history.push_back(now);
            if history.len() > self.k {
                history.pop_front();
            }
        }

        self.cache.get(key).map(|(v, _)| v)
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
        self.cache.clear()
    }
}

impl<K, V> MutableCache<K, V> for LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn remove(&mut self, key: &K) -> Option<V> {
        self.cache.remove(key).map(|(v, _)| v)
    }
}

impl<K, V> LRUKCacheTrait<K, V> for LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn pop_lru_k(&mut self) -> Option<(K, V)> {
        if self.cache.is_empty() {
            return None;
        }

        // Find the victim key using the same logic as the insert method
            let mut victim_key = None;
            let mut min_k_accesses = usize::MAX;
            let mut oldest_k_access = u64::MAX;

            for (k, (_, history)) in &self.cache {
                let num_accesses = history.len();

                // First prioritize items with fewer than k accesses
                if num_accesses < self.k {
                    if victim_key.is_none() || num_accesses < min_k_accesses {
                        min_k_accesses = num_accesses;
                        oldest_k_access = history.front().copied().unwrap_or(u64::MAX);
                        victim_key = Some(k.clone());
                    } else if num_accesses == min_k_accesses {
                        // If same number of accesses, choose the one with earlier first access
                        let first_access = history.front().copied().unwrap_or(u64::MAX);
                        if first_access < oldest_k_access {
                            oldest_k_access = first_access;
                            victim_key = Some(k.clone());
                        }
                    }
                } else if min_k_accesses >= self.k {
                    // For items with k or more accesses, compare their k-th most recent access
                    let k_access = history
                        .get(num_accesses - self.k)
                        .copied()
                        .unwrap_or(u64::MAX);

                    if victim_key.is_none() || k_access < oldest_k_access {
                        min_k_accesses = self.k;
                        oldest_k_access = k_access;
                        victim_key = Some(k.clone());
                    }
                }
            }

        // Remove and return the victim
        victim_key.and_then(|key| {
            self.cache.remove(&key).map(|(value, _)| (key, value))
        })
    }
    
    fn peek_lru_k(&self) -> Option<(&K, &V)> {
        if self.cache.is_empty() {
            return None;
        }

        let mut victim_key = None;
        let mut min_k_accesses = usize::MAX;
        let mut oldest_k_access = u64::MAX;

        for (k, (_, history)) in &self.cache {
            let num_accesses = history.len();

            if num_accesses < self.k {
                if victim_key.is_none() || num_accesses < min_k_accesses {
                    min_k_accesses = num_accesses;
                    oldest_k_access = history.front().copied().unwrap_or(u64::MAX);
                    victim_key = Some(k);
                } else if num_accesses == min_k_accesses {
                    let first_access = history.front().copied().unwrap_or(u64::MAX);
                    if first_access < oldest_k_access {
                        oldest_k_access = first_access;
                        victim_key = Some(k);
                    }
                }
            } else if min_k_accesses >= self.k {
                let k_access = history
                    .get(num_accesses - self.k)
                    .copied()
                    .unwrap_or(u64::MAX);

                if victim_key.is_none() || k_access < oldest_k_access {
                    min_k_accesses = self.k;
                    oldest_k_access = k_access;
                    victim_key = Some(k);
                }
            }
        }

        victim_key.and_then(|key| {
            self.cache.get(key).map(|(value, _)| (key, value))
        })
    }
    
    fn k_value(&self) -> usize {
        self.k
    }
    
    fn access_history(&self, key: &K) -> Option<Vec<u64>> {
        self.cache.get(key).map(|(_, history)| {
            history.iter().rev().copied().collect() // Most recent first
        })
    }
    
    fn access_count(&self, key: &K) -> Option<usize> {
        self.cache.get(key).map(|(_, history)| history.len())
    }
    
    fn k_distance(&self, key: &K) -> Option<u64> {
        self.cache.get(key).and_then(|(_, history)| {
            if history.len() >= self.k {
                // Get the K-th most recent access (index from the end)
                history.get(history.len() - self.k).copied()
            } else {
                None
            }
        })
    }
    
    fn touch(&mut self, key: &K) -> bool {
        if let Some((_, history)) = self.cache.get_mut(key) {
            let now = Self::current_time_in_micros();
            history.push_back(now);
            if history.len() > self.k {
                history.pop_front();
            }
            true
        } else {
            false
        }
    }
    
    fn k_distance_rank(&self, key: &K) -> Option<usize> {
        if !self.cache.contains_key(key) {
            return None;
        }

        let mut items_with_distances: Vec<(bool, u64)> = Vec::new();
        
        for (_k, (_, history)) in &self.cache {
            let num_accesses = history.len();
            
            if num_accesses < self.k {
                // Items with fewer than K accesses use their earliest access time
                let earliest = history.front().copied().unwrap_or(u64::MAX);
                items_with_distances.push((false, earliest)); // false = not full K accesses
            } else {
                // Items with K or more accesses use their K-distance
                let k_distance = history.get(num_accesses - self.k).copied().unwrap_or(u64::MAX);
                items_with_distances.push((true, k_distance)); // true = has full K accesses
            }
        }

        // Sort by priority: items with fewer than K accesses first (by earliest access),
        // then items with K+ accesses (by K-distance)
        items_with_distances.sort_by(|a, b| {
            match (a.0, b.0) {
                (false, false) => a.1.cmp(&b.1), // Both have < K accesses, sort by earliest
                (true, true) => a.1.cmp(&b.1),   // Both have >= K accesses, sort by K-distance  
                (false, true) => std::cmp::Ordering::Less,  // < K accesses comes first
                (true, false) => std::cmp::Ordering::Greater, // >= K accesses comes second
            }
        });

        // Find the rank of the target key
        let target_history = &self.cache[key].1;
        let target_num_accesses = target_history.len();
        let target_value = if target_num_accesses < self.k {
            (false, target_history.front().copied().unwrap_or(u64::MAX))
        } else {
            (true, target_history.get(target_num_accesses - self.k).copied().unwrap_or(u64::MAX))
        };

        items_with_distances.iter().position(|item| item == &target_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait};
    use std::collections::HashSet;

    // ==============================================
    // CORRECTNESS TESTS MODULE
    // ==============================================
    mod correctness {
        use super::*;

        // Basic LRU-K Behavior Tests
        mod basic_behavior {
            use super::*;

            #[test]
            fn test_basic_lru_k_insertion_and_retrieval() {
                // TODO: Test basic insertion and retrieval
            }

            #[test]
            fn test_lru_k_eviction_order() {
                // TODO: Test that LRU-K eviction prioritizes items with fewer than K accesses
            }

            #[test]
            fn test_capacity_enforcement() {
                // TODO: Test that cache never exceeds capacity
            }

            #[test]
            fn test_update_existing_key() {
                // TODO: Test updating existing key updates access history
            }

            #[test]
            fn test_access_history_tracking() {
                // TODO: Test that access history is correctly tracked and updated
            }

            #[test]
            fn test_k_value_behavior() {
                // TODO: Test behavior with different K values
            }

            #[test]
            fn test_key_operations_consistency() {
                // TODO: Test consistency between contains, get, and len operations
            }

            #[test]
            fn test_timestamp_ordering() {
                // TODO: Test that timestamps are correctly ordered for eviction decisions
            }
        }

        // Edge Cases Tests
        mod edge_cases {
            use super::*;

            #[test]
            fn test_empty_cache_operations() {
                // TODO: Test operations on empty cache
            }

            #[test]
            fn test_single_item_cache() {
                // TODO: Test cache with capacity of 1
            }

            #[test]
            fn test_zero_capacity_cache() {
                // TODO: Test cache with capacity of 0
            }

            #[test]
            fn test_k_equals_one() {
                // TODO: Test LRU-K with K=1 (should behave like LRU)
            }

            #[test]
            fn test_k_larger_than_capacity() {
                // TODO: Test LRU-K when K > capacity
            }

            #[test]
            fn test_same_key_rapid_accesses() {
                // TODO: Test rapid repeated accesses to the same key
            }

            #[test]
            fn test_duplicate_key_insertion() {
                // TODO: Test inserting the same key multiple times
            }

            #[test]
            fn test_large_cache_operations() {
                // TODO: Test operations on large capacity cache
            }

            #[test]
            fn test_access_history_overflow() {
                // TODO: Test that access history doesn't grow beyond K
            }
        }

        // LRU-K-Specific Operations Tests
        mod lru_k_operations {
            use super::*;

            #[test]
            fn test_pop_lru_k_basic() {
                // TODO: Test basic pop_lru_k functionality
            }

            #[test]
            fn test_peek_lru_k_basic() {
                // TODO: Test basic peek_lru_k functionality
            }

            #[test]
            fn test_k_value_retrieval() {
                // TODO: Test k_value() method accuracy
            }

            #[test]
            fn test_access_history_retrieval() {
                // TODO: Test access_history() method functionality
            }

            #[test]
            fn test_access_count() {
                // TODO: Test access_count() method accuracy
            }

            #[test]
            fn test_k_distance() {
                // TODO: Test k_distance() calculation
            }

            #[test]
            fn test_touch_functionality() {
                // TODO: Test touch() method for updating access history
            }

            #[test]
            fn test_k_distance_rank() {
                // TODO: Test k_distance_rank() calculation
            }

            #[test]
            fn test_pop_lru_k_empty_cache() {
                // TODO: Test pop_lru_k on empty cache
            }

            #[test]
            fn test_peek_lru_k_empty_cache() {
                // TODO: Test peek_lru_k on empty cache
            }

            #[test]
            fn test_lru_k_tie_breaking() {
                // TODO: Test behavior when multiple items have same K-distance
            }

            #[test]
            fn test_access_history_after_removal() {
                // TODO: Test that access history is cleaned up after removal
            }

            #[test]
            fn test_access_history_after_clear() {
                // TODO: Test that all access history is reset after clear
            }
        }

        // State Consistency Tests
        mod state_consistency {
            use super::*;

            #[test]
            fn test_cache_access_history_consistency() {
                // TODO: Test that cache and access history stay in sync
            }

            #[test]
            fn test_len_consistency() {
                // TODO: Test that len() always matches actual number of items
            }

            #[test]
            fn test_capacity_consistency() {
                // TODO: Test that capacity never changes and is respected
            }

            #[test]
            fn test_clear_resets_all_state() {
                // TODO: Test that clear() resets all internal state
            }

            #[test]
            fn test_remove_consistency() {
                // TODO: Test that remove operations maintain consistency
            }

            #[test]
            fn test_eviction_consistency() {
                // TODO: Test that evictions maintain consistency
            }

            #[test]
            fn test_access_history_update_on_get() {
                // TODO: Test that get() updates access history correctly
            }

            #[test]
            fn test_invariants_after_operations() {
                // TODO: Test that all invariants hold after various operations
            }

            #[test]
            fn test_k_distance_calculation_consistency() {
                // TODO: Test that K-distance calculations are consistent
            }

            #[test]
            fn test_timestamp_consistency() {
                // TODO: Test that timestamps are always increasing
            }
        }
    }

    // ==============================================
    // PERFORMANCE TESTS MODULE
    // ==============================================
    mod performance {
        use super::*;
        use std::time::{Duration, Instant};

        // Lookup Performance Tests
        mod lookup_performance {
            use super::*;

            #[test]
            fn test_get_performance_with_history_updates() {
                // TODO: Test get() performance with access history tracking overhead
            }

            #[test]
            fn test_contains_performance() {
                // TODO: Test contains() method performance
            }

            #[test]
            fn test_access_history_lookup_performance() {
                // TODO: Test access_history() method performance
            }

            #[test]
            fn test_k_distance_calculation_performance() {
                // TODO: Test k_distance() calculation performance
            }

            #[test]
            fn test_peek_lru_k_performance() {
                // TODO: Test peek_lru_k() performance with large cache
            }

            #[test]
            fn test_cache_hit_vs_miss_performance() {
                // TODO: Compare performance of cache hits vs misses
            }

            #[test]
            fn test_touch_performance() {
                // TODO: Test touch() method performance
            }

            #[test]
            fn test_k_distance_rank_performance() {
                // TODO: Test k_distance_rank() calculation performance
            }
        }

        // Insertion Performance Tests
        mod insertion_performance {
            use super::*;

            #[test]
            fn test_insertion_performance_with_eviction() {
                // TODO: Test insertion performance when eviction is triggered
            }

            #[test]
            fn test_batch_insertion_performance() {
                // TODO: Test performance of multiple sequential insertions
            }

            #[test]
            fn test_update_vs_new_insertion_performance() {
                // TODO: Compare performance of updating vs new insertions
            }

            #[test]
            fn test_insertion_with_history_tracking() {
                // TODO: Test overhead of access history maintenance during insertion
            }

            #[test]
            fn test_history_update_performance() {
                // TODO: Test performance of access history updates
            }

            #[test]
            fn test_timestamp_generation_overhead() {
                // TODO: Test overhead of timestamp generation
            }
        }

        // Eviction Performance Tests
        mod eviction_performance {
            use super::*;

            #[test]
            fn test_lru_k_eviction_performance() {
                // TODO: Test performance of finding and evicting LRU-K item
            }

            #[test]
            fn test_pop_lru_k_performance() {
                // TODO: Test pop_lru_k() method performance
            }

            #[test]
            fn test_eviction_with_varying_k_values() {
                // TODO: Test eviction performance with different K values
            }

            #[test]
            fn test_eviction_with_large_histories() {
                // TODO: Test eviction performance when access histories are large
            }

            #[test]
            fn test_victim_selection_performance() {
                // TODO: Test performance of victim selection algorithm
            }
        }

        // Memory Efficiency Tests
        mod memory_efficiency {
            use super::*;

            #[test]
            fn test_memory_overhead_of_history_tracking() {
                // TODO: Test memory overhead of maintaining access history
            }

            #[test]
            fn test_memory_usage_growth() {
                // TODO: Test memory usage as cache fills up
            }

            #[test]
            fn test_memory_cleanup_after_eviction() {
                // TODO: Test that memory is properly cleaned up after evictions
            }

            #[test]
            fn test_large_value_memory_handling() {
                // TODO: Test memory efficiency with large values
            }

            #[test]
            fn test_access_history_memory_efficiency() {
                // TODO: Test memory efficiency of access history storage
            }

            #[test]
            fn test_memory_scaling_with_k() {
                // TODO: Test how memory usage scales with different K values
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
        use std::time::Duration;

        // Thread Safety Tests
        mod thread_safety {
            use super::*;

            #[test]
            fn test_concurrent_insertions() {
                // TODO: Test multiple threads inserting concurrently
            }

            #[test]
            fn test_concurrent_gets() {
                // TODO: Test multiple threads getting values concurrently
            }

            #[test]
            fn test_concurrent_history_operations() {
                // TODO: Test concurrent access history and touch operations
            }

            #[test]
            fn test_concurrent_lru_k_operations() {
                // TODO: Test concurrent pop_lru_k and peek_lru_k operations
            }

            #[test]
            fn test_mixed_concurrent_operations() {
                // TODO: Test mixed read/write operations across threads
            }

            #[test]
            fn test_concurrent_eviction_scenarios() {
                // TODO: Test eviction behavior with concurrent access
            }

            #[test]
            fn test_thread_fairness() {
                // TODO: Test that no thread is starved under high contention
            }

            #[test]
            fn test_concurrent_timestamp_generation() {
                // TODO: Test concurrent timestamp generation and ordering
            }

            #[test]
            fn test_concurrent_access_history_updates() {
                // TODO: Test concurrent updates to access histories
            }
        }

        // Stress Testing
        mod stress_testing {
            use super::*;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            use std::time::{Duration, Instant};

            // Helper type for thread-safe testing
            type ThreadSafeLRUKCache<K, V> = Arc<Mutex<LRUKCache<K, V>>>;

            #[test]
            fn test_high_contention_scenario() {
                // TODO: Test many threads accessing same small set of keys
            }

            #[test]
            fn test_cache_thrashing_scenario() {
                // TODO: Test rapid insertions causing constant evictions
            }

            #[test]
            fn test_long_running_stability() {
                // TODO: Test stability over extended periods with continuous load
            }

            #[test]
            fn test_memory_pressure_scenario() {
                // TODO: Test behavior with large cache and memory-intensive operations
            }

            #[test]
            fn test_rapid_thread_creation_destruction() {
                // TODO: Test with threads being created and destroyed rapidly
            }

            #[test]
            fn test_burst_load_handling() {
                // TODO: Test handling of sudden burst loads
            }

            #[test]
            fn test_gradual_load_increase() {
                // TODO: Test behavior as load gradually increases
            }

            #[test]
            fn test_access_pattern_stress() {
                // TODO: Test stress scenarios with various access patterns
            }

            #[test]
            fn test_lru_k_eviction_under_stress() {
                // TODO: Test LRU-K eviction correctness under high stress
            }

            #[test]
            fn test_timestamp_stress() {
                // TODO: Test timestamp generation under stress conditions
            }

            #[test]
            fn test_varying_k_values_stress() {
                // TODO: Test stress scenarios with different K values
            }

            #[test]
            fn test_access_history_stress() {
                // TODO: Test access history management under stress
            }
        }
    }
}
