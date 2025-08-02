use std::collections::HashMap;
use std::hash::Hash;
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LFUCacheTrait, MutableCache};

/// LFU (Least Frequently Used) Cache implementation.
#[derive(Debug)]
pub struct LFUCache<K, V>
where 
    K: Eq + Hash,
{
    capacity: usize,
    cache: HashMap<K, (V, usize)>, // (value, frequency)
}

impl<K, V> LFUCache<K, V>
where
    K: Eq + Hash,
{
    pub fn new(capacity: usize) -> Self {
        LFUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
        }
    }
}

// Implementation of specialized traits
impl<K, V> CoreCache<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        // If key already exists, just update the value
        if let Some((old_value, _freq)) = self.cache.get_mut(&key) {
            return Some(std::mem::replace(old_value, value));
        }

        // Handle zero capacity case - reject all new insertions
        if self.capacity == 0 {
            return None;
        }

        // If cache is at capacity, remove the least frequently used item
        if self.cache.len() >= self.capacity {
            // Find the minimum frequency
            let min_freq = self.cache.values().map(|(_, freq)| *freq).min().unwrap_or(0);
            
            // Find the first key with minimum frequency and remove it
            let mut removed = false;
            self.cache.retain(|_, (_, freq)| {
                if !removed && *freq == min_freq {
                    removed = true;
                    false // Remove this entry
                } else {
                    true // Keep this entry
                }
            });
        }

        // Insert new entry with frequency 1
        self.cache.insert(key, (value, 1))
            .map(|(old_value, _)| old_value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some((value, freq)) = self.cache.get_mut(key) {
            // Increment frequency
            *freq += 1;
            Some(value)
        } else {
            None
        }
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
    }
}

impl<K, V> MutableCache<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash,
{
    fn remove(&mut self, key: &K) -> Option<V> {
        self.cache.remove(key).map(|(value, _)| value)
    }
}

impl<K, V> LFUCacheTrait<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash,
{
    fn pop_lfu(&mut self) -> Option<(K, V)> {
        // If cache is empty, return None
        if self.cache.is_empty() {
            return None;
        }
        
        // Find the minimum frequency
        let min_freq = self.cache.values().map(|(_, freq)| *freq).min()?;
        
        // We need to work around borrowing issues by using drain to temporarily 
        // take ownership of all entries, find the LFU one, and put back the rest
        // We'll extract all entries, find the LFU one, and put back the rest
        let mut entries: Vec<_> = self.cache.drain().collect();
        
        // Find the entry with minimum frequency
        if let Some(pos) = entries.iter().position(|(_, (_, freq))| *freq == min_freq) {
            let (key, (value, _)) = entries.swap_remove(pos);
            
            // Put back the remaining entries
            self.cache.extend(entries);
            
            return Some((key, value));
        } else {
            // Put everything back if we didn't find anything (shouldn't happen)
            self.cache.extend(entries);
        }
        
        None
    }

    fn peek_lfu(&self) -> Option<(&K, &V)> {
        // Find the minimum frequency
        let min_freq = self.cache.values().map(|(_, freq)| *freq).min()?;
        
        // Find the first key with minimum frequency
        for (k, (v, freq)) in &self.cache {
            if *freq == min_freq {
                return Some((k, v));
            }
        }
        None
    }

    fn frequency(&self, key: &K) -> Option<u64> {
        self.cache.get(key).map(|(_, freq)| *freq as u64)
    }

    fn reset_frequency(&mut self, key: &K) -> Option<u64> {
        if let Some((_, freq)) = self.cache.get_mut(key) {
            let previous_freq = *freq;
            *freq = 1;
            Some(previous_freq as u64)
        } else {
            None
        }
    }

    fn increment_frequency(&mut self, key: &K) -> Option<u64> {
        if let Some((_, freq)) = self.cache.get_mut(key) {
            *freq += 1;
            Some(*freq as u64)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LFUCacheTrait};
    use std::collections::HashSet;

    // ==============================================
    // CORRECTNESS TESTS MODULE
    // ==============================================
    mod correctness {
        use super::*;

        // Basic LFU Behavior Tests
        mod basic_behavior {
            use super::*;

            #[test]
            fn test_basic_lfu_insertion_and_retrieval() {
                let mut cache = LFUCache::new(3);
                
                // Test insertion and basic retrieval
                assert_eq!(cache.insert("key1".to_string(), 100), None);
                assert_eq!(cache.insert("key2".to_string(), 200), None);
                assert_eq!(cache.insert("key3".to_string(), 300), None);
                
                // Test retrieval
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.get(&"key2".to_string()), Some(&200));
                assert_eq!(cache.get(&"key3".to_string()), Some(&300));
                
                // Test non-existent key
                assert_eq!(cache.get(&"nonexistent".to_string()), None);
                
                // Test that initial frequencies are 1, then increment on access
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2)); // 1 + 1 from get
                assert_eq!(cache.frequency(&"key2".to_string()), Some(2)); // 1 + 1 from get
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2)); // 1 + 1 from get
            }

            #[test]
            fn test_lfu_eviction_order() {
                let mut cache = LFUCache::new(3);
                
                // Fill cache to capacity
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Create different access patterns to establish frequency order
                // key1: frequency = 1 (no additional accesses)
                // key2: frequency = 3 (2 additional accesses)
                // key3: frequency = 2 (1 additional access)
                cache.get(&"key2".to_string()); // key2 freq = 2
                cache.get(&"key2".to_string()); // key2 freq = 3
                cache.get(&"key3".to_string()); // key3 freq = 2
                
                // Verify frequencies before eviction
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1)); // LFU
                assert_eq!(cache.frequency(&"key2".to_string()), Some(3)); // MFU
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2)); // Middle
                
                // Insert new item - should evict key1 (LFU)
                cache.insert("key4".to_string(), 400);
                
                // Verify key1 was evicted (LFU)
                assert!(!cache.contains(&"key1".to_string()));
                assert_eq!(cache.get(&"key1".to_string()), None);
                
                // Verify other keys still exist
                assert!(cache.contains(&"key2".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                assert!(cache.contains(&"key4".to_string()));
                
                // Verify cache size
                assert_eq!(cache.len(), 3);
            }

            #[test]
            fn test_capacity_enforcement() {
                let mut cache = LFUCache::new(2);
                
                // Verify initial state
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 2);
                
                // Insert first item
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.len(), 1);
                assert!(cache.len() <= cache.capacity());
                
                // Insert second item (at capacity)
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.len(), 2);
                assert!(cache.len() <= cache.capacity());
                
                // Insert third item (should trigger eviction)
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 2); // Should still be 2
                assert!(cache.len() <= cache.capacity());
                
                // Insert many more items
                for i in 4..=10 {
                    cache.insert(format!("key{}", i), i * 100);
                    assert!(cache.len() <= cache.capacity());
                    assert_eq!(cache.len(), 2);
                }
                
                // Test with zero capacity
                let mut zero_cache = LFUCache::new(0);
                assert_eq!(zero_cache.capacity(), 0);
                assert_eq!(zero_cache.len(), 0);
                
                // Insert into zero capacity cache
                zero_cache.insert("key".to_string(), 100);
                assert_eq!(zero_cache.len(), 0); // Should remain 0
                assert!(zero_cache.len() <= zero_cache.capacity());
            }

            #[test]
            fn test_update_existing_key() {
                let mut cache = LFUCache::new(3);
                
                // Insert initial value
                assert_eq!(cache.insert("key1".to_string(), 100), None);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Access the key to increase frequency
                cache.get(&"key1".to_string());
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                
                // Update the value - should preserve frequency
                let old_value = cache.insert("key1".to_string(), 999);
                assert_eq!(old_value, Some(100));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3)); // Frequency preserved
                
                // Verify updated value
                assert_eq!(cache.get(&"key1".to_string()), Some(&999));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4)); // Incremented by get
                
                // Verify cache size didn't change
                assert_eq!(cache.len(), 1);
                
                // Add more items to test preservation during eviction scenarios
                cache.insert("key2".to_string(), 200); // freq = 1
                cache.insert("key3".to_string(), 300); // freq = 1
                
                // key1 has frequency 4, others have frequency 1
                // Update key1 again
                cache.insert("key1".to_string(), 1999);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4)); // Still preserved
                
                // Insert new item to trigger eviction - key2 or key3 should be evicted (freq 1)
                cache.insert("key4".to_string(), 400);
                
                // key1 should still be there with preserved frequency
                assert!(cache.contains(&"key1".to_string()));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4));
                assert_eq!(cache.get(&"key1".to_string()), Some(&1999));
            }

            #[test]
            fn test_frequency_tracking() {
                let mut cache = LFUCache::new(5);
                
                // Insert items with initial frequency of 1
                cache.insert("a".to_string(), 1);
                cache.insert("b".to_string(), 2);
                cache.insert("c".to_string(), 3);
                
                // Verify initial frequencies
                assert_eq!(cache.frequency(&"a".to_string()), Some(1));
                assert_eq!(cache.frequency(&"b".to_string()), Some(1));
                assert_eq!(cache.frequency(&"c".to_string()), Some(1));
                
                // Access patterns to create different frequencies
                // a: access 3 times -> freq = 4
                cache.get(&"a".to_string());
                cache.get(&"a".to_string());
                cache.get(&"a".to_string());
                assert_eq!(cache.frequency(&"a".to_string()), Some(4));
                
                // b: access 1 time -> freq = 2
                cache.get(&"b".to_string());
                assert_eq!(cache.frequency(&"b".to_string()), Some(2));
                
                // c: no additional access -> freq = 1
                assert_eq!(cache.frequency(&"c".to_string()), Some(1));
                
                // Test manual frequency operations
                // Reset frequency of 'a'
                let old_freq = cache.reset_frequency(&"a".to_string());
                assert_eq!(old_freq, Some(4));
                assert_eq!(cache.frequency(&"a".to_string()), Some(1));
                
                // Increment frequency of 'b'
                let new_freq = cache.increment_frequency(&"b".to_string());
                assert_eq!(new_freq, Some(3));
                assert_eq!(cache.frequency(&"b".to_string()), Some(3));
                
                // Test frequency operations on non-existent key
                assert_eq!(cache.frequency(&"nonexistent".to_string()), None);
                assert_eq!(cache.reset_frequency(&"nonexistent".to_string()), None);
                assert_eq!(cache.increment_frequency(&"nonexistent".to_string()), None);
                
                // Test LFU identification
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                // Both 'a' and 'c' have frequency 1, but 'c' was inserted first
                assert!(lfu_key == &"a".to_string() || lfu_key == &"c".to_string());
                
                // Verify frequency tracking after removal
                cache.remove(&"b".to_string());
                assert_eq!(cache.frequency(&"b".to_string()), None);
                
                // Verify frequency tracking after clear
                cache.clear();
                assert_eq!(cache.frequency(&"a".to_string()), None);
                assert_eq!(cache.frequency(&"c".to_string()), None);
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_key_operations_consistency() {
                let mut cache = LFUCache::new(4);
                
                // Test empty cache consistency
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"any_key".to_string()));
                assert_eq!(cache.get(&"any_key".to_string()), None);
                
                // Insert items and verify consistency
                let keys = vec!["key1", "key2", "key3"];
                let values = vec![100, 200, 300];
                
                for (i, (&key, &value)) in keys.iter().zip(values.iter()).enumerate() {
                    cache.insert(key.to_string(), value);
                    
                    // Verify len is consistent
                    assert_eq!(cache.len(), i + 1);
                    
                    // Verify contains is consistent with successful insertion
                    assert!(cache.contains(&key.to_string()));
                    
                    // Verify get is consistent with contains
                    assert_eq!(cache.get(&key.to_string()), Some(&value));
                }
                
                // Test consistency across all inserted keys
                for (&key, &value) in keys.iter().zip(values.iter()) {
                    // contains should be true
                    assert!(cache.contains(&key.to_string()));
                    
                    // get should return the value
                    assert_eq!(cache.get(&key.to_string()), Some(&value));
                    
                    // frequency should exist
                    assert!(cache.frequency(&key.to_string()).is_some());
                }
                
                // Test after removal
                cache.remove(&"key2".to_string());
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"key2".to_string()));
                assert_eq!(cache.get(&"key2".to_string()), None);
                assert_eq!(cache.frequency(&"key2".to_string()), None);
                
                // Verify other keys are unaffected
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.get(&"key3".to_string()), Some(&300));
                
                // Test eviction consistency
                cache.insert("key4".to_string(), 400);
                cache.insert("key5".to_string(), 500); // Should trigger eviction
                
                assert_eq!(cache.len(), 4); // Should not exceed capacity
                
                // Count how many of original keys are still present
                let mut remaining_count = 0;
                for &key in &keys {
                    if cache.contains(&key.to_string()) {
                        remaining_count += 1;
                        // If contains is true, get should work
                        assert!(cache.get(&key.to_string()).is_some());
                    } else {
                        // If contains is false, get should return None
                        assert_eq!(cache.get(&key.to_string()), None);
                    }
                }
                
                // At least some original keys should be evicted
                assert!(remaining_count < keys.len());
                
                // New keys should be present
                assert!(cache.contains(&"key4".to_string()));
                assert!(cache.contains(&"key5".to_string()));
                
                // Test clear consistency
                cache.clear();
                assert_eq!(cache.len(), 0);
                
                for &key in &["key1", "key3", "key4", "key5"] {
                    assert!(!cache.contains(&key.to_string()));
                    assert_eq!(cache.get(&key.to_string()), None);
                    assert_eq!(cache.frequency(&key.to_string()), None);
                }
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
            fn test_same_frequency_items() {
                // TODO: Test behavior when multiple items have same frequency
            }

            #[test]
            fn test_frequency_overflow_protection() {
                // TODO: Test handling of very high frequency counts
            }

            #[test]
            fn test_duplicate_key_insertion() {
                // TODO: Test inserting the same key multiple times
            }

            #[test]
            fn test_large_cache_operations() {
                // TODO: Test operations on large capacity cache
            }
        }

        // LFU-Specific Operations Tests
        mod lfu_operations {
            use super::*;

            #[test]
            fn test_pop_lfu_basic() {
                // TODO: Test basic pop_lfu functionality
            }

            #[test]
            fn test_peek_lfu_basic() {
                // TODO: Test basic peek_lfu functionality
            }

            #[test]
            fn test_frequency_retrieval() {
                // TODO: Test frequency() method accuracy
            }

            #[test]
            fn test_reset_frequency() {
                // TODO: Test reset_frequency() method
            }

            #[test]
            fn test_increment_frequency() {
                // TODO: Test increment_frequency() method
            }

            #[test]
            fn test_pop_lfu_empty_cache() {
                // TODO: Test pop_lfu on empty cache
            }

            #[test]
            fn test_peek_lfu_empty_cache() {
                // TODO: Test peek_lfu on empty cache
            }

            #[test]
            fn test_lfu_tie_breaking() {
                // TODO: Test behavior when multiple items have same frequency
            }

            #[test]
            fn test_frequency_after_removal() {
                // TODO: Test that frequency tracking is cleaned up after removal
            }

            #[test]
            fn test_frequency_after_clear() {
                // TODO: Test that all frequencies are reset after clear
            }
        }

        // State Consistency Tests
        mod state_consistency {
            use super::*;

            #[test]
            fn test_cache_frequency_consistency() {
                // TODO: Test that cache and frequency maps stay in sync
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
            fn test_frequency_increment_on_get() {
                // TODO: Test that get() increments frequency correctly
            }

            #[test]
            fn test_invariants_after_operations() {
                // TODO: Test that all invariants hold after various operations
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
            fn test_get_performance_with_varying_frequencies() {
                // TODO: Test get() performance with different frequency distributions
            }

            #[test]
            fn test_contains_performance() {
                // TODO: Test contains() method performance
            }

            #[test]
            fn test_frequency_lookup_performance() {
                // TODO: Test frequency() method performance
            }

            #[test]
            fn test_peek_lfu_performance() {
                // TODO: Test peek_lfu() performance with large cache
            }

            #[test]
            fn test_cache_hit_vs_miss_performance() {
                // TODO: Compare performance of cache hits vs misses
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
            fn test_insertion_with_frequency_tracking() {
                // TODO: Test overhead of frequency tracking during insertion
            }
        }

        // Eviction Performance Tests
        mod eviction_performance {
            use super::*;

            #[test]
            fn test_lfu_eviction_performance() {
                // TODO: Test performance of finding and evicting LFU item
            }

            #[test]
            fn test_pop_lfu_performance() {
                // TODO: Test pop_lfu() method performance
            }

            #[test]
            fn test_eviction_with_many_same_frequency() {
                // TODO: Test eviction performance when many items have same frequency
            }

            #[test]
            fn test_frequency_distribution_impact() {
                // TODO: Test how frequency distribution affects eviction performance
            }
        }

        // Memory Efficiency Tests
        mod memory_efficiency {
            use super::*;

            #[test]
            fn test_memory_overhead_of_frequency_tracking() {
                // TODO: Test memory overhead of maintaining frequency information
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
            fn test_concurrent_frequency_operations() {
                // TODO: Test concurrent frequency increment/reset operations
            }

            #[test]
            fn test_concurrent_lfu_operations() {
                // TODO: Test concurrent pop_lfu and peek_lfu operations
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
        }

        // Stress Testing
        mod stress_testing {
            use super::*;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            use std::time::{Duration, Instant};

            // Helper type for thread-safe testing
            type ThreadSafeLFUCache<K, V> = Arc<Mutex<LFUCache<K, V>>>;

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
            fn test_frequency_distribution_stress() {
                // TODO: Test stress scenarios with various frequency distributions
            }

            #[test]
            fn test_lfu_eviction_under_stress() {
                // TODO: Test LFU eviction correctness under high stress
            }
        }
    }
}
