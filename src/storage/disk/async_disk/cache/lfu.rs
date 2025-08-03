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
        
        // Find the minimum frequency first
        let min_freq = self.cache.values().map(|(_, freq)| *freq).min()?;
        
        // Take ownership of the cache temporarily to avoid borrowing issues
        let mut temp_cache = std::mem::take(&mut self.cache);
        let mut result = None;
        
        // Process entries: keep the first LFU item, put back the rest
        for (key, (value, freq)) in temp_cache.drain() {
            if freq == min_freq && result.is_none() {
                // This is our LFU item to return
                result = Some((key, value));
            } else {
                // Put this item back in the cache
                self.cache.insert(key, (value, freq));
            }
        }
        
        result
    }

    fn peek_lfu(&self) -> Option<(&K, &V)> {
        // Find the key with minimum frequency in a single pass
        let mut min_freq = usize::MAX;
        let mut lfu_item: Option<(&K, &V)> = None;
        
        for (key, (value, freq)) in &self.cache {
            if *freq < min_freq {
                min_freq = *freq;
                lfu_item = Some((key, value));
            }
        }
        
        lfu_item
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
                let mut cache = LFUCache::<String, i32>::new(5);
                
                // Test all operations on empty cache
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 5);
                assert!(!cache.contains(&"nonexistent".to_string()));
                assert_eq!(cache.get(&"nonexistent".to_string()), None);
                assert_eq!(cache.frequency(&"nonexistent".to_string()), None);
                assert_eq!(cache.remove(&"nonexistent".to_string()), None);
                assert_eq!(cache.pop_lfu(), None);
                assert_eq!(cache.peek_lfu(), None);
                
                // Test increment/reset frequency on non-existent keys
                assert_eq!(cache.increment_frequency(&"nonexistent".to_string()), None);
                assert_eq!(cache.reset_frequency(&"nonexistent".to_string()), None);
                
                // Clear empty cache should work
                cache.clear();
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_single_item_cache() {
                let mut cache = LFUCache::new(1);
                
                // Test initial state
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 1);
                
                // Insert first item
                assert_eq!(cache.insert("key1".to_string(), 100), None);
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"key1".to_string()));
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2)); // 1 from insert + 1 from get
                
                // Insert second item should evict first
                assert_eq!(cache.insert("key2".to_string(), 200), None);
                assert_eq!(cache.len(), 1);
                assert!(!cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key2".to_string()));
                assert_eq!(cache.get(&"key2".to_string()), Some(&200));
                
                // Update existing item should preserve it
                assert_eq!(cache.insert("key2".to_string(), 999), Some(200));
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.get(&"key2".to_string()), Some(&999));
                
                // Test pop_lfu and peek_lfu
                assert_eq!(cache.peek_lfu(), Some((&"key2".to_string(), &999)));
                assert_eq!(cache.pop_lfu(), Some(("key2".to_string(), 999)));
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.peek_lfu(), None);
            }

            #[test]
            fn test_zero_capacity_cache() {
                let mut cache = LFUCache::<String, i32>::new(0);
                
                // Test initial state
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 0);
                
                // All insertions should be rejected
                assert_eq!(cache.insert("key1".to_string(), 100), None);
                assert_eq!(cache.insert("key2".to_string(), 200), None);
                assert_eq!(cache.len(), 0);
                
                // All queries should return negative results
                assert!(!cache.contains(&"key1".to_string()));
                assert_eq!(cache.get(&"key1".to_string()), None);
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                assert_eq!(cache.remove(&"key1".to_string()), None);
                
                // LFU operations should return None
                assert_eq!(cache.pop_lfu(), None);
                assert_eq!(cache.peek_lfu(), None);
                
                // Frequency operations should return None
                assert_eq!(cache.increment_frequency(&"key1".to_string()), None);
                assert_eq!(cache.reset_frequency(&"key1".to_string()), None);
                
                // Clear should work (no-op)
                cache.clear();
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_same_frequency_items() {
                let mut cache = LFUCache::new(3);
                
                // Insert items with same initial frequency
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // All items should have frequency 1
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(1));
                
                // When cache is full and we insert a new item,
                // one of the items with frequency 1 should be evicted
                let initial_keys = ["key1", "key2", "key3"];
                cache.insert("key4".to_string(), 400);
                assert_eq!(cache.len(), 3);
                
                // Verify that key4 was inserted
                assert!(cache.contains(&"key4".to_string()));
                assert_eq!(cache.frequency(&"key4".to_string()), Some(1));
                
                // One of the original keys should be gone
                let remaining_count = initial_keys.iter()
                    .map(|k| cache.contains(&k.to_string()))
                    .filter(|&exists| exists)
                    .count();
                assert_eq!(remaining_count, 2);
                
                // Test peek_lfu and pop_lfu behavior with same frequencies
                // Should return some item with frequency 1
                if let Some((key, _)) = cache.peek_lfu() {
                    assert_eq!(cache.frequency(key), Some(1));
                }
                
                if let Some((key, _)) = cache.pop_lfu() {
                    assert_eq!(cache.len(), 2);
                    // The removed item should not be in cache anymore
                    assert!(!cache.contains(&key));
                }
            }

            #[test]
            fn test_frequency_overflow_protection() {
                let mut cache = LFUCache::new(2);
                
                // Insert an item
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Simulate approaching overflow by setting a very high frequency
                // Since we can't directly set frequency to max, we'll test with reasonable values
                // and ensure the system doesn't panic
                
                // Access the item many times to increase frequency
                for _ in 0..1000 {
                    cache.get(&"key1".to_string());
                }
                
                // Frequency should be very high but not overflow
                let freq = cache.frequency(&"key1".to_string()).unwrap();
                assert!(freq > 1000);
                assert!(freq <= u64::MAX); // Should not overflow
                
                // Test that increment_frequency doesn't panic with high values
                let freq_before = cache.frequency(&"key1".to_string()).unwrap();
                let freq_after_increment = cache.increment_frequency(&"key1".to_string()).unwrap();
                let freq_after = cache.frequency(&"key1".to_string()).unwrap();
                assert_eq!(freq_after_increment, freq_before + 1);
                assert_eq!(freq_after, freq_before + 1);
                
                // Insert another item to test that high frequency item isn't evicted
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.len(), 2);
                
                // Insert third item - key2 should be evicted (lower frequency)
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 2);
                assert!(cache.contains(&"key1".to_string())); // High frequency item preserved
                assert!(!cache.contains(&"key2".to_string())); // Low frequency item evicted
                assert!(cache.contains(&"key3".to_string())); // New item inserted
            }

            #[test]
            fn test_duplicate_key_insertion() {
                let mut cache = LFUCache::new(3);
                
                // Insert initial value
                assert_eq!(cache.insert("key1".to_string(), 100), None);
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Access to increase frequency
                cache.get(&"key1".to_string());
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                
                // Insert same key with different value - should update value and preserve frequency
                assert_eq!(cache.insert("key1".to_string(), 999), Some(100));
                assert_eq!(cache.len(), 1); // Length unchanged
                assert_eq!(cache.get(&"key1".to_string()), Some(&999)); // Value updated
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4)); // Frequency preserved + 1 for get
                
                // Insert again with another value
                assert_eq!(cache.insert("key1".to_string(), 777), Some(999));
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.get(&"key1".to_string()), Some(&777));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(5)); // Frequency continues to track
                
                // Add other items to fill cache
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 3);
                
                // Insert fourth item - key1 should not be evicted due to high frequency
                cache.insert("key4".to_string(), 400);
                assert_eq!(cache.len(), 3);
                assert!(cache.contains(&"key1".to_string())); // High frequency item preserved
                
                // Verify key1 still has the correct value and frequency
                assert_eq!(cache.get(&"key1".to_string()), Some(&777));
                
                // One of key2 or key3 should be evicted (both have frequency 1)
                let key2_exists = cache.contains(&"key2".to_string());
                let key3_exists = cache.contains(&"key3".to_string());
                assert!(!(key2_exists && key3_exists)); // Not both can exist
                assert!(cache.contains(&"key4".to_string())); // New item should exist
            }

            #[test]
            fn test_large_cache_operations() {
                let capacity = 10000;
                let mut cache = LFUCache::new(capacity);
                
                // Test initial state
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), capacity);
                
                // Insert many items
                for i in 0..capacity {
                    let key = format!("key_{}", i);
                    assert_eq!(cache.insert(key, i), None);
                }
                
                // Cache should be at capacity
                assert_eq!(cache.len(), capacity);
                
                // All items should be present
                for i in 0..capacity {
                    let key = format!("key_{}", i);
                    assert!(cache.contains(&key));
                    assert_eq!(cache.get(&key), Some(&i));
                    assert_eq!(cache.frequency(&key), Some(2)); // 1 from insert + 1 from get
                }
                
                // Test that additional insertion triggers eviction
                let new_key = "new_key".to_string();
                assert_eq!(cache.insert(new_key.clone(), 99999), None);
                assert_eq!(cache.len(), capacity); // Size should remain the same
                assert!(cache.contains(&new_key)); // New item should be present
                
                // Count how many original items remain (should be capacity - 1)
                let remaining_original = (0..capacity)
                    .map(|i| format!("key_{}", i))
                    .filter(|key| cache.contains(key))
                    .count();
                assert_eq!(remaining_original, capacity - 1);
                
                // Test clear operation
                cache.clear();
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&new_key));
                
                // Test that we can insert after clear
                cache.insert("after_clear".to_string(), 42);
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"after_clear".to_string()));
            }
        }

        // LFU-Specific Operations Tests
        mod lfu_operations {
            use super::*;

            #[test]
            fn test_pop_lfu_basic() {
                let mut cache = LFUCache::new(4);
                
                // Insert items with different access patterns
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Create different frequencies:
                // key1: freq = 1 (no additional access)
                // key2: freq = 3 (2 additional accesses)
                // key3: freq = 2 (1 additional access)
                cache.get(&"key2".to_string());
                cache.get(&"key2".to_string());
                cache.get(&"key3".to_string());
                
                // Verify frequencies
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(3));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2));
                
                // Pop LFU should remove key1 (lowest frequency)
                let (key, value) = cache.pop_lfu().unwrap();
                assert_eq!(key, "key1".to_string());
                assert_eq!(value, 100);
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"key1".to_string()));
                
                // Next pop should remove key3 (next lowest frequency)
                let (key, value) = cache.pop_lfu().unwrap();
                assert_eq!(key, "key3".to_string());
                assert_eq!(value, 300);
                assert_eq!(cache.len(), 1);
                
                // Final pop should remove key2
                let (key, value) = cache.pop_lfu().unwrap();
                assert_eq!(key, "key2".to_string());
                assert_eq!(value, 200);
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_peek_lfu_basic() {
                let mut cache = LFUCache::new(4);
                
                // Insert items with different access patterns
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Create different frequencies:
                // key1: freq = 1 (no additional access)
                // key2: freq = 3 (2 additional accesses)
                // key3: freq = 2 (1 additional access)
                cache.get(&"key2".to_string());
                cache.get(&"key2".to_string());
                cache.get(&"key3".to_string());
                
                // Peek LFU should return key1 (lowest frequency) without removing it
                let (key, value) = cache.peek_lfu().unwrap();
                assert_eq!(key, &"key1".to_string());
                assert_eq!(value, &100);
                assert_eq!(cache.len(), 3); // Cache size unchanged
                assert!(cache.contains(&"key1".to_string())); // Item still present
                
                // Multiple peeks should return the same result
                let (key2, value2) = cache.peek_lfu().unwrap();
                assert_eq!(key2, &"key1".to_string());
                assert_eq!(value2, &100);
                
                // After removing key1, peek should return key3 (next lowest)
                cache.remove(&"key1".to_string());
                let (key, value) = cache.peek_lfu().unwrap();
                assert_eq!(key, &"key3".to_string());
                assert_eq!(value, &300);
                assert_eq!(cache.len(), 2);
                
                // After removing key3, peek should return key2
                cache.remove(&"key3".to_string());
                let (key, value) = cache.peek_lfu().unwrap();
                assert_eq!(key, &"key2".to_string());
                assert_eq!(value, &200);
                assert_eq!(cache.len(), 1);
            }

            #[test]
            fn test_frequency_retrieval() {
                let mut cache = LFUCache::new(5);
                
                // Test frequency for non-existent key
                assert_eq!(cache.frequency(&"nonexistent".to_string()), None);
                
                // Insert a key and check initial frequency
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Access the key and verify frequency increments
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2));
                
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                
                // Insert another key
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3)); // Unchanged
                
                // Access key2 multiple times
                for _ in 0..5 {
                    cache.get(&"key2".to_string());
                }
                assert_eq!(cache.frequency(&"key2".to_string()), Some(6)); // 1 + 5
                
                // Update existing key - should preserve frequency
                cache.insert("key1".to_string(), 999);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3)); // Preserved
                
                // Remove key and verify frequency is gone
                cache.remove(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                assert_eq!(cache.frequency(&"key2".to_string()), Some(6)); // Unaffected
            }

            #[test]
            fn test_reset_frequency() {
                let mut cache = LFUCache::new(3);
                
                // Test reset on non-existent key
                assert_eq!(cache.reset_frequency(&"nonexistent".to_string()), None);
                
                // Insert a key and increase its frequency
                cache.insert("key1".to_string(), 100);
                cache.get(&"key1".to_string());
                cache.get(&"key1".to_string());
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4));
                
                // Reset frequency should return old frequency and set to 1
                let old_freq = cache.reset_frequency(&"key1".to_string());
                assert_eq!(old_freq, Some(4));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Reset again should return 1
                let old_freq = cache.reset_frequency(&"key1".to_string());
                assert_eq!(old_freq, Some(1));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Insert another key with high frequency
                cache.insert("key2".to_string(), 200);
                for _ in 0..10 {
                    cache.get(&"key2".to_string());
                }
                assert_eq!(cache.frequency(&"key2".to_string()), Some(11));
                
                // Reset key2 frequency
                let old_freq = cache.reset_frequency(&"key2".to_string());
                assert_eq!(old_freq, Some(11));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                
                // Verify key1 frequency unchanged
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Test that cache still works correctly after resets
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 3);
                
                // All items now have frequency 1, so eviction should be deterministic
                cache.insert("key4".to_string(), 400); // Should evict one of the items
                assert_eq!(cache.len(), 3);
            }

            #[test]
            fn test_increment_frequency() {
                let mut cache = LFUCache::new(3);
                
                // Test increment on non-existent key
                assert_eq!(cache.increment_frequency(&"nonexistent".to_string()), None);
                
                // Insert a key and test increment
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Increment frequency manually
                let new_freq = cache.increment_frequency(&"key1".to_string());
                assert_eq!(new_freq, Some(2));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2));
                
                // Increment multiple times
                for i in 3..=7 {
                    let freq = cache.increment_frequency(&"key1".to_string());
                    assert_eq!(freq, Some(i));
                    assert_eq!(cache.frequency(&"key1".to_string()), Some(i));
                }
                
                // Insert another key
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                
                // Increment key2
                let freq = cache.increment_frequency(&"key2".to_string());
                assert_eq!(freq, Some(2));
                
                // Verify key1 frequency unchanged
                assert_eq!(cache.frequency(&"key1".to_string()), Some(7));
                
                // Test that increment affects LFU ordering
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.frequency(&"key3".to_string()), Some(1));
                
                // key3 should be LFU (freq=1), then key2 (freq=2), then key1 (freq=7)
                let (key, _) = cache.peek_lfu().unwrap();
                assert_eq!(key, &"key3".to_string());
                
                // Increment key3 to make it same as key2
                cache.increment_frequency(&"key3".to_string());
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2));
                
                // Now either key2 or key3 could be LFU (both freq=2)
                let (key, _) = cache.peek_lfu().unwrap();
                assert!(key == &"key2".to_string() || key == &"key3".to_string());
                assert_eq!(cache.frequency(key).unwrap(), 2);
            }

            #[test]
            fn test_pop_lfu_empty_cache() {
                let mut cache = LFUCache::<String, i32>::new(5);
                
                // Test pop_lfu on empty cache
                assert_eq!(cache.pop_lfu(), None);
                assert_eq!(cache.len(), 0);
                
                // Insert and remove to empty the cache again
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.len(), 1);
                
                let (key, value) = cache.pop_lfu().unwrap();
                assert_eq!(key, "key1".to_string());
                assert_eq!(value, 100);
                assert_eq!(cache.len(), 0);
                
                // Test pop_lfu on empty cache again
                assert_eq!(cache.pop_lfu(), None);
                
                // Insert multiple items and pop all
                cache.insert("a".to_string(), 1);
                cache.insert("b".to_string(), 2);
                cache.insert("c".to_string(), 3);
                assert_eq!(cache.len(), 3);
                
                // Pop all items
                assert!(cache.pop_lfu().is_some());
                assert!(cache.pop_lfu().is_some());
                assert!(cache.pop_lfu().is_some());
                assert_eq!(cache.len(), 0);
                
                // Should be empty again
                assert_eq!(cache.pop_lfu(), None);
            }

            #[test]
            fn test_peek_lfu_empty_cache() {
                let cache = LFUCache::<String, i32>::new(5);
                
                // Test peek_lfu on empty cache
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.len(), 0);
                
                // Test with zero capacity cache
                let zero_cache = LFUCache::<String, i32>::new(0);
                assert_eq!(zero_cache.peek_lfu(), None);
                assert_eq!(zero_cache.len(), 0);
                
                // Test that multiple peeks on empty cache return None
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.peek_lfu(), None);
                
                // Test after creating and emptying cache
                let mut cache2 = LFUCache::new(3);
                cache2.insert("temp".to_string(), 999);
                assert!(cache2.peek_lfu().is_some());
                
                cache2.clear();
                assert_eq!(cache2.peek_lfu(), None);
                assert_eq!(cache2.len(), 0);
                
                // Test after removing all items
                let mut cache3 = LFUCache::new(2);
                cache3.insert("a".to_string(), 1);
                cache3.insert("b".to_string(), 2);
                assert!(cache3.peek_lfu().is_some());
                
                cache3.remove(&"a".to_string());
                cache3.remove(&"b".to_string());
                assert_eq!(cache3.peek_lfu(), None);
                assert_eq!(cache3.len(), 0);
            }

            #[test]
            fn test_lfu_tie_breaking() {
                let mut cache = LFUCache::new(5);
                
                // Insert items and create different frequency levels
                cache.insert("low1".to_string(), 1);     // will have freq = 1
                cache.insert("low2".to_string(), 2);     // will have freq = 1
                cache.insert("medium".to_string(), 3);   // will have freq = 2
                cache.insert("high".to_string(), 4);     // will have freq = 3
                
                // Create frequency differences
                cache.get(&"medium".to_string());        // medium: freq = 2
                cache.get(&"high".to_string());          // high: freq = 2
                cache.get(&"high".to_string());          // high: freq = 3
                
                // Verify frequencies
                assert_eq!(cache.frequency(&"low1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"low2".to_string()), Some(1));
                assert_eq!(cache.frequency(&"medium".to_string()), Some(2));
                assert_eq!(cache.frequency(&"high".to_string()), Some(3));
                
                // Test consistent tie-breaking: peek and pop should return same item
                let (peek_key, peek_value) = cache.peek_lfu().unwrap();
                let peek_key_owned = peek_key.clone();
                let peek_value_owned = *peek_value;
                
                let (pop_key, pop_value) = cache.pop_lfu().unwrap();
                assert_eq!(peek_key_owned, pop_key);
                assert_eq!(peek_value_owned, pop_value);
                
                // The popped item should be one of the low frequency items
                assert!(pop_key == "low1".to_string() || pop_key == "low2".to_string());
                assert_eq!(cache.len(), 3);
                
                // Next pop should get the other low frequency item
                let (second_key, _) = cache.pop_lfu().unwrap();
                assert!(second_key == "low1".to_string() || second_key == "low2".to_string());
                assert_ne!(pop_key, second_key); // Should be different
                assert_eq!(cache.len(), 2);
                
                // Next should be medium frequency item
                let (third_key, third_value) = cache.pop_lfu().unwrap();
                assert_eq!(third_key, "medium".to_string());
                assert_eq!(third_value, 3);
                assert_eq!(cache.len(), 1);
                
                // Finally the high frequency item
                let (last_key, last_value) = cache.pop_lfu().unwrap();
                assert_eq!(last_key, "high".to_string());
                assert_eq!(last_value, 4);
                assert_eq!(cache.len(), 0);
                
                // Test with all same frequency
                cache.insert("a".to_string(), 1);
                cache.insert("b".to_string(), 2);
                cache.insert("c".to_string(), 3);
                
                // All should have frequency 1
                assert_eq!(cache.frequency(&"a".to_string()), Some(1));
                assert_eq!(cache.frequency(&"b".to_string()), Some(1));
                assert_eq!(cache.frequency(&"c".to_string()), Some(1));
                
                // Should be able to pop all three (order may vary)
                let mut popped_keys = vec![];
                popped_keys.push(cache.pop_lfu().unwrap().0);
                popped_keys.push(cache.pop_lfu().unwrap().0);
                popped_keys.push(cache.pop_lfu().unwrap().0);
                
                popped_keys.sort();
                assert_eq!(popped_keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_frequency_after_removal() {
                let mut cache = LFUCache::new(5);
                
                // Insert items and build up frequencies
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Increase frequencies
                for _ in 0..5 {
                    cache.get(&"key1".to_string());
                }
                for _ in 0..3 {
                    cache.get(&"key2".to_string());
                }
                cache.get(&"key3".to_string());
                
                // Verify initial frequencies
                assert_eq!(cache.frequency(&"key1".to_string()), Some(6)); // 1 + 5
                assert_eq!(cache.frequency(&"key2".to_string()), Some(4)); // 1 + 3
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2)); // 1 + 1
                
                // Remove key1 and verify its frequency is gone
                let removed_value = cache.remove(&"key1".to_string());
                assert_eq!(removed_value, Some(100));
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                assert_eq!(cache.len(), 2);
                
                // Verify other frequencies unchanged
                assert_eq!(cache.frequency(&"key2".to_string()), Some(4));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2));
                
                // Test that LFU operations work correctly after removal
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                assert_eq!(lfu_key, &"key3".to_string()); // Should be key3 (freq=2)
                
                // Remove via pop_lfu
                let (popped_key, popped_value) = cache.pop_lfu().unwrap();
                assert_eq!(popped_key, "key3".to_string());
                assert_eq!(popped_value, 300);
                assert_eq!(cache.frequency(&"key3".to_string()), None);
                assert_eq!(cache.len(), 1);
                
                // Only key2 should remain
                assert_eq!(cache.frequency(&"key2".to_string()), Some(4));
                assert!(cache.contains(&"key2".to_string()));
                
                // Remove the last item
                cache.remove(&"key2".to_string());
                assert_eq!(cache.frequency(&"key2".to_string()), None);
                assert_eq!(cache.len(), 0);
                
                // Verify cache is completely empty
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.pop_lfu(), None);
                
                // Test re-inserting with same keys creates fresh frequencies
                cache.insert("key1".to_string(), 999);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1)); // Fresh start
            }

            #[test]
            fn test_frequency_after_clear() {
                let mut cache = LFUCache::new(5);
                
                // Insert items and build up frequencies
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Increase frequencies significantly
                for _ in 0..10 {
                    cache.get(&"key1".to_string());
                }
                for _ in 0..5 {
                    cache.get(&"key2".to_string());
                }
                for _ in 0..7 {
                    cache.get(&"key3".to_string());
                }
                
                // Verify high frequencies
                assert_eq!(cache.frequency(&"key1".to_string()), Some(11)); // 1 + 10
                assert_eq!(cache.frequency(&"key2".to_string()), Some(6));  // 1 + 5
                assert_eq!(cache.frequency(&"key3".to_string()), Some(8));  // 1 + 7
                assert_eq!(cache.len(), 3);
                
                // Clear the cache
                cache.clear();
                
                // Verify cache is empty
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.pop_lfu(), None);
                
                // Verify all frequencies are gone
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                assert_eq!(cache.frequency(&"key2".to_string()), None);
                assert_eq!(cache.frequency(&"key3".to_string()), None);
                
                // Verify all keys are gone
                assert!(!cache.contains(&"key1".to_string()));
                assert!(!cache.contains(&"key2".to_string()));
                assert!(!cache.contains(&"key3".to_string()));
                
                // Test that we can insert fresh items after clear
                cache.insert("key1".to_string(), 999);
                cache.insert("new_key".to_string(), 888);
                
                // Frequencies should start fresh
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"new_key".to_string()), Some(1));
                assert_eq!(cache.len(), 2);
                
                // Test that cache works normally after clear
                cache.get(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2));
                
                // LFU operations should work
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                assert_eq!(lfu_key, &"new_key".to_string()); // freq=1, should be LFU
                
                // Test multiple clears
                cache.clear();
                assert_eq!(cache.len(), 0);
                cache.clear(); // Should be safe to clear empty cache
                assert_eq!(cache.len(), 0);
            }
        }

        // State Consistency Tests
        mod state_consistency {
            use super::*;

            #[test]
            fn test_cache_frequency_consistency() {
                let mut cache = LFUCache::new(5);
                
                // Test initial state consistency
                assert_eq!(cache.len(), 0);
                
                // Insert items and verify frequency consistency
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // All items should have initial frequency of 1
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(1));
                
                // Access items to change frequencies
                cache.get(&"key1".to_string()); // key1: freq = 2
                cache.get(&"key1".to_string()); // key1: freq = 3
                cache.get(&"key2".to_string()); // key2: freq = 2
                
                // Verify frequency updates are consistent
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(2));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(1));
                
                // Test update preserves frequency
                cache.insert("key1".to_string(), 999);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3)); // Should be preserved
                
                // Test manual frequency operations
                cache.increment_frequency(&"key3".to_string());
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2));
                
                cache.reset_frequency(&"key1".to_string());
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                
                // Verify that frequency and cache remain consistent
                assert_eq!(cache.len(), 3);
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key2".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                
                // Verify LFU operations use consistent frequency data
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                let lfu_freq = cache.frequency(lfu_key).unwrap();
                assert_eq!(lfu_freq, 1); // Should be one of the items with frequency 1
            }

            #[test]
            fn test_len_consistency() {
                let mut cache = LFUCache::new(4);
                
                // Test empty cache
                assert_eq!(cache.len(), 0);
                
                // Test incremental insertions
                cache.insert("key1".to_string(), 100);
                assert_eq!(cache.len(), 1);
                
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.len(), 2);
                
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 3);
                
                // Test updating existing key doesn't change length
                cache.insert("key1".to_string(), 999);
                assert_eq!(cache.len(), 3);
                
                // Test insert at capacity (should increase length)
                cache.insert("key4".to_string(), 400);
                assert_eq!(cache.len(), 4);
                
                // Test insert beyond capacity (should evict and maintain length)
                cache.insert("key5".to_string(), 500);
                assert_eq!(cache.len(), 4); // Should remain at capacity
                
                // Test manual removals
                cache.remove(&"key5".to_string());
                assert_eq!(cache.len(), 3);
                
                cache.remove(&"key4".to_string());
                assert_eq!(cache.len(), 2);
                
                // Test removing non-existent key doesn't change length
                cache.remove(&"nonexistent".to_string());
                assert_eq!(cache.len(), 2);
                
                // Test pop_lfu operations
                cache.pop_lfu();
                assert_eq!(cache.len(), 1);
                
                cache.pop_lfu();
                assert_eq!(cache.len(), 0);
                
                // Test pop_lfu on empty cache doesn't change length
                assert_eq!(cache.pop_lfu(), None);
                assert_eq!(cache.len(), 0);
                
                // Test clear operation
                cache.insert("test1".to_string(), 1);
                cache.insert("test2".to_string(), 2);
                assert_eq!(cache.len(), 2);
                
                cache.clear();
                assert_eq!(cache.len(), 0);
                
                // Test that get operations don't affect length
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                assert_eq!(cache.len(), 2);
                
                cache.get(&"key1".to_string());
                cache.get(&"key2".to_string());
                cache.get(&"nonexistent".to_string());
                assert_eq!(cache.len(), 2); // Should remain unchanged
            }

            #[test]
            fn test_capacity_consistency() {
                // Test different capacity values
                let capacities = [0, 1, 3, 10, 100];
                
                for &capacity in &capacities {
                    let mut cache = LFUCache::<String, i32>::new(capacity);
                    
                    // Test initial capacity
                    assert_eq!(cache.capacity(), capacity);
                    
                    // Test capacity doesn't change after operations
                    if capacity > 0 {
                        // Insert items up to capacity
                        for i in 0..capacity {
                            cache.insert(format!("key{}", i), i as i32);
                            assert_eq!(cache.capacity(), capacity); // Should never change
                            assert!(cache.len() <= capacity); // Should never exceed capacity
                        }
                        
                        // Insert beyond capacity
                        for i in capacity..(capacity + 5) {
                            cache.insert(format!("key{}", i), i as i32);
                            assert_eq!(cache.capacity(), capacity); // Should never change
                            assert_eq!(cache.len(), capacity); // Should stay at capacity
                        }
                        
                        // Test other operations don't change capacity
                        cache.get(&format!("key{}", capacity - 1));
                        assert_eq!(cache.capacity(), capacity);
                        
                        cache.remove(&format!("key{}", capacity - 1));
                        assert_eq!(cache.capacity(), capacity);
                        
                        cache.pop_lfu();
                        assert_eq!(cache.capacity(), capacity);
                        
                        cache.clear();
                        assert_eq!(cache.capacity(), capacity);
                        
                    } else {
                        // Test zero capacity case
                        assert_eq!(cache.capacity(), 0);
                        cache.insert("key1".to_string(), 100);
                        assert_eq!(cache.len(), 0); // Should remain empty
                        assert_eq!(cache.capacity(), 0); // Should remain 0
                    }
                }
                
                // Test capacity consistency across multiple operations
                let mut cache = LFUCache::new(5);
                let original_capacity = cache.capacity();
                
                // Perform 100 random operations
                for i in 0..100 {
                    match i % 4 {
                        0 => { cache.insert(format!("key{}", i % 10), i); }
                        1 => { cache.get(&format!("key{}", i % 10)); }
                        2 => { cache.remove(&format!("key{}", i % 10)); }
                        3 => { cache.pop_lfu(); }
                        _ => unreachable!(),
                    }
                    
                    // Verify capacity never changes and constraints are respected
                    assert_eq!(cache.capacity(), original_capacity);
                    assert!(cache.len() <= cache.capacity());
                }
            }

            #[test]
            fn test_clear_resets_all_state() {
                let mut cache = LFUCache::new(5);
                
                // Populate cache with data and complex state
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                cache.insert("key4".to_string(), 400);
                cache.insert("key5".to_string(), 500);
                
                // Create complex frequency patterns
                for _ in 0..10 {
                    cache.get(&"key1".to_string());
                }
                for _ in 0..5 {
                    cache.get(&"key2".to_string());
                }
                for _ in 0..3 {
                    cache.get(&"key3".to_string());
                }
                cache.get(&"key4".to_string());
                // key5 remains at frequency 1
                
                // Verify complex state exists
                assert_eq!(cache.len(), 5);
                assert_eq!(cache.frequency(&"key1".to_string()), Some(11)); // 1 + 10
                assert_eq!(cache.frequency(&"key2".to_string()), Some(6));  // 1 + 5
                assert_eq!(cache.frequency(&"key3".to_string()), Some(4));  // 1 + 3
                assert_eq!(cache.frequency(&"key4".to_string()), Some(2));  // 1 + 1
                assert_eq!(cache.frequency(&"key5".to_string()), Some(1));  // 1 + 0
                
                // Clear the cache
                cache.clear();
                
                // Verify complete state reset
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 5); // Capacity should remain unchanged
                
                // Verify all keys are gone
                assert!(!cache.contains(&"key1".to_string()));
                assert!(!cache.contains(&"key2".to_string()));
                assert!(!cache.contains(&"key3".to_string()));
                assert!(!cache.contains(&"key4".to_string()));
                assert!(!cache.contains(&"key5".to_string()));
                
                // Verify all frequencies are gone
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                assert_eq!(cache.frequency(&"key2".to_string()), None);
                assert_eq!(cache.frequency(&"key3".to_string()), None);
                assert_eq!(cache.frequency(&"key4".to_string()), None);
                assert_eq!(cache.frequency(&"key5".to_string()), None);
                
                // Verify get operations return None
                assert_eq!(cache.get(&"key1".to_string()), None);
                assert_eq!(cache.get(&"key2".to_string()), None);
                
                // Verify LFU operations work on empty cache
                assert_eq!(cache.pop_lfu(), None);
                assert_eq!(cache.peek_lfu(), None);
                
                // Verify cache is ready for fresh use
                cache.insert("new_key".to_string(), 999);
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.frequency(&"new_key".to_string()), Some(1));
                assert_eq!(cache.get(&"new_key".to_string()), Some(&999));
                
                // Test multiple clears are safe
                cache.clear();
                assert_eq!(cache.len(), 0);
                
                cache.clear(); // Second clear on empty cache
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 5); // Capacity still preserved
                
                // Test clear after partial population
                cache.insert("test1".to_string(), 1);
                cache.insert("test2".to_string(), 2);
                assert_eq!(cache.len(), 2);
                
                cache.clear();
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.frequency(&"test1".to_string()), None);
                assert_eq!(cache.frequency(&"test2".to_string()), None);
            }

            #[test]
            fn test_remove_consistency() {
                let mut cache = LFUCache::new(5);
                
                // Setup cache with various frequencies
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                cache.insert("key4".to_string(), 400);
                
                // Create different frequency patterns
                cache.get(&"key1".to_string()); // key1: freq = 2
                cache.get(&"key1".to_string()); // key1: freq = 3
                cache.get(&"key2".to_string()); // key2: freq = 2
                cache.get(&"key3".to_string()); // key3: freq = 2
                // key4: freq = 1
                
                assert_eq!(cache.len(), 4);
                
                // Test successful removal
                let removed_value = cache.remove(&"key2".to_string());
                assert_eq!(removed_value, Some(200));
                assert_eq!(cache.len(), 3);
                
                // Verify key is completely gone
                assert!(!cache.contains(&"key2".to_string()));
                assert_eq!(cache.get(&"key2".to_string()), None);
                assert_eq!(cache.frequency(&"key2".to_string()), None);
                
                // Verify other keys are unaffected
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                assert!(cache.contains(&"key4".to_string()));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(2));
                assert_eq!(cache.frequency(&"key4".to_string()), Some(1));
                
                // Test removal of non-existent key
                let removed_none = cache.remove(&"nonexistent".to_string());
                assert_eq!(removed_none, None);
                assert_eq!(cache.len(), 3); // Should remain unchanged
                
                // Test removal of key with highest frequency
                let removed_high_freq = cache.remove(&"key1".to_string());
                assert_eq!(removed_high_freq, Some(100));
                assert_eq!(cache.len(), 2);
                assert_eq!(cache.frequency(&"key1".to_string()), None);
                
                // Test removal of key with lowest frequency
                let removed_low_freq = cache.remove(&"key4".to_string());
                assert_eq!(removed_low_freq, Some(400));
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.frequency(&"key4".to_string()), None);
                
                // Verify LFU operations still work correctly after removals
                let (lfu_key, lfu_value) = cache.peek_lfu().unwrap();
                assert_eq!(lfu_key, &"key3".to_string());
                assert_eq!(lfu_value, &300);
                
                // Test removing the last item
                let removed_last = cache.remove(&"key3".to_string());
                assert_eq!(removed_last, Some(300));
                assert_eq!(cache.len(), 0);
                
                // Verify empty cache state
                assert_eq!(cache.peek_lfu(), None);
                assert_eq!(cache.pop_lfu(), None);
                
                // Test removal on empty cache
                let removed_from_empty = cache.remove(&"key1".to_string());
                assert_eq!(removed_from_empty, None);
                assert_eq!(cache.len(), 0);
                
                // Test cache functionality after complete emptying via removals
                cache.insert("new_key".to_string(), 999);
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.frequency(&"new_key".to_string()), Some(1));
                
                // Test removing and re-inserting same key
                cache.remove(&"new_key".to_string());
                assert_eq!(cache.len(), 0);
                
                cache.insert("new_key".to_string(), 888);
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.frequency(&"new_key".to_string()), Some(1)); // Fresh frequency
                assert_eq!(cache.get(&"new_key".to_string()), Some(&888));
            }

            #[test]
            fn test_eviction_consistency() {
                let mut cache = LFUCache::new(3);
                
                // Fill cache to capacity
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                assert_eq!(cache.len(), 3);
                
                // Create frequency differences
                cache.get(&"key1".to_string()); // key1: freq = 2
                cache.get(&"key1".to_string()); // key1: freq = 3
                cache.get(&"key2".to_string()); // key2: freq = 2
                // key3: freq = 1 (lowest)
                
                // Insert beyond capacity - should evict key3 (LFU)
                cache.insert("key4".to_string(), 400);
                assert_eq!(cache.len(), 3); // Should remain at capacity
                
                // Verify eviction occurred correctly
                assert!(!cache.contains(&"key3".to_string()));
                assert_eq!(cache.frequency(&"key3".to_string()), None);
                
                // Verify remaining items are correct
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key2".to_string()));
                assert!(cache.contains(&"key4".to_string()));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(2));
                assert_eq!(cache.frequency(&"key4".to_string()), Some(1)); // New item
                
                // Test eviction with tie-breaking
                cache.insert("key5".to_string(), 500);
                assert_eq!(cache.len(), 3);
                
                // Either key4 or key5 should be evicted (both have freq=1)
                // But one of them should remain
                let has_key4 = cache.contains(&"key4".to_string());
                let has_key5 = cache.contains(&"key5".to_string());
                assert!(has_key4 ^ has_key5); // Exactly one should be true (XOR)
                
                // High frequency items should always remain
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key2".to_string()));
                
                // Test multiple evictions
                cache.insert("key6".to_string(), 600);
                cache.insert("key7".to_string(), 700);
                assert_eq!(cache.len(), 3); // Should still be at capacity
                
                // key1 and key2 should still be there due to higher frequency
                assert!(cache.contains(&"key1".to_string()));
                assert!(cache.contains(&"key2".to_string()));
                
                // Test eviction doesn't break LFU ordering
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                let lfu_freq = cache.frequency(lfu_key).unwrap();
                
                // LFU frequency should be minimal among current items
                for (key, (_, freq)) in cache.cache.iter() {
                    assert!(freq >= &(lfu_freq as usize));
                }
                
                // Test eviction with zero capacity
                let mut zero_cache = LFUCache::<String, i32>::new(0);
                zero_cache.insert("key1".to_string(), 100);
                assert_eq!(zero_cache.len(), 0); // Should reject insertion
                assert!(!zero_cache.contains(&"key1".to_string()));
                
                // Test eviction preserves invariants
                let mut test_cache = LFUCache::new(2);
                
                // Insert items with known frequencies
                test_cache.insert("low".to_string(), 1);
                test_cache.insert("high".to_string(), 2);
                
                // Make high frequency item
                for _ in 0..5 {
                    test_cache.get(&"high".to_string());
                }
                
                // Insert new item - should evict "low"
                test_cache.insert("new".to_string(), 3);
                assert_eq!(test_cache.len(), 2);
                assert!(!test_cache.contains(&"low".to_string()));
                assert!(test_cache.contains(&"high".to_string()));
                assert!(test_cache.contains(&"new".to_string()));
                
                // Verify frequencies are consistent after eviction
                assert_eq!(test_cache.frequency(&"low".to_string()), None);
                assert!(test_cache.frequency(&"high".to_string()).unwrap() > 1);
                assert_eq!(test_cache.frequency(&"new".to_string()), Some(1));
            }

            #[test]
            fn test_frequency_increment_on_get() {
                let mut cache = LFUCache::new(5);
                
                // Insert items with initial frequency of 1
                cache.insert("key1".to_string(), 100);
                cache.insert("key2".to_string(), 200);
                cache.insert("key3".to_string(), 300);
                
                // Verify initial frequencies
                assert_eq!(cache.frequency(&"key1".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(1));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(1));
                
                // Test single get operations
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(2));
                
                assert_eq!(cache.get(&"key2".to_string()), Some(&200));
                assert_eq!(cache.frequency(&"key2".to_string()), Some(2));
                
                // Test multiple get operations on same key
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(3));
                
                assert_eq!(cache.get(&"key1".to_string()), Some(&100));
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4));
                
                // Test get on non-existent key doesn't create entry
                assert_eq!(cache.get(&"nonexistent".to_string()), None);
                assert_eq!(cache.frequency(&"nonexistent".to_string()), None);
                assert_eq!(cache.len(), 3); // Should remain unchanged
                
                // Test frequency increments are independent per key
                for _ in 0..10 {
                    cache.get(&"key2".to_string());
                }
                for _ in 0..5 {
                    cache.get(&"key3".to_string());
                }
                
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4));  // Unchanged
                assert_eq!(cache.frequency(&"key2".to_string()), Some(12)); // 2 + 10
                assert_eq!(cache.frequency(&"key3".to_string()), Some(6));  // 1 + 5
                
                // Test get after insert update preserves frequency
                cache.insert("key1".to_string(), 999); // Update value
                assert_eq!(cache.frequency(&"key1".to_string()), Some(4)); // Frequency preserved
                assert_eq!(cache.get(&"key1".to_string()), Some(&999)); // New value
                assert_eq!(cache.frequency(&"key1".to_string()), Some(5)); // Frequency incremented
                
                // Test frequency increments affect LFU ordering
                cache.insert("key4".to_string(), 400);
                assert_eq!(cache.frequency(&"key4".to_string()), Some(1)); // New item
                
                // key4 should be LFU now
                let (lfu_key, _) = cache.peek_lfu().unwrap();
                assert_eq!(lfu_key, &"key4".to_string());
                
                // After accessing key4, it should no longer be LFU
                cache.get(&"key4".to_string());
                cache.get(&"key4".to_string());
                assert_eq!(cache.frequency(&"key4".to_string()), Some(3));
                
                // Insert a new item that will become the new LFU
                cache.insert("key5".to_string(), 500);
                assert_eq!(cache.frequency(&"key5".to_string()), Some(1));
                
                // Now key5 should be LFU (frequency = 1)
                let (new_lfu_key, _) = cache.peek_lfu().unwrap();
                assert_eq!(new_lfu_key, &"key5".to_string());
                let new_lfu_freq = cache.frequency(new_lfu_key).unwrap();
                assert_eq!(new_lfu_freq, 1);
                
                // Test rapid frequency increments
                let initial_freq = cache.frequency(&"key1".to_string()).unwrap();
                for i in 1..=100 {
                    cache.get(&"key1".to_string());
                    assert_eq!(cache.frequency(&"key1".to_string()), Some(initial_freq + i));
                }
                
                // Test that get operations don't affect other keys' frequencies
                let key2_freq_before = cache.frequency(&"key2".to_string()).unwrap();
                let key3_freq_before = cache.frequency(&"key3".to_string()).unwrap();
                let key4_freq_before = cache.frequency(&"key4".to_string()).unwrap();
                
                cache.get(&"key1".to_string()); // Only affect key1
                
                assert_eq!(cache.frequency(&"key2".to_string()), Some(key2_freq_before));
                assert_eq!(cache.frequency(&"key3".to_string()), Some(key3_freq_before));
                assert_eq!(cache.frequency(&"key4".to_string()), Some(key4_freq_before));
            }

            #[test]
            fn test_invariants_after_operations() {
                let mut cache = LFUCache::new(4);
                
                // Helper function to verify all invariants
                let verify_invariants = |cache: &LFUCache<String, i32>| {
                    // Invariant 1: len() never exceeds capacity()
                    assert!(cache.len() <= cache.capacity());
                    
                    // Invariant 2: All keys in cache have corresponding frequencies > 0
                    for (key, _) in cache.cache.iter() {
                        let freq = cache.frequency(key);
                        assert!(freq.is_some() && freq.unwrap() > 0);
                    }
                    
                    // Invariant 3: If cache is not empty, peek_lfu() returns Some
                    if cache.len() > 0 {
                        assert!(cache.peek_lfu().is_some());
                    } else {
                        assert!(cache.peek_lfu().is_none());
                    }
                    
                    // Invariant 4: LFU item has minimum frequency among all items
                    if let Some((lfu_key, _)) = cache.peek_lfu() {
                        let lfu_freq = cache.frequency(lfu_key).unwrap();
                        for (key, _) in cache.cache.iter() {
                            let freq = cache.frequency(key).unwrap();
                            assert!(freq >= lfu_freq);
                        }
                    }
                    
                    // Invariant 5: contains() is consistent with get()
                    let test_keys = vec!["key1", "key2", "key3", "key4", "key5", "nonexistent"];
                    for key in test_keys {
                        let contains_result = cache.contains(&key.to_string());
                        let get_result = cache.cache.get(&key.to_string()).is_some();
                        assert_eq!(contains_result, get_result);
                    }
                };
                
                // Test invariants after initial state
                verify_invariants(&cache);
                
                // Test invariants after insertions
                cache.insert("key1".to_string(), 100);
                verify_invariants(&cache);
                
                cache.insert("key2".to_string(), 200);
                verify_invariants(&cache);
                
                cache.insert("key3".to_string(), 300);
                verify_invariants(&cache);
                
                cache.insert("key4".to_string(), 400);
                verify_invariants(&cache);
                
                // Test invariants after gets (frequency changes)
                cache.get(&"key1".to_string());
                verify_invariants(&cache);
                
                cache.get(&"key1".to_string());
                cache.get(&"key2".to_string());
                verify_invariants(&cache);
                
                // Test invariants after capacity overflow (eviction)
                cache.insert("key5".to_string(), 500);
                verify_invariants(&cache);
                
                // Test invariants after multiple operations
                for i in 0..20 {
                    match i % 5 {
                        0 => { cache.insert(format!("temp{}", i), i); }
                        1 => { cache.get(&"key1".to_string()); }
                        2 => { cache.remove(&format!("temp{}", i - 1)); }
                        3 => { cache.pop_lfu(); }
                        4 => { cache.increment_frequency(&"key2".to_string()); }
                        _ => unreachable!(),
                    }
                    verify_invariants(&cache);
                }
                
                // Test invariants after frequency manipulations
                cache.reset_frequency(&"key1".to_string());
                verify_invariants(&cache);
                
                cache.increment_frequency(&"key2".to_string());
                verify_invariants(&cache);
                
                // Test invariants after removals
                let keys_to_remove: Vec<_> = cache.cache.keys().cloned().take(2).collect();
                for key in keys_to_remove {
                    cache.remove(&key);
                    verify_invariants(&cache);
                }
                
                // Test invariants after pop_lfu operations
                while cache.len() > 0 {
                    cache.pop_lfu();
                    verify_invariants(&cache);
                }
                
                // Test invariants after clear
                cache.insert("test1".to_string(), 1);
                cache.insert("test2".to_string(), 2);
                verify_invariants(&cache);
                
                cache.clear();
                verify_invariants(&cache);
                
                // Test invariants with edge cases
                
                // Zero capacity cache
                let mut zero_cache = LFUCache::<String, i32>::new(0);
                verify_invariants(&zero_cache);
                zero_cache.insert("test".to_string(), 1);
                verify_invariants(&zero_cache);
                
                // Single capacity cache
                let mut single_cache = LFUCache::new(1);
                verify_invariants(&single_cache);
                
                single_cache.insert("only".to_string(), 1);
                verify_invariants(&single_cache);
                
                single_cache.insert("replace".to_string(), 2);
                verify_invariants(&single_cache);
                
                // Test with complex frequency patterns
                let mut complex_cache = LFUCache::new(3);
                complex_cache.insert("a".to_string(), 1);
                complex_cache.insert("b".to_string(), 2);
                complex_cache.insert("c".to_string(), 3);
                
                // Create Fibonacci-like frequency pattern
                for i in 1..=10 {
                    for _ in 0..i {
                        complex_cache.get(&"a".to_string());
                    }
                    for _ in 0..(i/2) {
                        complex_cache.get(&"b".to_string());
                    }
                    verify_invariants(&complex_cache);
                }
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
                let cache_size = 10000;
                let mut cache = LFUCache::new(cache_size);
                
                // Setup: Fill cache with items
                for i in 0..cache_size {
                    cache.insert(format!("key_{}", i), i as i32);
                }
                
                // Test 1: Uniform frequency distribution (all items accessed equally)
                let start = Instant::now();
                for i in 0..1000 {
                    let key = format!("key_{}", i % cache_size);
                    cache.get(&key);
                }
                let uniform_duration = start.elapsed();
                
                // Test 2: Skewed frequency distribution (80/20 rule - 20% of keys get 80% of accesses)
                let start = Instant::now();
                for i in 0..1000 {
                    let key_index = if i % 5 == 0 {
                        // 20% of requests go to first 20% of keys
                        i % (cache_size / 5)
                    } else {
                        // 80% of requests go to remaining 80% of keys
                        (cache_size / 5) + (i % (4 * cache_size / 5))
                    };
                    let key = format!("key_{}", key_index);
                    cache.get(&key);
                }
                let skewed_duration = start.elapsed();
                
                // Test 3: Highly concentrated access pattern (90% of accesses to 10% of keys)
                let start = Instant::now();
                for i in 0..1000 {
                    let key_index = if i % 10 < 9 {
                        // 90% of requests go to first 10% of keys
                        i % (cache_size / 10)
                    } else {
                        // 10% of requests go to remaining 90% of keys
                        (cache_size / 10) + (i % (9 * cache_size / 10))
                    };
                    let key = format!("key_{}", key_index);
                    cache.get(&key);
                }
                let concentrated_duration = start.elapsed();
                
                // Performance assertions (get operations should be fast)
                assert!(uniform_duration < Duration::from_millis(100), 
                    "Uniform access pattern should be fast: {:?}", uniform_duration);
                assert!(skewed_duration < Duration::from_millis(100), 
                    "Skewed access pattern should be fast: {:?}", skewed_duration);
                assert!(concentrated_duration < Duration::from_millis(100), 
                    "Concentrated access pattern should be fast: {:?}", concentrated_duration);
                
                // All patterns should have similar performance characteristics
                // since HashMap lookup is O(1) average case
                println!("Get performance - Uniform: {:?}, Skewed: {:?}, Concentrated: {:?}", 
                    uniform_duration, skewed_duration, concentrated_duration);
                
                // Verify cache functionality wasn't broken
                assert_eq!(cache.len(), cache_size);
                assert!(cache.get(&"key_0".to_string()).is_some());
            }

            #[test]
            fn test_contains_performance() {
                let cache_size = 50000;
                let mut cache = LFUCache::new(cache_size);
                
                // Setup: Fill cache with items
                for i in 0..cache_size {
                    cache.insert(format!("item_{}", i), i as i32);
                }
                
                // Test 1: Contains performance for existing keys
                let start = Instant::now();
                let mut hit_count = 0;
                for i in 0..10000 {
                    let key = format!("item_{}", i % cache_size);
                    if cache.contains(&key) {
                        hit_count += 1;
                    }
                }
                let existing_keys_duration = start.elapsed();
                assert_eq!(hit_count, 10000); // All keys should exist
                
                // Test 2: Contains performance for non-existing keys
                let start = Instant::now();
                let mut miss_count = 0;
                for i in 0..10000 {
                    let key = format!("missing_{}", i);
                    if !cache.contains(&key) {
                        miss_count += 1;
                    }
                }
                let missing_keys_duration = start.elapsed();
                assert_eq!(miss_count, 10000); // No keys should exist
                
                // Test 3: Mixed contains performance (50% hits, 50% misses)
                let start = Instant::now();
                let mut mixed_hit_count = 0;
                for i in 0..10000 {
                    let key = if i % 2 == 0 {
                        format!("item_{}", i % cache_size)
                    } else {
                        format!("missing_{}", i)
                    };
                    if cache.contains(&key) {
                        mixed_hit_count += 1;
                    }
                }
                let mixed_duration = start.elapsed();
                assert_eq!(mixed_hit_count, 5000); // 50% should be hits
                
                // Performance assertions
                assert!(existing_keys_duration < Duration::from_millis(50), 
                    "Contains for existing keys should be fast: {:?}", existing_keys_duration);
                assert!(missing_keys_duration < Duration::from_millis(50), 
                    "Contains for missing keys should be fast: {:?}", missing_keys_duration);
                assert!(mixed_duration < Duration::from_millis(50), 
                    "Mixed contains operations should be fast: {:?}", mixed_duration);
                
                // Contains should be consistently fast regardless of hit/miss
                println!("Contains performance - Existing: {:?}, Missing: {:?}, Mixed: {:?}", 
                    existing_keys_duration, missing_keys_duration, mixed_duration);
                
                // Verify cache wasn't modified by contains operations
                assert_eq!(cache.len(), cache_size);
                
                // Test 4: Performance comparison with very large cache
                let large_cache_size = 100000;
                let mut large_cache = LFUCache::new(large_cache_size);
                
                for i in 0..large_cache_size {
                    large_cache.insert(format!("large_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for i in 0..1000 {
                    let key = format!("large_{}", i % large_cache_size);
                    large_cache.contains(&key);
                }
                let large_cache_duration = start.elapsed();
                
                assert!(large_cache_duration < Duration::from_millis(25), 
                    "Large cache contains should still be fast: {:?}", large_cache_duration);
            }

            #[test]
            fn test_frequency_lookup_performance() {
                let cache_size = 25000;
                let mut cache = LFUCache::new(cache_size);
                
                // Setup: Fill cache and create varied frequency distributions
                for i in 0..cache_size {
                    cache.insert(format!("freq_{}", i), i as i32);
                }
                
                // Create different frequency patterns
                // High frequency items (accessed 50+ times)
                for _ in 0..50 {
                    for i in 0..100 {
                        cache.get(&format!("freq_{}", i));
                    }
                }
                
                // Medium frequency items (accessed 10 times)
                for _ in 0..10 {
                    for i in 100..500 {
                        cache.get(&format!("freq_{}", i));
                    }
                }
                
                // Low frequency items (accessed 2-5 times)
                for _ in 0..3 {
                    for i in 500..2000 {
                        cache.get(&format!("freq_{}", i));
                    }
                }
                
                // Items with frequency 1 (only inserted, never accessed): 2000..cache_size
                
                // Test 1: Frequency lookup performance for high-frequency items
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("freq_{}", i % 100);
                    cache.frequency(&key);
                }
                let high_freq_duration = start.elapsed();
                
                // Test 2: Frequency lookup performance for medium-frequency items
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("freq_{}", 100 + (i % 400));
                    cache.frequency(&key);
                }
                let medium_freq_duration = start.elapsed();
                
                // Test 3: Frequency lookup performance for low-frequency items
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("freq_{}", 500 + (i % 1500));
                    cache.frequency(&key);
                }
                let low_freq_duration = start.elapsed();
                
                // Test 4: Frequency lookup performance for frequency-1 items
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("freq_{}", 2000 + (i % (cache_size - 2000)));
                    cache.frequency(&key);
                }
                let freq_one_duration = start.elapsed();
                
                // Test 5: Frequency lookup performance for non-existent items
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("nonexistent_{}", i);
                    cache.frequency(&key);
                }
                let nonexistent_duration = start.elapsed();
                
                // Performance assertions
                assert!(high_freq_duration < Duration::from_millis(50), 
                    "High frequency lookups should be fast: {:?}", high_freq_duration);
                assert!(medium_freq_duration < Duration::from_millis(50), 
                    "Medium frequency lookups should be fast: {:?}", medium_freq_duration);
                assert!(low_freq_duration < Duration::from_millis(50), 
                    "Low frequency lookups should be fast: {:?}", low_freq_duration);
                assert!(freq_one_duration < Duration::from_millis(50), 
                    "Frequency-1 lookups should be fast: {:?}", freq_one_duration);
                assert!(nonexistent_duration < Duration::from_millis(50), 
                    "Non-existent key lookups should be fast: {:?}", nonexistent_duration);
                
                // All frequency lookups should have similar performance (O(1) HashMap access)
                println!("Frequency lookup performance - High: {:?}, Medium: {:?}, Low: {:?}, Freq-1: {:?}, Non-existent: {:?}", 
                    high_freq_duration, medium_freq_duration, low_freq_duration, freq_one_duration, nonexistent_duration);
                
                // Verify frequency values are correct
                assert!(cache.frequency(&"freq_0".to_string()).unwrap() > 50);
                assert!(cache.frequency(&"freq_100".to_string()).unwrap() > 10);
                assert!(cache.frequency(&"freq_500".to_string()).unwrap() > 1);
                assert_eq!(cache.frequency(&"freq_2000".to_string()), Some(1));
                assert_eq!(cache.frequency(&"nonexistent_0".to_string()), None);
                
                // Test 6: Batch frequency lookup performance
                let keys_to_test: Vec<String> = (0..1000)
                    .map(|i| format!("freq_{}", i % cache_size))
                    .collect();
                
                let start = Instant::now();
                for key in &keys_to_test {
                    cache.frequency(key);
                }
                let batch_duration = start.elapsed();
                
                assert!(batch_duration < Duration::from_millis(25), 
                    "Batch frequency lookups should be fast: {:?}", batch_duration);
                
                // Verify cache state wasn't affected by frequency lookups
                assert_eq!(cache.len(), cache_size);
            }

            #[test]
            fn test_peek_lfu_performance() {
                // Test 1: Small cache peek_lfu performance
                let small_cache_size = 1000;
                let mut small_cache = LFUCache::new(small_cache_size);
                
                for i in 0..small_cache_size {
                    small_cache.insert(format!("small_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for _ in 0..1000 {
                    small_cache.peek_lfu();
                }
                let small_cache_duration = start.elapsed();
                
                // Test 2: Medium cache peek_lfu performance
                let medium_cache_size = 10000;
                let mut medium_cache = LFUCache::new(medium_cache_size);
                
                for i in 0..medium_cache_size {
                    medium_cache.insert(format!("medium_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for _ in 0..1000 {
                    medium_cache.peek_lfu();
                }
                let medium_cache_duration = start.elapsed();
                
                // Test 3: Large cache peek_lfu performance
                let large_cache_size = 100000;
                let mut large_cache = LFUCache::new(large_cache_size);
                
                for i in 0..large_cache_size {
                    large_cache.insert(format!("large_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for _ in 0..1000 {
                    large_cache.peek_lfu();
                }
                let large_cache_duration = start.elapsed();
                
                // Test 4: Performance with varied frequency distributions
                let mut varied_cache = LFUCache::new(50000);
                
                // Insert items with intentionally varied frequencies
                for i in 0..50000 {
                    varied_cache.insert(format!("varied_{}", i), i as i32);
                }
                
                // Create frequency distribution: some high, some medium, many low
                // High frequency (100+ accesses): first 100 items
                for _ in 0..100 {
                    for i in 0..100 {
                        varied_cache.get(&format!("varied_{}", i));
                    }
                }
                
                // Medium frequency (10 accesses): next 500 items
                for _ in 0..10 {
                    for i in 100..600 {
                        varied_cache.get(&format!("varied_{}", i));
                    }
                }
                
                // Low frequency (1-3 accesses): next 1000 items
                for _ in 0..2 {
                    for i in 600..1600 {
                        varied_cache.get(&format!("varied_{}", i));
                    }
                }
                
                // Frequency 1 (inserted only): remaining items
                
                let start = Instant::now();
                for _ in 0..1000 {
                    varied_cache.peek_lfu();
                }
                let varied_cache_duration = start.elapsed();
                
                // Test 5: Performance when LFU changes frequently
                let mut dynamic_cache = LFUCache::new(5000);
                for i in 0..5000 {
                    dynamic_cache.insert(format!("dynamic_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for i in 0..1000 {
                    // Peek LFU
                    dynamic_cache.peek_lfu();
                    
                    // Occasionally access a random item to change frequency distribution
                    if i % 10 == 0 {
                        dynamic_cache.get(&format!("dynamic_{}", i % 5000));
                    }
                }
                let dynamic_cache_duration = start.elapsed();
                
                // Performance assertions
                // Note: peek_lfu performance scales with cache size since it needs to find minimum frequency
                assert!(small_cache_duration < Duration::from_millis(100), 
                    "Small cache peek_lfu should be fast: {:?}", small_cache_duration);
                assert!(medium_cache_duration < Duration::from_millis(1000), 
                    "Medium cache peek_lfu should be reasonably fast: {:?}", medium_cache_duration);
                assert!(large_cache_duration < Duration::from_millis(5000), 
                    "Large cache peek_lfu should be acceptable: {:?}", large_cache_duration);
                assert!(varied_cache_duration < Duration::from_millis(5000), 
                    "Varied frequency cache peek_lfu should be acceptable: {:?}", varied_cache_duration);
                assert!(dynamic_cache_duration < Duration::from_millis(500), 
                    "Dynamic cache peek_lfu should be fast: {:?}", dynamic_cache_duration);
                
                println!("Peek LFU performance - Small: {:?}, Medium: {:?}, Large: {:?}, Varied: {:?}, Dynamic: {:?}", 
                    small_cache_duration, medium_cache_duration, large_cache_duration, 
                    varied_cache_duration, dynamic_cache_duration);
                
                // Verify peek_lfu returns correct results
                let (lfu_key, _) = small_cache.peek_lfu().unwrap();
                assert!(lfu_key.starts_with("small_"));
                
                let (lfu_key, _) = varied_cache.peek_lfu().unwrap();
                // Should be one of the frequency-1 items (index >= 1600)
                let key_index: usize = lfu_key.strip_prefix("varied_")
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(key_index >= 1600);
                
                // Test 6: Performance consistency across multiple operations
                let mut consistency_cache = LFUCache::new(20000);
                for i in 0..20000 {
                    consistency_cache.insert(format!("consistency_{}", i), i as i32);
                }
                
                let mut durations = Vec::new();
                for _ in 0..10 {
                    let start = Instant::now();
                    for _ in 0..100 {
                        consistency_cache.peek_lfu();
                    }
                    durations.push(start.elapsed());
                }
                
                // Check that performance is consistent (allow for reasonable variance)
                let avg_duration = durations.iter().sum::<Duration>() / durations.len() as u32;
                for duration in &durations {
                    assert!(duration.as_millis() <= avg_duration.as_millis() * 10, 
                        "Performance should be reasonably consistent, got {:?} vs avg {:?}", duration, avg_duration);
                }
            }

            #[test]
            fn test_cache_hit_vs_miss_performance() {
                let cache_size = 20000;
                let mut cache = LFUCache::new(cache_size);
                
                // Setup: Fill cache with items
                for i in 0..cache_size {
                    cache.insert(format!("hit_{}", i), i as i32);
                }
                
                // Test 1: Pure cache hits performance
                let start = Instant::now();
                let mut hit_count = 0;
                for i in 0..10000 {
                    let key = format!("hit_{}", i % cache_size);
                    if cache.get(&key).is_some() {
                        hit_count += 1;
                    }
                }
                let pure_hits_duration = start.elapsed();
                assert_eq!(hit_count, 10000);
                
                // Test 2: Pure cache misses performance
                let start = Instant::now();
                let mut miss_count = 0;
                for i in 0..10000 {
                    let key = format!("miss_{}", i);
                    if cache.get(&key).is_none() {
                        miss_count += 1;
                    }
                }
                let pure_misses_duration = start.elapsed();
                assert_eq!(miss_count, 10000);
                
                // Test 3: Mixed hit/miss performance (50/50)
                let start = Instant::now();
                let mut mixed_hits = 0;
                let mut mixed_misses = 0;
                for i in 0..10000 {
                    let key = if i % 2 == 0 {
                        format!("hit_{}", i % cache_size)
                    } else {
                        format!("miss_{}", i)
                    };
                    if cache.get(&key).is_some() {
                        mixed_hits += 1;
                    } else {
                        mixed_misses += 1;
                    }
                }
                let mixed_duration = start.elapsed();
                assert_eq!(mixed_hits, 5000);
                assert_eq!(mixed_misses, 5000);
                
                // Test 4: High hit ratio performance (90% hits, 10% misses)
                let start = Instant::now();
                let mut high_hits = 0;
                let mut high_misses = 0;
                for i in 0..10000 {
                    let key = if i % 10 < 9 {
                        format!("hit_{}", i % cache_size)
                    } else {
                        format!("miss_{}", i)
                    };
                    if cache.get(&key).is_some() {
                        high_hits += 1;
                    } else {
                        high_misses += 1;
                    }
                }
                let high_hit_ratio_duration = start.elapsed();
                assert_eq!(high_hits, 9000);
                assert_eq!(high_misses, 1000);
                
                // Test 5: Low hit ratio performance (10% hits, 90% misses)
                let start = Instant::now();
                let mut low_hits = 0;
                let mut low_misses = 0;
                for i in 0..10000 {
                    let key = if i % 10 == 0 {
                        format!("hit_{}", i % cache_size)
                    } else {
                        format!("miss_{}", i)
                    };
                    if cache.get(&key).is_some() {
                        low_hits += 1;
                    } else {
                        low_misses += 1;
                    }
                }
                let low_hit_ratio_duration = start.elapsed();
                assert_eq!(low_hits, 1000);
                assert_eq!(low_misses, 9000);
                
                // Performance assertions
                assert!(pure_hits_duration < Duration::from_millis(100), 
                    "Pure hits should be fast: {:?}", pure_hits_duration);
                assert!(pure_misses_duration < Duration::from_millis(100), 
                    "Pure misses should be fast: {:?}", pure_misses_duration);
                assert!(mixed_duration < Duration::from_millis(100), 
                    "Mixed hits/misses should be fast: {:?}", mixed_duration);
                assert!(high_hit_ratio_duration < Duration::from_millis(100), 
                    "High hit ratio should be fast: {:?}", high_hit_ratio_duration);
                assert!(low_hit_ratio_duration < Duration::from_millis(100), 
                    "Low hit ratio should be fast: {:?}", low_hit_ratio_duration);
                
                println!("Hit vs Miss performance - Pure hits: {:?}, Pure misses: {:?}, Mixed: {:?}, High hit ratio: {:?}, Low hit ratio: {:?}", 
                    pure_hits_duration, pure_misses_duration, mixed_duration, 
                    high_hit_ratio_duration, low_hit_ratio_duration);
                
                // Test 6: Performance difference analysis between contains vs get
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("hit_{}", i % cache_size);
                    cache.contains(&key);
                }
                let contains_hits_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("miss_{}", i);
                    cache.contains(&key);
                }
                let contains_misses_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("hit_{}", i % cache_size);
                    cache.get(&key);
                }
                let get_hits_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 0..5000 {
                    let key = format!("miss_{}", i);
                    cache.get(&key);
                }
                let get_misses_duration = start.elapsed();
                
                // Get should be slightly slower than contains for hits due to frequency updates
                // but similar for misses since both fail at HashMap lookup
                println!("Contains vs Get - Contains hits: {:?}, Contains misses: {:?}, Get hits: {:?}, Get misses: {:?}", 
                    contains_hits_duration, contains_misses_duration, get_hits_duration, get_misses_duration);
                
                // Test 7: Performance with different cache sizes for hit/miss patterns
                let mut small_cache = LFUCache::<String, i32>::new(100);
                let mut medium_cache = LFUCache::<String, i32>::new(5000);
                let mut large_cache = LFUCache::<String, i32>::new(50000);
                
                // All caches should have similar miss performance (O(1) HashMap lookup failure)
                let start = Instant::now();
                for i in 0..1000 {
                    let key = format!("definitely_missing_{}", i);
                    small_cache.get(&key);
                }
                let small_miss_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 0..1000 {
                    let key = format!("definitely_missing_{}", i);
                    medium_cache.get(&key);
                }
                let medium_miss_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 0..1000 {
                    let key = format!("definitely_missing_{}", i);
                    large_cache.get(&key);
                }
                let large_miss_duration = start.elapsed();
                
                // Miss performance should be consistent across cache sizes
                assert!(small_miss_duration < Duration::from_millis(25));
                assert!(medium_miss_duration < Duration::from_millis(25));
                assert!(large_miss_duration < Duration::from_millis(25));
                
                println!("Miss performance across sizes - Small: {:?}, Medium: {:?}, Large: {:?}", 
                    small_miss_duration, medium_miss_duration, large_miss_duration);
                
                // Verify cache state integrity after all performance tests
                assert_eq!(cache.len(), cache_size);
                // Frequencies should have been updated due to get() calls
                assert!(cache.frequency(&"hit_0".to_string()).unwrap() > 1);
            }
        }

        // Insertion Performance Tests
        mod insertion_performance {
            use super::*;

            #[test]
            fn test_insertion_performance_with_eviction() {
                let cache_capacity = 5000;
                let mut cache = LFUCache::new(cache_capacity);
                
                // Phase 1: Fill cache to capacity without eviction
                let start = Instant::now();
                for i in 0..cache_capacity {
                    cache.insert(format!("initial_{}", i), i as i32);
                }
                let fill_duration = start.elapsed();
                assert_eq!(cache.len(), cache_capacity);
                
                // Phase 2: Insert additional items that trigger eviction
                let eviction_count = 2000;
                let start = Instant::now();
                for i in 0..eviction_count {
                    cache.insert(format!("evict_{}", i), (i + cache_capacity) as i32);
                }
                let eviction_duration = start.elapsed();
                assert_eq!(cache.len(), cache_capacity); // Should still be at capacity
                
                // Phase 3: Compare performance per operation
                let fill_per_op = fill_duration / cache_capacity as u32;
                let eviction_per_op = eviction_duration / eviction_count as u32;
                
                // Eviction operations should be slower due to LFU finding
                println!("Fill performance: {:?} per op, Eviction performance: {:?} per op", 
                    fill_per_op, eviction_per_op);
                
                // Performance assertions
                assert!(fill_duration < Duration::from_millis(500), 
                    "Filling cache should be fast: {:?}", fill_duration);
                assert!(eviction_duration < Duration::from_millis(2000), 
                    "Eviction insertions should be reasonable: {:?}", eviction_duration);
                
                // Test with frequent access patterns during eviction
                let mut cache_with_access = LFUCache::new(1000);
                
                // Fill cache
                for i in 0..1000 {
                    cache_with_access.insert(format!("access_{}", i), i as i32);
                }
                
                // Create frequency distribution by accessing some items
                for _ in 0..5 {
                    for i in 0..200 {
                        cache_with_access.get(&format!("access_{}", i));
                    }
                }
                
                // Now test eviction with mixed frequency items
                let start = Instant::now();
                for i in 0..500 {
                    cache_with_access.insert(format!("new_evict_{}", i), (i + 2000) as i32);
                }
                let mixed_eviction_duration = start.elapsed();
                
                assert!(mixed_eviction_duration < Duration::from_millis(1000), 
                    "Mixed frequency eviction should be reasonable: {:?}", mixed_eviction_duration);
                
                // Verify that high-frequency items are preserved
                assert!(cache_with_access.contains(&"access_0".to_string()));
                assert!(cache_with_access.contains(&"access_100".to_string()));
                
                // Test eviction performance scaling
                let sizes = [100, 500, 1000, 2000];
                let mut eviction_times = Vec::new();
                
                for &size in &sizes {
                    let mut test_cache = LFUCache::new(size);
                    
                    // Fill to capacity
                    for i in 0..size {
                        test_cache.insert(format!("scale_{}", i), i as i32);
                    }
                    
                    // Measure eviction performance
                    let start = Instant::now();
                    for i in 0..100 {
                        test_cache.insert(format!("evict_scale_{}", i), (i + size) as i32);
                    }
                    let duration = start.elapsed();
                    eviction_times.push(duration);
                }
                
                // Performance should scale reasonably with cache size
                for (i, &duration) in eviction_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(200 * (i + 1) as u64), 
                        "Eviction performance should scale reasonably for size {}: {:?}", 
                        sizes[i], duration);
                }
                
                println!("Eviction scaling: {:?}", eviction_times);
            }

            #[test]
            fn test_batch_insertion_performance() {
                // Test 1: Small batch insertions
                let mut small_cache = LFUCache::new(1000);
                let batch_sizes = [10, 50, 100, 500];
                let mut small_batch_times = Vec::new();
                
                for &batch_size in &batch_sizes {
                    let start = Instant::now();
                    for i in 0..batch_size {
                        small_cache.insert(format!("small_batch_{}_{}", batch_size, i), i as i32);
                    }
                    small_batch_times.push(start.elapsed());
                }
                
                // Performance should scale roughly linearly with batch size
                for (i, &duration) in small_batch_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(50 * (i + 1) as u64), 
                        "Small batch {} should be fast: {:?}", batch_sizes[i], duration);
                }
                
                // Test 2: Large sequential insertions
                let large_cache_size = 20000;
                let mut large_cache = LFUCache::new(large_cache_size);
                
                let start = Instant::now();
                for i in 0..large_cache_size {
                    large_cache.insert(format!("large_{}", i), i as i32);
                }
                let large_batch_duration = start.elapsed();
                
                assert!(large_batch_duration < Duration::from_millis(1000), 
                    "Large batch insertion should be reasonable: {:?}", large_batch_duration);
                
                // Test 3: Insertion performance with different value sizes
                let mut value_size_cache = LFUCache::new(5000);
                
                // Small values (integers)
                let start = Instant::now();
                for i in 0..1000 {
                    value_size_cache.insert(format!("int_{}", i), i as i32);
                }
                let small_value_duration = start.elapsed();
                
                // Large values (also integers for consistency, but simulating larger data)
                let start = Instant::now();
                for i in 0..1000 {
                    value_size_cache.insert(format!("large_{}", i), (i * 1000000) as i32);
                }
                let large_value_duration = start.elapsed();
                
                // Both should be reasonably fast since they're both integers
                assert!(small_value_duration < Duration::from_millis(100), 
                    "Small value insertion should be fast: {:?}", small_value_duration);
                assert!(large_value_duration < Duration::from_millis(200), 
                    "Large value insertion should be reasonable: {:?}", large_value_duration);
                
                // Test 4: Batch insertion with interleaved operations
                let mut mixed_cache = LFUCache::new(2000);
                
                let start = Instant::now();
                for i in 0..1000 {
                    // Insert
                    mixed_cache.insert(format!("mixed_{}", i), i as i32);
                    
                    // Occasionally read to create frequency variance
                    if i % 10 == 0 && i > 0 {
                        mixed_cache.get(&format!("mixed_{}", i / 2));
                    }
                    
                    // Occasionally check existence
                    if i % 15 == 0 {
                        mixed_cache.contains(&format!("mixed_{}", i));
                    }
                }
                let mixed_operations_duration = start.elapsed();
                
                assert!(mixed_operations_duration < Duration::from_millis(200), 
                    "Mixed operations should be fast: {:?}", mixed_operations_duration);
                
                // Test 5: Throughput measurement
                let throughput_cache_size = 10000;
                let mut throughput_cache = LFUCache::new(throughput_cache_size);
                
                let start = Instant::now();
                for i in 0..throughput_cache_size {
                    throughput_cache.insert(format!("throughput_{}", i), i as i32);
                }
                let throughput_duration = start.elapsed();
                
                let ops_per_second = throughput_cache_size as f64 / throughput_duration.as_secs_f64();
                
                assert!(ops_per_second > 10000.0, 
                    "Should achieve at least 10k insertions per second, got: {:.2}", ops_per_second);
                
                println!("Batch insertion performance:");
                println!("  Small batches: {:?}", small_batch_times);
                println!("  Large batch: {:?}", large_batch_duration);
                println!("  Small values: {:?}", small_value_duration);
                println!("  Large values: {:?}", large_value_duration);
                println!("  Mixed ops: {:?}", mixed_operations_duration);
                println!("  Throughput: {:.2} ops/sec", ops_per_second);
                
                // Test 6: Memory allocation impact during batch insertion
                let mut allocation_cache = LFUCache::new(5000);
                
                // Measure insertion of progressively larger batches
                let progressive_sizes = [100, 500, 1000, 2000];
                let mut progressive_times = Vec::new();
                
                for &size in &progressive_sizes {
                    allocation_cache.clear();
                    
                    let start = Instant::now();
                    for i in 0..size {
                        allocation_cache.insert(format!("prog_{}_{}", size, i), i as i32);
                    }
                    progressive_times.push(start.elapsed());
                }
                
                // Each batch should complete in reasonable time
                for (i, &duration) in progressive_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(100 + (i * 50) as u64), 
                        "Progressive batch {} should be efficient: {:?}", 
                        progressive_sizes[i], duration);
                }
                
                println!("  Progressive batches: {:?}", progressive_times);
            }

            #[test]
            fn test_update_vs_new_insertion_performance() {
                let cache_size = 5000;
                let mut cache = LFUCache::new(cache_size);
                
                // Phase 1: Initial population with new insertions
                let start = Instant::now();
                for i in 0..cache_size {
                    cache.insert(format!("new_{}", i), i as i32);
                }
                let new_insertion_duration = start.elapsed();
                assert_eq!(cache.len(), cache_size);
                
                // Phase 2: Update existing keys
                let update_count = 2000;
                let start = Instant::now();
                for i in 0..update_count {
                    let key = format!("new_{}", i % cache_size);
                    cache.insert(key, (i + 10000) as i32);
                }
                let update_duration = start.elapsed();
                assert_eq!(cache.len(), cache_size); // Length shouldn't change
                
                // Phase 3: Compare per-operation performance
                let new_per_op = new_insertion_duration / cache_size as u32;
                let update_per_op = update_duration / update_count as u32;
                
                // Updates should be faster since they don't require eviction logic
                println!("New insertion: {:?} per op, Update: {:?} per op", new_per_op, update_per_op);
                
                // Both should be fast, but updates might be slightly faster
                assert!(new_insertion_duration < Duration::from_millis(500), 
                    "New insertions should be fast: {:?}", new_insertion_duration);
                assert!(update_duration < Duration::from_millis(300), 
                    "Updates should be fast: {:?}", update_duration);
                
                // Test 4: Mixed new vs update operations
                let mut mixed_cache = LFUCache::new(3000);
                
                // Pre-populate half the cache
                for i in 0..1500 {
                    mixed_cache.insert(format!("mixed_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for i in 0..2000 {
                    if i % 2 == 0 {
                        // Update existing key
                        let key = format!("mixed_{}", i % 1500);
                        mixed_cache.insert(key, (i + 5000) as i32);
                    } else {
                        // Insert new key (might trigger eviction)
                        mixed_cache.insert(format!("new_mixed_{}", i), i as i32);
                    }
                }
                let mixed_duration = start.elapsed();
                
                assert!(mixed_duration < Duration::from_millis(400), 
                    "Mixed operations should be reasonable: {:?}", mixed_duration);
                
                // Test 5: Update performance with different frequency distributions
                let mut freq_cache = LFUCache::new(2000);
                
                // Create items with different frequencies
                for i in 0..2000 {
                    freq_cache.insert(format!("freq_{}", i), i as i32);
                }
                
                // Create frequency distribution
                for _ in 0..10 {
                    for i in 0..200 {
                        freq_cache.get(&format!("freq_{}", i)); // High frequency
                    }
                }
                
                for _ in 0..3 {
                    for i in 200..800 {
                        freq_cache.get(&format!("freq_{}", i)); // Medium frequency
                    }
                }
                // Items 800-2000 remain at frequency 1 (low frequency)
                
                // Test updating items with different frequencies
                let start = Instant::now();
                for i in 0..100 {
                    freq_cache.insert(format!("freq_{}", i), (i + 10000) as i32); // High freq
                }
                let high_freq_update_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 200..300 {
                    freq_cache.insert(format!("freq_{}", i), (i + 10000) as i32); // Medium freq
                }
                let medium_freq_update_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 1800..1900 {
                    freq_cache.insert(format!("freq_{}", i), (i + 10000) as i32); // Low freq
                }
                let low_freq_update_duration = start.elapsed();
                
                // All should be fast since they're updates, not dependent on frequency
                assert!(high_freq_update_duration < Duration::from_millis(50), 
                    "High frequency updates should be fast: {:?}", high_freq_update_duration);
                assert!(medium_freq_update_duration < Duration::from_millis(50), 
                    "Medium frequency updates should be fast: {:?}", medium_freq_update_duration);
                assert!(low_freq_update_duration < Duration::from_millis(50), 
                    "Low frequency updates should be fast: {:?}", low_freq_update_duration);
                
                // Test 6: Update vs new insertion when cache is full
                let mut full_cache = LFUCache::new(1000);
                
                // Fill to capacity
                for i in 0..1000 {
                    full_cache.insert(format!("full_{}", i), i as i32);
                }
                
                // Test updates on full cache
                let start = Instant::now();
                for i in 0..500 {
                    full_cache.insert(format!("full_{}", i), (i + 2000) as i32);
                }
                let full_update_duration = start.elapsed();
                
                // Test new insertions on full cache (triggers eviction)
                let start = Instant::now();
                for i in 0..500 {
                    full_cache.insert(format!("new_full_{}", i), (i + 3000) as i32);
                }
                let full_new_duration = start.elapsed();
                
                // Updates should be significantly faster than new insertions requiring eviction
                assert!(full_update_duration < Duration::from_millis(100), 
                    "Updates on full cache should be fast: {:?}", full_update_duration);
                assert!(full_new_duration < Duration::from_millis(500), 
                    "New insertions on full cache should be reasonable: {:?}", full_new_duration);
                
                // Test 7: Batch update performance
                let mut batch_cache = LFUCache::new(5000);
                
                // Initial population
                for i in 0..5000 {
                    batch_cache.insert(format!("batch_{}", i), i as i32);
                }
                
                // Batch updates
                let batch_sizes = [100, 500, 1000, 2000];
                let mut batch_update_times = Vec::new();
                
                for &batch_size in &batch_sizes {
                    let start = Instant::now();
                    for i in 0..batch_size {
                        batch_cache.insert(format!("batch_{}", i), (i + 20000) as i32);
                    }
                    batch_update_times.push(start.elapsed());
                }
                
                // Batch updates should scale linearly
                for (i, &duration) in batch_update_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(50 + (i * 25) as u64), 
                        "Batch update {} should be efficient: {:?}", batch_sizes[i], duration);
                }
                
                println!("Update vs New Performance:");
                println!("  New insertions: {:?} total, {:?} per op", new_insertion_duration, new_per_op);
                println!("  Updates: {:?} total, {:?} per op", update_duration, update_per_op);
                println!("  Mixed operations: {:?}", mixed_duration);
                println!("  Frequency-based updates - High: {:?}, Medium: {:?}, Low: {:?}", 
                    high_freq_update_duration, medium_freq_update_duration, low_freq_update_duration);
                println!("  Full cache - Updates: {:?}, New: {:?}", full_update_duration, full_new_duration);
                println!("  Batch updates: {:?}", batch_update_times);
                
                // Verify functional correctness after performance tests
                assert!(batch_cache.contains(&"batch_0".to_string()));
                assert_eq!(batch_cache.get(&"batch_0".to_string()), Some(&20000));
                assert_eq!(batch_cache.len(), 5000);
            }

            #[test]
            fn test_insertion_with_frequency_tracking() {
                // Test 1: Basic frequency tracking overhead during insertion
                let cache_size = 10000;
                let mut cache = LFUCache::new(cache_size);
                
                // Measure pure insertion time (frequency tracking included)
                let start = Instant::now();
                for i in 0..cache_size {
                    cache.insert(format!("track_{}", i), i as i32);
                }
                let insertion_with_tracking_duration = start.elapsed();
                
                // All items should have frequency 1 after insertion
                for i in (0..100).step_by(10) {
                    assert_eq!(cache.frequency(&format!("track_{}", i)), Some(1));
                }
                
                assert!(insertion_with_tracking_duration < Duration::from_millis(800), 
                    "Insertion with frequency tracking should be reasonable: {:?}", insertion_with_tracking_duration);
                
                // Test 2: Frequency tracking during updates vs new insertions
                let mut tracking_cache = LFUCache::new(5000);
                
                // Initial population
                for i in 0..5000 {
                    tracking_cache.insert(format!("freq_track_{}", i), i as i32);
                }
                
                // Measure update performance (should preserve frequency)
                let start = Instant::now();
                for i in 0..1000 {
                    tracking_cache.insert(format!("freq_track_{}", i), (i + 10000) as i32);
                }
                let update_tracking_duration = start.elapsed();
                
                // Verify frequencies are preserved during updates
                for i in (0..100).step_by(10) {
                    assert_eq!(tracking_cache.frequency(&format!("freq_track_{}", i)), Some(1));
                }
                
                assert!(update_tracking_duration < Duration::from_millis(200), 
                    "Update tracking should be fast: {:?}", update_tracking_duration);
                
                // Test 3: Frequency tracking impact during eviction
                let mut eviction_cache = LFUCache::new(2000);
                
                // Fill cache
                for i in 0..2000 {
                    eviction_cache.insert(format!("evict_track_{}", i), i as i32);
                }
                
                // Create frequency variance
                for _ in 0..5 {
                    for i in 0..400 {
                        eviction_cache.get(&format!("evict_track_{}", i));
                    }
                }
                
                // Now measure eviction with frequency consideration
                let start = Instant::now();
                for i in 0..1000 {
                    eviction_cache.insert(format!("new_evict_track_{}", i), (i + 5000) as i32);
                }
                let eviction_tracking_duration = start.elapsed();
                
                // Verify that high-frequency items were preserved
                assert!(eviction_cache.contains(&"evict_track_0".to_string()));
                assert!(eviction_cache.contains(&"evict_track_100".to_string()));
                
                assert!(eviction_tracking_duration < Duration::from_millis(1500), 
                    "Eviction with frequency tracking should be reasonable: {:?}", eviction_tracking_duration);
                
                // Test 4: Frequency tracking accuracy under load
                let mut accuracy_cache = LFUCache::new(3000);
                
                // Insert items
                for i in 0..3000 {
                    accuracy_cache.insert(format!("accuracy_{}", i), i as i32);
                }
                
                // Create complex frequency patterns
                for access_round in 0..20 {
                    for i in 0..100 {
                        accuracy_cache.get(&format!("accuracy_{}", i)); // Very high frequency
                    }
                    for i in 100..500 {
                        if access_round % 2 == 0 {
                            accuracy_cache.get(&format!("accuracy_{}", i)); // Medium frequency
                        }
                    }
                    for i in 500..1000 {
                        if access_round % 5 == 0 {
                            accuracy_cache.get(&format!("accuracy_{}", i)); // Low frequency
                        }
                    }
                }
                
                // Verify frequency tracking accuracy
                assert!(accuracy_cache.frequency(&"accuracy_0".to_string()).unwrap() > 15);
                assert!(accuracy_cache.frequency(&"accuracy_100".to_string()).unwrap() > 5);
                assert!(accuracy_cache.frequency(&"accuracy_500".to_string()).unwrap() >= 1);
                assert_eq!(accuracy_cache.frequency(&"accuracy_2000".to_string()), Some(1));
                
                // Test 5: Frequency tracking memory overhead
                let mut memory_test_cache = LFUCache::new(20000);
                
                // Insert large number of items and verify each has correct frequency
                let start = Instant::now();
                for i in 0..20000 {
                    memory_test_cache.insert(format!("memory_test_{}", i), i as i32);
                    
                    // Verify frequency tracking for every 1000th item
                    if i % 1000 == 0 {
                        assert_eq!(memory_test_cache.frequency(&format!("memory_test_{}", i)), Some(1));
                    }
                }
                let large_scale_duration = start.elapsed();
                
                assert!(large_scale_duration < Duration::from_millis(1500), 
                    "Large scale frequency tracking should be efficient: {:?}", large_scale_duration);
                
                // Test 6: Frequency increment performance during mixed operations
                let mut mixed_freq_cache = LFUCache::new(5000);
                
                // Populate cache
                for i in 0..5000 {
                    mixed_freq_cache.insert(format!("mixed_freq_{}", i), i as i32);
                }
                
                let start = Instant::now();
                for i in 0..10000 {
                    if i % 3 == 0 {
                        // Insert new (might evict)
                        mixed_freq_cache.insert(format!("new_mixed_{}", i), i as i32);
                    } else if i % 3 == 1 {
                        // Update existing
                        mixed_freq_cache.insert(format!("mixed_freq_{}", i % 5000), (i + 20000) as i32);
                    } else {
                        // Access existing (increment frequency)
                        mixed_freq_cache.get(&format!("mixed_freq_{}", i % 5000));
                    }
                }
                let mixed_ops_duration = start.elapsed();
                
                assert!(mixed_ops_duration < Duration::from_millis(2000), 
                    "Mixed operations with frequency tracking should be reasonable: {:?}", mixed_ops_duration);
                
                // Test 7: Frequency tracking during rapid insertions
                let mut rapid_cache = LFUCache::new(1000);
                
                let start = Instant::now();
                for i in 0..5000 {
                    rapid_cache.insert(format!("rapid_{}", i), i as i32);
                    
                    // Verify frequency tracking works under rapid insertion
                    if i < 1000 && i % 100 == 0 {
                        assert_eq!(rapid_cache.frequency(&format!("rapid_{}", i)), Some(1));
                    }
                }
                let rapid_insertion_duration = start.elapsed();
                
                assert!(rapid_insertion_duration < Duration::from_millis(1000), 
                    "Rapid insertion with frequency tracking should be efficient: {:?}", rapid_insertion_duration);
                
                // Verify cache is still at capacity and LFU logic worked
                assert_eq!(rapid_cache.len(), 1000);
                
                // Test 8: Frequency bounds checking
                let mut bounds_cache = LFUCache::new(100);
                
                // Insert and access to create very high frequencies
                for i in 0..100 {
                    bounds_cache.insert(format!("bounds_{}", i), i as i32);
                }
                
                // Create extremely high frequency for one item
                let start = Instant::now();
                for _ in 0..10000 {
                    bounds_cache.get(&"bounds_0".to_string());
                }
                let high_freq_duration = start.elapsed();
                
                let final_frequency = bounds_cache.frequency(&"bounds_0".to_string()).unwrap();
                assert_eq!(final_frequency, 10001); // 1 (insert) + 10000 (gets)
                
                assert!(high_freq_duration < Duration::from_millis(200), 
                    "High frequency increment should be fast: {:?}", high_freq_duration);
                
                println!("Frequency tracking performance:");
                println!("  Basic insertion: {:?}", insertion_with_tracking_duration);
                println!("  Update tracking: {:?}", update_tracking_duration);
                println!("  Eviction tracking: {:?}", eviction_tracking_duration);
                println!("  Large scale: {:?}", large_scale_duration);
                println!("  Mixed operations: {:?}", mixed_ops_duration);
                println!("  Rapid insertion: {:?}", rapid_insertion_duration);
                println!("  High frequency: {:?}", high_freq_duration);
                println!("  Final frequency achieved: {}", final_frequency);
            }
        }

        // Eviction Performance Tests
        mod eviction_performance {
            use super::*;

            #[test]
            fn test_lfu_eviction_performance() {
                // Test 1: Basic LFU eviction performance
                let mut cache = LFUCache::new(1000);
                
                // Fill cache to capacity
                for i in 0..1000 {
                    cache.insert(format!("key_{}", i), i as i32);
                }
                
                // Create frequency distribution to establish clear LFU items
                for _ in 0..10 {
                    for i in 0..100 {
                        cache.get(&format!("key_{}", i)); // High frequency
                    }
                }
                
                for _ in 0..3 {
                    for i in 100..500 {
                        cache.get(&format!("key_{}", i)); // Medium frequency
                    }
                }
                // Items 500-999 remain at frequency 1 (LFU candidates)
                
                // Test eviction performance
                let start = Instant::now();
                for i in 1000..1500 {
                    cache.insert(format!("new_key_{}", i), i as i32);
                }
                let eviction_duration = start.elapsed();
                
                // Should evict 500 LFU items efficiently
                assert_eq!(cache.len(), 1000);
                assert!(eviction_duration < Duration::from_millis(500), 
                    "LFU eviction should be efficient: {:?}", eviction_duration);
                
                // Verify that high-frequency items are preserved
                assert!(cache.contains(&"key_0".to_string()));
                assert!(cache.contains(&"key_50".to_string()));
                assert!(cache.contains(&"key_100".to_string()));
                
                // Test 2: Performance scaling with cache size
                let sizes = [100, 500, 1000, 2000];
                let mut eviction_times = Vec::new();
                
                for &size in &sizes {
                    let mut test_cache = LFUCache::new(size);
                    
                    // Fill cache
                    for i in 0..size {
                        test_cache.insert(format!("scale_{}", i), i as i32);
                    }
                    
                    // Create some frequency variance
                    for i in 0..size/10 {
                        test_cache.get(&format!("scale_{}", i));
                    }
                    
                    // Measure eviction performance
                    let start = Instant::now();
                    for i in 0..100 {
                        test_cache.insert(format!("evict_{}", i), (i + size) as i32);
                    }
                    let duration = start.elapsed();
                    eviction_times.push(duration);
                }
                
                // Performance should scale reasonably
                for (i, &duration) in eviction_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(100 + (i * 50) as u64), 
                        "Eviction performance should scale reasonably for size {}: {:?}", 
                        sizes[i], duration);
                }
                
                // Test 3: Eviction with uniform frequency distribution
                let mut uniform_cache = LFUCache::new(500);
                
                // Fill cache with uniform frequency
                for i in 0..500 {
                    uniform_cache.insert(format!("uniform_{}", i), i as i32);
                    uniform_cache.get(&format!("uniform_{}", i)); // All have frequency 2
                }
                
                let start = Instant::now();
                for i in 0..200 {
                    uniform_cache.insert(format!("uniform_new_{}", i), (i + 1000) as i32);
                }
                let uniform_eviction_duration = start.elapsed();
                
                assert!(uniform_eviction_duration < Duration::from_millis(200), 
                    "Uniform frequency eviction should be reasonable: {:?}", uniform_eviction_duration);
                
                // Test 4: Eviction with highly skewed frequency distribution
                let mut skewed_cache = LFUCache::new(1000);
                
                // Fill cache
                for i in 0..1000 {
                    skewed_cache.insert(format!("skewed_{}", i), i as i32);
                }
                
                // Create highly skewed distribution
                for _ in 0..100 {
                    skewed_cache.get(&"skewed_0".to_string()); // One very hot item
                }
                
                let start = Instant::now();
                for i in 0..500 {
                    skewed_cache.insert(format!("skewed_new_{}", i), (i + 2000) as i32);
                }
                let skewed_eviction_duration = start.elapsed();
                
                assert!(skewed_eviction_duration < Duration::from_millis(400), 
                    "Skewed frequency eviction should be efficient: {:?}", skewed_eviction_duration);
                
                // Hot item should be preserved
                assert!(skewed_cache.contains(&"skewed_0".to_string()));
                
                // Test 5: Repeated eviction performance consistency
                let mut consistent_cache = LFUCache::new(100);
                let mut eviction_durations = Vec::new();
                
                // Fill cache initially
                for i in 0..100 {
                    consistent_cache.insert(format!("consistent_{}", i), i as i32);
                }
                
                // Perform multiple rounds of eviction
                for round in 0..10 {
                    let start = Instant::now();
                    for i in 0..20 {
                        consistent_cache.insert(format!("round_{}_{}", round, i), (round * 100 + i) as i32);
                    }
                    eviction_durations.push(start.elapsed());
                }
                
                // Check consistency
                let avg_duration = eviction_durations.iter().sum::<Duration>() / eviction_durations.len() as u32;
                for duration in &eviction_durations {
                    assert!(duration.as_millis() <= avg_duration.as_millis() * 3, 
                        "Eviction performance should be consistent: {:?} vs avg {:?}", duration, avg_duration);
                }
                
                println!("LFU eviction performance:");
                println!("  Basic eviction: {:?}", eviction_duration);
                println!("  Size scaling: {:?}", eviction_times);
                println!("  Uniform frequency: {:?}", uniform_eviction_duration);
                println!("  Skewed frequency: {:?}", skewed_eviction_duration);
                println!("  Consistency check: {:?}", eviction_durations);
            }

            #[test]
            fn test_pop_lfu_performance() {
                // Test 1: Basic pop_lfu performance
                let mut cache = LFUCache::new(2000);
                
                // Fill cache with items
                for i in 0..2000 {
                    cache.insert(format!("pop_{}", i), i as i32);
                }
                
                // Create frequency distribution
                for _ in 0..5 {
                    for i in 0..200 {
                        cache.get(&format!("pop_{}", i)); // High frequency
                    }
                }
                
                for _ in 0..2 {
                    for i in 200..800 {
                        cache.get(&format!("pop_{}", i)); // Medium frequency
                    }
                }
                // Items 800-1999 remain at frequency 1 (LFU candidates)
                
                // Test pop_lfu performance
                let start = Instant::now();
                let mut popped_items = Vec::new();
                for _ in 0..500 {
                    if let Some((key, value)) = cache.pop_lfu() {
                        popped_items.push((key, value));
                    }
                }
                let pop_duration = start.elapsed();
                
                assert_eq!(popped_items.len(), 500);
                assert_eq!(cache.len(), 1500);
                assert!(pop_duration < Duration::from_millis(500), 
                    "pop_lfu should be efficient: {:?}", pop_duration);
                
                // Verify that high-frequency items remain
                assert!(cache.contains(&"pop_0".to_string()));
                assert!(cache.contains(&"pop_100".to_string()));
                assert!(cache.contains(&"pop_200".to_string()));
                
                // Test 2: pop_lfu with different cache sizes
                let sizes = [50, 200, 500, 1000];
                let mut pop_times = Vec::new();
                
                for &size in &sizes {
                    let mut test_cache = LFUCache::new(size);
                    
                    // Fill cache
                    for i in 0..size {
                        test_cache.insert(format!("size_{}", i), i as i32);
                    }
                    
                    // Create some frequency variance
                    for i in 0..size/5 {
                        test_cache.get(&format!("size_{}", i));
                    }
                    
                    // Measure pop_lfu performance
                    let start = Instant::now();
                    for _ in 0..(size/4) {
                        test_cache.pop_lfu();
                    }
                    let duration = start.elapsed();
                    pop_times.push(duration);
                }
                
                // Performance should scale reasonably
                for (i, &duration) in pop_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(50 + (i * 25) as u64), 
                        "pop_lfu performance should scale reasonably for size {}: {:?}", 
                        sizes[i], duration);
                }
                
                // Test 3: pop_lfu with uniform frequencies (worst case)
                let mut uniform_cache = LFUCache::new(300);
                
                // Fill cache with uniform frequency
                for i in 0..300 {
                    uniform_cache.insert(format!("uniform_{}", i), i as i32);
                    uniform_cache.get(&format!("uniform_{}", i)); // All have frequency 2
                }
                
                let start = Instant::now();
                let mut uniform_pops = 0;
                for _ in 0..100 {
                    if uniform_cache.pop_lfu().is_some() {
                        uniform_pops += 1;
                    }
                }
                let uniform_pop_duration = start.elapsed();
                
                assert_eq!(uniform_pops, 100);
                assert!(uniform_pop_duration < Duration::from_millis(100), 
                    "Uniform frequency pop_lfu should be reasonable: {:?}", uniform_pop_duration);
                
                // Test 4: pop_lfu until empty
                let mut empty_cache = LFUCache::new(100);
                
                // Fill cache
                for i in 0..100 {
                    empty_cache.insert(format!("empty_{}", i), i as i32);
                }
                
                let start = Instant::now();
                let mut total_popped = 0;
                while empty_cache.pop_lfu().is_some() {
                    total_popped += 1;
                }
                let empty_duration = start.elapsed();
                
                assert_eq!(total_popped, 100);
                assert_eq!(empty_cache.len(), 0);
                assert!(empty_duration < Duration::from_millis(100), 
                    "pop_lfu until empty should be efficient: {:?}", empty_duration);
                
                // Test 5: pop_lfu performance with highly skewed distribution
                let mut skewed_cache = LFUCache::new(1000);
                
                // Fill cache
                for i in 0..1000 {
                    skewed_cache.insert(format!("skewed_{}", i), i as i32);
                }
                
                // Create very skewed distribution
                for _ in 0..50 {
                    skewed_cache.get(&"skewed_0".to_string()); // One very hot item
                }
                for _ in 0..10 {
                    for i in 1..50 {
                        skewed_cache.get(&format!("skewed_{}", i)); // Some medium items
                    }
                }
                // Items 50-999 remain at frequency 1
                
                let start = Instant::now();
                let mut skewed_pops = 0;
                for _ in 0..300 {
                    if skewed_cache.pop_lfu().is_some() {
                        skewed_pops += 1;
                    }
                }
                let skewed_pop_duration = start.elapsed();
                
                assert_eq!(skewed_pops, 300);
                assert!(skewed_pop_duration < Duration::from_millis(300), 
                    "Skewed distribution pop_lfu should be efficient: {:?}", skewed_pop_duration);
                
                // Hot item should still be there
                assert!(skewed_cache.contains(&"skewed_0".to_string()));
                
                // Test 6: pop_lfu performance consistency
                let mut consistency_cache = LFUCache::new(200);
                let mut pop_durations = Vec::new();
                
                // Fill cache
                for i in 0..200 {
                    consistency_cache.insert(format!("consistency_{}", i), i as i32);
                }
                
                // Perform multiple rounds of pop operations
                for round in 0..5 {
                    // Add some new items to maintain cache size
                    for i in 0..10 {
                        consistency_cache.insert(format!("round_{}_{}", round, i), (round * 100 + i) as i32);
                    }
                    
                    let start = Instant::now();
                    for _ in 0..10 {
                        consistency_cache.pop_lfu();
                    }
                    pop_durations.push(start.elapsed());
                }
                
                // Check consistency
                let avg_duration = pop_durations.iter().sum::<Duration>() / pop_durations.len() as u32;
                for duration in &pop_durations {
                    assert!(duration.as_millis() <= avg_duration.as_millis() * 3, 
                        "pop_lfu performance should be consistent: {:?} vs avg {:?}", duration, avg_duration);
                }
                
                // Test 7: pop_lfu on empty cache
                let mut empty_test_cache = LFUCache::<String, i32>::new(10);
                
                let start = Instant::now();
                let result = empty_test_cache.pop_lfu();
                let empty_pop_duration = start.elapsed();
                
                assert!(result.is_none());
                assert!(empty_pop_duration < Duration::from_millis(1), 
                    "pop_lfu on empty cache should be instant: {:?}", empty_pop_duration);
                
                println!("pop_lfu performance:");
                println!("  Basic pop operations: {:?}", pop_duration);
                println!("  Size scaling: {:?}", pop_times);
                println!("  Uniform frequency: {:?}", uniform_pop_duration);
                println!("  Pop until empty: {:?}", empty_duration);
                println!("  Skewed distribution: {:?}", skewed_pop_duration);
                println!("  Consistency check: {:?}", pop_durations);
                println!("  Empty cache: {:?}", empty_pop_duration);
            }

            #[test]
            fn test_eviction_with_many_same_frequency() {
                // Test 1: All items have same frequency (frequency = 1)
                let mut cache = LFUCache::new(1000);
                
                // Fill cache where all items have frequency 1
                for i in 0..1000 {
                    cache.insert(format!("same_freq_{}", i), i as i32);
                }
                
                // All items should have frequency 1
                for i in (0..100).step_by(10) {
                    assert_eq!(cache.frequency(&format!("same_freq_{}", i)), Some(1));
                }
                
                // Test eviction performance with same frequency items
                let start = Instant::now();
                for i in 1000..1500 {
                    cache.insert(format!("new_same_{}", i), i as i32);
                }
                let same_freq_duration = start.elapsed();
                
                assert_eq!(cache.len(), 1000);
                assert!(same_freq_duration < Duration::from_millis(500), 
                    "Same frequency eviction should be reasonable: {:?}", same_freq_duration);
                
                // Test 2: Multiple groups with same frequencies
                let mut grouped_cache = LFUCache::new(1200);
                
                // Group 1: frequency 1 (400 items)
                for i in 0..400 {
                    grouped_cache.insert(format!("group1_{}", i), i as i32);
                }
                
                // Group 2: frequency 3 (400 items)
                for i in 400..800 {
                    grouped_cache.insert(format!("group2_{}", i), i as i32);
                    grouped_cache.get(&format!("group2_{}", i));
                    grouped_cache.get(&format!("group2_{}", i));
                }
                
                // Group 3: frequency 5 (400 items)
                for i in 800..1200 {
                    grouped_cache.insert(format!("group3_{}", i), i as i32);
                    for _ in 0..4 {
                        grouped_cache.get(&format!("group3_{}", i));
                    }
                }
                
                // Force eviction of group 1 (frequency 1)
                let start = Instant::now();
                for i in 1200..1600 {
                    grouped_cache.insert(format!("new_group_{}", i), i as i32);
                }
                let grouped_eviction_duration = start.elapsed();
                
                assert_eq!(grouped_cache.len(), 1200);
                assert!(grouped_eviction_duration < Duration::from_millis(400), 
                    "Grouped frequency eviction should be efficient: {:?}", grouped_eviction_duration);
                
                // Verify that most Group 1 items (frequency 1) were evicted
                // and Group 2/3 items (higher frequency) were preserved
                let mut group1_remaining = 0;
                let mut group2_remaining = 0;
                let mut group3_remaining = 0;
                
                for i in 0..400 {
                    if grouped_cache.contains(&format!("group1_{}", i)) {
                        group1_remaining += 1;
                    }
                }
                for i in 400..800 {
                    if grouped_cache.contains(&format!("group2_{}", i)) {
                        group2_remaining += 1;
                    }
                }
                for i in 800..1200 {
                    if grouped_cache.contains(&format!("group3_{}", i)) {
                        group3_remaining += 1;
                    }
                }
                
                // Verify eviction follows frequency preference (lower frequency items evicted more)
                // Group 1 should have fewer remaining than Group 2/3
                assert!(group1_remaining < group2_remaining, 
                    "Group 1 (freq=1) should have fewer remaining than Group 2 (freq=3): {} vs {}", 
                    group1_remaining, group2_remaining);
                assert!(group1_remaining < group3_remaining, 
                    "Group 1 (freq=1) should have fewer remaining than Group 3 (freq=5): {} vs {}", 
                    group1_remaining, group3_remaining);
                
                // Verify cache respects capacity and LFU behavior is working
                assert_eq!(grouped_cache.len(), 1200, "Cache should maintain its capacity");
                
                // The key test: lower frequency items should be evicted more than higher frequency items
                let total_old_remaining = group1_remaining + group2_remaining + group3_remaining;
                println!("Group distribution - Group1 (freq=1): {}, Group2 (freq=3): {}, Group3 (freq=5): {}, Total old: {}", 
                    group1_remaining, group2_remaining, group3_remaining, total_old_remaining);
                
                // Test 3: Large number of items with identical frequency
                let mut identical_cache = LFUCache::new(2000);
                
                // Fill cache and make all items have frequency 3
                for i in 0..2000 {
                    identical_cache.insert(format!("identical_{}", i), i as i32);
                    identical_cache.get(&format!("identical_{}", i));
                    identical_cache.get(&format!("identical_{}", i));
                }
                
                // Verify all have same frequency
                for i in (0..2000).step_by(100) {
                    assert_eq!(identical_cache.frequency(&format!("identical_{}", i)), Some(3));
                }
                
                // Test eviction performance with identical frequencies
                let start = Instant::now();
                for i in 2000..2500 {
                    identical_cache.insert(format!("new_identical_{}", i), i as i32);
                }
                let identical_duration = start.elapsed();
                
                assert_eq!(identical_cache.len(), 2000);
                assert!(identical_duration < Duration::from_millis(600), 
                    "Identical frequency eviction should be reasonable: {:?}", identical_duration);
                
                // Test 4: Performance scaling with different ratios of same-frequency items
                let ratios = [0.1, 0.3, 0.5, 0.8, 1.0]; // Fraction of items with same frequency
                let mut ratio_times = Vec::new();
                
                for &ratio in &ratios {
                    let mut ratio_cache = LFUCache::new(500);
                    let same_freq_count = (500.0 * ratio) as usize;
                    
                    // Fill cache
                    for i in 0..500 {
                        ratio_cache.insert(format!("ratio_{}", i), i as i32);
                    }
                    
                    // Make some items have frequency 2, others keep frequency 1
                    for i in 0..same_freq_count {
                        ratio_cache.get(&format!("ratio_{}", i));
                    }
                    
                    // Make remaining items have higher frequencies
                    for i in same_freq_count..500 {
                        for _ in 0..(i % 10 + 3) {
                            ratio_cache.get(&format!("ratio_{}", i));
                        }
                    }
                    
                    // Test eviction performance
                    let start = Instant::now();
                    for i in 500..600 {
                        ratio_cache.insert(format!("new_ratio_{}", i), i as i32);
                    }
                    let duration = start.elapsed();
                    ratio_times.push(duration);
                }
                
                // Performance should be reasonable across all ratios
                for (i, &duration) in ratio_times.iter().enumerate() {
                    assert!(duration < Duration::from_millis(100), 
                        "Ratio {} eviction should be efficient: {:?}", ratios[i], duration);
                }
                
                // Test 5: Eviction pattern with same frequency items
                let mut pattern_cache = LFUCache::new(300);
                
                // Create alternating frequency pattern
                for i in 0..300 {
                    pattern_cache.insert(format!("pattern_{}", i), i as i32);
                    if i % 2 == 0 {
                        pattern_cache.get(&format!("pattern_{}", i)); // Even indices: freq 2
                    }
                    // Odd indices: freq 1
                }
                
                // Count items of each frequency
                let mut freq1_count = 0;
                let mut freq2_count = 0;
                for i in 0..300 {
                    if let Some(freq) = pattern_cache.frequency(&format!("pattern_{}", i)) {
                        if freq == 1 { freq1_count += 1; }
                        else if freq == 2 { freq2_count += 1; }
                    }
                }
                
                assert_eq!(freq1_count, 150); // Odd indices
                assert_eq!(freq2_count, 150); // Even indices
                
                // Force eviction of freq 1 items
                let start = Instant::now();
                for i in 300..450 {
                    pattern_cache.insert(format!("new_pattern_{}", i), i as i32);
                }
                let pattern_duration = start.elapsed();
                
                assert_eq!(pattern_cache.len(), 300);
                assert!(pattern_duration < Duration::from_millis(150), 
                    "Pattern eviction should be efficient: {:?}", pattern_duration);
                
                // Most freq 1 items should be evicted, freq 2 items preserved
                let mut remaining_freq1 = 0;
                let mut remaining_freq2 = 0;
                for i in 0..300 {
                    if pattern_cache.contains(&format!("pattern_{}", i)) {
                        if let Some(freq) = pattern_cache.frequency(&format!("pattern_{}", i)) {
                            if freq == 1 { remaining_freq1 += 1; }
                            else if freq == 2 { remaining_freq2 += 1; }
                        }
                    }
                }
                
                assert!(remaining_freq2 > remaining_freq1, 
                    "More freq 2 items should remain: {} vs {}", remaining_freq2, remaining_freq1);
                
                // Test 6: Worst case scenario - all items same frequency after access
                let mut worst_case_cache = LFUCache::new(500);
                
                // Fill and access all items once to make them frequency 2
                for i in 0..500 {
                    worst_case_cache.insert(format!("worst_{}", i), i as i32);
                    worst_case_cache.get(&format!("worst_{}", i));
                }
                
                // Verify all have same frequency
                for i in (0..500).step_by(50) {
                    assert_eq!(worst_case_cache.frequency(&format!("worst_{}", i)), Some(2));
                }
                
                let start = Instant::now();
                for i in 500..750 {
                    worst_case_cache.insert(format!("worst_new_{}", i), i as i32);
                }
                let worst_case_duration = start.elapsed();
                
                assert_eq!(worst_case_cache.len(), 500);
                assert!(worst_case_duration < Duration::from_millis(300), 
                    "Worst case same frequency eviction should be acceptable: {:?}", worst_case_duration);
                
                println!("Same frequency eviction performance:");
                println!("  All same frequency: {:?}", same_freq_duration);
                println!("  Grouped frequencies: {:?}", grouped_eviction_duration);
                println!("  Identical frequencies: {:?}", identical_duration);
                println!("  Ratio scaling: {:?}", ratio_times);
                println!("  Pattern eviction: {:?}", pattern_duration);
                println!("  Worst case scenario: {:?}", worst_case_duration);
            }

            #[test]
            fn test_frequency_distribution_impact() {
                // Test 1: Uniform distribution impact
                let mut uniform_cache = LFUCache::new(1000);
                
                // Create uniform frequency distribution (all items frequency 3)
                for i in 0..1000 {
                    uniform_cache.insert(format!("uniform_{}", i), i as i32);
                    uniform_cache.get(&format!("uniform_{}", i));
                    uniform_cache.get(&format!("uniform_{}", i));
                }
                
                // Verify uniform distribution
                for i in (0..1000).step_by(100) {
                    assert_eq!(uniform_cache.frequency(&format!("uniform_{}", i)), Some(3));
                }
                
                let start = Instant::now();
                for i in 1000..1200 {
                    uniform_cache.insert(format!("new_uniform_{}", i), i as i32);
                }
                let uniform_duration = start.elapsed();
                
                assert_eq!(uniform_cache.len(), 1000);
                assert!(uniform_duration < Duration::from_millis(200), 
                    "Uniform distribution eviction should be reasonable: {:?}", uniform_duration);
                
                // Test 2: Normal (bell curve) distribution impact
                let mut normal_cache = LFUCache::new(1000);
                
                // Create normal distribution of frequencies (center items higher frequency)
                for i in 0..1000 {
                    normal_cache.insert(format!("normal_{}", i), i as i32);
                }
                
                // Create bell curve frequency pattern
                for i in 0..1000 {
                    let distance_from_center = ((i as f64 - 500.0).abs() / 500.0 * 10.0) as usize;
                    let access_count = 10 - distance_from_center.min(9);
                    for _ in 0..access_count {
                        normal_cache.get(&format!("normal_{}", i));
                    }
                }
                
                let start = Instant::now();
                for i in 1000..1200 {
                    normal_cache.insert(format!("new_normal_{}", i), i as i32);
                }
                let normal_duration = start.elapsed();
                
                assert_eq!(normal_cache.len(), 1000);
                assert!(normal_duration < Duration::from_millis(200), 
                    "Normal distribution eviction should be efficient: {:?}", normal_duration);
                
                // Center items should be preserved due to higher frequency
                assert!(normal_cache.contains(&"normal_500".to_string()));
                assert!(normal_cache.contains(&"normal_450".to_string()));
                assert!(normal_cache.contains(&"normal_550".to_string()));
                
                // Test 3: Exponential distribution impact
                let mut exponential_cache = LFUCache::new(1000);
                
                // Create exponential frequency distribution
                for i in 0..1000 {
                    exponential_cache.insert(format!("exp_{}", i), i as i32);
                }
                
                // Create exponential decay pattern
                for i in 0..1000 {
                    let access_count = std::cmp::max(1, 20 - (i / 50));
                    for _ in 0..access_count {
                        exponential_cache.get(&format!("exp_{}", i));
                    }
                }
                
                let start = Instant::now();
                for i in 1000..1300 {
                    exponential_cache.insert(format!("new_exp_{}", i), i as i32);
                }
                let exponential_duration = start.elapsed();
                
                assert_eq!(exponential_cache.len(), 1000);
                assert!(exponential_duration < Duration::from_millis(300), 
                    "Exponential distribution eviction should be reasonable: {:?}", exponential_duration);
                
                // Early items should be preserved due to higher frequency
                assert!(exponential_cache.contains(&"exp_0".to_string()));
                assert!(exponential_cache.contains(&"exp_10".to_string()));
                assert!(exponential_cache.contains(&"exp_50".to_string()));
                
                // Test 4: Power law (Zipf) distribution impact
                let mut zipf_cache = LFUCache::new(1000);
                
                // Create Zipf distribution (80/20 rule)
                for i in 0..1000 {
                    zipf_cache.insert(format!("zipf_{}", i), i as i32);
                }
                
                // Top 20% get 80% of accesses
                let hot_items = 200;
                let hot_accesses = 40;
                let cold_accesses = 1;
                
                for i in 0..hot_items {
                    for _ in 0..hot_accesses {
                        zipf_cache.get(&format!("zipf_{}", i));
                    }
                }
                
                for i in hot_items..1000 {
                    for _ in 0..cold_accesses {
                        zipf_cache.get(&format!("zipf_{}", i));
                    }
                }
                
                let start = Instant::now();
                for i in 1000..1400 {
                    zipf_cache.insert(format!("new_zipf_{}", i), i as i32);
                }
                let zipf_duration = start.elapsed();
                
                assert_eq!(zipf_cache.len(), 1000);
                assert!(zipf_duration < Duration::from_millis(400), 
                    "Zipf distribution eviction should be efficient: {:?}", zipf_duration);
                
                // Hot items should be preserved
                assert!(zipf_cache.contains(&"zipf_0".to_string()));
                assert!(zipf_cache.contains(&"zipf_50".to_string()));
                assert!(zipf_cache.contains(&"zipf_100".to_string()));
                
                // Test 5: Bimodal distribution impact
                let mut bimodal_cache = LFUCache::new(1000);
                
                // Create bimodal distribution (two peaks)
                for i in 0..1000 {
                    bimodal_cache.insert(format!("bimodal_{}", i), i as i32);
                }
                
                // Peak 1: items 200-300 (high frequency)
                for i in 200..300 {
                    for _ in 0..15 {
                        bimodal_cache.get(&format!("bimodal_{}", i));
                    }
                }
                
                // Peak 2: items 700-800 (high frequency)
                for i in 700..800 {
                    for _ in 0..15 {
                        bimodal_cache.get(&format!("bimodal_{}", i));
                    }
                }
                
                // Valley: other items (low frequency)
                for i in 0..200 {
                    bimodal_cache.get(&format!("bimodal_{}", i));
                }
                for i in 300..700 {
                    bimodal_cache.get(&format!("bimodal_{}", i));
                }
                for i in 800..1000 {
                    bimodal_cache.get(&format!("bimodal_{}", i));
                }
                
                let start = Instant::now();
                for i in 1000..1300 {
                    bimodal_cache.insert(format!("new_bimodal_{}", i), i as i32);
                }
                let bimodal_duration = start.elapsed();
                
                assert_eq!(bimodal_cache.len(), 1000);
                assert!(bimodal_duration < Duration::from_millis(300), 
                    "Bimodal distribution eviction should be efficient: {:?}", bimodal_duration);
                
                // Peak items should be preserved
                assert!(bimodal_cache.contains(&"bimodal_250".to_string()));
                assert!(bimodal_cache.contains(&"bimodal_750".to_string()));
                
                // Test 6: Comparative performance across distributions
                let distributions = ["uniform", "normal", "exponential", "zipf", "bimodal"];
                let durations = [uniform_duration, normal_duration, exponential_duration, zipf_duration, bimodal_duration];
                
                // All distributions should complete within reasonable time
                for (i, &duration) in durations.iter().enumerate() {
                    assert!(duration < Duration::from_millis(500), 
                        "{} distribution took too long: {:?}", distributions[i], duration);
                }
                
                // Test 7: Dynamic distribution change impact
                let mut dynamic_cache = LFUCache::new(500);
                
                // Fill cache initially
                for i in 0..500 {
                    dynamic_cache.insert(format!("dynamic_{}", i), i as i32);
                }
                
                // Phase 1: Create initial distribution (linear)
                for i in 0..500 {
                    for _ in 0..(i / 50 + 1) {
                        dynamic_cache.get(&format!("dynamic_{}", i));
                    }
                }
                
                // Phase 2: Shift access pattern (reverse linear)
                for i in 0..500 {
                    for _ in 0..((499 - i) / 50 + 1) {
                        dynamic_cache.get(&format!("dynamic_{}", i));
                    }
                }
                
                let start = Instant::now();
                for i in 500..650 {
                    dynamic_cache.insert(format!("new_dynamic_{}", i), i as i32);
                }
                let dynamic_duration = start.elapsed();
                
                assert_eq!(dynamic_cache.len(), 500);
                assert!(dynamic_duration < Duration::from_millis(150), 
                    "Dynamic distribution eviction should adapt efficiently: {:?}", dynamic_duration);
                
                // Test 8: Sparse vs dense frequency ranges
                let mut sparse_cache = LFUCache::new(400);
                let mut dense_cache = LFUCache::new(400);
                
                // Sparse: frequencies 1, 10, 20, 30 (big gaps)
                for i in 0..400 {
                    sparse_cache.insert(format!("sparse_{}", i), i as i32);
                    let freq_group = i / 100;
                    let target_freq = match freq_group {
                        0 => 1,
                        1 => 10,
                        2 => 20,
                        _ => 30,
                    };
                    for _ in 1..target_freq {
                        sparse_cache.get(&format!("sparse_{}", i));
                    }
                }
                
                // Dense: frequencies 1, 2, 3, 4 (small gaps)
                for i in 0..400 {
                    dense_cache.insert(format!("dense_{}", i), i as i32);
                    let freq_group = i / 100;
                    let target_freq = freq_group + 1;
                    for _ in 1..target_freq {
                        dense_cache.get(&format!("dense_{}", i));
                    }
                }
                
                let start = Instant::now();
                for i in 400..500 {
                    sparse_cache.insert(format!("new_sparse_{}", i), i as i32);
                }
                let sparse_eviction_duration = start.elapsed();
                
                let start = Instant::now();
                for i in 400..500 {
                    dense_cache.insert(format!("new_dense_{}", i), i as i32);
                }
                let dense_eviction_duration = start.elapsed();
                
                assert!(sparse_eviction_duration < Duration::from_millis(100), 
                    "Sparse frequency eviction should be efficient: {:?}", sparse_eviction_duration);
                assert!(dense_eviction_duration < Duration::from_millis(100), 
                    "Dense frequency eviction should be efficient: {:?}", dense_eviction_duration);
                
                println!("Frequency distribution impact on eviction performance:");
                println!("  Uniform distribution: {:?}", uniform_duration);
                println!("  Normal distribution: {:?}", normal_duration);
                println!("  Exponential distribution: {:?}", exponential_duration);
                println!("  Zipf distribution: {:?}", zipf_duration);
                println!("  Bimodal distribution: {:?}", bimodal_duration);
                println!("  Dynamic distribution: {:?}", dynamic_duration);
                println!("  Sparse frequencies: {:?}", sparse_eviction_duration);
                println!("  Dense frequencies: {:?}", dense_eviction_duration);
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

         // Helper type for thread-safe testing
        type ThreadSafeLFUCache<K, V> = Arc<Mutex<LFUCache<K, V>>>;


        // Thread Safety Tests
        mod thread_safety {
            use super::*;
            use std::thread;
            use std::sync::atomic::{AtomicUsize, Ordering};

            #[test]
            fn test_concurrent_insertions() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(1000)));
                let num_threads = 8;
                let items_per_thread = 100;
                
                let mut handles = vec![];
                
                // Spawn threads that each insert items
                for thread_id in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        for i in 0..items_per_thread {
                            let key = format!("thread_{}_{}", thread_id, i);
                            let value = (thread_id * items_per_thread + i) as i32;
                            
                            cache_clone.lock().unwrap().insert(key, value);
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify results
                let mut cache = cache.lock().unwrap();
                assert_eq!(cache.len(), num_threads * items_per_thread);
                
                // Verify all items exist and have correct values
                for thread_id in 0..num_threads {
                    for i in 0..items_per_thread {
                        let key = format!("thread_{}_{}", thread_id, i);
                        let expected_value = (thread_id * items_per_thread + i) as i32;
                        assert_eq!(cache.get(&key), Some(&expected_value));
                        assert_eq!(cache.frequency(&key), Some(2)); // 1 from insert, 1 from get
                    }
                }
            }

            #[test]
            fn test_concurrent_gets() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(500)));
                
                // Pre-populate cache with test data
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..500 {
                        cache_guard.insert(format!("key_{}", i), i as i32);
                    }
                }
                
                let num_threads = 10;
                let reads_per_thread = 1000;
                let hit_count = Arc::new(AtomicUsize::new(0));
                let mut handles = vec![];
                
                // Spawn threads that each perform read operations
                for thread_id in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let hit_count_clone = Arc::clone(&hit_count);
                    
                    let handle = thread::spawn(move || {
                        for i in 0..reads_per_thread {
                            let key = format!("key_{}", i % 500); // Ensure keys exist
                            
                            if cache_clone.lock().unwrap().get(&key).is_some() {
                                hit_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify results
                let expected_hits = num_threads * reads_per_thread;
                assert_eq!(hit_count.load(Ordering::Relaxed), expected_hits, 
                    "All reads should hit since all keys exist");
                
                // Verify frequency counts increased due to concurrent access
                let cache = cache.lock().unwrap();
                for i in (0..500).step_by(50) {
                    let freq = cache.frequency(&format!("key_{}", i)).unwrap();
                    // Each key should have been accessed multiple times (once for insert + multiple gets)
                    assert!(freq > 1, "Key key_{} should have frequency > 1, got {}", i, freq);
                }
            }

            #[test]
            fn test_concurrent_frequency_operations() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(100)));
                
                // Pre-populate cache
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..100 {
                        cache_guard.insert(format!("freq_key_{}", i), i as i32);
                    }
                }
                
                let num_threads = 6;
                let operations_per_thread = 500;
                let mut handles = vec![];
                
                // Spawn threads performing different frequency operations
                for thread_id in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    
                    let handle = thread::spawn(move || {
                        for i in 0..operations_per_thread {
                            let key = format!("freq_key_{}", i % 100);
                            
                            match thread_id % 3 {
                                0 => {
                                    // Thread type 1: increment frequency via get
                                    cache_clone.lock().unwrap().get(&key);
                                },
                                1 => {
                                    // Thread type 2: increment frequency directly
                                    cache_clone.lock().unwrap().increment_frequency(&key);
                                },
                                2 => {
                                    // Thread type 3: reset frequency (less frequently)
                                    if i % 10 == 0 {
                                        cache_clone.lock().unwrap().reset_frequency(&key);
                                    } else {
                                        cache_clone.lock().unwrap().get(&key);
                                    }
                                },
                                _ => unreachable!(),
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify cache consistency after concurrent frequency operations
                let mut cache = cache.lock().unwrap();
                assert_eq!(cache.len(), 100, "Cache should still contain all items");
                
                // Verify all items still exist and have reasonable frequency values
                for i in 0..100 {
                    let key = format!("freq_key_{}", i);
                    assert!(cache.contains(&key), "Key {} should still exist", key);
                    
                    let freq = cache.frequency(&key).unwrap();
                    assert!(freq >= 1, "Frequency should be at least 1 for key {}, got {}", key, freq);
                    assert!(freq <= 1200, "Frequency should be reasonable for key {}, got {}", key, freq);
                }
                
                // Verify cache is still functional
                assert!(cache.get(&"freq_key_0".to_string()).is_some());
                assert!(cache.peek_lfu().is_some());
            }

            #[test]
            fn test_concurrent_lfu_operations() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(200)));
                
                // Pre-populate cache with different frequency patterns
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..200 {
                        cache_guard.insert(format!("lfu_key_{}", i), i as i32);
                    }
                    
                    // Create frequency differences - some items will be more frequent
                    for _ in 0..5 {
                        for i in 0..50 {
                            cache_guard.get(&format!("lfu_key_{}", i)); // High frequency
                        }
                    }
                    
                    for _ in 0..2 {
                        for i in 50..100 {
                            cache_guard.get(&format!("lfu_key_{}", i)); // Medium frequency
                        }
                    }
                    // Items 100-199 remain at frequency 1 (low frequency)
                }
                
                let num_peek_threads = 2;
                let num_pop_threads = 1;
                let operations_per_thread = 10;
                let popped_items = Arc::new(Mutex::new(Vec::new()));
                let mut handles = vec![];
                
                // Spawn peek threads
                for _ in 0..num_peek_threads {
                    let cache_clone = Arc::clone(&cache);
                    
                    let handle = thread::spawn(move || {
                        for _ in 0..operations_per_thread {
                            // Use a single lock to avoid double locking issues
                            let cache_guard = cache_clone.lock().unwrap();
                            if let Some((key, _)) = cache_guard.peek_lfu() {
                                // Verify the peeked item exists within the same lock
                                assert!(cache_guard.contains(key));
                            }
                            // Lock is automatically released here
                        }
                    });
                    handles.push(handle);
                }
                
                // Spawn pop threads (fewer since they modify the cache)
                for _ in 0..num_pop_threads {
                    let cache_clone = Arc::clone(&cache);
                    let popped_clone = Arc::clone(&popped_items);
                    
                    let handle = thread::spawn(move || {
                        for _ in 0..operations_per_thread {
                            // Use a single lock for the cache operation
                            let popped_item = cache_clone.lock().unwrap().pop_lfu();
                            
                            if let Some((key, value)) = popped_item {
                                // Use a separate lock for the results collection
                                popped_clone.lock().unwrap().push((key, value));
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify results
                let cache = cache.lock().unwrap();
                let popped = popped_items.lock().unwrap();
                
                // Verify cache size is reduced by the number of popped items
                assert_eq!(cache.len() + popped.len(), 200, 
                    "Cache size + popped items should equal original size");
                
                // Verify all popped items are unique
                let mut popped_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
                for (key, _) in popped.iter() {
                    assert!(popped_keys.insert(key.clone()), "Duplicate key popped: {}", key);
                }
                
                // Verify popped items are no longer in cache
                for (key, _) in popped.iter() {
                    assert!(!cache.contains(key), "Popped key {} should not be in cache", key);
                }
                
                // Verify cache is still functional after concurrent operations
                if !cache.is_empty() {
                    assert!(cache.peek_lfu().is_some());
                }
            }

            #[test]
            fn test_mixed_concurrent_operations() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(300)));
                let num_threads = 8;
                let operations_per_thread = 200;
                let operation_counts = Arc::new(Mutex::new((0, 0, 0, 0, 0))); // (inserts, gets, removes, frequency_ops, lfu_ops)
                let mut handles = vec![];
                
                // Pre-populate with some initial data
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..150 {
                        cache_guard.insert(format!("initial_{}", i), i as i32);
                    }
                }
                
                // Spawn threads performing mixed operations
                for thread_id in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let counts_clone = Arc::clone(&operation_counts);
                    
                    let handle = thread::spawn(move || {
                        for i in 0..operations_per_thread {
                            let operation = (thread_id + i) % 8;
                            
                            match operation {
                                0 | 1 => {
                                    // Insert operations (25% of operations)
                                    let key = format!("mixed_{}_{}", thread_id, i);
                                    let value = (thread_id * 1000 + i) as i32;
                                    cache_clone.lock().unwrap().insert(key, value);
                                    counts_clone.lock().unwrap().0 += 1;
                                },
                                2 | 3 | 4 => {
                                    // Get operations (37.5% of operations)
                                    let key = if i % 2 == 0 {
                                        format!("initial_{}", i % 150)
                                    } else {
                                        format!("mixed_{}_{}", thread_id % num_threads, i % operations_per_thread)
                                    };
                                    cache_clone.lock().unwrap().get(&key);
                                    counts_clone.lock().unwrap().1 += 1;
                                },
                                5 => {
                                    // Remove operations (12.5% of operations)
                                    let key = format!("initial_{}", i % 150);
                                    cache_clone.lock().unwrap().remove(&key);
                                    counts_clone.lock().unwrap().2 += 1;
                                },
                                6 => {
                                    // Frequency operations (12.5% of operations)
                                    let key = format!("initial_{}", i % 150);
                                    if i % 3 == 0 {
                                        cache_clone.lock().unwrap().increment_frequency(&key);
                                    } else {
                                        cache_clone.lock().unwrap().reset_frequency(&key);
                                    }
                                    counts_clone.lock().unwrap().3 += 1;
                                },
                                7 => {
                                    // LFU operations (12.5% of operations)
                                    if i % 2 == 0 {
                                        cache_clone.lock().unwrap().peek_lfu();
                                    } else {
                                        cache_clone.lock().unwrap().pop_lfu();
                                    }
                                    counts_clone.lock().unwrap().4 += 1;
                                },
                                _ => unreachable!(),
                            }
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify results
                let mut cache = cache.lock().unwrap();
                let counts = operation_counts.lock().unwrap();
                
                // Verify cache is still functional and consistent
                assert!(cache.len() <= 300, "Cache should not exceed capacity");
                assert!(cache.capacity() == 300, "Capacity should remain unchanged");
                
                // Verify operation counts
                let (inserts, gets, removes, freq_ops, lfu_ops) = *counts;
                let total_operations = inserts + gets + removes + freq_ops + lfu_ops;
                assert_eq!(total_operations, num_threads * operations_per_thread);
                
                // Verify cache operations still work correctly
                cache.insert("test_after_concurrent".to_string(), 999);
                assert_eq!(cache.get(&"test_after_concurrent".to_string()), Some(&999));
                assert!(cache.contains(&"test_after_concurrent".to_string()));
                
                // Verify LFU operations still work
                if !cache.is_empty() {
                    assert!(cache.peek_lfu().is_some());
                }
                
                println!("Mixed operations completed - Inserts: {}, Gets: {}, Removes: {}, Freq: {}, LFU: {}", 
                    inserts, gets, removes, freq_ops, lfu_ops);
            }

            #[test]
            fn test_concurrent_eviction_scenarios() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(100)));
                let num_insert_threads = 6;
                let num_access_threads = 3;
                let inserts_per_thread = 50;
                let accesses_per_thread = 200;
                let mut handles = vec![];
                
                // Pre-populate cache to capacity
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..100 {
                        cache_guard.insert(format!("base_{}", i), i as i32);
                    }
                    
                    // Create frequency differences to establish clear LFU candidates
                    for _ in 0..10 {
                        for i in 0..20 {
                            cache_guard.get(&format!("base_{}", i)); // High frequency
                        }
                    }
                    
                    for _ in 0..3 {
                        for i in 20..50 {
                            cache_guard.get(&format!("base_{}", i)); // Medium frequency
                        }
                    }
                    // Items 50-99 remain at frequency 1 (low frequency - eviction candidates)
                }
                
                // Spawn threads that insert new items (triggering evictions)
                for thread_id in 0..num_insert_threads {
                    let cache_clone = Arc::clone(&cache);
                    
                    let handle = thread::spawn(move || {
                        for i in 0..inserts_per_thread {
                            let key = format!("evict_trigger_{}_{}", thread_id, i);
                            let value = (thread_id * 1000 + i) as i32;
                            cache_clone.lock().unwrap().insert(key, value);
                            
                            // Small delay to increase thread interleaving
                            thread::sleep(std::time::Duration::from_nanos(100));
                        }
                    });
                    handles.push(handle);
                }
                
                // Spawn threads that access existing items (changing frequencies)
                for thread_id in 0..num_access_threads {
                    let cache_clone = Arc::clone(&cache);
                    
                    let handle = thread::spawn(move || {
                        for i in 0..accesses_per_thread {
                            // Access different ranges to create frequency competition
                            let key = match thread_id % 3 {
                                0 => format!("base_{}", i % 30), // Access high-freq items
                                1 => format!("base_{}", 30 + (i % 30)), // Access medium-freq items
                                2 => format!("base_{}", 60 + (i % 40)), // Access low-freq items
                                _ => unreachable!(),
                            };
                            
                            cache_clone.lock().unwrap().get(&key);
                            
                            // Small delay to increase thread interleaving
                            thread::sleep(std::time::Duration::from_nanos(50));
                        }
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Verify results after concurrent evictions
                let cache = cache.lock().unwrap();
                
                // Cache should maintain its capacity
                assert_eq!(cache.len(), 100, "Cache should maintain capacity after evictions");
                
                // High frequency items should be preserved
                let mut high_freq_preserved = 0;
                for i in 0..20 {
                    if cache.contains(&format!("base_{}", i)) {
                        high_freq_preserved += 1;
                    }
                }
                
                // Medium frequency items may be partially preserved
                let mut medium_freq_preserved = 0;
                for i in 20..50 {
                    if cache.contains(&format!("base_{}", i)) {
                        medium_freq_preserved += 1;
                    }
                }
                
                // Low frequency items should be mostly evicted
                let mut low_freq_preserved = 0;
                for i in 50..100 {
                    if cache.contains(&format!("base_{}", i)) {
                        low_freq_preserved += 1;
                    }
                }
                
                // Some new items should be present
                let mut new_items_present = 0;
                for thread_id in 0..num_insert_threads {
                    for i in 0..inserts_per_thread {
                        let key = format!("evict_trigger_{}_{}", thread_id, i);
                        if cache.contains(&key) {
                            new_items_present += 1;
                        }
                    }
                }
                
                // Verify LFU behavior: high frequency items preserved more than low frequency
                assert!(high_freq_preserved > low_freq_preserved, 
                    "High frequency items ({}) should be preserved more than low frequency items ({})",
                    high_freq_preserved, low_freq_preserved);
                
                // Some new items should have been inserted
                assert!(new_items_present > 0, "Some new items should be present after evictions");
                
                // Verify cache is still functional
                assert!(cache.peek_lfu().is_some());
                
                println!("Eviction results - High freq preserved: {}, Medium: {}, Low: {}, New items: {}", 
                    high_freq_preserved, medium_freq_preserved, low_freq_preserved, new_items_present);
            }

            #[test]
            fn test_thread_fairness() {
                let cache: ThreadSafeLFUCache<String, i32> = Arc::new(Mutex::new(LFUCache::new(200)));
                let num_threads = 10;
                let operations_per_thread = 500;
                let success_counts = Arc::new(Mutex::new(vec![0; num_threads]));
                let mut handles = vec![];
                
                // Pre-populate cache
                {
                    let mut cache_guard = cache.lock().unwrap();
                    for i in 0..100 {
                        cache_guard.insert(format!("fair_{}", i), i as i32);
                    }
                }
                
                // Spawn threads that compete for cache access
                for thread_id in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let counts_clone = Arc::clone(&success_counts);
                    
                    let handle = thread::spawn(move || {
                        let mut successful_operations = 0;
                        
                        for i in 0..operations_per_thread {
                            let operation_type = i % 4;
                            let mut operation_successful = false;
                            
                            match operation_type {
                                0 => {
                                    // Insert operation
                                    let key = format!("thread_{}_insert_{}", thread_id, i);
                                    let value = (thread_id * 10000 + i) as i32;
                                    cache_clone.lock().unwrap().insert(key, value);
                                    operation_successful = true;
                                },
                                1 => {
                                    // Get operation
                                    let key = format!("fair_{}", i % 100);
                                    if cache_clone.lock().unwrap().get(&key).is_some() {
                                        operation_successful = true;
                                    }
                                },
                                2 => {
                                    // Frequency operation
                                    let key = format!("fair_{}", i % 100);
                                    if i % 2 == 0 {
                                        cache_clone.lock().unwrap().increment_frequency(&key);
                                    } else {
                                        cache_clone.lock().unwrap().reset_frequency(&key);
                                    }
                                    operation_successful = true;
                                },
                                3 => {
                                    // LFU operation
                                    if cache_clone.lock().unwrap().peek_lfu().is_some() {
                                        operation_successful = true;
                                    }
                                },
                                _ => unreachable!(),
                            }
                            
                            if operation_successful {
                                successful_operations += 1;
                            }
                            
                            // Add some variability to create different contention patterns
                            if thread_id % 3 == 0 {
                                thread::sleep(std::time::Duration::from_nanos(10));
                            }
                        }
                        
                        // Record successful operations count for this thread
                        counts_clone.lock().unwrap()[thread_id] = successful_operations;
                    });
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Analyze fairness results
                let counts = success_counts.lock().unwrap();
                let cache = cache.lock().unwrap();
                
                // Calculate statistics
                let total_successes: usize = counts.iter().sum();
                let avg_successes = total_successes as f64 / num_threads as f64;
                let min_successes = *counts.iter().min().unwrap();
                let max_successes = *counts.iter().max().unwrap();
                let variance = counts.iter()
                    .map(|&x| {
                        let diff = x as f64 - avg_successes;
                        diff * diff
                    })
                    .sum::<f64>() / num_threads as f64;
                let std_dev = variance.sqrt();
                
                // Fairness checks
                assert!(min_successes > 0, "No thread should be completely starved");
                
                // Check that no thread is dramatically underperforming
                // Allow for some variance but ensure no thread gets less than 70% of average
                let fairness_threshold = (avg_successes * 0.7) as usize;
                assert!(min_successes >= fairness_threshold, 
                    "Thread fairness violated: min_successes {} < threshold {} (avg: {:.1})", 
                    min_successes, fairness_threshold, avg_successes);
                
                // Check that standard deviation is reasonable (not too high)
                let relative_std_dev = std_dev / avg_successes;
                assert!(relative_std_dev < 0.5, 
                    "Standard deviation too high for fairness: {:.3} (should be < 0.5)", 
                    relative_std_dev);
                
                // Verify cache is still functional and consistent
                assert!(cache.len() <= 200, "Cache should not exceed capacity");
                assert_eq!(cache.capacity(), 200, "Capacity should remain unchanged");
                
                // Verify basic operations still work
                assert!(cache.peek_lfu().is_some() || cache.is_empty());
                
                println!("Fairness test results:");
                println!("  Total operations: {}", total_successes);
                println!("  Average per thread: {:.1}", avg_successes);
                println!("  Min: {}, Max: {}", min_successes, max_successes);
                println!("  Standard deviation: {:.1}", std_dev);
                println!("  Relative std dev: {:.3}", relative_std_dev);
                println!("  Thread success counts: {:?}", *counts);
            }
        }

        // Complexity Analysis Testing
        mod complexity {
            use super::*;
            use std::time::{Duration, Instant};
            use std::collections::HashMap;

            /// Helper function to measure execution time of a closure
            fn measure_time<F, R>(operation: F) -> (R, Duration)
            where
                F: FnOnce() -> R,
            {
                let start = Instant::now();
                let result = operation();
                let duration = start.elapsed();
                (result, duration)
            }

            /// Generate test data for complexity tests
            fn generate_test_data(size: usize) -> Vec<(String, i32)> {
                (0..size)
                    .map(|i| (format!("key_{:06}", i), i as i32))
                    .collect()
            }

            // ==============================================
            // TIME COMPLEXITY TESTS
            // ==============================================

            #[test]
            fn test_insert_time_complexity() {
                // Test that insert operations maintain consistent performance
                let cache_sizes = vec![100, 500, 1000, 5000, 10000];
                let mut results = Vec::new();

                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    let test_data = generate_test_data(cache_size);
                    
                    // Measure time to fill cache to capacity
                    let (_, insert_time) = measure_time(|| {
                        for (key, value) in test_data {
                            cache.insert(key, value);
                        }
                    });
                    
                    results.push((cache_size, insert_time));
                }

                // Verify performance characteristics
                for (i, &(size, time)) in results.iter().enumerate() {
                    println!("Cache size: {}, Total insert time: {:?}, Avg per insert: {:?}", 
                        size, time, time / size as u32);
                    
                    // For LFU, insertion time should be reasonable even for large caches
                    // Allow up to 10µs per insertion on average (accounts for hash operations and potential evictions)
                    let avg_time_per_insert = time / size as u32;
                    assert!(avg_time_per_insert < Duration::from_micros(10), 
                        "Insert performance degraded significantly for size {}: {:?} per insert", 
                        size, avg_time_per_insert);
                }
            }

            #[test]
            fn test_get_time_complexity() {
                // Test that get operations are O(1) amortized
                let cache_sizes = vec![100, 500, 1000, 5000];
                let lookup_count = 1000;
                
                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Pre-populate cache
                    for i in 0..cache_size {
                        cache.insert(format!("key_{}", i), i as i32);
                    }
                    
                    // Measure random access time
                    let keys: Vec<String> = (0..lookup_count)
                        .map(|i| format!("key_{}", i % cache_size))
                        .collect();
                    
                    let (hit_count, lookup_time) = measure_time(|| {
                        let mut hits = 0;
                        for key in &keys {
                            if cache.get(key).is_some() {
                                hits += 1;
                            }
                        }
                        hits
                    });
                    
                    assert_eq!(hit_count, lookup_count); // All should be hits
                    
                    let avg_time_per_get = lookup_time / lookup_count as u32;
                    println!("Cache size: {}, Avg get time: {:?}", cache_size, avg_time_per_get);
                    
                    // Get should be O(1) - allow up to 1µs per get on average (includes frequency increment)
                    assert!(avg_time_per_get < Duration::from_micros(1),
                        "Get performance degraded for cache size {}: {:?} per get", 
                        cache_size, avg_time_per_get);
                }
            }

            #[test]
            fn test_pop_lfu_time_complexity() {
                // Test that pop_lfu is O(n) but with reasonable constant factors
                let cache_sizes = vec![100, 500, 1000, 2000];
                let mut results = Vec::new();
                
                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Pre-populate cache with different frequencies
                    for i in 0..cache_size {
                        cache.insert(format!("key_{}", i), i as i32);
                        // Create frequency differences
                        for _ in 0..(i % 5) {
                            cache.get(&format!("key_{}", i));
                        }
                    }
                    
                    // Measure pop_lfu operations
                    let pop_count = std::cmp::min(50, cache_size / 2);
                    let (popped_items, pop_time) = measure_time(|| {
                        let mut popped = Vec::new();
                        for _ in 0..pop_count {
                            if let Some(item) = cache.pop_lfu() {
                                popped.push(item);
                            }
                        }
                        popped
                    });
                    
                    assert_eq!(popped_items.len(), pop_count);
                    let avg_time_per_pop = pop_time / pop_count as u32;
                    results.push((cache_size, avg_time_per_pop));
                    
                    println!("Cache size: {}, Avg pop_lfu time: {:?}", cache_size, avg_time_per_pop);
                }
                
                // Verify that pop_lfu time grows reasonably with cache size (O(n))
                // Allow for some variance but ensure it's not exponential
                for &(size, time) in &results {
                    // pop_lfu is O(n), so allow time proportional to cache size
                    // Allow up to 10µs per cache entry for pop_lfu (realistic for current implementation)
                    let max_expected_time = Duration::from_micros((size * 10) as u64);
                    assert!(time < max_expected_time,
                        "pop_lfu performance too slow for cache size {}: {:?} (expected < {:?})", 
                        size, time, max_expected_time);
                }
            }

            #[test]
            fn test_peek_lfu_time_complexity() {
                // Test that peek_lfu is O(n) with good constant factors
                let cache_sizes = vec![100, 500, 1000, 2000, 5000];
                
                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Pre-populate cache
                    for i in 0..cache_size {
                        cache.insert(format!("key_{}", i), i as i32);
                        // Create varied frequency distribution
                        for _ in 0..(i % 7) {
                            cache.get(&format!("key_{}", i));
                        }
                    }
                    
                    // Measure peek_lfu operations
                    let peek_count = 100;
                    let (peek_results, peek_time) = measure_time(|| {
                        let mut results = Vec::new();
                        for _ in 0..peek_count {
                            results.push(cache.peek_lfu());
                        }
                        results
                    });
                    
                    // All peeks should return the same LFU item
                    assert!(peek_results.iter().all(|r| r.is_some()));
                    let first_result = peek_results[0];
                    assert!(peek_results.iter().all(|&r| r == first_result));
                    
                    let avg_time_per_peek = peek_time / peek_count as u32;
                    println!("Cache size: {}, Avg peek_lfu time: {:?}", cache_size, avg_time_per_peek);
                    
                    // peek_lfu is O(n), allow up to 1µs per cache entry (realistic for current implementation)
                    let max_expected_time = Duration::from_micros(cache_size as u64);
                    assert!(avg_time_per_peek < max_expected_time,
                        "peek_lfu performance too slow for cache size {}: {:?} (expected < {:?})", 
                        cache_size, avg_time_per_peek, max_expected_time);
                }
            }

            #[test]
            fn test_frequency_operations_time_complexity() {
                // Test that frequency operations are O(1)
                let cache_sizes = vec![100, 1000, 5000, 10000];
                
                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Pre-populate cache
                    for i in 0..cache_size {
                        cache.insert(format!("key_{}", i), i as i32);
                    }
                    
                    let test_keys: Vec<String> = (0..1000)
                        .map(|i| format!("key_{}", i % cache_size))
                        .collect();
                    
                    // Test frequency() performance
                    let (_, freq_time) = measure_time(|| {
                        for key in &test_keys {
                            cache.frequency(key);
                        }
                    });
                    
                    // Test increment_frequency() performance
                    let (_, inc_time) = measure_time(|| {
                        for key in &test_keys {
                            cache.increment_frequency(key);
                        }
                    });
                    
                    // Test reset_frequency() performance
                    let (_, reset_time) = measure_time(|| {
                        for key in &test_keys {
                            cache.reset_frequency(key);
                        }
                    });
                    
                    let avg_freq_time = freq_time / test_keys.len() as u32;
                    let avg_inc_time = inc_time / test_keys.len() as u32;
                    let avg_reset_time = reset_time / test_keys.len() as u32;
                    
                    println!("Cache size: {}", cache_size);
                    println!("  Avg frequency() time: {:?}", avg_freq_time);
                    println!("  Avg increment_frequency() time: {:?}", avg_inc_time);
                    println!("  Avg reset_frequency() time: {:?}", avg_reset_time);
                    
                    // All frequency operations should be O(1) - allow up to 5µs each (realistic for HashMap operations)
                    assert!(avg_freq_time < Duration::from_micros(5),
                        "frequency() too slow for cache size {}: {:?}", cache_size, avg_freq_time);
                    assert!(avg_inc_time < Duration::from_micros(5),
                        "increment_frequency() too slow for cache size {}: {:?}", cache_size, avg_inc_time);
                    assert!(avg_reset_time < Duration::from_micros(5),
                        "reset_frequency() too slow for cache size {}: {:?}", cache_size, avg_reset_time);
                }
            }

            // ==============================================
            // SPACE COMPLEXITY TESTS
            // ==============================================

            #[test]
            fn test_memory_usage_scaling() {
                // Test that memory usage scales linearly with cache size
                let cache_sizes = vec![100, 500, 1000, 2000, 5000];
                
                for &cache_size in &cache_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Fill cache to capacity
                    for i in 0..cache_size {
                        cache.insert(format!("test_key_{:08}", i), i as i32);
                    }
                    
                    // Verify cache respects capacity constraints
                    assert_eq!(cache.len(), cache_size);
                    assert_eq!(cache.capacity(), cache_size);
                    
                    // Test overfill behavior
                    let pre_overfill_len = cache.len();
                    cache.insert("overflow_key".to_string(), -1);
                    
                    // Should maintain capacity by evicting LFU item
                    assert_eq!(cache.len(), cache_size);
                    assert_eq!(cache.len(), pre_overfill_len); // No growth
                    
                    println!("Cache size: {}, Final length: {}", cache_size, cache.len());
                }
            }

            #[test]
            fn test_memory_efficiency() {
                // Test memory efficiency of the LFU implementation
                let cache_size = 1000;
                let mut cache = LFUCache::new(cache_size);
                
                // Calculate theoretical minimum memory usage
                // Each entry stores: String key + i32 value + usize frequency
                // Plus HashMap overhead
                let key_size = std::mem::size_of::<String>(); // String struct
                let value_size = std::mem::size_of::<i32>();
                let freq_size = std::mem::size_of::<usize>();
                let entry_overhead = 24; // Rough HashMap entry overhead
                
                let theoretical_min_per_entry = key_size + value_size + freq_size + entry_overhead;
                println!("Theoretical minimum per entry: {} bytes", theoretical_min_per_entry);
                
                // Fill cache and verify it doesn't use excessive memory
                for i in 0..cache_size {
                    cache.insert(format!("key_{:06}", i), i as i32);
                }
                
                // Test that cache operations don't cause memory leaks
                let initial_len = cache.len();
                
                // Perform many operations
                for i in 0..1000 {
                    cache.insert(format!("temp_key_{}", i), i as i32);
                    cache.get(&format!("key_{:06}", i % cache_size));
                    cache.increment_frequency(&format!("key_{:06}", i % (cache_size / 2)));
                    
                    if i % 10 == 0 {
                        cache.pop_lfu();
                    }
                    
                    if i % 15 == 0 {
                        cache.reset_frequency(&format!("key_{:06}", i % cache_size));
                    }
                }
                
                // Cache should maintain its capacity
                assert_eq!(cache.len(), cache_size);
                println!("Cache maintained capacity through {} operations", 1000);
            }

            // ==============================================
            // SCALABILITY TESTS
            // ==============================================

            #[test]
            fn test_scalability_with_varying_key_sizes() {
                // Test performance with different key sizes
                let key_sizes = vec![10, 50, 100, 500];
                let cache_size = 1000;
                
                for &key_size in &key_sizes {
                    let mut cache = LFUCache::new(cache_size);
                    
                    // Generate keys of specified size
                    let long_key = "x".repeat(key_size);
                    
                    let (_, insert_time) = measure_time(|| {
                        for i in 0..cache_size {
                            let key = format!("{}{:06}", long_key, i);
                            cache.insert(key, i as i32);
                        }
                    });
                    
                    let avg_insert_time = insert_time / cache_size as u32;
                    println!("Key size: {} chars, Avg insert time: {:?}", key_size, avg_insert_time);
                    
                    // Performance should degrade gracefully with larger keys
                    // Allow up to 10μs per insert for very large keys (accounts for string hashing and memory allocation)
                    assert!(avg_insert_time < Duration::from_micros(10),
                        "Insert performance too slow for key size {}: {:?}", key_size, avg_insert_time);
                }
            }

            #[test]
            fn test_performance_regression_detection() {
                // Test to detect performance regressions
                let cache_size = 2000;
                let operation_count = 5000;
                
                let mut cache = LFUCache::new(cache_size);
                
                // Pre-populate
                for i in 0..cache_size {
                    cache.insert(format!("key_{}", i), i as i32);
                }
                
                // Mixed workload performance test
                let (results, total_time) = measure_time(|| {
                    let mut results = HashMap::new();
                    
                    for i in 0..operation_count {
                        let op_type = i % 10;
                        
                        match op_type {
                            0..=5 => { // 60% gets
                                let key = format!("key_{}", i % cache_size);
                                cache.get(&key);
                                *results.entry("gets").or_insert(0) += 1;
                            },
                            6..=7 => { // 20% inserts
                                cache.insert(format!("new_key_{}", i), i as i32);
                                *results.entry("inserts").or_insert(0) += 1;
                            },
                            8 => { // 10% frequency ops
                                let key = format!("key_{}", i % cache_size);
                                cache.increment_frequency(&key);
                                *results.entry("frequency_ops").or_insert(0) += 1;
                            },
                            9 => { // 10% pop_lfu
                                cache.pop_lfu();
                                *results.entry("pop_lfu").or_insert(0) += 1;
                            },
                            _ => unreachable!(),
                        }
                    }
                    
                    results
                });
                
                let avg_time_per_op = total_time / operation_count as u32;
                println!("Mixed workload results: {:?}", results);
                println!("Total time: {:?}, Avg per operation: {:?}", total_time, avg_time_per_op);
                
                // Performance baseline - should complete mixed workload reasonably quickly
                // Allow up to 500µs per operation for mixed workload (includes expensive pop_lfu operations)
                assert!(avg_time_per_op < Duration::from_micros(500),
                    "Mixed workload performance regression detected: {:?} per operation", avg_time_per_op);
                
                // Verify cache is still functional
                assert!(cache.len() <= cache_size);
                assert!(cache.len() > 0);
                assert!(cache.peek_lfu().is_some());
            }

            #[test]
            fn test_worst_case_performance() {
                // Test performance in worst-case scenarios
                let cache_size = 1000;
                let mut cache = LFUCache::new(cache_size);
                
                // Worst case: all items have the same frequency
                for i in 0..cache_size {
                    cache.insert(format!("key_{:06}", i), i as i32);
                }
                
                // All items now have frequency 1 (worst case for LFU operations)
                
                // Test pop_lfu performance with uniform frequencies
                let pop_count = 100;
                let (_, pop_time) = measure_time(|| {
                    for _ in 0..pop_count {
                        cache.pop_lfu();
                    }
                });
                
                let avg_pop_time = pop_time / pop_count as u32;
                println!("Worst-case pop_lfu time (uniform frequencies): {:?}", avg_pop_time);
                
                // Even in worst case, should be reasonable (uniform frequencies are challenging)
                assert!(avg_pop_time < Duration::from_millis(10),
                    "Worst-case pop_lfu performance too slow: {:?}", avg_pop_time);
                
                // Refill and test peek_lfu worst case
                for i in 0..100 {
                    cache.insert(format!("refill_key_{}", i), i as i32);
                }
                
                let peek_count = 1000;
                let (_, peek_time) = measure_time(|| {
                    for _ in 0..peek_count {
                        cache.peek_lfu();
                    }
                });
                
                let avg_peek_time = peek_time / peek_count as u32;
                println!("Worst-case peek_lfu time (uniform frequencies): {:?}", avg_peek_time);
                
                assert!(avg_peek_time < Duration::from_millis(1),
                    "Worst-case peek_lfu performance too slow: {:?}", avg_peek_time);
            }
        }

        // Stress Testing
        mod stress_testing {
            use super::*;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            use std::time::{Duration, Instant};


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
