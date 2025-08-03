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
