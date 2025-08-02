use std::collections::HashMap;
use std::hash::Hash;
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LFUCacheTrait, MutableCache};

/// LFU (Least Frequently Used) Cache implementation.
#[derive(Debug)]
pub struct LFUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    cache: HashMap<K, (V, usize)>, // (value, frequency)
    frequencies: HashMap<K, usize>, // Track frequencies separately for easier updates
}

impl<K, V> LFUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(capacity: usize) -> Self {
        LFUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            frequencies: HashMap::with_capacity(capacity),
        }
    }
}

// Implementation of specialized traits
impl<K, V> CoreCache<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        // If key already exists, just update the value
        if let Some((old_value, _freq)) = self.cache.get_mut(&key) {
            let old_val = old_value.clone();
            *old_value = value;
            // Keep the same frequency
            return Some(old_val);
        }

        // If cache is at capacity, remove the least frequently used item
        if self.cache.len() >= self.capacity && !self.cache.is_empty() {
            // Find the key with the lowest frequency
            if let Some((lfu_key, _)) = self.frequencies
                .iter()
                .min_by_key(|(_, freq)| **freq)
                .map(|(k, f)| (k.clone(), *f))
            {
                self.cache.remove(&lfu_key);
                self.frequencies.remove(&lfu_key);
            }
        }

        // Add the new key with frequency 1
        self.frequencies.insert(key.clone(), 1);
        self.cache.insert(key, (value, 1))
            .map(|(old_value, _)| old_value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some((value, freq)) = self.cache.get_mut(key) {
            // Increment frequency
            *freq += 1;
            if let Some(freq_entry) = self.frequencies.get_mut(key) {
                *freq_entry += 1;
            }
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
        self.frequencies.clear();
    }
}

impl<K, V> MutableCache<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn remove(&mut self, key: &K) -> Option<V> {
        self.frequencies.remove(key);
        self.cache.remove(key).map(|(value, _)| value)
    }
}

impl<K, V> LFUCacheTrait<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn pop_lfu(&mut self) -> Option<(K, V)> {
        // Find the key with the lowest frequency
        let lfu_key = self.frequencies
            .iter()
            .min_by_key(|(_, freq)| **freq)
            .map(|(k, _)| k.clone());

        if let Some(lfu_key) = lfu_key {
            self.frequencies.remove(&lfu_key);
            if let Some((value, _)) = self.cache.remove(&lfu_key) {
                return Some((lfu_key, value));
            }
        }
        None
    }

    fn peek_lfu(&self) -> Option<(&K, &V)> {
        // Find the key with the lowest frequency
        let lfu_key = self.frequencies
            .iter()
            .min_by_key(|(_, freq)| **freq)
            .map(|(k, _)| k);

        if let Some(lfu_key) = lfu_key {
            if let Some((value, _)) = self.cache.get(lfu_key) {
                return Some((lfu_key, value));
            }
        }
        None
    }

    fn frequency(&self, key: &K) -> Option<u64> {
        self.frequencies.get(key).map(|&freq| freq as u64)
    }

    fn reset_frequency(&mut self, key: &K) -> Option<u64> {
        if let Some(old_freq) = self.frequencies.insert(key.clone(), 1) {
            // Update the cache entry as well
            if let Some((_, freq)) = self.cache.get_mut(key) {
                *freq = 1;
            }
            Some(old_freq as u64)
        } else {
            None
        }
    }

    fn increment_frequency(&mut self, key: &K) -> Option<u64> {
        if let Some(freq) = self.frequencies.get_mut(key) {
            *freq += 1;
            // Update the cache entry as well
            if let Some((_, cache_freq)) = self.cache.get_mut(key) {
                *cache_freq = *freq;
            }
            Some(*freq as u64)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lfu_cache_basic() {
        let mut cache = LFUCache::new(2);

        // Insert two items
        assert_eq!(cache.insert(1, "one"), None);
        assert_eq!(cache.insert(2, "two"), None);

        // Cache should be at capacity
        assert_eq!(cache.len(), 2);

        // Access item 1 multiple times to increase its frequency
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&1), Some(&"one"));

        // Insert a third item, should evict item 2 (least frequently used)
        assert_eq!(cache.insert(3, "three"), None);

        // Check that item 2 was evicted
        assert_eq!(cache.get(&2), None);

        // Check that items 1 and 3 are still in the cache
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lfu_cache_update() {
        let mut cache = LFUCache::new(2);

        // Insert two items
        cache.insert(1, "one");
        cache.insert(2, "two");

        // Update item 1
        assert_eq!(cache.insert(1, "ONE"), Some("one"));

        // Access item 2 multiple times to increase its frequency
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&2), Some(&"two"));

        // Insert a third item, should evict item 1 despite being updated
        cache.insert(3, "three");

        // Check that item 1 was evicted
        assert_eq!(cache.get(&1), None);

        // Check that items 2 and 3 are still in the cache
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lfu_cache_remove() {
        let mut cache = LFUCache::new(2);

        cache.insert(1, "one");
        cache.insert(2, "two");

        // Remove item 1
        assert_eq!(cache.remove(&1), Some("one"));

        // Check that item 1 is no longer in the cache
        assert_eq!(cache.get(&1), None);

        // Insert a new item
        cache.insert(3, "three");

        // Check that items 2 and 3 are in the cache
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    // Tests for new specialized traits
    #[test]
    fn test_lfu_specialized_traits_basic() {
        let mut cache = LFUCache::new(3);

        // Test CoreCache trait
        assert_eq!(<LFUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 1, "one".to_string()), None);
        assert_eq!(<LFUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 2, "two".to_string()), None);
        assert_eq!(<LFUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 3, "three".to_string()), None);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        assert!(cache.contains(&1));
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_lfu_mutable_cache_trait() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());

        // Test MutableCache trait
        assert_eq!(<LFUCache<i32, String> as MutableCache<i32, String>>::remove(&mut cache, &1), Some("one".to_string()));
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains(&1));

        // Test remove_batch
        cache.insert(3, "three".to_string());
        cache.insert(4, "four".to_string());
        let results = <LFUCache<i32, String> as MutableCache<i32, String>>::remove_batch(&mut cache, &[2, 3, 5]);
        assert_eq!(results, vec![Some("two".to_string()), Some("three".to_string()), None]);
    }

    #[test]
    fn test_lfu_trait_pop_lfu() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Access item 2 multiple times to increase its frequency
        cache.get(&2);
        cache.get(&2);

        // LFU should be either item 1 or 3 (both have frequency 1)
        let (lfu_key, _) = <LFUCache<i32, String> as LFUCacheTrait<i32, String>>::pop_lfu(&mut cache).unwrap();
        assert!(lfu_key == 1 || lfu_key == 3);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&lfu_key));
    }

    #[test]
    fn test_lfu_trait_peek_lfu() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Access item 3 multiple times
        cache.get(&3);
        cache.get(&3);

        // LFU should be either item 1 or 2 (both have frequency 1)
        let (lfu_key, _) = <LFUCache<i32, String> as LFUCacheTrait<i32, String>>::peek_lfu(&cache).unwrap();
        assert!(lfu_key == &1 || lfu_key == &2);
        
        // Cache should be unchanged
        assert_eq!(cache.len(), 3);
        assert!(cache.contains(&1));
        assert!(cache.contains(&2));
        assert!(cache.contains(&3));
    }

    #[test]
    fn test_lfu_trait_frequency() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());

        // Initial frequency should be 1
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(1));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &2), Some(1));

        // Access item 1 multiple times
        cache.get(&1);
        cache.get(&1);

        // Frequency should be updated
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(3));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &2), Some(1));

        // Non-existent key
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &99), None);
    }

    #[test]
    fn test_lfu_trait_reset_frequency() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());

        // Access multiple times to increase frequency
        cache.get(&1);
        cache.get(&1);
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(3));

        // Reset frequency
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::reset_frequency(&mut cache, &1), Some(3));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(1));

        // Reset non-existent key
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::reset_frequency(&mut cache, &99), None);
    }

    #[test]
    fn test_lfu_trait_increment_frequency() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());

        // Initial frequency should be 1
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(1));

        // Increment frequency manually
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::increment_frequency(&mut cache, &1), Some(2));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(2));

        // Increment again
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::increment_frequency(&mut cache, &1), Some(3));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(3));

        // Increment non-existent key
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::increment_frequency(&mut cache, &99), None);
    }

    #[test]
    fn test_lfu_trait_coexistence() {
        let mut cache = LFUCache::new(2);

        // Use CoreCache trait
        <LFUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 1, "core_trait".to_string());
        <LFUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 2, "another_core".to_string());

        // Use LFU-specific trait
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(1));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &2), Some(1));

        // Both items should be accessible
        assert_eq!(<LFUCache<i32, String> as CoreCache<i32, String>>::get(&mut cache, &1), Some(&"core_trait".to_string()));
        assert_eq!(<LFUCache<i32, String> as CoreCache<i32, String>>::get(&mut cache, &2), Some(&"another_core".to_string()));

        // Frequencies should be updated after access
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(2));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &2), Some(2));
    }

    #[test]
    fn test_lfu_empty_cache_operations() {
        let mut cache = LFUCache::new(2);

        // Test operations on empty cache
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::pop_lfu(&mut cache), None);
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::peek_lfu(&cache), None);
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), None);
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::reset_frequency(&mut cache, &1), None);
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::increment_frequency(&mut cache, &1), None);
    }

    #[test]
    fn test_lfu_frequency_consistency() {
        let mut cache = LFUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Make item 2 most frequently used
        for _ in 0..5 {
            cache.get(&2);
        }

        // Make item 3 moderately used
        for _ in 0..2 {
            cache.get(&3);
        }

        // Item 1 should be LFU (frequency 1)
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &1), Some(1));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &2), Some(6));
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::frequency(&cache, &3), Some(3));

        // Peek LFU should return item 1
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::peek_lfu(&cache), Some((&1, &"one".to_string())));

        // Pop LFU should remove item 1
        assert_eq!(<LFUCache<i32, String> as LFUCacheTrait<i32, String>>::pop_lfu(&mut cache), Some((1, "one".to_string())));
        assert_eq!(cache.len(), 2);
    }
}
