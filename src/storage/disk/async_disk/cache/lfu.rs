use std::collections::HashMap;
use std::hash::Hash;

use super::cache_trait::Cache;

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

impl<K, V> Cache<K, V> for LFUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize) -> Self {
        LFUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            frequencies: HashMap::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        // If key already exists, update its value and frequency
        if let Some((old_value, freq)) = self.cache.get_mut(&key) {
            let old_value_clone = old_value.clone();
            *old_value = value;
            *freq += 1;
            self.frequencies.insert(key, *freq);
            return Some(old_value_clone);
        }

        // If cache is at capacity, remove the least frequently used item
        if self.cache.len() >= self.capacity && !self.cache.is_empty() {
            // Find the key with the lowest frequency
            let lfu_key = self.frequencies
                .iter()
                .min_by_key(|(_, freq)| **freq)
                .map(|(k, _)| k.clone());

            if let Some(lfu_key) = lfu_key {
                self.frequencies.remove(&lfu_key);
                self.cache.remove(&lfu_key);
            }
        }

        // Add the new key with frequency 1
        self.frequencies.insert(key.clone(), 1);
        self.cache.insert(key, (value, 1)).map(|(v, _)| v)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some((_, freq)) = self.cache.get_mut(key) {
            // Increment frequency
            *freq += 1;
            self.frequencies.insert(key.clone(), *freq);
            return self.cache.get(key).map(|(v, _)| v);
        }
        None
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.frequencies.remove(key);
        self.cache.remove(key).map(|(v, _)| v)
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.frequencies.clear();
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
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
}
