use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;

use super::cache_trait::Cache;

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

impl<K, V> Cache<K, V> for FIFOCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize) -> Self {
        FIFOCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
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
        // If key already exists, just update the value
        if self.cache.contains_key(&key) {
            return self.cache.insert(key, value);
        }

        // If cache is at capacity, remove the oldest item (FIFO)
        if self.cache.len() >= self.capacity && !self.cache.is_empty() {
            if let Some(oldest_key) = self.insertion_order.pop_front() {
                self.cache.remove(&oldest_key);
            }
        }

        // Add the new key to the insertion order and cache
        self.insertion_order.push_back(key.clone());
        self.cache.insert(key, value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        // In FIFO, getting an item doesn't change its position
        self.cache.get(key)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        // Remove from insertion order if present
        if let Some(pos) = self.insertion_order.iter().position(|k| k == key) {
            self.insertion_order.remove(pos);
        }
        // Remove from cache and return the value
        self.cache.remove(key)
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.insertion_order.clear();
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_cache_basic() {
        let mut cache = FIFOCache::new(2);

        // Insert two items
        assert_eq!(cache.insert(1, "one"), None);
        assert_eq!(cache.insert(2, "two"), None);

        // Cache should be at capacity
        assert_eq!(cache.len(), 2);

        // Access item 1 (this shouldn't affect eviction order in FIFO)
        assert_eq!(cache.get(&1), Some(&"one"));

        // Insert a third item, should evict item 1 (first in)
        assert_eq!(cache.insert(3, "three"), None);

        // Check that item 1 was evicted
        assert_eq!(cache.get(&1), None);

        // Check that items 2 and 3 are still in the cache
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_fifo_cache_update() {
        let mut cache = FIFOCache::new(2);

        // Insert two items
        cache.insert(1, "one");
        cache.insert(2, "two");

        // Update item 1 (this shouldn't change its position in the FIFO order)
        assert_eq!(cache.insert(1, "ONE"), Some("one"));

        // Insert a third item, should still evict item 1 despite being updated
        cache.insert(3, "three");

        // Check that item 1 was evicted
        assert_eq!(cache.get(&1), None);

        // Check that items 2 and 3 are still in the cache
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_fifo_cache_remove() {
        let mut cache = FIFOCache::new(2);

        cache.insert(1, "one");
        cache.insert(2, "two");

        // Remove item 1
        assert_eq!(cache.remove(&1), Some("one"));

        // Check that item 1 is no longer in the cache
        assert_eq!(cache.get(&1), None);

        // Insert a new item
        cache.insert(3, "three");

        // Insert another item, should evict item 2 (oldest remaining)
        cache.insert(4, "four");

        // Check that item 2 was evicted
        assert_eq!(cache.get(&2), None);

        // Check that items 3 and 4 are in the cache
        assert_eq!(cache.get(&3), Some(&"three"));
        assert_eq!(cache.get(&4), Some(&"four"));
    }
}
