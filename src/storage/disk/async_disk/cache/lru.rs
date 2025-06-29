use std::collections::HashMap;
use std::hash::Hash;
use std::collections::VecDeque;

use super::cache_trait::Cache;

/// LRU (Least Recently Used) Cache implementation.
pub struct LRUCache<K, V> 
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    cache: HashMap<K, V>,
    usage_list: VecDeque<K>,
}

impl<K, V> Cache<K, V> for LRUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            usage_list: VecDeque::with_capacity(capacity),
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
        // Update usage list
        if self.cache.contains_key(&key) {
            if let Some(pos) = self.usage_list.iter().position(|k| k == &key) {
                self.usage_list.remove(pos);
            }
        } else if self.cache.len() >= self.capacity && !self.cache.is_empty() {
            // Evict least recently used item
            if let Some(lru_key) = self.usage_list.pop_front() {
                self.cache.remove(&lru_key);
            }
        }

        self.usage_list.push_back(key.clone());
        self.cache.insert(key, value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.cache.contains_key(key) {
            // Update usage
            if let Some(pos) = self.usage_list.iter().position(|k| k == key) {
                self.usage_list.remove(pos);
            }
            self.usage_list.push_back(key.clone());
            return self.cache.get(key);
        }
        None
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(pos) = self.usage_list.iter().position(|k| k == key) {
            self.usage_list.remove(pos);
        }
        self.cache.remove(key)
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.usage_list.clear();
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LRUCache::new(2);

        // Insert two items
        assert_eq!(cache.insert(1, "one"), None);
        assert_eq!(cache.insert(2, "two"), None);

        // Cache should be at capacity
        assert_eq!(cache.len(), 2);

        // Access item 1 to make it most recently used
        assert_eq!(cache.get(&1), Some(&"one"));

        // Insert a third item, should evict item 2
        assert_eq!(cache.insert(3, "three"), None);

        // Check that item 2 was evicted
        assert_eq!(cache.get(&2), None);

        // Check that items 1 and 3 are still in the cache
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lru_cache_update() {
        let mut cache = LRUCache::new(2);

        // Insert two items
        cache.insert(1, "one");
        cache.insert(2, "two");

        // Update item 1
        assert_eq!(cache.insert(1, "ONE"), Some("one"));

        // Insert a third item, should evict item 2 since 1 was recently used
        cache.insert(3, "three");

        // Check that item 2 was evicted
        assert_eq!(cache.get(&2), None);

        // Check that items 1 and 3 are still in the cache
        assert_eq!(cache.get(&1), Some(&"ONE"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lru_cache_remove() {
        let mut cache = LRUCache::new(2);

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
