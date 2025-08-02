use std::collections::HashMap;
use std::hash::Hash;
use std::collections::VecDeque;
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUCacheTrait, MutableCache};

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

impl<K, V> LRUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new LRU cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            usage_list: VecDeque::with_capacity(capacity),
        }
    }
}

// Implementation of specialized traits
impl<K, V> CoreCache<K, V> for LRUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
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
        self.usage_list.clear();
    }
}

impl<K, V> MutableCache<K, V> for LRUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(pos) = self.usage_list.iter().position(|k| k == key) {
            self.usage_list.remove(pos);
        }
        self.cache.remove(key)
    }
}

impl<K, V> LRUCacheTrait<K, V> for LRUCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn pop_lru(&mut self) -> Option<(K, V)> {
        if let Some(lru_key) = self.usage_list.front().cloned() {
            if let Some(pos) = self.usage_list.iter().position(|k| k == &lru_key) {
                self.usage_list.remove(pos);
            }
            if let Some(value) = self.cache.remove(&lru_key) {
                return Some((lru_key, value));
            }
        }
        None
    }

    fn peek_lru(&self) -> Option<(&K, &V)> {
        if let Some(lru_key) = self.usage_list.front() {
            if let Some(value) = self.cache.get(lru_key) {
                return Some((lru_key, value));
            }
        }
        None
    }

    fn touch(&mut self, key: &K) -> bool {
        if self.cache.contains_key(key) {
            // Remove from current position and add to back
            if let Some(pos) = self.usage_list.iter().position(|k| k == key) {
                self.usage_list.remove(pos);
                self.usage_list.push_back(key.clone());
                return true;
            }
        }
        false
    }

    fn recency_rank(&self, key: &K) -> Option<usize> {
        if self.cache.contains_key(key) {
            // Find position in usage list (0 = most recent from back)
            // Since we add to back, we need to reverse the index
            if let Some(pos) = self.usage_list.iter().position(|k| k == key) {
                return Some(self.usage_list.len() - 1 - pos);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUCacheTrait, MutableCache};
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

    // Tests for new specialized traits
    #[test]
    fn test_lru_specialized_traits_basic() {
        let mut cache = LRUCache::new(3);

        // Test CoreCache trait
        assert_eq!(<LRUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 1, "one".to_string()), None);
        assert_eq!(<LRUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 2, "two".to_string()), None);
        assert_eq!(<LRUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 3, "three".to_string()), None);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        assert!(cache.contains(&1));
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_lru_mutable_cache_trait() {
        let mut cache = LRUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());

        // Test MutableCache trait
        assert_eq!(<LRUCache<i32, String> as MutableCache<i32, String>>::remove(&mut cache, &1), Some("one".to_string()));
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains(&1));

        // Test remove_batch
        cache.insert(3, "three".to_string());
        cache.insert(4, "four".to_string());
        let results = <LRUCache<i32, String> as MutableCache<i32, String>>::remove_batch(&mut cache, &[2, 3, 5]);
        assert_eq!(results, vec![Some("two".to_string()), Some("three".to_string()), None]);
    }

    #[test]
    fn test_lru_trait_pop_lru() {
        let mut cache = LRUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Access item 2 to make it most recently used
        cache.get(&2);

        // LRU should be item 1
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::pop_lru(&mut cache), Some((1, "one".to_string())));
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&1));
    }

    #[test]
    fn test_lru_trait_peek_lru() {
        let mut cache = LRUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Access item 3 to make it most recently used
        cache.get(&3);

        // LRU should be item 1
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::peek_lru(&cache), Some((&1, &"one".to_string())));
        
        // Cache should be unchanged
        assert_eq!(cache.len(), 3);
        assert!(cache.contains(&1));
    }

    #[test]
    fn test_lru_trait_touch() {
        let mut cache = LRUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Touch item 1 to make it most recently used
        assert!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::touch(&mut cache, &1));

        // After touching 1, LRU should be item 2
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::peek_lru(&cache), Some((&2, &"two".to_string())));

        // Touch non-existent key
        assert!(!<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::touch(&mut cache, &99));
    }

    #[test]
    fn test_lru_trait_recency_rank() {
        let mut cache = LRUCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Item 3 is most recently used (rank 0), item 1 is least recently used (rank 2)
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &3), Some(0));
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &2), Some(1));
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &1), Some(2));

        // Access item 1 to make it most recently used
        cache.get(&1);

        // Now item 1 should have rank 0
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &1), Some(0));

        // Non-existent key
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &99), None);
    }

    #[test]
    fn test_lru_trait_coexistence() {
        let mut cache = LRUCache::new(2);

        // Use CoreCache trait
        <LRUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 1, "core_trait".to_string());

        // Use CoreCache trait
        <LRUCache<i32, String> as CoreCache<i32, String>>::insert(&mut cache, 2, "another_core".to_string());

        // Use LRU-specific trait
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &1), Some(1));
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &2), Some(0));

        // Both items should be accessible
        assert_eq!(cache.get(&1), Some(&"old_trait".to_string()));
        assert_eq!(cache.get(&2), Some(&"new_trait".to_string()));
    }

    #[test]
    fn test_lru_empty_cache_operations() {
        let mut cache = LRUCache::new(2);

        // Test operations on empty cache
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::pop_lru(&mut cache), None);
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::peek_lru(&cache), None);
        assert!(!<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::touch(&mut cache, &1));
        assert_eq!(<LRUCache<i32, String> as LRUCacheTrait<i32, String>>::recency_rank(&cache, &1), None);
    }
}
