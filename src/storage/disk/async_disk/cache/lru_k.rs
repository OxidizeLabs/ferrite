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
    use std::thread::sleep;
    use std::time::Duration;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait, MutableCache};

    #[test]
    fn test_lru_k_cache_basic() {
        let mut cache = LRUKCache::new(3); // Default k=2

        // Insert three items
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1)); // Ensure different timestamps
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");

        // Cache should be at capacity
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 3);

        // Access items to build history
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1); // Second access for item 1
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2); // Second access for item 2

        // Insert a fourth item, should evict item 3 (only one access)
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 4, "four");

        // Check that item 3 was evicted
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &3), None);

        // Check that other items are still in the cache
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1), Some(&"one"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2), Some(&"two"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &4), Some(&"four"));
    }

    #[test]
    fn test_lru_k_cache_with_custom_k() {
        let mut cache = LRUKCache::with_k(3, 3); // k=3

        // Insert three items
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");

        // Access items to build history
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1); // Second access for item 1
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1); // Third access for item 1
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2); // Second access for item 2

        // Insert a fourth item, should evict item 3 (only one access)
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 4, "four");

        // Check that item 3 was evicted
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &3), None);

        // Check that other items are still in the cache
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1), Some(&"one"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2), Some(&"two"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &4), Some(&"four"));
    }

    #[test]
    fn test_lru_k_cache_eviction_order() {
        let mut cache = LRUKCache::new(3); // Default k=2

        // Insert three items
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");

        // Access items to build history
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1); // Second access for item 1
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2); // Second access for item 2
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &3); // Second access for item 3

        // Now all items have 2 accesses, so the k-th (2nd) access time is used
        // Item 1 has the oldest 2nd access, so it should be evicted

        // Insert a fourth item
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 4, "four");

        // Check that item 1 was evicted
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1), None);

        // Check that other items are still in the cache
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2), Some(&"two"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &3), Some(&"three"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &4), Some(&"four"));
    }

    #[test]
    fn test_lru_k_specialized_traits_basic() {
        let mut cache = LRUKCache::new(3); // Default k=2

        // Test CoreCache trait
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 2);
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::capacity(&cache), 3);
        assert!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &1));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1), Some(&"one"));
    }

    #[test]
    fn test_lru_k_mutable_cache_trait() {
        let mut cache = LRUKCache::new(3);
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");
        
        // Test MutableCache remove
        assert_eq!(<LRUKCache<i32, &str> as MutableCache<i32, &str>>::remove(&mut cache, &2), Some("two"));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 2);
        assert!(!<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &2));
        
        // Test batch removal
        let keys = [1, 3];
        let removed = <LRUKCache<i32, &str> as MutableCache<i32, &str>>::remove_batch(&mut cache, &keys);
        assert_eq!(removed, vec![Some("one"), Some("three")]);
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 0);
    }

    #[test]
    fn test_lru_k_trait_pop_lru_k() {
        let mut cache = LRUKCache::with_k(3, 2);
        
        // Test empty cache
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::pop_lru_k(&mut cache), None);
        
        // Insert items with different access patterns
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");
        
        // Give item 1 second access (so it has K=2 accesses)
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        sleep(Duration::from_millis(1));
        
        // Item 2 and 3 have only 1 access, item 1 has 2 accesses
        // Items 2 and 3 should be preferred for eviction (fewer than K accesses)
        // Item 2 was inserted first, so it should be evicted first
        let evicted = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::pop_lru_k(&mut cache);
        assert_eq!(evicted, Some((2, "two")));
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 2);
    }

    #[test]
    fn test_lru_k_trait_peek_lru_k() {
        let mut cache = LRUKCache::with_k(3, 2);
        
        // Test empty cache
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::peek_lru_k(&cache), None);
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");
        
        // Should peek at the item that would be evicted next (item 1 - oldest with < K accesses)
        let peeked = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::peek_lru_k(&cache);
        assert_eq!(peeked, Some((&1, &"one")));
        
        // Cache should be unchanged
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 3);
        assert!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &1));
    }

    #[test]
    fn test_lru_k_trait_k_value() {
        let cache = LRUKCache::with_k(10, 3);
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_value(&cache), 3);
        
        let cache2 = LRUKCache::new(10); // Default k=2
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_value(&cache2), 2);
    }

    #[test]
    fn test_lru_k_trait_access_history() {
        let mut cache = LRUKCache::with_k(5, 3);
        
        // Test non-existent key
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_history(&cache, &1), None);
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        let history1 = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_history(&cache, &1).unwrap();
        assert_eq!(history1.len(), 1);
        
        // Access the item multiple times
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        
        let history2 = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_history(&cache, &1).unwrap();
        assert_eq!(history2.len(), 3);
        
        // History should be in reverse chronological order (most recent first)
        assert!(history2[0] > history2[1]);
        assert!(history2[1] > history2[2]);
    }

    #[test]
    fn test_lru_k_trait_access_count() {
        let mut cache = LRUKCache::with_k(5, 3);
        
        // Test non-existent key
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1), None);
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1), Some(1));
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1), Some(3));
        
        // Access count should be capped at K
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1), Some(3));
    }

    #[test]
    fn test_lru_k_trait_k_distance() {
        let mut cache = LRUKCache::with_k(5, 3);
        
        // Test non-existent key
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance(&cache, &1), None);
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        // Only 1 access, no K-distance yet
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance(&cache, &1), None);
        
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        // Only 2 accesses, still no K-distance (K=3)
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance(&cache, &1), None);
        
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        // Now has 3 accesses, should have K-distance
        let k_distance = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance(&cache, &1);
        assert!(k_distance.is_some());
    }

    #[test]
    fn test_lru_k_trait_touch() {
        let mut cache = LRUKCache::with_k(5, 2);
        
        // Test non-existent key
        assert!(!<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::touch(&mut cache, &1));
        
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        let count_before = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1).unwrap();
        
        // Touch should increment access count
        assert!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::touch(&mut cache, &1));
        let count_after = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1).unwrap();
        assert_eq!(count_after, count_before + 1);
    }

    #[test]
    fn test_lru_k_trait_k_distance_rank() {
        let mut cache = LRUKCache::with_k(4, 2);
        
        // Test non-existent key
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &1), None);
        
        // Insert items at different times
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");
        
        // All items have only 1 access, so rank by insertion order
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &1), Some(0)); // oldest
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &2), Some(1));
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &3), Some(2)); // newest
        
        // Give item 1 a second access, making it have K=2 accesses
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        
        // Now item 1 should rank lower (has K accesses), items 2,3 rank higher (< K accesses)
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &2), Some(0)); // highest priority (< K accesses, oldest)
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &3), Some(1)); // second priority (< K accesses, newer)
        assert_eq!(<LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_distance_rank(&cache, &1), Some(2)); // lowest priority (>= K accesses)
    }

    #[test]
    fn test_lru_k_trait_coexistence() {
        let mut cache = LRUKCache::new(3);
        
        // Use old Cache trait
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "old_way");
        let value = <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        assert_eq!(value, Some(&"old_way"));
        
        // Use new CoreCache trait on same instance
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "new_way");
        let value2 = <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &2);
        assert_eq!(value2, Some(&"new_way"));
        
        // Use LRU-K specific trait
        let k_val = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::k_value(&cache);
        assert_eq!(k_val, 2);
        
        let count = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::access_count(&cache, &1);
        assert_eq!(count, Some(2)); // insert + get
        
        // All traits work on the same cache instance
        assert_eq!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::len(&cache), 2);
    }

    #[test]
    fn test_lru_k_empty_cache_operations() {
        let mut cache = LRUKCache::<i32, String>::new(5);
        
        // All operations should handle empty cache gracefully
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::pop_lru_k(&mut cache), None);
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::peek_lru_k(&cache), None);
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::access_history(&cache, &1), None);
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::access_count(&cache, &1), None);
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::k_distance(&cache, &1), None);
        assert!(!<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::touch(&mut cache, &1));
        assert_eq!(<LRUKCache<i32, String> as LRUKCacheTrait<i32, String>>::k_distance_rank(&cache, &1), None);
        
        assert_eq!(<LRUKCache<i32, String> as CoreCache<i32, String>>::len(&cache), 0);
        assert!(<LRUKCache<i32, String> as CoreCache<i32, String>>::is_empty(&cache));
    }

    #[test]
    fn test_lru_k_complex_eviction_scenario() {
        let mut cache = LRUKCache::with_k(3, 2);
        
        // Insert items
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 1, "one");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 2, "two");
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 3, "three");
        
        // Access item 1 multiple times to give it K accesses
        sleep(Duration::from_millis(1));
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::get(&mut cache, &1);
        
        // Items 2 and 3 have 1 access, item 1 has 2 accesses
        // When we insert item 4, item 2 should be evicted (oldest with < K accesses)
        <LRUKCache<i32, &str> as CoreCache<i32, &str>>::insert(&mut cache, 4, "four");
        
        assert!(!<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &2)); // Should be evicted
        assert!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &1));   // Should remain (has K accesses)
        assert!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &3));   // Should remain
        assert!(<LRUKCache<i32, &str> as CoreCache<i32, &str>>::contains(&cache, &4));   // Should be present
        
        // Verify we can peek at the next victim
        let next_victim = <LRUKCache<i32, &str> as LRUKCacheTrait<i32, &str>>::peek_lru_k(&cache);
        assert_eq!(next_victim, Some((&3, &"three"))); // Next oldest with < K accesses
    }
}
