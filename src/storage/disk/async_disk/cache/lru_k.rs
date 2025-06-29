use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

use super::cache_trait::Cache;

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

impl<K, V> Cache<K, V> for LRUKCache<K, V>
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new(capacity: usize) -> Self {
        // Default K value is 2
        Self::with_k(capacity, 2)
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

    fn remove(&mut self, key: &K) -> Option<V> {
        self.cache.remove(key).map(|(v, _)| v)
    }

    fn clear(&mut self) {
        self.cache.clear();
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_lru_k_cache_basic() {
        let mut cache = LRUKCache::new(3); // Default k=2

        // Insert three items
        cache.insert(1, "one");
        sleep(Duration::from_millis(1)); // Ensure different timestamps
        cache.insert(2, "two");
        sleep(Duration::from_millis(1));
        cache.insert(3, "three");

        // Cache should be at capacity
        assert_eq!(cache.len(), 3);

        // Access items to build history
        cache.get(&1); // Second access for item 1
        sleep(Duration::from_millis(1));
        cache.get(&2); // Second access for item 2

        // Insert a fourth item, should evict item 3 (only one access)
        cache.insert(4, "four");

        // Check that item 3 was evicted
        assert_eq!(cache.get(&3), None);

        // Check that other items are still in the cache
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&4), Some(&"four"));
    }

    #[test]
    fn test_lru_k_cache_with_custom_k() {
        let mut cache = LRUKCache::with_k(3, 3); // k=3

        // Insert three items
        cache.insert(1, "one");
        sleep(Duration::from_millis(1));
        cache.insert(2, "two");
        sleep(Duration::from_millis(1));
        cache.insert(3, "three");

        // Access items to build history
        cache.get(&1); // Second access for item 1
        sleep(Duration::from_millis(1));
        cache.get(&1); // Third access for item 1
        sleep(Duration::from_millis(1));
        cache.get(&2); // Second access for item 2

        // Insert a fourth item, should evict item 3 (only one access)
        cache.insert(4, "four");

        // Check that item 3 was evicted
        assert_eq!(cache.get(&3), None);

        // Check that other items are still in the cache
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&4), Some(&"four"));
    }

    #[test]
    fn test_lru_k_cache_eviction_order() {
        let mut cache = LRUKCache::new(3); // Default k=2

        // Insert three items
        cache.insert(1, "one");
        sleep(Duration::from_millis(1));
        cache.insert(2, "two");
        sleep(Duration::from_millis(1));
        cache.insert(3, "three");

        // Access items to build history
        cache.get(&1); // Second access for item 1
        sleep(Duration::from_millis(1));
        cache.get(&2); // Second access for item 2
        sleep(Duration::from_millis(1));
        cache.get(&3); // Second access for item 3

        // Now all items have 2 accesses, so the k-th (2nd) access time is used
        // Item 1 has the oldest 2nd access, so it should be evicted

        // Insert a fourth item
        cache.insert(4, "four");

        // Check that item 1 was evicted
        assert_eq!(cache.get(&1), None);

        // Check that other items are still in the cache
        assert_eq!(cache.get(&2), Some(&"two"));
        assert_eq!(cache.get(&3), Some(&"three"));
        assert_eq!(cache.get(&4), Some(&"four"));
    }
}
