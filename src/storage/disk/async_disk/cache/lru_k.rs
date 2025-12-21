//! LRU-K Cache Implementation.
//!
//! This module provides an implementation of the LRU-K replacement policy (specifically LRU-2 by default).
//!
//! # Architecture
//!
//! The implementation uses:
//! - A `HashMap` to store key-value pairs along with their access history.
//! - `VecDeque` for tracking the last K access timestamps for each item.
//! - `SystemTime` for generating timestamps (in microseconds) to order accesses.
//!
//! Unlike the O(1) LRU implementation, this LRU-K implementation is **O(N)** for eviction operations
//! because it must scan all entries to find the one with the maximum "backward K-distance".
//! This design decision prioritizes simplicity and correctness of the complex LRU-K logic over raw performance,
//! which is acceptable given that LRU-K is typically used in scenarios where hit ratios are more critical
//! than eviction throughput (e.g., database buffer pools) or where N is manageable.
//!
//! # Key Features
//!
//! - **LRU-K Policy**: Evicts the item whose K-th most recent access is furthest in the past.
//!   This creates a "correlated reference period" that prevents cold pages from being evicted
//!   prematurely due to a single scan.
//! - **Configurable K**: Defaults to K=2 (LRU-2), which provides a good balance between
//!   responsiveness and scan resistance.
//! - **Access History Tracking**: Maintains a history of the last K access times for each item.
//!
//! # Design Rationale
//!
//! - **Scan Resistance**: Standard LRU is vulnerable to sequential scans (e.g., table scans)
//!   flushing the entire cache. LRU-K mitigates this by requiring K accesses before an item
//!   is considered "hot" enough to be retained over others.
//! - **Simplicity**: The current O(N) implementation avoids the complexity of maintaining
//!   multiple priority queues or complex pointer structures required for an O(log N) or O(1) LRU-K.
//!
//! # Performance Characteristics
//!
//! - **Time Complexity**:
//!   - `get`, `insert` (without eviction), `remove`: **O(1)** (average case HashMap).
//!   - `insert` (with eviction), `pop_lru_k`: **O(N)** where N is the number of items in the cache.
//!     It scans all items to calculate the K-distance.
//! - **Space Overhead**:
//!   - Per entry: Value storage + `VecDeque` of K timestamps (typically small, e.g., 2 `u64`s).
//! - **Concurrency**:
//!   - This struct is **not thread-safe**. It is intended to be wrapped in a `Mutex` or `RwLock`
//!     (like `ConcurrentLRUCache` does for `LRUCore`) or used in single-threaded contexts.
//!
//! # Trade-offs
//!
//! - **Pros**:
//!   - Better hit ratios than LRU for database workloads involving scans.
//!   - Robust against "cache pollution" from one-time access patterns.
//! - **Cons**:
//!   - **O(N) Eviction**: Performance degrades linearly with cache size during evictions. This is
//!     the major trade-off for the improved replacement policy logic without complex data structures.
//!   - Memory overhead of storing timestamps.
//!
//! # When to Use
//!
//! - **Use when**:
//!   - You are implementing a database buffer pool where scan resistance is critical.
//!   - The cost of a cache miss (disk I/O) vastly outweighs the CPU cost of an O(N) eviction scan.
//!   - The cache size is moderate, or eviction is infrequent compared to hits.
//! - **Avoid when**:
//!   - You need strictly bounded O(1) latency for all operations (use `LRUCache`).
//!   - The cache size is extremely large (millions of items) and evictions are frequent.
//!   - You are in a high-frequency, low-latency environment (e.g., CPU cache simulation).

use crate::storage::disk::async_disk::cache::cache_traits::{
    CoreCache, LRUKCacheTrait, MutableCache,
};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

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
        if self.capacity == 0 {
            return None;
        }

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
        victim_key.and_then(|key| self.cache.remove(&key).map(|(value, _)| (key, value)))
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

        victim_key.and_then(|key| self.cache.get(key).map(|(value, _)| (key, value)))
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

        for (_, history) in self.cache.values() {
            let num_accesses = history.len();

            if num_accesses < self.k {
                // Items with fewer than K accesses use their earliest access time
                let earliest = history.front().copied().unwrap_or(u64::MAX);
                items_with_distances.push((false, earliest)); // false = not full K accesses
            } else {
                // Items with K or more accesses use their K-distance
                let k_distance = history
                    .get(num_accesses - self.k)
                    .copied()
                    .unwrap_or(u64::MAX);
                items_with_distances.push((true, k_distance)); // true = has full K accesses
            }
        }

        // Sort by priority: items with fewer than K accesses first (by earliest access),
        // then items with K+ accesses (by K-distance)
        items_with_distances.sort_by(|a, b| {
            match (a.0, b.0) {
                (false, false) => a.1.cmp(&b.1), // Both have < K accesses, sort by earliest
                (true, true) => a.1.cmp(&b.1),   // Both have >= K accesses, sort by K-distance
                (false, true) => std::cmp::Ordering::Less, // < K accesses comes first
                (true, false) => std::cmp::Ordering::Greater, // >= K accesses comes second
            }
        });

        // Find the rank of the target key
        let target_history = &self.cache[key].1;
        let target_num_accesses = target_history.len();
        let target_value = if target_num_accesses < self.k {
            (false, target_history.front().copied().unwrap_or(u64::MAX))
        } else {
            (
                true,
                target_history
                    .get(target_num_accesses - self.k)
                    .copied()
                    .unwrap_or(u64::MAX),
            )
        };

        items_with_distances
            .iter()
            .position(|item| item == &target_value)
    }
}

#[cfg(test)]
mod tests {
    mod basic_behavior {
        use super::super::*;
        use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait};
        use std::thread;
        use std::time::Duration;

        #[test]
        fn test_basic_lru_k_insertion_and_retrieval() {
            let mut cache = LRUKCache::new(2);
            cache.insert(1, "one");
            assert_eq!(cache.get(&1), Some(&"one"));
            
            cache.insert(2, "two");
            assert_eq!(cache.get(&2), Some(&"two"));
            assert_eq!(cache.len(), 2);
        }

        #[test]
        fn test_lru_k_eviction_order() {
            // Capacity 3, K=2
            let mut cache = LRUKCache::with_k(3, 2);
            
            // Access pattern:
            // 1: access (history: [t1]) -> < K accesses
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(2));
            
            // 2: access (history: [t2]) -> < K accesses
            cache.insert(2, 20);
            thread::sleep(Duration::from_millis(2));

            // 3: access (history: [t3]) -> < K accesses
            cache.insert(3, 30);
            thread::sleep(Duration::from_millis(2));

            // Cache full: {1, 2, 3} all have 1 access.
            // Eviction policy prioritizes items with < K accesses, then earliest access.
            // 1 is oldest.
            
            // Insert 4
            cache.insert(4, 40);
            
            assert!(!cache.contains(&1), "1 should be evicted");
            assert!(cache.contains(&2));
            assert!(cache.contains(&3));
            assert!(cache.contains(&4));
            
            // Now make 2 have K accesses.
            thread::sleep(Duration::from_millis(2));
            cache.get(&2); // 2 now has 2 accesses.
            
            // Current state:
            // 2: 2 accesses (>= K). Last access t5.
            // 3: 1 access (< K). Last access t3.
            // 4: 1 access (< K). Last access t4.
            
            // If we insert 5, we look for items with < K accesses first.
            // Candidates: 3, 4.
            // 3 is older (t3 < t4). Victim: 3.
            
            cache.insert(5, 50);
            assert!(!cache.contains(&3), "3 should be evicted");
            assert!(cache.contains(&2));
            assert!(cache.contains(&4));
            assert!(cache.contains(&5));
        }

        #[test]
        fn test_capacity_enforcement() {
            let mut cache = LRUKCache::new(2);
            cache.insert(1, 1);
            thread::sleep(Duration::from_millis(1));
            cache.insert(2, 2);
            assert_eq!(cache.len(), 2);
            
            cache.insert(3, 3);
            assert_eq!(cache.len(), 2);
            // 1 should be evicted (earliest access, < K)
            assert!(!cache.contains(&1));
            assert!(cache.contains(&2));
            assert!(cache.contains(&3));
        }

        #[test]
        fn test_update_existing_key() {
            let mut cache = LRUKCache::new(2);
            cache.insert(1, 10);
            cache.insert(1, 20);
            
            assert_eq!(cache.get(&1), Some(&20));
            // Insert counts as access. First insert = 1 access. Second insert = 2 accesses.
            assert_eq!(cache.access_count(&1), Some(2)); 
        }

        #[test]
        fn test_access_history_tracking() {
            let mut cache = LRUKCache::with_k(2, 3); // K=3
            cache.insert(1, 10); // 1 access
            thread::sleep(Duration::from_millis(2));
            
            cache.get(&1); // 2 accesses
            thread::sleep(Duration::from_millis(2));
            
            cache.get(&1); // 3 accesses
            thread::sleep(Duration::from_millis(2));
            
            assert_eq!(cache.access_count(&1), Some(3));
            
            cache.get(&1); // 4 accesses. Should keep last 3.
            assert_eq!(cache.access_count(&1), Some(3));
            
            let history = cache.access_history(&1).unwrap();
            assert_eq!(history.len(), 3);
            // Verify order (most recent first)
            assert!(history[0] > history[1]);
            assert!(history[1] > history[2]);
        }

        #[test]
        fn test_k_value_behavior() {
            let cache = LRUKCache::<i32, i32>::with_k(10, 5);
            assert_eq!(cache.k_value(), 5);
        }

        #[test]
        fn test_key_operations_consistency() {
            let mut cache = LRUKCache::new(2);
            cache.insert(1, 10);
             
            assert!(cache.contains(&1));
            assert_eq!(cache.get(&1), Some(&10));
            assert_eq!(cache.len(), 1);
        }

        #[test]
        fn test_timestamp_ordering() {
            let mut cache = LRUKCache::with_k(2, 1); // K=1 (LRU)
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(2));
            cache.insert(2, 20);
             
            let dist1 = cache.k_distance(&1).unwrap();
            let dist2 = cache.k_distance(&2).unwrap();
             
            // K=1, k_distance is the timestamp of the last access.
            // 2 was inserted after 1, so dist2 > dist1.
            assert!(dist2 > dist1);
        }
    }

    // Edge Cases Tests
    mod edge_cases {
        use super::super::*;
        use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait, MutableCache};
        use std::thread;
        use std::time::Duration;

        #[test]
        fn test_empty_cache_operations() {
            let mut cache = LRUKCache::<i32, i32>::new(5);
            assert_eq!(cache.get(&1), None);
            assert_eq!(cache.remove(&1), None);
            assert_eq!(cache.len(), 0);
            assert!(!cache.contains(&1));
        }

        #[test]
        fn test_single_item_cache() {
            let mut cache = LRUKCache::new(1);
            cache.insert(1, 10);
            assert_eq!(cache.len(), 1);
            assert_eq!(cache.get(&1), Some(&10));
            
            cache.insert(2, 20);
            assert_eq!(cache.len(), 1);
            assert_eq!(cache.get(&2), Some(&20));
            assert!(!cache.contains(&1));
        }

        #[test]
        fn test_zero_capacity_cache() {
            let mut cache = LRUKCache::new(0);
            cache.insert(1, 10);
            assert_eq!(cache.len(), 0);
            assert!(!cache.contains(&1));
        }

        #[test]
        fn test_k_equals_one() {
            // K=1 behaves like regular LRU
            let mut cache = LRUKCache::with_k(2, 1);
            
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(2));
            
            cache.insert(2, 20);
            thread::sleep(Duration::from_millis(2));
            
            // Access 1 to make it most recent
            cache.get(&1);
            thread::sleep(Duration::from_millis(2));
            
            // Cache: 1 (MRU), 2 (LRU)
            cache.insert(3, 30);
            
            assert!(cache.contains(&1));
            assert!(!cache.contains(&2)); // 2 was LRU
            assert!(cache.contains(&3));
        }

        #[test]
        fn test_k_larger_than_capacity() {
            let mut cache = LRUKCache::with_k(2, 5); // K=5, Cap=2
            
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(1)); // Ensure t1 < t2
            cache.insert(2, 20);
            
            // Access them a few times
            cache.get(&1);
            cache.get(&2);
            
            // Both have < K accesses. Eviction based on earliest access.
            // 1 was inserted first, then accessed.
            // 2 was inserted second, then accessed.
            // Timestamps:
            // 1: t1, t3
            // 2: t2, t4
            // Earliest access for 1 is t1. Earliest access for 2 is t2.
            // t1 < t2. So 1 should be evicted if we strictly follow "earliest access" rule for < K.
            
            cache.insert(3, 30);
            assert!(!cache.contains(&1));
            assert!(cache.contains(&2));
            assert!(cache.contains(&3));
        }

        #[test]
        fn test_same_key_rapid_accesses() {
            let mut cache = LRUKCache::with_k(5, 3);
            cache.insert(1, 10);
            for _ in 0..10 {
                cache.get(&1);
            }
            assert_eq!(cache.access_count(&1), Some(3)); // History capped at K
        }

        #[test]
        fn test_duplicate_key_insertion() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            cache.insert(1, 20);
            assert_eq!(cache.get(&1), Some(&20));
            assert_eq!(cache.len(), 1);
        }

        #[test]
        fn test_large_cache_operations() {
            let mut cache = LRUKCache::new(100);
            
            // Insert 0 first and wait to ensure it has the distinctly oldest timestamp
            cache.insert(0, 0);
            thread::sleep(Duration::from_millis(1));

            for i in 1..100 {
                cache.insert(i, i);
            }
            assert_eq!(cache.len(), 100);
            
            cache.insert(100, 100);
            assert_eq!(cache.len(), 100);
            assert!(!cache.contains(&0)); // 0 should be evicted (oldest, < K)
        }

        #[test]
        fn test_access_history_overflow() {
             let mut cache = LRUKCache::with_k(2, 3); // K=3
             cache.insert(1, 10);
             cache.get(&1);
             cache.get(&1);
             cache.get(&1);
             cache.get(&1);
             
             let history = cache.access_history(&1).unwrap();
             assert_eq!(history.len(), 3);
        }
    }

    // LRU-K-Specific Operations Tests
    mod lru_k_operations {
        use super::super::*;
        use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait, MutableCache};
        use std::thread;
        use std::time::Duration;

        #[test]
        fn test_pop_lru_k_basic() {
            let mut cache = LRUKCache::with_k(3, 2);
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(2));
            cache.insert(2, 20);
            
            // Both < K accesses. 1 is older.
            let popped = cache.pop_lru_k();
            assert_eq!(popped, Some((1, 10)));
            assert!(!cache.contains(&1));
            assert_eq!(cache.len(), 1);
        }

        #[test]
        fn test_peek_lru_k_basic() {
            let mut cache = LRUKCache::with_k(3, 2);
            cache.insert(1, 10);
            thread::sleep(Duration::from_millis(2));
            cache.insert(2, 20);
            
            // 1 should be the victim
            let peeked = cache.peek_lru_k();
            assert_eq!(peeked, Some((&1, &10)));
            assert!(cache.contains(&1));
            assert_eq!(cache.len(), 2);
        }

        #[test]
        fn test_k_value_retrieval() {
            let cache = LRUKCache::<i32, i32>::with_k(10, 4);
            assert_eq!(cache.k_value(), 4);
        }

        #[test]
        fn test_access_history_retrieval() {
            let mut cache = LRUKCache::with_k(10, 3);
            cache.insert(1, 10);
            cache.get(&1);
            
            let history = cache.access_history(&1).unwrap();
            assert_eq!(history.len(), 2);
            // Check if history is returned
        }

        #[test]
        fn test_access_count() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            assert_eq!(cache.access_count(&1), Some(1));
            cache.get(&1);
            assert_eq!(cache.access_count(&1), Some(2));
        }

        #[test]
        fn test_k_distance() {
             let mut cache = LRUKCache::with_k(5, 2);
             cache.insert(1, 10);
             
             // < K accesses, k_distance returns None
             assert_eq!(cache.k_distance(&1), None);
             
             cache.get(&1);
             // >= K accesses, returns Some(timestamp)
             assert!(cache.k_distance(&1).is_some());
        }

        #[test]
        fn test_touch_functionality() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            
            assert!(cache.touch(&1));
            assert_eq!(cache.access_count(&1), Some(2));
            
            assert!(!cache.touch(&999)); // Non-existent key
        }

        #[test]
        fn test_k_distance_rank() {
            let mut cache = LRUKCache::with_k(5, 2);
            
            cache.insert(1, 10); // < K
            thread::sleep(Duration::from_millis(2));
            cache.insert(2, 20); // < K
            thread::sleep(Duration::from_millis(2));
            
            // 1 is oldest < K. Rank should be 0 (most eligible for eviction).
            // 2 is newer < K. Rank should be 1.
            
            // The method `k_distance_rank` logic:
            // Sorts by: (< K accesses, earliest access), then (>= K accesses, K-distance).
            // Returns index in this sorted list.
            
            // 1: < K, t1
            // 2: < K, t2
            // t1 < t2, so 1 comes first.
            
            assert_eq!(cache.k_distance_rank(&1), Some(0));
            assert_eq!(cache.k_distance_rank(&2), Some(1));
            
            cache.get(&1); // 1 now has >= K (2 accesses).
            // 1: >= K, t1 (k-dist is t1? No, k-dist is k-th most recent access).
            // History for 1: [t1, t3]. K=2. k-th most recent is t1.
            // 2: < K, t2.
            
            // List sorted:
            // (< K items first): 2 (t2)
            // (>= K items next): 1 (t1)
            
            // So 2 should be rank 0. 1 should be rank 1.
            assert_eq!(cache.k_distance_rank(&2), Some(0));
            assert_eq!(cache.k_distance_rank(&1), Some(1));
        }

        #[test]
        fn test_pop_lru_k_empty_cache() {
            let mut cache = LRUKCache::<i32, i32>::new(5);
            assert_eq!(cache.pop_lru_k(), None);
        }

        #[test]
        fn test_peek_lru_k_empty_cache() {
            let cache = LRUKCache::<i32, i32>::new(5);
            assert_eq!(cache.peek_lru_k(), None);
        }

        #[test]
        fn test_lru_k_tie_breaking() {
            let mut cache = LRUKCache::with_k(5, 2);
            // Since we rely on SystemTime, true ties are hard to manufacture without mocking.
            // But logic says: if same K-distance, result is undefined/implementation dependent 
            // unless we have secondary sort key.
            // The implementation handles "same number of accesses" for < K by checking earliest access.
            // For >= K, it just compares K-distance.
            // If K-distances are equal, it picks one (the first one encountered or last).
            
            // We can test that it returns *something*.
            cache.insert(1, 10);
            cache.insert(2, 20);
            // Both < K.
            assert!(cache.peek_lru_k().is_some());
        }

        #[test]
        fn test_access_history_after_removal() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            cache.remove(&1);
            
            assert!(!cache.contains(&1));
            assert_eq!(cache.access_count(&1), None);
        }

        #[test]
        fn test_access_history_after_clear() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            cache.clear();
            
            assert_eq!(cache.len(), 0);
            assert_eq!(cache.access_count(&1), None);
        }
    }

    // State Consistency Tests
    mod state_consistency {
        use super::super::*;
        use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUKCacheTrait, MutableCache};

        #[test]
        fn test_cache_access_history_consistency() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            
            // Check if access history exists for inserted key
            assert!(cache.access_history(&1).is_some());
            
            // Check if access history is removed for removed key
            cache.remove(&1);
            assert!(cache.access_history(&1).is_none());
        }

        #[test]
        fn test_len_consistency() {
            let mut cache = LRUKCache::new(5);
            assert_eq!(cache.len(), 0);
            
            cache.insert(1, 10);
            assert_eq!(cache.len(), 1);
            
            cache.insert(2, 20);
            assert_eq!(cache.len(), 2);
            
            cache.remove(&1);
            assert_eq!(cache.len(), 1);
            
            cache.clear();
            assert_eq!(cache.len(), 0);
        }

        #[test]
        fn test_capacity_consistency() {
            let mut cache = LRUKCache::new(2);
            assert_eq!(cache.capacity(), 2);
            
            cache.insert(1, 10);
            cache.insert(2, 20);
            cache.insert(3, 30);
            
            assert_eq!(cache.len(), 2); // Should not exceed capacity
        }

        #[test]
        fn test_clear_resets_all_state() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            cache.insert(2, 20);
            
            cache.clear();
            
            assert_eq!(cache.len(), 0);
            assert!(!cache.contains(&1));
            assert!(!cache.contains(&2));
            assert!(cache.access_history(&1).is_none());
        }

        #[test]
        fn test_remove_consistency() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            
            let removed = cache.remove(&1);
            assert_eq!(removed, Some(10));
            assert!(!cache.contains(&1));
            assert!(cache.access_history(&1).is_none());
            
            let removed_again = cache.remove(&1);
            assert_eq!(removed_again, None);
        }

        #[test]
        fn test_eviction_consistency() {
            let mut cache = LRUKCache::new(1);
            cache.insert(1, 10);
            
            // Should evict 1
            cache.insert(2, 20);
            
            assert!(!cache.contains(&1));
            assert!(cache.contains(&2));
            assert!(cache.access_history(&1).is_none());
            assert!(cache.access_history(&2).is_some());
        }

        #[test]
        fn test_access_history_update_on_get() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            
            let count_before = cache.access_count(&1).unwrap();
            cache.get(&1);
            let count_after = cache.access_count(&1).unwrap();
            
            assert_eq!(count_after, count_before + 1);
        }

        #[test]
        fn test_invariants_after_operations() {
            let mut cache = LRUKCache::with_k(2, 2);
            cache.insert(1, 10);
            cache.insert(2, 20);
            
            // Invariant: len <= capacity
            assert!(cache.len() <= cache.capacity());
            
            // Invariant: history length <= K
            let h1 = cache.access_history(&1).unwrap();
            assert!(h1.len() <= 2);
            
            cache.get(&1);
            cache.get(&1);
             let h1_new = cache.access_history(&1).unwrap();
            assert!(h1_new.len() <= 2);
        }

        #[test]
        fn test_k_distance_calculation_consistency() {
             let mut cache = LRUKCache::with_k(5, 2);
             cache.insert(1, 10); // 1 access
             
             assert_eq!(cache.k_distance(&1), None);
             
             cache.get(&1); // 2 accesses
             assert!(cache.k_distance(&1).is_some());
        }

        #[test]
        fn test_timestamp_consistency() {
            let mut cache = LRUKCache::new(5);
            cache.insert(1, 10);
            
            let history = cache.access_history(&1).unwrap();
            let ts1 = history[0];
            
            std::thread::sleep(std::time::Duration::from_millis(1));
            cache.get(&1);
            
            let history_new = cache.access_history(&1).unwrap();
            let ts2 = history_new[0]; // Most recent
            
            assert!(ts2 > ts1);
        }
    }
}
