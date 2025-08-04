use crate::storage::disk::async_disk::cache::cache_traits::{
    CoreCache, LRUCacheTrait, MutableCache,
};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ptr::NonNull;

/// Node in the doubly-linked list for LRU tracking
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<NonNull<Node<K, V>>>,
    next: Option<NonNull<Node<K, V>>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Node {
            key,
            value,
            prev: None,
            next: None,
        }
    }
}

/// High-performance LRU Cache using HashMap + Doubly-Linked List
/// All operations are O(1): insert, get, remove, eviction
pub struct LRUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    map: HashMap<K, NonNull<Node<K, V>>>,
    head: Option<NonNull<Node<K, V>>>, // Most recently used
    tail: Option<NonNull<Node<K, V>>>, // Least recently used
    _phantom: PhantomData<(K, V)>,
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
            map: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
            _phantom: PhantomData,
        }
    }

    /// Move node to head (mark as most recently used) - O(1)
    unsafe fn move_to_head(&mut self, node: NonNull<Node<K, V>>) {
        unsafe {
            self.remove_from_list(node);
            self.add_to_head(node);
        }
    }

    /// Add node to head of list - O(1)
    unsafe fn add_to_head(&mut self, node: NonNull<Node<K, V>>) {
        unsafe {
            match self.head {
                Some(old_head) => {
                    (*node.as_ptr()).next = Some(old_head);
                    (*node.as_ptr()).prev = None;
                    (*old_head.as_ptr()).prev = Some(node);
                    self.head = Some(node);
                }
                None => {
                    // Empty list
                    (*node.as_ptr()).next = None;
                    (*node.as_ptr()).prev = None;
                    self.head = Some(node);
                    self.tail = Some(node);
                }
            }
        }
    }

    /// Remove node from doubly-linked list - O(1)
    unsafe fn remove_from_list(&mut self, node: NonNull<Node<K, V>>) {
        unsafe {
            let node_ref = &*node.as_ptr();

            match (node_ref.prev, node_ref.next) {
                (Some(prev), Some(next)) => {
                    // Middle node
                    (*prev.as_ptr()).next = Some(next);
                    (*next.as_ptr()).prev = Some(prev);
                }
                (Some(prev), None) => {
                    // Tail node
                    (*prev.as_ptr()).next = None;
                    self.tail = Some(prev);
                }
                (None, Some(next)) => {
                    // Head node
                    (*next.as_ptr()).prev = None;
                    self.head = Some(next);
                }
                (None, None) => {
                    // Only node
                    self.head = None;
                    self.tail = None;
                }
            }
        }
    }

    /// Remove tail node (least recently used) and return it - O(1)
    unsafe fn remove_tail(&mut self) -> Option<NonNull<Node<K, V>>> {
        self.tail.map(|tail_node| {
            unsafe {
                self.remove_from_list(tail_node);
            }
            tail_node
        })
    }

    /// Allocate a new node on the heap
    fn allocate_node(&self, key: K, value: V) -> NonNull<Node<K, V>> {
        let boxed = Box::new(Node::new(key, value));
        NonNull::from(Box::leak(boxed))
    }

    /// Deallocate a node from the heap
    unsafe fn deallocate_node(&self, node: NonNull<Node<K, V>>) {
        unsafe {
            let _ = Box::from_raw(node.as_ptr());
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
        if let Some(&existing_node) = self.map.get(&key) {
            // Update existing node - O(1)
            let old_value = unsafe {
                let node_ref = &mut *existing_node.as_ptr();
                std::mem::replace(&mut node_ref.value, value)
            };

            // Move to head (mark as most recently used) - O(1)
            unsafe {
                self.move_to_head(existing_node);
            }

            Some(old_value)
        } else {
            // Insert new node
            if self.map.len() >= self.capacity && self.capacity > 0 {
                // Evict LRU item - O(1)
                if let Some(tail_node) = unsafe { self.remove_tail() } {
                    let tail_key = unsafe { (*tail_node.as_ptr()).key.clone() };
                    self.map.remove(&tail_key);
                    unsafe {
                        self.deallocate_node(tail_node);
                    }
                }
            }

            // Allocate and insert new node - O(1)
            let new_node = self.allocate_node(key.clone(), value);
            self.map.insert(key, new_node);

            unsafe {
                self.add_to_head(new_node);
            }

            None
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&node) = self.map.get(key) {
            // Move to head (mark as most recently used) - O(1)
            unsafe {
                self.move_to_head(node);
                Some(&(*node.as_ptr()).value)
            }
        } else {
            None
        }
    }

    fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn clear(&mut self) {
        // Collect all nodes first to avoid borrow checker issues
        let nodes: Vec<_> = self.map.drain().map(|(_, node)| node).collect();

        // Now deallocate all nodes
        for node in nodes {
            unsafe {
                self.deallocate_node(node);
            }
        }

        self.head = None;
        self.tail = None;
    }
}

impl<K, V> MutableCache<K, V> for LRUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(node) = self.map.remove(key) {
            unsafe {
                let value = (*node.as_ptr()).value.clone();
                self.remove_from_list(node);
                self.deallocate_node(node);
                Some(value)
            }
        } else {
            None
        }
    }
}

impl<K, V> LRUCacheTrait<K, V> for LRUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn pop_lru(&mut self) -> Option<(K, V)> {
        self.tail.map(|tail_node| unsafe {
            let key = (*tail_node.as_ptr()).key.clone();
            let value = (*tail_node.as_ptr()).value.clone();

            self.map.remove(&key);
            self.remove_from_list(tail_node);
            self.deallocate_node(tail_node);

            (key, value)
        })
    }

    fn peek_lru(&self) -> Option<(&K, &V)> {
        self.tail.map(|tail_node| unsafe {
            let node_ref = &*tail_node.as_ptr();
            (&node_ref.key, &node_ref.value)
        })
    }

    fn touch(&mut self, key: &K) -> bool {
        if let Some(&node) = self.map.get(key) {
            unsafe {
                self.move_to_head(node);
            }
            true
        } else {
            false
        }
    }

    fn recency_rank(&self, key: &K) -> Option<usize> {
        if let Some(&target_node) = self.map.get(key) {
            let mut rank = 0;
            let mut current = self.head;

            // Walk from head (most recent) to find the target node
            while let Some(node) = current {
                unsafe {
                    if node == target_node {
                        return Some(rank);
                    }
                    current = (*node.as_ptr()).next;
                    rank += 1;
                }
            }
        }
        None
    }
}

// Proper cleanup when cache is dropped
impl<K, V> Drop for LRUCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, LRUCacheTrait};
    use std::sync::{Arc, Mutex};

    // ==============================================
    // CORRECTNESS TESTS MODULE
    // ==============================================
    mod correctness {
        // Basic LRU Behavior Tests
        mod basic_behavior {
            use super::super::*;

            #[test]
            fn test_basic_lru_insertion_and_retrieval() {
                let mut cache = LRUCache::new(3);

                // Test insertions
                assert_eq!(cache.insert(1, "one"), None);
                assert_eq!(cache.insert(2, "two"), None);
                assert_eq!(cache.insert(3, "three"), None);
                assert_eq!(cache.len(), 3);

                // Test retrievals
                assert_eq!(cache.get(&1), Some(&"one"));
                assert_eq!(cache.get(&2), Some(&"two"));
                assert_eq!(cache.get(&3), Some(&"three"));
                assert_eq!(cache.get(&4), None);

                // Test capacity enforcement
                assert_eq!(cache.insert(4, "four"), None); // Should evict least recently used
                assert_eq!(cache.len(), 3);
                assert_eq!(cache.get(&1), None); // 1 should be evicted (was LRU before insertion)
            }

            #[test]
            fn test_lru_eviction_order() {
                let mut cache = LRUCache::new(2);

                // Insert two items
                cache.insert(1, "one");
                cache.insert(2, "two");

                // Access first item to make it more recently used
                cache.get(&1);

                // Insert third item - should evict 2 (least recently used)
                cache.insert(3, "three");

                assert_eq!(cache.get(&1), Some(&"one")); // Should still be there
                assert_eq!(cache.get(&2), None); // Should be evicted
                assert_eq!(cache.get(&3), Some(&"three")); // Should be there
            }

            #[test]
            fn test_capacity_enforcement() {
                // TODO: Test that cache never exceeds capacity
            }

            #[test]
            fn test_update_existing_key() {
                // TODO: Test updating existing key updates recency
            }

            #[test]
            fn test_recency_tracking() {
                // TODO: Test that recency is correctly tracked and updated
            }

            #[test]
            fn test_key_operations_consistency() {
                // TODO: Test consistency between contains, get, and len operations
            }

            #[test]
            fn test_usage_list_consistency() {
                // TODO: Test that usage list stays consistent with cache contents
            }
        }

        // Edge Cases Tests
        mod edge_cases {
            #[test]
            fn test_empty_cache_operations() {
                // TODO: Test operations on empty cache
            }

            #[test]
            fn test_single_item_cache() {
                // TODO: Test cache with capacity of 1
            }

            #[test]
            fn test_zero_capacity_cache() {
                // TODO: Test cache with capacity of 0
            }

            #[test]
            fn test_same_key_multiple_accesses() {
                // TODO: Test accessing the same key multiple times
            }

            #[test]
            fn test_rapid_key_updates() {
                // TODO: Test rapid updates to the same key
            }

            #[test]
            fn test_duplicate_key_insertion() {
                // TODO: Test inserting the same key multiple times
            }

            #[test]
            fn test_large_cache_operations() {
                // TODO: Test operations on large capacity cache
            }

            #[test]
            fn test_usage_list_edge_cases() {
                // TODO: Test edge cases in usage list management
            }
        }

        // LRU-Specific Operations Tests
        mod lru_operations {
            #[test]
            fn test_pop_lru_basic() {
                // TODO: Test basic pop_lru functionality
            }

            #[test]
            fn test_peek_lru_basic() {
                // TODO: Test basic peek_lru functionality
            }

            #[test]
            fn test_touch_basic() {
                // TODO: Test touch() method for updating recency
            }

            #[test]
            fn test_recency_rank() {
                // TODO: Test recency_rank() method accuracy
            }

            #[test]
            fn test_pop_lru_empty_cache() {
                // TODO: Test pop_lru on empty cache
            }

            #[test]
            fn test_peek_lru_empty_cache() {
                // TODO: Test peek_lru on empty cache
            }

            #[test]
            fn test_touch_nonexistent_key() {
                // TODO: Test touch on non-existent key
            }

            #[test]
            fn test_recency_rank_nonexistent_key() {
                // TODO: Test recency_rank on non-existent key
            }

            #[test]
            fn test_lru_after_touch() {
                // TODO: Test LRU behavior after touch operations
            }

            #[test]
            fn test_recency_after_get() {
                // TODO: Test that get() updates recency correctly
            }
        }

        // State Consistency Tests
        mod state_consistency {
            #[test]
            fn test_cache_usage_list_consistency() {
                // TODO: Test that cache and usage list stay in sync
            }

            #[test]
            fn test_len_consistency() {
                // TODO: Test that len() always matches actual number of items
            }

            #[test]
            fn test_capacity_consistency() {
                // TODO: Test that capacity never changes and is respected
            }

            #[test]
            fn test_clear_resets_all_state() {
                // TODO: Test that clear() resets all internal state
            }

            #[test]
            fn test_remove_consistency() {
                // TODO: Test that remove operations maintain consistency
            }

            #[test]
            fn test_eviction_consistency() {
                // TODO: Test that evictions maintain consistency
            }

            #[test]
            fn test_recency_update_on_access() {
                // TODO: Test that all access methods update recency correctly
            }

            #[test]
            fn test_invariants_after_operations() {
                // TODO: Test that all invariants hold after various operations
            }

            #[test]
            fn test_usage_list_ordering() {
                // TODO: Test that usage list maintains correct ordering
            }
        }
    }

    // ==============================================
    // PERFORMANCE TESTS MODULE
    // ==============================================
    mod performance {
        use super::*;

        // Lookup Performance Tests
        mod lookup_performance {
            #[test]
            fn test_get_performance_with_recency_updates() {
                // TODO: Test get() performance with recency tracking overhead
            }

            #[test]
            fn test_contains_performance() {
                // TODO: Test contains() method performance
            }

            #[test]
            fn test_recency_rank_performance() {
                // TODO: Test recency_rank() method performance
            }

            #[test]
            fn test_peek_lru_performance() {
                // TODO: Test peek_lru() performance with large cache
            }

            #[test]
            fn test_cache_hit_vs_miss_performance() {
                // TODO: Compare performance of cache hits vs misses
            }

            #[test]
            fn test_touch_performance() {
                // TODO: Test touch() method performance
            }
        }

        // Insertion Performance Tests
        mod insertion_performance {
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
            fn test_insertion_with_usage_tracking() {
                // TODO: Test overhead of usage list maintenance during insertion
            }

            #[test]
            fn test_usage_list_update_performance() {
                // TODO: Test performance of usage list updates
            }
        }

        // Eviction Performance Tests
        mod eviction_performance {
            #[test]
            fn test_lru_eviction_performance() {
                // TODO: Test performance of finding and evicting LRU item
            }

            #[test]
            fn test_pop_lru_performance() {
                // TODO: Test pop_lru() method performance
            }

            #[test]
            fn test_eviction_with_frequent_access() {
                // TODO: Test eviction performance with frequently accessed items
            }

            #[test]
            fn test_usage_list_maintenance_overhead() {
                // TODO: Test overhead of maintaining usage list during evictions
            }
        }

        // Memory Efficiency Tests
        mod memory_efficiency {
            #[test]
            fn test_memory_overhead_of_usage_tracking() {
                // TODO: Test memory overhead of maintaining usage information
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

            #[test]
            fn test_usage_list_memory_efficiency() {
                // TODO: Test memory efficiency of usage list storage
            }
        }
    }

    // ==============================================
    // CONCURRENCY TESTS MODULE
    // ==============================================
    mod concurrency {
        use super::*;
        use std::sync::{Arc, Mutex};

        // Thread Safety Tests
        mod thread_safety {
            #[test]
            fn test_concurrent_insertions() {
                // TODO: Test multiple threads inserting concurrently
            }

            #[test]
            fn test_concurrent_gets() {
                // TODO: Test multiple threads getting values concurrently
            }

            #[test]
            fn test_concurrent_recency_operations() {
                // TODO: Test concurrent touch and recency tracking operations
            }

            #[test]
            fn test_concurrent_lru_operations() {
                // TODO: Test concurrent pop_lru and peek_lru operations
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

            #[test]
            fn test_concurrent_usage_list_updates() {
                // TODO: Test concurrent updates to usage list
            }
        }

        // Stress Testing
        mod stress_testing {
            use super::*;

            // Helper type for thread-safe testing
            type ThreadSafeLRUCache<K, V> = Arc<Mutex<LRUCache<K, V>>>;

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
            fn test_recency_tracking_stress() {
                // TODO: Test stress scenarios with heavy recency tracking
            }

            #[test]
            fn test_lru_eviction_under_stress() {
                // TODO: Test LRU eviction correctness under high stress
            }

            #[test]
            fn test_usage_list_stress() {
                // TODO: Test usage list performance under stress conditions
            }
        }
    }
}
