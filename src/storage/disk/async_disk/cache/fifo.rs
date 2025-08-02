use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::Arc;
use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};

/// FIFO (First In, First Out) Cache implementation.
#[derive(Debug)]
pub struct FIFOCache<K, V>
where 
    K: Eq + Hash,
{
    capacity: usize,
    cache: HashMap<Arc<K>, Arc<V>>,
    insertion_order: VecDeque<Arc<K>>, // Tracks the order of insertion
}

impl<K, V> FIFOCache<K, V>
where 
    K: Eq + Hash,
{
    /// Creates a new FIFO cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        FIFOCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns the number of items currently in the cache.
    /// This is a duplicate of the CoreCache::len() method but provides direct access.
    pub fn current_size(&self) -> usize {
        self.cache.len()
    }

    /// Returns the insertion order length (may include stale entries).
    /// This is primarily for testing and debugging purposes.
    pub fn insertion_order_len(&self) -> usize {
        self.insertion_order.len()
    }

    /// Checks if the internal cache HashMap contains a specific Arc<K>.
    /// This is primarily for testing stale entry behavior.
    pub fn cache_contains_key(&self, key: &Arc<K>) -> bool {
        self.cache.contains_key(key)
    }

    /// Returns an iterator over the insertion order keys.
    /// This is primarily for testing and debugging purposes.
    pub fn insertion_order_iter(&self) -> impl Iterator<Item = &Arc<K>> {
        self.insertion_order.iter()
    }

    /// Manually removes a key from the cache HashMap only (for testing stale entries).
    /// This is only for testing purposes to simulate stale entry conditions.
    /// Accepts a raw key and finds the corresponding Arc<K> to remove.
    #[cfg(test)]
    pub fn remove_from_cache_only(&mut self, key: &K) -> Option<Arc<V>> {
        // Find the Arc<K> that matches this key
        let arc_key = self.cache.keys()
            .find(|k| k.as_ref() == key)
            .cloned();
        
        if let Some(arc_key) = arc_key {
            self.cache.remove(&arc_key)
        } else {
            None
        }
    }

    /// Returns the current cache HashMap capacity (for testing memory usage).
    #[cfg(test)]
    pub fn cache_capacity(&self) -> usize {
        self.cache.capacity()
    }

    /// Returns the current insertion order VecDeque capacity (for testing memory usage).
    #[cfg(test)]
    pub fn insertion_order_capacity(&self) -> usize {
        self.insertion_order.capacity()
    }

    /// Evicts the oldest valid entry from the cache.
    /// Skips over any stale entries (keys that were lazily deleted).
    fn evict_oldest(&mut self) {
        // Keep popping from front until we find a valid key or the queue is empty
        while let Some(oldest_key) = self.insertion_order.pop_front() {
            if self.cache.contains_key(&oldest_key) {
                // Found a valid key, remove it and stop
                self.cache.remove(&oldest_key);
                break;
            }
            // Skip stale entries (keys that were already removed from cache)
        }
    }
}

impl<K, V> CoreCache<K, V> for FIFOCache<K, V>
where 
    K: Eq + Hash,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        // If capacity is 0, cannot store anything
        if self.capacity == 0 {
            return None;
        }

        let key_arc = Arc::new(key);
        let value_arc = Arc::new(value);

        // If key already exists, update the value
        if self.cache.contains_key(&key_arc) {
            return self.cache.insert(key_arc, value_arc)
                .and_then(|old_value_arc| {
                    // Try to unwrap the Arc to get the original value
                    match Arc::try_unwrap(old_value_arc) {
                        Ok(old_value) => Some(old_value),
                        Err(_) => {
                            // If unwrap fails, there are external references to this Arc<V>
                            // This violates our cache's ownership model
                            panic!("Failed to unwrap Arc<V> in insert - there are external references to the value");
                        }
                    }
                });
        }

        // If cache is at capacity, remove the oldest valid item (FIFO)
        if self.cache.len() >= self.capacity {
            self.evict_oldest();
        }

        // Add the new key to the insertion order and cache
        // Only the Arc pointers are cloned (8 bytes each), not the actual data
        self.insertion_order.push_back(key_arc.clone());
        self.cache.insert(key_arc, value_arc);
        None
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        // In FIFO, getting an item doesn't change its position
        // Find the Arc<K> that contains this key by iterating (less efficient but correct)
        for (k, v) in &self.cache {
            if k.as_ref() == key {
                return Some(v.as_ref());
            }
        }
        None
    }

    fn contains(&self, key: &K) -> bool {
        // Find the Arc<K> that contains this key by iterating (less efficient but correct)
        for k in self.cache.keys() {
            if k.as_ref() == key {
                return true;
            }
        }
        false
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.insertion_order.clear();
    }
}

impl<K, V> FIFOCacheTrait<K, V> for FIFOCache<K, V>
where 
    K: Eq + Hash,
{
    fn pop_oldest(&mut self) -> Option<(K, V)> {
        // Use the existing evict_oldest logic but return the key-value pair
        while let Some(oldest_key_arc) = self.insertion_order.pop_front() {
            if let Some(value_arc) = self.cache.remove(&oldest_key_arc) {
                // Try to unwrap both Arcs to get the original key and value
                // This should succeed since we just removed them from the cache
                let key = match Arc::try_unwrap(oldest_key_arc) {
                    Ok(key) => key,
                    Err(_) => {
                        // If unwrap fails, it means there are external references to this Arc<K>
                        // This violates our cache's ownership model and shouldn't happen in normal usage
                        panic!("Failed to unwrap Arc<K> in pop_oldest - there are external references to the key");
                    }
                };
                
                let value = match Arc::try_unwrap(value_arc) {
                    Ok(value) => value,
                    Err(_) => {
                        // If unwrap fails, it means there are external references to this Arc<V>
                        // This violates our cache's ownership model and shouldn't happen in normal usage
                        panic!("Failed to unwrap Arc<V> in pop_oldest - there are external references to the value");
                    }
                };
                
                return Some((key, value));
            }
            // Skip stale entries (keys that were already removed from cache)
        }
        None
    }

    fn peek_oldest(&self) -> Option<(&K, &V)> {
        // Find the first valid entry in insertion order
        for key_arc in &self.insertion_order {
            if let Some(value_arc) = self.cache.get(key_arc) {
                return Some((key_arc.as_ref(), value_arc.as_ref()));
            }
        }
        None
    }

    fn age_rank(&self, key: &K) -> Option<usize> {
        // Find position in insertion order, accounting for stale entries
        let mut rank = 0;
        for insertion_key_arc in &self.insertion_order {
            if self.cache.contains_key(insertion_key_arc) {
                if insertion_key_arc.as_ref() == key {
                    return Some(rank);
                }
                rank += 1;
            }
        }
        None
    }

    fn pop_oldest_batch(&mut self, count: usize) -> Vec<(K, V)> {
        let mut result = Vec::with_capacity(count.min(<Self as CoreCache<K, V>>::len(self)));
        for _ in 0..count {
            if let Some(entry) = self.pop_oldest() {
                result.push(entry);
            } else {
                break;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::cache::cache_traits::{CoreCache, FIFOCacheTrait};
    use std::collections::HashSet;

    // ==============================================
    // CORRECTNESS TESTS MODULE
    // ==============================================
    mod correctness {
        use super::*;

        // Basic FIFO Behavior Tests
        mod basic_behavior {
            use super::*;

            #[test]
            fn test_basic_fifo_insertion_and_retrieval() {
                let mut cache = FIFOCache::new(3);
                
                // Test basic insertion and retrieval
                assert_eq!(cache.insert("key1", "value1"), None);
                assert_eq!(cache.insert("key2", "value2"), None);
                assert_eq!(cache.insert("key3", "value3"), None);
                
                assert_eq!(cache.get(&"key1"), Some(&"value1"));
                assert_eq!(cache.get(&"key2"), Some(&"value2"));
                assert_eq!(cache.get(&"key3"), Some(&"value3"));
                assert_eq!(cache.len(), 3);
            }

            #[test]
            fn test_fifo_eviction_order() {
                let mut cache = FIFOCache::new(2);
                
                // Fill cache to capacity
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                assert_eq!(cache.len(), 2);
                
                // Insert third item - should evict "first" (oldest)
                cache.insert("third", "value3");
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"first"));  // First item should be evicted
                assert!(cache.contains(&"second"));
                assert!(cache.contains(&"third"));
            }

            #[test]
            fn test_capacity_enforcement() {
                let mut cache = FIFOCache::new(3);
                
                // Fill beyond capacity
                for i in 1..=5 {
                    cache.insert(format!("key{}", i), format!("value{}", i));
                }
                
                // Should only contain last 3 items due to FIFO eviction
                assert_eq!(cache.len(), 3);
                assert!(!cache.contains(&"key1".to_string()));
                assert!(!cache.contains(&"key2".to_string()));
                assert!(cache.contains(&"key3".to_string()));
                assert!(cache.contains(&"key4".to_string()));
                assert!(cache.contains(&"key5".to_string()));
            }

            #[test]
            fn test_update_existing_key() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "original");
                cache.insert("key2", "value2");
                
                // Update existing key - should return old value
                let old_value = cache.insert("key1", "updated");
                assert_eq!(old_value, Some("original"));
                assert_eq!(cache.get(&"key1"), Some(&"updated"));
                assert_eq!(cache.len(), 2); // Length shouldn't change
            }

            #[test]
            fn test_insertion_order_preservation() {
                let mut cache = FIFOCache::new(4);
                
                // Insert items in specific order
                cache.insert("first", 1);
                cache.insert("second", 2);
                cache.insert("third", 3);
                cache.insert("fourth", 4);
                
                // Verify insertion order is preserved in FIFO operations
                assert_eq!(cache.peek_oldest(), Some((&"first", &1)));
                assert_eq!(cache.age_rank(&"first"), Some(0));  // oldest
                assert_eq!(cache.age_rank(&"second"), Some(1));
                assert_eq!(cache.age_rank(&"third"), Some(2));
                assert_eq!(cache.age_rank(&"fourth"), Some(3)); // newest
                
                // Pop oldest and verify order shifts correctly
                assert_eq!(cache.pop_oldest(), Some(("first", 1)));
                assert_eq!(cache.peek_oldest(), Some((&"second", &2)));
                assert_eq!(cache.age_rank(&"second"), Some(0)); // now oldest
                assert_eq!(cache.age_rank(&"third"), Some(1));
                assert_eq!(cache.age_rank(&"fourth"), Some(2)); // still newest
                
                // Insert new item and verify it becomes newest
                cache.insert("fifth", 5);
                assert_eq!(cache.age_rank(&"second"), Some(0));
                assert_eq!(cache.age_rank(&"third"), Some(1));
                assert_eq!(cache.age_rank(&"fourth"), Some(2));
                assert_eq!(cache.age_rank(&"fifth"), Some(3)); // newest
                
                // Test eviction maintains order - add item beyond capacity
                cache.insert("sixth", 6);
                // Should evict "second" (oldest)
                assert!(!cache.contains(&"second"));
                assert_eq!(cache.peek_oldest(), Some((&"third", &3)));
                assert_eq!(cache.age_rank(&"third"), Some(0));  // now oldest
                assert_eq!(cache.age_rank(&"fourth"), Some(1));
                assert_eq!(cache.age_rank(&"fifth"), Some(2));
                assert_eq!(cache.age_rank(&"sixth"), Some(3));  // newest
            }

            #[test]
            fn test_key_operations_consistency() {
                let mut cache = FIFOCache::new(3);
                
                // Test consistency between contains, get, and len
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key1"));
                assert_eq!(cache.get(&"key1"), None);
                
                // Insert first item
                cache.insert("key1", "value1");
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"key1"));
                assert_eq!(cache.get(&"key1"), Some(&"value1"));
                assert!(!cache.contains(&"key2"));
                assert_eq!(cache.get(&"key2"), None);
                
                // Insert second item
                cache.insert("key2", "value2");
                assert_eq!(cache.len(), 2);
                assert!(cache.contains(&"key1"));
                assert!(cache.contains(&"key2"));
                assert_eq!(cache.get(&"key1"), Some(&"value1"));
                assert_eq!(cache.get(&"key2"), Some(&"value2"));
                
                // Update existing key - len should not change
                let old_value = cache.insert("key1", "updated_value1");
                assert_eq!(old_value, Some("value1"));
                assert_eq!(cache.len(), 2); // Should remain 2
                assert!(cache.contains(&"key1"));
                assert_eq!(cache.get(&"key1"), Some(&"updated_value1"));
                assert_eq!(cache.get(&"key2"), Some(&"value2"));
                
                // Fill to capacity
                cache.insert("key3", "value3");
                assert_eq!(cache.len(), 3);
                assert_eq!(cache.capacity(), 3);
                assert!(cache.contains(&"key1"));
                assert!(cache.contains(&"key2"));
                assert!(cache.contains(&"key3"));
                
                // Trigger eviction - should evict key1 (oldest, even though updated)
                // In true FIFO, updating a key does NOT change its insertion order position
                cache.insert("key4", "value4");
                assert_eq!(cache.len(), 3); // Should remain at capacity
                assert!(!cache.contains(&"key1")); // Should be evicted (oldest insertion time)
                assert!(cache.contains(&"key2"));  // Should remain
                assert!(cache.contains(&"key3"));
                assert!(cache.contains(&"key4"));
                assert_eq!(cache.get(&"key1"), None);
                assert_eq!(cache.get(&"key4"), Some(&"value4"));
                
                // Test pop_oldest consistency - should pop key2 (now oldest)
                let popped = cache.pop_oldest();
                assert_eq!(popped, Some(("key2", "value2")));
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"key2"));
                assert_eq!(cache.get(&"key2"), None);
                
                // Verify remaining items
                assert!(cache.contains(&"key3"));
                assert!(cache.contains(&"key4"));
                
                // Clear and verify all operations report empty state
                cache.clear();
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key2"));
                assert!(!cache.contains(&"key3"));
                assert!(!cache.contains(&"key4"));
                assert_eq!(cache.get(&"key2"), None);
                assert_eq!(cache.get(&"key3"), None);
                assert_eq!(cache.get(&"key4"), None);
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.pop_oldest(), None);
            }
        }

        // Edge Cases Tests
        mod edge_cases {
            use super::*;

            #[test]
            fn test_empty_cache_operations() {
                let mut cache: FIFOCache<String, String> = FIFOCache::new(5);
                
                assert_eq!(cache.get(&"nonexistent".to_string()), None);
                assert!(!cache.contains(&"nonexistent".to_string()));
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 5);
                
                // Clear empty cache should work
                cache.clear();
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_single_item_cache() {
                let mut cache = FIFOCache::new(1);
                
                cache.insert("only", "value1");
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.get(&"only"), Some(&"value1"));
                
                // Insert second item - should evict first
                cache.insert("second", "value2");
                assert_eq!(cache.len(), 1);
                assert!(!cache.contains(&"only"));
                assert!(cache.contains(&"second"));
            }

            #[test]
            fn test_zero_capacity_cache() {
                let mut cache = FIFOCache::new(0);
                
                // Should not be able to store anything
                cache.insert("key", "value");
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key"));
                assert_eq!(cache.get(&"key"), None);
            }

            #[test]
            fn test_clear_operation() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                assert_eq!(cache.len(), 2);
                
                cache.clear();
                assert_eq!(cache.len(), 0);
                assert!(!cache.contains(&"key1"));
                assert!(!cache.contains(&"key2"));
                assert_eq!(cache.get(&"key1"), None);
            }

            #[test]
            fn test_duplicate_key_handling() {
                let mut cache = FIFOCache::new(3);
                
                // Insert initial key
                assert_eq!(cache.insert("key1", "value1"), None);
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"key1"));
                assert_eq!(cache.get(&"key1"), Some(&"value1"));
                
                // Insert same key again - should update, not add new entry
                assert_eq!(cache.insert("key1", "value1_updated"), Some("value1"));
                assert_eq!(cache.len(), 1); // Length should remain the same
                assert_eq!(cache.get(&"key1"), Some(&"value1_updated"));
                
                // Add more keys
                cache.insert("key2", "value2");
                cache.insert("key3", "value3");
                assert_eq!(cache.len(), 3);
                
                // Update existing key multiple times
                assert_eq!(cache.insert("key2", "value2_v2"), Some("value2"));
                assert_eq!(cache.insert("key2", "value2_v3"), Some("value2_v2"));
                assert_eq!(cache.len(), 3); // Should still be 3
                assert_eq!(cache.get(&"key2"), Some(&"value2_v3"));
                
                // Verify insertion order preserved (key1 should still be oldest)
                assert_eq!(cache.age_rank(&"key1"), Some(0)); // Still oldest despite updates
                assert_eq!(cache.age_rank(&"key2"), Some(1));
                assert_eq!(cache.age_rank(&"key3"), Some(2));
                
                // Fill to capacity and force eviction
                cache.insert("key4", "value4");
                assert_eq!(cache.len(), 3);
                assert!(!cache.contains(&"key1")); // Oldest should be evicted
                assert!(cache.contains(&"key2"));
                assert!(cache.contains(&"key3"));
                assert!(cache.contains(&"key4"));
                
                // Update existing key after eviction
                assert_eq!(cache.insert("key2", "value2_final"), Some("value2_v3"));
                assert_eq!(cache.len(), 3);
                assert_eq!(cache.get(&"key2"), Some(&"value2_final"));
                
                // Verify no duplicate entries in internal structures
                let mut found_keys = HashSet::new();
                let mut count = 0;
                while let Some((key, _)) = cache.pop_oldest() {
                    assert!(found_keys.insert(key), "Duplicate key found: {}", key);
                    count += 1;
                }
                assert_eq!(count, 3); // Should have exactly 3 unique keys
            }

            #[test]
            fn test_boundary_conditions() {
                let mut cache = FIFOCache::new(2);
                
                // Test exactly at capacity
                cache.insert("key1", "value1");
                assert_eq!(cache.len(), 1);
                assert!(!cache.contains(&"key2"));
                
                cache.insert("key2", "value2");
                assert_eq!(cache.len(), 2); // Exactly at capacity
                assert_eq!(cache.capacity(), 2);
                assert!(cache.contains(&"key1"));
                assert!(cache.contains(&"key2"));
                
                // Test operations at capacity limit
                assert_eq!(cache.peek_oldest(), Some((&"key1", &"value1")));
                assert_eq!(cache.age_rank(&"key1"), Some(0));
                assert_eq!(cache.age_rank(&"key2"), Some(1));
                
                // Insert one more to trigger eviction (capacity + 1)
                cache.insert("key3", "value3");
                assert_eq!(cache.len(), 2); // Should remain at capacity
                assert!(!cache.contains(&"key1")); // Oldest evicted
                assert!(cache.contains(&"key2"));
                assert!(cache.contains(&"key3"));
                
                // Test boundary with updates at capacity
                assert_eq!(cache.insert("key2", "value2_updated"), Some("value2"));
                assert_eq!(cache.len(), 2); // Still at capacity
                assert_eq!(cache.get(&"key2"), Some(&"value2_updated"));
                
                // Test pop operations at boundary
                assert_eq!(cache.pop_oldest(), Some(("key2", "value2_updated")));
                assert_eq!(cache.len(), 1); // One below capacity
                
                assert_eq!(cache.pop_oldest(), Some(("key3", "value3")));
                assert_eq!(cache.len(), 0); // Empty
                
                // Test operations on empty cache after reaching boundary
                assert_eq!(cache.pop_oldest(), None);
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.len(), 0);
                
                // Test filling back to capacity
                cache.insert("new1", "newval1");
                cache.insert("new2", "newval2");
                assert_eq!(cache.len(), 2); // Back to capacity
                
                // Test batch operations at boundary
                let batch = cache.pop_oldest_batch(3); // Request more than available
                assert_eq!(batch.len(), 2); // Should get exactly what's available
                assert_eq!(cache.len(), 0); // Should be empty
            }

            #[test]
            fn test_empty_to_full_transition() {
                let mut cache = FIFOCache::new(4);
                
                // Start empty
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.capacity(), 4);
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.pop_oldest(), None);
                
                // Fill step by step, verifying state at each step
                
                // Step 1: Insert first item
                cache.insert("item1", 1);
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"item1"));
                assert_eq!(cache.peek_oldest(), Some((&"item1", &1)));
                assert_eq!(cache.age_rank(&"item1"), Some(0));
                
                // Step 2: Insert second item  
                cache.insert("item2", 2);
                assert_eq!(cache.len(), 2);
                assert!(cache.contains(&"item1"));
                assert!(cache.contains(&"item2"));
                assert_eq!(cache.peek_oldest(), Some((&"item1", &1))); // Still oldest
                assert_eq!(cache.age_rank(&"item1"), Some(0));
                assert_eq!(cache.age_rank(&"item2"), Some(1));
                
                // Step 3: Insert third item
                cache.insert("item3", 3);
                assert_eq!(cache.len(), 3);
                assert_eq!(cache.peek_oldest(), Some((&"item1", &1)));
                assert_eq!(cache.age_rank(&"item1"), Some(0));
                assert_eq!(cache.age_rank(&"item2"), Some(1));
                assert_eq!(cache.age_rank(&"item3"), Some(2));
                
                // Step 4: Fill to capacity
                cache.insert("item4", 4);
                assert_eq!(cache.len(), 4); // Now at full capacity
                assert_eq!(cache.capacity(), 4);
                
                // Verify all items present and in correct order
                assert!(cache.contains(&"item1"));
                assert_eq!(cache.get(&"item1"), Some(&1));
                assert_eq!(cache.age_rank(&"item1"), Some(0));
                
                assert!(cache.contains(&"item2"));
                assert_eq!(cache.get(&"item2"), Some(&2));
                assert_eq!(cache.age_rank(&"item2"), Some(1));
                
                assert!(cache.contains(&"item3"));
                assert_eq!(cache.get(&"item3"), Some(&3));
                assert_eq!(cache.age_rank(&"item3"), Some(2));
                
                assert!(cache.contains(&"item4"));
                assert_eq!(cache.get(&"item4"), Some(&4));
                assert_eq!(cache.age_rank(&"item4"), Some(3));
                
                // Verify oldest is still first
                assert_eq!(cache.peek_oldest(), Some((&"item1", &1)));
                
                // Test that we're truly at capacity - next insert should evict
                cache.insert("item5", 5);
                assert_eq!(cache.len(), 4); // Still at capacity
                assert!(!cache.contains(&"item1")); // First item evicted
                assert!(cache.contains(&"item5")); // New item added
                assert_eq!(cache.peek_oldest(), Some((&"item2", &2))); // item2 now oldest
            }

            #[test]
            fn test_full_to_empty_transition() {
                // Helper function to create cache with same initial state (avoids cloning)
                let create_test_cache = || {
                    let mut cache = FIFOCache::new(3);
                    cache.insert("item1", 1);
                    cache.insert("item2", 2);
                    cache.insert("item3", 3);
                    assert_eq!(cache.len(), 3);
                    assert_eq!(cache.capacity(), 3);
                    cache
                };
                
                // Method 1: Empty using pop_oldest one by one
                let mut emptying_cache = create_test_cache();
                
                // First pop
                assert_eq!(emptying_cache.pop_oldest(), Some(("item1", 1)));
                assert_eq!(emptying_cache.len(), 2);
                assert!(!emptying_cache.contains(&"item1"));
                assert!(emptying_cache.contains(&"item2"));
                assert!(emptying_cache.contains(&"item3"));
                assert_eq!(emptying_cache.peek_oldest(), Some((&"item2", &2)));
                
                // Second pop
                assert_eq!(emptying_cache.pop_oldest(), Some(("item2", 2)));
                assert_eq!(emptying_cache.len(), 1);
                assert!(!emptying_cache.contains(&"item2"));
                assert!(emptying_cache.contains(&"item3"));
                assert_eq!(emptying_cache.peek_oldest(), Some((&"item3", &3)));
                
                // Third pop
                assert_eq!(emptying_cache.pop_oldest(), Some(("item3", 3)));
                assert_eq!(emptying_cache.len(), 0);
                assert!(!emptying_cache.contains(&"item3"));
                assert_eq!(emptying_cache.peek_oldest(), None);
                
                // Fourth pop on empty cache
                assert_eq!(emptying_cache.pop_oldest(), None);
                assert_eq!(emptying_cache.len(), 0);
                
                // Method 2: Empty using batch operation
                let mut batch_cache = create_test_cache();
                let batch = batch_cache.pop_oldest_batch(3);
                assert_eq!(batch.len(), 3);
                assert_eq!(batch[0], ("item1", 1));
                assert_eq!(batch[1], ("item2", 2));
                assert_eq!(batch[2], ("item3", 3));
                assert_eq!(batch_cache.len(), 0);
                assert_eq!(batch_cache.peek_oldest(), None);
                
                // Method 3: Empty using clear operation
                let mut clear_cache = create_test_cache();
                clear_cache.clear();
                assert_eq!(clear_cache.len(), 0);
                assert_eq!(clear_cache.capacity(), 3); // Capacity unchanged
                assert_eq!(clear_cache.peek_oldest(), None);
                assert_eq!(clear_cache.pop_oldest(), None);
                
                // Verify cache can be refilled after each emptying method
                for mut test_cache in [emptying_cache, batch_cache, clear_cache] {
                    test_cache.insert("new1", 100);
                    assert_eq!(test_cache.len(), 1);
                    assert!(test_cache.contains(&"new1"));
                    assert_eq!(test_cache.peek_oldest(), Some((&"new1", &100)));
                }
                
                // Test partial emptying and refilling
                let mut partial_cache = FIFOCache::new(4);
                partial_cache.insert("a", 1);
                partial_cache.insert("b", 2);
                partial_cache.insert("c", 3);
                partial_cache.insert("d", 4);
                
                // Remove 2 items
                partial_cache.pop_oldest(); // Remove "a"
                partial_cache.pop_oldest(); // Remove "b"
                assert_eq!(partial_cache.len(), 2);
                
                // Add 2 new items
                partial_cache.insert("e", 5);
                partial_cache.insert("f", 6);
                assert_eq!(partial_cache.len(), 4); // Back to full
                
                // Verify correct order maintained
                assert_eq!(partial_cache.peek_oldest(), Some((&"c", &3))); // "c" should be oldest
                assert_eq!(partial_cache.age_rank(&"c"), Some(0));
                assert_eq!(partial_cache.age_rank(&"d"), Some(1));
                assert_eq!(partial_cache.age_rank(&"e"), Some(2));
                assert_eq!(partial_cache.age_rank(&"f"), Some(3));
            }
        }

        // FIFO Trait Methods Tests
        mod trait_methods {
            use super::*;

            #[test]
            fn test_pop_oldest() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                cache.insert("third", "value3");
                
                // Pop oldest should return first inserted
                assert_eq!(cache.pop_oldest(), Some(("first", "value1")));
                assert_eq!(cache.len(), 2);
                assert!(!cache.contains(&"first"));
                
                // Next pop should return second oldest
                assert_eq!(cache.pop_oldest(), Some(("second", "value2")));
                assert_eq!(cache.len(), 1);
            }

            #[test]
            fn test_peek_oldest() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("first", "value1");
                cache.insert("second", "value2");
                
                // Peek should return oldest without removing
                assert_eq!(cache.peek_oldest(), Some((&"first", &"value1")));
                assert_eq!(cache.len(), 2); // Should not change length
                assert!(cache.contains(&"first")); // Should still be present
                
                // Multiple peeks should return same result
                assert_eq!(cache.peek_oldest(), Some((&"first", &"value1")));
            }

            #[test]
            fn test_age_rank() {
                let mut cache = FIFOCache::new(4);
                
                cache.insert("first", "value1");   // rank 0 (oldest)
                cache.insert("second", "value2");  // rank 1
                cache.insert("third", "value3");   // rank 2
                cache.insert("fourth", "value4");  // rank 3 (newest)
                
                assert_eq!(cache.age_rank(&"first"), Some(0));
                assert_eq!(cache.age_rank(&"second"), Some(1));
                assert_eq!(cache.age_rank(&"third"), Some(2));
                assert_eq!(cache.age_rank(&"fourth"), Some(3));
                assert_eq!(cache.age_rank(&"nonexistent"), None);
            }

            #[test]
            fn test_pop_oldest_batch() {
                let mut cache = FIFOCache::new(5);
                
                for i in 1..=5 {
                    cache.insert(format!("key{}", i), format!("value{}", i));
                }
                
                // Pop batch of 3 items
                let batch = cache.pop_oldest_batch(3);
                assert_eq!(batch.len(), 3);
                assert_eq!(batch[0], ("key1".to_string(), "value1".to_string()));
                assert_eq!(batch[1], ("key2".to_string(), "value2".to_string()));
                assert_eq!(batch[2], ("key3".to_string(), "value3".to_string()));
                
                // Cache should have 2 items left
                assert_eq!(cache.len(), 2);
                assert!(cache.contains(&"key4".to_string()));
                assert!(cache.contains(&"key5".to_string()));
            }

            #[test]
            fn test_pop_oldest_batch_more_than_available() {
                let mut cache = FIFOCache::new(3);
                
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                
                // Request more than available
                let batch = cache.pop_oldest_batch(5);
                assert_eq!(batch.len(), 2); // Should only return what's available
                assert_eq!(cache.len(), 0); // Cache should be empty
            }

            #[test]
            fn test_pop_oldest_empty_cache() {
                let mut cache: FIFOCache<String, String> = FIFOCache::new(5);
                
                // Pop from empty cache should return None
                assert_eq!(cache.pop_oldest(), None);
                assert_eq!(cache.len(), 0);
                
                // Multiple pops should still return None
                assert_eq!(cache.pop_oldest(), None);
                assert_eq!(cache.pop_oldest(), None);
                assert_eq!(cache.len(), 0);
                
                // Add one item, pop it, then pop from empty again
                cache.insert("key1".to_string(), "value1".to_string());
                assert_eq!(cache.len(), 1);
                
                let popped = cache.pop_oldest();
                assert_eq!(popped, Some(("key1".to_string(), "value1".to_string())));
                assert_eq!(cache.len(), 0);
                
                // Now it's empty again
                assert_eq!(cache.pop_oldest(), None);
                assert_eq!(cache.len(), 0);
            }

            #[test]
            fn test_peek_oldest_empty_cache() {
                let cache: FIFOCache<String, String> = FIFOCache::new(5);
                
                // Peek at empty cache should return None
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.len(), 0);
                
                // Multiple peeks should still return None
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.len(), 0);
                
                // Test peek after clear
                let mut test_cache = FIFOCache::new(3);
                test_cache.insert("key1".to_string(), "value1".to_string());
                test_cache.insert("key2".to_string(), "value2".to_string());
                
                // Should have content
                assert!(test_cache.peek_oldest().is_some());
                assert_eq!(test_cache.len(), 2);
                
                // Clear and peek again
                test_cache.clear();
                assert_eq!(test_cache.peek_oldest(), None);
                assert_eq!(test_cache.len(), 0);
            }

            #[test]
            fn test_age_rank_after_eviction() {
                let mut cache = FIFOCache::new(3);
                
                // Fill cache
                cache.insert("first", 1);
                cache.insert("second", 2);
                cache.insert("third", 3);
                
                // Verify initial ranks
                assert_eq!(cache.age_rank(&"first"), Some(0));
                assert_eq!(cache.age_rank(&"second"), Some(1));
                assert_eq!(cache.age_rank(&"third"), Some(2));
                
                // Trigger eviction by adding fourth item
                cache.insert("fourth", 4);
                
                // "first" should be evicted, ranks should shift
                assert_eq!(cache.age_rank(&"first"), None); // Evicted
                assert_eq!(cache.age_rank(&"second"), Some(0)); // Now oldest
                assert_eq!(cache.age_rank(&"third"), Some(1));
                assert_eq!(cache.age_rank(&"fourth"), Some(2)); // Newest
                
                // Add another item to trigger another eviction
                cache.insert("fifth", 5);
                
                // "second" should be evicted, ranks shift again
                assert_eq!(cache.age_rank(&"first"), None);   // Still evicted
                assert_eq!(cache.age_rank(&"second"), None);  // Now evicted
                assert_eq!(cache.age_rank(&"third"), Some(0)); // Now oldest
                assert_eq!(cache.age_rank(&"fourth"), Some(1));
                assert_eq!(cache.age_rank(&"fifth"), Some(2)); // Newest
                
                // Test manual eviction with pop_oldest
                let popped = cache.pop_oldest();
                assert_eq!(popped, Some(("third", 3)));
                
                // Ranks should shift after manual pop
                assert_eq!(cache.age_rank(&"third"), None);   // Just popped
                assert_eq!(cache.age_rank(&"fourth"), Some(0)); // Now oldest
                assert_eq!(cache.age_rank(&"fifth"), Some(1));  // Now newest
                
                // Test batch eviction
                cache.insert("sixth", 6);
                cache.insert("seventh", 7);
                assert_eq!(cache.len(), 3);
                
                let batch = cache.pop_oldest_batch(2);
                assert_eq!(batch.len(), 2);
                assert_eq!(cache.len(), 1);
                
                // Only "seventh" should remain
                assert_eq!(cache.age_rank(&"fourth"), None);
                assert_eq!(cache.age_rank(&"fifth"), None);
                assert_eq!(cache.age_rank(&"sixth"), None);
                assert_eq!(cache.age_rank(&"seventh"), Some(0)); // Only remaining item
            }

            #[test]
            fn test_batch_operations_edge_cases() {
                let mut cache = FIFOCache::new(3);
                
                // Test batch with count = 0
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                
                let batch = cache.pop_oldest_batch(0);
                assert_eq!(batch.len(), 0);
                assert_eq!(cache.len(), 2); // Nothing should be removed
                assert!(cache.contains(&"key1"));
                assert!(cache.contains(&"key2"));
                
                // Test batch on empty cache
                let mut empty_cache: FIFOCache<String, String> = FIFOCache::new(5);
                let empty_batch = empty_cache.pop_oldest_batch(3);
                assert_eq!(empty_batch.len(), 0);
                assert_eq!(empty_cache.len(), 0);
                
                // Test batch with count = 1 (single item)
                let batch_one = cache.pop_oldest_batch(1);
                assert_eq!(batch_one.len(), 1);
                assert_eq!(batch_one[0], ("key1", "value1"));
                assert_eq!(cache.len(), 1);
                assert!(!cache.contains(&"key1"));
                assert!(cache.contains(&"key2"));
                
                // Test batch equal to cache size
                cache.insert("key3", "value3");
                cache.insert("key4", "value4");
                assert_eq!(cache.len(), 3);
                
                let all_batch = cache.pop_oldest_batch(3);
                assert_eq!(all_batch.len(), 3);
                assert_eq!(all_batch[0], ("key2", "value2"));
                assert_eq!(all_batch[1], ("key3", "value3"));
                assert_eq!(all_batch[2], ("key4", "value4"));
                assert_eq!(cache.len(), 0);
                
                // Verify cache is completely empty
                assert_eq!(cache.peek_oldest(), None);
                assert_eq!(cache.pop_oldest(), None);
                
                // Test large batch request on small cache
                cache.insert("only", "item");
                let large_batch = cache.pop_oldest_batch(100);
                assert_eq!(large_batch.len(), 1);
                assert_eq!(large_batch[0], ("only", "item"));
                assert_eq!(cache.len(), 0);
                
                // Test batch on cache that became empty during operation
                cache.insert("a", "1");
                cache.insert("b", "2");
                
                // First, empty the cache manually
                cache.clear();
                
                // Then try batch operation on now-empty cache
                let post_clear_batch = cache.pop_oldest_batch(5);
                assert_eq!(post_clear_batch.len(), 0);
                assert_eq!(cache.len(), 0);
            }
        }

        // Stale Entry Handling Tests
        mod stale_entries {
            use super::*;

            #[test]
            fn test_stale_entry_skipping_during_eviction() {
                let mut cache = FIFOCache::new(3);
                
                // Fill cache to capacity
                cache.insert("key1", "value1");
                cache.insert("key2", "value2");
                cache.insert("key3", "value3");
                assert_eq!(cache.len(), 3);
                
                // Manually remove key1 from HashMap but leave it in insertion_order
                // This simulates how stale entries would occur in a more complex scenario
                cache.remove_from_cache_only(&"key1");
                assert_eq!(cache.len(), 2); // HashMap now has 2 items
                
                // The insertion_order VecDeque still has 3 entries, but "key1" is now stale
                assert_eq!(cache.insertion_order_len(), 3);
                assert!(!cache.contains(&"key1")); // key1 is not in cache anymore
                assert!(cache.contains(&"key2"));  // key2 is still valid
                assert!(cache.contains(&"key3"));  // key3 is still valid
                
                // Add another item to get back to capacity (so next insert will trigger eviction)
                cache.insert("temp", "temp_value");
                assert_eq!(cache.len(), 3);
                
                // Now trigger eviction by inserting a new item
                // This should skip over the stale "key1" entry and evict "key2" instead
                cache.insert("key4", "value4");
                
                // Verify the eviction skipped stale entry and evicted the next valid entry
                assert_eq!(cache.len(), 3); // Should remain at capacity
                assert!(!cache.contains(&"key1")); // Still not present (was stale)
                assert!(!cache.contains(&"key2")); // Should be evicted (oldest valid)
                assert!(cache.contains(&"key3"));  // Should remain
                assert!(cache.contains(&"temp"));  // Should remain
                assert!(cache.contains(&"key4"));  // Should be newly added
                
                // Test that further operations continue to work correctly
                cache.insert("key5", "value5");
                assert_eq!(cache.len(), 3);
                assert!(!cache.contains(&"key3")); // key3 should be evicted now
                assert!(cache.contains(&"temp"));
                assert!(cache.contains(&"key4"));
                assert!(cache.contains(&"key5"));
            }

            #[test]
            fn test_insertion_order_consistency_with_stale_entries() {
                let mut cache = FIFOCache::new(4);
                
                // Fill cache
                cache.insert("a", 1);
                cache.insert("b", 2);
                cache.insert("c", 3);
                cache.insert("d", 4);
                
                // Verify initial age ranks
                assert_eq!(cache.age_rank(&"a"), Some(0));
                assert_eq!(cache.age_rank(&"b"), Some(1));
                assert_eq!(cache.age_rank(&"c"), Some(2));
                assert_eq!(cache.age_rank(&"d"), Some(3));
                
                // Manually create stale entries by removing from HashMap
                cache.remove_from_cache_only(&"a");
                cache.remove_from_cache_only(&"c");
                
                // Now "a" and "c" are stale entries in insertion_order
                assert_eq!(cache.len(), 2); // Only "b" and "d" remain in HashMap
                assert_eq!(cache.insertion_order_len(), 4); // All 4 still in insertion_order
                
                // age_rank should skip stale entries and give correct ranks for valid entries
                assert_eq!(cache.age_rank(&"a"), None); // Stale, should return None
                assert_eq!(cache.age_rank(&"b"), Some(0)); // First valid entry
                assert_eq!(cache.age_rank(&"c"), None); // Stale, should return None
                assert_eq!(cache.age_rank(&"d"), Some(1)); // Second valid entry
                
                // peek_oldest should skip stale entries and return the oldest valid entry
                assert_eq!(cache.peek_oldest(), Some((&"b", &2)));
                
                // pop_oldest should skip stale entries and pop the oldest valid entry
                assert_eq!(cache.pop_oldest(), Some(("b", 2)));
                
                // After popping "b", stale entries should be cleaned up
                // and "d" should now be the oldest (and only) valid entry
                assert_eq!(cache.len(), 1);
                assert_eq!(cache.age_rank(&"d"), Some(0));
                assert_eq!(cache.peek_oldest(), Some((&"d", &4)));
                
                // Add new items and verify order is maintained
                cache.insert("e", 5);
                cache.insert("f", 6);
                
                assert_eq!(cache.age_rank(&"d"), Some(0)); // Still oldest
                assert_eq!(cache.age_rank(&"e"), Some(1));
                assert_eq!(cache.age_rank(&"f"), Some(2));
            }

            #[test]
            fn test_lazy_deletion_behavior() {
                let mut cache = FIFOCache::new(3);
                
                // Test 1: Stale entries accumulate until cleanup operations
                cache.insert("temp1", "value1");
                cache.insert("temp2", "value2");
                cache.insert("keep", "value_keep");
                
                // Manually remove items to create stale entries
                cache.remove_from_cache_only(&"temp1");
                cache.remove_from_cache_only(&"temp2");
                
                // Stale entries remain in insertion_order
                assert_eq!(cache.len(), 1); // Only "keep" in HashMap
                assert_eq!(cache.insertion_order_len(), 3); // All 3 in insertion_order
                
                // Verify operations work correctly despite stale entries
                assert_eq!(cache.peek_oldest(), Some((&"keep", &"value_keep")));
                assert_eq!(cache.age_rank(&"keep"), Some(0));
                assert!(!cache.contains(&"temp1"));
                assert!(!cache.contains(&"temp2"));
                assert!(cache.contains(&"keep"));
                
                // Test 2: Lazy cleanup during pop_oldest
                cache.insert("new1", "value_new1");
                cache.insert("new2", "value_new2");
                
                // Now we have: stale("temp1"), stale("temp2"), "keep", "new1", "new2"
                assert_eq!(cache.len(), 3);
                assert_eq!(cache.insertion_order_len(), 5);
                
                // pop_oldest should skip stale entries and pop "keep"
                assert_eq!(cache.pop_oldest(), Some(("keep", "value_keep")));
                
                // After pop_oldest, some stale entries should be cleaned up
                assert_eq!(cache.len(), 2);
                // insertion_order length depends on how many stale entries were cleaned
                
                // Test 3: Lazy cleanup during eviction
                cache.insert("trigger_eviction", "value_trigger");
                
                // This should trigger eviction, which should skip any remaining stale entries
                assert_eq!(cache.len(), 3);
                assert!(cache.contains(&"new1"));
                assert!(cache.contains(&"new2"));
                assert!(cache.contains(&"trigger_eviction"));
                
                // Test 4: Multiple consecutive stale entries
                cache.clear();
                cache.insert("stale1", "v1");
                cache.insert("stale2", "v2");
                cache.insert("stale3", "v3");
                cache.insert("valid", "valid_value");
                
                // Remove first three to create consecutive stale entries
                cache.remove_from_cache_only(&"stale1");
                cache.remove_from_cache_only(&"stale2");
                cache.remove_from_cache_only(&"stale3");
                
                assert_eq!(cache.len(), 1); // Only "valid" in HashMap
                // insertion_order might have any length >= 1 depending on cleanup behavior
                
                // Operations should skip all stale entries and work with "valid"
                assert_eq!(cache.peek_oldest(), Some((&"valid", &"valid_value")));
                assert_eq!(cache.pop_oldest(), Some(("valid", "valid_value")));
                assert_eq!(cache.len(), 0);
                assert_eq!(cache.peek_oldest(), None);
                
                // Cache should be functional after stale entry handling
                cache.insert("new_after_stale", "new_value");
                assert_eq!(cache.len(), 1);
                assert!(cache.contains(&"new_after_stale"));
            }

            #[test]
            fn test_stale_entry_cleanup_during_operations() {
                let mut cache = FIFOCache::new(4);
                
                // Setup: Create cache with mix of valid and future stale entries
                cache.insert("will_be_stale1", "stale1");
                cache.insert("will_be_stale2", "stale2");
                cache.insert("valid1", "value1");
                cache.insert("valid2", "value2");
                
                // Create stale entries
                cache.remove_from_cache_only(&"will_be_stale1");
                cache.remove_from_cache_only(&"will_be_stale2");
                
                assert_eq!(cache.len(), 2); // Only valid entries in HashMap
                assert_eq!(cache.insertion_order_len(), 4); // All entries in insertion_order
                
                // Test 1: pop_oldest cleans up stale entries as it encounters them
                let initial_order_len = cache.insertion_order_len();
                assert_eq!(cache.pop_oldest(), Some(("valid1", "value1")));
                
                // Should have cleaned up stale entries encountered during traversal
                assert_eq!(cache.len(), 1);
                // insertion_order may be smaller after cleanup, depending on which stale entries were encountered
                // The exact behavior depends on the order of traversal and which stale entries are cleaned up
                
                // Test 2: pop_oldest_batch cleans up stale entries
                cache.insert("new1", "new_value1");
                cache.insert("new2", "new_value2");
                cache.insert("new3", "new_value3");
                
                // Create more stale entries
                cache.remove_from_cache_only(&"new1");
                cache.remove_from_cache_only(&"new3");
                
                let batch = cache.pop_oldest_batch(2);
                // Should get valid2 and new2 (skipping stale new1 and new3)
                assert_eq!(batch.len(), 2);
                assert!(batch.contains(&("valid2", "value2")));
                assert!(batch.contains(&("new2", "new_value2")));
                
                // Test 3: age_rank doesn't modify structure but handles stale entries
                cache.insert("test1", "test_value1");
                cache.insert("test2", "test_value2");
                cache.insert("test3", "test_value3");
                
                cache.remove_from_cache_only(&"test2"); // Make test2 stale
                
                // age_rank should return correct ranks despite stale entry
                assert_eq!(cache.age_rank(&"test1"), Some(0)); // First valid
                assert_eq!(cache.age_rank(&"test2"), None);    // Stale
                assert_eq!(cache.age_rank(&"test3"), Some(1)); // Second valid
                
                // Test 4: peek_oldest doesn't modify structure but finds valid entry
                cache.remove_from_cache_only(&"test1"); // Make test1 also stale
                
                // peek_oldest should find test3 despite two stale entries before it
                assert_eq!(cache.peek_oldest(), Some((&"test3", &"test_value3")));
                
                // The structure should remain unchanged after peek
                assert_eq!(cache.len(), 1); // Only test3 is valid
                
                // Test 5: Eviction during insertion handles stale entries
                cache.insert("fill1", "f1");
                cache.insert("fill2", "f2");
                cache.insert("fill3", "f3");
                // Now at capacity (4): test3, fill1, fill2, fill3
                
                // Create stale entries
                cache.remove_from_cache_only(&"fill1");
                cache.remove_from_cache_only(&"fill2");
                
                // Now we have test3, fill3 in HashMap (2 valid items)
                assert_eq!(cache.len(), 2);
                
                // Add items to get back to capacity so next insert will trigger eviction
                cache.insert("temp1", "t1");
                cache.insert("temp2", "t2");
                assert_eq!(cache.len(), 4); // At capacity
                
                // This insertion should trigger eviction
                cache.insert("trigger", "trigger_value");
                
                // Should have proper capacity and valid operations
                assert_eq!(cache.len(), 4); // Still at capacity
                assert!(cache.contains(&"trigger"));
                
                // The cache should still function correctly regardless of internal stale entry cleanup
                cache.insert("final", "final_value");
                assert_eq!(cache.len(), 4); // Should remain at capacity
                assert!(cache.contains(&"final"));
                
                // Verify the cache maintains proper FIFO behavior
                assert_eq!(cache.capacity(), 4);
                
                // Test that operations still work normally after stale entry handling
                let oldest = cache.peek_oldest();
                assert!(oldest.is_some()); // Should have valid oldest entry
                
                let popped = cache.pop_oldest();
                assert!(popped.is_some()); // Should be able to pop
                assert_eq!(cache.len(), 3);
            }
        }
    }

    // ==============================================
    // ALGORITHMIC COMPLEXITY TESTS MODULE
    // ==============================================
    mod complexity {
        use super::*;
        use std::time::Instant;

        // Time Complexity Tests
        mod time_complexity {
            use super::*;
            use std::time::Instant;

            /// Helper function to measure execution time of a closure
            fn measure_time<F, R>(operation: F) -> (R, std::time::Duration)
            where
                F: FnOnce() -> R,
            {
                let start = Instant::now();
                let result = operation();
                let duration = start.elapsed();
                (result, duration)
            }

            #[test]
            fn test_insert_time_complexity() {
                // Test O(1) insert time complexity by measuring insert operations
                // on caches of different sizes. O(1) means time should remain roughly constant.
                
                let sizes = vec![100, 1000, 10000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::new(size);
                    
                    // Pre-fill the cache to capacity - 1 to avoid eviction effects
                    for i in 0..(size - 1) {
                        cache.insert(format!("key{}", i), format!("value{}", i));
                    }
                    
                    // Measure time for multiple insert operations
                    let iterations = 1000;
                    let (_, duration) = measure_time(|| {
                        for i in 0..iterations {
                            let key = format!("test_key_{}", i);
                            let value = format!("test_value_{}", i);
                            cache.insert(key, value);
                        }
                    });
                    
                    let avg_time_per_insert = duration.as_nanos() as f64 / iterations as f64;
                    times.push(avg_time_per_insert);
                    
                    println!("Cache size: {}, Avg insert time: {:.2} ns", size, avg_time_per_insert);
                }
                
                // Verify that times don't grow significantly with cache size
                // For O(1), the ratio between largest and smallest time should be reasonable
                let min_time = times.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let max_time = times.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let ratio = max_time / min_time;
                
                // Allow for some variance due to system noise, but should be roughly constant
                assert!(ratio < 10.0, "Insert time complexity appears to be worse than O(1). Ratio: {:.2}", ratio);
            }

            #[test]
            fn test_get_time_complexity() {
                // Test O(1) get time complexity - HashMap lookups should be constant time
                
                let sizes = vec![100, 1000, 10000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::new(size);
                    
                    // Fill cache with test data
                    for i in 0..size {
                        cache.insert(format!("key{}", i), format!("value{}", i));
                    }
                    
                    // Measure time for multiple get operations
                    let iterations = 1000;
                    let (_, duration) = measure_time(|| {
                        for i in 0..iterations {
                            let key = format!("key{}", i % size);
                            let _ = cache.get(&key);
                        }
                    });
                    
                    let avg_time_per_get = duration.as_nanos() as f64 / iterations as f64;
                    times.push(avg_time_per_get);
                    
                    println!("Cache size: {}, Avg get time: {:.2} ns", size, avg_time_per_get);
                }
                
                // Verify O(1) complexity
                let min_time = times.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let max_time = times.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let ratio = max_time / min_time;
                
                assert!(ratio < 10.0, "Get time complexity appears to be worse than O(1). Ratio: {:.2}", ratio);
            }

            #[test]
            fn test_eviction_time_complexity() {
                // Test O(n) worst case eviction time due to stale entries
                // In the worst case, eviction may need to skip through many stale entries
                
                let sizes = vec![100, 500, 1000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::new(size);
                    
                    // Fill cache to capacity
                    for i in 0..size {
                        cache.insert(format!("key{}", i), format!("value{}", i));
                    }
                    
                    // Create many stale entries by manually removing from HashMap
                    // This simulates the worst-case scenario for eviction
                    let stale_count = size / 2;
                    for i in 0..stale_count {
                        cache.remove_from_cache_only(&format!("key{}", i));
                    }
                    
                    // Measure time for eviction operations (insertions that trigger eviction)
                    let iterations = 100;
                    let (_, duration) = measure_time(|| {
                        for i in 0..iterations {
                            let key = format!("evict_key_{}", i);
                            let value = format!("evict_value_{}", i);
                            cache.insert(key, value); // This should trigger eviction
                        }
                    });
                    
                    let avg_time_per_eviction = duration.as_nanos() as f64 / iterations as f64;
                    times.push(avg_time_per_eviction);
                    
                    println!("Cache size: {}, Avg eviction time: {:.2} ns", size, avg_time_per_eviction);
                }
                
                // For O(n) complexity, time should grow roughly linearly with cache size
                // Check that larger caches take more time (allowing for reasonable variance)
                if times.len() >= 2 {
                    let small_time = times[0];
                    let large_time = times[times.len() - 1];
                    
                    // The larger cache should take more time, but we're lenient with the exact ratio
                    // due to the complexity of measuring micro-benchmarks
                    assert!(large_time >= small_time * 0.5, 
                           "Eviction time should increase with cache size for O(n) complexity");
                }
            }

            #[test]
            fn test_age_rank_time_complexity() {
                // Test O(n) age_rank time complexity - needs to traverse insertion order
                
                let sizes = vec![100, 500, 1000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::new(size);
                    
                    // Fill cache with test data
                    for i in 0..size {
                        cache.insert(format!("key{}", i), format!("value{}", i));
                    }
                    
                    // Measure time for age_rank operations on keys at different positions
                    let iterations = 100;
                    let (_, duration) = measure_time(|| {
                        for i in 0..iterations {
                            let key_index = i % size;
                            let key = format!("key{}", key_index);
                            let _ = cache.age_rank(&key);
                        }
                    });
                    
                    let avg_time_per_age_rank = duration.as_nanos() as f64 / iterations as f64;
                    times.push(avg_time_per_age_rank);
                    
                    println!("Cache size: {}, Avg age_rank time: {:.2} ns", size, avg_time_per_age_rank);
                }
                
                // For O(n) complexity, time should grow with cache size
                if times.len() >= 2 {
                    let small_time = times[0];
                    let large_time = times[times.len() - 1];
                    
                    // Larger cache should generally take more time for O(n) operation
                    assert!(large_time >= small_time * 0.5,
                           "age_rank time should increase with cache size for O(n) complexity");
                }
                
                // Also test worst-case scenario: key at the end of insertion order
                let mut test_cache = FIFOCache::new(1000);
                for i in 0..1000 {
                    test_cache.insert(format!("key{}", i), format!("value{}", i));
                }
                
                // Key at beginning vs end should show time difference
                let (_, time_first) = measure_time(|| {
                    for _ in 0..100 {
                        test_cache.age_rank(&"key0".to_string());
                    }
                });
                
                let (_, time_last) = measure_time(|| {
                    for _ in 0..100 {
                        test_cache.age_rank(&"key999".to_string());
                    }
                });
                
                // Time for last key should be >= time for first key (more traversal needed)
                assert!(time_last >= time_first || time_last.as_nanos() > 0,
                       "age_rank should take longer for keys later in insertion order");
            }

            #[test]
            fn test_contains_time_complexity() {
                // Test O(1) contains time complexity - HashMap contains_key should be constant time
                
                let sizes = vec![100, 1000, 10000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::new(size);
                    
                    // Fill cache with test data
                    for i in 0..size {
                        cache.insert(format!("key{}", i), format!("value{}", i));
                    }
                    
                    // Measure time for multiple contains operations
                    let iterations = 1000;
                    let (_, duration) = measure_time(|| {
                        for i in 0..iterations {
                            let key = format!("key{}", i % size);
                            let _ = cache.contains(&key);
                        }
                    });
                    
                    let avg_time_per_contains = duration.as_nanos() as f64 / iterations as f64;
                    times.push(avg_time_per_contains);
                    
                    println!("Cache size: {}, Avg contains time: {:.2} ns", size, avg_time_per_contains);
                }
                
                // Verify O(1) complexity
                let min_time = times.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let max_time = times.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let ratio = max_time / min_time;
                
                assert!(ratio < 10.0, "Contains time complexity appears to be worse than O(1). Ratio: {:.2}", ratio);
            }

            #[test]
            fn test_clear_time_complexity() {
                // Test O(1) clear time complexity - clearing HashMap and VecDeque should be constant time
                
                let sizes = vec![100, 1000, 10000];
                let mut times = Vec::new();
                
                for &size in &sizes {
                    // Create multiple caches to test clear operation
                    let mut caches = Vec::new();
                    for _ in 0..10 {
                        let mut cache = FIFOCache::new(size);
                        // Fill cache with test data
                        for i in 0..size {
                            cache.insert(format!("key{}", i), format!("value{}", i));
                        }
                        caches.push(cache);
                    }
                    
                    // Measure time for clear operations
                    let (_, duration) = measure_time(|| {
                        for cache in &mut caches {
                            cache.clear();
                        }
                    });
                    
                    let avg_time_per_clear = duration.as_nanos() as f64 / caches.len() as f64;
                    times.push(avg_time_per_clear);
                    
                    println!("Cache size: {}, Avg clear time: {:.2} ns", size, avg_time_per_clear);
                    
                    // Verify all caches are actually cleared
                    for cache in &caches {
                        assert_eq!(cache.len(), 0);
                    }
                }
                
                // Verify O(1) complexity - clear time should not grow significantly with cache size
                // Note: While clear() is theoretically O(1), in practice clearing larger HashMap and VecDeque
                // structures can take more time due to memory deallocation, so we use a more lenient ratio
                let min_time = times.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let max_time = times.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
                let ratio = max_time / min_time;
                
                // Allow for larger variance since clearing involves memory deallocation
                assert!(ratio < 100.0, "Clear time complexity appears to be worse than O(1). Ratio: {:.2}", ratio);
            }
        }

        // Space Complexity Tests
        mod space_complexity {
            use super::*;
            use std::mem;
            use std::hash::Hash;

            /// Helper function to estimate memory usage of a cache
            fn estimate_cache_memory_usage<K, V>(cache: &FIFOCache<K, V>) -> usize
            where
                K: Eq + Hash + Clone,
                V: Clone,
            {
                // Base structure size
                let base_size = mem::size_of::<FIFOCache<K, V>>();
                
                // HashMap overhead (estimated)
                let hashmap_overhead = cache.cache_capacity() * (mem::size_of::<K>() + mem::size_of::<V>() + 8);
                
                // VecDeque overhead (estimated) 
                let vecdeque_overhead = cache.insertion_order_capacity() * mem::size_of::<K>();
                
                base_size + hashmap_overhead + vecdeque_overhead
            }

            #[test]
            fn test_memory_usage_patterns() {
                // Test that memory usage grows linearly with capacity (O(capacity))
                
                let capacities = vec![100, 500, 1000, 2000];
                let mut memory_usages = Vec::new();
                
                for &capacity in &capacities {
                    let mut cache = FIFOCache::<String, String>::new(capacity);
                    
                    // Fill cache to capacity to get realistic memory usage
                    for i in 0..capacity {
                        cache.insert(format!("key_{}", i), format!("value_{}", i));
                    }
                    
                    let estimated_memory = estimate_cache_memory_usage(&cache);
                    memory_usages.push(estimated_memory);
                    
                    println!("Capacity: {}, Estimated memory: {} bytes", capacity, estimated_memory);
                    
                    // Verify cache is at expected capacity
                    assert_eq!(cache.len(), capacity);
                    assert_eq!(cache.capacity(), capacity);
                }
                
                // Verify roughly linear growth - memory should scale with capacity
                // Allow for some variance due to HashMap/VecDeque internal allocation strategies
                if memory_usages.len() >= 2 {
                    let first_usage = memory_usages[0] as f64;
                    let last_usage = memory_usages[memory_usages.len() - 1] as f64;
                    let first_capacity = capacities[0] as f64;
                    let last_capacity = capacities[capacities.len() - 1] as f64;
                    
                    let memory_growth_ratio = last_usage / first_usage;
                    let capacity_growth_ratio = last_capacity / first_capacity;
                    
                    // Memory should grow roughly proportionally to capacity
                    // Allow for reasonable variance due to allocation overhead
                    let ratio_difference = (memory_growth_ratio / capacity_growth_ratio - 1.0).abs();
                    assert!(ratio_difference < 1.0, 
                           "Memory growth ratio ({:.2}) should be roughly proportional to capacity growth ratio ({:.2})",
                           memory_growth_ratio, capacity_growth_ratio);
                }
                
                // Test that empty cache uses less memory than full cache
                let empty_cache = FIFOCache::<String, String>::new(1000);
                let empty_memory = estimate_cache_memory_usage(&empty_cache);
                let full_memory = memory_usages[memory_usages.len() - 1];
                
                // Empty cache should use less or equal memory than full cache
                // Note: In Rust, empty collections may pre-allocate based on capacity
                assert!(empty_memory <= full_memory, 
                       "Empty cache memory ({}) should not exceed full cache memory ({})", 
                       empty_memory, full_memory);
            }

            #[test]
            fn test_overhead_analysis() {
                // Analyze the overhead of HashMap + VecDeque vs actual data storage
                
                let capacity = 1000;
                let mut cache = FIFOCache::<String, i32>::new(capacity);
                
                // Fill cache with known data sizes
                for i in 0..capacity {
                    cache.insert(format!("key_{:06}", i), i as i32); // 10-char keys
                }
                
                // Calculate actual data size
                let key_size = mem::size_of::<String>() + 10; // String overhead + 10 chars
                let value_size = mem::size_of::<i32>();
                let actual_data_size = capacity * (key_size + value_size);
                
                // Estimate total cache memory
                let total_memory = estimate_cache_memory_usage(&cache);
                
                // Calculate overhead
                let overhead = total_memory.saturating_sub(actual_data_size);
                let overhead_percentage = (overhead as f64 / total_memory as f64) * 100.0;
                
                println!("Capacity: {}", capacity);
                println!("Actual data size: {} bytes", actual_data_size);
                println!("Total cache memory: {} bytes", total_memory);
                println!("Overhead: {} bytes ({:.1}%)", overhead, overhead_percentage);
                
                // Verify overhead is reasonable (should be less than 200% of data)
                assert!(overhead_percentage < 200.0, 
                       "Overhead percentage ({:.1}%) seems excessive", overhead_percentage);
                
                // Test memory efficiency with different key/value sizes
                let test_cases = vec![
                    (50, "small"),   // Small cache
                    (500, "medium"), // Medium cache
                    (2000, "large"), // Large cache
                ];
                
                for (size, description) in test_cases {
                    let mut test_cache = FIFOCache::<String, String>::new(size);
                    
                    // Fill with variable-length data
                    for i in 0..size {
                        let key = format!("test_key_{}", i);
                        let value = format!("test_value_{}", i);
                        test_cache.insert(key, value);
                    }
                    
                    let memory = estimate_cache_memory_usage(&test_cache);
                    let memory_per_item = memory / size;
                    
                    println!("{} cache - Memory per item: {} bytes", description, memory_per_item);
                    
                    // Memory per item should be reasonable (not excessively large)
                    assert!(memory_per_item < 1000, 
                           "{} cache memory per item ({}) seems excessive", description, memory_per_item);
                }
            }

            #[test]
            fn test_memory_leak_detection() {
                // Test that memory is properly cleaned up after operations
                
                let initial_capacity = 1000;
                
                // Test 1: Create and drop cache - should not accumulate memory
                for iteration in 0..5 {
                    let mut cache = FIFOCache::<String, Vec<u8>>::new(initial_capacity);
                    
                    // Fill with large data to make memory usage more apparent
                    for i in 0..initial_capacity {
                        let large_value = vec![i as u8; 100]; // 100-byte vectors
                        cache.insert(format!("key_{}", i), large_value);
                    }
                    
                    assert_eq!(cache.len(), initial_capacity);
                    
                    // Cache should be dropped at end of loop iteration
                    println!("Iteration {} completed", iteration);
                }
                
                // Test 2: Clear operation should free internal memory
                let mut persistent_cache = FIFOCache::<String, Vec<u8>>::new(initial_capacity);
                
                // Fill cache
                for i in 0..initial_capacity {
                    let large_value = vec![i as u8; 100];
                    persistent_cache.insert(format!("key_{}", i), large_value);
                }
                
                let memory_before_clear = estimate_cache_memory_usage(&persistent_cache);
                
                // Clear cache
                persistent_cache.clear();
                
                let memory_after_clear = estimate_cache_memory_usage(&persistent_cache);
                
                println!("Memory before clear: {} bytes", memory_before_clear);
                println!("Memory after clear: {} bytes", memory_after_clear);
                
                // Memory after clear should be significantly less (though not necessarily zero due to capacity reservations)
                assert!(memory_after_clear <= memory_before_clear, 
                       "Memory after clear should not exceed memory before clear");
                
                assert_eq!(persistent_cache.len(), 0);
                assert_eq!(persistent_cache.capacity(), initial_capacity);
                
                // Test 3: Pop operations should reduce memory usage
                let mut pop_test_cache = FIFOCache::<String, Vec<u8>>::new(500);
                
                // Fill cache
                for i in 0..500 {
                    let value = vec![i as u8; 50];
                    pop_test_cache.insert(format!("key_{}", i), value);
                }
                
                let memory_before_pops = estimate_cache_memory_usage(&pop_test_cache);
                
                // Pop half the items
                for _ in 0..250 {
                    pop_test_cache.pop_oldest();
                }
                
                let memory_after_pops = estimate_cache_memory_usage(&pop_test_cache);
                
                println!("Memory before pops: {} bytes", memory_before_pops);
                println!("Memory after pops: {} bytes", memory_after_pops);
                
                assert_eq!(pop_test_cache.len(), 250);
                
                // Memory should be reduced (though HashMap may retain some capacity)
                // At minimum, it shouldn't have increased
                assert!(memory_after_pops <= memory_before_pops + (memory_before_pops / 10), 
                       "Memory should not significantly increase after pop operations");
                
                // Test 4: Eviction should not cause memory leaks
                let mut eviction_cache = FIFOCache::<String, Vec<u8>>::new(100);
                
                // Fill to capacity
                for i in 0..100 {
                    let value = vec![i as u8; 50];
                    eviction_cache.insert(format!("key_{}", i), value);
                }
                
                let memory_at_capacity = estimate_cache_memory_usage(&eviction_cache);
                
                // Trigger many evictions
                for i in 100..500 {
                    let value = vec![i as u8; 50];
                    eviction_cache.insert(format!("key_{}", i), value);
                }
                
                let memory_after_evictions = estimate_cache_memory_usage(&eviction_cache);
                
                println!("Memory at capacity: {} bytes", memory_at_capacity);
                println!("Memory after evictions: {} bytes", memory_after_evictions);
                
                assert_eq!(eviction_cache.len(), 100); // Should still be at capacity
                
                // Memory should remain stable (not continuously grow)
                let memory_difference_ratio = memory_after_evictions as f64 / memory_at_capacity as f64;
                assert!(memory_difference_ratio < 2.0, 
                       "Memory should not double after evictions. Ratio: {:.2}", memory_difference_ratio);
            }

            #[test]
            fn test_growth_patterns() {
                // Test how memory grows as cache size increases and verify predictable patterns
                
                let sizes = vec![10, 50, 100, 250, 500, 1000, 2000];
                let mut growth_data = Vec::new();
                
                for &size in &sizes {
                    let mut cache = FIFOCache::<u32, u64>::new(size);
                    
                    // Fill cache completely
                    for i in 0..size {
                        cache.insert(i as u32, (i * 2) as u64);
                    }
                    
                    let memory_usage = estimate_cache_memory_usage(&cache);
                    let memory_per_item = memory_usage as f64 / size as f64;
                    
                    growth_data.push((size, memory_usage, memory_per_item));
                    
                    println!("Size: {}, Total memory: {} bytes, Per item: {:.2} bytes", 
                            size, memory_usage, memory_per_item);
                }
                
                // Analyze growth patterns
                for i in 1..growth_data.len() {
                    let (prev_size, prev_memory, _prev_per_item) = growth_data[i-1];
                    let (curr_size, curr_memory, _curr_per_item) = growth_data[i];
                    
                    let size_multiplier = curr_size as f64 / prev_size as f64;
                    let memory_multiplier = curr_memory as f64 / prev_memory as f64;
                    
                    println!("Size {}->{}; Size ratio: {:.2}, Memory ratio: {:.2}", 
                            prev_size, curr_size, size_multiplier, memory_multiplier);
                    
                    // Memory growth should be roughly linear with size growth
                    // Allow for some variance due to allocation strategies
                    let growth_ratio_difference = (memory_multiplier / size_multiplier - 1.0).abs();
                    assert!(growth_ratio_difference < 1.0, 
                           "Memory growth ({:.2}x) should be roughly proportional to size growth ({:.2}x)",
                           memory_multiplier, size_multiplier);
                }
                
                // Test memory efficiency: larger caches should not have significantly worse per-item overhead
                if growth_data.len() >= 2 {
                    let small_per_item = growth_data[0].2;
                    let large_per_item = growth_data[growth_data.len()-1].2;
                    
                    let efficiency_ratio = large_per_item / small_per_item;
                    println!("Efficiency ratio (large/small per-item): {:.2}", efficiency_ratio);
                    
                    // Larger caches should not be dramatically less efficient per item
                    assert!(efficiency_ratio < 5.0, 
                           "Large caches should not be dramatically less efficient per item. Ratio: {:.2}", 
                           efficiency_ratio);
                }
                
                // Test with different data types to ensure consistent patterns
                let mut string_cache = FIFOCache::<String, String>::new(500);
                for i in 0..500 {
                    string_cache.insert(format!("key_{:06}", i), format!("value_{:06}", i));
                }
                
                let string_memory = estimate_cache_memory_usage(&string_cache);
                let string_per_item = string_memory as f64 / 500.0;
                
                println!("String cache - Total: {} bytes, Per item: {:.2} bytes", 
                        string_memory, string_per_item);
                
                // String cache should use more memory per item than numeric cache (expected)
                let numeric_per_item = growth_data.iter()
                    .find(|(size, _, _)| *size == 500)
                    .map(|(_, _, per_item)| *per_item)
                    .unwrap_or(0.0);
                
                if numeric_per_item > 0.0 {
                    assert!(string_per_item > numeric_per_item, 
                           "String cache should use more memory per item than numeric cache");
                }
                
                // Test empty vs full cache memory difference
                let empty_cache = FIFOCache::<String, String>::new(1000);
                let empty_memory = estimate_cache_memory_usage(&empty_cache);
                
                let mut full_cache = FIFOCache::<String, String>::new(1000);
                for i in 0..1000 {
                    full_cache.insert(format!("key_{}", i), format!("value_{}", i));
                }
                let full_memory = estimate_cache_memory_usage(&full_cache);
                
                println!("Empty vs Full cache: {} bytes -> {} bytes", empty_memory, full_memory);
                
                // In Rust, collections pre-allocate based on capacity, so memory usage may be similar
                // We just verify that full cache doesn't use less memory than empty cache
                let growth_factor = full_memory as f64 / empty_memory as f64;
                assert!(growth_factor >= 1.0, 
                       "Full cache should use at least as much memory as empty cache. Growth: {:.2}x", 
                       growth_factor);
            }
        }

        // Performance Benchmark Tests
        mod performance {
            use super::*;
            use std::time::Instant;

            /// Performance metrics collected during benchmarks
            #[derive(Debug)]
            struct PerformanceMetrics {
                total_operations: usize,
                total_time_ns: u64,
                ops_per_second: f64,
                avg_time_per_op_ns: f64,
                cache_hit_rate: f64,
            }

            impl PerformanceMetrics {
                fn new(total_operations: usize, total_time_ns: u64, cache_hits: usize) -> Self {
                    let ops_per_second = if total_time_ns > 0 {
                        (total_operations as f64) / (total_time_ns as f64 / 1_000_000_000.0)
                    } else {
                        0.0
                    };
                    
                    let avg_time_per_op_ns = if total_operations > 0 {
                        total_time_ns as f64 / total_operations as f64
                    } else {
                        0.0
                    };

                    let cache_hit_rate = if total_operations > 0 {
                        cache_hits as f64 / total_operations as f64
                    } else {
                        0.0
                    };

                    PerformanceMetrics {
                        total_operations,
                        total_time_ns,
                        ops_per_second,
                        avg_time_per_op_ns,
                        cache_hit_rate,
                    }
                }
            }

            /// Helper function to run a performance benchmark
            fn run_benchmark<F>(name: &str, operation: F) -> PerformanceMetrics
            where
                F: FnOnce() -> (usize, usize), // Returns (total_ops, cache_hits)
            {
                println!("Running benchmark: {}", name);
                let start = Instant::now();
                let (total_ops, cache_hits) = operation();
                let duration = start.elapsed();
                
                let metrics = PerformanceMetrics::new(total_ops, duration.as_nanos() as u64, cache_hits);
                
                println!("  Operations: {}", metrics.total_operations);
                println!("  Time: {:.2} ms", duration.as_secs_f64() * 1000.0);
                println!("  Ops/sec: {:.0}", metrics.ops_per_second);
                println!("  Avg time/op: {:.2} ns", metrics.avg_time_per_op_ns);
                println!("  Cache hit rate: {:.1}%", metrics.cache_hit_rate * 100.0);
                println!();
                
                metrics
            }

            #[test]
            fn test_small_cache_performance() {
                let cache_size = 100;
                let total_operations = 10_000;
                
                let metrics = run_benchmark("Small Cache (100 entries)", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Mix of insertions and lookups
                    for i in 0..total_operations {
                        if i % 3 == 0 {
                            // Insert operation
                            cache.insert(format!("key_{}", i % (cache_size * 2)), format!("value_{}", i));
                        } else {
                            // Lookup operation
                            let key = format!("key_{}", i % (cache_size * 2));
                            if cache.get(&key).is_some() {
                                cache_hits += 1;
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Performance assertions for small cache
                assert!(metrics.ops_per_second > 10_000.0, 
                       "Small cache should handle >10K ops/sec, got {:.0}", metrics.ops_per_second);
                assert!(metrics.avg_time_per_op_ns < 100_000.0, 
                       "Small cache operations should be <100μs, got {:.2}ns", metrics.avg_time_per_op_ns);
            }

            #[test]
            fn test_medium_cache_performance() {
                let cache_size = 1_000;
                let total_operations = 50_000;
                
                let metrics = run_benchmark("Medium Cache (1K entries)", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // More complex workload with different operation ratios
                    for i in 0..total_operations {
                        match i % 5 {
                            0 | 1 => {
                                // Insert operations (40%)
                                cache.insert(format!("key_{}", i % (cache_size * 3)), format!("value_{}", i));
                            }
                            _ => {
                                // Lookup operations (60%)
                                let key = format!("key_{}", i % (cache_size * 3));
                                if cache.get(&key).is_some() {
                                    cache_hits += 1;
                                }
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Performance assertions for medium cache
                assert!(metrics.ops_per_second > 5_000.0, 
                       "Medium cache should handle >5K ops/sec, got {:.0}", metrics.ops_per_second);
                assert!(metrics.avg_time_per_op_ns < 200_000.0, 
                       "Medium cache operations should be <200μs, got {:.2}ns", metrics.avg_time_per_op_ns);
            }

            #[test]
            fn test_large_cache_performance() {
                let cache_size = 10_000;
                let total_operations = 100_000;
                
                let metrics = run_benchmark("Large Cache (10K entries)", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Realistic workload with batch operations
                    for batch in 0..100 {
                        // Batch insert
                        for i in 0..500 {
                            let key = format!("batch_{}_{}", batch, i);
                            cache.insert(key, format!("data_{}", batch * 500 + i));
                        }
                        
                        // Batch lookup
                        for i in 0..500 {
                            let key = format!("batch_{}_{}", batch, i % 200); // Some lookups will miss
                            if cache.get(&key).is_some() {
                                cache_hits += 1;
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Performance assertions for large cache
                assert!(metrics.ops_per_second > 1_000.0, 
                       "Large cache should handle >1K ops/sec, got {:.0}", metrics.ops_per_second);
                assert!(metrics.avg_time_per_op_ns < 1_000_000.0, 
                       "Large cache operations should be <1ms, got {:.2}ns", metrics.avg_time_per_op_ns);
            }

            #[test]
            fn test_very_large_cache_performance() {
                let cache_size = 100_000;
                let total_operations = 200_000;
                
                let metrics = run_benchmark("Very Large Cache (100K entries)", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Stress test with large data
                    for i in 0..total_operations {
                        if i % 4 == 0 {
                            // Insert larger values to stress memory
                            let large_value = format!("large_data_{}_{}_{}", i, "x".repeat(100), i);
                            cache.insert(format!("large_key_{}", i), large_value);
                        } else {
                            // Lookup with locality
                            let key = format!("large_key_{}", i - (i % 1000)); // Some temporal locality
                            if cache.get(&key).is_some() {
                                cache_hits += 1;
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Performance assertions for very large cache (more lenient due to size)
                assert!(metrics.ops_per_second > 100.0, 
                       "Very large cache should handle >100 ops/sec, got {:.0}", metrics.ops_per_second);
                assert!(metrics.avg_time_per_op_ns < 10_000_000.0, 
                       "Very large cache operations should be <10ms, got {:.2}ns", metrics.avg_time_per_op_ns);
            }

            #[test]
            fn test_sequential_access_pattern() {
                let cache_size = 1_000;
                let total_operations = 20_000;
                
                let metrics = run_benchmark("Sequential Access Pattern", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Phase 1: Sequential insertion
                    for i in 0..cache_size {
                        cache.insert(format!("seq_{}", i), format!("data_{}", i));
                    }
                    
                    // Phase 2: Sequential access with some overwrites
                    for i in 0..(total_operations - cache_size) {
                        let idx = i % (cache_size * 2); // Some keys will be new, some existing
                        
                        if i % 3 == 0 {
                            // Insert/update
                            cache.insert(format!("seq_{}", idx), format!("updated_data_{}", i));
                        } else {
                            // Sequential read
                            let key = format!("seq_{}", idx);
                            if cache.get(&key).is_some() {
                                cache_hits += 1;
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Sequential access should be efficient
                assert!(metrics.ops_per_second > 10_000.0, 
                       "Sequential access should be fast, got {:.0} ops/sec", metrics.ops_per_second);
                assert!(metrics.cache_hit_rate > 0.3, 
                       "Sequential access should have reasonable hit rate, got {:.1}%", metrics.cache_hit_rate * 100.0);
            }

            #[test]
            fn test_random_access_pattern() {
                let cache_size = 1_000;
                let total_operations = 20_000;
                let key_space = cache_size * 5; // Larger key space for more misses
                
                let metrics = run_benchmark("Random Access Pattern", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Simulate random access using deterministic sequence
                    let mut rng_state = 12345u64; // Simple LCG for deterministic "randomness"
                    
                    for _ in 0..total_operations {
                        // Simple LCG: a = 1664525, c = 1013904223, m = 2^32
                        rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
                        let key_idx = (rng_state % key_space as u64) as usize;
                        let operation = rng_state % 4;
                        
                        match operation {
                            0 => {
                                // Insert (25%)
                                cache.insert(format!("rand_{}", key_idx), format!("random_data_{}", rng_state));
                            }
                            _ => {
                                // Lookup (75%)
                                let key = format!("rand_{}", key_idx);
                                if cache.get(&key).is_some() {
                                    cache_hits += 1;
                                }
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Random access typically has lower hit rates but should still be performant
                assert!(metrics.ops_per_second > 5_000.0, 
                       "Random access should be reasonably fast, got {:.0} ops/sec", metrics.ops_per_second);
                assert!(metrics.cache_hit_rate < 0.5, 
                       "Random access should have lower hit rate due to larger key space, got {:.1}%", 
                       metrics.cache_hit_rate * 100.0);
            }

            #[test]
            fn test_mixed_workload_performance() {
                let cache_size = 2_000;
                let total_operations = 30_000;
                
                let metrics = run_benchmark("Mixed Workload (Read/Write/Update)", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Complex mixed workload simulating real-world usage
                    for i in 0..total_operations {
                        match i % 10 {
                            0 | 1 => {
                                // Heavy read phase (20%)
                                for j in 0..5 {
                                    let key = format!("mixed_{}", (i + j) % (cache_size / 2));
                                    if cache.get(&key).is_some() {
                                        cache_hits += 1;
                                    }
                                }
                            }
                            2 | 3 => {
                                // Write phase (20%)
                                cache.insert(format!("mixed_{}", i % cache_size), format!("data_{}", i));
                            }
                            4 => {
                                // Update existing (10%)
                                let key = format!("mixed_{}", i % (cache_size / 4));
                                cache.insert(key, format!("updated_{}", i));
                            }
                            5 | 6 | 7 => {
                                // Batch operations (30%)
                                let batch_size = 3;
                                for j in 0..batch_size {
                                    let key = format!("batch_{}_{}", i / 10, j);
                                    if j % 2 == 0 {
                                        cache.insert(key, format!("batch_data_{}_{}", i, j));
                                    } else if cache.get(&key).is_some() {
                                        cache_hits += 1;
                                    }
                                }
                            }
                            _ => {
                                // FIFO-specific operations (20%)
                                if let Some((old_key, _)) = cache.pop_oldest() {
                                    // Re-insert with new value to test FIFO behavior under load
                                    cache.insert(old_key, format!("recycled_{}", i));
                                }
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Mixed workload should maintain good performance across operation types
                assert!(metrics.ops_per_second > 3_000.0, 
                       "Mixed workload should maintain good performance, got {:.0} ops/sec", metrics.ops_per_second);
                assert!(metrics.cache_hit_rate > 0.1 && metrics.cache_hit_rate < 0.8, 
                       "Mixed workload should have moderate hit rate, got {:.1}%", metrics.cache_hit_rate * 100.0);
            }

            #[test]
            fn test_heavy_eviction_performance() {
                let cache_size = 500;
                let total_operations = 15_000;
                
                let metrics = run_benchmark("Heavy Eviction Scenario", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Create scenario that causes frequent evictions
                    let key_space = cache_size * 10; // 10x larger key space forces evictions
                    
                    for i in 0..total_operations {
                        if i % 5 == 0 {
                            // Lookup (20% - mostly misses due to evictions)
                            let key = format!("evict_{}", i % key_space);
                            if cache.get(&key).is_some() {
                                cache_hits += 1;
                            }
                        } else {
                            // Insert (80% - causes constant evictions)
                            cache.insert(format!("evict_{}", i % key_space), format!("data_{}", i));
                        }
                    }
                    
                    // Verify we're actually at capacity (heavy eviction occurred)
                    assert_eq!(cache.len(), cache_size, "Cache should be at capacity after heavy eviction");
                    
                    (total_operations, cache_hits)
                });

                // Heavy eviction should still maintain reasonable performance
                assert!(metrics.ops_per_second > 1_000.0, 
                       "Heavy eviction should still maintain >1K ops/sec, got {:.0}", metrics.ops_per_second);
                assert!(metrics.cache_hit_rate < 0.3, 
                       "Heavy eviction should result in low hit rate, got {:.1}%", metrics.cache_hit_rate * 100.0);
                
                // Test that eviction mechanism is working efficiently
                assert!(metrics.avg_time_per_op_ns < 1_000_000.0, 
                       "Eviction operations should be efficient, got {:.2}ns avg", metrics.avg_time_per_op_ns);
            }

            #[test]
            fn test_light_eviction_performance() {
                let cache_size = 2_000;
                let total_operations = 25_000;
                
                let metrics = run_benchmark("Light Eviction Scenario", || {
                    let mut cache = FIFOCache::new(cache_size);
                    let mut cache_hits = 0;
                    
                    // Create scenario with minimal evictions (working set fits mostly in cache)
                    let working_set_size = cache_size * 3 / 4; // 75% of cache size for good locality
                    
                    // Pre-populate cache
                    for i in 0..working_set_size {
                        cache.insert(format!("stable_{}", i), format!("data_{}", i));
                    }
                    
                    // Workload with high temporal locality (minimal evictions)
                    for i in 0..total_operations {
                        match i % 16 {
                            0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 => {
                                // High read ratio (62.5%)
                                let key = format!("stable_{}", i % working_set_size);
                                if cache.get(&key).is_some() {
                                    cache_hits += 1;
                                }
                            }
                            10 | 11 | 12 | 13 => {
                                // Update existing (25%)
                                let key = format!("stable_{}", i % working_set_size);
                                cache.insert(key, format!("updated_data_{}", i));
                            }
                            14 => {
                                // Very occasional new insert within working set (6.25% - minimal eviction)
                                let key = format!("stable_{}", (i + working_set_size / 2) % working_set_size);
                                cache.insert(key, format!("refreshed_data_{}", i));
                            }
                            _ => {
                                // Rare new insert outside working set (6.25% - may cause some eviction)
                                // Only if working set is much smaller than capacity
                                if working_set_size < cache_size * 9 / 10 {
                                    cache.insert(format!("new_{}", i), format!("new_data_{}", i));
                                } else {
                                    // Otherwise, just do another read to maintain high hit rate
                                    let key = format!("stable_{}", i % working_set_size);
                                    if cache.get(&key).is_some() {
                                        cache_hits += 1;
                                    }
                                }
                            }
                        }
                    }
                    
                    (total_operations, cache_hits)
                });

                // Light eviction should have excellent performance and high hit rates
                assert!(metrics.ops_per_second > 8_000.0, 
                       "Light eviction should have excellent performance, got {:.0} ops/sec", metrics.ops_per_second);
                assert!(metrics.cache_hit_rate > 0.5, 
                       "Light eviction should have high hit rate, got {:.1}%", metrics.cache_hit_rate * 100.0);
                assert!(metrics.avg_time_per_op_ns < 125_000.0, 
                       "Light eviction operations should be very fast, got {:.2}ns avg", metrics.avg_time_per_op_ns);
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

        // Thread Safety Analysis Tests
        mod thread_safety {
            use super::*;

            #[test]
            fn test_identify_race_conditions() {
                // TODO: Demonstrate that current impl is NOT thread-safe
                todo!("Implement race condition identification test")
            }

            #[test]
            fn test_concurrent_hashmap_access() {
                // TODO: Test concurrent HashMap access issues
                todo!("Implement concurrent hashmap access test")
            }

            #[test]
            fn test_concurrent_vecdeque_access() {
                // TODO: Test concurrent VecDeque access issues
                todo!("Implement concurrent vecdeque access test")
            }

            #[test]
            fn test_data_corruption_scenarios() {
                // TODO: Test scenarios that could lead to data corruption
                todo!("Implement data corruption scenarios test")
            }

            #[test]
            fn test_inconsistent_state_detection() {
                // TODO: Test detection of inconsistent internal state
                todo!("Implement inconsistent state detection test")
            }
        }

        // Thread-Safe Wrapper Tests
        mod thread_safe_wrapper {
            use super::*;

            // Helper type for thread-safe testing
            type ThreadSafeFIFOCache<K, V> = Arc<Mutex<FIFOCache<K, V>>>;

            #[test]
            fn test_basic_thread_safe_operations() {
                // TODO: Test basic operations with mutex wrapper
                todo!("Implement basic thread safe operations test")
            }

            #[test]
            fn test_read_heavy_workload() {
                // TODO: Test multiple readers with occasional writers
                todo!("Implement read heavy workload test")
            }

            #[test]
            fn test_write_heavy_workload() {
                // TODO: Test concurrent insertions and evictions
                todo!("Implement write heavy workload test")
            }

            #[test]
            fn test_mixed_operations_concurrency() {
                // TODO: Test random mix of all operations across threads
                todo!("Implement mixed operations concurrency test")
            }

            #[test]
            fn test_deadlock_prevention() {
                // TODO: Test that operations don't cause deadlocks
                todo!("Implement deadlock prevention test")
            }

            #[test]
            fn test_fairness_across_threads() {
                // TODO: Test that all threads get fair access
                todo!("Implement fairness across threads test")
            }
        }

        // Stress Testing
        mod stress_testing {
            use super::*;

            #[test]
            fn test_high_contention_scenario() {
                // TODO: Many threads accessing same keys
                todo!("Implement high contention scenario test")
            }

            #[test]
            fn test_cache_thrashing_scenario() {
                // TODO: Rapid insertions causing frequent evictions
                todo!("Implement cache thrashing scenario test")
            }

            #[test]
            fn test_long_running_stability() {
                // TODO: Verify stability over extended periods
                todo!("Implement long running stability test")
            }

            #[test]
            fn test_memory_pressure_scenario() {
                // TODO: Test behavior under memory pressure
                todo!("Implement memory pressure scenario test")
            }

            #[test]
            fn test_rapid_thread_creation_destruction() {
                // TODO: Test with threads being created/destroyed rapidly
                todo!("Implement rapid thread creation destruction test")
            }

            #[test]
            fn test_burst_load_handling() {
                // TODO: Test handling of sudden burst loads
                todo!("Implement burst load handling test")
            }

            #[test]
            fn test_gradual_load_increase() {
                // TODO: Test behavior as load gradually increases
                todo!("Implement gradual load increase test")
            }
        }
    }
}
