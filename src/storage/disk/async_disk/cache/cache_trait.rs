use std::hash::Hash;

/// A trait defining the interface for cache implementations.
pub trait Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new cache with the specified capacity.
    fn new(capacity: usize) -> Self;

    /// Returns the current number of entries in the cache.
    fn len(&self) -> usize;

    /// Returns true if the cache is empty.
    fn is_empty(&self) -> bool;

    /// Returns the maximum number of entries the cache can hold.
    fn capacity(&self) -> usize;

    /// Inserts a key-value pair into the cache.
    /// Returns the previous value associated with the key if it exists.
    fn insert(&mut self, key: K, value: V) -> Option<V>;

    /// Retrieves a value from the cache by its key.
    /// This operation may update the cache's internal state (e.g., update access time).
    fn get(&mut self, key: &K) -> Option<&V>;

    /// Removes a key-value pair from the cache.
    /// Returns the value associated with the key if it exists.
    fn remove(&mut self, key: &K) -> Option<V>;

    /// Clears all entries from the cache.
    fn clear(&mut self);

    /// Checks if the cache contains the specified key.
    fn contains(&self, key: &K) -> bool;
}