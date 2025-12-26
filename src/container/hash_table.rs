//! # In-Memory Hash Table Trait
//!
//! This module defines the `HashTable` trait, a generic interface for
//! in-memory hash table implementations.

/// A generic trait for in-memory hash table implementations.
///
/// This trait defines the basic operations for a hash table: find, remove, and insert.
/// Unlike [`DiskHashTable`](super::disk_hash_table::DiskHashTable), this trait is
/// designed for simpler, non-transactional hash table operations.
///
/// # Type Parameters
///
/// - `K`: The key type.
/// - `V`: The value type.
///
/// # Note
///
/// This is a minimal trait definition. Implementations may extend this with
/// additional functionality such as iteration, capacity management, or
/// collision handling strategies.
pub trait HashTable<K, V> {
    /// Checks if a key-value pair exists in the hash table.
    ///
    /// # Parameters
    ///
    /// - `key`: The key to search for.
    /// - `value`: The value to match against.
    ///
    /// # Returns
    ///
    /// `true` if the key-value pair exists, `false` otherwise.
    fn find(key: K, value: V) -> bool;

    /// Removes an entry by key from the hash table.
    ///
    /// # Parameters
    ///
    /// - `key`: The key to remove.
    ///
    /// # Returns
    ///
    /// `true` if the key was found and removed, `false` otherwise.
    fn remove(key: K) -> bool;

    /// Inserts a key-value pair into the hash table.
    ///
    /// If the key already exists, the behavior depends on the implementation
    /// (may update the value or store duplicate entries).
    ///
    /// # Parameters
    ///
    /// - `key`: The key to insert.
    /// - `value`: The value to associate with the key.
    fn insert(key: K, value: V) -> ();
}
