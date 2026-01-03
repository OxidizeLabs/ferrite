//! # Disk Hash Table Trait
//!
//! This module defines the `DiskHashTable` trait, a generic interface for
//! disk-based hash table implementations that support transactional operations.

use std::vec::Vec;

use crate::concurrency::transaction::Transaction;

/// A generic trait for disk-based hash table implementations.
///
/// This trait defines the common interface for hash tables that store data on disk
/// and support transactional operations. Implementations must provide insert, remove,
/// and lookup operations that are transaction-aware.
///
/// # Type Parameters
///
/// - `KeyType`: The type of keys stored in the hash table.
/// - `ValueType`: The type of values associated with keys.
/// - `KeyComparator`: A comparator type for comparing keys (used for equality checks).
///
/// # Thread Safety
///
/// Implementations should ensure thread safety when accessed from multiple transactions.
/// The `&self` receiver on all methods allows for concurrent read access, with
/// implementations handling internal synchronization as needed.
///
/// # Example
///
/// ```rust,ignore
/// struct MyHashTable;
///
/// impl DiskHashTable<i32, RID, DefaultComparator> for MyHashTable {
///     fn insert(&self, txn: &Transaction, key: &i32, value: &RID) -> bool {
///         // Implementation
///         true
///     }
///     // ... other methods
/// }
/// ```
pub trait DiskHashTable<KeyType, ValueType, KeyComparator> {
    /// Inserts a key-value pair into the hash table.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The current transaction.
    /// * `key` - The key to insert.
    /// * `value` - The value to be associated with the key.
    ///
    /// # Returns
    ///
    /// `true` if the insert succeeded, `false` otherwise.
    fn insert(&self, transaction: &Transaction, key: &KeyType, value: &ValueType) -> bool;

    /// Deletes the associated value for the given key.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The current transaction.
    /// * `key` - The key to delete.
    /// * `value` - The value to delete.
    ///
    /// # Returns
    ///
    /// `true` if the remove succeeded, `false` otherwise.
    fn remove(&self, transaction: &Transaction, key: &KeyType, value: &ValueType) -> bool;

    /// Performs a point query on the hash table.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The current transaction.
    /// * `key` - The key to look up.
    /// * `result` - The value(s) associated with the given key.
    ///
    /// # Returns
    ///
    /// `true` if the lookup succeeded, `false` otherwise.
    fn get_value(
        &self,
        transaction: &Transaction,
        key: &KeyType,
        result: &mut Vec<ValueType>,
    ) -> bool;
}
