use crate::concurrency::transaction::Transaction;
use std::vec::Vec;

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
