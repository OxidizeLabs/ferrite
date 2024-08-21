use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::transaction::Transaction;
use crate::container::hash_function::HashFunction;
use crate::storage::page::page_types::{
    hash_table_block_page::HashTableBlockPage, hash_table_header_page::HashTableHeaderPage,
};
use spin::RwLock;
use std::marker::PhantomData;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

pub struct LinearProbeHashTable<KeyType, ValueType, KeyComparator> {
    header_page_id: u32,
    buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
    comparator: KeyComparator,
    table_latch: RwLock<()>,
    hash_fn: HashFunction<KeyType>,
    _marker: PhantomData<ValueType>,
}

impl<KeyType, ValueType, KeyComparator> LinearProbeHashTable<KeyType, ValueType, KeyComparator>
where
    KeyType: Eq + Clone + std::hash::Hash,
    ValueType: Clone,
    KeyComparator: Fn(&KeyType, &KeyType) -> std::cmp::Ordering,
{
    /// Creates a new LinearProbeHashTable.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the hash table.
    /// * `buffer_pool_manager` - Buffer pool manager to be used.
    /// * `comparator` - Comparator for keys.
    /// * `num_buckets` - Initial number of buckets contained by this hash table.
    /// * `hash_fn` - The hash function.
    pub fn new(
        name: String,
        buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
        comparator: KeyComparator,
        num_buckets: usize,
        hash_fn: HashFunction<KeyType>,
    ) -> Self {
        Self {
            header_page_id: 0, // This should be set appropriately
            buffer_pool_manager,
            comparator,
            table_latch: RwLock::new(()),
            hash_fn,
            _marker: Default::default(),
        }
    }

    /// Inserts a key-value pair into the hash table.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The current transaction.
    /// * `key` - The key to create.
    /// * `value` - The value to be associated with the key.
    ///
    /// # Returns
    ///
    /// `true` if insert succeeded, `false` otherwise.
    pub fn insert(&self, transaction: &Transaction, key: &KeyType, value: &ValueType) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }

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
    /// `true` if remove succeeded, `false` otherwise.
    pub fn remove(&self, transaction: &Transaction, key: &KeyType, value: &ValueType) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }

    /// Performs a point query on the hash table.
    ///
    /// # Arguments
    ///
    /// * `transaction` - The current transaction.
    /// * `key` - The key to look up.
    /// * `result` - The value(s) associated with a given key.
    ///
    /// # Returns
    ///
    /// `true` if lookup succeeded, `false` otherwise.
    pub fn get_value(
        &self,
        transaction: &Transaction,
        key: &KeyType,
        result: &mut Vec<ValueType>,
    ) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }

    /// Resizes the table to at least twice the initial size provided.
    ///
    /// # Arguments
    ///
    /// * `initial_size` - The initial size of the hash table.
    pub fn resize(&self, initial_size: usize) {
        // TODO: Add implementation
        unimplemented!()
    }

    /// Gets the size of the hash table.
    ///
    /// # Returns
    ///
    /// The current size of the hash table.
    pub fn get_size(&self) -> usize {
        // TODO: Add implementation
        unimplemented!()
    }

    fn get_header_page(&self) -> &HashTableHeaderPage {
        // TODO: Add implementation
        unimplemented!()
    }

    fn get_block_page(
        &self,
        block_page_id: u32,
    ) -> &HashTableBlockPage<KeyType, ValueType, KeyComparator> {
        // TODO: Add implementation
        unimplemented!()
    }

    fn resize_insert(&self, header_page: &HashTableHeaderPage, key: &KeyType, value: &ValueType) {
        // TODO: Add implementation
        unimplemented!()
    }

    fn delete_block_pages(&self, old_header_page: &HashTableHeaderPage) {
        // TODO: Add implementation
        unimplemented!()
    }

    fn create_new_block_pages(&self, header_page: &HashTableHeaderPage, num_blocks: usize) {
        // TODO: Add implementation
        unimplemented!()
    }

    fn get_value_latch_free(
        &self,
        transaction: &Transaction,
        key: &KeyType,
        result: &mut Vec<ValueType>,
    ) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }
}
