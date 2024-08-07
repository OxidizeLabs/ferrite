use std::cmp::Ordering;
use std::fmt::Debug;
use std::vec::Vec;
use std::string::String;
use std::hash::Hash;
use std::marker::PhantomData;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::transaction::Transaction;
use crate::container::hash_function::HashFunction;
use crate::storage::index::generic_key::Comparator;
use crate::storage::page::{
    extendable_hash_table_bucket_page::ExtendableHTableBucketPage,
    extendable_hash_table_header_page::ExtendableHTableHeaderPage,
    extendable_hash_table_directory_page::ExtendableHTableDirectoryPage
};
use crate::storage::page::page_guard::ReadPageGuard;

pub struct DiskExtendableHashTable<K, V, C> {
    index_name: String,
    bpm: BufferPoolManager,
    cmp: C,
    hash_fn: HashFunction<K>,
    header_max_depth: u32,
    directory_max_depth: u32,
    bucket_max_size: usize,
    header_page_id: u32,
    _marker: PhantomData<V>
}

impl<K, V, C> DiskExtendableHashTable<K, V, C>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone + Debug,
    C: Fn(&K, &K) -> Ordering + Comparator<K> + Clone,
{
    /// Creates a new DiskExtendibleHashTable.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the hash table.
    /// * `bpm` - Buffer pool manager to be used.
    /// * `cmp` - Comparator for keys.
    /// * `hash_fn` - The hash function.
    /// * `header_max_depth` - The max depth allowed for the header page.
    /// * `directory_max_depth` - The max depth allowed for the directory page.
    /// * `bucket_max_size` - The max size allowed for the bucket page array.
    pub fn new(
        name: String,
        bpm: BufferPoolManager,
        cmp: C,
        hash_fn: HashFunction<K>,
        header_max_depth: u32,
        directory_max_depth: u32,
        bucket_max_size: usize,
    ) -> Self {
        Self {
            index_name: name,
            bpm,
            cmp,
            hash_fn,
            header_max_depth,
            directory_max_depth,
            bucket_max_size,
            header_page_id: 0, // This should be set appropriately
            _marker: Default::default(),
        }
    }

    /// Inserts a key-value pair into the hash table.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to be associated with the key.
    /// * `transaction` - The current transaction (optional).
    ///
    /// # Returns
    ///
    /// `true` if the insert succeeded, `false` otherwise.
    pub fn insert(&self, key: &K, value: &V, transaction: Option<&Transaction>) -> bool {
        // TODO(P2): Add implementation
        unimplemented!()
    }

    /// Removes a key-value pair from the hash table.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    /// * `transaction` - The current transaction (optional).
    ///
    /// # Returns
    ///
    /// `true` if the remove succeeded, `false` otherwise.
    pub fn remove(&self, key: &K, transaction: Option<&Transaction>) -> bool {
        // TODO(P2): Add implementation
        unimplemented!()
    }

    /// Gets the value associated with a given key in the hash table.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    /// * `result` - The value(s) associated with the given key.
    /// * `transaction` - The current transaction (optional).
    ///
    /// # Returns
    ///
    /// `true` if the lookup succeeded, `false` otherwise.
    pub fn get_value(&self, key: &K, result: &mut Vec<V>, transaction: Option<&Transaction>) -> bool {
        // TODO(P2): Add implementation
        unimplemented!()
    }

    /// Helper function to verify the integrity of the extendible hash table's directory.
    pub fn verify_integrity(&self) {
        // TODO: Add implementation
        unimplemented!()
    }

    /// Helper function to expose the header page id.
    pub fn get_header_page_id(&self) -> u32 {
        self.header_page_id
    }

    /// Helper function to print out the HashTable.
    pub fn print_ht(&self) {
        // TODO: Add implementation
        unimplemented!()
    }

    /// Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
    /// for extendible hashing.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to hash.
    ///
    /// # Returns
    ///
    /// The down-casted 32-bit hash.
    fn hash(&self, key: &K) -> u32 {
        // TODO: Add implementation
        unimplemented!()
    }

    fn insert_to_new_directory(
        &self,
        header: &ExtendableHTableHeaderPage,
        directory_idx: u32,
        hash: u32,
        key: &K,
        value: &V,
    ) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }

    fn insert_to_new_bucket(
        &self,
        directory: &ExtendableHTableDirectoryPage,
        bucket_idx: u32,
        key: &K,
        value: &V,
    ) -> bool {
        // TODO: Add implementation
        unimplemented!()
    }

    fn update_directory_mapping(
        &self,
        directory: &ExtendableHTableDirectoryPage,
        new_bucket_idx: u32,
        new_bucket_page_id: u32,
        new_local_depth: u32,
        local_depth_mask: u32,
    ) {
        // TODO: Add implementation
        unimplemented!()
    }

    fn migrate_entries(
        &self,
        old_bucket: &ExtendableHTableBucketPage<K, V, C>,
        new_bucket: &ExtendableHTableBucketPage<K, V, C>,
        new_bucket_idx: u32,
        local_depth_mask: u32,
    ) {
        // TODO: Add implementation
        unimplemented!()
    }
}
