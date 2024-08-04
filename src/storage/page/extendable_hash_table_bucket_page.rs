use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of;

use crate::common::config::DB_PAGE_SIZE;
use crate::storage::index::generic_key::{Comparator, GenericComparator};

pub const HTABLE_BUCKET_PAGE_METADATA_SIZE: usize = size_of::<u32>() * 2;

pub fn htable_bucket_array_size(mapping_type_size: usize) -> usize {
    (DB_PAGE_SIZE - HTABLE_BUCKET_PAGE_METADATA_SIZE) / mapping_type_size
}

pub type MappingType<K, V> = (K, V);

/// Bucket page for extendable hash table.
pub struct ExtendableHTableBucketPage<K, V, C>
where
    K: Clone + Debug,
    V: Clone + Debug,
    C: Comparator<K> + Clone,
{
    size: u32,
    max_size: u32,
    array: Vec<MappingType<K, V>>,
    _marker: PhantomData<C>,
}

impl<K, V, C> ExtendableHTableBucketPage<K, V, C>
where
    K: Clone + Debug,
    V: Clone + Debug,
    C: Comparator<K> + Clone,
{
    /// Initializes a new bucket page with the specified maximum size.
    ///
    /// # Parameters
    /// - `max_size`: The maximum size of the bucket array.
    pub fn init(&mut self, max_size: u32) {
        self.size = 0;
        self.max_size = max_size;
        self.array = Vec::with_capacity(max_size as usize);
    }

    /// Looks up a key in the bucket.
    ///
    /// # Parameters
    /// - `key`: The key to look up.
    /// - `cmp`: The comparator.
    ///
    /// # Returns
    /// An optional value associated with the key.
    pub fn lookup(&self, key: &K, cmp: &C) -> Option<V> {
        println!("Looking up key: {:?}", key);
        for (k, v) in &self.array {
            println!("Comparing with key: {:?}", k);
            if cmp.compare(k, key) == std::cmp::Ordering::Equal {
                println!("Key found: {:?}", k);
                return Some(v.clone());
            }
        }
        println!("Key not found: {:?}", key);
        None
    }

    /// Attempts to insert a key-value pair into the bucket.
    ///
    /// # Parameters
    /// - `key`: The key to insert.
    /// - `value`: The value to insert.
    /// - `cmp`: The comparator to use.
    ///
    /// # Returns
    /// True if inserted, false if bucket is full or the same key is already present.
    pub fn insert(&mut self, key: K, value: V, cmp: &C) -> bool {
        if self.is_full() {
            return false;
        }
        if self.lookup(&key, cmp).is_some() {
            return false;
        }
        self.array.push((key, value));
        self.size += 1;
        true
    }

    /// Removes a key from the bucket.
    ///
    /// # Parameters
    /// - `key`: The key to remove.
    /// - `cmp`: The comparator to use.
    ///
    /// # Returns
    /// True if removed, false if not found.
    pub fn remove(&mut self, key: &K, cmp: &C) -> bool {
        if let Some(index) = self.array.iter().position(|(k, _)| cmp.compare(k, key) == std::cmp::Ordering::Equal) {
            self.array.swap_remove(index);
            self.size -= 1;
            true
        } else {
            false
        }
    }

    /// Removes an entry at a specific index in the bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the bucket to remove the entry from.
    pub fn remove_at(&mut self, bucket_idx: usize) {
        if bucket_idx < self.size as usize {
            self.array.swap_remove(bucket_idx);
            self.size -= 1;
        }
    }

    /// Gets the key at a specific index in the bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the bucket to get the key from.
    ///
    /// # Returns
    /// The key at the specified index.
    pub fn key_at(&self, bucket_idx: usize) -> Option<&K> {
        self.array.get(bucket_idx).map(|(k, _)| k)
    }

    /// Gets the value at a specific index in the bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the bucket to get the value from.
    ///
    /// # Returns
    /// The value at the specified index.
    pub fn value_at(&self, bucket_idx: usize) -> Option<&V> {
        self.array.get(bucket_idx).map(|(_, v)| v)
    }

    /// Gets the entry at a specific index in the bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the bucket to get the entry from.
    ///
    /// # Returns
    /// The entry at the specified index.
    pub fn entry_at(&self, bucket_idx: usize) -> Option<&MappingType<K, V>> {
        self.array.get(bucket_idx)
    }

    /// Returns the number of entries in the bucket.
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Returns whether the bucket is full.
    pub fn is_full(&self) -> bool {
        self.size as usize >= self.max_size as usize
    }

    /// Returns whether the bucket is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Prints the bucket's occupancy information.
    pub fn print_bucket(&self) {
        println!("ExtendableHTableBucketPage:");
        println!("Size: {}", self.size);
        println!("Max size: {}", self.max_size);
        for (i, (key, value)) in self.array.iter().enumerate() {
            println!("Entry {}: Key: {:?}, Value: {:?}", i, key, value);
        }
    }
}
