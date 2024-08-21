use crate::common::config::DB_PAGE_SIZE;
use crate::storage::index::generic_key::{Comparator, GenericComparator};
use log::{debug, error, info, trace, warn};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of;

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
        debug!("Looking up key: {:?}", key);
        for (k, v) in &self.array {
            if cmp.compare(k, key) == std::cmp::Ordering::Equal {
                debug!("Found key: {:?} with value: {:?}", key, v);
                return Some(v.clone());
            }
        }
        warn!("Key: {:?} not found in bucket", key);
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
            warn!("Bucket is full, cannot insert key: {:?}", key);
            return false;
        }
        if self.lookup(&key, cmp).is_some() {
            warn!("Key: {:?} already exists in bucket, insertion aborted", key);
            return false;
        }
        self.array.push((key.clone(), value.clone()));
        info!("Inserted key-value pair: {:?} -> {:?}, new size: {}", key, value, self.array.iter().len());
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
        debug!("Removing key: {:?}", key);
        if let Some(index) = self
            .array
            .iter()
            .position(|(k, _)| cmp.compare(k, key) == std::cmp::Ordering::Equal)
        {
            self.array.swap_remove(index);
            info!("Removed key: {:?}, new size: {}", key, self.array.iter().count());
            true
        } else {
            warn!("Key: {:?} not found in bucket, removal failed", key);
            false
        }
    }

    /// Removes an entry at a specific index in the bucket.
    ///
    /// # Parameters
    /// - `bucket_idx`: The index in the bucket to remove the entry from.
    pub fn remove_at(&mut self, bucket_idx: usize) {
        if bucket_idx < self.array.iter().count() as usize {
            let removed = self.array.swap_remove(bucket_idx);
            info!("Removed entry at index: {}, entry: {:?}, new size: {}", bucket_idx, removed, self.array.iter().count());
        } else {
            warn!("Attempted to remove entry at invalid index: {}", bucket_idx);
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
        trace!("Fetching key at index: {}", bucket_idx);
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
        trace!("Fetching value at index: {}", bucket_idx);
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
        trace!("Fetching entry at index: {}", bucket_idx);
        self.array.get(bucket_idx)
    }

    /// Returns the number of entries in the bucket.
    pub fn get_size(&self) -> u32 {
        self.array.iter().count() as u32
    }

    /// Returns the maximum number of entries the bucket can support.
    pub fn get_max_size(&self) -> u32 {
        self.max_size
    }

    /// Returns whether the bucket is full.
    pub fn is_full(&self) -> bool {
        debug!("Checking if bucket is full: current size: {}, max size: {}", self.array.iter().count(), self.max_size);
        self.array.iter().count() >= self.max_size as usize
    }

    /// Returns whether the bucket is empty.
    pub fn is_empty(&self) -> bool {
        trace!("Checking if bucket is empty: current size: {}", self.array.iter().count());
        self.array.is_empty()
    }

    /// Prints the bucket's occupancy information.
    pub fn print_bucket(&self) {
        // Define the column headers
        let header_idx = "i";
        let header_key = "k";
        let header_value = "v";

        // Calculate the maximum width for each column
        let max_idx_width = std::cmp::max(header_idx.len(), self.array.len().to_string().len());
        let max_key_width = std::cmp::max(header_key.len(), self.array.iter().map(|(key, _)| format!("{:?}", key).len()).max().unwrap_or(0));
        let max_value_width = std::cmp::max(header_value.len(), self.array.iter().map(|(_, value)| format!("{:?}", value).len()).max().unwrap_or(0));

        println!(
            "======== BUCKET (size: {} | max_size: {}) ========",
            self.get_size(),
            self.max_size
        );
        println!(
            "| {:<width_idx$} | {:<width_key$} | {:<width_value$} |",
            header_idx,
            header_key,
            header_value,
            width_idx = max_idx_width,
            width_key = max_key_width,
            width_value = max_value_width
        );
        for (idx, (key, value)) in self.array.iter().enumerate() {
            println!(
                "| {:<width_idx$} | {:<width_key$} | {:<width_value$} |",
                idx,
                format!("{:?}", key),
                format!("{:?}", value),
                width_idx = max_idx_width,
                width_key = max_key_width,
                width_value = max_value_width
            );
        }
        println!("================ END BUCKET ================\n");
    }

}
