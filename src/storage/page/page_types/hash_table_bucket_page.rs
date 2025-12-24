//! Bucket page implementation for chained/directory-based hash table indexes.
//!
//! This module provides [`HashTableBucketPage`], which stores key-value pairs
//! in a bucket for use with chained hashing or directory-based hash tables.
//! Unlike linear probing, buckets are independent containers that can overflow
//! to additional pages.
//!
//! # Bucket Layout
//!
//! Keys and values are stored in a fixed-size array within the bucket:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ (K₀,V₀) │ (K₁,V₁) │ (K₂,V₂) │ ... │ (Kₙ₋₁,Vₙ₋₁) │ (empty) │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Slot State Tracking
//!
//! Each slot has two state bits tracked in separate bitmaps:
//!
//! | Occupied | Readable | Meaning                              |
//! |----------|----------|--------------------------------------|
//! |    0     |    0     | Empty (available for insertion)      |
//! |    1     |    1     | Valid entry (key-value pair)         |
//! |    1     |    0     | Tombstone (deleted entry)            |
//!
//! The distinction between occupied and readable enables efficient reuse of
//! deleted slots while maintaining correct lookup semantics.
//!
//! # Thread Safety
//!
//! The bucket uses atomic operations for bitmap updates and a mutex for the
//! key-value array, supporting concurrent access:
//!
//! - Bitmaps use `AtomicU8` with `SeqCst` ordering
//! - Key-value array protected by `Mutex`
//!
//! # Key Operations
//!
//! - [`insert`](HashTableBucketPage::insert): Insert if not duplicate and not full
//! - [`remove`](HashTableBucketPage::remove): Find and tombstone matching entry
//! - [`get_value`](HashTableBucketPage::get_value): Collect all values for a key
//! - [`is_full`](HashTableBucketPage::is_full) / [`is_empty`](HashTableBucketPage::is_empty):
//!   Capacity checks for overflow/merge decisions
//!
//! # Generics
//!
//! - `KeyType`: The key type (must be `Clone + Default`)
//! - `ValueType`: The value type (must be `Clone + Default + PartialEq`)
//! - `KeyComparator`: A function `Fn(&K, &K) -> bool` for key equality

use crate::storage::page::page_types::hash_table_page_defs::bucket_array_size;
use log::info;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};

pub type MappingType<KeyType, ValueType> = (KeyType, ValueType);

/**
 * Store indexed key and value together within bucket page. Supports
 * non-unique keys.
 *
 * Bucket page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *  The above format omits the space required for the occupied_ and
 *  readable_ arrays. More information is in storage/page/hash_table_page_defs.h.
 *
 */
pub struct HashTableBucketPage<KeyType, ValueType, KeyComparator> {
    occupied: Vec<AtomicU8>,
    readable: Vec<AtomicU8>,
    array: Mutex<Vec<MappingType<KeyType, ValueType>>>,
    _marker: PhantomData<KeyComparator>,
}

impl<KeyType, ValueType, KeyComparator> Default
    for HashTableBucketPage<KeyType, ValueType, KeyComparator>
where
    KeyComparator: Fn(&KeyType, &KeyType) -> bool,
    KeyType: Clone + Default,
    ValueType: Clone + Default + PartialEq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<KeyType, ValueType, KeyComparator> HashTableBucketPage<KeyType, ValueType, KeyComparator>
where
    KeyComparator: Fn(&KeyType, &KeyType) -> bool,
    KeyType: Clone + Default,
    ValueType: Clone + Default + PartialEq,
{
    /// Creates a new `HashTableBucketPage` with the appropriate size.
    pub fn new() -> Self {
        let array_size = bucket_array_size::<KeyType, ValueType>();
        Self {
            occupied: (0..(array_size - 1) / 8 + 1)
                .map(|_| AtomicU8::new(0))
                .collect(),
            readable: (0..(array_size - 1) / 8 + 1)
                .map(|_| AtomicU8::new(0))
                .collect(),
            array: Mutex::new(vec![(Default::default(), Default::default()); array_size]),
            _marker: PhantomData,
        }
    }

    /// Scans the bucket and collects values that have the matching key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    /// * `cmp` - The comparator function to use for matching keys.
    /// * `result` - The vector to collect the matching values.
    ///
    /// # Returns
    ///
    /// `true` if at least one key matched, `false` otherwise.
    pub fn get_value(&self, key: KeyType, cmp: KeyComparator, result: &mut Vec<ValueType>) -> bool {
        let mut found = false;
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) && cmp(&self.key_at(i as u32), &key) {
                result.push(self.value_at(i as u32));
                found = true;
            }
        }
        found
    }

    /// Attempts to insert a key and value in the bucket. Uses the occupied
    /// and readable arrays to keep track of each slot's availability.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    /// * `cmp` - The comparator function to use for matching keys.
    ///
    /// # Returns
    ///
    /// `true` if inserted, `false` if duplicate key-value pair or bucket is full.
    pub fn insert(&self, key: KeyType, value: ValueType, cmp: KeyComparator) -> bool {
        if self.is_full() {
            return false;
        }
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) && cmp(&self.key_at(i as u32), &key) {
                return false;
            }
        }
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            if !self.is_occupied(i as u32) {
                return self.insert_at(i as u32, key, value);
            }
        }
        false
    }

    /// Helper method to insert a key and value at a specific index in the bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to write the key and value to.
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    ///
    /// # Returns
    ///
    /// `true` if the value is inserted successfully, `false` otherwise.
    fn insert_at(&self, bucket_idx: u32, key: KeyType, value: ValueType) -> bool {
        let idx = bucket_idx as usize;
        if self.is_occupied(bucket_idx) {
            return false;
        }

        let mut array = self.array.lock().unwrap();
        array[idx] = (key, value);
        drop(array);

        self.set_occupied(bucket_idx);
        self.set_readable(bucket_idx);

        true
    }

    /// Removes a key and value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    /// * `value` - The value to remove.
    /// * `cmp` - The comparator function to use for matching keys.
    ///
    /// # Returns
    ///
    /// `true` if removed, `false` if not found.
    pub fn remove(&self, key: KeyType, value: ValueType, cmp: KeyComparator) -> bool {
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32)
                && cmp(&self.key_at(i as u32), &key)
                && self.value_at(i as u32) == value
            {
                self.remove_at(i as u32);
                return true;
            }
        }
        false
    }

    /// Gets the key at an index in the bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index in the bucket to get the key at.
    ///
    /// # Returns
    ///
    /// The key at index `bucket_idx` of the bucket.
    pub fn key_at(&self, bucket_idx: u32) -> KeyType {
        self.array.lock().unwrap()[bucket_idx as usize].0.clone()
    }

    /// Gets the value at an index in the bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index in the bucket to get the value at.
    ///
    /// # Returns
    ///
    /// The value at index `bucket_idx` of the bucket.
    pub fn value_at(&self, bucket_idx: u32) -> ValueType {
        self.array.lock().unwrap()[bucket_idx as usize].1.clone()
    }

    /// Removes the key-value pair at `bucket_idx`.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to remove the value from.
    pub fn remove_at(&self, bucket_idx: u32) {
        let idx = bucket_idx as usize;
        self.readable[idx / 8].fetch_and(!(1 << (bucket_idx % 8)), Ordering::SeqCst);
    }

    /// Returns whether or not an index is occupied (key/value pair or tombstone).
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to check.
    ///
    /// # Returns
    ///
    /// `true` if the index is occupied, `false` otherwise.
    pub fn is_occupied(&self, bucket_idx: u32) -> bool {
        let idx = bucket_idx as usize;
        self.occupied[idx / 8].load(Ordering::SeqCst) & (1 << (bucket_idx % 8)) != 0
    }

    /// Updates the bitmap to indicate that the entry at `bucket_idx` is occupied.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to update.
    pub fn set_occupied(&self, bucket_idx: u32) {
        let idx = bucket_idx as usize;
        self.occupied[idx / 8].fetch_or(1 << (bucket_idx % 8), Ordering::SeqCst);
    }

    /// Returns whether or not an index is readable (valid key/value pair).
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to check.
    ///
    /// # Returns
    ///
    /// `true` if the index is readable, `false` otherwise.
    pub fn is_readable(&self, bucket_idx: u32) -> bool {
        let idx = bucket_idx as usize;
        self.readable[idx / 8].load(Ordering::SeqCst) & (1 << (bucket_idx % 8)) != 0
    }

    /// Updates the bitmap to indicate that the entry at `bucket_idx` is readable.
    ///
    /// # Arguments
    ///
    /// * `bucket_idx` - The index to update.
    pub fn set_readable(&self, bucket_idx: u32) {
        let idx = bucket_idx as usize;
        self.readable[idx / 8].fetch_or(1 << (bucket_idx % 8), Ordering::SeqCst);
    }

    /// Returns the number of readable elements, i.e., the current size.
    ///
    /// # Returns
    ///
    /// The number of readable elements.
    pub fn num_readable(&self) -> u32 {
        let mut count = 0;
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) {
                count += 1;
            }
        }
        count
    }

    /// Returns whether the bucket is full.
    ///
    /// # Returns
    ///
    /// `true` if the bucket is full, `false` otherwise.
    pub fn is_full(&self) -> bool {
        self.num_readable() == bucket_array_size::<KeyType, ValueType>() as u32
    }

    /// Returns whether the bucket is empty.
    ///
    /// # Returns
    ///
    /// `true` if the bucket is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.num_readable() == 0
    }

    /// Prints the bucket's occupancy information.
    pub fn print_bucket(&self) {
        for i in 0..bucket_array_size::<KeyType, ValueType>() {
            info!(
                "Index {}: occupied = {}, readable = {}",
                i,
                self.is_occupied(i as u32),
                self.is_readable(i as u32)
            );
        }
    }
}
