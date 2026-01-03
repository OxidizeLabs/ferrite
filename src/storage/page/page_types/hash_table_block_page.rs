//! Block page implementation for linear probe hash table indexes.
//!
//! This module provides [`HashTableBlockPage`], which stores key-value pairs
//! in a flat array format for use in linear probing hash tables. It supports
//! non-unique keys and thread-safe concurrent access.
//!
//! # Block Layout
//!
//! Keys and values are stored contiguously in a fixed-size array:
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
//! |    0     |    0     | Empty (never used)                   |
//! |    1     |    1     | Valid entry (key-value pair)         |
//! |    1     |    0     | Tombstone (deleted, skip during probe)|
//!
//! This distinction enables efficient linear probing: probes continue past
//! tombstones but stop at empty slots.
//!
//! # Thread Safety
//!
//! The block uses atomic operations for bitmap updates and a mutex for the
//! key-value array, enabling concurrent inserts and removes:
//!
//! - [`insert_at`](HashTableBlockPage::insert_at): Atomically claims a slot
//! - [`remove_at`](HashTableBlockPage::remove_at): Marks slot as tombstone
//! - Bitmaps use `AtomicU8` with `SeqCst` ordering
//!
//! # Key Operations
//!
//! - [`insert`](HashTableBlockPage::insert): Find empty slot and insert
//! - [`remove`](HashTableBlockPage::remove): Find matching entry and tombstone it
//! - [`get_value`](HashTableBlockPage::get_value): Scan for all values matching a key
//! - [`is_full`](HashTableBlockPage::is_full) / [`is_empty`](HashTableBlockPage::is_empty):
//!   Capacity checks
//!
//! # Generics
//!
//! - `KeyType`: The key type (must be `Clone + Default`)
//! - `ValueType`: The value type (must be `Clone + Default + PartialEq`)
//! - `KeyComparator`: A function `Fn(&K, &K) -> bool` for key equality

use std::marker::PhantomData;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};

use log::info;

use crate::storage::page::page_types::hash_table_page_defs::{MappingType, block_array_size};

/**
 * Store indexed key and value together within block page. Supports
 * non-unique keys.
 *
 * Block page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *
 */
pub struct HashTableBlockPage<KeyType, ValueType, KeyComparator> {
    occupied: Vec<AtomicU8>,
    readable: Vec<AtomicU8>,
    array: Mutex<Vec<MappingType<KeyType, ValueType>>>,
    _marker: PhantomData<KeyComparator>,
}

impl<KeyType, ValueType, KeyComparator> Default
    for HashTableBlockPage<KeyType, ValueType, KeyComparator>
where
    KeyComparator: Fn(&KeyType, &KeyType) -> bool,
    KeyType: Clone + Default,
    ValueType: Clone + Default + PartialEq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<KeyType, ValueType, KeyComparator> HashTableBlockPage<KeyType, ValueType, KeyComparator>
where
    KeyComparator: Fn(&KeyType, &KeyType) -> bool,
    KeyType: Clone + Default,
    ValueType: Clone + Default + PartialEq,
{
    /// Creates a new `HashTableBlockPage` with the appropriate size.
    pub fn new() -> Self {
        let array_size = block_array_size::<KeyType, ValueType>();
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

    /// Gets the key at the specified index in the block.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index in the block to get the key from.
    ///
    /// # Returns
    ///
    /// The key at the specified index.
    pub fn key_at(&self, bucket_ind: u32) -> KeyType {
        self.array.lock().unwrap()[bucket_ind as usize].0.clone()
    }

    /// Gets the value at the specified index in the block.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index in the block to get the value from.
    ///
    /// # Returns
    ///
    /// The value at the specified index.
    pub fn value_at(&self, bucket_ind: u32) -> ValueType {
        self.array.lock().unwrap()[bucket_ind as usize].1.clone()
    }

    /// Attempts to insert a key and value into the specified index in the block.
    /// The insert is thread-safe and uses compare-and-swap to claim the index,
    /// then writes the key and value into the index, and marks the index as readable.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to write the key and value to.
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    ///
    /// # Returns
    ///
    /// `true` if the value is inserted successfully, `false` otherwise.
    pub fn insert_at(&self, bucket_ind: u32, key: KeyType, value: ValueType) -> bool {
        let idx = bucket_ind as usize;
        if self.is_occupied(bucket_ind) {
            return false;
        }

        let mut array = self.array.lock().unwrap();
        array[idx] = (key, value);
        drop(array);

        self.set_occupied(bucket_ind);
        self.set_readable(bucket_ind);

        true
    }

    /// Removes a key and value at the specified index.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to remove the value from.
    pub fn remove_at(&self, bucket_ind: u32) {
        let idx = bucket_ind as usize;
        self.readable[idx / 8].fetch_and(!(1 << (bucket_ind % 8)), Ordering::SeqCst);
    }

    /// Checks if the specified index is occupied (key/value pair or tombstone).
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to check.
    ///
    /// # Returns
    ///
    /// `true` if the index is occupied, `false` otherwise.
    pub fn is_occupied(&self, bucket_ind: u32) -> bool {
        let idx = bucket_ind as usize;
        self.occupied[idx / 8].load(Ordering::SeqCst) & (1 << (bucket_ind % 8)) != 0
    }

    /// Checks if the specified index is readable (valid key/value pair).
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to check.
    ///
    /// # Returns
    ///
    /// `true` if the index is readable, `false` otherwise.
    pub fn is_readable(&self, bucket_ind: u32) -> bool {
        let idx = bucket_ind as usize;
        self.readable[idx / 8].load(Ordering::SeqCst) & (1 << (bucket_ind % 8)) != 0
    }

    /// Scans the block and collects values that have the matching key.
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
        for i in 0..block_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) && cmp(&self.key_at(i as u32), &key) {
                result.push(self.value_at(i as u32));
                found = true;
            }
        }
        found
    }

    /// Attempts to insert a key and value in the block.
    /// The insert is thread-safe and uses compare-and-swap to claim the index,
    /// then writes the key and value into the index, and marks the index as readable.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    /// * `cmp` - The comparator function to use for matching keys.
    ///
    /// # Returns
    ///
    /// `true` if inserted, `false` if duplicate key-value pair or block is full.
    pub fn insert(&self, key: KeyType, value: ValueType, cmp: KeyComparator) -> bool {
        if self.is_full() {
            return false;
        }
        for i in 0..block_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) && cmp(&self.key_at(i as u32), &key) {
                return false;
            }
        }
        for i in 0..block_array_size::<KeyType, ValueType>() {
            if !self.is_occupied(i as u32) {
                return self.insert_at(i as u32, key, value);
            }
        }
        false
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
        for i in 0..block_array_size::<KeyType, ValueType>() {
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

    /// Returns the number of readable elements, i.e., the current size.
    ///
    /// # Returns
    ///
    /// The number of readable elements.
    pub fn num_readable(&self) -> u32 {
        let mut count = 0;
        for i in 0..block_array_size::<KeyType, ValueType>() {
            if self.is_readable(i as u32) {
                count += 1;
            }
        }
        count
    }

    /// Checks if the block is full.
    ///
    /// # Returns
    ///
    /// `true` if the block is full, `false` otherwise.
    pub fn is_full(&self) -> bool {
        self.num_readable() == block_array_size::<KeyType, ValueType>() as u32
    }

    /// Checks if the block is empty.
    ///
    /// # Returns
    ///
    /// `true` if the block is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.num_readable() == 0
    }

    /// Prints the block's occupancy information.
    pub fn print_bucket(&self) {
        for i in 0..block_array_size::<KeyType, ValueType>() {
            info!(
                "Index {}: occupied = {}, readable = {}",
                i,
                self.is_occupied(i as u32),
                self.is_readable(i as u32)
            );
        }
    }

    /// Sets the specified index as occupied.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to set as occupied.
    fn set_occupied(&self, bucket_ind: u32) {
        let idx = bucket_ind as usize;
        self.occupied[idx / 8].fetch_or(1 << (bucket_ind % 8), Ordering::SeqCst);
    }

    /// Sets the specified index as readable.
    ///
    /// # Arguments
    ///
    /// * `bucket_ind` - The index to set as readable.
    fn set_readable(&self, bucket_ind: u32) {
        let idx = bucket_ind as usize;
        self.readable[idx / 8].fetch_or(1 << (bucket_ind % 8), Ordering::SeqCst);
    }
}
