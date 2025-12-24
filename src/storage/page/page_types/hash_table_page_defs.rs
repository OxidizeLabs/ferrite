//! Common type definitions and capacity calculations for hash table pages.
//!
//! This module provides shared types and constants used across different hash
//! table implementations (linear probing and extendible hashing).
//!
//! # Type Aliases
//!
//! - [`MappingType`]: A `(Key, Value)` tuple stored in hash table pages
//! - [`HashTableBlockType`]: Block page for linear probing hash tables
//! - [`HashTableBucketType`]: Bucket page for extendible hash tables
//!
//! # Capacity Calculations
//!
//! Both block and bucket pages store key-value pairs along with two status bits
//! per entry (occupied and readable). The capacity formulas account for this:
//!
//! ```text
//! Bits per entry = 8 * sizeof(Key, Value) + 2
//!
//! Entries per page = (8 * PAGE_SIZE) / (8 * sizeof(Key, Value) + 2)
//!                  ≈ PAGE_SIZE / (sizeof(Key, Value) + 0.25)
//! ```
//!
//! The functions [`block_array_size`] and [`bucket_array_size`] compute this
//! at compile time for any key/value type combination.
//!
//! # Directory Size
//!
//! The [`DIRECTORY_ARRAY_SIZE`] constant (512) defines the maximum number of
//! bucket pointers in a single directory page. This is chosen as a power of 2
//! while leaving room for directory metadata (page_id, lsn, global_depth,
//! local_depths array).

use crate::storage::page::page_types::hash_table_block_page::HashTableBlockPage;
use crate::storage::page::page_types::hash_table_bucket_page::HashTableBucketPage;
use crate::common::config::DB_PAGE_SIZE;
use std::mem::size_of;

pub type MappingType<KeyType, ValueType> = (KeyType, ValueType);

pub const fn index_template_arguments<KeyType, ValueType, KeyComparator>() {}

/**
 * Linear Probe Hashing Definitions
 */
pub type HashTableBlockType<KeyType, ValueType, KeyComparator> =
    HashTableBlockPage<KeyType, ValueType, KeyComparator>;

/**
 * BLOCK_ARRAY_SIZE is the number of (key, value) pairs that can be stored in a linear probe hash block page. It is an
 * approximate calculation based on the size of MappingType (which is a std::pair of KeyType and ValueType). For each
 * key/value pair, we need two additional bits for occupied_ and readable_. 4 * BUSTUB_PAGE_SIZE / (4 * sizeof
 * (MappingType) + 1) = BUSTUB_PAGE_SIZE/(sizeof (MappingType) + 0.25) because 0.25 bytes = 2 bits is the space required
 * to maintain the occupied and readable flags for a key value pair.
 */
/**
 * BLOCK_ARRAY_SIZE is the number of (key, value) pairs that can be stored in a linear probe hash block page. It is an
 * approximate calculation based on the size of MappingType (which is a std::pair of KeyType and ValueType). For each
 * key/value pair, we need two additional bits for occupied_ and readable_. 4 * DB_PAGE_SIZE / (4 * sizeof
 * (MappingType) + 1) = DB_PAGE_SIZE/(sizeof (MappingType) + 0.25) because 0.25 bytes = 2 bits is the space required
 * to maintain the occupied and readable flags for a key value pair.
 */
pub const fn block_array_size<KeyType, ValueType>() -> usize {
    ((4 * DB_PAGE_SIZE) as usize) / (4 * size_of::<MappingType<KeyType, ValueType>>() + 1)
}

/**
 * Extendible Hashing Definitions
 */
pub type HashTableBucketType<KeyType, ValueType, KeyComparator> =
    HashTableBucketPage<KeyType, ValueType, KeyComparator>;

/**
 * BUCKET_ARRAY_SIZE is the number of (key, value) pairs that can be stored in an extendible hash index bucket page.
 * The computation is the same as the above BLOCK_ARRAY_SIZE, but blocks and buckets have different implementations
 * of search, insertion, removal, and helper methods.
 */
pub const fn bucket_array_size<KeyType, ValueType>() -> usize {
    ((4 * DB_PAGE_SIZE) as usize) / (4 * size_of::<MappingType<KeyType, ValueType>>() + 1)
}

/**
 * DIRECTORY_ARRAY_SIZE is the number of page_ids that can fit in the directory page of an extendible hash index.
 * This is 512 because the directory array must grow in powers of 2, and 1024 page_ids leaves zero room for
 * storage of the other member variables: page_id_, lsn_, global_depth_, and the array local_depths_.
 * Extending the directory implementation to span multiple pages would be a meaningful improvement to the
 * implementation.
 */
pub const DIRECTORY_ARRAY_SIZE: usize = 512;
