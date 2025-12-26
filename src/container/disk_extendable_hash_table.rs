//! # Disk-Based Extendable Hash Table
//!
//! This module provides `DiskExtendableHashTable`, an extendable hash table backed
//! by the buffer pool manager. It supports dynamic growth/shrinkage through bucket
//! splitting and merging, and allows non-unique keys.
//!
//! ## Architecture
//!
//! ```text
//!   Three-Level Structure
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Level 1: Header Page (single page, top-level entry point)
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                    ExtendableHTableHeaderPage                           │
//!   │                                                                         │
//!   │   max_depth: 4  (supports up to 2^4 = 16 directory pages)               │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  directory_page_ids[0..16]                                      │   │
//!   │   │  [Dir0] [Dir0] [Dir0] [Dir0] [Dir1] [Dir1] ...                  │   │
//!   │   └────┬─────────────────────────────┬──────────────────────────────┘   │
//!   └────────┼─────────────────────────────┼──────────────────────────────────┘
//!            │                             │
//!            ▼                             ▼
//!   Level 2: Directory Pages (multiple, map hash → bucket)
//!   ┌─────────────────────────────┐  ┌─────────────────────────────┐
//!   │ ExtendableHTableDirectoryPage│  │ ExtendableHTableDirectoryPage│
//!   │                             │  │                             │
//!   │ global_depth: 2             │  │ global_depth: 2             │
//!   │                             │  │                             │
//!   │ bucket_page_ids:            │  │ bucket_page_ids:            │
//!   │ [B0] [B1] [B0] [B1]         │  │ [B2] [B3] [B2] [B3]         │
//!   │                             │  │                             │
//!   │ local_depths:               │  │ local_depths:               │
//!   │ [1]  [1]  [1]  [1]          │  │ [1]  [1]  [1]  [1]          │
//!   └───┬────┬────────────────────┘  └───┬────┬────────────────────┘
//!       │    │                           │    │
//!       ▼    ▼                           ▼    ▼
//!   Level 3: Bucket Pages (store actual key-value pairs)
//!   ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
//!   │ Bucket Page B0  │ │ Bucket Page B1  │ │ Bucket Page B2  │ ...
//!   │                 │ │                 │ │                 │
//!   │ [(k1,v1)]       │ │ [(k2,v2)]       │ │ [(k4,v4)]       │
//!   │ [(k3,v3)]       │ │                 │ │ [(k5,v5)]       │
//!   └─────────────────┘ └─────────────────┘ └─────────────────┘
//! ```
//!
//! ## Lookup Flow
//!
//! ```text
//!   get_value(key)
//!        │
//!        ▼
//!   ┌────────────────┐
//!   │ hash = H(key)  │
//!   └───────┬────────┘
//!           │
//!           ▼
//!   ┌────────────────────────────────────────┐
//!   │ Header: directory_index =              │
//!   │         hash >> (32 - header_max_depth)│
//!   └───────────────────┬────────────────────┘
//!                       │
//!                       ▼
//!   ┌────────────────────────────────────────┐
//!   │ Directory: bucket_index =              │
//!   │            hash & ((1 << global_depth) │
//!   │                      - 1)              │
//!   └───────────────────┬────────────────────┘
//!                       │
//!                       ▼
//!   ┌────────────────────────────────────────┐
//!   │ Bucket: linear scan for key match      │
//!   │         return value if found          │
//!   └────────────────────────────────────────┘
//! ```
//!
//! ## Bucket Split Process
//!
//! ```text
//!   insert(key, value) when bucket is full
//!        │
//!        ▼
//!   ┌───────────────────────────────────────────────────────────────────────┐
//!   │                         Before Split                                  │
//!   │                                                                       │
//!   │   Directory (global_depth = 1):  [B0] [B0]  ← both point to same      │
//!   │   Bucket B0 (local_depth = 1):   [k1] [k2] [k3] [FULL!]               │
//!   └───────────────────────────────────────────────────────────────────────┘
//!        │
//!        │  1. Create new bucket B1
//!        │  2. Increment local_depth of B0 and B1
//!        │  3. Update directory pointers
//!        │  4. Redistribute entries based on new bit
//!        ▼
//!   ┌───────────────────────────────────────────────────────────────────────┐
//!   │                         After Split                                   │
//!   │                                                                       │
//!   │   Directory (global_depth = 1):  [B0] [B1]  ← now different           │
//!   │   Bucket B0 (local_depth = 2):   [k1] [k3]  ← keys with bit 0 = 0     │
//!   │   Bucket B1 (local_depth = 2):   [k2]       ← keys with bit 0 = 1     │
//!   └───────────────────────────────────────────────────────────────────────┘
//!
//!   If global_depth < local_depth after split → directory doubles
//! ```
//!
//! ## Key Components
//!
//! | Component                  | Description                                   |
//! |----------------------------|-----------------------------------------------|
//! | `DiskExtendableHashTable`  | Main struct managing the hash table           |
//! | `header_page_id`           | Root page ID for lookups                      |
//! | `hash_fn`                  | Hash function for key → u32 conversion        |
//! | `directory_max_depth`      | Maximum depth for directory (limits buckets)  |
//! | `bucket_max_size`          | Maximum entries per bucket before split       |
//!
//! ## Core Operations
//!
//! | Method          | Description                                          |
//! |-----------------|------------------------------------------------------|
//! | `new()`         | Create hash table with header, directory, and bucket |
//! | `insert()`      | Insert key-value pair, split bucket if full          |
//! | `get_value()`   | Lookup value by key, returns `Option<RID>`           |
//! | `remove()`      | Remove key-value pair, merge bucket if empty         |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::container::disk_extendable_hash_table::DiskExtendableHashTable;
//! use crate::container::hash_function::HashFunction;
//! use crate::common::rid::RID;
//! use crate::types_db::value::Value;
//!
//! // Create hash table
//! let mut ht = DiskExtendableHashTable::new(
//!     "my_index".to_string(),
//!     bpm.clone(),
//!     HashFunction::new(),
//!     4,   // header_max_depth: up to 16 directory pages
//!     4,   // directory_max_depth: up to 16 buckets per directory
//!     256, // bucket_max_size: entries per bucket
//! )?;
//!
//! // Insert entries
//! let key = Value::from(42);
//! let rid = RID::new(1, 0);
//! ht.insert(key.clone(), rid);
//!
//! // Lookup
//! let result = ht.get_value(&key);
//! assert_eq!(result, Some(rid));
//!
//! // Remove
//! ht.remove(&key);
//! assert_eq!(ht.get_value(&key), None);
//! ```
//!
//! ## Configuration
//!
//! | Parameter             | Effect                                          |
//! |-----------------------|-------------------------------------------------|
//! | `header_max_depth`    | Controls max directory pages (2^depth)          |
//! | `directory_max_depth` | Controls max buckets per directory (2^depth)    |
//! | `bucket_max_size`     | Entries before split; smaller = more splits     |
//!
//! ## Thread Safety
//!
//! The current implementation is **not thread-safe**. The `insert()` and `remove()`
//! methods require `&mut self`. External synchronization (e.g., `RwLock`) is needed
//! for concurrent access. Read-only `get_value()` takes `&self` but may race with
//! concurrent modifications.
//!
//! ## Limitations
//!
//! - **Non-unique keys**: Multiple values can be stored for the same key, but only
//!   the first match is returned by `get_value()`.
//! - **No range scans**: Hash tables only support point lookups.
//! - **Insert retry limit**: Failed splits after `MAX_INSERT_RETRIES` cause insert
//!   to return `false`.

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::common::config::INVALID_PAGE_ID;
use crate::common::config::PageId;
use crate::common::rid::RID;
use crate::container::hash_function::HashFunction;
use crate::storage::page::PageTrait;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use crate::types_db::value::Value;
use log::{debug, info};
use std::sync::Arc;

/// Implementation of an extendable hash table backed by a buffer pool manager.
///
/// This structure provides a disk-based hash index that supports dynamic growth
/// and shrinkage through bucket splitting and merging. Non-unique keys are supported.
///
/// See the module-level documentation for architecture details and usage examples.
///
/// # Thread Safety
///
/// The current implementation is **not thread-safe**. The `insert()` and `remove()`
/// methods require `&mut self`. External synchronization (e.g., `RwLock`) is needed
/// for concurrent access.
pub struct DiskExtendableHashTable {
    /// Reference to the buffer pool manager for page I/O operations.
    bpm: Arc<BufferPoolManager>,
    /// Hash function used to compute bucket indices from keys.
    hash_fn: HashFunction<Value>,
    /// Maximum depth for directory pages, limiting the number of buckets per directory.
    /// The maximum number of buckets is `2^directory_max_depth`.
    directory_max_depth: u32,
    /// Maximum number of entries a bucket can hold before requiring a split.
    bucket_max_size: u32,
    /// Page ID of the header page, which is the root entry point for all lookups.
    header_page_id: PageId,
}

impl DiskExtendableHashTable {
    /// Creates a new `DiskExtendableHashTable` with the specified configuration.
    ///
    /// This constructor allocates and initializes the header page, a single directory
    /// page, and an initial empty bucket page. All directory entries in the header
    /// initially point to the same directory page.
    ///
    /// # Parameters
    ///
    /// - `name`: A descriptive name for the hash table (used for logging).
    /// - `bpm`: The buffer pool manager for page allocation and I/O.
    /// - `hash_fn`: The hash function used to compute bucket indices from keys.
    /// - `header_max_depth`: Maximum depth for the header, controlling the maximum
    ///   number of directory pages (`2^header_max_depth`).
    /// - `directory_max_depth`: Maximum depth for each directory, controlling the
    ///   maximum number of buckets per directory (`2^directory_max_depth`).
    /// - `bucket_max_size`: Maximum number of entries per bucket before splitting.
    ///
    /// # Returns
    ///
    /// - `Ok(Self)`: The newly created hash table.
    /// - `Err(String)`: An error message if page allocation fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let ht = DiskExtendableHashTable::new(
    ///     "my_index".to_string(),
    ///     bpm.clone(),
    ///     HashFunction::new(),
    ///     4,   // up to 16 directory pages
    ///     4,   // up to 16 buckets per directory
    ///     256, // 256 entries per bucket
    /// )?;
    /// ```
    pub fn new(
        name: String,
        bpm: Arc<BufferPoolManager>,
        hash_fn: HashFunction<Value>,
        header_max_depth: u32,
        directory_max_depth: u32,
        bucket_max_size: u32,
    ) -> Result<Self, String> {
        info!("Creating DiskExtendableHashTable with index name: {}", name);

        // Create header page
        let header_page = bpm
            .new_page::<ExtendableHTableHeaderPage>()
            .ok_or_else(|| "Failed to create header page".to_string())?;
        let header_page_id = {
            let header = header_page.read();
            header.get_page_id()
        };

        // Initialize header page
        {
            let mut header = header_page.write();
            header.init(header_max_depth);
            debug!(
                "Initialized header page with max depth: {}",
                header_max_depth
            );
        }

        // Create directory page
        let directory_page = bpm
            .new_page::<ExtendableHTableDirectoryPage>()
            .ok_or_else(|| "Failed to create directory page".to_string())?;
        let directory_page_id = directory_page.read().get_page_id();

        // Initialize directory page
        {
            let mut directory = directory_page.write();
            directory.init(directory_max_depth);

            // Create first bucket page
            let bucket_page = bpm
                .new_page::<ExtendableHTableBucketPage>()
                .ok_or_else(|| "Failed to create bucket page".to_string())?;
            let bucket_page_id = bucket_page.read().get_page_id();

            // Initialize bucket page
            {
                let mut bucket = bucket_page.write();
                bucket.init(bucket_max_size as u16);
            }

            // Set up initial directory entry
            directory.set_bucket_page_id(0, bucket_page_id);
            debug!(
                "Set initial bucket page {} in directory at index 0",
                bucket_page_id
            );
        }

        // Update header page with directory page ID
        {
            let mut header = header_page.write();
            // Set directory page ID for all possible directory indices initially
            for i in 0..(1 << header_max_depth) {
                header.set_directory_page_id(i, directory_page_id);
            }
            debug!(
                "Set directory page {} in header for all indices",
                directory_page_id
            );
        }

        Ok(Self {
            bpm,
            hash_fn,
            directory_max_depth,
            bucket_max_size,
            header_page_id,
        })
    }

    /// Maximum number of retry attempts for insert operations.
    ///
    /// When a bucket is full and needs to be split, the insert operation retries
    /// after splitting. This constant limits retries to prevent infinite loops
    /// in pathological cases (e.g., many keys hashing to the same bucket).
    const MAX_INSERT_RETRIES: usize = 10;

    /// Inserts a key-value pair into the hash table.
    ///
    /// If the target bucket is full, this method automatically splits the bucket
    /// and retries the insertion. The operation may fail if the maximum retry
    /// limit is reached or if the bucket cannot be split (e.g., maximum depth).
    ///
    /// # Parameters
    ///
    /// - `key`: The key to insert.
    /// - `value`: The RID (record identifier) to associate with the key.
    ///
    /// # Returns
    ///
    /// - `true`: The key-value pair was successfully inserted.
    /// - `false`: Insertion failed (bucket full and cannot split, or max retries exceeded).
    ///
    /// # Note
    ///
    /// Non-unique keys are supported. Inserting a duplicate key adds another entry
    /// rather than updating the existing one.
    pub fn insert(&mut self, key: Value, value: RID) -> bool {
        debug!("Starting insert operation for key: {:?}", key);
        let mut attempt = 0;
        loop {
            debug!("Insert attempt {} for key: {:?}", attempt, key);
            let hash = self.hash_fn.get_hash(&key) as u32;
            debug!("Computed hash: {} for key: {:?}", hash, key);

            // Get directory index from header page
            let header_page = match self
                .bpm
                .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)
            {
                Some(page) => page,
                None => return false,
            };

            let directory_index = {
                let header = header_page.read();
                header.hash_to_directory_index(hash)
            } as usize;

            debug!("Mapped hash to directory index: {}", directory_index);

            // Get directory page and find bucket index
            let directory_page = match self.get_directory_page(directory_index) {
                Some(page) => page,
                None => {
                    debug!("Failed to get directory page for index {}", directory_index);
                    return false;
                },
            };

            let bucket_index = {
                let directory = directory_page.read();
                directory.hash_to_bucket_index(hash)
            } as usize;

            debug!("Mapped hash to bucket index: {}", bucket_index);

            // Get or create bucket page
            let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
                Some(page) => page,
                None => {
                    debug!("Failed to get bucket page for index {}", bucket_index);
                    return false;
                },
            };

            // Try to insert into bucket
            let mut bucket = bucket_page.write();
            if !bucket.is_full() {
                if bucket.insert(key.clone(), value) {
                    debug!("Successfully inserted key: {:?}", key);
                    return true;
                }
            } else {
                // Bucket is full, need to split
                debug!("Bucket is full, splitting required");
                drop(bucket);
                if self.split_bucket_internal(directory_index, bucket_index) {
                    attempt += 1;
                    if attempt < Self::MAX_INSERT_RETRIES {
                        continue;
                    }
                }
            }
            return false;
        }
    }

    /// Looks up the value associated with a key.
    ///
    /// Traverses the header → directory → bucket chain to find the key.
    /// For non-unique keys, returns the first matching entry found.
    ///
    /// # Parameters
    ///
    /// - `key`: The key to look up.
    ///
    /// # Returns
    ///
    /// - `Some(RID)`: The record identifier if the key is found.
    /// - `None`: The key does not exist in the hash table.
    ///
    /// # Performance
    ///
    /// Average case O(1) with low load factor. Bucket lookup is O(n) where n
    /// is the number of entries in the bucket.
    pub fn get_value(&self, key: &Value) -> Option<RID> {
        debug!("Get value called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;
        debug!("Computed hash: {} for key: {:?}", hash, key);

        // Get directory index from header
        let header_page = self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)?;
        let directory_index = {
            let header = header_page.read();
            header.hash_to_directory_index(hash)
        } as usize;

        // Get bucket index from directory
        let directory_page = self.get_directory_page(directory_index)?;
        let bucket_index = {
            let directory = directory_page.read();
            directory.hash_to_bucket_index(hash)
        } as usize;

        // Get bucket and lookup key
        let bucket_page = self.get_bucket_page(directory_index, bucket_index)?;
        let bucket = bucket_page.read();
        bucket.lookup(key)
    }

    /// Removes a key-value pair from the hash table.
    ///
    /// If the bucket becomes empty after removal, this method attempts to merge
    /// it with a sibling bucket to reclaim space and reduce directory entries.
    ///
    /// # Parameters
    ///
    /// - `key`: The key to remove.
    ///
    /// # Returns
    ///
    /// - `true`: The key was found and removed.
    /// - `false`: The key was not found in the hash table.
    ///
    /// # Note
    ///
    /// For non-unique keys, this removes only the first matching entry.
    pub fn remove(&mut self, key: &Value) -> bool {
        debug!("Remove called for key: {:?}", key);
        let hash = self.hash_fn.get_hash(key) as u32;

        // Get directory index from header
        let header_page = match self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)
        {
            Some(page) => page,
            None => return false,
        };

        let directory_index = {
            let header = header_page.read();
            header.hash_to_directory_index(hash)
        } as usize;

        // Get bucket index from directory
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        let bucket_index = {
            let directory = directory_page.read();
            directory.hash_to_bucket_index(hash)
        } as usize;

        // Remove from bucket
        let bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        let removed = {
            let mut bucket = bucket_page.write();
            bucket.remove(key)
        };

        // If removal was successful and bucket is empty, try to merge
        if removed {
            let is_empty = bucket_page.read().is_empty();
            if is_empty {
                self.merge_bucket(directory_index, bucket_index);
            }
        }

        removed
    }

    /// Fetches a directory page by its index in the header.
    ///
    /// # Parameters
    ///
    /// - `directory_index`: The index into the header's directory page ID array.
    ///
    /// # Returns
    ///
    /// - `Some(PageGuard)`: The directory page if found.
    /// - `None`: The header page couldn't be fetched, the index is invalid,
    ///   or the directory page ID is `INVALID_PAGE_ID`.
    fn get_directory_page(
        &self,
        directory_index: usize,
    ) -> Option<PageGuard<ExtendableHTableDirectoryPage>> {
        let header_page = self
            .bpm
            .fetch_page::<ExtendableHTableHeaderPage>(self.header_page_id)?;
        let directory_page_id = header_page.read().get_directory_page_id(directory_index)?;

        if directory_page_id == INVALID_PAGE_ID {
            None
        } else {
            self.bpm
                .fetch_page::<ExtendableHTableDirectoryPage>(directory_page_id)
        }
    }

    /// Fetches a bucket page by its directory and bucket indices.
    ///
    /// # Parameters
    ///
    /// - `directory_index`: The index of the directory page in the header.
    /// - `bucket_index`: The index of the bucket within the directory.
    ///
    /// # Returns
    ///
    /// - `Some(PageGuard)`: The bucket page if found.
    /// - `None`: The directory or bucket page couldn't be fetched, or the
    ///   bucket page ID is `INVALID_PAGE_ID`.
    fn get_bucket_page(
        &self,
        directory_index: usize,
        bucket_index: usize,
    ) -> Option<PageGuard<ExtendableHTableBucketPage>> {
        let directory_page = self.get_directory_page(directory_index)?;
        let bucket_page_id = directory_page.read().get_bucket_page_id(bucket_index)?;

        if bucket_page_id == INVALID_PAGE_ID {
            None
        } else {
            self.bpm
                .fetch_page::<ExtendableHTableBucketPage>(bucket_page_id)
        }
    }

    /// Splits a full bucket into two buckets.
    ///
    /// This method handles the core bucket splitting logic:
    /// 1. Creates a new bucket page.
    /// 2. Increments the local depth of both buckets.
    /// 3. Updates directory pointers to reference the new bucket.
    /// 4. Redistributes entries between old and new buckets based on the new bit.
    ///
    /// # Parameters
    ///
    /// - `directory_index`: The index of the directory page containing the bucket.
    /// - `bucket_index`: The index of the bucket to split.
    ///
    /// # Returns
    ///
    /// - `true`: The bucket was successfully split.
    /// - `false`: Split failed (max depth reached, page allocation failed, etc.).
    fn split_bucket_internal(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        debug!(
            "Splitting bucket for directory index {} and bucket index {}",
            directory_index, bucket_index
        );

        // Get directory page
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Check if we've hit the maximum local depth
        let local_depth = {
            let directory = directory_page.read();
            directory.get_local_depth(bucket_index as u32)
        };

        if local_depth >= self.directory_max_depth {
            debug!("Cannot split bucket: maximum local depth reached");
            return false;
        }

        // Create new bucket page
        let new_bucket_page = match self.bpm.new_page::<ExtendableHTableBucketPage>() {
            Some(page) => page,
            None => return false,
        };
        let new_bucket_page_id = new_bucket_page.read().get_page_id();

        // Initialize the new bucket
        {
            let mut new_bucket = new_bucket_page.write();
            new_bucket.init(self.bucket_max_size as u16);
        }

        // Get the old bucket page
        let old_bucket_page = match self.get_bucket_page(directory_index, bucket_index) {
            Some(page) => page,
            None => return false,
        };

        // Update directory and redistribute entries
        {
            let mut directory = directory_page.write();

            // Use the new directory page method to handle the split
            if let Some(_split_bucket_index) =
                directory.update_directory_for_split(bucket_index, new_bucket_page_id)
            {
                // Redistribute entries between old and new buckets
                let mut old_bucket = old_bucket_page.write();
                let mut new_bucket = new_bucket_page.write();

                let entries = old_bucket.get_all_entries();
                old_bucket.clear();

                // Redistribute entries based on the new local depth
                let new_local_depth = directory.get_local_depth(bucket_index as u32);
                let mask = (1 << new_local_depth) - 1;

                for (key, value) in entries {
                    let hash = self.hash_fn.get_hash(&key) as u32;
                    let target_bucket_index = hash & mask;

                    if target_bucket_index == bucket_index as u32 {
                        old_bucket.insert(key, value);
                    } else {
                        new_bucket.insert(key, value);
                    }
                }

                return true;
            }
        }

        false
    }

    /// Attempts to merge an empty bucket with its sibling.
    ///
    /// When a bucket becomes empty after a removal, this method tries to merge
    /// it with its sibling bucket (the bucket that differs only in the highest
    /// bit of the local depth). If successful, the merged bucket page is deleted.
    ///
    /// # Parameters
    ///
    /// - `directory_index`: The index of the directory page containing the bucket.
    /// - `bucket_index`: The index of the empty bucket to merge.
    ///
    /// # Returns
    ///
    /// - `true`: The bucket was successfully merged and deleted.
    /// - `false`: Merge was not possible (no sibling, sibling not empty, etc.).
    fn merge_bucket(&mut self, directory_index: usize, bucket_index: usize) -> bool {
        let directory_page = match self.get_directory_page(directory_index) {
            Some(page) => page,
            None => return false,
        };

        // Try to merge buckets using the new directory page method
        let removed_bucket_id = {
            let mut directory = directory_page.write();
            directory.merge_bucket(bucket_index)
        };

        // If merge was successful, delete the removed bucket page
        if let Some(page_id) = removed_bucket_id {
            let _ = self.bpm.delete_page(page_id);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    Arc::from(disk_manager.unwrap()),
                    replacer.clone(),
                )
                .unwrap(),
            );

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let test_context = TestContext::new("test_insert_and_get").await;
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht =
            DiskExtendableHashTable::new("test_table".to_string(), bpm, hash_fn, 4, 4, 4).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let key2 = Value::from(2);
        let key3 = Value::from(3);
        let rid1 = RID::new(1, 0);
        let rid2 = RID::new(2, 0);

        // Test insert
        assert!(ht.insert(key1.clone(), rid1));
        assert!(ht.insert(key2.clone(), rid2));

        // Test get
        assert_eq!(ht.get_value(&key1), Some(rid1));
        assert_eq!(ht.get_value(&key2), Some(rid2));
        assert_eq!(ht.get_value(&key3), None);
    }

    #[tokio::test]
    async fn test_remove() {
        let test_context = TestContext::new("test_remove").await;
        let bpm = test_context.bpm();
        let hash_fn = HashFunction::new();

        let mut ht =
            DiskExtendableHashTable::new("test_table".to_string(), bpm, hash_fn, 4, 4, 4).unwrap();

        // Create test values
        let key1 = Value::from(1);
        let rid1 = RID::new(1, 1);

        // Test insert and remove
        assert!(ht.insert(key1.clone(), rid1));
        assert!(ht.remove(&key1));
        assert_eq!(ht.get_value(&key1), None);
    }

    #[tokio::test]
    async fn test_full_bucket() {
        initialize_logger();
        let test_context = TestContext::new("test_full_bucket").await;
        let bpm = test_context.bpm.clone();
        let hash_fn = HashFunction::new();

        debug!("Creating hash table...");
        let mut ht = DiskExtendableHashTable::new(
            "test_table".to_string(),
            bpm,
            hash_fn,
            4,
            4,
            2, // Small bucket size to test splitting
        )
        .unwrap();

        debug!("Starting insertions...");
        // Insert enough items to cause bucket split
        for i in 0..5 {
            debug!("Inserting item {}", i);
            let key = Value::new(i);
            let rid = RID::new(i, 0);
            assert!(ht.insert(key, rid), "Failed to insert item {}", i);
        }

        debug!("Verifying insertions...");
        // Verify all values can still be retrieved
        for i in 0..5 {
            let key = Value::from(i);
            assert_eq!(
                ht.get_value(&key),
                Some(RID::new(i, 0)),
                "Failed to retrieve item {}",
                i
            );
        }
        debug!("Test completed successfully");
    }
}
