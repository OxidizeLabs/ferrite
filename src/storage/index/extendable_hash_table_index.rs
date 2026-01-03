//! # Extendable Hash Table Index
//!
//! This module provides a disk-based **extendable hash table index** for O(1) average-case
//! point lookups. It wraps the [`DiskExtendableHashTable`] container and implements the
//! [`Index`] trait for integration with the query executor.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                     ExtendableHashTableIndex                                │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │   ┌──────────────────────────────────────────────────────────────────────┐  │
//! │   │                         Index Trait API                              │  │
//! │   │  insert_entry() ─┐                              ┌─ scan_key()        │  │
//! │   │  delete_entry() ─┤  ──▶ extract_key_value() ───▶├─ create_iterator() │  │
//! │   │                  │      (single-column key)     └─ get_metadata()    │  │
//! │   └──────────────────┼───────────────────────────────────────────────────┘  │
//! │                      │                                                      │
//! │                      ▼                                                      │
//! │   ┌──────────────────────────────────────────────────────────────────────┐  │
//! │   │              DiskExtendableHashTable (Mutex-protected)               │  │
//! │   │  ┌─────────────────────────────────────────────────────────────────┐ │  │
//! │   │  │  Header Page  ──▶  Directory Pages  ──▶  Bucket Pages           │ │  │
//! │   │  │  (entry point)     (hash → bucket)      (key-value storage)     │ │  │
//! │   │  └─────────────────────────────────────────────────────────────────┘ │  │
//! │   │                              │                                       │  │
//! │   │                              ▼                                       │  │
//! │   │                    BufferPoolManager (disk I/O)                      │  │
//! │   └──────────────────────────────────────────────────────────────────────┘  │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Hash Index vs B+ Tree Index
//!
//! | Feature              | ExtendableHashTableIndex | BPlusTreeIndex       |
//! |----------------------|--------------------------|----------------------|
//! | Point lookup         | O(1) average             | O(log n)             |
//! | Range scan           | ❌ Not supported         | ✅ Efficient          |
//! | Ordered iteration    | ❌ Unordered             | ✅ Sorted order       |
//! | Key types            | Single-column only       | Multi-column         |
//! | Space overhead       | Lower                    | Higher (tree nodes)  |
//! | Best use case        | Equality predicates      | Range/order queries  |
//!
//! ## Key Features
//!
//! - **O(1) Lookups**: Average-case constant-time point queries via hashing
//! - **Dynamic Resizing**: Buckets split automatically as data grows (extendable hashing)
//! - **Disk-Based**: All pages managed through `BufferPoolManager` for persistence
//! - **Thread-Safe**: `Mutex`-protected container for concurrent access
//!
//! ## Limitations
//!
//! - **Single-Column Keys Only**: Multi-column composite keys are not supported
//! - **No Range Scans**: `create_iterator()` returns an empty iterator (logs warning)
//! - **No Ordering**: Hash indexes provide no ordering guarantees
//!
//! ## Index Trait Implementation
//!
//! | Method                | Behavior                                              |
//! |-----------------------|-------------------------------------------------------|
//! | `insert_entry()`      | Hashes key value, inserts into bucket                 |
//! | `delete_entry()`      | Removes key from hash table                           |
//! | `scan_key()`          | Point lookup, returns matching RID(s)                 |
//! | `create_iterator()`   | Returns empty iterator (logs warning)                 |
//! | `create_point_iterator()` | Returns iterator with single matching entry       |
//! | `get_metadata()`      | Returns `IndexInfo` metadata                          |
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! // Create hash index
//! let index =
//!     ExtendableHashTableIndex::try_new(metadata, buffer_pool_manager, HashFunction::new())?;
//!
//! // Insert entry
//! let tuple = Tuple::new(&[Value::from(42)], &schema, rid);
//! index.insert_entry(&tuple, rid, &transaction);
//!
//! // Point lookup
//! let results = index.scan_key(&tuple, &transaction)?;
//! // results: [(Value(42), RID)]
//!
//! // Delete entry
//! index.delete_entry(&tuple, rid, &transaction);
//! ```
//!
//! ## Configuration
//!
//! Default parameters (TODO: make configurable):
//! - `header_max_depth`: 4 (max directory page slots in header)
//! - `directory_max_depth`: 4 (max local depth for directories)
//! - `bucket_max_size`: 4 (max entries per bucket before split)
//!
//! ## Thread Safety
//!
//! The underlying `DiskExtendableHashTable` is protected by a `parking_lot::Mutex`,
//! ensuring exclusive access during insert/delete/lookup operations.

use std::sync::Arc;

use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::container::disk_extendable_hash_table::DiskExtendableHashTable;
use crate::container::hash_function::HashFunction;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::index_iterator_mem::IndexIterator;
use crate::storage::index::{Index, IndexInfo};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;

/// An index implementation backed by [`DiskExtendableHashTable`].
///
/// ## Notes / current limitations
/// - Only **single-column keys** are supported (the underlying hash table hashes a single
///   [`Value`]).
/// - Range scans are not supported (hash indexes are unordered). `create_iterator()` returns an
///   empty iterator and logs a warning.
pub struct ExtendableHashTableIndex {
    metadata: Arc<IndexInfo>,
    container: Mutex<DiskExtendableHashTable>,
}

impl ExtendableHashTableIndex {
    /// Creates a new `ExtendableHashTableIndex` with default sizing parameters.
    ///
    /// Use this constructor from the storage layer, since the [`Index`] trait's `new()` does not
    /// provide access to a buffer pool manager.
    pub fn try_new(
        metadata: Arc<IndexInfo>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        hash_fn: HashFunction<Value>,
    ) -> Result<Self, String> {
        // TODO: make these configurable via metadata or server config.
        let header_max_depth = 4;
        let directory_max_depth = 4;
        let bucket_max_size = 4;

        let container = DiskExtendableHashTable::new(
            metadata.get_index_name().clone(),
            buffer_pool_manager,
            hash_fn,
            header_max_depth,
            directory_max_depth,
            bucket_max_size,
        )?;

        Ok(Self {
            metadata,
            container: Mutex::new(container),
        })
    }

    fn extract_key_value(&self, tuple: &Tuple) -> Result<Value, String> {
        let key_attrs = self.metadata.get_key_attrs();
        if key_attrs.len() != 1 {
            return Err(format!(
                "ExtendableHashTableIndex currently supports only single-column keys (key_attrs={key_attrs:?})"
            ));
        }
        tuple
            .keys_from_tuple_checked(key_attrs)
            .map_err(|e| format!("failed to extract index key from tuple: {e}"))?
            .into_iter()
            .next()
            .ok_or_else(|| "key_attrs.len()==1 implies exactly one key value".to_string())
    }
}

impl Index for ExtendableHashTableIndex {
    fn new(_metadata: Box<IndexInfo>) -> Self
    where
        Self: Sized,
    {
        unimplemented!(
            "ExtendableHashTableIndex requires a BufferPoolManager; use ExtendableHashTableIndex::try_new()"
        )
    }

    fn insert_entry(&mut self, tuple: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
        let key = match self.extract_key_value(tuple) {
            Ok(k) => k,
            Err(e) => {
                warn!("hash index insert skipped: {e}");
                return false;
            },
        };
        self.container.lock().insert(key, rid)
    }

    fn delete_entry(&self, tuple: &Tuple, _rid: RID, _transaction: &Transaction) -> bool {
        let key = match self.extract_key_value(tuple) {
            Ok(k) => k,
            Err(e) => {
                warn!("hash index delete skipped: {e}");
                return false;
            },
        };
        self.container.lock().remove(&key)
    }

    fn scan_key(
        &self,
        key_tuple: &Tuple,
        _transaction: &Transaction,
    ) -> Result<Vec<(Value, RID)>, String> {
        let key = self.extract_key_value(key_tuple)?;
        let rid_opt = self.container.lock().get_value(&key);
        Ok(rid_opt.map(|rid| vec![(key, rid)]).unwrap_or_default())
    }

    fn create_iterator(&self, start_key: Option<Tuple>, end_key: Option<Tuple>) -> IndexIterator {
        // Hash indexes are unordered, so we can't provide correct range iteration here without
        // a full scan API on the underlying container.
        warn!("ExtendableHashTableIndex does not support range scans; returning empty iterator");

        let tree = BPlusTree::new(4, Arc::clone(&self.metadata));
        let tree_arc = Arc::new(RwLock::new(tree));
        IndexIterator::new(tree_arc, start_key.map(Arc::new), end_key.map(Arc::new))
    }

    fn create_point_iterator(&self, key: &Tuple) -> IndexIterator {
        let start_end = Arc::new(key.clone());
        let tree_arc = Arc::new(RwLock::new(BPlusTree::new(4, Arc::clone(&self.metadata))));

        // Populate a temporary B+ tree with at most one entry so we can reuse IndexIterator.
        if let Ok(k) = self.extract_key_value(key)
            && let Some(rid) = self.container.lock().get_value(&k)
        {
            let _ = tree_arc.write().insert(k, rid);
        }

        IndexIterator::new(tree_arc, Some(start_end.clone()), Some(start_end))
    }

    fn get_metadata(&self) -> Arc<IndexInfo> {
        Arc::clone(&self.metadata)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::index::IndexType;
    use crate::types_db::type_id::TypeId;

    async fn create_test_index() -> ExtendableHashTableIndex {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 32;
        const K: usize = 2;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("extendable_hash_index_test.db")
            .to_str()
            .unwrap()
            .to_string();
        let log_path = temp_dir
            .path()
            .join("extendable_hash_index_test.log")
            .to_str()
            .unwrap()
            .to_string();

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(
            BufferPoolManager::new(BUFFER_POOL_SIZE, Arc::new(disk_manager), replacer).unwrap(),
        );

        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let metadata = Arc::new(IndexInfo::new(
            key_schema,
            "test_hash_index".to_string(),
            0,
            "test_table".to_string(),
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0],
        ));

        let hash_fn = HashFunction::new();
        ExtendableHashTableIndex::try_new(metadata, bpm, hash_fn).unwrap()
    }

    fn key_tuple(id: i32) -> Tuple {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        Tuple::new(&[Value::from(id)], &schema, RID::new(0, 0))
    }

    #[tokio::test]
    async fn test_insert_scan_delete_and_point_iter() {
        let mut index = create_test_index().await;
        let txn = Transaction::new(0, IsolationLevel::ReadCommitted);

        let t1 = key_tuple(1);
        let rid1 = RID::new(1, 1);

        assert!(index.insert_entry(&t1, rid1, &txn));

        let result = index.scan_key(&t1, &txn).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Value::from(1));
        assert_eq!(result[0].1, rid1);

        // Point iterator should yield the same RID.
        let mut it = index.create_point_iterator(&t1);
        assert_eq!(it.next(), Some(rid1));

        assert!(index.delete_entry(&t1, rid1, &txn));
        let result2 = index.scan_key(&t1, &txn).unwrap();
        assert!(result2.is_empty());
    }
}
