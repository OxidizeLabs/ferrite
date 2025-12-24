//! # Index Abstractions and Metadata
//!
//! This module defines the core abstractions for the index subsystem, providing
//! a unified interface for different index implementations (B+ tree, hash table, etc.).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                        Index Subsystem                                  │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐    │
//! │  │                      Index (trait)                              │    │
//! │  │  ┌─────────────────┬───────────────────┬─────────────────────┐  │    │
//! │  │  │  insert_entry() │   delete_entry()  │     scan_key()      │  │    │
//! │  │  │  create_iterator() │ create_point_iterator() │ get_metadata()│  │    │
//! │  │  └─────────────────┴───────────────────┴─────────────────────┘  │    │
//! │  └──────────────────────────────┬──────────────────────────────────┘    │
//! │                                 │ implemented by                        │
//! │                 ┌───────────────┼───────────────┐                       │
//! │                 ▼               ▼               ▼                       │
//! │          ┌──────────┐    ┌──────────┐    ┌──────────┐                   │
//! │          │ BPlusTree│    │HashTable │    │  Future  │                   │
//! │          │  Index   │    │  Index   │    │  Index   │                   │
//! │          └──────────┘    └──────────┘    └──────────┘                   │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐    │
//! │  │                      IndexInfo                                  │    │
//! │  │  ┌──────────────┬───────────────┬─────────────┬─────────────┐   │    │
//! │  │  │  key_schema  │  index_name   │  index_oid  │ table_name  │   │    │
//! │  │  │  key_size    │ is_primary_key│ index_type  │  key_attrs  │   │    │
//! │  │  └──────────────┴───────────────┴─────────────┴─────────────┘   │    │
//! │  └─────────────────────────────────────────────────────────────────┘    │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! - **[`Index`]**: Trait defining the interface for all index implementations
//! - **[`IndexInfo`]**: Metadata struct containing index properties and schema
//! - **[`IndexType`]**: Enum of supported index types (B+ tree, hash table, etc.)
//!
//! ## Index Trait
//!
//! The [`Index`] trait provides a unified interface for index operations:
//!
//! | Method | Description |
//! |--------|-------------|
//! | `insert_entry()` | Insert a key-RID pair into the index |
//! | `delete_entry()` | Remove a key-RID pair from the index |
//! | `scan_key()` | Find all RIDs matching a key |
//! | `create_iterator()` | Create a range scan iterator |
//! | `create_point_iterator()` | Create a point lookup iterator |
//! | `get_metadata()` | Access index metadata |
//!
//! ## IndexInfo Metadata
//!
//! The [`IndexInfo`] struct stores comprehensive metadata about an index:
//!
//! - **key_schema**: Schema defining the types of indexed columns
//! - **index_name**: Human-readable name for the index
//! - **index_oid**: Unique object identifier for catalog lookups
//! - **table_name**: Name of the table this index belongs to
//! - **key_size**: Total size of the index key in bytes
//! - **is_primary_key**: Whether this is the primary key index
//! - **index_type**: The type of index structure (B+ tree, hash, etc.)
//! - **key_attrs**: Column indices in the base table that form the key
//!
//! ## Supported Index Types
//!
//! | Type | Description | Use Case |
//! |------|-------------|----------|
//! | `BPlusTreeIndex` | Balanced tree with sorted keys | Range scans, ordered access |
//! | *(future)* `HashTableIndex` | Hash-based lookup | Point queries |
//!
//! ## Example Usage
//!
//! ```ignore
//! // Create index metadata
//! let info = IndexInfo::new(
//!     key_schema,
//!     "users_pk".to_string(),
//!     1,  // index_oid
//!     "users".to_string(),
//!     4,  // key_size (i32)
//!     true,  // is_primary_key
//!     IndexType::BPlusTreeIndex,
//!     vec![0],  // key_attrs (first column)
//! );
//!
//! // Use with an index implementation
//! let index = BPlusTree::new(Box::new(info));
//! let txn = Transaction::new(...);
//!
//! // Insert entries
//! index.insert_entry(&key_tuple, RID::new(1, 5), &txn);
//!
//! // Point lookup
//! let results = index.scan_key(&search_key, &txn)?;
//!
//! // Range scan with iterator
//! let iter = index.create_iterator(Some(start), Some(end));
//! for (key, rid) in iter {
//!     // Process entries
//! }
//! ```
//!
//! ## Thread Safety
//!
//! The [`Index`] trait requires `Send + Sync`, ensuring all implementations
//! are safe for concurrent access. Individual implementations handle their
//! own synchronization (e.g., `RwLock` for in-memory trees, latch crabbing
//! for disk-based trees).

use crate::catalog::schema::Schema;
use crate::common::config::IndexOidT;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::index_iterator_mem::IndexIterator;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use core::fmt;
use parking_lot::RwLock;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub enum IndexType {
    BPlusTreeIndex,
    // HashTableIndex,
    // STLOrderedIndex,
    // STLUnorderedIndex,
}

/// The IndexInfo struct maintains metadata about an index.
/// The IndexInfo struct maintains metadata about an index.
#[derive(Debug)]
pub struct IndexInfo {
    /// The schema for the index key
    key_schema: Schema,
    /// The name of the index
    index_name: String,
    /// The unique OID for the index
    index_oid: IndexOidT,
    /// The name of the table on which the index is created
    table_name: String,
    /// The size of the index key, in bytes
    key_size: usize,
    /// Is primary key index?
    is_primary_key: bool,
    /// The index type
    index_type: IndexType,
    /// The key attributes (column indices) in the table
    key_attrs: Vec<usize>,
}

impl IndexInfo {
    /// Constructs a new IndexInfo instance.
    ///
    /// # Parameters
    /// - `key_schema`: The schema for the index key.
    /// - `name`: The name of the index.
    /// - `index`: An owning pointer to the index.
    /// - `index_oid`: The unique OID for the index.
    /// - `table_name`: The name of the table on which the index is created.
    /// - `key_size`: The size of the index key, in bytes.
    /// - `is_primary_key`: Indicates if it is a primary key index.
    /// - `index_type`: The index type.
    pub fn new(
        key_schema: Schema,
        index_name: String,
        index_oid: IndexOidT,
        table_name: String,
        key_size: usize,
        is_primary_key: bool,
        index_type: IndexType,
        key_attrs: Vec<usize>,
    ) -> Self {
        IndexInfo {
            key_schema,
            index_name,
            index_oid,
            table_name,
            key_size,
            is_primary_key,
            index_type,
            key_attrs,
        }
    }

    pub fn create_dummy_key(&self) -> Tuple {
        let key_schema = self.get_key_schema();
        Tuple::new(&[Value::new(0)], key_schema, RID::new(0, 0))
    }

    pub fn get_key_schema(&self) -> &Schema {
        &self.key_schema
    }

    /// Returns the key attributes (column indices) for this index
    pub fn get_key_attrs(&self) -> &Vec<usize> {
        &self.key_attrs
    }

    pub fn get_index_oid(&self) -> IndexOidT {
        self.index_oid
    }

    pub fn get_index_name(&self) -> &String {
        &self.index_name
    }

    pub fn get_index_type(&self) -> &IndexType {
        &self.index_type
    }

    pub fn get_key_size(&self) -> usize {
        self.key_size
    }

    pub fn get_index_column_count(&self) -> u32 {
        self.key_schema.get_column_count()
    }

    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    // Creates an iterator for scanning the index
    pub fn create_iterator(
        &self,
        tree: Arc<RwLock<BPlusTree>>,
        start_key: Option<Arc<Tuple>>,
        end_key: Option<Arc<Tuple>>,
    ) -> IndexIterator {
        IndexIterator::new(tree, start_key, end_key)
    }
}

/// Base class for derived indices of different types.
pub trait Index: Send + Sync {
    /// Constructs a new `Index` instance.
    ///
    /// # Parameters
    /// - `metadata`: An owning pointer to the index metadata.
    fn new(metadata: Box<IndexInfo>) -> Self
    where
        Self: Sized;

    /// Returns the number of indexed columns.
    fn get_index_column_count(&self) -> u32 {
        self.get_metadata().get_index_column_count()
    }

    /// Returns the index name.
    fn get_index_name(&self) -> String {
        self.get_metadata().get_index_name().clone()
    }

    /// Returns the index key schema.
    fn get_key_schema(&self) -> Schema {
        self.get_metadata().get_key_schema().clone()
    }

    /// Returns the index key attributes.
    fn get_key_attrs(&self) -> Vec<usize> {
        self.get_metadata().get_key_attrs().clone()
    }

    /// Returns a string representation for debugging.
    fn to_string(&self) -> String {
        format!("INDEX: ({}){}", self.get_index_name(), self.get_metadata())
    }

    /// Inserts an entry into the index.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `rid`: The RID associated with the key.
    /// - `transaction`: The transaction context.
    ///
    /// # Returns
    /// Whether insertion is successful.
    fn insert_entry(&mut self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool;

    /// Deletes an index entry by key.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `rid`: The RID associated with the key (unused).
    /// - `transaction`: The transaction context.
    fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool;

    /// Searches the index for the provided key.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `result`: The collection of RIDs that is populated with results of the search.
    /// - `transaction`: The transaction context.
    fn scan_key(&self, key: &Tuple, transaction: &Transaction)
    -> Result<Vec<(Value, RID)>, String>;

    /// Creates an iterator for scanning the index
    fn create_iterator(&self, start_key: Option<Tuple>, end_key: Option<Tuple>) -> IndexIterator;

    /// Creates an iterator for scanning a specific key
    fn create_point_iterator(&self, key: &Tuple) -> IndexIterator;

    fn get_metadata(&self) -> Arc<IndexInfo>;
}

/// Formatter implementation for `IndexType`.
impl Display for IndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            IndexType::BPlusTreeIndex => "BPlusTree",
            // IndexType::HashTableIndex => "Hash",
            // IndexType::STLOrderedIndex => "STLOrdered",
            // IndexType::STLUnorderedIndex => "STLUnordered",
        };
        write!(f, "{}", name)
    }
}

impl Display for IndexInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            " {{ name: {}, type: {}, table: {}, primary_key: {}, key_size: {} }}",
            self.index_name, self.index_type, self.table_name, self.is_primary_key, self.key_size
        )
    }
}

impl Clone for IndexInfo {
    fn clone(&self) -> Self {
        IndexInfo::new(
            self.get_key_schema().clone(),
            self.get_index_name().parse().unwrap(),
            self.get_index_oid(),
            self.get_table_name().parse().unwrap(),
            self.get_key_size(),
            self.is_primary_key(),
            self.get_index_type().clone(),
            self.get_key_attrs().clone(),
        )
    }
}

impl Clone for IndexType {
    fn clone(&self) -> Self {
        match *self {
            IndexType::BPlusTreeIndex => IndexType::BPlusTreeIndex,
        }
    }
}
