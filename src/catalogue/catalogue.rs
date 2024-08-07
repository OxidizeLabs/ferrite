use core::fmt;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::recovery::log_manager::LogManager;
use crate::storage::index::index::Index;
use crate::storage::table::table_heap::TableHeap;

pub enum IndexType {
    BPlusTreeIndex,
    HashTableIndex,
    STLOrderedIndex,
    STLUnorderedIndex,
}

/// The TableInfo struct maintains metadata about a table.
pub struct TableInfo {
    /// The table schema
    schema: Schema,
    /// The table name
    name: String,
    /// An owning pointer to the table heap
    table: Box<TableHeap>,
    /// The table OID
    oid: TableOidT,
}

impl TableInfo {
    /// Constructs a new TableInfo instance.
    ///
    /// # Parameters
    /// - `schema`: The table schema.
    /// - `name`: The table name.
    /// - `table`: An owning pointer to the table heap.
    /// - `oid`: The unique OID for the table.
    pub fn new(schema: Schema, name: String, table: Box<TableHeap>, oid: TableOidT) -> Self {
        TableInfo {
            schema,
            name,
            table,
            oid,
        }
    }
}

/// The IndexInfo struct maintains metadata about an index.
pub struct IndexInfo {
    /// The schema for the index key
    key_schema: Schema,
    /// The name of the index
    name: String,
    /// An owning pointer to the index
    index: Box<dyn Index>,
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
        name: String,
        index: Box<dyn Index>,
        index_oid: IndexOidT,
        table_name: String,
        key_size: usize,
        is_primary_key: bool,
        index_type: IndexType,
    ) -> Self {
        IndexInfo {
            key_schema,
            name,
            index,
            index_oid,
            table_name,
            key_size,
            is_primary_key,
            index_type,
        }
    }
}

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// table creation, table lookup, index creation, and index lookup.
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    lock_manager: Arc<Mutex<LockManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    tables: HashMap<TableOidT, Box<TableInfo>>,
    table_names: HashMap<String, TableOidT>,
    next_table_oid: AtomicI32,
    indexes: HashMap<IndexOidT, Box<IndexInfo>>,
    index_names: HashMap<String, HashMap<String, IndexOidT>>,
    next_index_oid: AtomicI32,
}

impl Catalog {
    /// Constructs a new Catalog instance.
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager backing tables created by this catalog.
    /// - `lock_manager`: The lock manager in use by the system.
    /// - `log_manager`: The log manager in use by the system.
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        lock_manager: Arc<Mutex<LockManager>>,
        log_manager: Arc<Mutex<LogManager>>,
    ) -> Self {
        Catalog {
            bpm,
            lock_manager,
            log_manager,
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_oid: AtomicI32::new(0),
            indexes: HashMap::new(),
            index_names: HashMap::new(),
            next_index_oid: AtomicI32::new(0),
        }
    }

    /// Creates a new table and returns its metadata.
    ///
    /// # Parameters
    /// - `txn`: The transaction in which the table is being created.
    /// - `table_name`: The name of the new table. Note that all tables beginning with `__` are reserved for the system.
    /// - `schema`: The schema of the new table.
    /// - `create_table_heap`: Whether to create a table heap for the new table.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn create_table(
        &mut self,
        txn: &Transaction,
        table_name: &str,
        schema: Schema,
        create_table_heap: bool,
    ) -> Option<&TableInfo> {
        if self.table_names.contains_key(table_name) {
            return None;
        }

        let table = if create_table_heap {
            Box::new(TableHeap::new(self.bpm.clone()))
        } else {
            TableHeap::create_empty_heap(false)?
        };

        let table_oid = self.next_table_oid.fetch_add(1, Ordering::SeqCst);
        let table_info = Box::new(TableInfo::new(
            schema,
            table_name.to_string(),
            table,
            table_oid,
        ));

        self.table_names.insert(table_name.to_string(), table_oid);
        self.tables.insert(table_oid, table_info);

        self.tables.get(&table_oid).map(|t| &**t)
    }

    /// Queries table metadata by name.
    ///
    /// # Parameters
    /// - `table_name`: The name of the table.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn get_table(&self, table_name: &str) -> Option<&TableInfo> {
        self.table_names
            .get(table_name)
            .and_then(|&table_oid| self.tables.get(&table_oid))
            .map(|t| &**t)
    }

    /// Queries table metadata by OID.
    ///
    /// # Parameters
    /// - `table_oid`: The OID of the table to query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn get_table_by_oid(&self, table_oid: TableOidT) -> Option<&TableInfo> {
        self.tables.get(&table_oid).map(|t| &**t)
    }

    /// Creates a new index, populates existing data of the table, and returns its metadata.
    ///
    /// # Parameters
    /// - `txn`: The transaction in which the table is being created.
    /// - `index_name`: The name of the new index.
    /// - `table_name`: The name of the table.
    /// - `schema`: The schema of the table.
    /// - `key_schema`: The schema of the key.
    /// - `key_attrs`: Key attributes.
    /// - `key_size`: Size of the key.
    /// - `hash_function`: The hash function for the index.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata of the new table.
    // pub fn create_index<KeyType: Eq + Hash + Clone, ValueType, KeyComparator>(
    //     &mut self,
    //     txn: &Transaction,
    //     index_name: &str,
    //     table_name: &str,
    //     schema: Schema,
    //     key_schema: Schema,
    //     key_attrs: Vec<u32>,
    //     key_size: usize,
    //     hash_function: HashFunction<KeyType>,
    //     is_primary_key: bool,
    //     index_type: IndexType,
    // ) -> Option<&IndexInfo> {
    //     if !self.table_names.contains_key(table_name) {
    //         return None;
    //     }
    //
    //     let table_indexes = self.index_names.entry(table_name.to_string()).or_default();
    //     if table_indexes.contains_key(index_name) {
    //         return None;
    //     }
    //
    //     let meta = Box::new(IndexMetadata::new(index_name.to_string(), table_name.to_string(), &schema, key_attrs, is_primary_key));
    //     let index: Box<dyn Index> = match index_type {
    //         IndexType::HashTableIndex => Box::new(ExtendableHashTableIndex::new(Arc::from(meta), self.bpm.clone(), hash_function)),
    //         IndexType::BPlusTreeIndex => Box::new(BPlusTreeIndex::new(meta, self.bpm.clone(), ())),
    //         IndexType::STLOrderedIndex => Box::new(STLOrderedIndex::new(meta, self.bpm.clone())),
    //         IndexType::STLUnorderedIndex => Box::new(STLUnorderedIndex::new(meta, self.bpm.clone(), hash_function)),
    //     };
    //
    //     let table_meta = self.get_table(table_name)?;
    //     let mut iter = table_meta.table.make_iterator();
    //     while !iter.is_end() {
    //         let (meta, tuple) = iter.get_tuple();
    //         index.insert_entry(tuple.key_from_tuple(&schema, &key_schema, &key_attrs), tuple.get_rid(), txn);
    //         iter.next();
    //     }
    //
    //     let index_oid = self.next_index_oid.fetch_add(1, Ordering::SeqCst);
    //     let index_info = Box::new(IndexInfo::new(key_schema, index_name.to_string(), index, index_oid, table_name.to_string(), key_size, is_primary_key, index_type));
    //
    //     self.indexes.insert(index_oid, index_info);
    //     table_indexes.insert(index_name.to_string(), index_oid);
    //
    //     self.indexes.get(&index_oid).map(|i| &**i)
    // }

    /// Gets the index `index_name` for table `table_name`.
    ///
    /// # Parameters
    /// - `index_name`: The name of the index for which to query.
    /// - `table_name`: The name of the table on which to perform the query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the index.
    pub fn get_index(&self, index_name: &str, table_name: &str) -> Option<&IndexInfo> {
        self.index_names
            .get(table_name)
            .and_then(|table_indexes| {
                table_indexes
                    .get(index_name)
                    .and_then(|&index_oid| self.indexes.get(&index_oid))
            })
            .map(|i| &**i)
    }

    /// Gets the index `index_name` for table identified by `table_oid`.
    ///
    /// # Parameters
    /// - `index_name`: The name of the index for which to query.
    /// - `table_oid`: The OID of the table on which to perform the query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the index.
    pub fn get_index_by_oid(&self, index_name: &str, table_oid: TableOidT) -> Option<&IndexInfo> {
        let table_meta = self.tables.get(&table_oid)?;
        self.get_index(index_name, &table_meta.name)
    }

    /// Gets the index identified by `index_oid`.
    ///
    /// # Parameters
    /// - `index_oid`: The OID of the index for which to query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the index.
    pub fn get_index_by_index_oid(&self, index_oid: IndexOidT) -> Option<&IndexInfo> {
        self.indexes.get(&index_oid).map(|i| &**i)
    }

    /// Gets all the indexes for the table identified by `table_name`.
    ///
    /// # Parameters
    /// - `table_name`: The name of the table for which indexes should be retrieved.
    ///
    /// # Returns
    /// A vector of `IndexInfo` for each index on the given table. Returns an empty vector
    /// if the table exists but no indexes have been created for it.
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&IndexInfo> {
        self.table_names
            .get(table_name)
            .and_then(|&table_oid| {
                self.index_names.get(table_name).map(|table_indexes| {
                    table_indexes
                        .values()
                        .filter_map(|&index_oid| self.indexes.get(&index_oid))
                        .map(|i| &**i)
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    /// Gets the names of all tables.
    ///
    /// # Returns
    /// A vector of table names.
    pub fn get_table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }
}

/// Formatter implementation for `IndexType`.
impl Display for IndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            IndexType::BPlusTreeIndex => "BPlusTree",
            IndexType::HashTableIndex => "Hash",
            IndexType::STLOrderedIndex => "STLOrdered",
            IndexType::STLUnorderedIndex => "STLUnordered",
        };
        write!(f, "{}", name)
    }
}
