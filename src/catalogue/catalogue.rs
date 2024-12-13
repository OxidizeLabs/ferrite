use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalogue::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::common::logger::initialize_logger;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::Transaction;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::storage::index::index::Index;
use crate::storage::table::table_heap::TableHeap;
use chrono::Utc;
use core::fmt;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::ops::Add;
use std::sync::Arc;

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
    table: Arc<TableHeap>,
    /// The table OID
    oid: TableOidT,
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

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// table creation, table lookup, index creation, and index lookup.
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    // lock_manager: Arc<LockManager>,
    log_manager: Arc<LogManager>,
    tables: HashMap<TableOidT, Box<TableInfo>>,
    table_names: HashMap<String, TableOidT>,
    next_table_oid: TableOidT,
    indexes: HashMap<IndexOidT, Box<IndexInfo>>,
    index_names: HashMap<String, HashMap<String, IndexOidT>>,
    next_index_oid: IndexOidT,
}

impl TableInfo {
    /// Constructs a new TableInfo instance.
    ///
    /// # Parameters
    /// - `schema`: The table schema.
    /// - `name`: The table name.
    /// - `table`: An owning pointer to the table heap.
    /// - `oid`: The unique OID for the table.
    pub fn new(schema: Schema, name: String, table: Arc<TableHeap>, oid: TableOidT) -> Self {
        TableInfo {
            schema,
            name,
            table,
            oid,
        }
    }

    pub fn get_table_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn get_table_oidt(&self) -> TableOidT {
        self.oid
    }

    pub fn get_table_heap(&self) -> Arc<TableHeap> {
        self.table.clone()
    }

    pub fn get_table_name(&self) -> &str {
        &self.name
    }
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

impl Catalog {
    /// Constructs a new Catalog instance.
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager backing tables created by this catalog.
    /// - `lock_manager`: The lock manager in use by the system.
    /// - `log_manager`: The log manager in use by the system.
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        // lock_manager: Arc<LockManager>,
        log_manager: Arc<LogManager>,
        next_index_oid: IndexOidT,
        next_table_oid: TableOidT,
        tables: HashMap<TableOidT, Box<TableInfo>>,
        indexes: HashMap<IndexOidT, Box<IndexInfo>>,
        table_names: HashMap<String, TableOidT>,
        index_names: HashMap<String, HashMap<String, IndexOidT>>,
    ) -> Self {
        Catalog {
            bpm,
            // lock_manager,
            log_manager,
            tables,
            table_names,
            next_table_oid,
            indexes,
            index_names,
            next_index_oid,
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
        // Check if table already exists
        if self.table_names.contains_key(table_name) {
            return None;
        }

        // Create new table heap
        let table = Arc::new(TableHeap::new(self.bpm.clone()));

        // Generate new table OID
        let table_oid = self.next_table_oid.add(1);

        // Create table info
        let table_info = Box::new(TableInfo::new(
            schema,
            table_name.to_string(),
            table,
            table_oid,
        ));

        // Add to catalog maps
        self.table_names.insert(table_name.to_string(), table_oid);
        self.tables.insert(table_oid, table_info);

        // Return reference to the newly created table info
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

    // /// Creates a new index, populates existing data of the table, and returns its metadata.
    // ///
    // /// # Parameters
    // /// - `txn`: The transaction in which the table is being created.
    // /// - `index_name`: The name of the new index.
    // /// - `table_name`: The name of the table.
    // /// - `schema`: The schema of the table.
    // /// - `key_schema`: The schema of the key.
    // /// - `key_attrs`: Key attributes.
    // /// - `key_size`: Size of the key.
    // /// - `hash_function`: The hash function for the index.
    // ///
    // /// # Returns
    // /// A (non-owning) pointer to the metadata of the new table.
    // pub fn create_index<KeyType: Eq + Hash + Clone, ValueType, KeyComparator>(
    //     &mut self,
    //     txn: &Transaction,
    //     index_name: &str,
    //     table_name: &str,
    //     schema: Schema,
    //     key_schema: Schema,
    //     key_attrs: Vec<usize>,
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
    //         // IndexType::HashTableIndex => Box::new(ExtendableHashTableIndex::new(Arc::from(meta), self.bpm.clone(), hash_function)),
    //         IndexType::BPlusTreeIndex => Box::new(BPlusTreeIndex::new(meta, self.bpm.clone(), ())),
    //         // IndexType::STLOrderedIndex => Box::new(STLOrderedIndex::new(meta, self.bpm.clone())),
    //         // IndexType::STLUnorderedIndex => Box::new(STLUnorderedIndex::new(meta, self.bpm.clone(), hash_function)),
    //         _ => {}
    //     };
    //
    //     let table_meta = self.get_table(table_name)?;
    //     let mut iter = table_meta.table.make_iterator();
    //     while !iter.is_end() {
    //         let (meta, tuple) = iter.next().unwrap();
    //         index.insert_entry(tuple.key_from_tuple(key_schema.clone(), key_attrs.clone()), tuple.get_rid(), txn);
    //         iter.next();
    //     }
    //
    //     let index_oid = self.next_index_oid.add(1);
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
            .and_then(|&_table_oid| {
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

    pub fn get_table_schema(&self, table_name: &str) -> Option<Schema> {
        self.table_names.get(table_name).and_then(|&table_oid| {
            self.tables
                .get(&table_oid)
                .map(|table_schema| table_schema.get_table_schema())
        })
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

pub struct TestContext {
    bpm: Arc<BufferPoolManager>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    lock_manager: Arc<LockManager>,
    log_manager: Arc<LogManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    pub fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(
            db_file.clone(),
            db_log_file.clone(),
            100,
        ));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));
        let file_disk_manager = Arc::new(FileDiskManager::new(
            "db_file.db".to_string(),
            "log_file.log".to_string(),
            10,
        ));
        let log_manager = Arc::new(LogManager::new(file_disk_manager));
        let catalog = Arc::new(RwLock::new(Catalog::new(
            bpm.clone(),
            log_manager,
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )));
        // Create TransactionManager with a placeholder Catalog
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new((catalog))));
        let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));
        let log_manager = Arc::new(LogManager::new(Arc::clone(&disk_manager)));

        Self {
            bpm,
            transaction_manager,
            lock_manager,
            log_manager,
            db_file,
            db_log_file,
        }
    }

    pub fn bpm(&self) -> Arc<BufferPoolManager> {
        Arc::clone(&self.bpm)
    }

    pub fn lock_manager(&self) -> Arc<LockManager> {
        Arc::clone(&self.lock_manager)
    }

    pub fn log_manager(&self) -> Arc<LogManager> {
        Arc::clone(&self.log_manager)
    }

    fn cleanup(&self) {
        let _ = fs::remove_file(&self.db_file);
        let _ = fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::types_db::type_id::TypeId;

    fn create_catalog(
        bpm: Arc<BufferPoolManager>,
        lock_manager: Arc<LockManager>,
        log_manager: Arc<LogManager>,
    ) -> Catalog {
        Catalog::new(
            bpm,
            // lock_manager,
            log_manager,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn test_create_table() {
        let ctx = TestContext::new("test_create_table");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();

        let mut catalog = create_catalog(bpm, lock_manager, log_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let txn = Transaction::new(0, IsolationLevel::Serializable); // Assuming Transaction::new() takes a transaction ID

        let table_info = catalog.create_table(&txn, "test_table", schema.clone(), true);
        assert!(table_info.is_some());

        let retrieved_info = catalog.get_table("test_table");
        assert!(retrieved_info.is_some());
        assert_eq!(retrieved_info.unwrap().get_table_name(), "test_table");
        assert_eq!(retrieved_info.unwrap().get_table_schema(), schema);
    }

    #[test]
    fn test_get_table_by_oid() {
        let ctx = TestContext::new("test_get_table_by_oid");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();

        let mut catalog = create_catalog(bpm, lock_manager, log_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let txn = Transaction::new(0, IsolationLevel::Serializable); // Assuming Transaction::new() takes a transaction ID

        let table_info = catalog
            .create_table(&txn, "test_get_table_by_oid", schema.clone(), true)
            .unwrap();
        let table_oid = table_info.get_table_oidt();

        let retrieved_info = catalog.get_table_by_oid(table_oid);
        assert!(retrieved_info.is_some());
        assert_eq!(
            retrieved_info.unwrap().get_table_name(),
            "test_get_table_by_oid"
        );
    }

    #[test]
    fn test_get_table_names() {
        let ctx = TestContext::new("test_get_table_names");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();

        let mut catalog = create_catalog(bpm, lock_manager, log_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let txn = Transaction::new(0, IsolationLevel::Serializable); // Assuming Transaction::new() takes a transaction ID

        catalog.create_table(&txn, "test_get_table_names_a", schema.clone(), true);
        catalog.create_table(&txn, "test_get_table_names_b", schema.clone(), true);

        let table_names = catalog.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test_get_table_names_a".to_string()));
        assert!(table_names.contains(&"test_get_table_names_b".to_string()));
    }

    #[test]
    fn test_get_table_schema() {
        let ctx = TestContext::new("test_get_table_schema");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();

        let mut catalog = create_catalog(bpm, lock_manager, log_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let txn = Transaction::new(0, IsolationLevel::Serializable); // Assuming Transaction::new() takes a transaction ID

        catalog.create_table(&txn, "test_get_table_schema", schema.clone(), true);

        let retrieved_schema = catalog.get_table_schema("test_get_table_schema");
        assert!(retrieved_schema.is_some());
        assert_eq!(retrieved_schema.unwrap(), schema);
    }

    // Additional tests for index-related functions can be added here
    // once the create_index function is implemented
}
