use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT, TxnId};
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::storage::index::b_plus_tree_i::BPlusTree;
use crate::storage::index::index::{Index, IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use core::fmt;
use log::{info, warn};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// table creation, table lookup, index creation, and index lookup.
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    tables: HashMap<TableOidT, TableInfo>,
    table_names: HashMap<String, TableOidT>,
    next_table_oid: TableOidT,
    indexes: HashMap<IndexOidT, IndexInfo>,
    index_names: HashMap<String, IndexOidT>,
    next_index_oid: IndexOidT,
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
        next_index_oid: IndexOidT,
        next_table_oid: TableOidT,
        tables: HashMap<TableOidT, TableInfo>,
        indexes: HashMap<IndexOidT, IndexInfo>,
        table_names: HashMap<String, TableOidT>,
        index_names: HashMap<String, IndexOidT>,
    ) -> Self {
        Catalog {
            bpm,
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
    pub fn create_table(&mut self, table_name: &str, schema: Schema) -> Option<&TableInfo> {
        // Debug: Print input schema details
        info!("Creating table '{}' with schema:", table_name);
        for i in 0..schema.get_column_count() {
            let column = schema.get_column(i as usize).unwrap();
            info!(
                "Column {}: Name = {}, Type = {:?}, Offset = {}",
                i,
                column.get_name(),
                column.get_type(),
                column.get_offset()
            );
        }

        // Check if table already exists
        if self.table_names.contains_key(table_name) {
            warn!("Table '{}' already exists", table_name);
            return None;
        }

        // Create new table heap
        let table = Arc::new(TableHeap::new(self.bpm.clone()));

        // Increment table OID (note: this was .add(1) before, which might be incorrect)
        self.next_table_oid += 1;
        let table_oid = self.next_table_oid;

        // Create table info
        let table_info = TableInfo::new(schema.clone(), table_name.to_string(), table, table_oid);

        // Add to catalog maps
        self.table_names.insert(table_name.to_string(), table_oid);
        self.tables.insert(table_oid, table_info);

        // Print confirmation
        info!(
            "Table '{}' created successfully with OID {}",
            table_name, table_oid
        );

        // Return reference to the newly created table info
        self.tables.get(&table_oid)
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
    }

    /// Queries table metadata by OID.
    ///
    /// # Parameters
    /// - `table_oid`: The OID of the table to query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn get_table_by_oid(&self, table_oid: TableOidT) -> Option<&TableInfo> {
        self.tables.get(&table_oid)
    }

    /// Creates a new index, populates existing data of the table, and returns its metadata.
    pub fn create_index(
        &mut self,
        txn_id: TxnId,
        index_name: &str,
        table_name: &str,
        key_schema: Schema,
        key_attrs: Vec<usize>,
        key_size: usize,
        is_primary_key: bool,
        index_type: IndexType,
    ) -> Option<&IndexInfo> {
        // Check if table exists
        if !self.table_names.contains_key(table_name) {
            warn!("Cannot create index: table '{}' does not exist", table_name);
            return None;
        }

        // Check if index already exists
        if self.index_names.contains_key(index_name) {
            warn!("Cannot create index: index '{}' already exists", index_name);
            return None;
        }

        // Create index metadata
        let index_oid = self.next_index_oid;

        // Create the appropriate index based on index type
        let index: Box<dyn Index> = match index_type {
            IndexType::BPlusTreeIndex => {
                // Initialize B+Tree with appropriate order (e.g. 4)
                let order = 4; // This could be made configurable
                Box::new(BPlusTree::new(order))
            }
        };

        // Create index info and insert it into catalog
        let index_info = IndexInfo::new(
            key_schema.clone(),
            index_name.to_string(),
            index_oid,
            table_name.to_string(),
            key_size,
            is_primary_key,
            index_type,
            key_attrs.clone(),
        );

        // Update catalog maps first so we can access the index through the catalog
        self.index_names.insert(index_name.to_string(), index_oid);
        self.indexes.insert(index_oid, index_info);
        self.next_index_oid += 1;

        // Now populate the index using the stored version
        if let Some(table_info) = self.get_table(table_name) {
            let table_heap = table_info.get_table_heap();
            let mut iter = table_heap.make_iterator();

            // Get reference to the index we just created
            if let Some(index_info) = self.indexes.get(&index_oid) {
                // Populate index with existing table data
                while let Some((_, tuple)) = iter.next() {
                    let key_tuple = tuple.key_from_tuple(key_schema.clone(), key_attrs.clone());
                    let transaction = Transaction::new(txn_id, IsolationLevel::ReadUncommitted);
                    index.insert_entry(&key_tuple, tuple.get_rid(), &transaction);
                }
            }
        }

        // Return reference to the newly created index info
        self.indexes.get(&index_oid)
    }

    /// Gets the index identified by `index_oid`.
    ///
    /// # Parameters
    /// - `index_oid`: The OID of the index for which to query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the index.
    pub fn get_index_by_index_oid(&self, index_oid: IndexOidT) -> Option<&IndexInfo> {
        self.indexes.get(&index_oid)
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
        // First verify the table exists
        if !self.table_names.contains_key(table_name) {
            return Vec::new();
        }

        // Collect all indexes where table_name matches
        self.indexes
            .values()
            .filter(|index_info| index_info.get_index_name() == table_name)
            .collect()
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

impl Display for Catalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Catalog Contents:")?;
        writeln!(f, "----------------")?;

        // Iterate through all tables
        for (oid, table_info) in &self.tables {
            writeln!(f, "Table OID {}: '{}'", oid, table_info.get_table_name())?;

            // Get and display table schema
            let schema = table_info.get_table_schema();
            writeln!(f, "  Schema:")?;

            for i in 0..schema.get_column_count() {
                let column = schema.get_column(i as usize).unwrap();
                writeln!(
                    f,
                    "    Column {}: Name = {}, Type = {:?}, Offset = {}",
                    i,
                    column.get_name(),
                    column.get_type(),
                    column.get_offset()
                )?;
            }

            // Optional: List indexes for this table
            let table_indexes = self.get_table_indexes(table_info.get_table_name());
            if !table_indexes.is_empty() {
                writeln!(f, "  Indexes:")?;
                for index in table_indexes {
                    writeln!(f, "    - {}", index.get_index_name())?;
                }
            }

            writeln!(f)?; // Extra newline between tables
        }

        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::fs;

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<RwLock<LockManager>>,
        log_manager: Arc<RwLock<LogManager>>,
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
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )));
            let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                catalog,
                log_manager.clone(),
            )));
            let lock_manager = Arc::new(RwLock::new(LockManager::new(Arc::clone(
                &transaction_manager.clone(),
            ))));

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

        pub fn lock_manager(&self) -> Arc<RwLock<LockManager>> {
            Arc::clone(&self.lock_manager)
        }

        pub fn log_manager(&self) -> Arc<RwLock<LogManager>> {
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

    fn create_catalog(bpm: Arc<BufferPoolManager>) -> Catalog {
        Catalog::new(
            bpm,
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

        let mut catalog = create_catalog(bpm);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_info = catalog.create_table("test_table", schema.clone());
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

        let mut catalog = create_catalog(bpm);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_info = catalog
            .create_table("test_get_table_by_oid", schema.clone())
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

        let mut catalog = create_catalog(bpm);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        catalog.create_table("test_get_table_names_a", schema.clone());
        catalog.create_table("test_get_table_names_b", schema.clone());

        let table_names = catalog.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test_get_table_names_a".to_string()));
        assert!(table_names.contains(&"test_get_table_names_b".to_string()));
    }

    #[test]
    fn test_get_table_schema() {
        let ctx = TestContext::new("test_get_table_schema");
        let bpm = ctx.bpm();

        let mut catalog = create_catalog(bpm);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        catalog.create_table("test_get_table_schema", schema.clone());

        let retrieved_schema = catalog.get_table_schema("test_get_table_schema");
        assert!(retrieved_schema.is_some());
        assert_eq!(retrieved_schema.unwrap(), schema);
    }

    #[test]
    fn test_table_info_equality() {
        let ctx = TestContext::new("test_table_info_equality");
        let bpm = ctx.bpm();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_heap = Arc::new(TableHeap::new(bpm.clone()));
        let table_heap2 = Arc::new(TableHeap::new(bpm));

        // Create two identical TableInfo instances
        let info1 = TableInfo::new(
            schema.clone(),
            "test_table".to_string(),
            table_heap.clone(),
            1,
        );
        let info2 = TableInfo::new(
            schema.clone(),
            "test_table".to_string(),
            table_heap.clone(),
            1,
        );

        // Create a different TableInfo instance
        let info3 = TableInfo::new(
            schema.clone(),
            "different_table".to_string(),
            table_heap2,
            2,
        );

        // Test equality
        assert_eq!(info1, info2);
        assert_ne!(info1, info3);
    }
}
