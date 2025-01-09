use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::storage::index::b_plus_tree_i::BPlusTree;
use crate::storage::index::index::{Index, IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
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
    indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
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
        indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
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
        index_name: &str,
        table_name: &str,
        key_schema: Schema,
        key_attrs: Vec<usize>,
        key_size: usize,
        is_primary_key: bool,
        index_type: IndexType,
    ) -> Option<(Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)> {
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

        let index_oid = self.next_index_oid;

        // Create index info and insert it into catalog
        let index_info = Arc::new(IndexInfo::new(
            key_schema.clone(),
            index_name.parse().unwrap(),
            index_oid,
            table_name.to_string(),
            key_size,
            is_primary_key,
            index_type,
            key_attrs.clone(),
        ));

        // Create the appropriate index
        let index: Arc<RwLock<BPlusTree>> = match index_type {
            IndexType::BPlusTreeIndex => {
                let order = 4;
                Arc::new(RwLock::new(BPlusTree::new(order, index_info.clone())))
            }
        };

        // Update catalog maps
        self.index_names.insert(index_name.to_string(), index_oid);
        self.add_index(index_oid, index_info, index.clone());

        self.next_index_oid += 1;

        // Now populate the index using the stored version
        if let Some(table_info) = self.get_table(table_name) {
            let table_heap = table_info.get_table_heap();
            let mut iter = table_heap.make_iterator();

            // Get reference to the index we just created
            if let Some(_) = self.indexes.get(&index_oid) {
                let mut index_write_guard = index.write();
                // Populate index with existing table data
                while let Some((_, tuple)) = iter.next() {
                    let transaction = Transaction::new(0, IsolationLevel::ReadUncommitted);
                    index_write_guard.insert_entry(&tuple, tuple.get_rid(), &transaction);
                }
            }
        }

        // Return reference to the newly created index info
        self.get_index_by_index_oid(index_oid)
    }

    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&Arc<IndexInfo>> {
        if !self.table_names.contains_key(table_name) {
            return Vec::new();
        }

        self.indexes
            .values()
            .filter(|(info, _)| info.get_table_name() == table_name)
            .map(|(info, _)| info)
            .collect()
    }

    pub fn get_index_by_index_oid(&self, index_oid: IndexOidT) -> Option<(Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)> {
        self.indexes.get(&index_oid).cloned()
    }

    pub fn add_index(
        &mut self,
        index_oid: IndexOidT,
        index_info: Arc<IndexInfo>,
        btree: Arc<RwLock<BPlusTree>>,
    ) {
        self.indexes.insert(index_oid, (index_info, btree));
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
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::fs;

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
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

            Self {
                bpm,
                db_file,
                db_log_file,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
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
    fn test_get_table_indexes() {
        let ctx = TestContext::new("test_get_table_indexes");
        let bpm = ctx.bpm();
        let mut catalog = create_catalog(bpm);

        // Create a table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let table_info = catalog.create_table("indexed_table", schema.clone());
        assert!(table_info.is_some(), "Failed to create table");

        // Test getting indexes for non-existent table
        let no_table_indexes = catalog.get_table_indexes("nonexistent_table");
        assert!(no_table_indexes.is_empty());

        // Create multiple indexes
        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let index1_res = catalog.create_index(
            "index1",
            "indexed_table",
            key_schema.clone(),
            vec![0],
            4,
            false,
            IndexType::BPlusTreeIndex,
        );
        assert!(index1_res.is_some(), "Failed to create index1");
        let (info1, _) = index1_res.unwrap();
        assert_eq!(info1.get_index_name(), "index1");

        let key_schema2 = Schema::new(vec![Column::new("name", TypeId::VarChar)]);
        let index2_res = catalog.create_index(
            "index2",
            "indexed_table",
            key_schema2,
            vec![1],
            4,
            false,
            IndexType::BPlusTreeIndex,
        );
        assert!(index2_res.is_some(), "Failed to create index2");
        let (info2, _) = index2_res.unwrap();
        assert_eq!(info2.get_index_name(), "index2");

        // Get all indexes for the table
        let table_indexes = catalog.get_table_indexes("indexed_table");
        assert_eq!(table_indexes.len(), 2, "Expected 2 indexes for indexed_table");

        // Verify the index details
        let index_names: Vec<&str> = table_indexes
            .iter()
            .map(|info| info.get_index_name().as_str())
            .collect();
        assert!(
            index_names.contains(&"index1"),
            "index1 not found in table indexes"
        );
        assert!(
            index_names.contains(&"index2"),
            "index2 not found in table indexes"
        );
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

    #[test]
    fn test_create_duplicate_table() {
        let ctx = TestContext::new("test_create_duplicate_table");
        let bpm = ctx.bpm();
        let mut catalog = create_catalog(bpm);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // First creation should succeed
        let result1 = catalog.create_table("duplicate_table", schema.clone());
        assert!(result1.is_some());

        // Second creation with same name should fail
        let result2 = catalog.create_table("duplicate_table", schema.clone());
        assert!(result2.is_none());
    }

    #[test]
    fn test_schema_operations() {
        let ctx = TestContext::new("test_schema_operations");
        let bpm = ctx.bpm();
        let mut catalog = create_catalog(bpm);

        // Create a complex schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        // Create table with schema
        catalog.create_table("schema_test", schema.clone());

        // Test retrieving schema
        let retrieved_schema = catalog.get_table_schema("schema_test");
        assert!(retrieved_schema.is_some());
        let retrieved_schema = retrieved_schema.unwrap();

        // Verify schema details
        assert_eq!(retrieved_schema.get_column_count(), 4);
        assert_eq!(retrieved_schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(retrieved_schema.get_column(1).unwrap().get_name(), "name");
        assert_eq!(retrieved_schema.get_column(2).unwrap().get_name(), "age");
        assert_eq!(retrieved_schema.get_column(3).unwrap().get_name(), "email");

        // Test schema for non-existent table
        let nonexistent_schema = catalog.get_table_schema("nonexistent_table");
        assert!(nonexistent_schema.is_none());
    }

    #[test]
    fn test_create_index_operations() {
        let ctx = TestContext::new("test_create_index_operations");
        let bpm = ctx.bpm();
        let mut catalog = create_catalog(bpm);

        // Create a table first
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        catalog.create_table("test_table", schema.clone());

        // Test creating an index
        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let index_res = catalog.create_index(
            "test_index",
            "test_table",
            key_schema,
            vec![0],
            4,
            false,
            IndexType::BPlusTreeIndex,
        );
        assert!(index_res.is_some());
        let (info, _) = index_res.unwrap();
        assert_eq!(info.get_index_name(), "test_index");

        // Test creating duplicate index (should fail)
        let key_schema2 = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let duplicate_index = catalog.create_index(
            "test_index",
            "test_table",
            key_schema2,
            vec![0],
            4,
            false,
            IndexType::BPlusTreeIndex,
        );
        assert!(duplicate_index.is_none());
    }

    #[test]
    fn test_index_lookup_operations() {
        let ctx = TestContext::new("test_index_lookup_operations");
        let bpm = ctx.bpm();
        let mut catalog = create_catalog(bpm);

        // Create a table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        catalog.create_table("lookup_test", table_schema.clone());

        // Create an index
        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let index_res = catalog.create_index(
            "id_index",
            "lookup_test",
            key_schema,
            vec![0],
            4,
            true,
            IndexType::BPlusTreeIndex,
        );
        assert!(index_res.is_some());
        let (info, _) = index_res.unwrap();
        let index_oid = info.get_index_oid();

        // Test index lookup by OID
        let retrieved_index = catalog.get_index_by_index_oid(index_oid);
        assert!(retrieved_index.is_some());
        let (retrieved_info, _) = retrieved_index.unwrap();
        assert_eq!(retrieved_info.get_index_name(), "id_index");

        // Test lookup with invalid OID
        let invalid_index = catalog.get_index_by_index_oid(999);
        assert!(invalid_index.is_none());
    }
}
