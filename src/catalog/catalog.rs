use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::index::{Index, IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// table creation, table lookup, index creation, and index lookup.
#[derive(Debug)]
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    tables: HashMap<TableOidT, TableInfo>,
    table_names: HashMap<String, TableOidT>,
    next_table_oid: TableOidT,
    indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
    index_names: HashMap<String, IndexOidT>,
    next_index_oid: IndexOidT,
    txn_manager: Arc<TransactionManager>,
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
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        Catalog {
            bpm,
            tables,
            table_names,
            next_table_oid,
            indexes,
            index_names,
            next_index_oid,
            txn_manager,
        }
    }

    /// Creates a new table and returns its metadata.
    ///
    /// # Parameters
    /// - `name`: The name of the new table. Note that all tables beginning with `__` are reserved for the system.
    /// - `schema`: The schema of the new table.
    ///
    /// # Returns
    /// Some(TableInfo) if table creation succeeds, None if table with same name already exists
    pub fn create_table(&mut self, name: String, schema: Schema) -> Option<TableInfo> {
        // Check if table with this name already exists
        if self.table_names.contains_key(&name) {
            return None;
        }

        let table_oid = self.next_table_oid;
        let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), table_oid));

        // Increment table OID
        self.next_table_oid += 1;

        // Create table info
        let table_info = TableInfo::new(schema.clone(), name.clone(), table_heap, table_oid);

        // Add to catalog maps
        self.table_names.insert(name.clone(), table_oid);
        self.tables.insert(table_oid, table_info.clone());

        // Print confirmation
        info!(
            "Table '{}' created successfully with OID {}",
            name, table_oid
        );

        Some(table_info)
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
        unique: bool,
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
            unique,
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
        self.add_index(index_oid, index_info.clone(), index.clone());

        self.next_index_oid += 1;

        // Now populate the index using the stored version with transaction support
        if let Some(table_info) = self.get_table(table_name) {
            let table_heap = table_info.get_table_heap();

            // Create transactional table heap wrapper
            let txn_table_heap =
                TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

            // Create a transaction for populating the index
            let txn = self
                .txn_manager
                .begin(IsolationLevel::ReadCommitted)
                .unwrap();
            let txn_ctx = Arc::new(TransactionContext::new(
                txn.clone(),
                Arc::new(LockManager::new()),
                self.txn_manager.clone(),
            ));

            // Get reference to the index we just created
            if let Some(_) = self.indexes.get(&index_oid) {
                let mut index_write_guard = index.write();

                // Create iterator using transactional table heap
                let mut iter = txn_table_heap.make_iterator(Some(txn_ctx.clone()));

                // Populate index with existing table data
                while let Some((_, tuple)) = iter.next() {
                    index_write_guard.insert_entry(&tuple, tuple.get_rid(), &txn);
                }
            }

            // Commit the transaction
            self.txn_manager.commit(txn, self.bpm.clone());
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

    pub fn get_index_by_index_oid(
        &self,
        index_oid: IndexOidT,
    ) -> Option<(Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)> {
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

    /// Gets a reference to the buffer pool manager
    pub fn get_buffer_pool(&self) -> Arc<BufferPoolManager> {
        self.bpm.clone()
    }

    fn get_table_info_by_name(&self, table_name: &str) -> Option<Arc<TableInfo>> {
        let table_oid = self.table_names.get(table_name)?;
        self.tables
            .get(table_oid)
            .map(|info| Arc::new(info.clone()))
    }

    pub fn get_table_heap(&self, table_name: &str) -> Option<Arc<TableHeap>> {
        let table_info = self.get_table_info_by_name(table_name)?;
        Some(table_info.get_table_heap())
    }

    pub fn get_table_columns(&self, table_name: &str) -> Option<Vec<Column>> {
        let schema = self.get_table_schema(table_name)?;
        Some(schema.get_columns().to_vec())
    }

    pub fn get_all_tables(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
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
mod tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::common::rid::RID;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, BUFFER_POOL_SIZE));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create log manager and transaction manager
            let lock_manager = Arc::new(LockManager::new());
            let txn_manager = Arc::new(TransactionManager::new());

            Self {
                bpm,
                txn_manager,
                lock_manager,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn txn_manager(&self) -> Arc<TransactionManager> {
            Arc::clone(&self.txn_manager)
        }

        pub fn lock_manager(&self) -> Arc<LockManager> {
            Arc::clone(&self.lock_manager)
        }
    }

    fn create_catalog(
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
    ) -> Catalog {
        Catalog::new(
            bpm,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            txn_manager,
        )
    }

    #[test]
    fn test_create_table() {
        let ctx = TestContext::new("test_create_table");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // First create the table
        let table_info = catalog.create_table("test_table".to_string(), schema.clone());
        assert!(table_info.is_some(), "Failed to create table");

        // Then retrieve and verify the table info
        let retrieved_info = catalog.get_table("test_table");
        assert!(retrieved_info.is_some(), "Failed to retrieve table");
        assert_eq!(retrieved_info.unwrap().get_table_name(), "test_table");
        assert_eq!(retrieved_info.unwrap().get_table_schema(), schema);
    }

    #[test]
    fn test_get_table_by_oid() {
        let ctx = TestContext::new("test_get_table_by_oid");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_info = catalog
            .create_table("test_get_table_by_oid".to_string(), schema.clone())
            .unwrap();

        let retrieved_info = catalog.get_table_by_oid(table_info.get_table_oidt());
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
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        catalog.create_table("test_get_table_names_a".to_string(), schema.clone());
        catalog.create_table("test_get_table_names_b".to_string(), schema.clone());

        let table_names = catalog.get_table_names();
        assert_eq!(table_names.len(), 2);
        assert!(table_names.contains(&"test_get_table_names_a".to_string()));
        assert!(table_names.contains(&"test_get_table_names_b".to_string()));
    }

    #[test]
    fn test_get_table_schema() {
        let ctx = TestContext::new("test_get_table_schema");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        catalog.create_table("test_get_table_schema".to_string(), schema.clone());

        let retrieved_schema = catalog.get_table_schema("test_get_table_schema");
        assert!(retrieved_schema.is_some());
        assert_eq!(retrieved_schema.unwrap(), schema);
    }

    #[test]
    fn test_get_table_indexes() {
        let ctx = TestContext::new("test_get_table_indexes");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        // Create a table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let table_info = catalog
            .create_table("indexed_table".to_string(), schema.clone())
            .unwrap();
        let retrieved_info = catalog.get_table_by_oid(table_info.get_table_oidt());
        assert!(retrieved_info.is_some(), "Failed to create table");

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
        assert_eq!(
            table_indexes.len(),
            2,
            "Expected 2 indexes for indexed_table"
        );

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

        let table_heap = Arc::new(TableHeap::new(bpm.clone(), 1));
        let table_heap2 = Arc::new(TableHeap::new(bpm, 1));

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
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // First creation should succeed
        let _first_table = catalog
            .create_table("duplicate_table".to_string(), schema.clone())
            .expect("First table creation should succeed");

        // Verify first table was created successfully
        assert!(catalog.get_table("duplicate_table").is_some());

        // Second creation with same name should fail (return None)
        let second_table = catalog.create_table("duplicate_table".to_string(), schema.clone());
        assert!(second_table.is_none(), "Second table creation should fail");

        // Verify only one table exists
        let tables = catalog.get_table_names();
        assert_eq!(tables.len(), 1, "Should only have one table");
        assert!(tables.contains(&"duplicate_table".to_string()));
    }

    #[test]
    fn test_schema_operations() {
        let ctx = TestContext::new("test_schema_operations");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        // Create a complex schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);

        // Create table with schema
        let table_info = catalog
            .create_table("schema_test".to_string(), schema.clone())
            .unwrap();
        let retrieved_schema = table_info.get_table_schema();

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
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        // First create a table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_create_result =
            catalog.create_table("test_table".to_string(), table_schema.clone());
        assert!(table_create_result.is_some(), "Failed to create table");

        // Create an index on the 'id' column
        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let index_res = catalog.create_index(
            "test_index",
            "test_table",
            key_schema.clone(),
            vec![0], // index on first column (id)
            4,       // size of integer
            false,   // not unique
            IndexType::BPlusTreeIndex,
        );
        assert!(index_res.is_some(), "Failed to create index");

        let (index_info, _) = index_res.unwrap();
        assert_eq!(index_info.get_index_name(), "test_index");
        assert_eq!(index_info.get_table_name(), "test_table");

        // Try to create duplicate index (should fail)
        let duplicate_index = catalog.create_index(
            "test_index",
            "test_table",
            key_schema.clone(),
            vec![0],
            4,
            false,
            IndexType::BPlusTreeIndex,
        );
        assert!(
            duplicate_index.is_none(),
            "Should not be able to create duplicate index"
        );

        // Verify index exists in table's index list
        let table_indexes = catalog.get_table_indexes("test_table");
        assert_eq!(
            table_indexes.len(),
            1,
            "Table should have exactly one index"
        );
        assert_eq!(table_indexes[0].get_index_name(), "test_index");
    }

    #[test]
    fn test_index_lookup_operations() {
        let ctx = TestContext::new("test_index_lookup_operations");
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = create_catalog(bpm, txn_manager);

        // First create a table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_create_result =
            catalog.create_table("lookup_test".to_string(), table_schema.clone());
        assert!(table_create_result.is_some(), "Failed to create table");

        // Create an index on the 'id' column
        let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let index_res = catalog.create_index(
            "id_index",
            "lookup_test",
            key_schema,
            vec![0], // index on first column (id)
            4,       // size of integer
            true,    // unique
            IndexType::BPlusTreeIndex,
        );
        assert!(index_res.is_some(), "Failed to create index");

        let (info, _) = index_res.unwrap();
        let index_oid = info.get_index_oid();

        // Test index lookup by OID
        let retrieved_index = catalog.get_index_by_index_oid(index_oid);
        assert!(retrieved_index.is_some(), "Failed to retrieve index by OID");
        let (retrieved_info, _) = retrieved_index.unwrap();
        assert_eq!(retrieved_info.get_index_name(), "id_index");
        assert_eq!(retrieved_info.get_table_name(), "lookup_test");

        // Test lookup with invalid OID
        let invalid_index = catalog.get_index_by_index_oid(999);
        assert!(
            invalid_index.is_none(),
            "Should not find non-existent index"
        );
    }

    #[test]
    fn test_catalog_operations() {
        let ctx = TestContext::new("test_catalog_operations");
        let mut catalog = create_catalog(ctx.bpm(), ctx.txn_manager());

        // Create a transaction for testing
        let txn = ctx
            .txn_manager
            .begin(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            ctx.lock_manager(),
            ctx.txn_manager.clone(),
        ));

        // Create a test table
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let table_info = catalog
            .create_table("test_table".to_string(), schema.clone())
            .unwrap();
        let table_heap = table_info.get_table_heap();

        // Create transactional table heap wrapper
        let txn_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        // Create and insert a test tuple
        let mut tuple = Tuple::new(
            &[Value::new(1), Value::new("test".to_string())],
            &schema,
            RID::new(0, 0),
        );

        let tuple_meta = Arc::new(TupleMeta::new(txn.get_transaction_id()));

        // Insert the tuple using transactional table heap
        let rid = txn_table_heap
            .insert_tuple(tuple_meta, &mut tuple, txn_ctx.clone())
            .expect("Failed to insert tuple");
        tuple.set_rid(rid);

        // Test iterator using transactional table heap
        let mut iter = txn_table_heap.make_iterator(Some(txn_ctx.clone()));

        // Verify the tuple is visible through the iterator
        let next_result = iter.next();
        assert!(next_result.is_some(), "Iterator should return a tuple");

        let (meta, retrieved_tuple) = next_result.unwrap();
        assert_eq!(meta.get_creator_txn_id(), txn.get_transaction_id());
        assert_eq!(retrieved_tuple.get_rid(), rid);
        assert_eq!(retrieved_tuple.get_values(), tuple.get_values());

        // Verify no more tuples
        assert!(iter.next().is_none());

        // Test get_all_tables
        let tables = catalog.get_all_tables();
        assert!(tables.contains(&"test_table".to_string()));

        // Cleanup
        ctx.txn_manager.commit(txn, ctx.bpm());
    }
}
