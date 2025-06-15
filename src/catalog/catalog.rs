use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::column::Column;
use crate::catalog::database::Database;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::index::{IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// database creation, database lookup, table creation, table lookup,
/// index creation, and index lookup.
#[derive(Debug)]
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    databases: HashMap<String, Database>,
    current_database: Option<String>,
    txn_manager: Arc<TransactionManager>,
}

impl Catalog {
    /// Constructs a new Catalog instance.
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager backing tables created by this catalog.
    /// - `txn_manager`: The transaction manager in use by the system.
    pub fn new(bpm: Arc<BufferPoolManager>, txn_manager: Arc<TransactionManager>) -> Self {
        Self::new_with_existing_data(
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

    /// For backward compatibility - constructs a catalog with existing data
    pub fn new_with_existing_data(
        bpm: Arc<BufferPoolManager>,
        next_index_oid: IndexOidT,
        next_table_oid: TableOidT,
        tables: HashMap<TableOidT, TableInfo>,
        indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
        table_names: HashMap<String, TableOidT>,
        index_names: HashMap<String, IndexOidT>,
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        // Create a default database with the existing data
        let mut databases = HashMap::new();
        let default_db = Database::with_existing_data(
            "default".to_string(),
            bpm.clone(),
            next_index_oid,
            next_table_oid,
            tables,
            indexes,
            table_names,
            index_names,
            txn_manager.clone(),
            0, // Use 0 as the default database OID
        );
        databases.insert("default".to_string(), default_db);

        Catalog {
            bpm,
            databases,
            current_database: Some("default".to_string()),
            txn_manager,
        }
    }

    /// Creates a new database.
    ///
    /// # Parameters
    /// - `name`: The name of the new database.
    ///
    /// # Returns
    /// true if database creation succeeds, false if database with same name already exists
    pub fn create_database(&mut self, name: String) -> bool {
        if self.databases.contains_key(&name) {
            warn!("Database '{}' already exists", name);
            return false;
        }

        let database = Database::new(name.clone(), self.bpm.clone(), self.txn_manager.clone());
        self.databases.insert(name.clone(), database);
        info!("Database '{}' created successfully", name);
        true
    }

    /// Changes the current database context.
    ///
    /// # Parameters
    /// - `name`: The name of the database to use.
    ///
    /// # Returns
    /// true if database exists and was switched to, false otherwise
    pub fn use_database(&mut self, name: &str) -> bool {
        if !self.databases.contains_key(name) {
            warn!("Database '{}' does not exist", name);
            return false;
        }

        self.current_database = Some(name.to_string());
        info!("Switched to database '{}'", name);
        true
    }

    /// Gets the current database name.
    ///
    /// # Returns
    /// The name of the current database, or None if no database is selected.
    pub fn get_current_database_name(&self) -> Option<&String> {
        self.current_database.as_ref()
    }

    /// Gets a reference to the current database.
    ///
    /// # Returns
    /// A reference to the current database, or None if no database is selected.
    pub fn get_current_database(&self) -> Option<&Database> {
        self.current_database
            .as_ref()
            .and_then(|name| self.databases.get(name))
    }

    /// Gets a mutable reference to the current database.
    ///
    /// # Returns
    /// A mutable reference to the current database, or None if no database is selected.
    pub fn get_current_database_mut(&mut self) -> Option<&mut Database> {
        if let Some(name) = &self.current_database {
            self.databases.get_mut(name)
        } else {
            None
        }
    }

    /// Gets all database names.
    ///
    /// # Returns
    /// A vector of database names.
    pub fn get_database_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    /// Gets a database by name.
    ///
    /// # Parameters
    /// - `name`: The name of the database.
    ///
    /// # Returns
    /// A reference to the database, or None if it doesn't exist.
    pub fn get_database(&self, name: &str) -> Option<&Database> {
        self.databases.get(name)
    }

    /// Gets a mutable reference to a database by name.
    ///
    /// # Parameters
    /// - `name`: The name of the database.
    ///
    /// # Returns
    /// A mutable reference to the database, or None if it doesn't exist.
    pub fn get_database_mut(&mut self, name: &str) -> Option<&mut Database> {
        self.databases.get_mut(name)
    }

    /// Creates a new table in the current database and returns its metadata.
    ///
    /// # Parameters
    /// - `name`: The name of the new table. Note that all tables beginning with `__` are reserved for the system.
    /// - `schema`: The schema of the new table.
    ///
    /// # Returns
    /// Some(TableInfo) if table creation succeeds, None if current database is not set or table with same name already exists
    pub fn create_table(&mut self, name: String, schema: Schema) -> Option<TableInfo> {
        self.get_current_database_mut()?.create_table(name, schema)
    }

    /// Queries table metadata by name in the current database.
    ///
    /// # Parameters
    /// - `table_name`: The name of the table.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn get_table(&self, table_name: &str) -> Option<&TableInfo> {
        self.get_current_database()?.get_table(table_name)
    }

    /// Queries table metadata by OID in the current database.
    ///
    /// # Parameters
    /// - `table_oid`: The OID of the table to query.
    ///
    /// # Returns
    /// A (non-owning) pointer to the metadata for the table.
    pub fn get_table_by_oid(&self, table_oid: TableOidT) -> Option<&TableInfo> {
        self.get_current_database()?.get_table_by_oid(table_oid)
    }

    /// Creates a new index in the current database, populates existing data of the table, and returns its metadata.
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
        self.get_current_database_mut()?.create_index(
            index_name, table_name, key_schema, key_attrs, key_size, unique, index_type,
        )
    }

    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&Arc<IndexInfo>> {
        self.get_current_database()
            .map(|db| db.get_table_indexes(table_name))
            .unwrap_or_default()
    }

    pub fn get_index_by_index_oid(
        &self,
        index_oid: IndexOidT,
    ) -> Option<(Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)> {
        self.get_current_database()?
            .get_index_by_index_oid(index_oid)
    }

    pub fn add_index(
        &mut self,
        index_oid: IndexOidT,
        index_info: Arc<IndexInfo>,
        btree: Arc<RwLock<BPlusTree>>,
    ) {
        if let Some(db) = self.get_current_database_mut() {
            db.add_index(index_oid, index_info, btree);
        }
    }

    /// Gets the names of all tables in the current database.
    ///
    /// # Returns
    /// A vector of table names.
    pub fn get_table_names(&self) -> Vec<String> {
        self.get_current_database()
            .map(|db| db.get_table_names())
            .unwrap_or_default()
    }

    pub fn get_table_schema(&self, table_name: &str) -> Option<Schema> {
        self.get_current_database()?.get_table_schema(table_name)
    }

    /// Gets a reference to the buffer pool manager
    pub fn get_buffer_pool(&self) -> Arc<BufferPoolManager> {
        self.bpm.clone()
    }

    pub fn get_table_heap(&self, table_name: &str) -> Option<Arc<TableHeap>> {
        self.get_current_database()?.get_table_heap(table_name)
    }

    pub fn get_table_columns(&self, table_name: &str) -> Option<Vec<Column>> {
        self.get_current_database()?.get_table_columns(table_name)
    }

    pub fn get_all_tables(&self) -> Vec<String> {
        self.get_current_database()
            .map(|db| db.get_all_tables())
            .unwrap_or_default()
    }
}

impl Display for Catalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Catalog Contents:")?;
        writeln!(f, "----------------")?;

        // List all databases
        writeln!(f, "Databases:")?;
        for db_name in self.get_database_names() {
            if Some(&db_name) == self.current_database.as_ref() {
                writeln!(f, "* {} (current)", db_name)?;
            } else {
                writeln!(f, "  {}", db_name)?;
            }
        }
        writeln!(f)?;

        // Show contents of current database
        if let Some(current_db) = self.get_current_database() {
            write!(f, "{}", current_db)?;
        } else {
            writeln!(f, "No database selected")?;
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
    use crate::types_db::type_id::TypeId;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
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
            let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                Arc::from(disk_manager.unwrap()),
                replacer.clone(),
            ).unwrap());
            
            let txn_manager = Arc::new(TransactionManager::new());

            Self {
                bpm,
                txn_manager,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn txn_manager(&self) -> Arc<TransactionManager> {
            Arc::clone(&self.txn_manager)
        }
    }

    #[tokio::test]
    async fn test_database_operations() {
        let ctx = TestContext::new("test_database_operations").await;
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = Catalog::new(bpm, txn_manager);

        // Test creating a new database
        assert!(
            catalog.create_database("test_db".to_string()),
            "Failed to create database"
        );

        // Test creating a duplicate database (should fail)
        assert!(
            !catalog.create_database("test_db".to_string()),
            "Should not be able to create duplicate database"
        );

        // Test switching to a database
        assert!(
            catalog.use_database("test_db"),
            "Failed to switch to database"
        );
        assert_eq!(
            catalog.get_current_database_name(),
            Some(&"test_db".to_string())
        );

        // Test switching to a non-existent database
        assert!(
            !catalog.use_database("nonexistent_db"),
            "Should not be able to switch to non-existent database"
        );

        // Test getting all database names
        let db_names = catalog.get_database_names();
        assert_eq!(db_names.len(), 2); // default + test_db
        assert!(db_names.contains(&"default".to_string()));
        assert!(db_names.contains(&"test_db".to_string()));
    }

    #[tokio::test]
    async fn test_multi_database_tables() {
        let ctx = TestContext::new("test_multi_database_tables").await;
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = Catalog::new(bpm, txn_manager);

        // Create two databases
        catalog.create_database("db1".to_string());
        catalog.create_database("db2".to_string());

        // Create schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create table in db1
        catalog.use_database("db1");
        let table1 = catalog.create_table("table_in_db1".to_string(), schema.clone());
        assert!(table1.is_some(), "Failed to create table in db1");

        // Create table in db2
        catalog.use_database("db2");
        let table2 = catalog.create_table("table_in_db2".to_string(), schema.clone());
        assert!(table2.is_some(), "Failed to create table in db2");

        // Verify tables are in the correct databases
        catalog.use_database("db1");
        assert!(
            catalog.get_table("table_in_db1").is_some(),
            "Table should exist in db1"
        );
        assert!(
            catalog.get_table("table_in_db2").is_none(),
            "Table from db2 should not be visible in db1"
        );

        catalog.use_database("db2");
        assert!(
            catalog.get_table("table_in_db2").is_some(),
            "Table should exist in db2"
        );
        assert!(
            catalog.get_table("table_in_db1").is_none(),
            "Table from db1 should not be visible in db2"
        );
    }

    #[tokio::test]
    async fn test_create_table() {
        let ctx = TestContext::new("test_create_table").await;
        let bpm = ctx.bpm();
        let txn_manager = ctx.txn_manager();

        let mut catalog = Catalog::new(bpm, txn_manager);

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
}
