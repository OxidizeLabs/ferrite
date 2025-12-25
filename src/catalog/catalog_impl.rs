//! # Catalog Implementation
//!
//! This module provides the central metadata repository for the DBMS, managing
//! databases, tables, indexes, and their associated schemas. The catalog serves
//! as the authoritative source for all schema information used by the query
//! execution engine.
//!
//! ## Architecture
//!
//! ```text
//!                              ┌─────────────────────────────────────────┐
//!                              │               Catalog                   │
//!                              │                                         │
//!                              │  ┌─────────────────────────────────┐    │
//!                              │  │      databases: HashMap         │    │
//!                              │  │  ┌───────────┐  ┌───────────┐   │    │
//!                              │  │  │ "default" │  │  "db1"    │   │    │
//!                              │  │  │  Database │  │  Database │   │    │
//!                              │  │  └─────┬─────┘  └─────┬─────┘   │    │
//!                              │  └────────┼──────────────┼─────────┘    │
//!                              │           │              │              │
//!                              │  ┌────────▼──────────────▼─────────┐    │
//!                              │  │    Per-Database Metadata        │    │
//!                              │  │  ┌─────────┐  ┌─────────────┐   │    │
//!                              │  │  │ Tables  │  │   Indexes   │   │    │
//!                              │  │  │ HashMap │  │   HashMap   │   │    │
//!                              │  │  └─────────┘  └─────────────┘   │    │
//!                              │  └──────────────────────────────────┘   │
//!                              │                                         │
//!                              │  ┌─────────────────────────────────┐    │
//!                              │  │     System Catalog Tables       │    │
//!                              │  │  __tables  │  __indexes         │    │
//!                              │  │  (persisted metadata)           │    │
//!                              │  └─────────────────────────────────┘    │
//!                              └─────────────────────────────────────────┘
//!                                               │
//!                                               ▼
//!                              ┌─────────────────────────────────────────┐
//!                              │         BufferPoolManager               │
//!                              │    (backing storage for all tables)     │
//!                              └─────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component               | Description                                              |
//! |-------------------------|----------------------------------------------------------|
//! | `Catalog`               | Central metadata manager for all databases               |
//! | `Database`              | Per-database container for tables and indexes            |
//! | `CatalogCreationParams` | Parameters for bootstrapping with existing data          |
//! | `IndexCreationParams`   | Parameters for creating new indexes                      |
//! | `SystemCatalogTables`   | Persistent storage for table/index metadata              |
//!
//! ## Database Operations
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │ create_database │     │  use_database   │     │  get_database   │
//! │     ("db1")     │     │    ("db1")      │     │    ("db1")      │
//! └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
//!          │                       │                       │
//!          ▼                       ▼                       ▼
//!    Creates new            Sets current_database    Returns &Database
//!    Database instance      to "db1"                 reference
//! ```
//!
//! ## Table & Index Lifecycle
//!
//! ```text
//!  CREATE TABLE users (...)           CREATE INDEX idx ON users(id)
//!          │                                    │
//!          ▼                                    ▼
//! ┌────────────────────┐             ┌────────────────────┐
//! │   create_table()   │             │   create_index()   │
//! └─────────┬──────────┘             └─────────┬──────────┘
//!           │                                  │
//!           ▼                                  ▼
//! ┌────────────────────┐             ┌────────────────────┐
//! │ Database.create_   │             │ Database.create_   │
//! │   table()          │             │   index()          │
//! └─────────┬──────────┘             └─────────┬──────────┘
//!           │                                  │
//!           ▼                                  ▼
//! ┌────────────────────┐             ┌────────────────────┐
//! │ record_system_     │             │ record_system_     │
//! │   table()          │             │   index()          │
//! │ (persist to        │             │ (persist to        │
//! │  __tables)         │             │  __indexes)        │
//! └────────────────────┘             └────────────────────┘
//! ```
//!
//! ## System Catalog Tables
//!
//! The catalog maintains two internal tables for persistence:
//!
//! | Table       | Purpose                              | Key Fields                        |
//! |-------------|--------------------------------------|-----------------------------------|
//! | `__tables`  | Stores table metadata                | oid, name, first_page, last_page, schema |
//! | `__indexes` | Stores index metadata                | oid, name, table_oid, key_attrs, unique  |
//!
//! ## Recovery & Rebuild
//!
//! ```text
//!                    rebuild_from_system_catalog()
//!                              │
//!          ┌───────────────────┼───────────────────┐
//!          ▼                   ▼                   ▼
//!   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//!   │   Load      │    │   Scan      │    │   Rebuild   │
//!   │  snapshot   │    │  __tables   │    │  in-memory  │
//!   │ (optional)  │    │   heap      │    │   maps      │
//!   └─────────────┘    └─────────────┘    └─────────────┘
//!                              │
//!                              ▼
//!                    ┌─────────────────┐
//!                    │  Save updated   │
//!                    │    snapshot     │
//!                    └─────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::catalog::catalog_impl::Catalog;
//! use crate::catalog::schema::Schema;
//! use crate::catalog::column::Column;
//! use crate::types_db::type_id::TypeId;
//!
//! // Create catalog with buffer pool and transaction manager
//! let mut catalog = Catalog::new(bpm.clone(), txn_manager.clone());
//!
//! // Create and switch to a new database
//! catalog.create_database("mydb".to_string());
//! catalog.use_database("mydb");
//!
//! // Define schema and create table
//! let schema = Schema::new(vec![
//!     Column::new("id", TypeId::Integer),
//!     Column::new("name", TypeId::VarChar),
//! ]);
//! let table_info = catalog.create_table("users".to_string(), schema.clone());
//!
//! // Create index on the table
//! let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
//! let index = catalog.create_index(
//!     "users_pk",
//!     "users",
//!     key_schema,
//!     vec![0],  // key_attrs: column indices
//!     4,        // key_size
//!     true,     // unique
//!     IndexType::BPlusTree,
//! );
//!
//! // Query metadata
//! if let Some(table) = catalog.get_table("users") {
//!     let heap = table.get_table_heap();
//!     // ... perform operations
//! }
//! ```
//!
//! ## Thread Safety
//!
//! - The `Catalog` itself is not internally synchronized
//! - Individual components use appropriate synchronization:
//!   - `BufferPoolManager`: Thread-safe via internal locks
//!   - `BPlusTree`: Wrapped in `Arc<RwLock<BPlusTree>>`
//!   - `TableHeap`: Thread-safe via page-level locking
//! - Callers should synchronize access to the `Catalog` when used from
//!   multiple threads (typically wrapped in `Arc<RwLock<Catalog>>`)
//!
//! ## Design Notes
//!
//! - **Non-persistent in-memory maps**: The hashmaps are rebuilt on startup
//!   from system catalog tables for durability
//! - **Snapshot acceleration**: Catalog snapshots (`catalog.snapshot`) speed
//!   up rebuild by caching serialized metadata
//! - **OID generation**: Each database maintains its own OID counters for
//!   tables and indexes
//! - **Schema serialization**: Uses `bincode` for compact schema storage in
//!   system catalog rows

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::column::Column;
use crate::catalog::database::Database;
use crate::catalog::schema::Schema;
use crate::catalog::system_catalog::{
    SYS_TABLES_OID, SystemCatalogSchemas, SystemCatalogTables, TableCatalogRow,
};
use crate::common::config::{IndexOidT, TableOidT, storage_bincode_config};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::{IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::table_iterator::TableScanIterator;
use crate::storage::table::tuple::TupleMeta;
use crate::types_db::value::Value;
use bincode::{decode_from_slice, encode_to_vec};
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Parameters for creating a catalog with existing data
pub struct CatalogCreationParams {
    pub bpm: Arc<BufferPoolManager>,
    pub next_index_oid: IndexOidT,
    pub next_table_oid: TableOidT,
    pub tables: HashMap<TableOidT, TableInfo>,
    pub indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
    pub table_names: HashMap<String, TableOidT>,
    pub index_names: HashMap<String, IndexOidT>,
    pub txn_manager: Arc<TransactionManager>,
}

/// Parameters for creating an index
pub struct IndexCreationParams {
    pub index_name: String,
    pub table_name: String,
    pub key_schema: Schema,
    pub key_attrs: Vec<usize>,
    pub key_size: usize,
    pub unique: bool,
    pub index_type: IndexType,
}

/// The Catalog is a non-persistent catalog that is designed for
/// use by executors within the DBMS execution engine. It handles
/// database creation, database lookup, table creation, table lookup,
/// index creation, and index lookup.
#[derive(Debug)]
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    databases: HashMap<String, Database>,
    current_database: Option<String>,
    system: SystemCatalogTables,
    system_schemas: SystemCatalogSchemas,
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
    #[allow(clippy::too_many_arguments)]
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
        let system_schemas = SystemCatalogSchemas::new();
        let system = SystemCatalogTables::bootstrap(bpm.clone());

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

        let mut catalog = Catalog {
            bpm,
            databases,
            current_database: Some("default".to_string()),
            system,
            system_schemas,
            txn_manager,
        };

        // Seed system catalog with any provided tables
        if let Some(default_db_ref) = catalog.databases.get("default") {
            for table_info in default_db_ref.get_all_table_info() {
                catalog.record_system_table(table_info, &table_info.get_table_schema());
            }
        }

        catalog
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
        let table_info = self
            .get_current_database_mut()?
            .create_table(name.clone(), schema.clone())?;

        // Record metadata in system catalog (__tables)
        self.record_system_table(&table_info, &schema);
        Some(table_info)
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
    #[allow(clippy::too_many_arguments)]
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
        let result = self.get_current_database_mut()?.create_index(
            index_name,
            table_name,
            key_schema.clone(),
            key_attrs.clone(),
            key_size,
            unique,
            index_type.clone(),
        );

        if let Some((index_info, btree)) = &result {
            self.record_system_index(
                index_info.get_index_oid(),
                index_info,
                &key_schema,
                &key_attrs,
                unique,
                index_type,
            );
            // Ensure index metadata is tracked for lookups
            self.add_index(
                index_info.get_index_oid(),
                index_info.clone(),
                btree.clone(),
            );
        }

        result
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

    /// Rebuilds in-memory catalog maps by scanning the persisted system catalog tables.
    pub fn rebuild_from_system_catalog(&mut self) {
        let snapshot_path = Path::new("catalog.snapshot");
        let mut scan = TableScanIterator::new(self.system.tables.clone());
        let mut rebuilt: Vec<TableInfo> = Vec::new();
        let mut rows: Vec<TableCatalogRow> = Vec::new();

        // Load snapshot first if available to seed faster rebuild
        if snapshot_path.exists()
            && let Ok(bytes) = fs::read(snapshot_path)
            && let Ok((snapshot_rows, _)) =
                decode_from_slice::<Vec<TableCatalogRow>, _>(&bytes, storage_bincode_config())
        {
            for row in snapshot_rows {
                if row.table_oid <= SYS_TABLES_OID {
                    continue;
                }
                if let Some(info) = self.row_to_table_info(&row) {
                    rebuilt.push(info);
                }
            }
        }

        for (_meta, tuple) in scan {
            if let Some(row) = TableCatalogRow::from_tuple(&tuple) {
                // Skip system tables themselves
                if row.table_oid <= SYS_TABLES_OID {
                    continue;
                }

                if let Some(info) = self.row_to_table_info(&row) {
                    rebuilt.push(info);
                    rows.push(row);
                }
            }
        }

        if let Some(db) = self.get_current_database_mut() {
            db.load_tables_from_catalog(rebuilt);
        }

        if !rows.is_empty()
            && let Ok(bytes) = encode_to_vec(&rows, storage_bincode_config())
        {
            let _ = fs::write(snapshot_path, bytes);
        }
    }

    fn row_to_table_info(&self, row: &TableCatalogRow) -> Option<TableInfo> {
        let (schema, _): (Schema, usize) =
            match decode_from_slice::<Schema, _>(&row.schema_bin, storage_bincode_config()) {
                Ok(res) => res,
                Err(err) => {
                    warn!(
                        "Failed to decode schema for table {}: {}",
                        row.table_oid, err
                    );
                    return None;
                },
            };

        let heap = Arc::new(TableHeap::reopen(
            self.bpm.clone(),
            row.table_oid,
            row.first_page_id,
            row.last_page_id,
        ));

        Some(TableInfo::new(
            schema.clone(),
            row.table_name.clone(),
            heap,
            row.table_oid,
        ))
    }
}

impl Catalog {
    fn record_system_table(&self, table_info: &TableInfo, schema: &Schema) {
        let encoded_schema =
            bincode::encode_to_vec(schema, crate::common::config::storage_bincode_config())
                .unwrap_or_default();

        let row = TableCatalogRow {
            table_oid: table_info.get_table_oidt(),
            table_name: table_info.get_table_name().to_string(),
            first_page_id: table_info.get_table_heap().get_first_page_id_raw(),
            last_page_id: table_info.get_table_heap().get_last_page_id_raw(),
            schema_bin: encoded_schema,
        };

        let values: Vec<Value> = row.to_values();
        let sys_table = self.system.tables.get_table_heap();
        let meta = Arc::new(TupleMeta::new(0));

        let _ = sys_table.insert_tuple_from_values(values, &self.system_schemas.tables, meta);

        info!(
            "Recorded table '{}' (oid {}) in system catalog",
            table_info.get_table_name(),
            table_info.get_table_oidt()
        );
    }

    fn record_system_index(
        &self,
        index_oid: IndexOidT,
        index_info: &Arc<IndexInfo>,
        key_schema: &Schema,
        key_attrs: &[usize],
        unique: bool,
        index_type: IndexType,
    ) {
        let encoded_schema =
            bincode::encode_to_vec(key_schema, crate::common::config::storage_bincode_config())
                .unwrap_or_default();
        let row = crate::catalog::system_catalog::IndexCatalogRow {
            index_oid,
            index_name: index_info.get_index_name().clone(),
            table_oid: self
                .get_table(index_info.get_table_name())
                .map(|t| t.get_table_oidt())
                .unwrap_or_default(),
            unique,
            index_type: index_type as i32,
            key_attrs: key_attrs.to_vec(),
            key_schema_bin: encoded_schema,
        };

        let values: Vec<Value> = row.to_values();
        let sys_table = self.system.indexes.get_table_heap();
        let meta = Arc::new(TupleMeta::new(0));
        let _ = sys_table.insert_tuple_from_values(values, &self.system_schemas.indexes, meta);
        info!(
            "Recorded index '{}' (oid {}) in system catalog",
            index_info.get_index_name(),
            index_oid
        );
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
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::types_db::type_id::TypeId;
    use parking_lot::RwLock;
    use tempfile::TempDir;

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
