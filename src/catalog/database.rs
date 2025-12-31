//! # Database Container
//!
//! This module provides the `Database` struct which represents a single database
//! instance containing tables and indexes. Each database maintains its own
//! namespace for table and index names, with separate OID counters for
//! allocating unique identifiers.
//!
//! ## Architecture
//!
//! ```text
//!                           ┌────────────────────────────────────────────┐
//!                           │                 Database                   │
//!                           │                                            │
//!                           │  name: "mydb"        db_oid: 42            │
//!                           │                                            │
//!                           │  ┌──────────────────────────────────────┐  │
//!                           │  │              Tables                  │  │
//!                           │  │  ┌─────────────────────────────────┐ │  │
//!                           │  │  │  table_names    │    tables     │ │  │
//!                           │  │  │ "users" → 5     │  5 → TableInfo│ │  │
//!                           │  │  │ "orders" → 6    │  6 → TableInfo│ │  │
//!                           │  │  └─────────────────────────────────┘ │  │
//!                           │  │           next_table_oid: 7          │  │
//!                           │  └──────────────────────────────────────┘  │
//!                           │                                            │
//!                           │  ┌──────────────────────────────────────┐  │
//!                           │  │              Indexes                 │  │
//!                           │  │  ┌─────────────────────────────────┐ │  │
//!                           │  │  │  index_names    │    indexes    │ │  │
//!                           │  │  │ "users_pk" → 0  │ 0 → (Info,B+) │ │  │
//!                           │  │  │ "orders_idx" →1 │ 1 → (Info,B+) │ │  │
//!                           │  │  └─────────────────────────────────┘ │  │
//!                           │  │           next_index_oid: 2          │  │
//!                           │  └──────────────────────────────────────┘  │
//!                           │                                            │
//!                           │  ┌────────────────────┐ ┌───────────────┐  │
//!                           │  │ BufferPoolManager  │ │  TxnManager   │  │
//!                           │  └────────────────────┘ └───────────────┘  │
//!                           └────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component       | Description                                           |
//! |-----------------|-------------------------------------------------------|
//! | `Database`      | Container for tables and indexes with OID management  |
//! | `tables`        | Map from OID to `TableInfo` metadata                  |
//! | `table_names`   | Map from name to OID for fast lookup                  |
//! | `indexes`       | Map from OID to `(IndexInfo, BPlusTree)` pairs        |
//! | `index_names`   | Map from name to OID for fast lookup                  |
//!
//! ## Table Creation Flow
//!
//! ```text
//!   create_table("users", schema)
//!            │
//!            ▼
//!   ┌─────────────────────────┐
//!   │ Check name uniqueness   │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Validate FK constraints │
//!   │ (referenced tables must │
//!   │  exist with columns)    │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Allocate table OID      │
//!   │ Create TableHeap        │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Register with           │
//!   │ TransactionManager      │
//!   │ (TransactionalTableHeap)│
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Update internal maps    │
//!   │ Return TableInfo        │
//!   └─────────────────────────┘
//! ```
//!
//! ## Index Creation Flow
//!
//! ```text
//!   create_index("users_pk", "users", key_schema, ...)
//!            │
//!            ▼
//!   ┌─────────────────────────┐
//!   │ Verify table exists     │
//!   │ Verify name is unique   │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Allocate index OID      │
//!   │ Create IndexInfo        │
//!   │ Create BPlusTree        │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Populate index with     │
//!   │ existing table data     │
//!   │ (full table scan)       │
//!   └───────────┬─────────────┘
//!               │
//!               ▼
//!   ┌─────────────────────────┐
//!   │ Update internal maps    │
//!   │ Return (IndexInfo, B+)  │
//!   └─────────────────────────┘
//! ```
//!
//! ## OID Management
//!
//! ```text
//!   Global: NEXT_DATABASE_ID (AtomicU64)
//!              │
//!              ▼
//!        Database::new() ─────► db_oid = fetch_add(1)
//!
//!   Per-Database:
//!        next_table_oid ─────► Starts at SYS_COLUMNS_OID + 1
//!        next_index_oid ─────► Starts at 0
//! ```
//!
//! Table OIDs start after system catalog table OIDs to avoid conflicts.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::catalog::database::Database;
//! use crate::catalog::schema::Schema;
//! use crate::catalog::column::Column;
//! use crate::types_db::type_id::TypeId;
//!
//! // Create a new database
//! let mut db = Database::new("mydb".to_string(), bpm.clone(), txn_manager.clone());
//!
//! // Create a table
//! let schema = Schema::new(vec![
//!     Column::new("id", TypeId::Integer),
//!     Column::new("name", TypeId::VarChar),
//! ]);
//! let table_info = db.create_table("users".to_string(), schema.clone());
//!
//! // Create an index
//! let key_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
//! let index = db.create_index(
//!     "users_pk",
//!     "users",
//!     key_schema,
//!     vec![0],  // column indices
//!     4,        // key size
//!     true,     // unique
//!     IndexType::BPlusTreeIndex,
//! );
//!
//! // Query metadata
//! let table = db.get_table("users");
//! let indexes = db.get_table_indexes("users");
//! ```
//!
//! ## Foreign Key Validation
//!
//! When creating tables with foreign key constraints, the database validates:
//!
//! 1. Referenced table must exist in the database
//! 2. Referenced column must exist in the referenced table's schema
//!
//! If validation fails, table creation is rejected with a warning.
//!
//! ## Thread Safety
//!
//! - `Database` itself is not internally synchronized
//! - Individual components are thread-safe:
//!   - `BufferPoolManager`: Internal locking
//!   - `BPlusTree`: Wrapped in `Arc<RwLock<...>>`
//!   - `TransactionManager`: Thread-safe
//! - Global `NEXT_DATABASE_ID` uses atomic operations
//! - Callers typically wrap `Database` in `Arc<RwLock<Database>>`
//!
//! ## Recovery
//!
//! The `with_existing_data` constructor allows restoring a database from
//! persisted metadata (e.g., from system catalog tables):
//!
//! ```rust,ignore
//! let db = Database::with_existing_data(
//!     "restored_db".to_string(),
//!     bpm,
//!     next_index_oid,
//!     next_table_oid,
//!     tables,      // HashMap<TableOidT, TableInfo>
//!     indexes,     // HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>
//!     table_names, // HashMap<String, TableOidT>
//!     index_names, // HashMap<String, IndexOidT>
//!     txn_manager,
//!     db_oid,
//! );
//! ```

use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::catalog::system_catalog::SYS_COLUMNS_OID;
use crate::common::config::{DataBaseOid, IndexOidT, TableOidT};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::{IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A static atomic counter for generating unique database IDs
static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

/// A Database is a collection of tables and indexes.
#[derive(Debug)]
pub struct Database {
    name: String,
    db_oid: DataBaseOid,
    bpm: Arc<BufferPoolManager>,
    tables: HashMap<TableOidT, TableInfo>,
    table_names: HashMap<String, TableOidT>,
    next_table_oid: TableOidT,
    indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
    index_names: HashMap<String, IndexOidT>,
    next_index_oid: IndexOidT,
    txn_manager: Arc<TransactionManager>,
}

impl Database {
    /// Creates a new Database with the given name.
    ///
    /// # Parameters
    /// - `name`: The name of the database.
    /// - `bpm`: The buffer pool manager backing tables created in this database.
    /// - `txn_manager`: The transaction manager in use by the system.
    pub fn new(
        name: String,
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        // Generate a new unique database OID using the atomic counter
        let db_oid = NEXT_DATABASE_ID.fetch_add(1, Ordering::SeqCst);

        Database {
            name,
            db_oid,
            bpm,
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_oid: SYS_COLUMNS_OID + 1,
            indexes: HashMap::new(),
            index_names: HashMap::new(),
            next_index_oid: 0,
            txn_manager,
        }
    }

    /// Creates a new Database with a specific OID.
    ///
    /// # Parameters
    /// - `name`: The name of the database.
    /// - `db_oid`: The specific database OID to use.
    /// - `bpm`: The buffer pool manager backing tables created in this database.
    /// - `txn_manager`: The transaction manager in use by the system.
    pub fn new_with_oid(
        name: String,
        db_oid: DataBaseOid,
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        // Update the global counter if necessary to avoid future conflicts
        let current_max = NEXT_DATABASE_ID.load(Ordering::SeqCst);
        if db_oid >= current_max {
            NEXT_DATABASE_ID.store(db_oid + 1, Ordering::SeqCst);
        }

        Database {
            name,
            db_oid,
            bpm,
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_oid: SYS_COLUMNS_OID + 1,
            indexes: HashMap::new(),
            index_names: HashMap::new(),
            next_index_oid: 0,
            txn_manager,
        }
    }

    /// Creates a new Database with existing data.
    ///
    /// # Parameters
    /// - `name`: The name of the database.
    /// - `bpm`: The buffer pool manager backing tables created in this database.
    /// - `next_index_oid`: The next available index OID.
    /// - `next_table_oid`: The next available table OID.
    /// - `tables`: The existing tables.
    /// - `indexes`: The existing indexes.
    /// - `table_names`: The mapping of table names to OIDs.
    /// - `index_names`: The mapping of index names to OIDs.
    /// - `txn_manager`: The transaction manager in use by the system.
    #[allow(clippy::too_many_arguments)] // Recovery requires all state components
    pub fn with_existing_data(
        name: String,
        bpm: Arc<BufferPoolManager>,
        next_index_oid: IndexOidT,
        next_table_oid: TableOidT,
        tables: HashMap<TableOidT, TableInfo>,
        indexes: HashMap<IndexOidT, (Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)>,
        table_names: HashMap<String, TableOidT>,
        index_names: HashMap<String, IndexOidT>,
        txn_manager: Arc<TransactionManager>,
        db_oid: DataBaseOid,
    ) -> Self {
        // Update the global counter if necessary to avoid future conflicts
        let current_max = NEXT_DATABASE_ID.load(Ordering::SeqCst);
        if db_oid >= current_max {
            NEXT_DATABASE_ID.store(db_oid + 1, Ordering::SeqCst);
        }

        Database {
            name,
            db_oid,
            bpm,
            tables,
            table_names,
            next_table_oid: next_table_oid.max(SYS_COLUMNS_OID + 1),
            indexes,
            index_names,
            next_index_oid,
            txn_manager,
        }
    }

    /// Gets the name of the database.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Gets the OID of the database.
    pub fn get_oid(&self) -> DataBaseOid {
        self.db_oid
    }

    /// Creates a new table in the database and returns its metadata.
    ///
    /// # Parameters
    /// - `name`: The name of the new table.
    /// - `schema`: The schema of the new table.
    ///
    /// # Returns
    /// Some(TableInfo) if table creation succeeds, None if table with same name already exists
    pub fn create_table(&mut self, name: String, schema: Schema) -> Option<TableInfo> {
        // Check if table with this name already exists
        if self.table_names.contains_key(&name) {
            return None;
        }

        // Validate foreign key constraints
        if let Err(err) = self.validate_foreign_key_constraints(&schema) {
            warn!("Cannot create table '{}': {}", name, err);
            return None;
        }

        let table_oid = self.next_table_oid;
        let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), table_oid));

        // Create and register transactional table heap with the transaction manager
        let transactional_table_heap =
            Arc::new(TransactionalTableHeap::new(table_heap.clone(), table_oid));
        self.txn_manager.register_table(transactional_table_heap);

        // Increment table OID
        self.next_table_oid += 1;

        // Create table info
        let table_info = TableInfo::new(schema.clone(), name.clone(), table_heap, table_oid);

        // Add to database maps
        self.table_names.insert(name.clone(), table_oid);
        self.tables.insert(table_oid, table_info.clone());

        // Print confirmation
        info!(
            "Table '{}' created successfully with OID {} in database '{}'",
            name, table_oid, self.name
        );

        Some(table_info)
    }

    /// Validates foreign key constraints for a schema.
    ///
    /// # Parameters
    /// - `schema`: The schema to validate.
    ///
    /// # Returns
    /// Ok(()) if all foreign key constraints are valid, Err(String) with error message otherwise
    fn validate_foreign_key_constraints(&self, schema: &Schema) -> Result<(), String> {
        for column in schema.get_columns() {
            if let Some(fk) = column.get_foreign_key() {
                // Check if referenced table exists
                if !self.table_names.contains_key(&fk.referenced_table) {
                    return Err(format!(
                        "foreign key constraint references non-existent table '{}'",
                        fk.referenced_table
                    ));
                }

                // Check if referenced column exists in the referenced table
                if let Some(referenced_table_schema) = self.get_table_schema(&fk.referenced_table) {
                    if referenced_table_schema
                        .get_column_index(&fk.referenced_column)
                        .is_none()
                    {
                        return Err(format!(
                            "foreign key constraint references non-existent column '{}' in table '{}'",
                            fk.referenced_column, fk.referenced_table
                        ));
                    }
                } else {
                    return Err(format!(
                        "foreign key constraint references non-existent table '{}'",
                        fk.referenced_table
                    ));
                }
            }
        }
        Ok(())
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
    #[allow(clippy::too_many_arguments)] // Index creation requires all these parameters
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
        // Implementation similar to the original Catalog::create_index but operating on this database's tables
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
            },
        };

        // Update catalog maps
        self.index_names.insert(index_name.to_string(), index_oid);
        self.indexes
            .insert(index_oid, (index_info.clone(), index.clone()));

        self.next_index_oid += 1;

        // Populate the index with existing table data
        if let Some(table_info) = self.get_table_info_by_name(table_name) {
            self.populate_index_with_existing_data(&index, &index_info, &table_info);
        }

        // Return reference to the newly created index info
        self.get_index_by_index_oid(index_oid)
    }

    /// Returns all indexes associated with the specified table.
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

    /// Returns an index by its OID.
    pub fn get_index_by_index_oid(
        &self,
        index_oid: IndexOidT,
    ) -> Option<(Arc<IndexInfo>, Arc<RwLock<BPlusTree>>)> {
        self.indexes.get(&index_oid).cloned()
    }

    /// Adds an index to this database.
    pub fn add_index(
        &mut self,
        index_oid: IndexOidT,
        index_info: Arc<IndexInfo>,
        btree: Arc<RwLock<BPlusTree>>,
    ) {
        self.indexes.insert(index_oid, (index_info, btree));
    }

    /// Gets the names of all tables in this database.
    ///
    /// # Returns
    /// A vector of table names.
    pub fn get_table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }

    /// Returns all table metadata objects.
    pub fn get_all_table_info(&self) -> Vec<&TableInfo> {
        self.tables.values().collect()
    }

    /// Loads table metadata from catalog rows, updating name maps and OID counters.
    pub fn load_tables_from_catalog(&mut self, tables: Vec<TableInfo>) {
        for table in tables {
            let oid = table.get_table_oidt();
            self.next_table_oid = self.next_table_oid.max(oid + 1);
            self.table_names
                .insert(table.get_table_name().to_string(), oid);
            self.tables.insert(oid, table);
        }
    }

    /// Returns the schema for the specified table.
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

    /// Returns the table info for the specified table name.
    fn get_table_info_by_name(&self, table_name: &str) -> Option<Arc<TableInfo>> {
        let table_oid = self.table_names.get(table_name)?;
        self.tables
            .get(table_oid)
            .map(|info| Arc::new(info.clone()))
    }

    /// Returns the table heap for the specified table.
    pub fn get_table_heap(&self, table_name: &str) -> Option<Arc<TableHeap>> {
        let table_info = self.get_table_info_by_name(table_name)?;
        Some(table_info.get_table_heap())
    }

    /// Returns the columns for the specified table.
    pub fn get_table_columns(&self, table_name: &str) -> Option<Vec<Column>> {
        let schema = self.get_table_schema(table_name)?;
        Some(schema.get_columns().to_vec())
    }

    /// Returns all table names in this database.
    pub fn get_all_tables(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
    }

    /// Populates a newly created index with existing data from the table
    fn populate_index_with_existing_data(
        &self,
        index: &Arc<RwLock<BPlusTree>>,
        index_info: &Arc<IndexInfo>,
        table_info: &Arc<TableInfo>,
    ) {
        use crate::storage::table::table_iterator::TableScanIterator;
        use log::debug;

        debug!(
            "Populating index '{}' with existing table data",
            index_info.get_index_name()
        );

        let key_attrs = index_info.get_key_attrs();

        // Create a table scan iterator to scan through all tuples
        let mut iterator = TableScanIterator::new(table_info.clone());

        let mut entry_count = 0;

        // Iterate through all tuples in the table
        while let Some((_meta, tuple)) = iterator.next() {
            let rid = iterator.get_rid();

            // Extract key values for the index
            let mut key_values = Vec::new();
            let tuple_values = tuple.get_values();

            for &col_idx in key_attrs {
                if col_idx < tuple_values.len() {
                    key_values.push(tuple_values[col_idx].clone());
                }
            }

            // For single-column indexes, use the value directly
            if key_values.len() == 1 {
                let key_value = key_values[0].clone();

                // Insert into B+ tree
                let mut btree_write = index.write();
                btree_write.insert(key_value, rid);
                entry_count += 1;

                debug!(
                    "Inserted index entry for key: {:?}, RID: {:?}",
                    key_values[0], rid
                );
            } else if key_values.len() > 1 {
                // For composite keys, we would need to handle this differently
                // For now, we'll skip composite keys
                log::warn!("Composite key index population not yet implemented");
            }
        }

        debug!(
            "Successfully populated index '{}' with {} entries",
            index_info.get_index_name(),
            entry_count
        );
    }
}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Database: {}", self.name)?;
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
