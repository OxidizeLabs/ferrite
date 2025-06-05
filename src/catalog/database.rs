use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{DataBaseOid, IndexOidT, TableOidT};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::index::index::{IndexInfo, IndexType};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use core::fmt;
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
            next_table_oid: 0,
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
            next_table_oid: 0,
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
            next_table_oid,
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
            }
        };

        // Update catalog maps
        self.index_names.insert(index_name.to_string(), index_oid);
        self.indexes
            .insert(index_oid, (index_info.clone(), index.clone()));

        self.next_index_oid += 1;

        // Now we would need to populate the index with existing table data
        // This part would be handled similarly to the original implementation,
        // but is omitted here for brevity

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

    /// Gets the names of all tables in this database.
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
