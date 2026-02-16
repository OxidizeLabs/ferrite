//! # System Catalog
//!
//! This module provides the internal system catalog tables that store metadata
//! about user-created tables, indexes, databases, and columns. These tables are
//! bootstrapped at database startup and used for catalog persistence and recovery.
//!
//! ## Architecture
//!
//! ```text
//!                         ┌─────────────────────────────────────────────┐
//!                         │            System Catalog                   │
//!                         │                                             │
//!                         │  ┌───────────────────────────────────────┐  │
//!                         │  │        SystemCatalogSchemas           │  │
//!                         │  │  (Schema definitions for sys tables)  │  │
//!                         │  └───────────────────────────────────────┘  │
//!                         │                     │                       │
//!                         │                     ▼                       │
//!                         │  ┌───────────────────────────────────────┐  │
//!                         │  │        SystemCatalogTables            │  │
//!                         │  │  (TableInfo instances for sys tables) │  │
//!                         │  │                                       │  │
//!                         │  │  __tables   (OID: 1)                  │  │
//!                         │  │  __indexes  (OID: 2)                  │  │
//!                         │  │  __dbs      (OID: 3)                  │  │
//!                         │  │  __columns  (OID: 4)                  │  │
//!                         │  └───────────────────────────────────────┘  │
//!                         └─────────────────────────────────────────────┘
//! ```
//!
//! ## System Tables
//!
//! | Table        | OID | Purpose                              |
//! |--------------|-----|--------------------------------------|
//! | `__tables`   | 1   | Stores table metadata                |
//! | `__indexes`  | 2   | Stores index metadata                |
//! | `__dbs`      | 3   | Stores database metadata             |
//! | `__columns`  | 4   | Stores column metadata               |
//!
//! ## Table Schemas
//!
//! ### `__tables` Schema
//!
//! | Column        | Type     | Description                    |
//! |---------------|----------|--------------------------------|
//! | table_oid     | BigInt   | Unique table identifier        |
//! | table_name    | VarChar  | Name of the table              |
//! | first_page_id | BigInt   | First page in table heap       |
//! | last_page_id  | BigInt   | Last page in table heap        |
//! | schema_bin    | Binary   | Serialized Schema (bincode)    |
//!
//! ### `__indexes` Schema
//!
//! | Column         | Type     | Description                    |
//! |----------------|----------|--------------------------------|
//! | index_oid      | BigInt   | Unique index identifier        |
//! | index_name     | VarChar  | Name of the index              |
//! | table_oid      | BigInt   | Associated table OID           |
//! | unique         | Boolean  | Whether index enforces unique  |
//! | index_type     | Integer  | Index type enum value          |
//! | key_attrs      | Binary   | Serialized key column indices  |
//! | key_schema_bin | Binary   | Serialized key Schema          |
//!
//! ### `__dbs` Schema
//!
//! | Column   | Type     | Description                    |
//! |----------|----------|--------------------------------|
//! | db_oid   | BigInt   | Unique database identifier     |
//! | db_name  | VarChar  | Name of the database           |
//!
//! ### `__columns` Schema
//!
//! | Column      | Type     | Description                    |
//! |-------------|----------|--------------------------------|
//! | table_oid   | BigInt   | Parent table OID               |
//! | column_name | VarChar  | Name of the column             |
//! | ordinal     | Integer  | Column position (0-indexed)    |
//! | column_type | Integer  | TypeId enum value              |
//! | column_len  | Integer  | Storage length in bytes        |
//! | is_pk       | Boolean  | Whether column is primary key  |
//!
//! ## Key Components
//!
//! | Component              | Description                                      |
//! |------------------------|--------------------------------------------------|
//! | `SystemCatalogSchemas` | Schema definitions for all system tables         |
//! | `SystemCatalogTables`  | `TableInfo` instances with heap storage          |
//! | `TableCatalogRow`      | Serializable row for `__tables`                  |
//! | `IndexCatalogRow`      | Serializable row for `__indexes`                 |
//!
//! ## Bootstrap Flow
//!
//! ```text
//!   Database Startup
//!         │
//!         ▼
//!   SystemCatalogTables::bootstrap(bpm)
//!         │
//!         ├──► Create SystemCatalogSchemas
//!         │
//!         ├──► Create TableHeap for each system table
//!         │         __tables  (OID 1)
//!         │         __indexes (OID 2)
//!         │         __dbs     (OID 3)
//!         │         __columns (OID 4)
//!         │
//!         └──► Wrap in TableInfo with schema
//!                    │
//!                    ▼
//!              SystemCatalogTables ready
//! ```
//!
//! ## Row Conversion
//!
//! ```text
//!   TableCatalogRow                        Tuple (in __tables heap)
//!   ┌─────────────────┐                   ┌─────────────────┐
//!   │ table_oid: 100  │   to_values()     │ [BigInt(100),   │
//!   │ table_name: "x" │ ───────────────►  │  VarChar("x"),  │
//!   │ first_page: 5   │                   │  BigInt(5),     │
//!   │ last_page: 10   │                   │  BigInt(10),    │
//!   │ schema_bin: ... │                   │  Binary(...)]   │
//!   └─────────────────┘                   └─────────────────┘
//!
//!                       from_tuple()
//!                     ◄───────────────
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::catalog::system_catalog::{SystemCatalogTables, TableCatalogRow};
//!
//! // Bootstrap system catalog during database init
//! let system = SystemCatalogTables::bootstrap(bpm.clone());
//!
//! // Insert a new table record
//! let row = TableCatalogRow {
//!     table_oid: 100,
//!     table_name: "users".to_string(),
//!     first_page_id: 5,
//!     last_page_id: 10,
//!     schema_bin: serialized_schema,
//! };
//!
//! let values = row.to_values();
//! let heap = system.tables.get_table_heap();
//! heap.insert_tuple_from_values(values, &schema, meta);
//!
//! // Scan and reconstruct during recovery
//! let mut scan = TableScanIterator::new(system.tables.clone());
//! while let Some((_, tuple)) = scan.next() {
//!     if let Some(row) = TableCatalogRow::from_tuple(&tuple) {
//!         // Rebuild in-memory catalog from row
//!     }
//! }
//! ```
//!
//! ## Thread Safety
//!
//! - `SystemCatalogTables` holds `Arc<TableInfo>` for each system table
//! - Thread-safe access is provided by the underlying `TableHeap` and buffer pool
//! - Multiple readers can scan system tables concurrently

use std::sync::Arc;

use log::debug;
use serde::{Deserialize, Serialize};

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, PageId, TableOidT};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};

/// Reserved OID for the `__tables` system catalog table.
pub const SYS_TABLES_OID: TableOidT = 1;

/// Reserved OID for the `__indexes` system catalog table.
pub const SYS_INDEXES_OID: TableOidT = 2;

/// Reserved OID for the `__dbs` system catalog table.
pub const SYS_DATABASES_OID: TableOidT = 3;

/// Reserved OID for the `__columns` system catalog table.
pub const SYS_COLUMNS_OID: TableOidT = 4;

/// Schema definitions for all system catalog tables.
///
/// These schemas define the structure of the internal metadata tables
/// used to persist catalog information across database restarts.
#[derive(Debug)]
pub struct SystemCatalogSchemas {
    /// Schema for `__tables`: stores table metadata (OID, name, page bounds, serialized schema).
    pub tables: Schema,
    /// Schema for `__indexes`: stores index metadata (OID, name, table ref, uniqueness, key info).
    pub indexes: Schema,
    /// Schema for `__dbs`: stores database metadata (OID, name).
    pub databases: Schema,
    /// Schema for `__columns`: stores column metadata (table ref, name, position, type, length, PK flag).
    pub columns: Schema,
}

impl Default for SystemCatalogSchemas {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemCatalogSchemas {
    /// Creates a new set of system catalog schemas.
    ///
    /// Each schema defines the column structure for a system catalog table:
    /// - `__tables`: table_oid, table_name, first_page_id, last_page_id, schema_bin
    /// - `__indexes`: index_oid, index_name, table_oid, unique, index_type, key_attrs, key_schema_bin
    /// - `__dbs`: db_oid, db_name
    /// - `__columns`: table_oid, column_name, ordinal, column_type, column_len, is_pk
    pub fn new() -> Self {
        let tables = Schema::new(vec![
            Column::new("table_oid", TypeId::BigInt),
            Column::new("table_name", TypeId::VarChar),
            Column::new("first_page_id", TypeId::BigInt),
            Column::new("last_page_id", TypeId::BigInt),
            Column::new("schema_bin", TypeId::Binary),
        ]);

        let indexes = Schema::new(vec![
            Column::new("index_oid", TypeId::BigInt),
            Column::new("index_name", TypeId::VarChar),
            Column::new("table_oid", TypeId::BigInt),
            Column::new("unique", TypeId::Boolean),
            Column::new("index_type", TypeId::Integer),
            Column::new("key_attrs", TypeId::Binary),
            Column::new("key_schema_bin", TypeId::Binary),
        ]);

        let databases = Schema::new(vec![
            Column::new("db_oid", TypeId::BigInt),
            Column::new("db_name", TypeId::VarChar),
        ]);

        let columns = Schema::new(vec![
            Column::new("table_oid", TypeId::BigInt),
            Column::new("column_name", TypeId::VarChar),
            Column::new("ordinal", TypeId::Integer),
            Column::new("column_type", TypeId::Integer),
            Column::new("column_len", TypeId::Integer),
            Column::new("is_pk", TypeId::Boolean),
        ]);

        Self {
            tables,
            indexes,
            databases,
            columns,
        }
    }
}

/// Serializable representation of a row in the `__tables` system catalog.
///
/// This struct provides bidirectional conversion between in-memory
/// table metadata and the tuple format stored in the system catalog heap.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableCatalogRow {
    /// Unique object identifier for the table.
    pub table_oid: TableOidT,
    /// Name of the table (e.g., "users", "orders").
    pub table_name: String,
    /// Page ID of the first page in the table's heap.
    pub first_page_id: PageId,
    /// Page ID of the last page in the table's heap.
    pub last_page_id: PageId,
    /// Bincode-serialized [`Schema`] for the table.
    pub schema_bin: Vec<u8>,
}

impl TableCatalogRow {
    /// Converts this row to a vector of values for insertion into `__tables`.
    ///
    /// The values are ordered to match the `__tables` schema:
    /// `[table_oid, table_name, first_page_id, last_page_id, schema_bin]`
    pub fn to_values(&self) -> Vec<Value> {
        vec![
            Value::new(self.table_oid as i64),
            Value::new(self.table_name.clone()),
            Value::new(self.first_page_id as i64),
            Value::new(self.last_page_id as i64),
            Value::new_with_type(Val::Binary(self.schema_bin.clone()), TypeId::Binary),
        ]
    }

    /// Reconstructs a `TableCatalogRow` from a tuple scanned from `__tables`.
    ///
    /// # Parameters
    /// - `tuple`: A tuple from the `__tables` heap.
    ///
    /// # Returns
    /// `Some(TableCatalogRow)` if all fields can be extracted, `None` otherwise.
    pub fn from_tuple(tuple: &crate::storage::table::tuple::Tuple) -> Option<Self> {
        let oid = value_to_i64(&tuple.get_value(0))? as TableOidT;
        let name = value_to_string(&tuple.get_value(1))?;
        let first = value_to_i64(&tuple.get_value(2))? as PageId;
        let last = value_to_i64(&tuple.get_value(3))? as PageId;
        let schema_bin = value_to_binary(&tuple.get_value(4))?;
        Some(Self {
            table_oid: oid,
            table_name: name,
            first_page_id: first,
            last_page_id: last,
            schema_bin,
        })
    }
}

/// Serializable representation of a row in the `__indexes` system catalog.
///
/// This struct provides conversion between in-memory index metadata and
/// the tuple format stored in the system catalog heap.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexCatalogRow {
    /// Unique object identifier for the index.
    pub index_oid: IndexOidT,
    /// Name of the index (e.g., "users_pk", "orders_date_idx").
    pub index_name: String,
    /// OID of the table this index is built on.
    pub table_oid: TableOidT,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Index type enum value (e.g., B+ tree, hash).
    pub index_type: i32,
    /// Column indices from the table schema that form the index key.
    pub key_attrs: Vec<usize>,
    /// Bincode-serialized [`Schema`] for the index key.
    pub key_schema_bin: Vec<u8>,
}

impl IndexCatalogRow {
    /// Converts this row to a vector of values for insertion into `__indexes`.
    ///
    /// The values are ordered to match the `__indexes` schema:
    /// `[index_oid, index_name, table_oid, unique, index_type, key_attrs, key_schema_bin]`
    ///
    /// The `key_attrs` field is serialized using bincode before storage.
    pub fn to_values(&self) -> Vec<Value> {
        let key_attrs_bin = postcard::to_allocvec(&self.key_attrs).unwrap_or_default();
        vec![
            Value::new(self.index_oid as i64),
            Value::new(self.index_name.clone()),
            Value::new(self.table_oid as i64),
            Value::new(self.unique),
            Value::new(self.index_type),
            Value::new_with_type(Val::Binary(key_attrs_bin), TypeId::Binary),
            Value::new_with_type(Val::Binary(self.key_schema_bin.clone()), TypeId::Binary),
        ]
    }
}

/// Extracts an `i64` from a [`Value`], coercing numeric types.
///
/// Handles `BigInt`, `Integer`, `SmallInt`, `TinyInt`, `Decimal`, `Float`,
/// `Boolean` (as 0/1), and string types (via parsing).
fn value_to_i64(value: &Value) -> Option<i64> {
    match value.get_val() {
        Val::BigInt(v) => Some(*v),
        Val::Integer(v) => Some(*v as i64),
        Val::SmallInt(v) => Some(*v as i64),
        Val::TinyInt(v) => Some(*v as i64),
        Val::Decimal(v) => Some(*v as i64),
        Val::Float(v) => Some(*v as i64),
        Val::Boolean(v) => Some(if *v { 1 } else { 0 }),
        Val::VarLen(s) | Val::ConstLen(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// Extracts a `String` from a [`Value`].
///
/// Handles `VarLen`, `ConstLen`, `JSON`, `UUID`, and `Binary` (via UTF-8 decode).
fn value_to_string(value: &Value) -> Option<String> {
    match value.get_val() {
        Val::VarLen(s) | Val::ConstLen(s) | Val::JSON(s) | Val::UUID(s) => Some(s.clone()),
        Val::Binary(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

/// Extracts a `Vec<u8>` from a [`Value`].
///
/// Handles `Binary` directly and string types (via UTF-8 encoding).
fn value_to_binary(value: &Value) -> Option<Vec<u8>> {
    match value.get_val() {
        Val::Binary(b) => Some(b.clone()),
        Val::VarLen(s) | Val::ConstLen(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// Runtime container for system catalog table instances.
///
/// Each field holds a [`TableInfo`] backed by a [`TableHeap`] for storing
/// system metadata. These tables are bootstrapped during database initialization
/// and used for catalog persistence and recovery.
#[derive(Debug)]
pub struct SystemCatalogTables {
    /// `__tables` system table: stores metadata about user-created tables.
    pub tables: Arc<TableInfo>,
    /// `__indexes` system table: stores metadata about indexes.
    pub indexes: Arc<TableInfo>,
    /// `__dbs` system table: stores metadata about databases.
    pub databases: Arc<TableInfo>,
    /// `__columns` system table: stores metadata about table columns.
    pub columns: Arc<TableInfo>,
}

impl SystemCatalogTables {
    /// Bootstraps the system catalog tables during database initialization.
    ///
    /// Creates the four core system catalog tables (`__tables`, `__indexes`,
    /// `__dbs`, `__columns`) with their schemas and backing heap storage.
    /// Each table is assigned a reserved OID (1-4).
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager for allocating pages.
    ///
    /// # Returns
    /// A new `SystemCatalogTables` with initialized table heaps.
    ///
    /// # Panics
    /// Panics if table heap creation fails (e.g., buffer pool exhausted).
    pub fn bootstrap(
        bpm: Arc<crate::buffer::buffer_pool_manager_async::BufferPoolManager>,
    ) -> Self {
        let schemas = SystemCatalogSchemas::new();

        let tables_heap = Arc::new(TableHeap::new(bpm.clone(), SYS_TABLES_OID));
        let indexes_heap = Arc::new(TableHeap::new(bpm.clone(), SYS_INDEXES_OID));
        let databases_heap = Arc::new(TableHeap::new(bpm.clone(), SYS_DATABASES_OID));
        let columns_heap = Arc::new(TableHeap::new(bpm.clone(), SYS_COLUMNS_OID));

        let tables = Arc::new(TableInfo::new(
            schemas.tables.clone(),
            "__tables".to_string(),
            tables_heap,
            SYS_TABLES_OID,
        ));
        let indexes = Arc::new(TableInfo::new(
            schemas.indexes.clone(),
            "__indexes".to_string(),
            indexes_heap,
            SYS_INDEXES_OID,
        ));
        let databases = Arc::new(TableInfo::new(
            schemas.databases.clone(),
            "__dbs".to_string(),
            databases_heap,
            SYS_DATABASES_OID,
        ));
        let columns = Arc::new(TableInfo::new(
            schemas.columns.clone(),
            "__columns".to_string(),
            columns_heap,
            SYS_COLUMNS_OID,
        ));

        debug!("Bootstrapped system catalog tables");

        Self {
            tables,
            indexes,
            databases,
            columns,
        }
    }
}
