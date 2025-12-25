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

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, PageId, TableOidT, storage_bincode_config};
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use bincode::{Decode, Encode};
use log::debug;
use std::sync::Arc;

/// Reserved OIDs for system catalog tables.
pub const SYS_TABLES_OID: TableOidT = 1;
pub const SYS_INDEXES_OID: TableOidT = 2;
pub const SYS_DATABASES_OID: TableOidT = 3;
pub const SYS_COLUMNS_OID: TableOidT = 4;

/// Minimum set of catalog schemas used to persist metadata.
#[derive(Debug)]
pub struct SystemCatalogSchemas {
    pub tables: Schema,
    pub indexes: Schema,
    pub databases: Schema,
    pub columns: Schema,
}

impl SystemCatalogSchemas {
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

/// Serializable representation of a catalog table row for tables.
#[derive(Debug, Encode, Decode, Clone)]
pub struct TableCatalogRow {
    pub table_oid: TableOidT,
    pub table_name: String,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
    pub schema_bin: Vec<u8>,
}

impl TableCatalogRow {
    pub fn to_values(&self) -> Vec<Value> {
        vec![
            Value::new(self.table_oid as i64),
            Value::new(self.table_name.clone()),
            Value::new(self.first_page_id as i64),
            Value::new(self.last_page_id as i64),
            Value::new_with_type(Val::Binary(self.schema_bin.clone()), TypeId::Binary),
        ]
    }

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

#[derive(Debug, Encode, Decode, Clone)]
pub struct IndexCatalogRow {
    pub index_oid: IndexOidT,
    pub index_name: String,
    pub table_oid: TableOidT,
    pub unique: bool,
    pub index_type: i32,
    pub key_attrs: Vec<usize>,
    pub key_schema_bin: Vec<u8>,
}

impl IndexCatalogRow {
    pub fn to_values(&self) -> Vec<Value> {
        let key_attrs_bin =
            bincode::encode_to_vec(&self.key_attrs, storage_bincode_config()).unwrap_or_default();
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

fn value_to_string(value: &Value) -> Option<String> {
    match value.get_val() {
        Val::VarLen(s) | Val::ConstLen(s) | Val::JSON(s) | Val::UUID(s) => Some(s.clone()),
        Val::Binary(b) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

fn value_to_binary(value: &Value) -> Option<Vec<u8>> {
    match value.get_val() {
        Val::Binary(b) => Some(b.clone()),
        Val::VarLen(s) | Val::ConstLen(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// Holds the system catalog table infos for reuse.
#[derive(Debug)]
pub struct SystemCatalogTables {
    pub tables: Arc<TableInfo>,
    pub indexes: Arc<TableInfo>,
    pub databases: Arc<TableInfo>,
    pub columns: Arc<TableInfo>,
}

impl SystemCatalogTables {
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
