//! # Server Protocol
//!
//! This module defines the wire protocol types for client-server communication in Ferrite.
//! All types are serializable via bincode for efficient binary transmission over TCP.
//!
//! ## Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                          Client                                         │
//!   │                                                                         │
//!   │   DatabaseClient                                                        │
//!   │        │                                                                │
//!   │        │  execute_query("SELECT * FROM users")                          │
//!   │        │                                                                │
//!   │        ▼                                                                │
//!   │   DatabaseRequest::Query("SELECT * FROM users")                         │
//!   │        │                                                                │
//!   │        │  bincode::encode()                                             │
//!   │        ▼                                                                │
//!   │   [binary bytes] ─────────────────────────────────────────────────────► │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!                                    │ TCP
//!                                    │
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                          Server                                         │
//!   │                                                                         │
//!   │ ◄─────────────────────────────────────────────────────────────────────  │
//!   │   [binary bytes]                                                        │
//!   │        │                                                                │
//!   │        │  bincode::decode()                                             │
//!   │        ▼                                                                │
//!   │   DatabaseRequest::Query("SELECT * FROM users")                         │
//!   │        │                                                                │
//!   │        │  DBInstance::handle_network_query()                            │
//!   │        ▼                                                                │
//!   │   DatabaseResponse::Results(QueryResults { ... })                       │
//!   │        │                                                                │
//!   │        │  bincode::encode()                                             │
//!   │        ▼                                                                │
//!   │   [binary bytes] ─────────────────────────────────────────────────────► │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!                                    │ TCP
//!                                    │
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                          Client                                         │
//!   │                                                                         │
//!   │ ◄─────────────────────────────────────────────────────────────────────  │
//!   │   [binary bytes]                                                        │
//!   │        │                                                                │
//!   │        │  bincode::decode()                                             │
//!   │        ▼                                                                │
//!   │   DatabaseResponse::Results(QueryResults { ... })                       │
//!   │        │                                                                │
//!   │        │  return to caller                                              │
//!   │        ▼                                                                │
//!   │   QueryResults { column_names, rows, messages }                         │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Message Types
//!
//! ### DatabaseRequest
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  DatabaseRequest                                                        │
//!   │                                                                         │
//!   │  ├── Query(String)                                                      │
//!   │  │       Execute SQL query directly                                     │
//!   │  │                                                                      │
//!   │  ├── BeginTransaction { isolation_level: IsolationLevel }               │
//!   │  │       Start explicit transaction                                     │
//!   │  │                                                                      │
//!   │  ├── Commit                                                             │
//!   │  │       Commit current transaction                                     │
//!   │  │                                                                      │
//!   │  ├── Rollback                                                           │
//!   │  │       Abort current transaction                                      │
//!   │  │                                                                      │
//!   │  ├── Prepare(String)                                                    │
//!   │  │       Create prepared statement, returns stmt_id                     │
//!   │  │                                                                      │
//!   │  ├── Execute { stmt_id: u64, params: Vec<Value> }                       │
//!   │  │       Execute prepared statement with parameters                     │
//!   │  │                                                                      │
//!   │  └── Close(u64)                                                         │
//!   │          Deallocate prepared statement                                  │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### DatabaseResponse
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  DatabaseResponse                                                       │
//!   │                                                                         │
//!   │  ├── Results(QueryResults)                                              │
//!   │  │       Query execution results with column names and rows             │
//!   │  │                                                                      │
//!   │  ├── PrepareOk { stmt_id: u64, param_types: Vec<TypeId> }               │
//!   │  │       Prepared statement created successfully                        │
//!   │  │                                                                      │
//!   │  └── Error(String)                                                      │
//!   │          Error message for failed operations                            │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### QueryResults
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  QueryResults                                                           │
//!   │                                                                         │
//!   │  column_names: Vec<String>                                              │
//!   │      ["id", "name", "email"]                                            │
//!   │                                                                         │
//!   │  rows: Vec<Vec<Value>>                                                  │
//!   │      [                                                                  │
//!   │          [Int(1), String("Alice"), String("alice@example.com")],        │
//!   │          [Int(2), String("Bob"), String("bob@example.com")],            │
//!   │      ]                                                                  │
//!   │                                                                         │
//!   │  messages: Vec<String>                                                  │
//!   │      ["2 rows returned"]                                                │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Request/Response Mapping
//!
//! | Request                        | Success Response                | Error Response     |
//! |--------------------------------|---------------------------------|--------------------|
//! | `Query(sql)`                   | `Results(QueryResults)`         | `Error(message)`   |
//! | `BeginTransaction { iso }`     | `Results(empty)`                | `Error(message)`   |
//! | `Commit`                       | `Results(empty)`                | `Error(message)`   |
//! | `Rollback`                     | `Results(empty)`                | `Error(message)`   |
//! | `Prepare(sql)`                 | `PrepareOk { stmt_id, types }`  | `Error(message)`   |
//! | `Execute { stmt_id, params }`  | `Results(QueryResults)`         | `Error(message)`   |
//! | `Close(stmt_id)`               | `Results(empty)`                | `Error(message)`   |
//!
//! ## Serialization
//!
//! All types derive `bincode::Encode` and `bincode::Decode` for efficient binary
//! serialization:
//!
//! ```text
//!   Encoding characteristics:
//!   - Compact binary format (no field names)
//!   - Variable-length integers
//!   - Self-describing enums (discriminant + payload)
//!   - Vec length-prefixed
//!   - String UTF-8 with length prefix
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::server::protocol::{DatabaseRequest, DatabaseResponse, QueryResults};
//! use crate::concurrency::transaction::IsolationLevel;
//! use crate::types_db::value::Value;
//!
//! // Client sends query
//! let request = DatabaseRequest::Query("SELECT * FROM users".to_string());
//! let bytes = bincode::encode_to_vec(&request, bincode::config::standard())?;
//! // ... send bytes over TCP ...
//!
//! // Server receives and processes
//! let (request, _): (DatabaseRequest, _) =
//!     bincode::decode_from_slice(&bytes, bincode::config::standard())?;
//!
//! // Server sends response
//! let results = QueryResults::new(
//!     vec!["id".to_string(), "name".to_string()],
//!     vec![
//!         vec![Value::Integer(1), Value::String("Alice".to_string())],
//!         vec![Value::Integer(2), Value::String("Bob".to_string())],
//!     ],
//! );
//! let response = DatabaseResponse::Results(results);
//! let response_bytes = bincode::encode_to_vec(&response, bincode::config::standard())?;
//! // ... send response_bytes over TCP ...
//!
//! // Prepared statement workflow
//! let prepare_req = DatabaseRequest::Prepare("SELECT * FROM users WHERE id = ?".to_string());
//! // Server responds with: DatabaseResponse::PrepareOk { stmt_id: 1, param_types: [TypeId::Int] }
//!
//! let exec_req = DatabaseRequest::Execute {
//!     stmt_id: 1,
//!     params: vec![Value::Integer(42)],
//! };
//! // Server responds with: DatabaseResponse::Results(...)
//!
//! let close_req = DatabaseRequest::Close(1);
//! // Server responds with: DatabaseResponse::Results(QueryResults::empty())
//! ```
//!
//! ## Thread Safety
//!
//! - All types are `Send` and `Sync` (derived from their components)
//! - Types are typically owned and moved, not shared
//! - Safe to serialize/deserialize from any thread
//!
//! ## Implementation Notes
//!
//! - **Empty Results**: Use `QueryResults::empty()` for commands with no output
//! - **Messages**: Informational messages (row counts, warnings) go in `messages`
//! - **Type Safety**: `PrepareOk` returns expected parameter types for validation
//! - **Isolation Levels**: Passed as enum in `BeginTransaction` for explicit control

use crate::concurrency::transaction::IsolationLevel;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use bincode::{Decode, Encode};

/// Client request to the database server.
#[derive(Debug, Encode, Decode)]
pub enum DatabaseRequest {
    Query(String),
    BeginTransaction { isolation_level: IsolationLevel },
    Commit,
    Rollback,
    Prepare(String),
    Execute { stmt_id: u64, params: Vec<Value> },
    Close(u64),
}

#[derive(Debug, Encode, Decode)]
pub enum DatabaseResponse {
    Results(QueryResults),
    PrepareOk {
        stmt_id: u64,
        param_types: Vec<TypeId>,
    },
    Error(String),
}

#[derive(Debug, Encode, Decode)]
pub struct QueryResults {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub messages: Vec<String>,
}

impl QueryResults {
    pub fn empty() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
            messages: Vec::new(),
        }
    }

    pub fn new(column_names: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            column_names,
            rows,
            messages: Vec::new(),
        }
    }
}
