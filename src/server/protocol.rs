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

use bincode::{Decode, Encode};

use crate::concurrency::transaction::IsolationLevel;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;

/// Client request to the database server.
///
/// Represents all possible operations a client can request. Each variant is
/// serialized via bincode and sent over TCP to the server.
///
/// See the module-level documentation for the request/response mapping table.
#[derive(Debug, Encode, Decode)]
pub enum DatabaseRequest {
    /// Execute a SQL query directly.
    ///
    /// The SQL string is parsed, planned, and executed by the server.
    /// Returns `DatabaseResponse::Results` on success.
    Query(String),

    /// Start an explicit transaction with the specified isolation level.
    ///
    /// Returns an empty `Results` on success. Subsequent queries run within
    /// this transaction until `Commit` or `Rollback` is sent.
    BeginTransaction {
        /// The isolation level for the new transaction.
        isolation_level: IsolationLevel,
    },

    /// Commit the current transaction.
    ///
    /// Makes all changes durable. Returns an empty `Results` on success.
    /// Fails if no transaction is active.
    Commit,

    /// Abort the current transaction.
    ///
    /// Rolls back all changes. Returns an empty `Results` on success.
    /// Fails if no transaction is active.
    Rollback,

    /// Create a prepared statement.
    ///
    /// The SQL string is parsed and planned but not executed. Returns
    /// `DatabaseResponse::PrepareOk` with a statement ID and parameter types.
    Prepare(String),

    /// Execute a prepared statement with the given parameters.
    ///
    /// The statement must have been previously created with `Prepare`.
    /// Returns `DatabaseResponse::Results` on success.
    Execute {
        /// The statement ID returned by a previous `Prepare` request.
        stmt_id: u64,
        /// Parameter values to bind to the statement's placeholders.
        params: Vec<Value>,
    },

    /// Deallocate a prepared statement.
    ///
    /// Frees server-side resources for the statement. Returns an empty
    /// `Results` on success.
    Close(u64),
}

/// Server response to a client request.
///
/// Represents all possible responses the server can send back to the client.
/// Each variant is serialized via bincode and sent over TCP.
#[derive(Debug, Encode, Decode)]
pub enum DatabaseResponse {
    /// Successful query execution results.
    ///
    /// Contains column names, rows of values, and optional messages.
    /// Used for `Query`, `Execute`, and transaction control responses.
    Results(QueryResults),

    /// Prepared statement created successfully.
    ///
    /// Returned in response to a `Prepare` request.
    PrepareOk {
        /// Unique identifier for the prepared statement.
        ///
        /// Use this ID in subsequent `Execute` and `Close` requests.
        stmt_id: u64,
        /// Expected types for the statement's parameter placeholders.
        ///
        /// The client should provide values of these types in the
        /// `Execute` request's `params` vector.
        param_types: Vec<TypeId>,
    },

    /// Error response for failed operations.
    ///
    /// Contains a user-friendly error message describing what went wrong.
    Error(String),
}

/// Results from a successful query execution.
///
/// Contains the column schema, data rows, and optional informational messages.
/// This is the primary payload for successful query responses.
///
/// # Example
///
/// ```text
/// QueryResults {
///     column_names: ["id", "name"],
///     rows: [
///         [Int(1), String("Alice")],
///         [Int(2), String("Bob")],
///     ],
///     messages: ["2 rows returned"],
/// }
/// ```
#[derive(Debug, Encode, Decode)]
pub struct QueryResults {
    /// Names of the columns in the result set.
    ///
    /// The order matches the order of values in each row.
    pub column_names: Vec<String>,

    /// Data rows, where each row is a vector of values.
    ///
    /// Each row has the same length as `column_names`, with values
    /// corresponding to their respective columns.
    pub rows: Vec<Vec<Value>>,

    /// Informational messages (e.g., row counts, warnings).
    ///
    /// These are supplementary messages, not errors. Errors are returned
    /// via `DatabaseResponse::Error` instead.
    pub messages: Vec<String>,
}

impl QueryResults {
    /// Creates an empty result set.
    ///
    /// Used for commands that succeed but produce no output, such as
    /// `BEGIN`, `COMMIT`, `ROLLBACK`, or `CLOSE`.
    ///
    /// # Returns
    /// A `QueryResults` with empty column names, rows, and messages.
    pub fn empty() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
            messages: Vec::new(),
        }
    }

    /// Creates a result set with the given columns and rows.
    ///
    /// # Parameters
    /// - `column_names`: The names of the columns in the result set.
    /// - `rows`: The data rows, where each row is a vector of values.
    ///
    /// # Returns
    /// A `QueryResults` with the specified data and empty messages.
    ///
    /// # Note
    /// Messages can be added after construction by pushing to the
    /// `messages` field directly.
    pub fn new(column_names: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            column_names,
            rows,
            messages: Vec::new(),
        }
    }
}
