//! # Database Client
//!
//! This module provides the client-side implementation for connecting to and communicating
//! with the Ferrite database server over TCP. It supports both direct query execution and
//! prepared statements for parameterized queries.
//!
//! ## Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        Client Application                               │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │                     DatabaseClient                              │   │
//!   │   │                                                                 │   │
//!   │   │   stream: TcpStream ─────────────────────────────────────────┐  │   │
//!   │   │                                                              │  │   │
//!   │   │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │  │   │
//!   │   │   │ execute_query() │  │ prepare_stmt()  │  │ execute()   │  │  │   │
//!   │   │   └────────┬────────┘  └────────┬────────┘  └──────┬──────┘  │  │   │
//!   │   │            │                    │                  │         │  │   │
//!   │   │            └────────────────────┴──────────────────┘         │  │   │
//!   │   │                                 │                            │  │   │
//!   │   │                    ┌────────────┴────────────┐               │  │   │
//!   │   │                    │    send_request()       │               │  │   │
//!   │   │                    │    receive_response()   │               │  │   │
//!   │   │                    └────────────┬────────────┘               │  │   │
//!   │   │                                 │                            │  │   │
//!   │   └─────────────────────────────────┼────────────────────────────┘   │
//!   │                                     │                                │
//!   └─────────────────────────────────────┼────────────────────────────────┘
//!                                         │
//!                                         │ TCP (bincode serialization)
//!                                         │
//!                                         ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        Database Server                                  │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Request Handler                                                │   │
//!   │   │                                                                 │   │
//!   │   │  DatabaseRequest ──► ExecutionEngine ──► DatabaseResponse       │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Request/Response Flow
//!
//! ```text
//!   execute_query("SELECT * FROM users")
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  1. Create DatabaseRequest::Query("SELECT * FROM users")              │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  2. Serialize with bincode → Vec<u8>                                  │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  3. Send over TcpStream (async write_all)                             │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  4. Read response bytes (async, buffered reads)                       │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  5. Deserialize DatabaseResponse with bincode                         │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  6. Return QueryResults or DBError                                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Prepared Statement Workflow
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │  Prepared Statement Lifecycle                                            │
//!   │                                                                          │
//!   │   1. PREPARE                                                             │
//!   │      ┌─────────────────────────────────────────────────────────────────┐ │
//!   │      │ prepare_statement("SELECT * FROM users WHERE id = ?")          │ │
//!   │      │                                                                 │ │
//!   │      │   → DatabaseRequest::Prepare(sql)                               │ │
//!   │      │   ← DatabaseResponse::PrepareOk { stmt_id: 1, param_types }     │ │
//!   │      │                                                                 │ │
//!   │      │ Returns: (stmt_id=1, param_types=[TypeId::Int])                 │ │
//!   │      └─────────────────────────────────────────────────────────────────┘ │
//!   │                                                                          │
//!   │   2. EXECUTE (can be called multiple times with different params)       │
//!   │      ┌─────────────────────────────────────────────────────────────────┐ │
//!   │      │ execute_statement(1, vec![Value::Integer(42)])                  │ │
//!   │      │                                                                 │ │
//!   │      │   → DatabaseRequest::Execute { stmt_id: 1, params: [...] }      │ │
//!   │      │   ← DatabaseResponse::Results(QueryResults)                     │ │
//!   │      └─────────────────────────────────────────────────────────────────┘ │
//!   │                                                                          │
//!   │   3. CLOSE (release server-side resources)                              │
//!   │      ┌─────────────────────────────────────────────────────────────────┐ │
//!   │      │ close_statement(1)                                              │ │
//!   │      │                                                                 │ │
//!   │      │   → DatabaseRequest::Close(1)                                   │ │
//!   │      │   ← DatabaseResponse::Results(_)                                │ │
//!   │      └─────────────────────────────────────────────────────────────────┘ │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component         | Type                        | Purpose                     |
//! |-------------------|-----------------------------|-----------------------------|
//! | `DatabaseClient`  | struct                      | Main client interface       |
//! | `ClientSession`   | struct                      | Session state (server-side) |
//! | `TcpStream`       | `tokio::net::TcpStream`     | Async TCP connection        |
//!
//! ## Client Operations
//!
//! | Method               | Request Type           | Response Type               |
//! |----------------------|------------------------|-----------------------------|
//! | `connect(addr)`      | -                      | `DatabaseClient`            |
//! | `execute_query(sql)` | `Query(String)`        | `Results(QueryResults)`     |
//! | `prepare_statement`  | `Prepare(String)`      | `PrepareOk { stmt_id, .. }` |
//! | `execute_statement`  | `Execute { id, params }`| `Results(QueryResults)`    |
//! | `close_statement`    | `Close(stmt_id)`       | `Results(_)`                |
//!
//! ## ClientSession Fields
//!
//! | Field                 | Type                           | Description              |
//! |-----------------------|--------------------------------|--------------------------|
//! | `id`                  | `u64`                          | Unique session ID        |
//! | `current_transaction` | `Option<Arc<TransactionContext>>`| Active transaction    |
//! | `isolation_level`     | `IsolationLevel`               | Session isolation level  |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::client::client_impl::DatabaseClient;
//! use crate::types_db::value::Value;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to server
//!     let mut client = DatabaseClient::connect("127.0.0.1:5432").await?;
//!
//!     // Direct query execution
//!     let results = client.execute_query("SELECT * FROM users").await?;
//!     for row in &results.rows {
//!         println!("{:?}", row);
//!     }
//!
//!     // Prepared statement (for parameterized queries)
//!     let (stmt_id, param_types) = client
//!         .prepare_statement("SELECT * FROM users WHERE id = ?")
//!         .await?;
//!
//!     // Execute with parameters (prevents SQL injection)
//!     let results = client
//!         .execute_statement(stmt_id, vec![Value::Integer(42)])
//!         .await?;
//!
//!     // Execute again with different parameters
//!     let results = client
//!         .execute_statement(stmt_id, vec![Value::Integer(100)])
//!         .await?;
//!
//!     // Clean up
//!     client.close_statement(stmt_id).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Error Handling
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Error Sources                                                          │
//!   │                                                                         │
//!   │   DBError::Io          Network/connection failures                      │
//!   │   DBError::Client      Server-reported errors (syntax, constraints)     │
//!   │   DBError::Internal    Serialization failures, protocol errors          │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Wire Protocol
//!
//! - **Serialization**: bincode (compact binary format)
//! - **Transport**: TCP with async I/O (tokio)
//! - **Buffering**: 4KB read buffer, incremental parsing
//! - **Message framing**: bincode self-describing length
//!
//! ## Thread Safety
//!
//! - `DatabaseClient` is **NOT thread-safe** (single `TcpStream`)
//! - Use separate client instances per thread/task
//! - `ClientSession` is used server-side to track per-connection state
//!
//! ## Implementation Notes
//!
//! - **Async/Await**: All I/O operations are async via tokio
//! - **Buffered Reads**: Response may arrive in multiple TCP packets
//! - **Error Recovery**: Connection errors require reconnection
//! - **Logging**: Debug-level logging for request/response tracing

use std::sync::Arc;

use log::{debug, error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::common::exception::DBError;
use crate::concurrency::transaction::IsolationLevel;
use crate::server::{DatabaseRequest, DatabaseResponse, QueryResults};
use crate::sql::execution::transaction_context::TransactionContext;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;

/// Represents a client's session with the database (server-side state).
///
/// Each connected client has an associated `ClientSession` that tracks
/// their transaction state and session-level settings. This struct is
/// maintained by the server to manage per-connection context.
#[derive(Debug)]
pub struct ClientSession {
    /// Unique identifier for this session.
    pub id: u64,
    /// The currently active transaction, if any.
    pub current_transaction: Option<Arc<TransactionContext>>,
    /// The isolation level for transactions in this session.
    pub isolation_level: IsolationLevel,
}

/// Client for connecting to and communicating with a Ferrite database server.
///
/// Provides methods for executing SQL queries, preparing statements, and
/// managing prepared statement lifecycle over a TCP connection.
///
/// # Thread Safety
/// This struct is **NOT thread-safe**. Each task/thread should use its own
/// `DatabaseClient` instance.
///
/// # Example
/// ```rust,ignore
/// let mut client = DatabaseClient::connect("127.0.0.1:5432").await?;
/// let results = client.execute_query("SELECT * FROM users").await?;
/// ```
pub struct DatabaseClient {
    /// The underlying TCP connection to the database server.
    stream: TcpStream,
}

impl DatabaseClient {
    /// Establishes a TCP connection to the database server.
    ///
    /// # Parameters
    /// - `addr`: The server address in `host:port` format (e.g., `"127.0.0.1:5432"`).
    ///
    /// # Returns
    /// A connected `DatabaseClient` on success, or a [`DBError::Io`] on connection failure.
    ///
    /// # Example
    /// ```rust,ignore
    /// let client = DatabaseClient::connect("localhost:5432").await?;
    /// ```
    pub async fn connect(addr: &str) -> Result<Self, DBError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| DBError::Io(e.to_string()))?;
        Ok(Self { stream })
    }

    /// Executes a SQL query and returns the results.
    ///
    /// This method sends the query directly to the server for execution.
    /// For parameterized queries, use [`prepare_statement`](Self::prepare_statement)
    /// followed by [`execute_statement`](Self::execute_statement) instead.
    ///
    /// # Parameters
    /// - `query`: The SQL query string to execute.
    ///
    /// # Returns
    /// [`QueryResults`] containing the result rows and metadata on success,
    /// or a [`DBError`] on failure (network, syntax, or execution error).
    ///
    /// # Example
    /// ```rust,ignore
    /// let results = client.execute_query("SELECT id, name FROM users").await?;
    /// for row in &results.rows {
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub async fn execute_query(&mut self, query: &str) -> Result<QueryResults, DBError> {
        debug!("[Query] Executing: {}", query);

        let request = DatabaseRequest::Query(query.to_string());
        self.send_request(&request).await?;

        match self.receive_response().await? {
            DatabaseResponse::Results(results) => {
                debug!(
                    "[Query] Completed successfully with {} rows",
                    results.rows.len()
                );
                Ok(results)
            },
            DatabaseResponse::Error(err) => {
                debug!("[Query] Failed: {}", err);
                Err(DBError::Client(err))
            },
            DatabaseResponse::PrepareOk { .. } => {
                debug!("[Query] Unexpected prepare response");
                Err(DBError::Internal("Unexpected prepare response".to_string()))
            },
        }
    }

    /// Prepares a SQL statement for later execution with parameters.
    ///
    /// Prepared statements allow the query to be parsed and planned once,
    /// then executed multiple times with different parameter values. This
    /// improves performance for repeated queries and prevents SQL injection.
    ///
    /// Use `?` as parameter placeholders in the SQL string.
    ///
    /// # Parameters
    /// - `sql`: The SQL query with `?` placeholders for parameters.
    ///
    /// # Returns
    /// A tuple of `(stmt_id, param_types)` where:
    /// - `stmt_id`: Handle for executing or closing the statement
    /// - `param_types`: Expected types for each parameter placeholder
    ///
    /// # Example
    /// ```rust,ignore
    /// let (stmt_id, types) = client
    ///     .prepare_statement("SELECT * FROM users WHERE id = ?")
    ///     .await?;
    /// // types[0] will indicate the expected type for the `id` parameter
    /// ```
    pub async fn prepare_statement(&mut self, sql: &str) -> Result<(u64, Vec<TypeId>), DBError> {
        let request = DatabaseRequest::Prepare(sql.to_string());
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::PrepareOk {
                stmt_id,
                param_types,
            } => Ok((stmt_id, param_types)),
            DatabaseResponse::Error(err) => Err(DBError::Client(err)),
            _ => Err(DBError::Internal("Unexpected response type".to_string())),
        }
    }

    /// Executes a previously prepared statement with the given parameters.
    ///
    /// The parameters are bound to the `?` placeholders in the order they
    /// appear in the prepared SQL statement.
    ///
    /// # Parameters
    /// - `stmt_id`: Statement handle returned by [`prepare_statement`](Self::prepare_statement).
    /// - `params`: Values to bind to the statement's parameter placeholders.
    ///
    /// # Returns
    /// [`QueryResults`] containing the result rows and metadata on success,
    /// or a [`DBError`] on failure.
    ///
    /// # Example
    /// ```rust,ignore
    /// // After preparing: "SELECT * FROM users WHERE id = ? AND active = ?"
    /// let results = client
    ///     .execute_statement(stmt_id, vec![Value::Integer(42), Value::Boolean(true)])
    ///     .await?;
    /// ```
    pub async fn execute_statement(
        &mut self,
        stmt_id: u64,
        params: Vec<Value>,
    ) -> Result<QueryResults, DBError> {
        let request = DatabaseRequest::Execute { stmt_id, params };
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::Results(results) => Ok(results),
            DatabaseResponse::Error(err) => Err(DBError::Client(err)),
            _ => Err(DBError::Internal("Unexpected response type".to_string())),
        }
    }

    /// Closes a prepared statement and releases server-side resources.
    ///
    /// After closing, the `stmt_id` is no longer valid and should not be
    /// used with [`execute_statement`](Self::execute_statement).
    ///
    /// # Parameters
    /// - `stmt_id`: Statement handle returned by [`prepare_statement`](Self::prepare_statement).
    ///
    /// # Returns
    /// `Ok(())` on success, or a [`DBError`] if the statement doesn't exist
    /// or a network error occurs.
    pub async fn close_statement(&mut self, stmt_id: u64) -> Result<(), DBError> {
        let request = DatabaseRequest::Close(stmt_id);
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::Results(_) => Ok(()),
            DatabaseResponse::Error(err) => Err(DBError::Client(err)),
            _ => Err(DBError::Internal("Unexpected response type".to_string())),
        }
    }

    /// Serializes and sends a request to the database server.
    ///
    /// Uses bincode for compact binary serialization over the TCP stream.
    async fn send_request(&mut self, request: &DatabaseRequest) -> Result<(), DBError> {
        debug!("[Network] Serializing request");
        let data = postcard::to_allocvec(request).map_err(|e| {
            error!("[Network] Failed to serialize request: {}", e);
            DBError::Internal(format!("Failed to serialize request: {}", e))
        })?;

        debug!("[Network] Sending {} bytes", data.len());
        self.stream.write_all(&data).await.map_err(|e| {
            error!("[Network] Send failed: {}", e);
            DBError::Io(e.to_string())
        })
    }

    /// Receives and deserializes a response from the database server.
    ///
    /// Handles buffered reads to assemble the complete response, as data
    /// may arrive in multiple TCP packets. Uses bincode for deserialization.
    async fn receive_response(&mut self) -> Result<DatabaseResponse, DBError> {
        let mut buffer = Vec::with_capacity(4096);
        let mut temp_buffer = [0u8; 4096];

        debug!("[Network] Reading response");

        loop {
            let n = self.stream.read(&mut temp_buffer).await.map_err(|e| {
                error!("[Network] Read failed: {}", e);
                DBError::Io(e.to_string())
            })?;

            if n == 0 {
                if buffer.is_empty() {
                    error!("[Network] Server disconnected");
                    return Err(DBError::Client("Server disconnected".to_string()));
                }
                break;
            }

            buffer.extend_from_slice(&temp_buffer[..n]);

            match postcard::from_bytes::<DatabaseResponse>(&buffer) {
                Ok(response) => {
                    debug!("[Network] Received {} bytes total", buffer.len());
                    return Ok(response);
                },
                Err(e) => {
                    // We assume we need more data if decoding fails
                    if buffer.len() < 4096 {
                        continue;
                    }
                    error!("[Network] Failed to parse response: {}", e);
                    return Err(DBError::Internal(format!(
                        "Failed to parse response: {}",
                        e
                    )));
                },
            }
        }

        error!("[Network] Failed to parse complete response");
        Err(DBError::Internal(
            "Failed to parse complete response".to_string(),
        ))
    }
}
