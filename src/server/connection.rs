//! # Server Connection Handler
//!
//! This module handles individual client connections for the Ferrite database server.
//! It manages the connection lifecycle, request parsing, query dispatch, response
//! serialization, and structured error logging.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                        Server Accept Loop                                │
//!   │                                                                          │
//!   │   TcpListener::accept() ──► spawn(handle_connection(stream, db))         │
//!   └──────────────────────────────────────────────────────────────────────────┘
//!                                        │
//!                                        ▼
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                      handle_connection()                                 │
//!   │                                                                          │
//!   │   1. Generate unique client_id (AtomicU64)                               │
//!   │   2. Create client session in DBInstance                                 │
//!   │   3. Enter request loop (handle_client_connection)                       │
//!   │   4. Remove session on disconnect                                        │
//!   └──────────────────────────────────────────────────────────────────────────┘
//!                                        │
//!                                        ▼
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                    handle_client_connection()                            │
//!   │                                                                          │
//!   │   loop {                                                                 │
//!   │       bytes = stream.read()                                              │
//!   │           │                                                              │
//!   │           ├── 0 bytes ──► Client disconnected, break                     │
//!   │           │                                                              │
//!   │           └── n bytes ──► handle_client_request()                        │
//!   │                               │                                          │
//!   │                               ├── Ok(response) ──► send_response()       │
//!   │                               │                                          │
//!   │                               └── Err(e) ──► format_client_error()       │
//!   │                                              send_response(Error)        │
//!   │   }                                                                      │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Request Processing Flow
//!
//! ```text
//!   Raw bytes from TcpStream
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  bincode::decode_from_slice() → DatabaseRequest                        │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  db.handle_network_query(request, client_id)                           │
//!   │                                                                        │
//!   │  Request types:                                                        │
//!   │    Query(sql)           → Execute SQL                                  │
//!   │    BeginTransaction     → Start transaction                            │
//!   │    Commit               → Commit transaction                           │
//!   │    Rollback             → Abort transaction                            │
//!   │    Prepare(sql)         → Create prepared statement                    │
//!   │    Execute(id, params)  → Run prepared statement                       │
//!   │    Close(id)            → Deallocate statement                         │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  bincode::encode_to_vec(response) → bytes                              │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  stream.write() + stream.flush() → Client                              │
//!   │                                                                        │
//!   │  Chunked writes for large responses                                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Error Handling
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                      Error Processing Pipeline                           │
//!   │                                                                          │
//!   │   Error occurs                                                           │
//!   │       │                                                                  │
//!   │       ▼                                                                  │
//!   │   log_error(client_id, context, error, severity)                         │
//!   │       │                                                                  │
//!   │       ├── Log main message with [Client X] [Category] format             │
//!   │       │                                                                  │
//!   │       ├── If DBError: log detailed category-specific info                │
//!   │       │   ├── SqlError, PlanError, Internal, Catalog                     │
//!   │       │   ├── Execution, Client, Io, LockError                           │
//!   │       │   ├── Transaction, NotImplemented, Validation                    │
//!   │       │   └── TableNotFound, OptimizeError, Recovery                     │
//!   │       │                                                                  │
//!   │       └── Walk error chain (source()) and log causes                     │
//!   │                                                                          │
//!   │   format_client_error(error) → User-friendly message                     │
//!   │       │                                                                  │
//!   │       └── DatabaseResponse::Error(message) → Client                      │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component             | Purpose                                          |
//! |-----------------------|--------------------------------------------------|
//! | `NEXT_CLIENT_ID`      | Atomic counter for unique client IDs             |
//! | `handle_connection()` | Entry point for new connections                  |
//! | `handle_client_connection()`| Main request/response loop               |
//! | `handle_client_request()`| Parse and dispatch single request            |
//! | `send_response()`     | Serialize and transmit response                  |
//! | `format_log()`        | Consistent log message formatting                |
//! | `log_error()`         | Structured error logging with context            |
//! | `format_client_error()`| Convert errors to user-friendly messages        |
//!
//! ## Log Format
//!
//! ```text
//!   [Client {id}] [{category}] {message}
//!
//!   Categories:
//!     Connection  - Connect/disconnect events
//!     Query       - Query processing status
//!     Request     - Request parsing/handling
//!     Response    - Response sending
//!     Details     - Error details (DBError specifics)
//!     Cause N     - Error chain entries
//!
//!   Examples:
//!     [Client 1] [Connection] New connection established
//!     [Client 1] [Query] Processing successful
//!     [Client 1] [Details] SQL Error: syntax error near 'SELCT'
//!     [Client 1] [Connection] Connection closed
//! ```
//!
//! ## DBError Categories
//!
//! | Error Type       | Description                      | Logged As           |
//! |------------------|----------------------------------|---------------------|
//! | `SqlError`       | SQL syntax/parsing errors        | SQL Error           |
//! | `PlanError`      | Query planning failures          | Plan Error          |
//! | `Internal`       | Internal system errors           | Internal Error      |
//! | `Catalog`        | Metadata/catalog issues          | Catalog Error       |
//! | `Execution`      | Query execution failures         | Execution Error     |
//! | `Client`         | Client-side errors               | Database Error      |
//! | `Io`             | I/O operations                   | IO Error            |
//! | `LockError`      | Lock acquisition failures        | Lock Error          |
//! | `Transaction`    | Transaction management           | Transaction Error   |
//! | `NotImplemented` | Unimplemented features           | Operation not impl. |
//! | `Validation`     | Data validation failures         | Validation Error    |
//! | `TableNotFound`  | Missing table                    | Table not found     |
//! | `OptimizeError`  | Optimizer failures               | Optimization Error  |
//! | `Recovery`       | Recovery process errors          | Recovery Error      |
//!
//! ## Example Connection Lifecycle
//!
//! ```text
//!   Client connects (TCP)
//!       │
//!       ▼
//!   [Client 1] [Connection] New connection established
//!       │
//!       ▼
//!   db.create_client_session(1)
//!       │
//!       ▼
//!   ┌─────────────────────────────────────────────────┐
//!   │  Request Loop                                   │
//!   │                                                 │
//!   │  [Client 1] [Request] Parsing request           │
//!   │  [Client 1] [Request] Handling Query(...)       │
//!   │  [Client 1] [Response] Query handled success    │
//!   │  [Client 1] [Query] Processing successful       │
//!   │                                                 │
//!   │  ... (more requests) ...                        │
//!   │                                                 │
//!   └─────────────────────────────────────────────────┘
//!       │
//!       ▼
//!   [Client 1] [Connection] Client disconnected
//!       │
//!       ▼
//!   db.remove_client_session(1)
//!       │
//!       ▼
//!   [Client 1] [Connection] Connection closed
//! ```
//!
//! ## Thread Safety
//!
//! - `NEXT_CLIENT_ID`: `AtomicU64` for lock-free ID generation
//! - Each connection runs in its own tokio task
//! - `DBInstance` is `Arc`-wrapped and safe to share
//! - No shared mutable state between connection handlers
//!
//! ## Implementation Notes
//!
//! - **Read Buffer**: 1024 bytes per read (may require multiple reads for large requests)
//! - **Write Chunking**: Large responses written in chunks until complete
//! - **Flush Guarantee**: `stream.flush()` ensures data is sent before returning
//! - **Error Recovery**: Errors are logged and returned to client; connection continues
//! - **Clean Disconnect**: Session cleanup runs even on error paths

use crate::common::db_instance::DBInstance;
use crate::common::exception::DBError;
use crate::server::DatabaseResponse;
use log::{debug, error, info, warn};
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Atomic counter for generating unique client IDs.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

// Helper function to format log messages with consistent structure
fn format_log(client_id: u64, category: &str, message: &str) -> String {
    format!("[Client {}] [{}] {}", client_id, category, message)
}

// Helper function to log server errors with context and details
fn log_error(client_id: u64, context: &str, error: &(dyn StdError + 'static), severity: &str) {
    // Log main error with category
    let base_msg = format_log(client_id, context, &error.to_string());

    match severity {
        "ERROR" => error!("{}", base_msg),
        "WARN" => warn!("{}", base_msg),
        "DEBUG" => debug!("{}", base_msg),
        _ => info!("{}", base_msg),
    }

    // Log detailed error information for DBError types
    if let Some(db_error) = error.downcast_ref::<DBError>() {
        let details = match db_error {
            DBError::SqlError(msg) => format!("SQL Error: {}", msg),
            DBError::PlanError(msg) => format!("Plan Error: {}", msg),
            DBError::Internal(msg) => format!("Internal Error: {}", msg),
            DBError::Catalog(msg) => format!("Catalog Error: {}", msg),
            DBError::Execution(msg) => format!("Execution Error: {}", msg),
            DBError::Client(msg) => format!("Client Error: {}", msg),
            DBError::Io(msg) => format!("IO Error: {}", msg),
            DBError::LockError(msg) => format!("LockError Error: {}", msg),
            DBError::Transaction(msg) => format!("Transaction Error: {}", msg),
            DBError::NotImplemented(msg) => format!("NotImplemented Error: {}", msg),
            DBError::Validation(msg) => format!("Validation Error: {}", msg),
            DBError::TableNotFound(msg) => format!("TableNotFound Error: {}", msg),
            DBError::OptimizeError(msg) => format!("OptimizeError Error: {}", msg),
            DBError::Recovery(msg) => format!("RecoveryError Error: {}", msg),
        };
        debug!("{}", format_log(client_id, "Details", &details));
    }

    // Log error chain
    let mut source = error.source();
    let mut depth = 0;
    while let Some(err) = source {
        debug!(
            "{}",
            format_log(client_id, &format!("Cause {}", depth), &err.to_string())
        );
        source = err.source();
        depth += 1;
    }
}

pub async fn handle_connection(mut stream: TcpStream, db: Arc<DBInstance>) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst);

    db.create_client_session(client_id);
    info!(
        "{}",
        format_log(client_id, "Connection", "New connection established")
    );

    let result = handle_client_connection(&mut stream, &db, client_id).await;
    if let Err(e) = &result {
        log_error(client_id, "Error", e.as_ref(), "ERROR");
    }

    db.remove_client_session(client_id);
    info!(
        "{}",
        format_log(client_id, "Connection", "Connection closed")
    );
}

async fn handle_client_connection(
    stream: &mut TcpStream,
    db: &DBInstance,
    client_id: u64,
) -> Result<(), Box<dyn StdError>> {
    loop {
        let mut buffer = [0; 1024];
        match stream.read(&mut buffer).await {
            Ok(0) => {
                info!(
                    "{}",
                    format_log(client_id, "Connection", "Client disconnected")
                );
                break;
            }
            Ok(n) => match handle_client_request(&buffer[..n], db, client_id).await {
                Ok(response) => {
                    debug!(
                        "{}",
                        format_log(client_id, "Query", "Processing successful")
                    );
                    if let Err(e) = send_response(stream, response).await {
                        log_error(client_id, "Response", &*e, "ERROR");
                        return Err(e);
                    }
                }
                Err(e) => {
                    log_error(client_id, "Query", &*e, "ERROR");
                    let error_msg = format_client_error(&*e);
                    let error_response = DatabaseResponse::Error(error_msg);

                    if let Err(e) = send_response(stream, error_response).await {
                        log_error(client_id, "Response", &*e, "ERROR");
                        return Err(e);
                    }
                }
            },
            Err(e) => {
                log_error(client_id, "Connection", &e, "ERROR");
                return Err(Box::new(e));
            }
        }
    }

    Ok(())
}

fn format_client_error(error: &(dyn StdError + 'static)) -> String {
    if let Some(db_error) = error.downcast_ref::<DBError>() {
        match db_error {
            DBError::Io(e) => format!("IO Error: {}", e),
            DBError::LockError(msg) => format!("Lock Error: {}", msg),
            DBError::Transaction(msg) => format!("Transaction Error: {}", msg),
            DBError::NotImplemented(msg) => format!("Operation not implemented: {}", msg),
            DBError::Catalog(msg) => format!("Catalog Error: {}", msg),
            DBError::Execution(msg) => format!("Execution Error: {}", msg),
            DBError::Validation(msg) => format!("Validation Error: {}", msg),
            DBError::TableNotFound(msg) => format!("Table not found: {}", msg),
            DBError::PlanError(msg) => format!("Planning Error: {}", msg),
            DBError::Internal(msg) => format!("Internal Error: {}", msg),
            DBError::OptimizeError(msg) => format!("Optimization Error: {}", msg),
            DBError::SqlError(msg) => format!("SQL Error: {}", msg),
            DBError::Client(msg) => format!("Database Error: {}", msg),
            DBError::Recovery(msg) => format!("Recovery Error: {}", msg),
        }
    } else {
        error.to_string()
    }
}

async fn handle_client_request(
    data: &[u8],
    db: &DBInstance,
    client_id: u64,
) -> Result<DatabaseResponse, Box<dyn StdError>> {
    debug!("{}", format_log(client_id, "Request", "Parsing request"));
    let (request, _) = bincode::decode_from_slice(data, bincode::config::standard())?;

    debug!(
        "{}",
        format_log(client_id, "Request", &format!("Handling {:?}", request))
    );
    match db.handle_network_query(request, client_id).await {
        Ok(response) => {
            debug!(
                "{}",
                format_log(client_id, "Response", "Query handled successfully")
            );
            Ok(response)
        }
        Err(e) => {
            log_error(client_id, "Query", &e, "ERROR");
            Ok(DatabaseResponse::Error(format_client_error(&e)))
        }
    }
}

async fn send_response(
    stream: &mut TcpStream,
    response: DatabaseResponse,
) -> Result<(), Box<dyn StdError>> {
    let data = bincode::encode_to_vec(&response, bincode::config::standard())?;

    // Send data in chunks if needed
    let mut offset = 0;
    while offset < data.len() {
        let bytes_written = stream.write(&data[offset..]).await?;
        if bytes_written == 0 {
            return Err(Box::new(DBError::Io(
                "Failed to write response".to_string(),
            )));
        }
        offset += bytes_written;
    }

    stream.flush().await?;
    Ok(())
}
