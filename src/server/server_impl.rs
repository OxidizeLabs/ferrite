//! # Database Server
//!
//! This module provides `ServerHandle`, the TCP server implementation for Ferrite.
//! It manages the server lifecycle including startup, connection acceptance, and
//! graceful shutdown.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                          ServerHandle                                    │
//!   │                                                                          │
//!   │   port: u16                    Server listen port                        │
//!   │   runtime: Option<Runtime>     Tokio async runtime                       │
//!   │   join_handle: Option<JoinHandle<()>>   Server thread handle             │
//!   │                                                                          │
//!   │   ┌──────────────────────────────────────────────────────────────────┐   │
//!   │   │  Tokio Runtime                                                   │   │
//!   │   │                                                                  │   │
//!   │   │   ┌────────────────────────────────────────────────────────────┐ │   │
//!   │   │   │  Server Thread (std::thread)                               │ │   │
//!   │   │   │                                                            │ │   │
//!   │   │   │   runtime.block_on(async {                                 │ │   │
//!   │   │   │       TcpListener::bind("127.0.0.1:{port}")                │ │   │
//!   │   │   │       loop {                                               │ │   │
//!   │   │   │           listener.accept() → (stream, addr)               │ │   │
//!   │   │   │           spawn_blocking(handle_connection(stream, db))    │ │   │
//!   │   │   │       }                                                    │ │   │
//!   │   │   │   })                                                       │ │   │
//!   │   │   └────────────────────────────────────────────────────────────┘ │   │
//!   │   │                                                                  │   │
//!   │   │   ┌────────────────────────────────────────────────────────────┐ │   │
//!   │   │   │  Connection Task (tokio::task::spawn_blocking)             │ │   │
//!   │   │   │                                                            │ │   │
//!   │   │   │   handle_connection(stream, db)                            │ │   │
//!   │   │   │       - Read requests                                      │ │   │
//!   │   │   │       - Process queries                                    │ │   │
//!   │   │   │       - Send responses                                     │ │   │
//!   │   │   └────────────────────────────────────────────────────────────┘ │   │
//!   │   │                                                                  │   │
//!   │   │   ┌────────────────────────────────────────────────────────────┐ │   │
//!   │   │   │  Connection Task (another client)                          │ │   │
//!   │   │   └────────────────────────────────────────────────────────────┘ │   │
//!   │   │                                                                  │   │
//!   │   └──────────────────────────────────────────────────────────────────┘   │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Server Lifecycle
//!
//! ```text
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  1. CREATE                                                             │
//!   │                                                                        │
//!   │     ServerHandle::new(port)                                            │
//!   │         │                                                              │
//!   │         ├── Create Tokio Runtime                                       │
//!   │         ├── Store port                                                 │
//!   │         └── join_handle = None (not started yet)                       │
//!   └────────────────────────────────────────────────────────────────────────┘
//!                          │
//!                          ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  2. START                                                              │
//!   │                                                                        │
//!   │     server.start(db)                                                   │
//!   │         │                                                              │
//!   │         ├── Spawn server thread                                        │
//!   │         │       │                                                      │
//!   │         │       └── block_on(async {                                   │
//!   │         │              TcpListener::bind(addr)                         │
//!   │         │              loop { accept connections }                     │
//!   │         │          })                                                  │
//!   │         │                                                              │
//!   │         └── Store JoinHandle                                           │
//!   │                                                                        │
//!   │     Output: "Server listening on 127.0.0.1:{port}"                     │
//!   └────────────────────────────────────────────────────────────────────────┘
//!                          │
//!                          │  (server runs, accepting connections...)
//!                          ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  3. SHUTDOWN                                                           │
//!   │                                                                        │
//!   │     server.shutdown()                                                  │
//!   │         │                                                              │
//!   │         ├── runtime.shutdown_timeout(10s)                              │
//!   │         │       └── Waits for pending tasks to complete                │
//!   │         │                                                              │
//!   │         └── join_handle.join()                                         │
//!   │                 └── Waits for server thread to exit                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Connection Handling
//!
//! ```text
//!   Client connects (TCP)
//!        │
//!        ▼
//!   listener.accept() → Ok((stream, addr))
//!        │
//!        ├── Print: "New connection from: {addr}"
//!        │
//!        └── tokio::task::spawn_blocking(move || {
//!                runtime_handle.block_on(async {
//!                    handle_connection(stream, db).await
//!                })
//!            })
//!
//!   Why spawn_blocking?
//!   ─────────────────────────────────────────────────────────────────────────
//!   - Allows blocking I/O in connection handler
//!   - Each connection gets its own blocking thread
//!   - Prevents blocking the main accept loop
//!   - inner block_on enables async operations within the blocking context
//! ```
//!
//! ## Key Components
//!
//! | Field          | Type                      | Purpose                       |
//! |----------------|---------------------------|-------------------------------|
//! | `port`         | `u16`                     | TCP listen port               |
//! | `runtime`      | `Option<Runtime>`         | Tokio runtime (owned)         |
//! | `join_handle`  | `Option<JoinHandle<()>>`  | Server thread handle          |
//!
//! ## API
//!
//! | Method        | Description                                      |
//! |---------------|--------------------------------------------------|
//! | `new(port)`   | Create server handle with specified port         |
//! | `start(db)`   | Start accepting connections                      |
//! | `shutdown()`  | Gracefully stop server (10s timeout)             |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::server::server_impl::ServerHandle;
//! use crate::common::db_instance::{DBInstance, DBConfig};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create database instance
//!     let config = DBConfig::default();
//!     let db = Arc::new(DBInstance::new(config).await?);
//!
//!     // Create and start server
//!     let mut server = ServerHandle::new(5432);
//!     server.start(db)?;
//!
//!     println!("Server running on port 5432");
//!     println!("Press Ctrl+C to stop...");
//!
//!     // Wait for shutdown signal (e.g., Ctrl+C)
//!     tokio::signal::ctrl_c().await?;
//!
//!     // Graceful shutdown
//!     server.shutdown()?;
//!     println!("Server stopped");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Threading Model
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Main Thread                                                            │
//!   │      │                                                                  │
//!   │      ├── Creates ServerHandle                                           │
//!   │      ├── Calls start() → spawns Server Thread                           │
//!   │      ├── Returns to application                                         │
//!   │      └── Eventually calls shutdown()                                    │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Server Thread (std::thread::spawn)                                     │
//!   │      │                                                                  │
//!   │      └── runtime.block_on(accept loop)                                  │
//!   │              │                                                          │
//!   │              └── For each connection: spawn_blocking                    │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Blocking Thread Pool (tokio::task::spawn_blocking)                     │
//!   │      │                                                                  │
//!   │      ├── Connection Handler 1                                           │
//!   │      ├── Connection Handler 2                                           │
//!   │      └── Connection Handler N                                           │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `DBInstance` is `Arc`-wrapped and cloned for each connection
//! - `Runtime::Handle` is `Arc`-wrapped for sharing with connection tasks
//! - Each connection runs in its own blocking thread
//! - No shared mutable state between connections
//!
//! ## Implementation Notes
//!
//! - **Blocking Accept Loop**: Uses `block_on` in dedicated thread to avoid blocking caller
//! - **spawn_blocking for Connections**: Allows synchronous buffer pool operations
//! - **Shutdown Timeout**: 10 seconds to allow pending requests to complete
//! - **Error Handling**: Connection errors logged to stderr, don't crash server
//! - **Address**: Binds to `127.0.0.1` (localhost only) for security

use crate::common::db_instance::DBInstance;
use crate::server::connection::handle_connection;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

/// Handle for managing the database server lifecycle.
///
/// See module-level documentation for details.
pub struct ServerHandle {
    port: u16,
    runtime: Option<Runtime>,
    join_handle: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            runtime: Some(Runtime::new().unwrap()),
            join_handle: None,
        }
    }

    pub fn start(&mut self, db: Arc<DBInstance>) -> Result<(), Box<dyn std::error::Error>> {
        let port = self.port;
        let runtime = self.runtime.as_ref().unwrap();
        let runtime_handle = Arc::new(runtime.handle().clone());

        let handle = std::thread::spawn({
            let runtime_handle = runtime_handle.clone();
            move || {
                runtime_handle.block_on(async {
                    let addr = format!("127.0.0.1:{}", port);
                    let listener = TcpListener::bind(&addr).await.unwrap();
                    println!("Server listening on {}", addr);

                    loop {
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                println!("New connection from: {}", addr);
                                let db = Arc::clone(&db);
                                let handle = Arc::clone(&runtime_handle);

                                tokio::task::spawn_blocking(move || {
                                    handle.block_on(async {
                                        handle_connection(stream, db).await;
                                    });
                                });
                            }
                            Err(e) => eprintln!("Error accepting connection: {}", e),
                        }
                    }
                });
            }
        });

        self.join_handle = Some(handle);
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(10));
        }

        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }

        Ok(())
    }
}
