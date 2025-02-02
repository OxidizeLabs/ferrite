use crate::common::db_instance::DBInstance;
use crate::common::exception::DBError;
use crate::server::DatabaseResponse;
use log::{debug, error, info, warn};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    let request = serde_json::from_slice(data)?;

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
    let data = serde_json::to_vec(&response)?;

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
