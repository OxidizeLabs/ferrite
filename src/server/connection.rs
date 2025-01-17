use crate::common::db_instance::DBInstance;
use crate::server::protocol::{DatabaseRequest, DatabaseResponse};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn handle_connection(mut stream: TcpStream, db: Arc<DBInstance>) {
    let mut buffer = [0; 1024];

    loop {
        match stream.read(&mut buffer).await {
            Ok(n) if n == 0 => {
                println!("Client disconnected");
                return;
            }
            Ok(n) => {
                // Deserialize request
                if let Ok(request) = serde_json::from_slice::<DatabaseRequest>(&buffer[..n]) {
                    // Process request
                    let response = db.handle_network_query(request, 0).await
                        .unwrap_or_else(|e| DatabaseResponse::Error(e.to_string()));

                    // Serialize and send response
                    if let Ok(response_data) = serde_json::to_vec(&response) {
                        if let Err(e) = stream.write_all(&response_data).await {
                            eprintln!("Error sending response: {}", e);
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                return;
            }
        }
    }
}
