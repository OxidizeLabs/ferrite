use crate::server::{DatabaseRequest, DatabaseResponse, QueryResults};
use crate::types_db::value::Value;
use crate::types_db::type_id::TypeId;
use crate::common::exception::DBError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use log::{debug, error, info};

pub struct DatabaseClient {
    stream: TcpStream,
}

impl DatabaseClient {
    pub async fn connect(addr: &str) -> Result<Self, DBError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| DBError::Io(e.to_string()))?;
        Ok(Self { stream })
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<QueryResults, DBError> {
        debug!("[Query] Executing: {}", query);
        
        let request = DatabaseRequest::Query(query.to_string());
        self.send_request(&request).await?;
        
        match self.receive_response().await? {
            DatabaseResponse::Results(results) => {
                debug!("[Query] Completed successfully with {} rows", results.rows.len());
                Ok(results)
            }
            DatabaseResponse::Error(err) => {
                debug!("[Query] Failed: {}", err);
                Err(DBError::Client(err))
            }
            DatabaseResponse::PrepareOk { .. } => {
                debug!("[Query] Unexpected prepare response");
                Err(DBError::Internal("Unexpected prepare response".to_string()))
            }
        }
    }

    pub async fn prepare_statement(&mut self, sql: &str) -> Result<(u64, Vec<TypeId>), DBError> {
        let request = DatabaseRequest::Prepare(sql.to_string());
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::PrepareOk { stmt_id, param_types } => Ok((stmt_id, param_types)),
            DatabaseResponse::Error(err) => Err(DBError::Client(err)),
            _ => Err(DBError::Internal("Unexpected response type".to_string())),
        }
    }

    pub async fn execute_statement(&mut self, stmt_id: u64, params: Vec<Value>) -> Result<QueryResults, DBError> {
        let request = DatabaseRequest::Execute { stmt_id, params };
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::Results(results) => Ok(results),
            DatabaseResponse::Error(err) => Err(DBError::Client(err)),
            _ => Err(DBError::Internal("Unexpected response type".to_string())),
        }
    }

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

    async fn send_request(&mut self, request: &DatabaseRequest) -> Result<(), DBError> {
        debug!("[Network] Serializing request");
        let data = serde_json::to_vec(request)
            .map_err(|e| {
                error!("[Network] Failed to serialize request: {}", e);
                DBError::Internal(format!("Failed to serialize request: {}", e))
            })?;
            
        debug!("[Network] Sending {} bytes", data.len());
        self.stream
            .write_all(&data)
            .await
            .map_err(|e| {
                error!("[Network] Send failed: {}", e);
                DBError::Io(e.to_string())
            })
    }

    async fn receive_response(&mut self) -> Result<DatabaseResponse, DBError> {
        let mut buffer = Vec::with_capacity(4096);
        let mut temp_buffer = [0u8; 4096];
        
        debug!("[Network] Reading response");
        
        loop {
            let n = self.stream
                .read(&mut temp_buffer)
                .await
                .map_err(|e| {
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
            
            match serde_json::from_slice(&buffer) {
                Ok(response) => {
                    debug!("[Network] Received {} bytes total", buffer.len());
                    return Ok(response);
                }
                Err(ref e) if e.is_eof() => {
                    continue;
                }
                Err(e) => {
                    error!("[Network] Failed to parse response: {}", e);
                    return Err(DBError::Internal(format!("Failed to parse response: {}", e)));
                }
            }
        }

        error!("[Network] Failed to parse complete response");
        Err(DBError::Internal("Failed to parse complete response".to_string()))
    }
}
