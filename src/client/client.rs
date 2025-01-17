use crate::server::{DatabaseRequest, DatabaseResponse, QueryResults};
use crate::types_db::value::Value;
use crate::types_db::type_id::TypeId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct DatabaseClient {
    stream: TcpStream,
}

impl DatabaseClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<QueryResults, Box<dyn std::error::Error>> {
        let request = DatabaseRequest::Query(query.to_string());
        self.send_request(&request).await?;
        let response = self.receive_response().await?;
        
        match response {
            DatabaseResponse::Results(results) => Ok(results),
            DatabaseResponse::Error(err) => Err(err.into()),
            DatabaseResponse::PrepareOk { .. } => Err("Unexpected prepare response".into()),
        }
    }

    pub async fn prepare_statement(&mut self, sql: &str) -> Result<(u64, Vec<TypeId>), Box<dyn std::error::Error>> {
        let request = DatabaseRequest::Prepare(sql.to_string());
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::PrepareOk { stmt_id, param_types } => Ok((stmt_id, param_types)),
            DatabaseResponse::Error(err) => Err(err.into()),
            _ => Err("Unexpected response type".into()),
        }
    }

    pub async fn execute_statement(&mut self, stmt_id: u64, params: Vec<Value>) -> Result<QueryResults, Box<dyn std::error::Error>> {
        let request = DatabaseRequest::Execute { stmt_id, params };
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::Results(results) => Ok(results),
            DatabaseResponse::Error(err) => Err(err.into()),
            _ => Err("Unexpected response type".into()),
        }
    }

    pub async fn close_statement(&mut self, stmt_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let request = DatabaseRequest::Close(stmt_id);
        self.send_request(&request).await?;
        let response = self.receive_response().await?;

        match response {
            DatabaseResponse::Results(_) => Ok(()),
            DatabaseResponse::Error(err) => Err(err.into()),
            _ => Err("Unexpected response type".into()),
        }
    }

    async fn send_request(&mut self, request: &DatabaseRequest) -> Result<(), Box<dyn std::error::Error>> {
        let data = serde_json::to_vec(request)?;
        self.stream.write_all(&data).await?;
        Ok(())
    }

    async fn receive_response(&mut self) -> Result<DatabaseResponse, Box<dyn std::error::Error>> {
        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        let response = serde_json::from_slice(&buffer[..n])?;
        Ok(response)
    }
}
