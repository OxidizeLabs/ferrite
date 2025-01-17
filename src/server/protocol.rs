use crate::types_db::value::Value;
use crate::concurrency::transaction::IsolationLevel;
use serde::{Deserialize, Serialize};
use crate::types_db::type_id::TypeId;

#[derive(Debug, Serialize, Deserialize)]
pub enum DatabaseRequest {
    Query(String),
    BeginTransaction { isolation_level: IsolationLevel },
    Commit,
    Rollback,
    Prepare(String),
    Execute { stmt_id: u64, params: Vec<Value> },
    Close(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DatabaseResponse {
    Results(QueryResults),
    PrepareOk { stmt_id: u64, param_types: Vec<TypeId> },
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResults {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResults {
    pub fn empty() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn new(column_names: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            column_names,
            rows,
        }
    }
}
