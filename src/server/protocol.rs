use crate::concurrency::transaction::IsolationLevel;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use bincode::{Encode, Decode};

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
