use serde::{Deserialize, Serialize};

// Every possible SQL type ID
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeId {
    Invalid = 0,
    Boolean,
    Integer,
    Decimal,
    VarChar,
    Timestamp,
    Vector,
    TinyInt,
    SmallInt,
    BigInt,
}

// Implement the Default trait for TypeId
impl Default for TypeId {
    fn default() -> Self {
        TypeId::Invalid
    }
}