use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

// Every possible SQL type ID
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum TypeId {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Timestamp,
    VarChar,
    Vector,
    Invalid,
}

// Implement the Default trait for TypeId
impl Default for TypeId {
    fn default() -> Self {
        TypeId::Invalid
    }
}

impl Hash for TypeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the discriminant of the enum for hashing
        std::mem::discriminant(self).hash(state);
    }
}
