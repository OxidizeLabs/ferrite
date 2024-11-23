use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;

/// Represents a vector of integers in the database type system.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorType;

impl VectorType {
    /// Creates a new `VectorType` instance.
    pub fn new() -> Self {
        VectorType
    }
}

impl Type for VectorType {
    /// Returns the type ID for `VectorType`.
    fn get_type_id(&self) -> TypeId {
        TypeId::Vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_type_new() {
        let vt = VectorType::new();
        assert_eq!(vt.get_type_id(), TypeId::Vector);
    }

    #[test]
    fn test_is_coercible_from() {
        let vt = VectorType::new();
        assert!(vt.is_coercible_from(TypeId::Vector));
        assert!(!vt.is_coercible_from(TypeId::Integer));
    }
}
