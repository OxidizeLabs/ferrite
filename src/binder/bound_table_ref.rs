use std::fmt;

/// Table reference types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableReferenceType {
    Invalid = 0,
    BaseTable = 1,
    Join = 3,
    CrossProduct = 4,
    ExpressionList = 5,
    Subquery = 6,
    Cte = 7,
    Empty = 8,
}

impl fmt::Display for TableReferenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableReferenceType::Invalid => write!(f, "Invalid"),
            TableReferenceType::BaseTable => write!(f, "BaseTable"),
            TableReferenceType::Join => write!(f, "Join"),
            TableReferenceType::CrossProduct => write!(f, "CrossProduct"),
            TableReferenceType::ExpressionList => write!(f, "ExpressionList"),
            TableReferenceType::Subquery => write!(f, "Subquery"),
            TableReferenceType::Cte => write!(f, "CTE"),
            TableReferenceType::Empty => write!(f, "Empty"),
        }
    }
}

/// A bound table reference.
pub trait BoundTableRef: fmt::Display {
    /// Returns the type of table reference.
    fn table_reference_type(&self) -> TableReferenceType;

    /// Checks if the table reference is invalid.
    fn is_invalid(&self) -> bool {
        self.table_reference_type() == TableReferenceType::Invalid
    }
}

/// A default implementation for BoundTableRef that can be used as a base
/// for concrete table reference types.
pub struct DefaultBoundTableRef {
    table_reference_type: TableReferenceType,
}

impl DefaultBoundTableRef {
    /// Creates a new DefaultBoundTableRef with the given TableReferenceType.
    pub fn new(table_reference_type: TableReferenceType) -> Self {
        Self { table_reference_type }
    }
}

impl BoundTableRef for DefaultBoundTableRef {
    fn table_reference_type(&self) -> TableReferenceType {
        self.table_reference_type
    }
}

impl fmt::Display for DefaultBoundTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.table_reference_type {
            TableReferenceType::Invalid => write!(f, ""),
            TableReferenceType::Empty => write!(f, "<empty>"),
            _ => panic!("ToString should be implemented in derived structs"),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn table_reference_type_display() {
        assert_eq!(format!("{}", TableReferenceType::BaseTable), "BaseTable");
        assert_eq!(format!("{}", TableReferenceType::Join), "Join");
        assert_eq!(format!("{}", TableReferenceType::Invalid), "Invalid");
    }

    #[test]
    fn default_bound_table_ref() {
        let invalid_ref = DefaultBoundTableRef::new(TableReferenceType::Invalid);
        assert!(invalid_ref.is_invalid());
        assert_eq!(invalid_ref.to_string(), "");

        let empty_ref = DefaultBoundTableRef::new(TableReferenceType::Empty);
        assert!(!empty_ref.is_invalid());
        assert_eq!(empty_ref.to_string(), "<empty>");
    }

    #[test]
    #[should_panic(expected = "ToString should be implemented in derived structs")]
    fn default_bound_table_ref_panic() {
        let base_table_ref = DefaultBoundTableRef::new(TableReferenceType::BaseTable);
        base_table_ref.to_string(); // This should panic
    }
}