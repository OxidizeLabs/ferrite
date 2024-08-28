use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display};

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

/// A default implementation for BoundTableRef that can be used as a base
/// for concrete table reference types.
#[derive(Clone)]
pub struct DefaultBoundTableRef {
    table_reference_type: TableReferenceType,
}

/// A bound table reference.
pub trait BoundTableRef: Display {
    fn table_reference_type(&self) -> TableReferenceType;
    fn is_invalid(&self) -> bool {
        self.table_reference_type() == TableReferenceType::Invalid
    }
    fn as_any(&self) -> &dyn Any;
    fn clone_box(&self) -> Box<dyn BoundTableRef>;
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundTableRef> {
        Box::new(self.clone())
    }
}

impl Display for DefaultBoundTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.table_reference_type {
            TableReferenceType::Invalid => write!(f, "Invalid"),
            TableReferenceType::Empty => write!(f, "<empty>"),
            _ => write!(f, "DefaultBoundTableRef({})", self.table_reference_type),
        }
    }
}

impl Display for TableReferenceType {
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

impl Debug for dyn BoundTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundTableRef({:?})", self.table_reference_type())
    }
}

impl Clone for Box<dyn BoundTableRef> {
    fn clone(&self) -> Self {
        self.clone_box()
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
        assert_eq!(invalid_ref.to_string(), "Invalid");

        let empty_ref = DefaultBoundTableRef::new(TableReferenceType::Empty);
        assert!(!empty_ref.is_invalid());
        assert_eq!(empty_ref.to_string(), "<empty>");

        let base_table_ref = DefaultBoundTableRef::new(TableReferenceType::BaseTable);
        assert!(!base_table_ref.is_invalid());
        assert_eq!(base_table_ref.to_string(), "DefaultBoundTableRef(BaseTable)");
    }
}