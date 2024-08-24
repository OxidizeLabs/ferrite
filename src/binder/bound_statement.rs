use std::fmt;

use crate::common::statement_type::StatementType;

/// BoundStatement is the base trait of any type of bound SQL statement.
pub trait BoundStatement: fmt::Display {
    /// Returns the type of the statement.
    fn statement_type(&self) -> StatementType;
}

/// A default implementation for BoundStatement that can be used as a base
/// for concrete statement types.
pub struct DefaultBoundStatement {
    statement_type: StatementType,
}

impl DefaultBoundStatement {
    /// Creates a new DefaultBoundStatement with the given StatementType.
    pub fn new(statement_type: StatementType) -> Self {
        Self { statement_type }
    }
}

impl BoundStatement for DefaultBoundStatement {
    fn statement_type(&self) -> StatementType {
        self.statement_type
    }
}

impl fmt::Display for DefaultBoundStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToString not supported for this type of SQLStatement")
    }
}

// Custom error type for unsupported operations
#[derive(Debug)]
pub struct UnsupportedOperationError(String);

impl fmt::Display for UnsupportedOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UnsupportedOperationError {}