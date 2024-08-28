use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt::{Debug, Display};

pub struct DefaultBoundStatement {
    statement_type: StatementType,
}

pub trait BoundStatement: Any + Display + Debug {
    fn statement_type(&self) -> StatementType;
    fn as_any(&self) -> &dyn Any;
}

pub trait AnyBoundStatement {
    fn as_bound_statement(&self) -> &dyn BoundStatement;
}

impl<T: BoundStatement> AnyBoundStatement for T {
    fn as_bound_statement(&self) -> &dyn BoundStatement {
        self
    }
}

impl AnyBoundStatement for Box<dyn BoundStatement> {
    fn as_bound_statement(&self) -> &dyn BoundStatement {
        &**self
    }
}

impl DefaultBoundStatement {
    pub fn new(statement_type: StatementType) -> Self {
        Self { statement_type }
    }
}

impl Display for DefaultBoundStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DefaultBoundStatement({})", self.statement_type)
    }
}

impl Debug for dyn AnyBoundStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_bound_statement())
    }
}