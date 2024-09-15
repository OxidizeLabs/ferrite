use crate::binder::bound_expression::BoundExpression;
use crate::binder::bound_statement::BoundStatement;
use crate::binder::bound_table_ref::BoundTableRef;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// Represents a DELETE statement in SQL.
pub struct DeleteStatement {
    /// The table to delete from.
    table: Box<dyn BoundTableRef>,
    /// The WHERE clause expression.
    expr: Box<dyn BoundExpression>,
}

impl DeleteStatement {
    /// Creates a new DeleteStatement.
    pub fn new(table: Box<dyn BoundTableRef>, expr: Box<dyn BoundExpression>) -> Self {
        Self { table, expr }
    }
}

impl Debug for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Deleted Table: {:?}", self.table)
    }
}

impl BoundStatement for DeleteStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::DeleteStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.expr)
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::binder::bound_table_ref::TableReferenceType;
    use crate::binder::expressions::bound_constant::BoundConstant;
    use super::*;

    #[derive(Clone)]
    struct MockBoundBaseTableRef;

    impl Display for MockBoundBaseTableRef {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "mock_table")
        }
    }

    impl BoundTableRef for MockBoundBaseTableRef {
        fn table_reference_type(&self) -> TableReferenceType {
            TableReferenceType::BaseTable
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundTableRef> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn delete_statement() {
        let delete_stmt = DeleteStatement::new(
            Box::new(MockBoundBaseTableRef),
            Box::new(BoundConstant::new(true)),
        );

        assert_eq!(delete_stmt.statement_type(), StatementType::DeleteStatement);
        assert_eq!(delete_stmt.to_string(), "DELETE FROM mock_table WHERE true");
    }
}