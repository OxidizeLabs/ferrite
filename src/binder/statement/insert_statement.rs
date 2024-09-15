use crate::binder::bound_statement::BoundStatement;
use crate::binder::bound_table_ref::BoundTableRef;
use crate::binder::statement::select_statement::SelectStatement;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// Represents a bound INSERT statement in SQL.
#[derive(Clone)]
pub struct InsertStatement {
    table: Box<dyn BoundTableRef>,
    select: Box<SelectStatement>,
}

impl InsertStatement {
    /// Creates a new InsertStatement.
    ///
    /// # Arguments
    ///
    /// * `table` - The table into which data will be inserted.
    /// * `select` - The select statement that provides the data to be inserted.
    pub fn new(table: Box<dyn BoundTableRef>, select: Box<SelectStatement>) -> Self {
        Self { table, select }
    }
}

impl BoundStatement for InsertStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::InsertStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for InsertStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "INSERT INTO {} {}", self.table, self.select)
    }
}

impl Debug for InsertStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InsertStatement")
            .field("table", &self.table)
            .field("select", &self.select)
            .finish()
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::binder::bound_table_ref::TableReferenceType;
    use crate::binder::expressions::bound_constant::BoundConstant;
    use super::*;

    struct MockBoundTableRef;
    impl BoundTableRef for MockBoundTableRef {
        fn table_reference_type(&self) -> TableReferenceType {
            todo!()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundTableRef> {
            Box::new(MockBoundTableRef)
        }
    }

    impl Display for MockBoundTableRef {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "mock_table")
        }
    }

    #[test]
    fn insert_statement() {
        let select_stmt = SelectStatement::new(
            Box::new(MockBoundTableRef),
            vec![Box::new(BoundConstant::new("column1")), Box::new(BoundConstant::new("column2"))],
            None,
            vec![],
            None,
            None,
            None,
            vec![],
            vec![],
            false,
        );

        let insert_stmt = InsertStatement::new(
            Box::new(MockBoundTableRef),
            Box::new(select_stmt),
        );

        assert_eq!(insert_stmt.statement_type(), StatementType::InsertStatement);
        assert_eq!(
            insert_stmt.to_string(),
            "INSERT INTO mock_table SELECT column1, column2 FROM mock_table"
        );
    }
}