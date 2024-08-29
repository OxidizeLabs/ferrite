use crate::binder::bound_statement::BoundStatement;
use crate::binder::bound_expression::BoundExpression;
use crate::binder::expressions::bound_column_ref::BoundColumnRef;
use crate::binder::expressions::bound_constant::BoundConstant;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display};
use std::fmt::Formatter;
use crate::binder::bound_table_ref::{BoundTableRef, TableReferenceType};

/// Represents a bound UPDATE statement in SQL.
#[derive(Clone)]
pub struct UpdateStatement {
    table: Box<dyn BoundTableRef>,
    filter_expr: Option<Box<dyn BoundExpression>>,
    target_expr: Vec<(Box<BoundColumnRef>, Box<dyn BoundExpression>)>,
}

impl UpdateStatement {
    /// Creates a new UpdateStatement.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to be updated
    /// * `filter_expr` - The WHERE clause expression (if any)
    /// * `target_expr` - The list of column-expression pairs to be updated
    pub fn new(
        table: Box<dyn BoundTableRef>,
        filter_expr: Option<Box<dyn BoundExpression>>,
        target_expr: Vec<(Box<BoundColumnRef>, Box<dyn BoundExpression>)>,
    ) -> Self {
        Self {
            table,
            filter_expr,
            target_expr,
        }
    }
}

impl BoundStatement for UpdateStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::UpdateStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for UpdateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
        for (i, (col, expr)) in self.target_expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} = {}", col, expr)?;
        }
        if let Some(filter) = &self.filter_expr {
            write!(f, " WHERE {}", filter)?;
        }
        Ok(())
    }
}

impl Debug for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateStatement")
            .field("table", &self.table)
            .field("filter_expr", &self.filter_expr.is_some())
            .field("target_expr", &self.target_expr.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
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
    fn update_statement_creation_and_display() {
        let table = Box::new(MockBoundTableRef);
        let filter_expr = Some(Box::new(BoundConstant::new(true)) as Box<dyn BoundExpression>);
        let target_expr = vec![
            (
                Box::new(BoundColumnRef::new(vec!["column1".to_string()])),
                Box::new(BoundConstant::new(42)) as Box<dyn BoundExpression>,
            ),
            (
                Box::new(BoundColumnRef::new(vec!["column2".to_string()])),
                Box::new(BoundConstant::new("value")) as Box<dyn BoundExpression>,
            ),
        ];

        let update_stmt = UpdateStatement::new(table, filter_expr, target_expr);

        assert_eq!(update_stmt.statement_type(), StatementType::UpdateStatement);
        assert_eq!(
            update_stmt.to_string(),
            "UPDATE mock_table SET column1 = 42, column2 = \"value\" WHERE true"
        );
    }
}