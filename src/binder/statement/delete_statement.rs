// use std::fmt;
//
// use crate::binder::bound_expression::BoundExpression;
// use crate::binder::bound_statement::BoundStatement;
// use crate::binder::bound_table_ref::DefaultBoundTableRef;
// use crate::catalogue::column::Column;
// use crate::binder::expressions::bound_constant::BoundConstant;
// use crate::common::statement_type::StatementType;
// use crate::binder::bound_table_ref::{BoundTableRef, TableReferenceType};
//
//
// /// Represents a DELETE statement in SQL.
// pub struct DeleteStatement {
//     /// The table to delete from.
//     pub table: Box<DefaultBoundTableRef>,
//     /// The WHERE clause expression.
//     pub expr: Box<dyn BoundExpression>,
// }
//
// impl DeleteStatement {
//     /// Creates a new DeleteStatement.
//     pub fn new(table: Box<DefaultBoundTableRef>, expr: Box<dyn BoundExpression>) -> Self {
//         Self { table, expr }
//     }
// }
//
// impl BoundStatement for DeleteStatement {
//     fn statement_type(&self) -> StatementType {
//         StatementType::DeleteStatement
//     }
// }
//
// impl fmt::Display for DeleteStatement {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "DELETE FROM {} WHERE {}", self.table, self.expr)
//     }
// }
//
// #[cfg(test)]
// mod unit_tests {
//     use super::*;
//
//     struct MockBoundBaseTableRef;
//
//     impl fmt::Display for MockBoundBaseTableRef {
//         fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//             write!(f, "mock_table")
//         }
//     }
//
//     impl BoundTableRef for MockBoundBaseTableRef {
//         fn table_reference_type(&self) -> TableReferenceType {
//             todo!()
//         }
//
//         fn columns(&self) -> Vec<Column> {
//             vec![]
//         }
//
//         fn table_name(&self) -> &str {
//             "mock_table"
//         }
//     }
//
//     #[test]
//     fn test_delete_statement() {
//         let delete_stmt = DeleteStatement::new(
//             Box::new(DefaultBoundTableRef),
//             Box::new(BoundConstant::new(true)),
//         );
//
//         assert_eq!(delete_stmt.statement_type(), StatementType::DeleteStatement);
//         assert_eq!(delete_stmt.to_string(), "DELETE FROM mock_table WHERE true");
//     }
// }