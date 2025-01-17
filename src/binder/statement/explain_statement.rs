// use crate::binder::bound_statement::{AnyBoundStatement, BoundStatement};
// use crate::common::statement_type::StatementType;
// use bitflags::bitflags;
// use std::any::Any;
// use std::fmt;
// use std::fmt::{Debug, Display, Formatter};
//
// bitflags! {
//     #[derive(Debug)]
//     pub struct ExplainOptions: u8 {
//         const INVALID   = 0b00000000;
//         const BINDER    = 0b00000001;
//         const PLANNER   = 0b00000010;
//         const OPTIMIZER = 0b00000100;
//         const SCHEMA    = 0b00001000;
//     }
// }
//
// pub struct ExplainStatement {
//     statement: Box<dyn AnyBoundStatement>,
//     options: ExplainOptions,
// }
//
// impl BoundStatement for ExplainStatement {
//     fn statement_type(&self) -> StatementType {
//         StatementType::ExplainStatement
//     }
//
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
// }
//
// impl ExplainStatement {
//     pub fn new(statement: Box<dyn AnyBoundStatement>, options: ExplainOptions) -> Self {
//         Self { statement, options }
//     }
// }
//
// impl Display for ExplainStatement {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         write!(f, "EXPLAIN ")?;
//         if self.options.contains(ExplainOptions::BINDER) {
//             write!(f, "BINDER ")?;
//         }
//         if self.options.contains(ExplainOptions::PLANNER) {
//             write!(f, "PLANNER ")?;
//         }
//         if self.options.contains(ExplainOptions::OPTIMIZER) {
//             write!(f, "OPTIMIZER ")?;
//         }
//         if self.options.contains(ExplainOptions::SCHEMA) {
//             write!(f, "SCHEMA ")?;
//         }
//         write!(f, "{}", self.statement.as_bound_statement())
//     }
// }
//
// impl Debug for ExplainStatement {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.debug_struct("ExplainStatement")
//             .field("statement", &self.statement.as_bound_statement())
//             .field("options", &self.options)
//             .finish()
//     }
// }
//
// #[cfg(test)]
// mod unit_tests {
//     use super::*;
//     use crate::binder::bound_table_ref::{DefaultBoundTableRef, TableReferenceType};
//     use crate::binder::statement::select_statement::SelectStatement;
//
//     #[test]
//     fn explain_statement() {
//         let select_stmt = SelectStatement::new(
//             Box::new(DefaultBoundTableRef::new(TableReferenceType::BaseTable)),
//             vec![],
//             None,
//             vec![],
//             None,
//             None,
//             None,
//             vec![],
//             vec![],
//             false,
//         );
//
//         let explain_stmt = ExplainStatement::new(
//             Box::new(select_stmt),
//             ExplainOptions::BINDER | ExplainOptions::PLANNER,
//         );
//
//         assert_eq!(
//             explain_stmt.statement_type(),
//             StatementType::ExplainStatement
//         );
//         assert!(explain_stmt
//             .to_string()
//             .starts_with("EXPLAIN BINDER PLANNER SELECT"));
//     }
// }
