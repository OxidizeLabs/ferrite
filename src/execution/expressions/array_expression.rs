// use crate::catalogue::column::Column;
// use crate::catalogue::schema::Schema;
// use crate::common::exception::{ArrayExpressionError, ExpressionError};
// use crate::execution::expressions::abstract_expression::Expression;
// use crate::execution::expressions::constant_value_expression::ConstantExpression;
// use crate::storage::table::tuple::Tuple;
// use crate::types_db::type_id::TypeId;
// use crate::types_db::value::{Val, Value};
// use std::fmt;
// use std::fmt::{Display, Formatter};
// use std::rc::Rc;
//
// #[derive(Debug, Clone)]
// pub struct ArrayExpression {
//     children: Vec<Rc<Expression>>,
//     ret_type: Column,
// }
//
// impl ArrayExpression {
//     pub fn new(children: Vec<Rc<Expression>>) -> Self {
//         Self {
//             ret_type: Column::new("<val>", TypeId::Vector),
//             children,
//         }
//     }
//
//     pub fn get_children(&self) -> &[Rc<Expression>] {
//         &self.children
//     }
//
//     pub fn get_ret_type(&self) -> &Column {
//         &self.ret_type
//     }
//
//     fn evaluate_children<F>(&self, mut eval_func: F) -> Result<Value, ArrayExpressionError>
//     where
//         F: FnMut(&Expression) -> Result<Value, ExpressionError>,
//     {
//         let values: Result<Vec<i32>, ArrayExpressionError> = self.children
//             .iter()
//             .map(|expr| {
//                 let val = eval_func(expr).map_err(|e| ArrayExpressionError::ChildEvaluationError(e.to_string()))?;
//                 match val.get_value() {
//                     Val::Decimal(d) => {
//                         // Convert f64 to i32, handling potential loss of precision
//                         (d.round() as i32)
//                             .try_into()
//                             .map_err(|_| ArrayExpressionError::FloatToIntConversionError(*d))
//                     }
//                     _ => Err(ArrayExpressionError::NonDecimalType),
//                 }
//             })
//             .collect();
//
//         values.map(|v| Value::new(Val::Vector(v)))
//     }
//
//     pub fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
//         self.evaluate_children(|child| child.evaluate(tuple, schema))
//             .map_err(|e| ExpressionError::Array(e))
//     }
//
//     pub fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
//         self.evaluate_children(|child| child.evaluate_join(left_tuple, left_schema, right_tuple, right_schema))
//             .map_err(|e| ExpressionError::Array(e))
//     }
// }
//
// impl Display for ArrayExpression {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         write!(f, "[")?;
//         let mut iter = self.children.iter();
//         if let Some(first) = iter.next() {
//             write!(f, "{}", first)?;
//             for child in iter {
//                 write!(f, ", {}", child)?;
//             }
//         }
//         write!(f, "]")
//     }
// }
//
// impl Expression {
//     pub fn Array(children: Vec<Rc<Expression>>) -> Rc<Self> {
//         Rc::new(Self::Array(ArrayExpression::new(children)))
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::types_db::types::Type;
//
//     #[test]
//     fn array_expression() {
//         let children = vec![
//             Rc::new(Expression::Constant(ConstantExpression::new(
//                 Value::new(Val::Decimal(1.0)),
//                 Column::new("const", TypeId::Decimal),
//             ))),
//             Rc::new(Expression::Constant(ConstantExpression::new(
//                 Value::new(Val::Decimal(2.0)),
//                 Column::new("const", TypeId::Decimal),
//             ))),
//             Rc::new(Expression::Constant(ConstantExpression::new(
//                 Value::new(Val::Decimal(3.0)),
//                 Column::new("const", TypeId::Decimal),
//             ))),
//         ];
//         let expr = Expression::Array(children);
//
//         let schema = Schema::new(vec![]);
//         let tuple = Tuple::new(vec![], schema.clone(), 0);
//
//         let result = expr.evaluate(&tuple, &schema).expect("Evaluation should succeed");
//         assert_eq!(result.get_type_id(), TypeId::Vector);
//         if let Val::Vector(vec) = result.get_value() {
//             assert_eq!(vec, &[1, 2, 3]);
//         } else {
//             panic!("Expected Vector value");
//         }
//     }
//
//     #[test]
//     fn array_expression_invalid_type() {
//         let children = vec![
//             Rc::new(Expression::Constant(ConstantExpression::new(
//                 Value::new(Val::Decimal(1.0)),
//                 Column::new("const", TypeId::Decimal),
//             ))),
//             Rc::new(Expression::Constant(ConstantExpression::new(
//                 Value::new(Val::Integer(2)),
//                 Column::new("const", TypeId::Integer),
//             ))),
//         ];
//         let expr = Expression::Array(children);
//
//         let schema = Schema::new(vec![]);
//         let tuple = Tuple::new(vec![], schema.clone(), 0);
//
//         let result = expr.evaluate(&tuple, &schema);
//         assert!(result.is_err());
//         assert!(matches!(result.unwrap_err(), ExpressionError::Array(_)));
//     }
// }