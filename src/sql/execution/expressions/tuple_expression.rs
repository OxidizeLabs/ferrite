use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct TupleExpression {
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl TupleExpression {
    pub fn new(expressions: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            children: expressions,
            return_type,
        }
    }
}

impl ExpressionOps for TupleExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let mut values = Vec::new();
        for expr in &self.children {
            values.push(expr.evaluate(tuple, schema)?);
        }
        // Create tuple/row value
        Ok(Value::new_vector(values))
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let mut values = Vec::new();
        for expr in &self.children {
            values.push(expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?);
        }
        Ok(Value::new_vector(values))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::Tuple(TupleExpression::new(
            children,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate each child expression
        for expr in &self.children {
            expr.validate(schema)?;
        }
        Ok(())
    }
}

impl Display for TupleExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ROW({})",
               self.children.iter()
                   .map(|e| e.to_string())
                   .collect::<Vec<_>>()
                   .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;
    use sqlparser::ast::Value as SQLValue;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_tuple_expression_evaluate() {
        // Create a schema and tuple for testing
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], schema.clone(), RID::new(0, 0));

        // Create constant expressions for the tuple elements
        let const1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const1", TypeId::Integer),
            vec![],
        )));
        let const2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("test"),
            Column::new("const2", TypeId::VarChar),
            vec![],
        )));
        let const3 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3.14),
            Column::new("const3", TypeId::Decimal),
            vec![],
        )));

        // Create a tuple expression with these constants
        let tuple_expr = TupleExpression::new(
            vec![const1, const2, const3],
            Column::new("tuple", TypeId::Vector),
        );

        // Evaluate the tuple expression
        let result = tuple_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is a vector with the expected values
        match result.get_val() {
            crate::types_db::value::Val::Vector(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::new(1));
                assert_eq!(values[1], Value::new("test"));
                assert_eq!(values[2], Value::new(3.14));
            }
            _ => panic!("Expected Vector value"),
        }
    }

    #[test]
    fn test_tuple_expression_display() {
        // Create literal expressions for the tuple elements
        let lit1 = Arc::new(Expression::Literal(LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap()));
        let lit2 = Arc::new(Expression::Literal(LiteralValueExpression::new(SQLValue::SingleQuotedString("hello".to_string())).unwrap()));

        // Create a tuple expression with these literals
        let tuple_expr = TupleExpression::new(
            vec![lit1, lit2],
            Column::new("tuple", TypeId::Vector),
        );

        // Verify the string representation
        assert_eq!(tuple_expr.to_string(), "ROW(42, 'hello')");
    }

    #[test]
    fn test_tuple_expression_clone_with_children() {
        // Create constant expressions for the tuple elements
        let const1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const1", TypeId::Integer),
            vec![],
        )));
        let const2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const2", TypeId::Integer),
            vec![],
        )));

        // Create a tuple expression
        let tuple_expr = TupleExpression::new(
            vec![const1.clone()],
            Column::new("tuple", TypeId::Vector),
        );

        // Clone with different children
        let cloned_expr = tuple_expr.clone_with_children(vec![const1.clone(), const2.clone()]);

        // Verify the cloned expression has the new children
        match cloned_expr.as_ref() {
            Expression::Tuple(t) => {
                assert_eq!(t.get_children().len(), 2);
                assert_eq!(t.get_return_type().get_name(), "tuple");
            }
            _ => panic!("Expected Tuple expression"),
        }
    }

    #[test]
    fn test_tuple_expression_validate() {
        // Create a schema with one column
        let schema = Schema::new(vec![Column::new("col1", TypeId::Integer)]);

        // Create a valid column reference expression
        let col_ref = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0, 0, Column::new("col1", TypeId::Integer), vec![],
            ),
        ));

        // Create a tuple expression with the column reference
        let tuple_expr = TupleExpression::new(
            vec![col_ref],
            Column::new("tuple", TypeId::Vector),
        );

        // Validation should succeed
        assert!(tuple_expr.validate(&schema).is_ok());

        // Create an invalid column reference
        let invalid_col_ref = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0, 1, Column::new("col2", TypeId::Integer), vec![],
            ),
        ));

        // Create a tuple expression with the invalid column reference
        let invalid_tuple_expr = TupleExpression::new(
            vec![invalid_col_ref],
            Column::new("tuple", TypeId::Vector),
        );

        // Validation should fail
        assert!(invalid_tuple_expr.validate(&schema).is_err());
    }
} 