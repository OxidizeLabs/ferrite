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
pub struct BetweenExpression {
    expr: Arc<Expression>,
    low: Arc<Expression>,
    high: Arc<Expression>,
    negated: bool,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl BetweenExpression {
    pub fn new(
        expr: Arc<Expression>,
        low: Arc<Expression>,
        high: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        let children = vec![expr.clone(), low.clone(), high.clone()];

        Self {
            expr,
            low,
            high,
            negated,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for BetweenExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        let low = self.low.evaluate(tuple, schema)?;
        let high = self.high.evaluate(tuple, schema)?;

        // Check if value is between low and high (inclusive)
        let between = value >= low && value <= high;

        Ok(Value::new(if self.negated { !between } else { between }))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let low = self
            .low
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let high = self
            .high
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Check if value is between low and high (inclusive)
        let between = value >= low && value <= high;

        Ok(Value::new(if self.negated { !between } else { between }))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match child_idx {
            0 => &self.expr,
            1 => &self.low,
            2 => &self.high,
            _ => panic!("Invalid child index {} for BetweenExpression", child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert_eq!(
            children.len(),
            3,
            "BetweenExpression requires exactly 3 children"
        );
        Arc::new(Expression::Between(BetweenExpression::new(
            children[0].clone(),
            children[1].clone(),
            children[2].clone(),
            self.negated,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        self.expr.validate(schema)?;
        self.low.validate(schema)?;
        self.high.validate(schema)?;

        // Ensure the types are comparable
        let expr_type = self.expr.get_return_type().get_type();
        let low_type = self.low.get_return_type().get_type();
        let high_type = self.high.get_return_type().get_type();

        // Check against low bound
        if expr_type != low_type {
            return Err(ExpressionError::TypeMismatch {
                expected: expr_type,
                actual: low_type,
            });
        }

        // Check against high bound
        if expr_type != high_type {
            return Err(ExpressionError::TypeMismatch {
                expected: expr_type,
                actual: high_type,
            });
        }

        Ok(())
    }
}

impl Display for BetweenExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}BETWEEN {} AND {}",
            self.expr,
            if self.negated { "NOT " } else { "" },
            self.low,
            self.high
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Integer),
        ])
    }

    fn create_test_tuple(values: Vec<Value>, schema: &Schema) -> Tuple {
        Tuple::new(&values, schema, RID::new(0, 0))
    }

    fn create_constant(value: i32) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("const", TypeId::Integer),
            vec![],
        )))
    }

    #[test]
    fn test_between_basic() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(vec![Value::new(5), Value::new(10)], &schema);

        let expr = BetweenExpression::new(
            create_constant(7),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_between_not() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(vec![Value::new(5), Value::new(10)], &schema);

        let expr = BetweenExpression::new(
            create_constant(15),
            create_constant(5),
            create_constant(10),
            true, // NOT BETWEEN
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_between_edge_cases() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(vec![Value::new(5), Value::new(10)], &schema);

        // Test equal to lower bound
        let expr1 = BetweenExpression::new(
            create_constant(5),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(expr1.evaluate(&tuple, &schema).unwrap(), Value::new(true));

        // Test equal to upper bound
        let expr2 = BetweenExpression::new(
            create_constant(10),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(expr2.evaluate(&tuple, &schema).unwrap(), Value::new(true));
    }

    #[test]
    fn test_between_join() {
        let left_schema = create_test_schema();
        let right_schema = create_test_schema();

        let left_tuple = create_test_tuple(vec![Value::new(5), Value::new(10)], &left_schema);
        let right_tuple = create_test_tuple(vec![Value::new(15), Value::new(20)], &right_schema);

        let expr = BetweenExpression::new(
            create_constant(7),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_between_type_validation() {
        // Create expressions with mismatched types
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(7),
            Column::new("value", TypeId::Integer),
            vec![],
        )));

        let low_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5.0),
            Column::new("low", TypeId::Decimal),
            vec![],
        )));

        let high_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(10),
            Column::new("high", TypeId::Integer),
            vec![],
        )));

        let expr = BetweenExpression::new(
            value_expr,
            low_expr,
            high_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let schema = create_test_schema();
        let result = expr.validate(&schema);

        assert!(matches!(
            result,
            Err(ExpressionError::TypeMismatch {
                expected: TypeId::Integer,
                actual: TypeId::Decimal
            })
        ));
    }

    #[test]
    fn test_between_display() {
        let expr = BetweenExpression::new(
            create_constant(7),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(expr.to_string(), "7 BETWEEN 5 AND 10");

        let expr_not = BetweenExpression::new(
            create_constant(7),
            create_constant(5),
            create_constant(10),
            true,
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(expr_not.to_string(), "7 NOT BETWEEN 5 AND 10");
    }

    #[test]
    fn test_between_children() {
        let value_expr = create_constant(7);
        let low_expr = create_constant(5);
        let high_expr = create_constant(10);

        let expr = BetweenExpression::new(
            value_expr.clone(),
            low_expr.clone(),
            high_expr.clone(),
            false,
            Column::new("result", TypeId::Boolean),
        );

        assert_eq!(expr.get_children().len(), 3);
        assert_eq!(expr.get_child_at(0), &value_expr);
        assert_eq!(expr.get_child_at(1), &low_expr);
        assert_eq!(expr.get_child_at(2), &high_expr);
    }

    #[test]
    #[should_panic(expected = "Invalid child index")]
    fn test_between_invalid_child_index() {
        let expr = BetweenExpression::new(
            create_constant(7),
            create_constant(5),
            create_constant(10),
            false,
            Column::new("result", TypeId::Boolean),
        );

        expr.get_child_at(3); // Should panic
    }
}
